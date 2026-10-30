/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Internal;

using DotPulsar.Abstractions;
using System.Buffers;
using System.Collections.Concurrent;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java (possibleSendToDeadLetterTopicMessages, processPossibleToDLQ, initDeadLetterProducerIfNeeded), apache/pulsar-client-cpp@f71d7e9 lib/ConsumerImpl.cc (processPossibleToDLQ)
public sealed class DeadLetterHandler<TMessage> : IAsyncDisposable
{
    private const uint ProducerMaxPendingMessages = 500;
    private const string RandomCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private const int RandomLength = 5;

    private static readonly TimeSpan _defaultSendTimeout = TimeSpan.FromSeconds(30);
    private static readonly Random _random;

    private readonly ConcurrentDictionary<(ulong LedgerId, ulong EntryId), IMessage<TMessage>[]> _candidates;
    private readonly string _topic;
    private readonly string _subscriptionName;
    private readonly string _consumerName;
    private readonly SchemaInfo _schemaInfo;
    private readonly Func<ProducerOptions<ReadOnlySequence<byte>>, string?, IProducer<ReadOnlySequence<byte>>> _producerFactory;
    private readonly TimeSpan _sendTimeout;
    private readonly AsyncLock _producerLock;
    private IProducer<ReadOnlySequence<byte>>? _producer;
    private int _isDisposed;

    static DeadLetterHandler()
        => _random = new Random();

    // The 'producerFactory' creates the dead letter producer from the options and the optional name of the initial subscription of the dead letter topic.
    // It is called at most once at a time, and again only after the previous producer reached a final state. The handler owns and disposes the producers.
    // The 'sendTimeout' bounds the sending of the messages of one entry, and defaults to 30 seconds.
    public DeadLetterHandler(
        DeadLetterPolicy policy,
        string topic,
        string subscriptionName,
        string consumerName,
        SchemaInfo schemaInfo,
        Func<ProducerOptions<ReadOnlySequence<byte>>, string?, IProducer<ReadOnlySequence<byte>>> producerFactory,
        TimeSpan? sendTimeout = null)
    {
        MaxRedeliverCount = policy.MaxRedeliverCount;
        DeadLetterTopic = string.IsNullOrWhiteSpace(policy.DeadLetterTopic) ? RetryMessageUtil.GetDeadLetterTopic(topic, subscriptionName) : policy.DeadLetterTopic!;
        InitialSubscriptionName = string.IsNullOrWhiteSpace(policy.InitialSubscriptionName) ? null : policy.InitialSubscriptionName;
        RedeliveryCountExceeded = null;

        _candidates = new ConcurrentDictionary<(ulong LedgerId, ulong EntryId), IMessage<TMessage>[]>();
        _topic = topic;
        _subscriptionName = subscriptionName;
        _consumerName = consumerName;
        _schemaInfo = schemaInfo;
        _producerFactory = producerFactory;
        _sendTimeout = sendTimeout ?? _defaultSendTimeout;
        _producerLock = new AsyncLock();
        _producer = null;
        _isDisposed = 0;
    }

    public uint MaxRedeliverCount { get; }

    public string DeadLetterTopic { get; }

    public string? InitialSubscriptionName { get; }

    // Requests the redelivery of an entry that exceeded the max redeliver count. Set once, before the handler is used.
    public Func<MessageId, ValueTask>? RedeliveryCountExceeded { get; set; }

    public int Count => _candidates.Count;

    public bool Contains(ulong ledgerId, ulong entryId)
        => _candidates.ContainsKey((ledgerId, entryId));

    // A message of a non-batched entry. A later registration of the entry replaces the earlier one.
    public void Register(ulong ledgerId, ulong entryId, IMessage<TMessage> message)
        => _candidates[(ledgerId, entryId)] = [message];

    // The messages of a batched entry that were not acknowledged. A later registration of the entry replaces the earlier one, and no messages means no candidate.
    public void Register(ulong ledgerId, ulong entryId, IMessage<TMessage>[] messages)
    {
        if (messages.Length == 0)
            _ = _candidates.TryRemove((ledgerId, entryId), out _);
        else
            _candidates[(ledgerId, entryId)] = messages;
    }

    public void Remove(ulong ledgerId, ulong entryId)
        => _ = _candidates.TryRemove((ledgerId, entryId), out _);

    public void Clear()
        => _candidates.Clear();

    // Fire and forget, never throws. The callback must not be awaited by the caller, which may hold the receive lock of the channel.
    public void OnRedeliveryCountExceeded(MessageId entryMessageId)
    {
        var callback = RedeliveryCountExceeded;

        if (callback is not null)
            _ = Invoke(callback, entryMessageId);
    }

    // Send the messages of the entry to the dead letter topic, and then acknowledge the entry. The batch index of the message id is discarded.
    // Returns false if the entry is not a candidate, or if anything failed, in which case the entry must be redelivered as usual.
    // The entry stays a candidate until all of its messages have been sent. A cancellation requested by the caller is not swallowed.
    // Upstream: ConsumerImpl#processPossibleToDLQ (C# deviation: sends all the messages of the entry before acknowledging it, and acknowledges the entry rather than the batch index)
    public async ValueTask<bool> TryDeadLetter(MessageId messageId, Func<MessageId, CancellationToken, ValueTask> acknowledge, CancellationToken cancellationToken)
    {
        var key = (messageId.LedgerId, messageId.EntryId);

        if (_isDisposed != 0 || !_candidates.TryGetValue(key, out var messages))
            return false;

        IProducer<ReadOnlySequence<byte>>? producer = null;

        try
        {
            producer = await GetProducer(cancellationToken).ConfigureAwait(false);

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(_sendTimeout);

            for (var i = 0; i < messages.Length; ++i)
            {
                var message = messages[i];
                await producer.Send(CreateMetadata(message), message.Data, timeout.Token).ConfigureAwait(false);
            }

            _ = _candidates.TryRemove(key, out _);

            await acknowledge(new MessageId(messageId.LedgerId, messageId.EntryId, messageId.Partition, -1, messageId.Topic), cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            if (producer is not null)
                await DisposeIfFinal(producer).ConfigureAwait(false);

            return false;
        }
    }

    // The same properties, keys and event time as the upstream, but not the sequence id, producer name, publish time and schema version
    // Upstream: ConsumerImpl#getPropertiesMap, ConsumerImpl#copyMessageKeysIfNeeded, ConsumerImpl#copyMessageEventTime (C# deviation: an empty key is not copied, because the key of a received message without a key is empty)
    public MessageMetadata CreateMetadata(IMessage<TMessage> message)
    {
        var originMessageId = RetryMessageUtil.FormatMessageId(message.MessageId);
        var properties = new SortedDictionary<string, string>(StringComparer.Ordinal);

        foreach (var property in message.Properties)
            properties[property.Key] = property.Value;

        AddIfAbsent(properties, RetryMessageUtil.RealTopic, _topic);
        AddIfAbsent(properties, RetryMessageUtil.OriginMessageIdDeprecated, originMessageId);
        AddIfAbsent(properties, RetryMessageUtil.OriginMessageId, originMessageId);
        AddIfAbsent(properties, RetryMessageUtil.RealSubscription, _subscriptionName);

        var metadata = new MessageMetadata();

        foreach (var property in properties)
            metadata[property.Key] = property.Value;

        if (!string.IsNullOrEmpty(message.Key))
        {
            if (message.HasBase64EncodedKey)
                metadata.KeyBytes = message.KeyBytes;
            else
                metadata.Key = message.Key;
        }

        if (message.OrderingKey is not null)
            metadata.OrderingKey = message.OrderingKey;

        metadata.EventTime = message.EventTime;

        return metadata;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _candidates.Clear();

        await _producerLock.DisposeAsync().ConfigureAwait(false);

        var producer = Interlocked.Exchange(ref _producer, null);

        if (producer is not null)
            await DisposeProducer(producer).ConfigureAwait(false);
    }

    private static void AddIfAbsent(SortedDictionary<string, string> properties, string key, string value)
    {
        if (!properties.ContainsKey(key))
            properties.Add(key, value);
    }

    private static string CreateRandomSuffix()
    {
        var characters = new char[RandomLength];

        lock (_random)
        {
            for (var i = 0; i < characters.Length; ++i)
                characters[i] = RandomCharacters[_random.Next(RandomCharacters.Length)];
        }

        return new string(characters);
    }

    private static async Task Invoke(Func<MessageId, ValueTask> callback, MessageId messageId)
    {
        try
        {
            await callback(messageId).ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }

    private static async ValueTask DisposeProducer(IProducer<ReadOnlySequence<byte>> producer)
    {
        try
        {
            await producer.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }

    // One producer per handler, created on the first use. A producer in a final state (closed, faulted or fenced) is disposed and replaced.
    // Upstream: ConsumerImpl#initDeadLetterProducerIfNeeded (C# deviation: no backoff before recreating the producer)
    private async ValueTask<IProducer<ReadOnlySequence<byte>>> GetProducer(CancellationToken cancellationToken)
    {
        var producer = Volatile.Read(ref _producer);

        if (producer is not null && !producer.State.IsFinalState())
            return producer;

        using (await _producerLock.Lock(cancellationToken).ConfigureAwait(false))
        {
            producer = Volatile.Read(ref _producer);

            if (producer is not null)
            {
                if (!producer.State.IsFinalState())
                    return producer;

                if (ReferenceEquals(Interlocked.CompareExchange(ref _producer, null, producer), producer))
                    await DisposeProducer(producer).ConfigureAwait(false);
            }

            producer = _producerFactory(CreateProducerOptions(), InitialSubscriptionName);
            Volatile.Write(ref _producer, producer);
            return producer;
        }
    }

    private async ValueTask DisposeIfFinal(IProducer<ReadOnlySequence<byte>> producer)
    {
        try
        {
            if (!producer.State.IsFinalState())
                return;
        }
        catch
        {
            return;
        }

        if (ReferenceEquals(Interlocked.CompareExchange(ref _producer, null, producer), producer))
            await DisposeProducer(producer).ConfigureAwait(false);
    }

    // Upstream: ConsumerImpl#initDeadLetterProducerIfNeeded (C# deviation: no 'enableChunking(true)' until chunking is supported, and no 'blockIfQueueFull(false)')
    private ProducerOptions<ReadOnlySequence<byte>> CreateProducerOptions()
        => new(DeadLetterTopic, new DeadLetterSchema(_schemaInfo))
        {
            MaxPendingMessages = ProducerMaxPendingMessages,
            ProducerName = $"{_topic}-{_subscriptionName}-{_consumerName}-{CreateRandomSuffix()}-DLQ"
        };
}
