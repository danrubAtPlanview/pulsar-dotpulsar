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
using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.Transactions;
using System.Collections.Concurrent;
using System.Diagnostics;

public sealed class Producer<TMessage> : IProducer<TMessage>, IExtendedProducer, IRegisterEvent
{
    private readonly string _operationName;
    private readonly KeyValuePair<string, object?>[] _activityTags;
    private readonly KeyValuePair<string, object?>[] _meterTags;
    private readonly bool _attachTraceInfoToMessages;
    private readonly bool _transactionsEnabled;
    private readonly SequenceId _sequenceId;
    private readonly StateManager<ProducerState> _state;
    private readonly IConnectionPool _connectionPool;
    private readonly IHandleException _exceptionHandler;
    private readonly ICompressorFactory? _compressorFactory;
    private readonly ProducerOptions<TMessage> _options;
    private readonly ProcessManager _processManager;
    private readonly ConcurrentDictionary<int, SubProducer> _producers;
    private readonly IMessageRouter _messageRouter;
    private readonly CancellationTokenSource _cts;
    private readonly Executor _executor;
    private readonly TimeSpan _operationTimeout;
    private MessageCrypto? _initialMessageCrypto;
    private int _isDisposed;
    private int _producerCount;
    private Exception? _faultException;

    public Uri ServiceUrl { get; }
    public string Topic { get; }
    public ISendChannel<TMessage> SendChannel { get; }
    public IState<ProducerState> State => _state;

    public Producer(
        Uri serviceUrl,
        ProducerOptions<TMessage> options,
        IHandleException exceptionHandler,
        IConnectionPool connectionPool,
        ICompressorFactory? compressorFactory,
        bool transactionsEnabled,
        TimeSpan operationTimeout = default)
    {
        // Upstream: ProducerImpl#ProducerImpl, #addPublicKeyCipher
        // The public keys are read and the data key is wrapped now, so that a key that can not be used makes the creation of the producer fail, before anything is started.
        // The first sub producer takes this message crypto, and the sub producers of the other partitions of the topic create their own, so each of them rotates its data key on its own.
        _initialMessageCrypto = CreateMessageCrypto(options);

        _operationName = $"{options.Topic} send";
        _activityTags =
        [
            new("messaging.destination", options.Topic),
            new("messaging.destination_kind", "topic"),
            new("messaging.system", "pulsar"),
            new("messaging.url", serviceUrl),
        ];
        _meterTags =
        [
            new("topic", options.Topic)
        ];
        _attachTraceInfoToMessages = options.AttachTraceInfoToMessages;
        _transactionsEnabled = transactionsEnabled;
        _sequenceId = new SequenceId(options.InitialSequenceId);
        _state = CreateStateManager();
        ServiceUrl = serviceUrl;
        Topic = options.Topic;
        _isDisposed = 0;
        _options = options;
        _exceptionHandler = exceptionHandler;
        _connectionPool = connectionPool;
        _compressorFactory = compressorFactory;
        _operationTimeout = operationTimeout;
        _processManager = new ProcessManager();
        _messageRouter = options.MessageRouter;
        _cts = new CancellationTokenSource();
        _executor = new Executor(Guid.Empty, this, _exceptionHandler);
        _producers = new ConcurrentDictionary<int, SubProducer>();
        SendChannel = new SendChannel<TMessage>(this);
        _ = Setup();
    }

    // Upstream: ProducerConfigurationData#isEncryptionEnabled
    // Messages are encrypted if there is at least one encryption key and a crypto key reader. The data key is created, and wrapped for every key, before the message crypto is returned.
    private static MessageCrypto? CreateMessageCrypto(ProducerOptions<TMessage> options)
    {
        var cryptoKeyReader = options.CryptoKeyReader;

        if (cryptoKeyReader is null || !options.EncryptionKeys.Any())
            return null;

        var messageCrypto = new MessageCrypto(true);

        try
        {
            messageCrypto.AddPublicKeyCipher(options.EncryptionKeys, cryptoKeyReader);
        }
        catch
        {
            messageCrypto.Dispose();
            throw;
        }

        return messageCrypto;
    }

    private async Task Setup()
    {
        await Task.Yield();

        try
        {
            await _executor.Execute(Monitor, _cts.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (_cts.IsCancellationRequested)
                return;

            _faultException = exception;
            _state.SetState(ProducerState.Faulted);
        }
    }

    private async Task Monitor()
    {
        var numberOfPartitions = await _connectionPool.GetNumberOfPartitions(Topic, _cts.Token).ConfigureAwait(false);
        var isPartitionedTopic = numberOfPartitions != 0;
        var numberOfSubProducers = isPartitionedTopic ? numberOfPartitions : 1;
        var monitoringTasks = new Task<ProducerState>[numberOfSubProducers];
        var states = Enumerable.Repeat(ProducerState.Disconnected, (int) numberOfSubProducers).ToArray(); // What a sub producer is before it has reported

        for (var i = 0; i < numberOfSubProducers; ++i)
        {
            var topicName = isPartitionedTopic ? GetPartitionedTopicName(i) : Topic;
            var producer = CreateSubProducer(topicName, isPartitionedTopic ? i : -1);
            _ = _producers.TryAdd(i, producer);
            monitoringTasks[i] = producer.State.OnStateChangeFrom(ProducerState.Disconnected, _cts.Token).AsTask();
        }

        Interlocked.Exchange(ref _producerCount, monitoringTasks.Length);

        while (true)
        {
            await Task.WhenAny(monitoringTasks).ConfigureAwait(false);

            for (var i = 0; i < monitoringTasks.Length; ++i)
            {
                var task = monitoringTasks[i];
                if (!task.IsCompleted)
                    continue;

                var state = task.Result;
                states[i] = state;
                monitoringTasks[i] = _producers[i].State.OnStateChangeFrom(state, _cts.Token).AsTask();
            }

            if (!isPartitionedTopic)
                _state.SetState(states[0]);
            else if (states.Any(x => x == ProducerState.Faulted))
                _state.SetState(ProducerState.Faulted);
            else if (states.Any(x => x == ProducerState.Fenced))
                _state.SetState(ProducerState.Fenced);
            else if (states.All(x => x == ProducerState.Connected))
                _state.SetState(ProducerState.Connected);
            else if (states.All(x => x == ProducerState.Disconnected))
                _state.SetState(ProducerState.Disconnected);
            else if (states.Any(x => x == ProducerState.Disconnected))
                _state.SetState(ProducerState.PartiallyConnected);
            else
                _state.SetState(ProducerState.WaitingForExclusive);
        }
    }

    private SubProducer CreateSubProducer(string topic, int partition)
    {
        var correlationId = Guid.NewGuid();
        var producerName = _options.ProducerName;
        var schema = _options.Schema;
        var producerAccessMode = (PulsarApi.ProducerAccessMode) _options.ProducerAccessMode;
        var producerProperties = _options.ProducerProperties;
        var messageCrypto = Interlocked.Exchange(ref _initialMessageCrypto, null) ?? CreateMessageCrypto(_options);
        var factory = new ProducerChannelFactory(correlationId, _processManager, _connectionPool, topic, producerName, producerAccessMode, schema.SchemaInfo, _compressorFactory, producerProperties, _transactionsEnabled, _options.InitialSubscriptionName, messageCrypto, _options.CryptoFailureAction);
        var stateManager = CreateStateManager();
        var initialChannel = new NotReadyChannel<TMessage>();
        var executor = new Executor(correlationId, _processManager, _exceptionHandler);
        var producer = new SubProducer(
            correlationId,
            _processManager,
            initialChannel,
            executor,
            stateManager,
            factory,
            partition,
            _options.MaxPendingMessages,
            topic,
            _options.EnableBatching,
            _options.BatcherType,
            _options.BatchingMaxMessages,
            _options.BatchingMaxBytes,
            _options.BatchingMaxPublishDelay,
            _options.InitialSequenceId,
            _options.SendTimeout,
            IsChunkingEnabled(topic),
            _options.ChunkMaxMessageSize,
            messageCrypto,
            _operationTimeout,
            _options.FailOnlyTheRejectedMessage);
        var process = new ProducerProcess(correlationId, stateManager, producer);
        _processManager.Add(process);
        process.Start();
        return producer;
    }

    // Upstream: ProducerImpl#isChunkingEnabled
    // Chunking is only done on persistent topics, so the option does nothing on a non-persistent topic
    private bool IsChunkingEnabled(string topic)
        => _options.EnableChunking && !topic.StartsWith("non-persistent://", StringComparison.Ordinal);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _cts.Cancel();
        _cts.Dispose();

        await _processManager.DisposeAsync().ConfigureAwait(false);

        foreach (var subProducer in _producers.Values)
            await subProducer.DisposeAsync().ConfigureAwait(false);

        // The message crypto that no sub producer has taken, which is the case if the producer is disposed before its sub producers are created
        Interlocked.Exchange(ref _initialMessageCrypto, null)?.Dispose();

        _state.SetState(ProducerState.Closed);
    }

    private async ValueTask<int> ChoosePartitions(MessageMetadata metadata, CancellationToken cancellationToken)
    {
        if (_producerCount == 0)
        {
            var newState = await _state.OnStateChangeFrom(ProducerState.Disconnected, cancellationToken).ConfigureAwait(false);
            if (_faultException is not null)
                throw new ProducerFaultedException(_faultException);
            if (newState == ProducerState.Closed)
                throw new ProducerClosedException();
        }

        if (_producerCount == 1)
            return 0;

        return _messageRouter.ChoosePartition(metadata, _producerCount);
    }

    public async ValueTask<MessageId> Send(MessageMetadata metadata, TMessage message, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<MessageId>();
        using var registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

        ValueTask OnMessageSent(MessageId messageId)
        {
            tcs.TrySetResult(messageId);
#if NET6_0_OR_GREATER
            return ValueTask.CompletedTask;
#else
            return new ValueTask();
#endif
        }

        await InternalSend(metadata, message, tcs, OnMessageSent, x => tcs.TrySetException(x), cancellationToken).ConfigureAwait(false);
        return await tcs.Task.ConfigureAwait(false);
    }

    public async ValueTask Enqueue(
        MessageMetadata metadata,
        TMessage message,
        Func<MessageId, ValueTask>? onMessageSent = default,
        CancellationToken cancellationToken = default)
        => await InternalSend(metadata, message, null, onMessageSent, cancellationToken: cancellationToken).ConfigureAwait(false);

    private async ValueTask InternalSend(
        MessageMetadata metadata,
        TMessage message,
        TaskCompletionSource<MessageId>? tcs = default,
        Func<MessageId, ValueTask>? onMessageSent = default,
        Action<Exception>? onFailed = default,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var transaction = metadata.Transaction is null ? null : Transaction.From(metadata.Transaction);
        transaction?.EnsureOpen();

        var autoAssignSequenceId = metadata.SequenceId == 0;
        if (autoAssignSequenceId)
            metadata.SequenceId = _sequenceId.FetchNext();

        var activity = DotPulsarActivitySource.StartProducerActivity(metadata, _operationName, _activityTags);
        if (activity is not null && _attachTraceInfoToMessages)
        {
            metadata[Constants.TraceParent] = activity.Id;
            if (activity.TraceStateString is not null)
                metadata[Constants.TraceState] = activity.TraceStateString;
        }

        var startTimestamp = DotPulsarMeter.MessageSentEnabled ? Stopwatch.GetTimestamp() : 0;

        var isRegisteredWithTransaction = false;

        try
        {
            var data = _options.Schema.Encode(message);
            tcs ??= new TaskCompletionSource<MessageId>();

            // Upstream: TypedMessageBuilderImpl#sendAsync
            // The send is registered before anything is awaited, so a commit that is started after this call has been made waits for the send.
            // The send ends when its task is completed, which is handled by the continuation below, so no continuation is allocated for the transaction.
            if (transaction is not null)
            {
                transaction.BeginOp();
                isRegisteredWithTransaction = true;
            }

            var partition = await ChoosePartitions(metadata, cancellationToken).ConfigureAwait(false);
            var subProducer = _producers[partition];

            // Upstream: ProducerImpl#internalSendWithTxnAsync
            // The registration of the partition is not awaited here. The sub producer waits for it before the message is written, to keep the order of the messages.
            Task? registrationGate = null;
            if (transaction is not null)
            {
                var registration = transaction.RegisterProducedTopic(subProducer.Topic, CancellationToken.None);
                registrationGate = registration.IsCompletedSuccessfully ? null : registration.AsTask();
            }

            var sendOp = new SendOp(metadata.Metadata, data, tcs, transaction, registrationGate);
            await subProducer.Send(sendOp, cancellationToken).ConfigureAwait(false);

            _ = tcs.Task.ContinueWith(async task =>
            {
                sendOp.Transaction?.EndOp(task.IsFaulted || task.IsCanceled);

                if (startTimestamp != 0)
                    DotPulsarMeter.MessageSent(startTimestamp, _meterTags);

                if (task.IsFaulted || task.IsCanceled)
                {
                    Exception exception = task.IsCanceled ? new OperationCanceledException() : task.Exception!;
                    FailActivity(exception, activity);

                    if (autoAssignSequenceId)
                        metadata.SequenceId = 0;

                    try
                    {
                        onFailed?.Invoke(exception);
                    }
                    catch
                    {
                        // Ignore
                    }

                    return;
                }

                CompleteActivity(task.Result, sendOp.Data.Length, activity);

                try
                {
                    if (onMessageSent is not null)
                        await onMessageSent.Invoke(task.Result).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // ignored
                }
            }, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            FailActivity(exception, activity);

            if (autoAssignSequenceId)
                metadata.SequenceId = 0;

            // The send never reached the send queue, so nothing else will end it and the transaction would wait for it forever
            if (isRegisteredWithTransaction)
                transaction!.EndOp(true);

            throw;
        }
    }

    public async ValueTask Flush(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await Task.WhenAll(_producers.Values.Select(producer => producer.Flush())).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask WaitForSendQueueEmpty(CancellationToken cancellationToken)
        => await Task.WhenAll(_producers.Values.Select(producer => producer.WaitForSendQueueEmpty(cancellationToken).AsTask())).ConfigureAwait(false);

    private static void CompleteActivity(MessageId messageId, long payloadSize, Activity? activity)
    {
        if (activity is null)
            return;

        if (activity.IsAllDataRequested)
        {
            activity.SetMessageId(messageId);
            activity.SetPayloadSize(payloadSize);
            activity.SetStatus(ActivityStatusCode.Ok);
        }

        activity.Dispose();
    }

    private static void FailActivity(Exception exception, Activity? activity)
    {
        if (activity is null)
            return;

        if (activity.IsAllDataRequested)
            activity.AddException(exception);

        activity.Dispose();
    }

    private void ThrowIfDisposed()
    {
        if (_isDisposed != 0)
            throw new ProducerDisposedException(GetType().FullName!);
    }

    private static StateManager<ProducerState> CreateStateManager()
        => new(ProducerState.Disconnected, ProducerState.Closed, ProducerState.Faulted, ProducerState.Fenced);

    private string GetPartitionedTopicName(int partitionNumber) => $"{Topic}-partition-{partitionNumber}";

    public void Register(IEvent @event) { }
}
