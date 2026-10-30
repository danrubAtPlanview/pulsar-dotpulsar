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
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using Microsoft.Extensions.ObjectPool;
using System.Runtime.ExceptionServices;

public sealed class SubConsumer<TMessage> : IConsumer<TMessage>, IContainsChannel
{
    // Upstream: ConsumerImpl#MAX_REDELIVER_UNACKNOWLEDGED
    private const int MaxRedeliverUnacknowledged = 1000;

    private readonly Guid _correlationId;
    private readonly IRegisterEvent _eventRegister;
    private IConsumerChannel<TMessage> _channel;
    private readonly ObjectPool<CommandAck> _commandAckPool;
    private readonly IExecute _executor;
    private readonly IState<ConsumerState> _state;
    private readonly IConsumerChannelFactory<TMessage> _factory;
    private readonly bool _ackReceiptEnabled;
    private readonly TimeSpan _operationTimeout;
    private readonly ConsumerEpoch _consumerEpoch;
    private readonly UnAckedMessageTracker? _unAckedMessageTracker;
    private readonly NegativeAcknowledgeTracker _negativeAcknowledgeTracker;
    private readonly DeadLetterHandler<TMessage>? _deadLetterHandler;
    private readonly ChunkedMessageAssembler _chunkAssembler;
    private readonly MessageCrypto? _messageCrypto;
    private int _isDisposed;
    private Exception? _faultException;

    public Uri ServiceUrl { get; }
    public string SubscriptionName { get; }
    public SubscriptionType SubscriptionType { get; }
    public string Topic { get; }
    public IState<ConsumerState> State => _state;

    // The number of entries that the ack timeout waits for, which is 0 if the ack timeout is not enabled
    public int UnAckedMessageCount => _unAckedMessageTracker?.Count ?? 0;

    public SubConsumer(
        Guid correlationId,
        Uri serviceUrl,
        string subscriptionName,
        SubscriptionType subscriptionType,
        string topic,
        IRegisterEvent eventRegister,
        IConsumerChannel<TMessage> initialChannel,
        IExecute executor,
        IState<ConsumerState> state,
        IConsumerChannelFactory<TMessage> factory,
        bool ackReceiptEnabled,
        TimeSpan operationTimeout,
        ConsumerEpoch consumerEpoch,
        UnAckedMessageTracker? unAckedMessageTracker,
        NegativeAcknowledgeTracker negativeAcknowledgeTracker,
        DeadLetterHandler<TMessage>? deadLetterHandler,
        ChunkedMessageAssembler chunkAssembler,
        MessageCrypto? messageCrypto)
    {
        _correlationId = correlationId;
        ServiceUrl = serviceUrl;
        SubscriptionName = subscriptionName;
        SubscriptionType = subscriptionType;
        Topic = topic;
        _eventRegister = eventRegister;
        _channel = initialChannel;
        _executor = executor;
        _state = state;
        _factory = factory;
        _ackReceiptEnabled = ackReceiptEnabled;
        _operationTimeout = operationTimeout;
        _consumerEpoch = consumerEpoch;
        _unAckedMessageTracker = unAckedMessageTracker;
        _negativeAcknowledgeTracker = negativeAcknowledgeTracker;
        _deadLetterHandler = deadLetterHandler;
        _chunkAssembler = chunkAssembler;
        _messageCrypto = messageCrypto;
        _commandAckPool = new DefaultObjectPool<CommandAck>(new DefaultPooledObjectPolicy<CommandAck>());
        _isDisposed = 0;

        _eventRegister.Register(new ConsumerCreated(_correlationId));
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _eventRegister.Register(new ConsumerDisposed(_correlationId));
        _unAckedMessageTracker?.Dispose();
        _negativeAcknowledgeTracker.Dispose();
        _chunkAssembler.Dispose();
        _messageCrypto?.Dispose();

        if (_deadLetterHandler is not null)
            await _deadLetterHandler.DisposeAsync().ConfigureAwait(false);

        await DisposeChannel().ConfigureAwait(false);
    }

    private async ValueTask DisposeChannel()
    {
        await _channel.ClosedByClient(CancellationToken.None).ConfigureAwait(false);
        await _channel.DisposeAsync().ConfigureAwait(false);
    }

    public async ValueTask<IMessage<TMessage>> Receive(CancellationToken cancellationToken)
        => await _executor.Execute(() => InternalReceive(cancellationToken), cancellationToken).ConfigureAwait(false);

    // Upstream: ConsumerBase#trackUnAckedMsgIfNoListener
    // The message is tracked when it is handed to the application, and not when it is received from the broker
    public void Track(IMessage<TMessage> message)
        => _unAckedMessageTracker?.Add(message.MessageId);

    public async ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken)
        => await InternalAcknowledge(messageId, CommandAck.Types.AckType.Individual, cancellationToken).ConfigureAwait(false);

    public async ValueTask Acknowledge(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default)
        => await InternalAcknowledge(messageIds, cancellationToken).ConfigureAwait(false);

    public async ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken)
        => await InternalAcknowledge(messageId, CommandAck.Types.AckType.Cumulative, cancellationToken).ConfigureAwait(false);

    // Upstream: ConsumerBase#acknowledgeAsync(MessageId, Transaction)
    // This method and AcknowledgeCumulative are not async, so that they do not need a state machine of their own, which is allocated when the receipt has not arrived at once.
    // An exception that is thrown before the acknowledgment starts is thrown by the call and not by the returned task, which makes no difference to a caller that awaits it.
    public ValueTask Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken)
    {
        var transactionImpl = Transaction.From(transaction);
        transactionImpl.EnsureOpen();

        // An individual acknowledgment of a chunked message has an id for every chunk, and the pooled command has one, so it is not pooled
        if (ChunkMessageIdExpander.IsChunked(messageId.ChunkMessageIds))
        {
            var chunkedCommand = CreateTransactionalAck(CommandAck.Types.AckType.Individual, transactionImpl);
            AddIndividualMessageId(chunkedCommand, messageId);
            return InternalTransactionalAcknowledge(chunkedCommand, transactionImpl, false, cancellationToken);
        }

        var command = CreatePooledTransactionalAck(CommandAck.Types.AckType.Individual, transactionImpl, messageId);
        return InternalTransactionalAcknowledge(command, transactionImpl, true, cancellationToken);
    }

    // Upstream: ConsumerBase#acknowledgeAsync(List<MessageId>, Transaction)
    // Only the single message id acknowledgment checks that the transaction is open before anything is done. The other acknowledgments fail the
    // operation that is registered with the transaction, as in Java, because the transaction coordinator rejects them.
    public async ValueTask Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken)
    {
        var transactionImpl = Transaction.From(transaction);

        var command = CreateTransactionalAck(CommandAck.Types.AckType.Individual, transactionImpl);
        foreach (var messageId in messageIds)
        {
            AddIndividualMessageId(command, messageId);
        }

        if (command.MessageId.Count == 0)
            return;

        await InternalTransactionalAcknowledge(command, transactionImpl, false, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: ConsumerBase#acknowledgeCumulativeAsync(MessageId, Transaction)
    public ValueTask AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken)
    {
        if (SubscriptionType is SubscriptionType.Shared or SubscriptionType.KeyShared)
            throw new ConfigurationException("Cannot use cumulative acks on a non-exclusive/non-failover subscription");

        var transactionImpl = Transaction.From(transaction);

        var command = CreatePooledTransactionalAck(CommandAck.Types.AckType.Cumulative, transactionImpl, messageId);
        return InternalTransactionalAcknowledge(command, transactionImpl, true, cancellationToken);
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages(Set)
    // The broker only redelivers the messages of a Shared or KeyShared subscription one by one, so every other subscription type redelivers all messages.
    public async ValueTask RedeliverUnacknowledgedMessages(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken)
    {
        Guard();

        var entries = ToEntryLevel(messageIds);

        if (entries.Count == 0)
            return;

        if (SubscriptionType is not (SubscriptionType.Shared or SubscriptionType.KeyShared))
        {
            await RedeliverUnacknowledgedMessages(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_deadLetterHandler is not null)
            entries = await RemoveDeadLettered(entries, cancellationToken).ConfigureAwait(false);

        entries = ExpandChunks(entries);

        for (var offset = 0; offset < entries.Count; offset += MaxRedeliverUnacknowledged)
        {
            var command = new CommandRedeliverUnacknowledgedMessages();

            for (var i = offset; i < Math.Min(offset + MaxRedeliverUnacknowledged, entries.Count); ++i)
            {
                command.MessageIds.Add(new MessageIdData { LedgerId = entries[i].LedgerId, EntryId = entries[i].EntryId, Partition = entries[i].Partition });
            }

            await _executor.Execute(() => InternalRedeliverUnacknowledgedMessages(command, cancellationToken), cancellationToken).ConfigureAwait(false);
        }
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages()
    // The messages that the channel has already received are dropped, and so are the messages that are on their way from the broker when the subscription has an epoch.
    public async ValueTask RedeliverUnacknowledgedMessages(CancellationToken cancellationToken)
    {
        Guard();

        if (SubscriptionType is SubscriptionType.Exclusive or SubscriptionType.Failover)
            _consumerEpoch.Increment();

        _unAckedMessageTracker?.Clear();

        var command = new CommandRedeliverUnacknowledgedMessages { ConsumerEpoch = (ulong) _consumerEpoch.Value };
        await _executor.Execute(() => InternalRedeliverUnacknowledgedMessages(command, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    // Upstream: ConsumerImpl#negativeAcknowledge
    public ValueTask NegativeAcknowledge(MessageId messageId, CancellationToken cancellationToken)
    {
        Guard();
        _unAckedMessageTracker?.Remove(messageId);
        _negativeAcknowledgeTracker.Add(messageId);
        return default;
    }

    // Upstream: ConsumerImpl#removeExpireIncompleteChunkedMessages
    // The chunks of the incomplete chunked messages that expired are acknowledged, once and in the background. A failure is ignored, because the broker
    // redelivers the chunks if the consumer reconnects.
    public async ValueTask AcknowledgeChunks(IReadOnlyList<MessageIdData> chunkMessageIds)
    {
        if (_isDisposed != 0 || _faultException is not null)
            return;

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };

        foreach (var chunkMessageId in chunkMessageIds)
        {
            command.MessageId.Add(chunkMessageId);
        }

        using var timeout = new CancellationTokenSource(_operationTimeout > TimeSpan.Zero ? _operationTimeout : Timeout.InfiniteTimeSpan);
        await _executor.TryExecuteOnce(() => InternalAcknowledge(command, timeout.Token), timeout.Token).ConfigureAwait(false);
    }

    public async ValueTask Unsubscribe(CancellationToken cancellationToken)
    {
        var unsubscribe = new CommandUnsubscribe();
        await _executor.Execute(() => InternalUnsubscribe(unsubscribe, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId)
    // A chunked message is seeked to by its first chunk, because the broker redelivers entries
    public async ValueTask Seek(MessageId messageId, CancellationToken cancellationToken)
    {
        var seek = new CommandSeek { MessageId = (messageId.FirstChunkMessageId ?? messageId).ToMessageIdData() };
        await _executor.Execute(() => InternalSeek(seek, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask Seek(ulong publishTime, CancellationToken cancellationToken)
    {
        var seek = new CommandSeek { MessagePublishTime = publishTime };
        await _executor.Execute(() => InternalSeek(seek, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<MessageId> GetLastMessageId(CancellationToken cancellationToken)
    {
        var getLastMessageId = new CommandGetLastMessageId();
        return await _executor.Execute(() => InternalGetLastMessageId(getLastMessageId, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    private void Guard()
    {
        if (_isDisposed != 0)
            throw new ConsumerDisposedException(GetType().FullName!);

        if (_faultException is not null)
            throw _faultException;
    }

    public async Task EstablishNewChannel(CancellationToken cancellationToken)
    {
        var channel = await _executor.Execute(() => _factory.Create(cancellationToken), cancellationToken).ConfigureAwait(false);

        var oldChannel = _channel;
        if (oldChannel is not null)
            await oldChannel.DisposeAsync().ConfigureAwait(false);

        _channel = channel;
    }

    public async ValueTask CloseChannel(CancellationToken cancellationToken)
        => await _channel.ClosedByClient(cancellationToken).ConfigureAwait(false);

    public async ValueTask ChannelFaulted(Exception exception)
    {
        _faultException = exception;
        _unAckedMessageTracker?.Dispose();
        _chunkAssembler.Dispose();
        _messageCrypto?.Dispose();
        await DisposeChannel().ConfigureAwait(false);
    }

    private async ValueTask<BaseCommand?> InternalAcknowledge(CommandAck command, CancellationToken cancellationToken)
    {
        Guard();
        return await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    // An acknowledgment that does not wait for a receipt has no response, so it needs neither the result of the executor nor the layer of SendAndAwaitReceipt.
    // This is the hot path of the consumer: with the generic overloads of the executor, it took 32 B and about 40% more time per acknowledgment.
    // The closure is created here and not in the methods that call this, because a closure is created when its method is entered, and it would be for every acknowledgment.
    private ValueTask SendWithoutReceipt(CommandAck command, CancellationToken cancellationToken)
        => _executor.Execute(() => InternalAcknowledgeWithoutReceipt(command, cancellationToken), cancellationToken);

    private async ValueTask InternalAcknowledgeWithoutReceipt(CommandAck command, CancellationToken cancellationToken)
    {
        Guard();
        await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    // A transactional acknowledgment always waits for the receipt of the broker, and so does any other if the consumer is configured to do so
    private async ValueTask<BaseCommand?> SendAndAwaitReceipt(CommandAck command, bool transactional, CancellationToken cancellationToken)
    {
        if (_operationTimeout <= TimeSpan.Zero)
            return await Send(command, transactional, cancellationToken).ConfigureAwait(false);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_operationTimeout);

        try
        {
            return await Send(command, transactional, timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeoutCts.IsCancellationRequested)
        {
            throw new OperationTimeoutException($"The acknowledgment receipt was not received within {_operationTimeout}");
        }
    }

    // The closure of the plain acknowledgment is created in its own method, so that a transactional acknowledgment does not allocate it
    private ValueTask<BaseCommand?> Send(CommandAck command, bool transactional, CancellationToken cancellationToken)
        => transactional ? SendTransactional(command, cancellationToken) : SendPlain(command, cancellationToken);

    private ValueTask<BaseCommand?> SendPlain(CommandAck command, CancellationToken cancellationToken)
        => _executor.Execute(() => InternalAcknowledge(command, cancellationToken), cancellationToken);

    private async ValueTask<BaseCommand?> SendTransactional(CommandAck command, CancellationToken cancellationToken)
    {
        // The channel rejects an acknowledgment when the broker does not support acknowledgment receipts. That must fail only this
        // acknowledgment, but an exception that is not retried would fault the consumer when it is thrown inside the executor.
        NotSupportedException? notSupported = null;

        // A value task and not a task, because a task allocates for every response. The channel call is not in a method of its own for the same reason:
        // every method that waits for the response adds an allocation when the response does not arrive at once.
        async ValueTask<BaseCommand?> SendOnce()
        {
            try
            {
                Guard();
                return await _channel.SendTransactional(command, cancellationToken).ConfigureAwait(false);
            }
            catch (NotSupportedException exception)
            {
                notSupported = exception;
                return null;
            }
        }

        var response = await _executor.Execute(SendOnce, cancellationToken).ConfigureAwait(false);

        if (notSupported is not null)
            ExceptionDispatchInfo.Capture(notSupported).Throw();

        return response;
    }

    private async ValueTask InternalRedeliverUnacknowledgedMessages(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken)
    {
        Guard();
        await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<MessageId> InternalGetLastMessageId(CommandGetLastMessageId command, CancellationToken cancellationToken)
    {
        Guard();
        return await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    private async Task InternalSeek(CommandSeek command, CancellationToken cancellationToken)
    {
        Guard();
        await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<IMessage<TMessage>> InternalReceive(CancellationToken cancellationToken)
    {
        Guard();
        return await _channel.Receive(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask InternalUnsubscribe(CommandUnsubscribe command, CancellationToken cancellationToken)
    {
        Guard();
        await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask InternalAcknowledge(MessageId messageId, CommandAck.Types.AckType ackType, CancellationToken cancellationToken)
    {
        var commandAck = _commandAckPool.Get();
        ResetPooledAck(commandAck);
        commandAck.AckType = ackType;

        // Upstream: PersistentAcknowledgmentsGroupingTracker#flushAsync
        // An individual acknowledgment of a chunked message acknowledges every chunk, and a cumulative one only the last chunk, which acknowledges all before it.
        // The ids of the chunks belong to the message id, so they are removed from the command again, before the next acknowledgment reuses its first id.
        if (ackType == CommandAck.Types.AckType.Individual && ChunkMessageIdExpander.IsChunked(messageId.ChunkMessageIds))
        {
            commandAck.MessageId.Clear();
            ChunkMessageIdExpander.TryExpand(messageId.ChunkMessageIds, commandAck.MessageId);
        }
        else if (commandAck.MessageId.Count == 0)
            commandAck.MessageId.Add(messageId.ToMessageIdData());
        else
            commandAck.MessageId[0].MapFrom(messageId);

        try
        {
            if (_ackReceiptEnabled)
            {
                var response = await SendAndAwaitReceipt(commandAck, false, cancellationToken).ConfigureAwait(false);

                // The response is evaluated outside the executor, so that a server error fails only this call and does not fault the consumer
                response?.ThrowIfAckError();
            }
            else
            {
                await SendWithoutReceipt(commandAck, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            // Only an expanded command has more than one id
            if (commandAck.MessageId.Count > 1)
                commandAck.MessageId.Clear();

            _commandAckPool.Return(commandAck);
        }
    }

    private async ValueTask InternalAcknowledge(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken)
    {
        var commandAck = _commandAckPool.Get();
        ResetPooledAck(commandAck);
        commandAck.AckType = CommandAck.Types.AckType.Individual;
        commandAck.MessageId.Clear();

        foreach (var messageId in messageIds)
        {
            if (!ChunkMessageIdExpander.TryExpand(messageId.ChunkMessageIds, commandAck.MessageId))
                commandAck.MessageId.Add(messageId.ToMessageIdData());
        }

        try
        {
            if (_ackReceiptEnabled)
            {
                var response = await SendAndAwaitReceipt(commandAck, false, cancellationToken).ConfigureAwait(false);

                // The response is evaluated outside the executor, so that a server error fails only this call and does not fault the consumer
                response?.ThrowIfAckError();
            }
            else
            {
                await SendWithoutReceipt(commandAck, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            commandAck.MessageId.Clear();
            _commandAckPool.Return(commandAck);
        }
    }

    // Upstream: ConsumerBase#doAcknowledgeWithTxn
    // The acknowledgment is an operation of the transaction, and a commit or an abort waits for it. It begins before anything is awaited, so that a commit that follows right
    // after waits for it, and it ends when the acknowledgment ends, failed or not. The operation is not registered with the task of the acknowledgment (Transaction.RegisterAckOp),
    // because that needs a continuation and a method of its own when the receipt has not arrived yet.
    //
    // A pooled command goes back to the pool when the acknowledgment ends, whatever the outcome is, and not before. Nothing uses the command after that: the awaits below do not
    // end before the channel is done with it (a timeout or a cancellation stops the send, and the send ends then), and the connection has serialized it by then.
    private async ValueTask InternalTransactionalAcknowledge(CommandAck command, Transaction transaction, bool pooled, CancellationToken cancellationToken)
    {
        transaction.BeginOp();
        var hasFailed = true;

        try
        {
            await transaction.RegisterAckedTopic(Topic, SubscriptionName, cancellationToken).ConfigureAwait(false);

            RemoveFromUnAckedTracker(command);

            var response = await SendAndAwaitReceipt(command, true, cancellationToken).ConfigureAwait(false);

            // The response is evaluated outside the executor (see InternalAcknowledge), so that a conflict fails only this acknowledgment
            response?.ThrowIfAckError();

            hasFailed = false;
        }
        finally
        {
            if (pooled)
                _commandAckPool.Return(command);

            transaction.EndOp(hasFailed);
        }
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse
    // The command of an acknowledgment of one message id comes from the pool of the plain acknowledgments, together with its MessageIdData. It must be given back by
    // TransactionalAcknowledge. Its message id is mapped as that of a plain acknowledgment, which clears the ack set, the batch size and the chunk id of the former use.
    private CommandAck CreatePooledTransactionalAck(CommandAck.Types.AckType ackType, Transaction transaction, MessageId messageId)
    {
        var command = _commandAckPool.Get();
        ResetPooledAck(command);
        command.AckType = ackType;
        command.TxnidMostBits = transaction.Id.MostSigBits;
        command.TxnidLeastBits = transaction.Id.LeastSigBits;

        if (command.MessageId.Count == 0)
            command.MessageId.Add(new MessageIdData());

        var messageIdData = command.MessageId[0];
        messageIdData.MapFrom(messageId);

        // The batch size is needed by the channel, if it does not know the batch of the message
        if (messageId.BatchSize > 0)
            messageIdData.BatchSize = messageId.BatchSize;

        return command;
    }

    // The command of an acknowledgment of several message ids is not pooled, because the number of message ids is not bounded. The request id is assigned when it is sent.
    private static CommandAck CreateTransactionalAck(CommandAck.Types.AckType ackType, Transaction transaction)
        => new()
        {
            AckType = ackType,
            TxnidMostBits = transaction.Id.MostSigBits,
            TxnidLeastBits = transaction.Id.LeastSigBits
        };

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse (C# deviation: the list overload expands chunked messages too)
    // An individual acknowledgment of a chunked message acknowledges every chunk, in the same command and so in the same transaction
    private static void AddIndividualMessageId(CommandAck command, MessageId messageId)
    {
        if (!ChunkMessageIdExpander.TryExpand(messageId.ChunkMessageIds, command.MessageId))
            command.MessageId.Add(ToTransactionalMessageIdData(messageId));
    }

    // The batch size is needed by the channel, if it does not know the batch of the message
    private static MessageIdData ToTransactionalMessageIdData(MessageId messageId)
    {
        var messageIdData = messageId.ToMessageIdData();

        if (messageId.BatchSize > 0)
            messageIdData.BatchSize = messageId.BatchSize;

        return messageIdData;
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse
    // An entry is no longer waited for when the acknowledgment is sent, whatever the outcome of the transaction is. A message of a batch is only acknowledged
    // individually, and the entry is not complete until the other messages of the batch are acknowledged (C# deviation: Java does not remove the entry then either).
    private void RemoveFromUnAckedTracker(CommandAck command)
    {
        if (_unAckedMessageTracker is null)
            return;

        if (command.AckType == CommandAck.Types.AckType.Cumulative)
        {
            var messageId = command.MessageId[0];
            _unAckedMessageTracker.RemoveMessagesTill(messageId.LedgerId, messageId.EntryId);
            return;
        }

        foreach (var messageId in command.MessageId)
        {
            if (messageId.BatchIndex < 0)
                _unAckedMessageTracker.Remove(messageId.LedgerId, messageId.EntryId);
        }
    }

    // The broker redelivers entries, so the batch index of a message id is of no use. Every entry is only sent once.
    // The chunks of a chunked message are kept, so the message can be dead lettered as a whole, and are expanded by ExpandChunks.
    // Upstream: ConsumerImpl#getRedeliveryMessageIdData
    private static List<MessageId> ToEntryLevel(IEnumerable<MessageId> messageIds)
    {
        var entries = new List<MessageId>();
        var added = new HashSet<(ulong LedgerId, ulong EntryId)>();

        foreach (var messageId in messageIds)
        {
            if (added.Add((messageId.LedgerId, messageId.EntryId)))
                entries.Add(new MessageId(messageId.LedgerId, messageId.EntryId, messageId.Partition, -1, messageId.Topic, messageId.FirstChunkMessageId, messageId.ChunkMessageIds));
        }

        return entries;
    }

    // Every chunk of a chunked message is an entry that the broker must redeliver, so the id of a chunked message is replaced by the ids of its chunks.
    // Upstream: UnAckedMessageTracker#addChunkedMessageIdsAndRemoveFromSequenceMap
    private static List<MessageId> ExpandChunks(List<MessageId> entries)
    {
        if (!entries.Exists(entry => ChunkMessageIdExpander.IsChunked(entry.ChunkMessageIds)))
            return entries;

        var expanded = new List<MessageId>(entries.Count);
        var added = new HashSet<(ulong LedgerId, ulong EntryId)>();

        foreach (var entry in entries)
        {
            if (!ChunkMessageIdExpander.IsChunked(entry.ChunkMessageIds))
            {
                if (added.Add((entry.LedgerId, entry.EntryId)))
                    expanded.Add(entry);

                continue;
            }

            foreach (var chunkMessageId in entry.ChunkMessageIds!)
            {
                if (added.Add((chunkMessageId.LedgerId, chunkMessageId.EntryId)))
                    expanded.Add(new MessageId(chunkMessageId.LedgerId, chunkMessageId.EntryId, chunkMessageId.Partition, -1, entry.Topic));
            }
        }

        return expanded;
    }

    // The entries that are sent to the dead letter topic are acknowledged, and they are not redelivered. The others are redelivered.
    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages(Set)
    private async ValueTask<List<MessageId>> RemoveDeadLettered(List<MessageId> entries, CancellationToken cancellationToken)
    {
        var tasks = new Task<bool>[entries.Count];

        for (var i = 0; i < tasks.Length; ++i)
        {
            tasks[i] = _deadLetterHandler!.TryDeadLetter(entries[i], Acknowledge, cancellationToken).AsTask();
        }

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        var remaining = new List<MessageId>(entries.Count);

        for (var i = 0; i < results.Length; ++i)
        {
            if (!results[i])
                remaining.Add(entries[i]);
        }

        return remaining;
    }

    private static void ResetPooledAck(CommandAck command)
    {
        if (command.HasRequestId)
            command.ClearRequestId();

        if (command.HasTxnidMostBits)
            command.ClearTxnidMostBits();

        if (command.HasTxnidLeastBits)
            command.ClearTxnidLeastBits();

        if (command.HasValidationError)
            command.ClearValidationError();

        if (command.Properties.Count != 0)
            command.Properties.Clear();
    }

    public ValueTask<IEnumerable<MessageId>> GetLastMessageIds(CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();
}
