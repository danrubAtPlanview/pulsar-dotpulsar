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
        DeadLetterHandler<TMessage>? deadLetterHandler)
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
    public async ValueTask Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken)
    {
        var transactionImpl = Transaction.From(transaction);
        transactionImpl.EnsureOpen();

        var command = CreateTransactionalAck(CommandAck.Types.AckType.Individual, transactionImpl);
        command.MessageId.Add(ToTransactionalMessageIdData(messageId));
        await InternalTransactionalAcknowledge(command, transactionImpl, cancellationToken).ConfigureAwait(false);
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
            command.MessageId.Add(ToTransactionalMessageIdData(messageId));
        }

        if (command.MessageId.Count == 0)
            return;

        await InternalTransactionalAcknowledge(command, transactionImpl, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: ConsumerBase#acknowledgeCumulativeAsync(MessageId, Transaction)
    public async ValueTask AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken)
    {
        if (SubscriptionType is SubscriptionType.Shared or SubscriptionType.KeyShared)
            throw new ConfigurationException("Cannot use cumulative acks on a non-exclusive/non-failover subscription");

        var transactionImpl = Transaction.From(transaction);

        var command = CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, transactionImpl);
        command.MessageId.Add(ToTransactionalMessageIdData(messageId));
        await InternalTransactionalAcknowledge(command, transactionImpl, cancellationToken).ConfigureAwait(false);
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

    public async ValueTask Unsubscribe(CancellationToken cancellationToken)
    {
        var unsubscribe = new CommandUnsubscribe();
        await _executor.Execute(() => InternalUnsubscribe(unsubscribe, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask Seek(MessageId messageId, CancellationToken cancellationToken)
    {
        var seek = new CommandSeek { MessageId = messageId.ToMessageIdData() };
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
        await DisposeChannel().ConfigureAwait(false);
    }

    private async ValueTask<BaseCommand?> InternalAcknowledge(CommandAck command, CancellationToken cancellationToken)
    {
        Guard();
        return await _channel.Send(command, cancellationToken).ConfigureAwait(false);
    }

    private async Task<BaseCommand?> InternalSendTransactional(CommandAck command, CancellationToken cancellationToken)
    {
        Guard();
        return await _channel.SendTransactional(command, cancellationToken).ConfigureAwait(false);
    }

    // A transactional acknowledgment always waits for the receipt of the broker
    private async ValueTask<BaseCommand?> SendAndAwaitReceipt(CommandAck command, bool transactional, CancellationToken cancellationToken)
    {
        if (!(transactional || _ackReceiptEnabled) || _operationTimeout <= TimeSpan.Zero)
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

    private ValueTask<BaseCommand?> Send(CommandAck command, bool transactional, CancellationToken cancellationToken)
        => transactional ? SendTransactional(command, cancellationToken) : _executor.Execute(() => InternalAcknowledge(command, cancellationToken), cancellationToken);

    private async ValueTask<BaseCommand?> SendTransactional(CommandAck command, CancellationToken cancellationToken)
    {
        // The channel rejects an acknowledgment when the broker does not support acknowledgment receipts. That must fail only this
        // acknowledgment, but an exception that is not retried would fault the consumer when it is thrown inside the executor.
        NotSupportedException? notSupported = null;

        async Task<BaseCommand?> SendOnce()
        {
            try
            {
                return await InternalSendTransactional(command, cancellationToken).ConfigureAwait(false);
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
        if (commandAck.MessageId.Count == 0)
            commandAck.MessageId.Add(messageId.ToMessageIdData());
        else
            commandAck.MessageId[0].MapFrom(messageId);

        try
        {
            var response = await SendAndAwaitReceipt(commandAck, false, cancellationToken).ConfigureAwait(false);

            // The response is evaluated outside the executor, so that a server error fails only this call and does not fault the consumer
            response?.ThrowIfAckError();
        }
        finally
        {
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
            commandAck.MessageId.Add(messageId.ToMessageIdData());
        }

        try
        {
            var response = await SendAndAwaitReceipt(commandAck, false, cancellationToken).ConfigureAwait(false);

            // The response is evaluated outside the executor, so that a server error fails only this call and does not fault the consumer
            response?.ThrowIfAckError();
        }
        finally
        {
            commandAck.MessageId.Clear();
            _commandAckPool.Return(commandAck);
        }
    }

    // Upstream: ConsumerBase#doAcknowledgeWithTxn
    private async ValueTask InternalTransactionalAcknowledge(CommandAck command, Transaction transaction, CancellationToken cancellationToken)
    {
        var ackTask = TransactionalAcknowledge(command, transaction, cancellationToken);

        // The operation is registered before it completes, so a commit or an abort that follows right after waits for it
        transaction.RegisterAckOp(ackTask);

        await ackTask.ConfigureAwait(false);
    }

    private async Task TransactionalAcknowledge(CommandAck command, Transaction transaction, CancellationToken cancellationToken)
    {
        await transaction.RegisterAckedTopic(Topic, SubscriptionName, cancellationToken).ConfigureAwait(false);

        RemoveFromUnAckedTracker(command);

        var response = await SendAndAwaitReceipt(command, true, cancellationToken).ConfigureAwait(false);

        // The response is evaluated outside the executor (see InternalAcknowledge), so that a conflict fails only this acknowledgment
        response?.ThrowIfAckError();
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse
    // The command is not pooled, because it carries the transaction and is awaited by the transaction. The request id is assigned when it is sent.
    private static CommandAck CreateTransactionalAck(CommandAck.Types.AckType ackType, Transaction transaction)
        => new()
        {
            AckType = ackType,
            TxnidMostBits = transaction.Id.MostSigBits,
            TxnidLeastBits = transaction.Id.LeastSigBits
        };

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
    // Upstream: ConsumerImpl#getRedeliveryMessageIdData
    private static List<MessageId> ToEntryLevel(IEnumerable<MessageId> messageIds)
    {
        var entries = new List<MessageId>();
        var added = new HashSet<(ulong LedgerId, ulong EntryId)>();

        foreach (var messageId in messageIds)
        {
            if (added.Add((messageId.LedgerId, messageId.EntryId)))
                entries.Add(new MessageId(messageId.LedgerId, messageId.EntryId, messageId.Partition, -1, messageId.Topic));
        }

        return entries;
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
        command.ClearRequestId();
        command.ClearTxnidMostBits();
        command.ClearTxnidLeastBits();
        command.ClearValidationError();
        command.Properties.Clear();
    }

    public ValueTask<IEnumerable<MessageId>> GetLastMessageIds(CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();
}
