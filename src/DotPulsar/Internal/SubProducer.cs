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
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;

public sealed class SubProducer : IContainsChannel, IStateHolder<ProducerState>
{
    private readonly AsyncQueueWithCursor<SendOp> _sendQueue;
    private CancellationTokenSource? _dispatcherCts;
    private Task? _dispatcherTask;
    private readonly Guid _correlationId;
    private readonly IRegisterEvent _eventRegister;
    private IProducerChannel _channel;
    private readonly IExecute _executor;
    private readonly IState<ProducerState> _state;
    private readonly IProducerChannelFactory _factory;
    private readonly int _partition;
    private readonly BatcherType _batcherType;
    private readonly IBatchMessageContainer<SendOp>? _container;
    private readonly List<PreparedBatchGroup> _groups;
    private long _lastSequenceIdPushed;
    private int _flushRequests;
    private int _flushesHandled;
    private int _isDisposed;
    private Exception? _faultException;

    public string Topic { get; }
    public IState<ProducerState> State => _state;

    public SubProducer(
        Guid correlationId,
        IRegisterEvent registerEvent,
        IProducerChannel initialChannel,
        IExecute executor,
        IState<ProducerState> state,
        IProducerChannelFactory factory,
        int partition,
        uint maxPendingMessages,
        string topic,
        bool enableBatching,
        BatcherType batcherType,
        uint batchingMaxMessages,
        uint batchingMaxBytes,
        TimeSpan batchingMaxPublishDelay,
        ulong initialSequenceId)
    {
        _sendQueue = new AsyncQueueWithCursor<SendOp>(maxPendingMessages);
        _correlationId = correlationId;
        _eventRegister = registerEvent;
        _channel = initialChannel;
        _executor = executor;
        _state = state;
        _factory = factory;
        _partition = partition;
        _batcherType = batcherType;
        _container = enableBatching ? BatchMessageContainerFactory.Create<SendOp>(batcherType, batchingMaxMessages, batchingMaxBytes, batchingMaxPublishDelay) : null;
        _groups = [];
        _lastSequenceIdPushed = unchecked((long) initialSequenceId - 1);
        _flushRequests = 0;
        _flushesHandled = 0;
        Topic = topic;
        _isDisposed = 0;

        _eventRegister.Register(new ProducerCreated(_correlationId));
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _eventRegister.Register(new ProducerDisposed(_correlationId));
        await InternalDispose().ConfigureAwait(false);
    }

    private async ValueTask InternalDispose()
    {
        try
        {
            _dispatcherCts?.Cancel();
            _dispatcherCts?.Dispose();
            await (_dispatcherTask ?? Task.CompletedTask).ConfigureAwait(false);
        }
        catch
        {
            // Ignored
        }

        await _sendQueue.DisposeAsync().ConfigureAwait(false);
        await _channel.ClosedByClient(CancellationToken.None).ConfigureAwait(false);
        await _channel.DisposeAsync().ConfigureAwait(false);
    }

    public async ValueTask Send(SendOp sendOp, CancellationToken cancellationToken)
    {
        Guard();
        await _sendQueue.Enqueue(sendOp, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask WaitForSendQueueEmpty(CancellationToken cancellationToken)
    {
        Guard();
        RequestFlush();
        await _sendQueue.WaitForEmpty(cancellationToken).ConfigureAwait(false);
    }

    // Sends the messages that are waiting in a batch at once. The returned task is completed when the last message that was sent before the call has been acknowledged by the broker.
    public Task Flush()
    {
        Guard();

        if (!_sendQueue.TryPeekLast(out var last) || last is null)
            return Task.CompletedTask;

        RequestFlush();
        return last.ReceiptTcs.Task;
    }

    // There is nothing to flush if the last message has been put in a batch that is sent, because that does not wait
    private void RequestFlush()
    {
        if (_container is null || !_sendQueue.TryPeekLast(out var last) || last is null || last.BatchCount != 0)
            return;

        _ = Interlocked.Increment(ref _flushRequests);
        _sendQueue.SignalWaiters();
    }

    private async Task MessageDispatcher(IProducerChannel channel, CancellationToken cancellationToken)
    {
        using var responseQueue = new AsyncQueue<Task<BaseCommand>>();
        var responseProcessorTask = Task.Run(async () => await ResponseProcessor(responseQueue, cancellationToken), CancellationToken.None);

        _sendQueue.ResetCursor();

        // The messages of a batch that was not finished by the previous dispatcher are still in the queue, and they are put in batches again
        _container?.Clear();

        while (!cancellationToken.IsCancellationRequested)
        {
            var sendOp = await _sendQueue.NextItem(cancellationToken).ConfigureAwait(false);

            // A message that is part of a group is sent, even if the caller has cancelled it since
            if (sendOp.BatchCount == 0)
            {
                if (sendOp.CancellationToken.IsCancellationRequested)
                {
                    _sendQueue.RemoveCurrentItem();
                    continue;
                }

                // The message is not written before its topic is registered with its transaction. The message is the next one to be written, so the messages behind it wait too, and the order is kept.
                if (sendOp.RegistrationGate is not null && !await TryAwaitRegistrationGate(sendOp, cancellationToken).ConfigureAwait(false))
                    continue;

                if (_container is not null)
                {
                    // The group is frozen, and the cursor is put back in front of it, so that the group is sent in the same way as after a reconnect
                    await CreateGroups(sendOp, channel.MaxMessageSize, cancellationToken).ConfigureAwait(false);
                    continue;
                }
            }

            for (var i = 1; i < sendOp.BatchCount; ++i)
            {
                _ = _sendQueue.MoveNext();
            }

            var tcs = new TaskCompletionSource<BaseCommand>();
            _ = tcs.Task.ContinueWith(task =>
            {
                try
                {
                    responseQueue.Enqueue(task);
                }
                catch
                {
                    // Ignore
                }
            }, TaskContinuationOptions.NotOnCanceled | TaskContinuationOptions.ExecuteSynchronously);

            // Use CancellationToken.None here because otherwise it will throw exceptions on all fault actions even retry.
            var success = await _executor.TryExecuteOnce(() => SendGroup(channel, sendOp, tcs, cancellationToken), CancellationToken.None).ConfigureAwait(false);

            if (!success)
            {
                if (!cancellationToken.IsCancellationRequested)
                    _eventRegister.Register(new ChannelDisconnected(_correlationId));
                break;
            }
        }

        await responseProcessorTask.ConfigureAwait(false);
    }

    private Task SendGroup(IProducerChannel channel, SendOp head, TaskCompletionSource<BaseCommand> tcs, CancellationToken cancellationToken)
        => head.BatchCount > 1 ? SendBatch(channel, head, tcs, cancellationToken) : channel.Send(head.Metadata, head.Data, tcs, cancellationToken);

    private async Task SendBatch(IProducerChannel channel, SendOp head, TaskCompletionSource<BaseCommand> tcs, CancellationToken cancellationToken)
    {
        var batch = head.Batch!;

        try
        {
            await channel.Send(batch.Metadata, batch.Payload, tcs, cancellationToken).ConfigureAwait(false);
        }
        catch (TooLargeMessageException exception)
        {
            // Upstream: ProducerImpl#processOpSendMsg
            // The batch is not written, so no receipt will come. The messages of the batch fail, and the producer is not faulted.
            FailGroupAtCursor(head, exception);
        }
    }

    // The cursor is at the last message of the group
    private void FailGroupAtCursor(SendOp head, Exception exception)
    {
        var batch = head.Batch;
        var count = head.BatchCount;

        for (var i = 0; i < count; ++i)
        {
            var sendOp = _sendQueue.RemoveCurrentItem();
            sendOp.LeaveGroup();
            _ = sendOp.ReceiptTcs.TrySetException(exception);
        }

        batch?.Return();
    }

    // Upstream: ProducerImpl#serializeAndSendMessage, #canAddToBatch, #doBatchSendAndAdd, #triggerSendIfFullOrScheduleFlush, #batchMessageAndSend
    // Takes the messages that follow the first message, and that can be sent with it, from the queue and puts them in the container. The groups of messages that are sent as one entry each are
    // frozen on the messages, and the cursor is put back in front of the first message. The queue is not changed otherwise, so a group is sent in the same way again after a reconnect.
    private async ValueTask CreateGroups(SendOp first, int maxMessageSize, CancellationToken cancellationToken)
    {
        var container = _container!;

        if (!CanAddToBatch(first))
        {
            first.MakeGroupOfOne();
            _lastSequenceIdPushed = Math.Max(_lastSequenceIdPushed, unchecked((long) first.Metadata.SequenceId));
            _sendQueue.MoveCursorBack(1);
            return;
        }

        // A message with a sequence id that is not higher than the one that was sent last might be a duplicate. It is not batched with other messages, so it is not sent together with a message that is not a duplicate.
        var isPotentialDuplicate = unchecked((long) first.Metadata.SequenceId) <= _lastSequenceIdPushed;
        var isFull = container.Add(first, maxMessageSize) || isPotentialDuplicate;

        while (!isFull)
        {
            // The requests are read before the queue is, so a request that is read is for messages that are in the queue when it is empty. A request that is made later is not handled by this batch.
            var flushRequests = Volatile.Read(ref _flushRequests);

            if (!_sendQueue.TryPeekNextItem(out var next) || next is null)
            {
                if (flushRequests != _flushesHandled)
                {
                    _flushesHandled = flushRequests;
                    break;
                }

                var remaining = container.GetRemainingPublishDelay();

                if (remaining == TimeSpan.Zero)
                    break;

                _ = await _sendQueue.WaitForNextItem(remaining, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (next.CancellationToken.IsCancellationRequested)
            {
                _ = _sendQueue.MoveNext();
                _sendQueue.RemoveCurrentItem();
                continue;
            }

            if (next.RegistrationGate is not null)
            {
                // Only a message that is registered is added to the batch, but the batch does not wait longer than it may wait for more messages
                if (!next.RegistrationGate.IsCompleted && !await WaitForRegistrationGate(next, container.GetRemainingPublishDelay(), cancellationToken).ConfigureAwait(false))
                    break;

                if (next.RegistrationGate.Status != TaskStatus.RanToCompletion)
                {
                    _ = _sendQueue.MoveNext();
                    _ = await TryAwaitRegistrationGate(next, cancellationToken).ConfigureAwait(false);
                    continue;
                }
            }

            if (!CanAddToBatch(next))
                break;

            var sequenceId = next.Metadata.SequenceId;

            if (unchecked((long) sequenceId) <= _lastSequenceIdPushed || sequenceId <= container.HighestSequenceId)
                break;

            if (!container.CanAddToCurrentBatch(next, maxMessageSize))
                break;

            _ = _sendQueue.MoveNext();
            isFull = container.Add(next, maxMessageSize);
        }

        _groups.Clear();
        container.CreateBatches(_groups, maxMessageSize);

        var items = container.Items;

        if (_batcherType == BatcherType.KeyBased)
            _sendQueue.RewriteItemsUpToCursor(items);

        for (var i = 0; i < _groups.Count; ++i)
        {
            var group = _groups[i];
            if (group.Batch is null)
                items[group.Start].MakeGroupOfOne();
            else
                items[group.Start].MakeFirstOfGroup(group.Batch);

            for (var j = 1; j < group.Count; ++j)
            {
                items[group.Start + j].MakeMemberOfGroup();
            }

            _lastSequenceIdPushed = Math.Max(_lastSequenceIdPushed, unchecked((long) group.HighestSequenceId));
        }

        // The requests to flush are handled if there is nothing left to flush, so they do not flush the messages that are sent later
        var handledRequests = Volatile.Read(ref _flushRequests);
        if (!_sendQueue.TryPeekNextItem(out _))
            _flushesHandled = handledRequests;

        var numberOfMessages = items.Count;
        container.Clear();
        _sendQueue.MoveCursorBack(numberOfMessages);
    }

    // Upstream: ProducerImpl#canAddToBatch
    private static bool CanAddToBatch(SendOp sendOp)
        => !sendOp.Metadata.HasDeliverAtTime;

    // Returns false if the time ran out before the registration or the message was completed
    private static async ValueTask<bool> WaitForRegistrationGate(SendOp sendOp, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delay = Task.Delay(timeout, cts.Token);
        var completed = await Task.WhenAny(sendOp.RegistrationGate!, sendOp.ReceiptTcs.Task, delay).ConfigureAwait(false);
        cts.Cancel();
        cancellationToken.ThrowIfCancellationRequested();
        return completed != delay;
    }

    // Returns true if the message can be written. Returns false if it was removed from the queue, because the registration failed or the receipt of the message was completed before it was written.
    private async ValueTask<bool> TryAwaitRegistrationGate(SendOp sendOp, CancellationToken cancellationToken)
    {
        var registrationGate = sendOp.RegistrationGate!;

        try
        {
            if (!registrationGate.IsCompleted)
            {
                // The registration is not cancelled, because it is shared with the other messages of the transaction. Only the waiting is, and it ends if the dispatcher is stopped
                // or if the receipt of the message is completed, which is what happens when the caller cancels the send.
                var completed = await Task.WhenAny(registrationGate, sendOp.ReceiptTcs.Task).WaitAsync(cancellationToken).ConfigureAwait(false);

                if (completed != registrationGate)
                {
                    _sendQueue.RemoveCurrentItem();
                    return false;
                }
            }

            await registrationGate.ConfigureAwait(false); // Throws the exception of a failed registration
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The dispatcher is stopped. The message stays in the queue, so the next dispatcher will wait for the registration again.
            throw;
        }
        catch (Exception exception)
        {
            // The message was never written, so no receipt will come. Only this message fails, and the messages behind it are written.
            _sendQueue.RemoveCurrentItem();
            _ = sendOp.ReceiptTcs.TrySetException(exception);
            return false;
        }
    }

    private async ValueTask ResponseProcessor(IDequeue<Task<BaseCommand>> responseQueue, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var responseTask = await responseQueue.Dequeue(cancellationToken).ConfigureAwait(false);

            var success = await _executor.TryExecuteOnce(() =>
            {
                if (responseTask.IsFaulted)
                    throw responseTask.Exception!;

                var response = responseTask.Result;

                // Upstream: ClientCnx#handleSendError, ProducerImpl#recoverNotAllowedError
                // A message that is not allowed fails on its own. The other send errors still fault the producer, because they are thrown by Expect.
                if (response.Type == BaseCommand.Types.Type.SendError && response.SendError.Error == ServerError.NotAllowedError)
                {
                    ProcessNotAllowedSendError(response.SendError);
                    return;
                }

                response.Expect(BaseCommand.Types.Type.SendReceipt);
                ProcessReceipt(response.SendReceipt);
            }, CancellationToken.None).ConfigureAwait(false); // Use CancellationToken.None here because otherwise it will throw exceptions on all fault actions even retry.

            if (success)
                continue;

            _eventRegister.Register(new SendReceiptWrongOrdering(_correlationId));
            break;
        }
    }

    private void ProcessReceipt(CommandSendReceipt sendReceipt)
    {
        var receiptSequenceId = sendReceipt.SequenceId;

        if (!_sendQueue.TryPeek(out var sendOp) || sendOp is null)
            throw new ProducerSendReceiptOrderingException($"Received sequenceId {receiptSequenceId} but send queue is empty");

        var expectedSequenceId = GetSequenceId(sendOp);

        if (receiptSequenceId != expectedSequenceId)
            throw new ProducerSendReceiptOrderingException($"Received sequenceId {receiptSequenceId}. Expected {expectedSequenceId}");

        // Upstream: ProducerImpl#ackReceived
        if (sendOp.BatchCount > 1 && sendReceipt.HasHighestSequenceId && sendReceipt.HighestSequenceId != sendOp.Batch!.HighestSequenceId)
            throw new ProducerSendReceiptOrderingException($"Received highestSequenceId {sendReceipt.HighestSequenceId} for sequenceId {receiptSequenceId}. Expected {sendOp.Batch.HighestSequenceId}");

        var batch = sendOp.Batch;
        var count = Math.Max(sendOp.BatchCount, 1);

        for (var i = 0; i < count; ++i)
        {
            _ = _sendQueue.TryPeek(out var member);
            _sendQueue.Dequeue();
            member!.LeaveGroup();
            _ = member.ReceiptTcs.TrySetResult(BatchMessageIds.Create(sendReceipt.MessageId, _partition, Topic, count, i));
        }

        batch?.Return();
    }

    // Upstream: ProducerImpl#recoverNotAllowedError
    private void ProcessNotAllowedSendError(CommandSendError sendError)
    {
        var sequenceId = sendError.SequenceId;

        if (!_sendQueue.TryPeek(out var sendOp) || sendOp is null)
            throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId} but send queue is empty");

        // The broker answers with the highest sequence id of a batch
        var expectedSequenceId = GetSequenceId(sendOp);
        var expectedHighestSequenceId = sendOp.BatchCount > 1 ? sendOp.Batch!.HighestSequenceId : expectedSequenceId;

        if (sequenceId != expectedSequenceId && sequenceId != expectedHighestSequenceId)
            throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId}. Expected {expectedSequenceId}");

        var exception = new NotAllowedException(sendError.Message);
        var batch = sendOp.Batch;
        var count = Math.Max(sendOp.BatchCount, 1);

        for (var i = 0; i < count; ++i)
        {
            _ = _sendQueue.TryPeek(out var member);
            _sendQueue.Dequeue();
            member!.LeaveGroup();
            _ = member.ReceiptTcs.TrySetException(exception);
        }

        batch?.Return();
    }

    // The sequence id of the entry that the op is the first of, which is the sequence id that the broker answers with in a receipt
    private static ulong GetSequenceId(SendOp sendOp)
        => sendOp.BatchCount > 1 ? sendOp.Batch!.SequenceId : sendOp.Metadata.SequenceId;

    public async Task EstablishNewChannel(CancellationToken cancellationToken)
    {
        try
        {
            if (_dispatcherCts is not null)
            {
                if (!_dispatcherCts.IsCancellationRequested)
                    _dispatcherCts.Cancel();
                _dispatcherCts.Dispose();
            }
        }
        catch (Exception)
        {
            // Ignored
        }

        await _executor.TryExecuteOnce(() => _dispatcherTask ?? Task.CompletedTask, cancellationToken).ConfigureAwait(false);

        try
        {
            var oldChannel = _channel;
            await oldChannel.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Ignored
        }

        _channel = await _executor.Execute(() => _factory.Create(cancellationToken), cancellationToken).ConfigureAwait(false);

        var cts = new CancellationTokenSource();
        var token = cts.Token;
        var channel = _channel;
        _dispatcherCts = cts;
        _dispatcherTask = Task.Run(() => MessageDispatcher(channel, token), CancellationToken.None);
    }

    public async ValueTask CloseChannel(CancellationToken cancellationToken)
        => await _channel.ClosedByClient(cancellationToken).ConfigureAwait(false);

    public async ValueTask ChannelFaulted(Exception exception)
    {
        _faultException = exception;
        await InternalDispose().ConfigureAwait(false);
    }

    private void Guard()
    {
        if (_isDisposed != 0)
            throw new ProducerDisposedException(GetType().FullName!);

        if (_faultException is ProducerFencedException)
            throw _faultException;
        if (_faultException is not null)
            throw new ProducerFaultedException(_faultException);
    }
}
