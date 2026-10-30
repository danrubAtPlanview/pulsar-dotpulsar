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
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using System.Diagnostics;

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
    private readonly bool _enableChunking;
    private readonly int _chunkMaxMessageSize;
    private readonly BatcherType _batcherType;
    private readonly IBatchMessageContainer<SendOp>? _container;
    private readonly List<PreparedBatchGroup> _groups;
    private static readonly TimeSpan _maxDelay = TimeSpan.FromMilliseconds(int.MaxValue);

    private readonly TimeSpan _sendTimeout;
    private readonly bool _sendTimeoutEnabled;
    private readonly long _batchingDelayTicks;
    private readonly object _queueSync;
    private readonly List<SendOp> _dequeued;
    private readonly CancellationTokenSource? _sendTimeoutCts;
    private readonly Task? _sendTimeoutTask;
    private SendOp? _sendingHead;
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
        ulong initialSequenceId,
        TimeSpan sendTimeout,
        bool enableChunking = false,
        int chunkMaxMessageSize = 0)
    {
        _sendQueue = new AsyncQueueWithCursor<SendOp>(maxPendingMessages);
        _correlationId = correlationId;
        _eventRegister = registerEvent;
        _channel = initialChannel;
        _executor = executor;
        _state = state;
        _factory = factory;
        _partition = partition;
        _enableChunking = enableChunking;
        _chunkMaxMessageSize = chunkMaxMessageSize;
        _batcherType = batcherType;
        _container = enableBatching ? BatchMessageContainerFactory.Create<SendOp>(batcherType, batchingMaxMessages, batchingMaxBytes, batchingMaxPublishDelay) : null;
        _groups = [];
        _lastSequenceIdPushed = unchecked((long) initialSequenceId - 1);
        _flushRequests = 0;
        _flushesHandled = 0;
        _sendTimeout = sendTimeout;
        _sendTimeoutEnabled = sendTimeout > TimeSpan.Zero;
        _batchingDelayTicks = enableBatching ? (long) (batchingMaxPublishDelay.TotalSeconds * Stopwatch.Frequency) : 0;
        _queueSync = new object();
        _dequeued = [];
        Topic = topic;
        _isDisposed = 0;

        _eventRegister.Register(new ProducerCreated(_correlationId));

        if (!_sendTimeoutEnabled)
            return;

        _sendTimeoutCts = new CancellationTokenSource();
        var sendTimeoutToken = _sendTimeoutCts.Token;
        _sendTimeoutTask = Task.Run(() => SendTimeoutLoop(sendTimeoutToken), CancellationToken.None);
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
            _sendTimeoutCts?.Cancel();
            await (_sendTimeoutTask ?? Task.CompletedTask).ConfigureAwait(false);
            _sendTimeoutCts?.Dispose();
        }
        catch
        {
            // Ignored
        }

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

        // The time that the message waited to be put in the queue does not count, because a message that waits for room in the queue is not waiting to be sent
        if (!_sendTimeoutEnabled)
            return;

        using (EnterQueueSync())
        {
            sendOp.CreatedTimestamp = Stopwatch.GetTimestamp();
        }
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

        using (EnterQueueSync())
        {
            _sendQueue.ResetCursor();

            // The messages of a batch that was not finished by the previous dispatcher are still in the queue, and they are put in batches again
            _container?.Clear();
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var sendOp = await _sendQueue.NextItem(cancellationToken).ConfigureAwait(false);

            using (EnterQueueSync())
            {
                // The send timeout has removed it, and has reset the cursor, so the next item is the first one in the queue
                if (sendOp.IsRemoved)
                    continue;

                // A message that is part of a group is sent, even if the caller has ended it since
                if (sendOp.BatchCount == 0 && sendOp.ReceiptTcs.Task.IsCompleted)
                {
                    _sendQueue.RemoveCurrentItem();
                    continue;
                }
            }

            if (sendOp.BatchCount == 0)
            {
                // The message is not written before its topic is registered with its transaction. The message is the next one to be written, so the messages behind it wait too, and the order is kept.
                if (sendOp.RegistrationGate is not null && !await TryAwaitRegistrationGate(sendOp, cancellationToken).ConfigureAwait(false))
                    continue;

                if (_container is not null)
                {
                    // The group is frozen, and the cursor is put back in front of it, so that the group is sent in the same way as after a reconnect
                    await CreateGroups(sendOp, channel.MaxMessageSize, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // The message is a group of one when it is planned, so it is planned once, and it is split in the same chunks after a reconnect
                if (_enableChunking && !TryPlanChunking(channel, sendOp))
                    continue;
            }

            PreparedBatch? batch;

            using (EnterQueueSync())
            {
                if (sendOp.IsRemoved)
                    continue;

                for (var i = 1; i < sendOp.BatchCount; ++i)
                {
                    _ = _sendQueue.MoveNext();
                }

                batch = sendOp.Batch;
                _sendingHead = sendOp;
            }

            bool success;

            try
            {
                // Use CancellationToken.None here because otherwise it will throw exceptions on all fault actions even retry.
                success = await _executor.TryExecuteOnce(() => SendGroup(channel, sendOp, responseQueue, cancellationToken), CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                using (EnterQueueSync())
                {
                    _sendingHead = null;

                    // The send timeout has removed the messages while the batch was written, and it has left the batch to this method
                    if (batch is not null && sendOp.IsRemoved)
                        batch.Return();
                }
            }

            if (!success)
            {
                if (!cancellationToken.IsCancellationRequested)
                    _eventRegister.Register(new ChannelDisconnected(_correlationId));
                break;
            }
        }

        await responseProcessorTask.ConfigureAwait(false);
    }

    // The responses of the broker are put in the response queue, in the order they are completed
    private static TaskCompletionSource<BaseCommand> CreateResponse(AsyncQueue<Task<BaseCommand>> responseQueue)
    {
        var tcs = new TaskCompletionSource<BaseCommand>();
        _ = tcs.Task.ContinueWith(static (task, state) =>
        {
            try
            {
                ((AsyncQueue<Task<BaseCommand>>) state!).Enqueue(task);
            }
            catch
            {
                // Ignore
            }
        }, responseQueue, CancellationToken.None, TaskContinuationOptions.NotOnCanceled | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return tcs;
    }

    private Task SendGroup(IProducerChannel channel, SendOp head, AsyncQueue<Task<BaseCommand>> responseQueue, CancellationToken cancellationToken)
    {
        if (head.Chunking is { } state)
            return SendChunks(channel, head, state, responseQueue, cancellationToken);

        var tcs = CreateResponse(responseQueue);
        var send = head.BatchCount > 1 ? channel.Send(head.Batch!.Metadata, head.Batch.Payload, tcs, cancellationToken) : channel.Send(head.Metadata, head.Data, tcs, cancellationToken);

        // A send that is completed at once is returned as it is, so nothing is allocated for it
        return send.Status == TaskStatus.RanToCompletion ? send : AwaitSend(send, head);
    }

    private async Task AwaitSend(Task send, SendOp head)
    {
        try
        {
            await send.ConfigureAwait(false);
        }
        catch (TooLargeMessageException exception)
        {
            // Upstream: ProducerImpl#processOpSendMsg
            // The message or the batch is not written, so no receipt will come. The messages of the group fail, and the producer is not faulted.
            FailGroupAtCursor(head, exception);
        }
    }

    // Upstream: ProducerImpl#sendAsync
    // Splits the message in chunks, which are sent when the message is written, and makes the message a group of one. The cursor is at the message.
    // Returns false if the message is not written, because it is removed from the queue, and it must not be written.
    private bool TryPlanChunking(IProducerChannel channel, SendOp sendOp)
    {
        try
        {
            var state = channel.PlanChunking(sendOp.Metadata, sendOp.Data, _chunkMaxMessageSize);

            // The message has chunk fields already, so it is sent as it is
            if (state is null)
                return true;

            using (EnterQueueSync())
            {
                if (sendOp.IsRemoved)
                    return false;

                sendOp.MakeChunked(state);
            }

            return true;
        }
        catch (Exception exception)
        {
            // Upstream: ProducerImpl#sendAsync (InvalidMessageException)
            // The message is not written, so no receipt will come. Only this message fails, and the messages behind it are written.
            RemoveCurrentItemUnlessRemoved(sendOp);
            _ = sendOp.ReceiptTcs.TrySetException(exception);
            return false;
        }
    }

    // Writes the chunks that the broker has not acknowledged, which are all of them unless the message is written again after a reconnect. The broker answers every chunk with a receipt of its own.
    private async Task SendChunks(IProducerChannel channel, SendOp head, ChunkedSendState state, AsyncQueue<Task<BaseCommand>> responseQueue, CancellationToken cancellationToken)
    {
        for (var chunkId = state.ReceiptedChunks; chunkId < state.TotalChunks; ++chunkId)
        {
            // The send timeout has failed the message, so the rest of it is not written
            if (head.IsRemoved)
                return;

            await channel.SendChunk(state, chunkId, CreateResponse(responseQueue), cancellationToken).ConfigureAwait(false);
        }
    }

    // The cursor is at the last message of the group. A message that is not batched is not in a group, and is a group of one.
    private void FailGroupAtCursor(SendOp head, Exception exception)
    {
        var failed = new List<SendOp>();

        using (EnterQueueSync())
        {
            // The send timeout has failed them
            if (head.IsRemoved)
                return;

            var batch = head.Batch;
            var count = Math.Max(head.BatchCount, 1);

            for (var i = 0; i < count; ++i)
            {
                var sendOp = _sendQueue.RemoveCurrentItem();
                sendOp.LeaveGroup();
                failed.Add(sendOp);
            }

            batch?.Return();
        }

        foreach (var sendOp in failed)
        {
            _ = sendOp.ReceiptTcs.TrySetException(exception);
        }
    }

    // Upstream: ProducerImpl#serializeAndSendMessage, #canAddToBatch, #doBatchSendAndAdd, #triggerSendIfFullOrScheduleFlush, #batchMessageAndSend
    // Takes the messages that follow the first message, and that can be sent with it, from the queue and puts them in the container. The groups of messages that are sent as one entry each are
    // frozen on the messages, and the cursor is put back in front of the first message. The queue is not changed otherwise, so a group is sent in the same way again after a reconnect.
    private async ValueTask CreateGroups(SendOp first, int maxMessageSize, CancellationToken cancellationToken)
    {
        var container = _container!;
        bool isFull;

        using (EnterQueueSync())
        {
            if (first.IsRemoved)
                return;

            if (!CanAddToBatch(first))
            {
                first.MakeGroupOfOne();
                _lastSequenceIdPushed = Math.Max(_lastSequenceIdPushed, unchecked((long) first.Metadata.SequenceId));
                _sendQueue.MoveCursorBack(1);
                return;
            }

            // A message with a sequence id that is not higher than the one that was sent last might be a duplicate. It is not batched with other messages, so it is not sent together with a message that is not a duplicate.
            var isPotentialDuplicate = unchecked((long) first.Metadata.SequenceId) <= _lastSequenceIdPushed;
            isFull = container.Add(first, maxMessageSize) || isPotentialDuplicate;
        }

        while (!isFull)
        {
            SendOp? next;
            TimeSpan remaining;
            BatchStep step;

            using (EnterQueueSync())
            {
                // The send timeout has removed the messages of the batch, so they are not sent
                if (first.IsRemoved)
                {
                    container.Clear();
                    return;
                }

                step = TakeNext(container, maxMessageSize, out next, out remaining);
            }

            if (step == BatchStep.Close)
                break;

            if (step == BatchStep.WaitForNextItem)
                _ = await _sendQueue.WaitForNextItem(remaining, cancellationToken).ConfigureAwait(false);

            if (step == BatchStep.WaitForRegistration && !await WaitForRegistrationGate(next!, remaining, cancellationToken).ConfigureAwait(false))
                break;

            if (step == BatchStep.AwaitRegistration)
                _ = await TryAwaitRegistrationGate(next!, cancellationToken).ConfigureAwait(false);
        }

        using (EnterQueueSync())
        {
            if (first.IsRemoved)
            {
                container.Clear();
                return;
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

                // A group waits for the broker from when it is made, like the batch operation of the Java client, and not from when its first message was created
                if (_sendTimeoutEnabled)
                    items[group.Start].CreatedTimestamp = Stopwatch.GetTimestamp();

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
    }

    private enum BatchStep
    {
        Continue,
        Close,
        WaitForNextItem,
        WaitForRegistration,
        AwaitRegistration
    }

    // Looks at the message after the cursor. The queue is synchronized when this is called.
    private BatchStep TakeNext(IBatchMessageContainer<SendOp> container, int maxMessageSize, out SendOp? next, out TimeSpan remaining)
    {
        remaining = TimeSpan.Zero;

        // The requests are read before the queue is, so a request that is read is for messages that are in the queue when it is empty. A request that is made later is not handled by this batch.
        var flushRequests = Volatile.Read(ref _flushRequests);

        if (!_sendQueue.TryPeekNextItem(out next) || next is null)
        {
            if (flushRequests != _flushesHandled)
            {
                _flushesHandled = flushRequests;
                return BatchStep.Close;
            }

            remaining = container.GetRemainingPublishDelay();
            return remaining == TimeSpan.Zero ? BatchStep.Close : BatchStep.WaitForNextItem;
        }

        // The message has been ended by the caller or by the send timeout
        if (next.ReceiptTcs.Task.IsCompleted)
        {
            _ = _sendQueue.MoveNext();
            _sendQueue.RemoveCurrentItem();
            return BatchStep.Continue;
        }

        var registrationGate = next.RegistrationGate;

        if (registrationGate is not null)
        {
            // Only a message that is registered is added to the batch, but the batch does not wait longer than it may wait for more messages
            if (!registrationGate.IsCompleted)
            {
                remaining = container.GetRemainingPublishDelay();
                return BatchStep.WaitForRegistration;
            }

            if (registrationGate.Status != TaskStatus.RanToCompletion)
            {
                _ = _sendQueue.MoveNext();
                return BatchStep.AwaitRegistration;
            }
        }

        if (!CanAddToBatch(next))
            return BatchStep.Close;

        var sequenceId = next.Metadata.SequenceId;

        if (unchecked((long) sequenceId) <= _lastSequenceIdPushed || sequenceId <= container.HighestSequenceId)
            return BatchStep.Close;

        if (!container.CanAddToCurrentBatch(next, maxMessageSize))
            return BatchStep.Close;

        _ = _sendQueue.MoveNext();

        return container.Add(next, maxMessageSize) ? BatchStep.Close : BatchStep.Continue;
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
                    RemoveCurrentItemUnlessRemoved(sendOp);
                    return false;
                }
            }

            await registrationGate.ConfigureAwait(false); // Throws the exception of a failed registration
            return !sendOp.IsRemoved;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The dispatcher is stopped. The message stays in the queue, so the next dispatcher will wait for the registration again.
            throw;
        }
        catch (Exception exception)
        {
            // The message was never written, so no receipt will come. Only this message fails, and the messages behind it are written.
            RemoveCurrentItemUnlessRemoved(sendOp);
            _ = sendOp.ReceiptTcs.TrySetException(exception);
            return false;
        }
    }

    // The cursor is at the message, unless the send timeout has removed it
    private void RemoveCurrentItemUnlessRemoved(SendOp sendOp)
    {
        using (EnterQueueSync())
        {
            if (!sendOp.IsRemoved)
                _ = _sendQueue.RemoveCurrentItem();
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
        SendOp? single = null;
        PreparedBatch? batch;
        ChunkedSendState? chunked = null;
        int count;

        using (EnterQueueSync())
        {
            // Upstream: ProducerImpl#ackReceived
            // The messages have been removed by the send timeout (or never were there) if the queue is empty or if the receipt is for an earlier message than the first one of the queue
            if (!_sendQueue.TryPeek(out var sendOp) || sendOp is null)
                return;

            var expectedSequenceId = GetSequenceId(sendOp);

            if (receiptSequenceId < expectedSequenceId)
                return;

            if (receiptSequenceId > expectedSequenceId)
                throw new ProducerSendReceiptOrderingException($"Received sequenceId {receiptSequenceId}. Expected {expectedSequenceId}");

            if (sendOp.BatchCount > 1 && sendReceipt.HasHighestSequenceId && sendReceipt.HighestSequenceId != sendOp.Batch!.HighestSequenceId)
                throw new ProducerSendReceiptOrderingException($"Received highestSequenceId {sendReceipt.HighestSequenceId} for sequenceId {receiptSequenceId}. Expected {sendOp.Batch.HighestSequenceId}");

            batch = sendOp.Batch;
            count = Math.Max(sendOp.BatchCount, 1);

            // Upstream: ProducerImpl#ackReceived
            // The broker answers every chunk of a message with the sequence id of the message. The message stays at the head of the queue until the last chunk is answered, so the answers of the other chunks match it.
            if (sendOp.Chunking is { IsChunked: true } state)
            {
                sendReceipt.MessageId.Partition = _partition;

                if (!state.RecordReceipt(sendReceipt.MessageId))
                    return;

                chunked = state;
            }

            Dequeue(sendOp, count, out single);
        }

        // The messages are completed when the queue is not synchronized, because the continuations of them might run here
        if (chunked is not null)
            _ = single!.ReceiptTcs.TrySetResult(CreateChunkedMessageId(chunked));
        else if (single is not null)
            _ = single.ReceiptTcs.TrySetResult(BatchMessageIds.Create(sendReceipt.MessageId, _partition, Topic, 1, 0));
        else
        {
            for (var i = 0; i < count; ++i)
            {
                _ = _dequeued[i].ReceiptTcs.TrySetResult(BatchMessageIds.Create(sendReceipt.MessageId, _partition, Topic, count, i));
            }

            _dequeued.Clear();
        }

        batch?.Return();
    }

    // Upstream: ChunkMessageIdImpl
    // The id of a chunked message is the id of its last chunk, and it knows the ids of all the chunks
    private MessageId CreateChunkedMessageId(ChunkedSendState state)
    {
        var first = state.FirstChunkMessageId;
        var last = state.LastChunkMessageId;
        var firstChunkMessageId = new MessageId(first.LedgerId, first.EntryId, _partition, -1, Topic);
        return new MessageId(last.LedgerId, last.EntryId, _partition, -1, Topic, firstChunkMessageId, state.ChunkMessageIds);
    }

    // Upstream: ProducerImpl#recoverNotAllowedError
    private void ProcessNotAllowedSendError(CommandSendError sendError)
    {
        var sequenceId = sendError.SequenceId;
        SendOp? single = null;
        PreparedBatch? batch;
        int count;

        using (EnterQueueSync())
        {
            if (!_sendQueue.TryPeek(out var sendOp) || sendOp is null)
                return;

            // The broker answers with the highest sequence id of a batch
            var expectedSequenceId = GetSequenceId(sendOp);
            var expectedHighestSequenceId = sendOp.BatchCount > 1 ? sendOp.Batch!.HighestSequenceId : expectedSequenceId;

            if (sequenceId != expectedSequenceId && sequenceId != expectedHighestSequenceId)
            {
                // The messages have been removed by the send timeout if the error is for an earlier message than the first one of the queue
                if (sequenceId < expectedSequenceId)
                    return;

                throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId}. Expected {expectedSequenceId}");
            }

            batch = sendOp.Batch;
            count = Math.Max(sendOp.BatchCount, 1);
            Dequeue(sendOp, count, out single);
        }

        var exception = new NotAllowedException(sendError.Message);

        if (single is not null)
            _ = single.ReceiptTcs.TrySetException(exception);
        else
        {
            for (var i = 0; i < count; ++i)
            {
                _ = _dequeued[i].ReceiptTcs.TrySetException(exception);
            }

            _dequeued.Clear();
        }

        batch?.Return();
    }

    // Removes the messages of the group that starts with the first message of the queue. One message is returned as 'single' and several are put in '_dequeued', so that nothing is allocated for them.
    private void Dequeue(SendOp first, int count, out SendOp? single)
    {
        single = null;

        if (count == 1)
        {
            _sendQueue.Dequeue();
            first.LeaveGroup();
            single = first;
            return;
        }

        _dequeued.Clear();

        for (var i = 0; i < count; ++i)
        {
            _ = _sendQueue.TryPeek(out var sendOp);
            _sendQueue.Dequeue();
            sendOp!.LeaveGroup();
            _dequeued.Add(sendOp);
        }
    }

    // The sequence id of the entry that the op is the first of, which is the sequence id that the broker answers with in a receipt
    private static ulong GetSequenceId(SendOp sendOp)
        => sendOp.BatchCount > 1 ? sendOp.Batch!.SequenceId : sendOp.Metadata.SequenceId;

    private async Task SendTimeoutLoop(CancellationToken cancellationToken)
    {
        var next = _sendTimeout;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(next > _maxDelay ? _maxDelay : next, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            List<SendOp>? failed;

            try
            {
                failed = RemoveTimedOut(ref next);
            }
            catch (AsyncQueueWithCursorDisposedException)
            {
                return;
            }

            if (failed is not null)
                FailTimedOut(failed);
        }
    }

    // Removes every message from the queue if the oldest message has waited for the send timeout. 'next' is set to the time until the next check.
    private List<SendOp>? RemoveTimedOut(ref TimeSpan next)
    {
        using (EnterQueueSync())
        {
            // Upstream: ProducerImpl#run(Timeout)
            // The oldest message decides, because the messages that were sent after it have not been waiting for longer. A message that is not stamped yet was put in the queue just now.
            if (!_sendQueue.TryPeek(out var oldest) || oldest is null || oldest.CreatedTimestamp == 0)
            {
                next = _sendTimeout;
                return null;
            }

            // A message that is not in a group waits in a batch, so it is counted from when the batch would have been sent, like the Java client does
            var created = oldest.BatchCount == 0 ? oldest.CreatedTimestamp + _batchingDelayTicks : oldest.CreatedTimestamp;
            var remaining = _sendTimeout - GetElapsed(created);

            if (remaining > TimeSpan.Zero)
            {
                next = remaining;
                return null;
            }

            next = _sendTimeout;
            var failed = _sendQueue.RemoveAll();

            foreach (var sendOp in failed)
            {
                var batch = sendOp.Batch;
                sendOp.MakeRemoved();

                // A batch that is being written is returned by the dispatcher when it is done with it
                if (batch is not null && !ReferenceEquals(sendOp, _sendingHead))
                    batch.Return();
            }

            return failed;
        }
    }

    // The messages are failed when the queue is not synchronized, because the continuations of them might run here
    private void FailTimedOut(List<SendOp> failed)
    {
        foreach (var sendOp in failed)
        {
            var exception = new OperationTimeoutException($"The producer can not send the message with the sequence id {sendOp.Metadata.SequenceId} to the topic '{Topic}' within the send timeout of {_sendTimeout}");
            _ = sendOp.ReceiptTcs.TrySetException(exception);
        }
    }

    private static TimeSpan GetElapsed(long timestamp)
        => TimeSpan.FromTicks((long) ((Stopwatch.GetTimestamp() - timestamp) * (TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency)));

    // The queue is only synchronized when there is a send timeout, because that is what removes messages from it besides the dispatcher and the receipts
    private QueueSyncScope EnterQueueSync()
        => new(_sendTimeoutEnabled ? _queueSync : null);

    private readonly struct QueueSyncScope : IDisposable
    {
        private readonly object? _sync;

        public QueueSyncScope(object? sync)
        {
            _sync = sync;

            if (sync is not null)
                Monitor.Enter(sync);
        }

        public void Dispose()
        {
            if (_sync is not null)
                Monitor.Exit(_sync);
        }
    }

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
