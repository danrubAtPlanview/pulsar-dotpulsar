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
        string topic)
    {
        _sendQueue = new AsyncQueueWithCursor<SendOp>(maxPendingMessages);
        _correlationId = correlationId;
        _eventRegister = registerEvent;
        _channel = initialChannel;
        _executor = executor;
        _state = state;
        _factory = factory;
        _partition = partition;
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

    internal async ValueTask WaitForSendQueueEmpty(CancellationToken cancellationToken)
    {
        Guard();
        await _sendQueue.WaitForEmpty(cancellationToken).ConfigureAwait(false);
    }

    private async Task MessageDispatcher(IProducerChannel channel, CancellationToken cancellationToken)
    {
        using var responseQueue = new AsyncQueue<Task<BaseCommand>>();
        var responseProcessorTask = Task.Run(async () => await ResponseProcessor(responseQueue, cancellationToken), CancellationToken.None);

        _sendQueue.ResetCursor();

        while (!cancellationToken.IsCancellationRequested)
        {
            var sendOp = await _sendQueue.NextItem(cancellationToken).ConfigureAwait(false);

            if (sendOp.CancellationToken.IsCancellationRequested)
            {
                _sendQueue.RemoveCurrentItem();
                continue;
            }

            // The message is not written before its topic is registered with its transaction. The message is the next one to be written, so the messages behind it wait too, and the order is kept.
            if (sendOp.RegistrationGate is not null && !await TryAwaitRegistrationGate(sendOp, cancellationToken).ConfigureAwait(false))
                continue;

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
            var success = await _executor.TryExecuteOnce(() => channel.Send(sendOp.Metadata, sendOp.Data, tcs, cancellationToken), CancellationToken.None).ConfigureAwait(false);

            if (!success)
            {
                if (!cancellationToken.IsCancellationRequested)
                    _eventRegister.Register(new ChannelDisconnected(_correlationId));
                break;
            }
        }

        await responseProcessorTask.ConfigureAwait(false);
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

        var expectedSequenceId = sendOp.Metadata.SequenceId;

        if (receiptSequenceId != expectedSequenceId)
            throw new ProducerSendReceiptOrderingException($"Received sequenceId {receiptSequenceId}. Expected {expectedSequenceId}");

        _sendQueue.Dequeue();

        var srMsgId = sendReceipt.MessageId;
        var messageId = new MessageId(srMsgId.LedgerId, srMsgId.EntryId, _partition, srMsgId.BatchIndex, Topic);
        sendOp.ReceiptTcs.TrySetResult(messageId);
    }

    // Upstream: ProducerImpl#recoverNotAllowedError
    private void ProcessNotAllowedSendError(CommandSendError sendError)
    {
        var sequenceId = sendError.SequenceId;

        if (!_sendQueue.TryPeek(out var sendOp) || sendOp is null)
            throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId} but send queue is empty");

        var expectedSequenceId = sendOp.Metadata.SequenceId;

        if (sequenceId != expectedSequenceId)
            throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId}. Expected {expectedSequenceId}");

        _sendQueue.Dequeue();
        _ = sendOp.ReceiptTcs.TrySetException(new NotAllowedException(sendError.Message));
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
