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

namespace DotPulsar.Internal.Transactions;

using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.PulsarApi;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/TransactionMetaStoreHandler.java (OpBase, OpForTxnIdCallBack and OpForVoidCallBack)
public sealed class TransactionMetaStoreOp
{
    // The retry backoff of an operation starts at the client's initial backoff (100 ms) and ends at a tenth of the client's maximum backoff (60 seconds)
    private static readonly TimeSpan _retryInitialDelay = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan _retryMaxDelay = TimeSpan.FromSeconds(6);

    private readonly object _command;
    private readonly TaskCompletionSource<BaseCommand> _completionSource;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly object _backoffLock;
    private Backoff? _backoff;
    private IConnection? _sentOn;
    private int _isCompleted;
    private int _isRetryScheduled;

    private TransactionMetaStoreOp(object command, BaseCommand.Types.Type responseType, string description)
    {
        _command = command;
        _completionSource = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        _cancellationTokenSource = new CancellationTokenSource();
        _backoffLock = new object();
        _backoff = null;
        _sentOn = null;
        _isCompleted = 0;
        _isRetryScheduled = 0;
        ResponseType = responseType;
        Description = description;
    }

    public long Id { get; set; }

    public long CreatedTimestamp { get; set; }

    public string Description { get; }

    public BaseCommand.Types.Type ResponseType { get; }

    public Task<BaseCommand> Completion => _completionSource.Task;

    // Cancelled when the operation completes, so that a request that has been sent, but is no longer awaited, is released
    public CancellationToken CancellationToken => _cancellationTokenSource.Token;

    public static TransactionMetaStoreOp ForNewTxn(CommandNewTxn command, string description)
        => new(command, BaseCommand.Types.Type.NewTxnResponse, description);

    public static TransactionMetaStoreOp ForAddPartitionToTxn(CommandAddPartitionToTxn command, string description)
        => new(command, BaseCommand.Types.Type.AddPartitionToTxnResponse, description);

    public static TransactionMetaStoreOp ForAddSubscriptionToTxn(CommandAddSubscriptionToTxn command, string description)
        => new(command, BaseCommand.Types.Type.AddSubscriptionToTxnResponse, description);

    public static TransactionMetaStoreOp ForEndTxn(CommandEndTxn command, string description)
        => new(command, BaseCommand.Types.Type.EndTxnResponse, description);

    public Task<BaseCommand> Send(IConnection connection, CancellationToken cancellationToken)
        => _command switch
        {
            CommandNewTxn command => connection.Send(command, cancellationToken),
            CommandAddPartitionToTxn command => connection.Send(command, cancellationToken),
            CommandAddSubscriptionToTxn command => connection.Send(command, cancellationToken),
            CommandEndTxn command => connection.Send(command, cancellationToken),
            _ => throw new InvalidOperationException($"The command '{_command.GetType().Name}' cannot be sent")
        };

    public bool TryGetError(BaseCommand response, out ServerError error, out string message)
    {
        switch (response.Type)
        {
            case BaseCommand.Types.Type.NewTxnResponse:
                error = response.NewTxnResponse.Error;
                message = response.NewTxnResponse.Message;
                return response.NewTxnResponse.HasError;
            case BaseCommand.Types.Type.AddPartitionToTxnResponse:
                error = response.AddPartitionToTxnResponse.Error;
                message = response.AddPartitionToTxnResponse.Message;
                return response.AddPartitionToTxnResponse.HasError;
            case BaseCommand.Types.Type.AddSubscriptionToTxnResponse:
                error = response.AddSubscriptionToTxnResponse.Error;
                message = response.AddSubscriptionToTxnResponse.Message;
                return response.AddSubscriptionToTxnResponse.HasError;
            case BaseCommand.Types.Type.EndTxnResponse:
                error = response.EndTxnResponse.Error;
                message = response.EndTxnResponse.Message;
                return response.EndTxnResponse.HasError;
            default:
                throw new UnexpectedResponseException($"Expected '{ResponseType}' but got '{response.Type}'");
        }
    }

    // A request is sent on a connection once at a time, so the request is not duplicated when the handler looks up the same connection again.
    // An operation that has completed is not sent again. The completion is checked after the connection has been taken, because the response claims the completion
    // before EndSend gives the connection back: a send that takes the connection after that always sees the completion
    public bool TryBeginSend(IConnection connection)
    {
        var current = Volatile.Read(ref _sentOn);

        while (!ReferenceEquals(current, connection))
        {
            var previous = Interlocked.CompareExchange(ref _sentOn, connection, current);
            if (ReferenceEquals(previous, current))
            {
                if (Volatile.Read(ref _isCompleted) == 0)
                    return true;

                EndSend(connection);
                return false;
            }

            current = previous;
        }

        return false;
    }

    public void EndSend(IConnection connection)
        => Interlocked.CompareExchange(ref _sentOn, null, connection);

    public bool TryScheduleRetry()
        => Interlocked.CompareExchange(ref _isRetryScheduled, 1, 0) == 0;

    public void RetryElapsed()
        => Volatile.Write(ref _isRetryScheduled, 0);

    public TimeSpan NextRetryDelay()
    {
        lock (_backoffLock)
        {
            _backoff ??= new Backoff(_retryInitialDelay, _retryMaxDelay);
            return _backoff.Next();
        }
    }

    // Exactly one of the response, the timeout, the disposal and the failure to send completes the operation
    public bool TryClaimCompletion()
        => Interlocked.Exchange(ref _isCompleted, 1) == 0;

    public void SetResult(BaseCommand response)
    {
        _ = _completionSource.TrySetResult(response);
        _cancellationTokenSource.Cancel();
    }

    public void SetException(Exception exception)
    {
        _ = _completionSource.TrySetException(exception);
        _ = _completionSource.Task.Exception; // The caller may have stopped waiting, so the exception is observed here
        _cancellationTokenSource.Cancel();
    }
}
