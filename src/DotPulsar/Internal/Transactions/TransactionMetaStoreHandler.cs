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

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using System.Collections.Concurrent;
using System.Diagnostics;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/TransactionMetaStoreHandler.java, pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionHandler.java
public sealed class TransactionMetaStoreHandler : ITransactionMetaStoreHandler
{
    private const int MaxPendingOperations = 1000;
    private const int TcClientConnectProtocolVersion = 19;
    private const string ClosedMessage = "Could not get response from transaction meta store when the transaction meta store has already close.";
    private const string RejectedMessage = "Could not send the request to the transaction meta store: the client is already closed.";

    private readonly ulong _tcId;
    private readonly string _topic;
    private readonly IConnectionPool _connectionPool;
    private readonly TimeSpan _operationTimeout;
    private readonly long _operationTimeoutTimestamp;
    private readonly long _lookupDeadlineTimestamp;
    private readonly StateManager<TransactionMetaStoreHandlerState> _stateManager;
    private readonly object _stateLock;
    private readonly TaskCompletionSource<object?> _connected;
    private readonly ConcurrentDictionary<long, TransactionMetaStoreOp> _pendingOps;
    private readonly ConcurrentQueue<(long CreatedTimestamp, long OpId)> _timeoutQueue;
    private readonly SemaphoreSlim _permits;
    private readonly Backoff _reconnectBackoff;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Timer _timeoutTimer;
    private ConnectionSession? _session;
    private Task? _connectLoop;
    private long _nextOpId;
    private int _state;
    private int _isDisposed;

    public TransactionMetaStoreHandler(ulong tcId, string topic, IConnectionPool connectionPool, TimeSpan operationTimeout)
    {
        _tcId = tcId;
        _topic = topic;
        _connectionPool = connectionPool;
        _operationTimeout = operationTimeout;
        _operationTimeoutTimestamp = (long) (operationTimeout.TotalSeconds * Stopwatch.Frequency);
        _lookupDeadlineTimestamp = Stopwatch.GetTimestamp() + _operationTimeoutTimestamp;
        _stateManager = new StateManager<TransactionMetaStoreHandlerState>(TransactionMetaStoreHandlerState.Uninitialized, TransactionMetaStoreHandlerState.Closed, TransactionMetaStoreHandlerState.Failed);
        _stateLock = new object();
        _connected = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingOps = new ConcurrentDictionary<long, TransactionMetaStoreOp>();
        _timeoutQueue = new ConcurrentQueue<(long CreatedTimestamp, long OpId)>();
        _permits = new SemaphoreSlim(MaxPendingOperations, MaxPendingOperations);
        _reconnectBackoff = new Backoff(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(100));
        _cancellationTokenSource = new CancellationTokenSource();
        _session = null;
        _connectLoop = null;
        _nextOpId = 0;
        _state = (int) TransactionMetaStoreHandlerState.Uninitialized;
        _isDisposed = 0;
        _timeoutTimer = new Timer(static state => ((TransactionMetaStoreHandler) state!).OnTimeoutTimer(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _ = _timeoutTimer.Change(operationTimeout, Timeout.InfiniteTimeSpan);
    }

    public IState<TransactionMetaStoreHandlerState> State => _stateManager;

    public TransactionMetaStoreHandlerState CurrentState => (TransactionMetaStoreHandlerState) Volatile.Read(ref _state);

    public Task Connected => _connected.Task;

    public int PendingRequestsCount => _pendingOps.Count;

    public int AvailableRequestPermits => _permits.CurrentCount;

    public void Start()
    {
        lock (_stateLock)
        {
            if (_state != (int) TransactionMetaStoreHandlerState.Uninitialized)
                return;

            SetStateCore(TransactionMetaStoreHandlerState.Connecting);
            _connectLoop = Task.Run(() => ConnectLoop(_cancellationTokenSource.Token));
        }
    }

    // Upstream: TransactionMetaStoreHandler#connectionClosed
    public void ConnectionClosed()
    {
        // The notification carries no connection, so one that arrives after the handler has moved to another connection must be ignored
        var session = Volatile.Read(ref _session);
        if (session is not null && session.Connection.State.IsFinalState())
            session.Cancel();
    }

    // Upstream: TransactionMetaStoreHandler#newTransactionAsync
    public async ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var command = new CommandNewTxn { TcId = _tcId, TxnTtlMillis = (ulong) timeout.TotalMilliseconds };
        var op = TransactionMetaStoreOp.ForNewTxn(command, $"Create new transaction {_tcId}");
        var response = await Execute(op, cancellationToken).ConfigureAwait(false);
        return new TransactionId(response.NewTxnResponse.TxnidMostBits, response.NewTxnResponse.TxnidLeastBits);
    }

    // Upstream: TransactionMetaStoreHandler#addPublishPartitionToTxnAsync
    public async ValueTask AddPublishPartitionToTxn(TransactionId transactionId, string partition, CancellationToken cancellationToken)
    {
        var command = new CommandAddPartitionToTxn
        {
            TxnidMostBits = transactionId.MostSigBits,
            TxnidLeastBits = transactionId.LeastSigBits,
            Partitions = { partition }
        };
        var op = TransactionMetaStoreOp.ForAddPartitionToTxn(command, $"Add partition [{partition}] to TXN {transactionId}");
        _ = await Execute(op, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionMetaStoreHandler#addSubscriptionToTxn
    public async ValueTask AddSubscriptionToTxn(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken)
    {
        var command = new CommandAddSubscriptionToTxn
        {
            TxnidMostBits = transactionId.MostSigBits,
            TxnidLeastBits = transactionId.LeastSigBits,
            Subscription = { new Subscription { Topic = topic, Subscription_ = subscription } }
        };
        var op = TransactionMetaStoreOp.ForAddSubscriptionToTxn(command, $"Add subscription [{topic} {subscription}] to TXN {transactionId}");
        _ = await Execute(op, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionMetaStoreHandler#endTxnAsync
    public async ValueTask EndTxn(TransactionId transactionId, TxnAction action, CancellationToken cancellationToken)
    {
        var command = new CommandEndTxn
        {
            TxnidMostBits = transactionId.MostSigBits,
            TxnidLeastBits = transactionId.LeastSigBits,
            TxnAction = action
        };
        var op = TransactionMetaStoreOp.ForEndTxn(command, $"End [{action.ToString().ToUpperInvariant()}] TXN {transactionId}");
        _ = await Execute(op, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionMetaStoreHandler#close
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _timeoutTimer.Dispose();

        Task? connectLoop;

        lock (_stateLock)
        {
            SetStateCore(TransactionMetaStoreHandlerState.Closing);
            connectLoop = _connectLoop;
        }

        _cancellationTokenSource.Cancel();

        if (connectLoop is not null)
        {
            try
            {
                await connectLoop.ConfigureAwait(false);
            }
            catch
            {
                // ignored
            }
        }

        SetState(TransactionMetaStoreHandlerState.Closed);

        var exception = new PulsarClientClosedException(ClosedMessage);
        FailAllPending(exception);
        _ = TryFaultConnected(exception);
        _permits.Dispose();
    }

    // Upstream: TransactionMetaStoreHandler#getExceptionByServerError
    public static Exception GetExceptionByServerError(ServerError serverError, string message)
        => serverError switch
        {
            ServerError.TransactionCoordinatorNotFound => new TransactionCoordinatorNotFoundException(message),
            ServerError.InvalidTxnStatus => new InvalidTransactionStatusException(message),
            ServerError.TransactionNotFound => new TransactionNotFoundException(message),
            _ => new TransactionCoordinatorClientException(message)
        };

    // Upstream: TransactionMetaStoreHandler#canSendRequest, #registerAndSend (C# deviation: the permit is awaited asynchronously instead of blocking the thread)
    private async ValueTask<BaseCommand> Execute(TransactionMetaStoreOp op, CancellationToken cancellationToken)
    {
        await AcquirePermit(cancellationToken).ConfigureAwait(false);

        op.Id = Interlocked.Increment(ref _nextOpId);
        op.CreatedTimestamp = Stopwatch.GetTimestamp();
        _pendingOps[op.Id] = op;
        _timeoutQueue.Enqueue((op.CreatedTimestamp, op.Id));

        CheckStateAndSend(op);

        // The caller stops waiting when cancelled, but the operation stays pending until it is completed by a response, the timeout or the disposal
        return await op.Completion.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask AcquirePermit(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            if (_permits.Wait(0))
                return;

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token);
            await _permits.WaitAsync(cts.Token).ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            throw new PulsarClientClosedException(RejectedMessage);
        }
        catch (OperationCanceledException) when (_cancellationTokenSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new PulsarClientClosedException(RejectedMessage);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_isDisposed != 0)
            throw new PulsarClientClosedException(RejectedMessage);
    }

    // Upstream: TransactionMetaStoreHandler#checkStateAndSendRequest
    private void CheckStateAndSend(TransactionMetaStoreOp op)
    {
        switch (CurrentState)
        {
            case TransactionMetaStoreHandlerState.Ready:
                var session = Volatile.Read(ref _session);
                if (session is not null && op.TryBeginSend(session.Connection))
                    _ = SendOnce(op, session);
                break;
            case TransactionMetaStoreHandlerState.Uninitialized:
            case TransactionMetaStoreHandlerState.Connecting:
                // The operation stays pending and is sent when the handler is connected
                break;
            case TransactionMetaStoreHandlerState.Closing:
            case TransactionMetaStoreHandlerState.Closed:
                Complete(op, new MetaStoreHandlerNotReadyException($"Transaction meta store handler for tcId {_tcId} is closing or closed."));
                break;
            case TransactionMetaStoreHandlerState.Failed:
                Complete(op, new MetaStoreHandlerNotReadyException($"Transaction meta store handler for tcId {_tcId} not connected."));
                break;
        }
    }

    private async Task SendOnce(TransactionMetaStoreOp op, ConnectionSession session)
    {
        BaseCommand response;

        try
        {
            response = await op.Send(session.Connection, op.CancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The connection was lost or the operation has completed. An operation that is still pending is sent again after the reconnect
            op.EndSend(session.Connection);
            return;
        }

        try
        {
            HandleResponse(op, session, response);
        }
        finally
        {
            op.EndSend(session.Connection);
        }
    }

    // Upstream: TransactionMetaStoreHandler#handleNewTxnResponse, #handleAddPublishPartitionToTxnResponse, #handleAddSubscriptionToTxnResponse, #handleEndTxnResponse (C# deviation: the response is correlated by the request id of the connection, so it is not routed by the transaction coordinator id)
    private void HandleResponse(TransactionMetaStoreOp op, ConnectionSession session, BaseCommand response)
    {
        try
        {
            response.Expect(op.ResponseType);
        }
        catch (Exception exception)
        {
            Complete(op, exception);
            return;
        }

        if (!op.TryGetError(response, out var error, out var message))
        {
            Complete(op, response);
            return;
        }

        // Upstream: TransactionMetaStoreHandler#checkIfNeedRetryByError
        if (error == ServerError.TransactionCoordinatorNotFound)
        {
            if (CurrentState != TransactionMetaStoreHandlerState.Connecting)
                RequestReconnect(session);

            ScheduleResend(op);
            return;
        }

        Complete(op, GetExceptionByServerError(error, message));
    }

    // The coordinator may have moved to another broker, so the topic is looked up again. The connection is shared, so it is not closed
    private void RequestReconnect(ConnectionSession session)
    {
        if (ReferenceEquals(Volatile.Read(ref _session), session))
            session.Cancel();
    }

    private void ScheduleResend(TransactionMetaStoreOp op)
    {
        if (op.TryScheduleRetry())
            _ = ResendAfter(op, op.NextRetryDelay());
    }

    private async Task ResendAfter(TransactionMetaStoreOp op, TimeSpan delay)
    {
        try
        {
            await Task.Delay(delay, _cancellationTokenSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        op.RetryElapsed();

        if (_pendingOps.ContainsKey(op.Id))
            CheckStateAndSend(op);
    }

    // Upstream: TransactionMetaStoreHandler#onResponse
    private void Complete(TransactionMetaStoreOp op, BaseCommand response)
    {
        if (!op.TryClaimCompletion())
            return;

        Release(op);
        op.SetResult(response);
    }

    private void Complete(TransactionMetaStoreOp op, Exception exception)
    {
        if (!op.TryClaimCompletion())
            return;

        Release(op);
        op.SetException(exception);
    }

    private void Release(TransactionMetaStoreOp op)
    {
        _ = _pendingOps.TryRemove(op.Id, out _);

        try
        {
            _ = _permits.Release();
        }
        catch (ObjectDisposedException)
        {
            // The handler has been disposed
        }
    }

    // Upstream: TransactionMetaStoreHandler#failPendingRequest
    private void FailAllPending(Exception exception)
    {
        foreach (var op in _pendingOps.Values)
            Complete(op, exception);
    }

    // Upstream: TransactionMetaStoreHandler#resendPendingRequests (C# deviation: a new request id is assigned when the request is sent on the new connection)
    private void ResendPendingOps()
    {
        foreach (var op in _pendingOps.Values)
            CheckStateAndSend(op);
    }

    // Upstream: ConnectionHandler#grabCnx, ConnectionHandler#reconnectLater
    private async Task ConnectLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var session = await Connect(cancellationToken).ConfigureAwait(false);
                if (session is null)
                    return;

                await WaitForReconnect(session, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                if (cancellationToken.IsCancellationRequested || !ShouldReconnect(exception))
                    return;
            }

            try
            {
                await Task.Delay(_reconnectBackoff.Next(), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    // Upstream: TransactionMetaStoreHandler#connectionOpened, TransactionMetaStoreHandler#registerToConnection
    private async Task<ConnectionSession?> Connect(CancellationToken cancellationToken)
    {
        var connection = await _connectionPool.FindConnectionForTopic(_topic, cancellationToken).ConfigureAwait(false);

        // A broker with a lower protocol version does not know the command
        if (connection.ProtocolVersion >= TcClientConnectProtocolVersion)
        {
            var response = await connection.Send(new CommandTcClientConnectRequest { TcId = _tcId }, cancellationToken).ConfigureAwait(false);

            try
            {
                ThrowIfConnectFailed(response);
            }
            catch (NotAllowedException exception)
            {
                // The shared connection is not closed (C# deviation: the connection is shared with producers and consumers)
                CloseHandler(exception);
                return null;
            }
        }

        if (!TrySetState(TransactionMetaStoreHandlerState.Connecting, TransactionMetaStoreHandlerState.Ready))
        {
            _ = TryFaultConnected(new InvalidOperationException($"Failed to change the state from {CurrentState} to Ready"));
            return null;
        }

        var session = new ConnectionSession(connection, cancellationToken);

        try
        {
            session.RegistrationId = connection.AddTransactionMetaStoreHandler(this);
        }
        catch
        {
            session.Dispose();
            _ = TrySetState(TransactionMetaStoreHandlerState.Ready, TransactionMetaStoreHandlerState.Connecting);
            throw;
        }

        Volatile.Write(ref _session, session);
        _ = _connected.TrySetResult(null);
        _reconnectBackoff.Reset();
        ResendPendingOps();

        return session;
    }

    private static void ThrowIfConnectFailed(BaseCommand response)
    {
        response.Expect(BaseCommand.Types.Type.TcClientConnectResponse);

        if (response.TcClientConnectResponse.HasError)
            throw GetExceptionByServerError(response.TcClientConnectResponse.Error, response.TcClientConnectResponse.Message);
    }

    // The handler must never try again after the coordinator has refused the client
    private void CloseHandler(Exception exception)
    {
        SetState(TransactionMetaStoreHandlerState.Closed);
        FailAllPending(new PulsarClientClosedException(ClosedMessage));
        _ = TryFaultConnected(exception);
    }

    // Upstream: TransactionMetaStoreHandler#connectionFailed
    private bool ShouldReconnect(Exception exception)
    {
        var isNonRetriable = !RetriableErrors.IsRetriable(exception);
        var isTimedOut = Stopwatch.GetTimestamp() > _lookupDeadlineTimestamp;

        // Only a failure before the first successful connect stops the handler
        if ((isNonRetriable || isTimedOut) && TryFaultConnected(exception))
        {
            SetState(TransactionMetaStoreHandlerState.Failed);
            FailAllPending(new MetaStoreHandlerNotReadyException($"Transaction meta store handler for tcId {_tcId} not connected."));
            return false;
        }

        return true;
    }

    private async Task WaitForReconnect(ConnectionSession session, CancellationToken cancellationToken)
    {
        try
        {
            _ = await session.Connection.State.OnStateChangeFrom(ConnectionState.Connected, session.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // A reconnect was requested or the connection was closed
        }
        finally
        {
            Leave(session);
        }
    }

    private void Leave(ConnectionSession session)
    {
        _ = TrySetState(TransactionMetaStoreHandlerState.Ready, TransactionMetaStoreHandlerState.Connecting);
        _ = Interlocked.CompareExchange(ref _session, null, session);
        session.Connection.RemoveTransactionMetaStoreHandler(session.RegistrationId);
        session.Dispose();
    }

    private bool TryFaultConnected(Exception exception)
    {
        if (!_connected.TrySetException(exception))
            return false;

        _ = _connected.Task.Exception; // Nobody may be waiting for the connection, so the exception is observed here
        return true;
    }

    // Upstream: TransactionMetaStoreHandler#run
    private void OnTimeoutTimer()
    {
        try
        {
            var state = CurrentState;
            if (state is TransactionMetaStoreHandlerState.Closing or TransactionMetaStoreHandlerState.Closed)
                return;

            var now = Stopwatch.GetTimestamp();

            while (_timeoutQueue.TryPeek(out var head) && (head.CreatedTimestamp + _operationTimeoutTimestamp - now) <= 0)
            {
                _ = _timeoutQueue.TryDequeue(out _);

                if (!_pendingOps.TryGetValue(head.OpId, out var op))
                    continue;

                var connection = Volatile.Read(ref _session)?.Connection;
                Complete(op, new OperationTimeoutException($"{op.Description} failed due to timeout. connection: {connection?.ToString() ?? "null"}. pending-queue: {_pendingOps.Count - 1}"));
            }

            var next = _operationTimeout;

            if (_timeoutQueue.TryPeek(out var first))
            {
                var remaining = first.CreatedTimestamp + _operationTimeoutTimestamp - Stopwatch.GetTimestamp();
                if (remaining > 0)
                    next = TimeSpan.FromSeconds(remaining / (double) Stopwatch.Frequency);
            }

            _ = _timeoutTimer.Change(next, Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
            // The handler has been disposed
        }
    }

    private bool TrySetState(TransactionMetaStoreHandlerState expected, TransactionMetaStoreHandlerState next)
    {
        lock (_stateLock)
        {
            if (_state != (int) expected)
                return false;

            SetStateCore(next);
            return true;
        }
    }

    private void SetState(TransactionMetaStoreHandlerState next)
    {
        lock (_stateLock)
        {
            SetStateCore(next);
        }
    }

    // A final state never changes, just like the state of the StateManager
    private void SetStateCore(TransactionMetaStoreHandlerState next)
    {
        if (_stateManager.IsFinalState((TransactionMetaStoreHandlerState) _state))
            return;

        _state = (int) next;
        _ = _stateManager.SetState(next);
    }

    private sealed class ConnectionSession : IDisposable
    {
        private readonly CancellationTokenSource _cancellationTokenSource;

        public ConnectionSession(IConnection connection, CancellationToken cancellationToken)
        {
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            Connection = connection;
        }

        public IConnection Connection { get; }

        public ulong RegistrationId { get; set; }

        public CancellationToken Token => _cancellationTokenSource.Token;

        public void Cancel()
        {
            try
            {
                _cancellationTokenSource.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The handler has already left the connection
            }
        }

        public void Dispose()
            => _cancellationTokenSource.Dispose();
    }
}
