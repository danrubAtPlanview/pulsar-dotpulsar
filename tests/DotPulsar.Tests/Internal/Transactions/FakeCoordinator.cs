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

namespace DotPulsar.Tests.Internal.Transactions;

using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.PulsarApi;

// A connection to a transaction coordinator. The requests are recorded and answered by 'Respond'. A request that is not answered stays pending until 'Answer' or 'Drop'
public sealed class FakeCoordinator
{
    private readonly object _lock;
    private readonly List<object> _requests;
    private readonly List<CommandTcClientConnectRequest> _connectRequests;
    private readonly List<PendingRequest> _pendingRequests;
    private readonly List<TaskCompletionSource<ConnectionState>> _stateWaiters;
    private readonly List<ulong> _removedRegistrations;
    private readonly Dictionary<Type, int> _requestNumbers;
    private ulong _lastRegistrationId;
    private int _registrationCount;
    private bool _isDropped;

    public FakeCoordinator(int protocolVersion = 19)
    {
        _lock = new object();
        _requests = [];
        _connectRequests = [];
        _pendingRequests = [];
        _stateWaiters = [];
        _removedRegistrations = [];
        _requestNumbers = [];
        _lastRegistrationId = 0;
        _registrationCount = 0;
        _isDropped = false;

        Connection = Substitute.For<IConnection>();
        Connection.ProtocolVersion.Returns(protocolVersion);
        Connection.State.IsFinalState().Returns(_ => IsDropped);
        Connection.State.OnStateChangeFrom(ConnectionState.Connected, Arg.Any<CancellationToken>()).Returns(callInfo => WaitForStateChange(callInfo.Arg<CancellationToken>()));
        Connection.Send(Arg.Any<CommandTcClientConnectRequest>(), Arg.Any<CancellationToken>()).Returns(callInfo => HandleConnect(callInfo.Arg<CommandTcClientConnectRequest>(), callInfo.Arg<CancellationToken>()));
        Connection.Send(Arg.Any<CommandNewTxn>(), Arg.Any<CancellationToken>()).Returns(callInfo => Handle(callInfo.Arg<CommandNewTxn>(), callInfo.Arg<CancellationToken>()));
        Connection.Send(Arg.Any<CommandAddPartitionToTxn>(), Arg.Any<CancellationToken>()).Returns(callInfo => Handle(callInfo.Arg<CommandAddPartitionToTxn>(), callInfo.Arg<CancellationToken>()));
        Connection.Send(Arg.Any<CommandAddSubscriptionToTxn>(), Arg.Any<CancellationToken>()).Returns(callInfo => Handle(callInfo.Arg<CommandAddSubscriptionToTxn>(), callInfo.Arg<CancellationToken>()));
        Connection.Send(Arg.Any<CommandEndTxn>(), Arg.Any<CancellationToken>()).Returns(callInfo => Handle(callInfo.Arg<CommandEndTxn>(), callInfo.Arg<CancellationToken>()));
        Connection.AddTransactionMetaStoreHandler(Arg.Any<ITransactionMetaStoreHandler>()).Returns(callInfo => Register(callInfo.Arg<ITransactionMetaStoreHandler>()));
        Connection.When(connection => connection.RemoveTransactionMetaStoreHandler(Arg.Any<ulong>())).Do(callInfo => Unregister(callInfo.Arg<ulong>()));

        Respond = (command, _) => TransactionResponses.Success(command);
        RespondToConnect = (_, _) => TransactionResponses.TcClientConnect();
        OnRegister = null;
    }

    public IConnection Connection { get; }

    // Answers a request. The arguments are the request and its number among the requests of the same type, starting at 1. Return null to leave the request pending
    public Func<object, int, BaseCommand?> Respond { get; set; }

    // Answers a TcClientConnectRequest. The arguments are the request and its number, starting at 1. Return null to leave the request pending
    public Func<CommandTcClientConnectRequest, int, BaseCommand?> RespondToConnect { get; set; }

    public Action<ITransactionMetaStoreHandler>? OnRegister { get; set; }

    public bool IsDropped
    {
        get
        {
            lock (_lock)
                return _isDropped;
        }
    }

    // The CommandNewTxn, CommandAddPartitionToTxn, CommandAddSubscriptionToTxn and CommandEndTxn requests in the order they were received
    public IReadOnlyList<object> Requests
    {
        get
        {
            lock (_lock)
                return _requests.ToArray();
        }
    }

    public IReadOnlyList<CommandTcClientConnectRequest> ConnectRequests
    {
        get
        {
            lock (_lock)
                return _connectRequests.ToArray();
        }
    }

    public int PendingRequestCount
    {
        get
        {
            lock (_lock)
                return _pendingRequests.Count(request => !request.Response.Task.IsCompleted);
        }
    }

    public int RegistrationCount
    {
        get
        {
            lock (_lock)
                return _registrationCount;
        }
    }

    public IReadOnlyList<ulong> RemovedRegistrations
    {
        get
        {
            lock (_lock)
                return _removedRegistrations.ToArray();
        }
    }

    public IReadOnlyList<TCommand> RequestsOf<TCommand>()
        => Requests.OfType<TCommand>().ToArray();

    // Answers a request that is pending
    public void Answer(object request, BaseCommand response)
    {
        PendingRequest? pending;

        lock (_lock)
            pending = _pendingRequests.FirstOrDefault(r => ReferenceEquals(r.Request, request));

        if (pending is null)
            throw new InvalidOperationException("The request is not pending");

        _ = pending.Response.TrySetResult(response);
    }

    // Simulates that the connection is lost. The requests that are pending are cancelled, like the requests of a disposed connection
    public void Drop()
    {
        PendingRequest[] pendingRequests;
        TaskCompletionSource<ConnectionState>[] stateWaiters;

        lock (_lock)
        {
            _isDropped = true;
            pendingRequests = _pendingRequests.ToArray();
            stateWaiters = _stateWaiters.ToArray();
            _pendingRequests.Clear();
            _stateWaiters.Clear();
        }

        foreach (var pending in pendingRequests)
            _ = pending.Response.TrySetCanceled();

        foreach (var waiter in stateWaiters)
            _ = waiter.TrySetResult(ConnectionState.Disconnected);
    }

    // A pool that returns the connections in the order they are asked for, and the last one when there are no more
    public static IConnectionPool CreatePool(uint partitions, params FakeCoordinator[] coordinators)
    {
        var pool = Substitute.For<IConnectionPool>();
        var calls = 0;

        pool.GetNumberOfPartitions(Constants.TransactionCoordinatorAssignTopic, Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(partitions));
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            var index = Math.Min(Interlocked.Increment(ref calls) - 1, coordinators.Length - 1);
            return new ValueTask<IConnection>(coordinators[index].Connection);
        });

        return pool;
    }

    // A request is recorded and, when it is not answered at once, made pending in one step. A thread that is descheduled in between would otherwise leave a request that
    // 'Requests' lists and 'Answer' can not find yet
    private Task<BaseCommand> HandleConnect(CommandTcClientConnectRequest request, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_isDropped)
                return Task.FromException<BaseCommand>(new ConnectionDisposedException());

            _connectRequests.Add(request);
            return ReplyLocked(request, RespondToConnect(request, _connectRequests.Count), cancellationToken);
        }
    }

    private Task<BaseCommand> Handle(object request, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_isDropped)
                return Task.FromException<BaseCommand>(new ConnectionDisposedException());

            _requests.Add(request);
            var number = _requestNumbers[request.GetType()] = _requestNumbers.GetValueOrDefault(request.GetType()) + 1;
            return ReplyLocked(request, Respond(request, number), cancellationToken);
        }
    }

    private Task<BaseCommand> ReplyLocked(object request, BaseCommand? response, CancellationToken cancellationToken)
    {
        if (response is not null)
            return Task.FromResult(response);

        var pending = new PendingRequest(request);
        _pendingRequests.Add(pending);
        _ = cancellationToken.Register(() => pending.Response.TrySetCanceled(cancellationToken));
        return pending.Response.Task;
    }

    private ValueTask<ConnectionState> WaitForStateChange(CancellationToken cancellationToken)
    {
        var waiter = new TaskCompletionSource<ConnectionState>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = cancellationToken.Register(() => waiter.TrySetCanceled(cancellationToken));

        lock (_lock)
        {
            if (_isDropped)
                _ = waiter.TrySetResult(ConnectionState.Disconnected);
            else
                _stateWaiters.Add(waiter);
        }

        return new ValueTask<ConnectionState>(waiter.Task);
    }

    private ulong Register(ITransactionMetaStoreHandler handler)
    {
        ulong id;

        lock (_lock)
        {
            id = ++_lastRegistrationId;
            ++_registrationCount;
        }

        OnRegister?.Invoke(handler);
        return id;
    }

    private void Unregister(ulong id)
    {
        lock (_lock)
            _removedRegistrations.Add(id);
    }

    private sealed class PendingRequest
    {
        public PendingRequest(object request)
        {
            Request = request;
            Response = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public object Request { get; }

        public TaskCompletionSource<BaseCommand> Response { get; }
    }
}
