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

using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using NSubstitute.ReceivedExtensions;
using System.Diagnostics;

[Trait("Category", "Unit")]
public sealed class TransactionCoordinatorClientTests
{
    private const string AssignPartitionTopic = "persistent://pulsar/system/transaction_coordinator_assign-partition-0";

    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _operationTimeout = TimeSpan.FromSeconds(30);
    private static readonly TransactionId _transactionId = new(0, 0);

    public enum Request
    {
        NewTransaction,
        AddPublishPartitionToTxn,
        AddSubscriptionToTxn,
        Commit,
        Abort
    }

    // Upstream: TransactionCoordinatorClientTest#testClientStart
    [Fact]
    public async Task Start_GivenAlreadyStarted_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        await using var uut = CreateClient(CreateDiscovery());
        await uut.Start(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>().Message.ShouldBe("Can not start while current state is Ready");
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
    }

    [Fact]
    public async Task Start_GivenStartInProgress_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var discovery = CreateDiscovery();
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        await using var uut = CreateClient(discovery);
        var starting = uut.Start(Current.CancellationToken).AsTask();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());
        start.SetResult(null);
        await starting.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>().Message.ShouldBe("Can not start while current state is Starting");
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
    }

    [Fact]
    public async Task Start_GivenDiscoveryFails_ShouldResetToNoneAndAllowRetry()
    {
        //Arrange
        var failing = CreateDiscovery();
        failing.Start(Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new TransactionCoordinatorClientException("not available")));
        var working = CreateDiscovery();
        var discoveries = new Queue<ITcDiscovery>(new[] { failing, working });
        await using var uut = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), _operationTimeout, () => discoveries.Dequeue());

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());
        var stateAfterFailure = uut.State;
        await uut.Start(Current.CancellationToken);

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientException>().Message.ShouldBe("not available");
        stateAfterFailure.ShouldBe(TransactionCoordinatorClientState.None);
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
        await failing.Received(1).DisposeAsync();
        await working.DidNotReceive().DisposeAsync();
    }

    // Upstream: TransactionClientConnectTest#testNotEnableTransactionInBroker
    [Fact]
    public async Task Start_GivenNoTransactionCoordinatorPartitions_ShouldThrowTransactionCoordinatorClientException()
    {
        //Arrange
        var pool = FakeCoordinator.CreatePool(0, new FakeCoordinator());
        await using var uut = CreateClient(pool);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientException>().Message.ShouldBe("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized");
        uut.State.ShouldBe(TransactionCoordinatorClientState.None);
        await pool.DidNotReceive().FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task EnsureStarted_GivenStartedClient_ShouldReturnWithoutStartingAgain()
    {
        //Arrange
        var factoryCalls = 0;
        await using var uut = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), _operationTimeout, () =>
        {
            Interlocked.Increment(ref factoryCalls);
            return CreateDiscovery();
        });
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.EnsureStarted(Current.CancellationToken);

        //Assert
        factoryCalls.ShouldBe(1);
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
    }

    [Fact]
    public async Task EnsureStarted_GivenConcurrentCallers_ShouldStartOnce()
    {
        //Arrange
        var factoryCalls = 0;
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var discovery = CreateDiscovery();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        await using var uut = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), _operationTimeout, () =>
        {
            Interlocked.Increment(ref factoryCalls);
            return discovery;
        });
        var callers = Enumerable.Range(0, 10).Select(_ => Task.Run(() => uut.EnsureStarted(Current.CancellationToken).AsTask())).ToArray();
        await WaitFor.Until(() => uut.State == TransactionCoordinatorClientState.Starting, "the client is starting");
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);

        //Act
        start.SetResult(null);
        await Task.WhenAll(callers).WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        factoryCalls.ShouldBe(1);
        await discovery.Received(1).Start(Arg.Any<CancellationToken>());
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
    }

    // Upstream: TransactionClientConnectTest#testNotEnableTransactionInBroker (the start is lazy, and a failed start can be retried)
    [Fact]
    public async Task EnsureStarted_GivenStartFails_ShouldThrowToEveryWaiterAndRetryOnNextCall()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failing = CreateDiscovery();
        failing.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        var working = CreateDiscovery();
        var discoveries = new Queue<ITcDiscovery>(new[] { failing, working });
        await using var uut = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), _operationTimeout, () => discoveries.Dequeue());
        var callers = Enumerable.Range(0, 5).Select(_ => Task.Run(() => uut.EnsureStarted(Current.CancellationToken).AsTask())).ToArray();
        await WaitFor.Until(() => uut.State == TransactionCoordinatorClientState.Starting, "the client is starting");

        //Act
        start.SetException(new TransactionCoordinatorClientException("not available"));
        var exceptions = await Task.WhenAll(callers.Select(caller => Record.ExceptionAsync(() => caller).AsTask()));
        await uut.EnsureStarted(Current.CancellationToken);

        //Assert
        exceptions.ShouldAllBe(exception => exception is TransactionCoordinatorClientException);
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
        discoveries.ShouldBeEmpty();
    }

    [Fact]
    public async Task EnsureStarted_GivenCancelledCallerAndFailedStart_ShouldRetryOnNextCall()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failing = CreateDiscovery();
        failing.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        var working = CreateDiscovery();
        var discoveries = new Queue<ITcDiscovery>(new[] { failing, working });
        await using var uut = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), _operationTimeout, () => discoveries.Dequeue());
        using var cts = new CancellationTokenSource();
        var caller = uut.EnsureStarted(cts.Token).AsTask();
        await cts.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(caller.WaitAsync(_timeout, Current.CancellationToken));

        //Act
        start.SetException(new TransactionCoordinatorClientException("not available"));
        var stopwatch = Stopwatch.StartNew();
        while (uut.State != TransactionCoordinatorClientState.None && stopwatch.Elapsed < _timeout) { } // The next call comes as soon as the state is reset, which is before the task of the failed start is faulted
        await uut.EnsureStarted(Current.CancellationToken);

        //Assert
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
        discoveries.ShouldBeEmpty();
    }

    [Fact]
    public async Task EnsureStarted_GivenCallerCancels_ShouldNotCancelTheStart()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var discovery = CreateDiscovery();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        await using var uut = CreateClient(discovery);
        using var cts = new CancellationTokenSource();
        var cancelled = uut.EnsureStarted(cts.Token).AsTask();
        var other = uut.EnsureStarted(Current.CancellationToken).AsTask();

        //Act
        await cts.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(cancelled.WaitAsync(_timeout, Current.CancellationToken));
        start.SetResult(null);
        await other.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
        await discovery.Received(1).Start(CancellationToken.None);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EnsureStarted_GivenDisposedClient_ShouldThrowTransactionCoordinatorClientStateException(bool isStarted)
    {
        //Arrange
        var uut = CreateClient(CreateDiscovery());
        if (isStarted)
            await uut.Start(Current.CancellationToken);
        await uut.DisposeAsync();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.EnsureStarted(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>().Message.ShouldBe("Can not send a request while current state is Closed");
    }

    [Fact]
    public async Task NewTransaction_GivenNotStarted_ShouldThrowStateException()
    {
        //Arrange
        var discovery = CreateDiscovery(CreateHandler());
        await using var uut = CreateClient(discovery);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>().Message.ShouldBe("Can not send a request while current state is None");
        discovery.DidNotReceive().NextHandler();
    }

    // Upstream: TransactionClientCloseTest#testCoordinatorRequestsFailBeforeStart
    [Fact]
    public async Task Requests_GivenClientNotStarted_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var pool = Substitute.For<IConnectionPool>();
        await using var uut = CreateClient(pool);

        //Act
        var exceptions = await SendAll(uut);

        //Assert
        exceptions.ShouldAllBe(exception => exception is TransactionCoordinatorClientStateException && exception.Message == "Can not send a request while current state is None");
        pool.ReceivedCalls().ShouldBeEmpty();
    }

    // Upstream: TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose
    [Fact]
    public async Task Requests_GivenDisposedClient_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateClient(FakeCoordinator.CreatePool(1, coordinator));
        await uut.Start(Current.CancellationToken);
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);

        //Act
        await uut.DisposeAsync();
        var exceptions = await SendAll(uut);

        //Assert
        exceptions.ShouldAllBe(exception => exception is TransactionCoordinatorClientStateException && exception.Message == "Can not send a request while current state is Closed");
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
        coordinator.Requests.ShouldBeEmpty();
    }

    [Fact]
    public async Task AllRequests_GivenDisposed_ShouldThrowStateExceptionAndStateClosed()
    {
        //Arrange
        var handler = CreateHandler();
        var uut = CreateClient(CreateDiscovery(handler));
        await uut.Start(Current.CancellationToken);
        await uut.DisposeAsync();

        //Act
        var exceptions = await SendAll(uut);

        //Assert
        exceptions.ShouldAllBe(exception => exception is TransactionCoordinatorClientStateException);
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
        handler.ReceivedCalls().ShouldBeEmpty();
    }

    [Fact]
    public async Task Requests_GivenStartedClient_ShouldDisposeHandlersWithoutPendingOperations()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        await using var uut = CreateClient(FakeCoordinator.CreatePool(2, coordinator));
        await uut.Start(Current.CancellationToken);
        var pending = new[]
        {
            uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask(),
            uut.Commit(new TransactionId(0, 1), Current.CancellationToken).AsTask(),
            uut.Abort(new TransactionId(1, 1), Current.CancellationToken).AsTask()
        };
        await WaitFor.Until(() => coordinator.Requests.Count == 3, "the requests are sent");

        //Act
        await uut.DisposeAsync();

        //Assert
        foreach (var task in pending)
            await Should.ThrowAsync<PulsarClientClosedException>(task.WaitAsync(_timeout, Current.CancellationToken));

        uut.Handlers.ShouldBeEmpty();
    }

    [Fact]
    public async Task NewTransaction_GivenNoHandler_ShouldThrowTransactionCoordinatorClientException()
    {
        //Arrange
        var discovery = CreateDiscovery();
        discovery.NextHandler().Returns((ITransactionMetaStoreHandler?) null);
        await using var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientException>().Message.ShouldBe("No transaction coordinator is currently available");
    }

    [Fact]
    public async Task NewTransaction_ShouldUseNextHandler()
    {
        //Arrange
        var handler = CreateHandler();
        handler.NewTransaction(TimeSpan.FromMilliseconds(200), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(3, 7)));
        var discovery = CreateDiscovery(handler);
        await using var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);

        //Act
        var id = await uut.NewTransaction(TimeSpan.FromMilliseconds(200), Current.CancellationToken);

        //Assert
        id.ShouldBe(new TransactionId(3, 7));
        discovery.Received(1).NextHandler();
    }

    [Fact]
    public async Task Commit_GivenUnknownCoordinatorId_ShouldThrowMetaStoreHandlerNotExistsException()
    {
        //Arrange
        await using var uut = CreateClient(CreateDiscovery(CreateHandler()));
        await uut.Start(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(new TransactionId(1, 1), Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<MetaStoreHandlerNotExistsException>();
    }

    [Theory]
    [InlineData(Request.AddPublishPartitionToTxn)]
    [InlineData(Request.AddSubscriptionToTxn)]
    [InlineData(Request.Commit)]
    [InlineData(Request.Abort)]
    public async Task Request_GivenUnknownCoordinatorId_ShouldThrowMetaStoreHandlerNotExistsException(Request request)
    {
        //Arrange
        await using var uut = CreateClient(CreateDiscovery(CreateHandler()));
        await uut.Start(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => Send(uut, request, new TransactionId(5, 1)));

        //Assert
        exception.ShouldBeOfType<MetaStoreHandlerNotExistsException>().Message.ShouldBe("Transaction meta store handler for transaction meta store 5 not exists.");
    }

    [Fact]
    public async Task Commit_ShouldRouteToHandlerForMostSigBits()
    {
        //Arrange
        var handlers = new[] { CreateHandler(), CreateHandler(), CreateHandler() };
        await using var uut = CreateClient(CreateDiscovery(handlers));
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.Commit(new TransactionId(2, 9), Current.CancellationToken);

        //Assert
        await handlers[2].Received(1).EndTxn(new TransactionId(2, 9), TxnAction.Commit, Current.CancellationToken);
        await handlers[0].DidNotReceive().EndTxn(Arg.Any<TransactionId>(), Arg.Any<TxnAction>(), Arg.Any<CancellationToken>());
        await handlers[1].DidNotReceive().EndTxn(Arg.Any<TransactionId>(), Arg.Any<TxnAction>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Abort_ShouldRouteToHandlerForMostSigBits()
    {
        //Arrange
        var handlers = new[] { CreateHandler(), CreateHandler(), CreateHandler() };
        await using var uut = CreateClient(CreateDiscovery(handlers));
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.Abort(new TransactionId(1, 9), Current.CancellationToken);

        //Assert
        await handlers[1].Received(1).EndTxn(new TransactionId(1, 9), TxnAction.Abort, Current.CancellationToken);
        await handlers[0].DidNotReceive().EndTxn(Arg.Any<TransactionId>(), Arg.Any<TxnAction>(), Arg.Any<CancellationToken>());
        await handlers[2].DidNotReceive().EndTxn(Arg.Any<TransactionId>(), Arg.Any<TxnAction>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AddSubscriptionToTxn_ShouldRouteToHandlerForMostSigBits()
    {
        //Arrange
        var handlers = new[] { CreateHandler(), CreateHandler() };
        await using var uut = CreateClient(CreateDiscovery(handlers));
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.AddSubscriptionToTxn(new TransactionId(1, 9), "my-topic", "my-subscription", Current.CancellationToken);

        //Assert
        await handlers[1].Received(1).AddSubscriptionToTxn(new TransactionId(1, 9), "my-topic", "my-subscription", Current.CancellationToken);
        await handlers[0].DidNotReceive().AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AddPublishPartitionToTxn_ShouldRouteToHandlerForMostSigBits()
    {
        //Arrange
        var handlers = new[] { CreateHandler(), CreateHandler() };
        await using var uut = CreateClient(CreateDiscovery(handlers));
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.AddPublishPartitionToTxn(new TransactionId(0, 9), "my-topic", Current.CancellationToken);

        //Assert
        await handlers[0].Received(1).AddPublishPartitionToTxn(new TransactionId(0, 9), "my-topic", Current.CancellationToken);
        await handlers[1].DidNotReceive().AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Upstream: TransactionClientCloseTest#testNewTransactionWhenClientClosesDuringCoordinatorSelection
    [Fact]
    public async Task NewTransaction_GivenClientDisposedWhileSelectingHandler_ShouldThrowPulsarClientClosedException()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var handler = await CreateConnectedHandler(coordinator);
        var discovery = CreateDiscovery();
        discovery.DisposeAsync().Returns(_ => handler.DisposeAsync());
        var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);
        var disposal = default(ValueTask);
        var isDisposed = false;
        discovery.NextHandler().Returns(_ =>
        {
            if (!isDisposed)
            {
                isDisposed = true;
                disposal = uut.DisposeAsync();
            }

            return handler;
        });

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        await disposal;

        //Assert
        isDisposed.ShouldBeTrue("the client is disposed while the coordinator is selected");
        exception.ShouldBeOfType<PulsarClientClosedException>();
        coordinator.Requests.ShouldBeEmpty();
    }

    // Upstream: TransactionClientCloseTest#testCommitWhenClientClosesBeforeTheRequestIsSent
    [Fact]
    public async Task Commit_GivenClientDisposedBeforeRequestIsSent_ShouldThrowPulsarClientClosedExceptionAndReleaseRequest()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var handler = await CreateConnectedHandler(coordinator);
        var discovery = CreateDiscovery();
        discovery.DisposeAsync().Returns(_ => handler.DisposeAsync());
        var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);
        var disposal = default(ValueTask);
        discovery.HandlerForCoordinator(0).Returns(_ =>
        {
            disposal = uut.DisposeAsync();
            return handler;
        });

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(_transactionId, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        await disposal;

        //Assert
        exception.ShouldBeOfType<PulsarClientClosedException>();
        await coordinator.Connection.DidNotReceive().Send(Arg.Any<CommandEndTxn>(), Arg.Any<CancellationToken>());
        handler.PendingRequestsCount.ShouldBe(0);
        handler.AvailableRequestPermits.ShouldBe(1000);
    }

    [Fact]
    public async Task Request_GivenClientDisposedWhileHandlerIsLookedUp_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var discovery = CreateDiscovery();
        var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);
        var disposal = default(ValueTask);
        discovery.HandlerForCoordinator(0).Returns(_ =>
        {
            disposal = uut.DisposeAsync();
            return null; // The disposed discovery has no handlers
        });

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(_transactionId, Current.CancellationToken).AsTask());
        await disposal;

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>();
    }

    [Fact]
    public async Task Handlers_GivenStartedClient_ShouldReturnTheHandlersOfTheDiscovery()
    {
        //Arrange
        var handlers = new[] { CreateHandler(), CreateHandler() };
        await using var uut = CreateClient(CreateDiscovery(handlers));
        var handlersBeforeStart = uut.Handlers;
        await uut.Start(Current.CancellationToken);

        //Act
        var handlersAfterStart = uut.Handlers;

        //Assert
        handlersBeforeStart.ShouldBeEmpty();
        handlersAfterStart.ShouldBe(handlers);
    }

    // Upstream: TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose (AssignTopicTcDiscovery#close)
    [Fact]
    public async Task DisposeAsync_GivenCalledTwice_ShouldBeNoOp()
    {
        //Arrange
        var discovery = CreateDiscovery();
        var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.DisposeAsync();
        var exception = await Record.ExceptionAsync(() => uut.DisposeAsync().AsTask());

        //Assert
        exception.ShouldBeNull();
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
        await discovery.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_GivenNotStarted_ShouldCloseClient()
    {
        //Arrange
        var uut = CreateClient(CreateDiscovery());

        //Act
        await uut.DisposeAsync();

        //Assert
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
    }

    [Fact]
    public async Task DisposeAsync_GivenDiscoveryThatFailsToDispose_ShouldStillCloseClient()
    {
        //Arrange
        var discovery = CreateDiscovery();
        discovery.DisposeAsync().Returns(_ => ValueTask.FromException(new InvalidOperationException()));
        var uut = CreateClient(discovery);
        await uut.Start(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.DisposeAsync().AsTask());

        //Assert
        exception.ShouldBeNull();
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
    }

    [Fact]
    public async Task DisposeAsync_GivenStartInProgress_ShouldNotBeUndoneWhenStartCompletes()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var discovery = CreateDiscovery();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        var uut = CreateClient(discovery);
        var starting = uut.Start(Current.CancellationToken).AsTask();

        //Act
        await uut.DisposeAsync();
        start.SetResult(null);
        await starting.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
        await discovery.Received(2).DisposeAsync(); // The discovery is disposed when the client closes, and again when the start finds out that the client has closed
    }

    [Fact]
    public async Task DisposeAsync_GivenStartInProgressThatFails_ShouldStayClosed()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var discovery = CreateDiscovery();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        var uut = CreateClient(discovery);
        var starting = uut.Start(Current.CancellationToken).AsTask();

        //Act
        await uut.DisposeAsync();
        start.SetException(new PulsarClientClosedException());
        var exception = await Record.ExceptionAsync(() => starting.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<PulsarClientClosedException>();
        uut.State.ShouldBe(TransactionCoordinatorClientState.Closed);
    }

    // Upstream: TransactionClientConnectTest#testTransactionNewReconnect
    [Fact]
    public async Task NewTransaction_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess()
    {
        await TryCommandReconnect<CommandNewTxn>(
            client => client.NewTransaction(TimeSpan.FromMilliseconds(200), Current.CancellationToken).AsTask(),
            client => client.NewTransaction(TimeSpan.FromMilliseconds(200), Current.CancellationToken).AsTask(),
            commands => commands.ShouldAllBe(command => command.TcId == 0 && command.TxnTtlMillis == 200));
    }

    // Upstream: TransactionClientConnectTest#testTransactionAddSubscriptionToTxnAsyncReconnect
    [Fact]
    public async Task AddSubscriptionToTxn_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess()
    {
        await TryCommandReconnect<CommandAddSubscriptionToTxn>(
            client => client.AddSubscriptionToTxn(new TransactionId(0, 0), "test", "test", Current.CancellationToken).AsTask(),
            client => client.AddSubscriptionToTxn(new TransactionId(0, 0), "test", "test", Current.CancellationToken).AsTask(),
            commands => commands.ShouldAllBe(command => command.TxnidMostBits == 0 && command.TxnidLeastBits == 0 && command.Subscription.Count == 1 && command.Subscription[0].Topic == "test" && command.Subscription[0].Subscription_ == "test"));
    }

    // Upstream: TransactionClientConnectTest#testTransactionAbortToTxnAsyncReconnect
    [Fact]
    public async Task Abort_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess()
    {
        await TryCommandReconnect<CommandEndTxn>(
            client => client.Abort(new TransactionId(0, 0), Current.CancellationToken).AsTask(),
            client => client.Abort(new TransactionId(0, 1), Current.CancellationToken).AsTask(),
            commands => commands.ShouldAllBe(command => command.TxnAction == TxnAction.Abort && command.TxnidMostBits == 0 && (command.TxnidLeastBits == 0 || command.TxnidLeastBits == 1)));
    }

    // Upstream: TransactionClientConnectTest#testTransactionCommitToTxnAsyncReconnect
    [Fact]
    public async Task Commit_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess()
    {
        await TryCommandReconnect<CommandEndTxn>(
            client => client.Commit(new TransactionId(0, 0), Current.CancellationToken).AsTask(),
            client => client.Commit(new TransactionId(0, 1), Current.CancellationToken).AsTask(),
            commands => commands.ShouldAllBe(command => command.TxnAction == TxnAction.Commit && command.TxnidMostBits == 0 && (command.TxnidLeastBits == 0 || command.TxnidLeastBits == 1)));
    }

    // Upstream: TransactionClientConnectTest#testTransactionAddPublishPartitionToTxnReconnect
    [Fact]
    public async Task AddPublishPartitionToTxn_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess()
    {
        await TryCommandReconnect<CommandAddPartitionToTxn>(
            client => client.AddPublishPartitionToTxn(new TransactionId(0, 0), "test", Current.CancellationToken).AsTask(),
            client => client.AddPublishPartitionToTxn(new TransactionId(0, 0), "test", Current.CancellationToken).AsTask(),
            commands => commands.ShouldAllBe(command => command.Partitions.Count == 1 && command.Partitions[0] == "test"));
    }

    // Upstream: TransactionClientConnectTest#tryCommandReconnect
    private static async Task TryCommandReconnect<TCommand>(Func<TransactionCoordinatorClient, Task> callable1, Func<TransactionCoordinatorClient, Task> callable2, Action<IReadOnlyList<TCommand>> assertCommands)
    {
        //Arrange
        var isFenced = false;

        // The coordinator is not found for the first three requests, which is when the transaction metadata store has been removed. It is found again for the fourth request.
        // The coordinator is not found as long as the transaction metadata store is fenced.
        var coordinator = new FakeCoordinator { Respond = (command, number) => command is TCommand && (number <= 3 || Volatile.Read(ref isFenced)) ? TransactionResponses.Failure(command, ServerError.TransactionCoordinatorNotFound, "TC not found") : TransactionResponses.Success(command) };
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        await using var uut = CreateClient(pool);
        await uut.Start(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => callable1(uut).WaitAsync(_timeout, Current.CancellationToken));
        var commandsAfterRemoval = coordinator.RequestsOf<TCommand>();

        Volatile.Write(ref isFenced, true);
        var fenced = callable2(uut);
        var isCompletedWhileFenced = await Task.WhenAny(fenced, Task.Delay(TimeSpan.FromSeconds(3), Current.CancellationToken)) == fenced;
        var exceptionWhileFenced = isCompletedWhileFenced ? await Record.ExceptionAsync(() => fenced) : null;

        Volatile.Write(ref isFenced, false);
        var exceptionAfterUnfence = await Record.ExceptionAsync(() => fenced.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        commandsAfterRemoval.Count.ShouldBe(4);
        assertCommands(commandsAfterRemoval);
        await pool.Received(Quantity.Within(2, int.MaxValue)).FindConnectionForTopic(AssignPartitionTopic, Arg.Any<CancellationToken>());
        coordinator.ConnectRequests.Count.ShouldBeGreaterThanOrEqualTo(2);
        (exceptionWhileFenced is TransactionCoordinatorNotFoundException).ShouldBeFalse();
        exceptionAfterUnfence.ShouldBeNull();
        assertCommands(coordinator.RequestsOf<TCommand>());
    }

    private static TransactionCoordinatorClient CreateClient(IConnectionPool pool)
        => new(pool, _operationTimeout);

    private static TransactionCoordinatorClient CreateClient(ITcDiscovery discovery)
        => new(Substitute.For<IConnectionPool>(), _operationTimeout, () => discovery);

    private static ITcDiscovery CreateDiscovery(params ITransactionMetaStoreHandler[] handlers)
    {
        var discovery = Substitute.For<ITcDiscovery>();
        var calls = 0;
        discovery.Start(Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);
        discovery.Handlers.Returns(handlers);
        discovery.HandlerForCoordinator(Arg.Any<ulong>()).Returns(callInfo => callInfo.Arg<ulong>() < (ulong) handlers.Length ? handlers[callInfo.Arg<ulong>()] : null);
        discovery.NextHandler().Returns(_ => handlers.Length == 0 ? null : handlers[Interlocked.Increment(ref calls) % handlers.Length]);
        return discovery;
    }

    private static ITransactionMetaStoreHandler CreateHandler()
        => Substitute.For<ITransactionMetaStoreHandler>();

    private static async Task<TransactionMetaStoreHandler> CreateConnectedHandler(FakeCoordinator coordinator)
    {
        var handler = new TransactionMetaStoreHandler(0, AssignPartitionTopic, FakeCoordinator.CreatePool(1, coordinator), _operationTimeout);
        handler.Start();
        await handler.Connected.WaitAsync(_timeout, Current.CancellationToken);
        return handler;
    }

    private static async Task<Exception?[]> SendAll(TransactionCoordinatorClient client)
    {
        var requests = Enum.GetValues<Request>();
        var exceptions = new Exception?[requests.Length];

        for (var i = 0; i < requests.Length; ++i)
            exceptions[i] = await Record.ExceptionAsync(() => Send(client, requests[i], _transactionId).WaitAsync(_timeout, Current.CancellationToken));

        return exceptions;
    }

    private static Task Send(TransactionCoordinatorClient client, Request request, TransactionId transactionId)
        => request switch
        {
            Request.NewTransaction => client.NewTransaction(TimeSpan.FromMilliseconds(60000), Current.CancellationToken).AsTask(),
            Request.AddPublishPartitionToTxn => client.AddPublishPartitionToTxn(transactionId, "topic", Current.CancellationToken).AsTask(),
            Request.AddSubscriptionToTxn => client.AddSubscriptionToTxn(transactionId, "topic", "subscription", Current.CancellationToken).AsTask(),
            Request.Commit => client.Commit(transactionId, Current.CancellationToken).AsTask(),
            _ => client.Abort(transactionId, Current.CancellationToken).AsTask()
        };
}
