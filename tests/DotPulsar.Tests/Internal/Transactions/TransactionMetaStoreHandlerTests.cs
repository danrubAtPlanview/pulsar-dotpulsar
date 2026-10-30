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
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using NSubstitute.ReceivedExtensions;
using System.Diagnostics;

[Trait("Category", "Unit")]
public sealed class TransactionMetaStoreHandlerTests
{
    private const string AssignPartitionTopic = "persistent://pulsar/system/transaction_coordinator_assign-partition-0";
    private const int MaxPendingOperations = 1000;

    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TransactionId _transactionId = new(1, 1);

    public enum Request
    {
        NewTransaction,
        AddPublishPartitionToTxn,
        AddSubscriptionToTxn,
        EndTxn
    }

    [Fact]
    public async Task Start_GivenCoordinatorAnswers_ShouldConnectAndBeReady()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));

        //Act
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);
        (await uut.State.OnStateChangeTo(TransactionMetaStoreHandlerState.Ready, Current.CancellationToken)).ShouldBe(TransactionMetaStoreHandlerState.Ready);
    }

    // Upstream: TransactionClientConnectTest#testHandlerStateChangeToReady
    [Fact]
    public async Task Connected_GivenReadyHandler_ShouldCompleteAndKeepReadyState()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));

        //Act
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var state = uut.CurrentState;

        //Assert
        uut.Connected.IsCompletedSuccessfully.ShouldBeTrue();
        state.ShouldBe(TransactionMetaStoreHandlerState.Ready);
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Fact]
    public async Task Start_GivenCalledTwice_ShouldConnectOnce()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        await using var uut = CreateHandler(pool);

        //Act
        uut.Start();
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await pool.Received(1).FindConnectionForTopic(AssignPartitionTopic, Arg.Any<CancellationToken>());
        coordinator.ConnectRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Connect_ShouldLookUpTheAssignPartitionOfTheCoordinatorAndSendTcClientConnectRequestWithTcId()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        await using var uut = CreateHandler(pool, tcId: 5);

        //Act
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        coordinator.ConnectRequests.ShouldHaveSingleItem().TcId.ShouldBe(5UL);
        await pool.Received(1).FindConnectionForTopic("persistent://pulsar/system/transaction_coordinator_assign-partition-5", Arg.Any<CancellationToken>());
    }

    // The connection is only kept open while it has channels, and the handler counts as a channel
    [Fact]
    public async Task Connected_ShouldRegisterHandlerWithConnectionSoItStaysActive()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));

        //Act
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        coordinator.RegistrationCount.ShouldBe(1);
        coordinator.Connection.Received(1).AddTransactionMetaStoreHandler(uut);
        coordinator.RemovedRegistrations.ShouldBeEmpty();
    }

    [Fact]
    public async Task Connect_GivenProtocolVersionBelowNineteen_ShouldNotSendTcClientConnectRequest()
    {
        //Arrange
        var coordinator = new FakeCoordinator(protocolVersion: 18);
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));

        //Act
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        coordinator.ConnectRequests.ShouldBeEmpty();
        coordinator.RegistrationCount.ShouldBe(1);
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);
    }

    // Upstream: TransactionMetaStoreHandler#connectionOpened (NotAllowedException)
    [Fact]
    public async Task Connect_GivenTcClientConnectNotAllowed_ShouldCloseHandler()
    {
        //Arrange
        var coordinator = new FakeCoordinator { RespondToConnect = (_, _) => TransactionResponses.Error(ServerError.NotAllowedError, "Transactions are not enabled.") };
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        await using var uut = CreateHandler(pool);
        var pending = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();

        //Act
        uut.Start();
        var exception = await Record.ExceptionAsync(() => uut.Connected.WaitAsync(_timeout, Current.CancellationToken));
        var pendingException = await Record.ExceptionAsync(() => pending.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<NotAllowedException>();
        pendingException.ShouldBeOfType<PulsarClientClosedException>();
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Closed);
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
        coordinator.RegistrationCount.ShouldBe(0);
        await coordinator.Connection.DidNotReceive().DisposeAsync();
        await pool.Received(1).FindConnectionForTopic(AssignPartitionTopic, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Connect_GivenTcClientConnectResponseWithError_ShouldRetryWithTheErrorMappedByTheHandler()
    {
        //Arrange
        var coordinator = new FakeCoordinator { RespondToConnect = (_, number) => number == 1 ? TransactionResponses.TcClientConnect(ServerError.TransactionCoordinatorNotFound, "not found") : TransactionResponses.TcClientConnect() };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));

        //Act
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        coordinator.ConnectRequests.Count.ShouldBe(2);
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);
    }

    // Upstream: TransactionMetaStoreHandler#connectionFailed (non-retriable error)
    [Fact]
    public async Task Connect_GivenNonRetriableErrorBeforeFirstConnect_ShouldFail()
    {
        //Arrange
        var error = new AuthorizationException("denied");
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException<IConnection>(error));
        await using var uut = CreateHandler(pool);
        var pending = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();

        //Act
        uut.Start();
        var exception = await Record.ExceptionAsync(() => uut.Connected.WaitAsync(_timeout, Current.CancellationToken));
        var pendingException = await Record.ExceptionAsync(() => pending.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeSameAs(error);
        pendingException.ShouldBeOfType<MetaStoreHandlerNotReadyException>();
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Failed);
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
        await pool.Received(1).FindConnectionForTopic(AssignPartitionTopic, Arg.Any<CancellationToken>());
    }

    // Upstream: TransactionMetaStoreHandler#connectionFailed (lookup timeout)
    [Fact]
    public async Task Connect_GivenRetriableErrorsUntilDeadline_ShouldFail()
    {
        //Arrange
        var error = new ServiceNotReadyException("not ready");
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException<IConnection>(error));
        await using var uut = CreateHandler(pool, operationTimeout: TimeSpan.FromMilliseconds(400));

        //Act
        uut.Start();
        var exception = await Record.ExceptionAsync(() => uut.Connected.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeSameAs(error);
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Failed);
        await pool.Received(Quantity.Within(2, int.MaxValue)).FindConnectionForTopic(AssignPartitionTopic, Arg.Any<CancellationToken>());
    }

    // Upstream: TransactionMetaStoreHandler#connectionFailed (only a failure before the first connect stops the handler)
    [Fact]
    public async Task Connect_GivenErrorAfterFirstConnect_ShouldKeepReconnecting()
    {
        //Arrange
        var first = new FakeCoordinator();
        var second = new FakeCoordinator();
        var calls = 0;
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Interlocked.Increment(ref calls) switch
        {
            1 => new ValueTask<IConnection>(first.Connection),
            <= 4 => ValueTask.FromException<IConnection>(new AuthorizationException("denied")),
            _ => new ValueTask<IConnection>(second.Connection)
        });
        await using var uut = CreateHandler(pool, operationTimeout: TimeSpan.FromMilliseconds(400));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        first.Drop();
        await WaitFor.Until(() => second.RegistrationCount == 1, "the handler is registered with the second connection");

        //Assert
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);
        uut.Connected.IsCompletedSuccessfully.ShouldBeTrue();
        calls.ShouldBe(5);
    }

    [Fact]
    public async Task Connect_GivenConnectionLost_ShouldUnregisterFromTheOldConnectionAndRegisterWithTheNewOne()
    {
        //Arrange
        var first = new FakeCoordinator();
        var second = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, first, second));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        first.Drop();
        await WaitFor.Until(() => second.RegistrationCount == 1, "the handler is registered with the second connection");

        //Assert
        first.RemovedRegistrations.ShouldBe([1UL]);
        second.ConnectRequests.Count.ShouldBe(1);
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);

        await uut.DisposeAsync();
        second.RemovedRegistrations.ShouldBe([1UL]);
    }

    [Fact]
    public async Task ConnectionClosed_GivenConnectionIsClosed_ShouldReconnect()
    {
        //Arrange
        var first = new FakeCoordinator();
        var second = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, first, second));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        first.Connection.State.IsFinalState().Returns(true);

        //Act
        uut.ConnectionClosed();
        await WaitFor.Until(() => second.RegistrationCount == 1, "the handler is registered with the second connection");

        //Assert
        first.RemovedRegistrations.ShouldBe([1UL]);
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);
    }

    [Fact]
    public async Task ConnectionClosed_GivenConnectionIsStillOpen_ShouldIgnoreNotification()
    {
        //Arrange
        var first = new FakeCoordinator();
        var second = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, first, second));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        uut.ConnectionClosed();
        await Task.Delay(TimeSpan.FromMilliseconds(500), Current.CancellationToken);

        //Assert
        second.RegistrationCount.ShouldBe(0);
        first.RemovedRegistrations.ShouldBeEmpty();
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Ready);
    }

    // Upstream: TransactionMetaStoreHandlerTest#testStateChangeFailure
    [Fact]
    public async Task Connect_GivenHandlerDisposedWhileConnecting_ShouldFaultConnectedAndNotRegister()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        var uut = CreateHandler(pool);
        ValueTask disposal = default;
        coordinator.RespondToConnect = (_, _) =>
        {
            disposal = uut.DisposeAsync(); // The state of the handler is no longer 'Connecting' when the response arrives
            return TransactionResponses.TcClientConnect();
        };

        //Act
        uut.Start();
        var exception = await Record.ExceptionAsync(() => uut.Connected.WaitAsync(_timeout, Current.CancellationToken));
        await disposal;

        //Assert
        exception.ShouldBeOfType<InvalidOperationException>();
        coordinator.RegistrationCount.ShouldBe(0);
        coordinator.Connection.DidNotReceive().AddTransactionMetaStoreHandler(Arg.Any<ITransactionMetaStoreHandler>());
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Closed);
    }

    // Upstream: TransactionMetaStoreHandlerTest#testFailedResendOnReconnectRemovesPendingRequest
    [Fact]
    public async Task Connect_GivenHandlerDisposedBeforePendingRequestIsResent_ShouldFailPendingRequestWithMetaStoreHandlerNotReadyException()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        var pending = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();
        await WaitFor.Until(() => uut.PendingRequestsCount == 1, "the request is pending");
        ValueTask disposal = default;
        coordinator.OnRegister = _ => disposal = uut.DisposeAsync(); // The handler is disposed while it registers with the connection, before the pending request is resent

        //Act
        uut.Start();
        var exception = await Record.ExceptionAsync(() => pending.WaitAsync(_timeout, Current.CancellationToken));
        await disposal;

        //Assert
        exception.ShouldBeOfType<MetaStoreHandlerNotReadyException>();
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
        coordinator.RequestsOf<CommandNewTxn>().ShouldBeEmpty();
    }

    [Fact]
    public async Task NewTransaction_GivenReadyHandler_ShouldSendCommandNewTxnWithMillisTtlAndTcId()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => TransactionResponses.NewTxn(3, 9) };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator), tcId: 3);
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var id = await uut.NewTransaction(TimeSpan.FromMilliseconds(200), Current.CancellationToken);

        //Assert
        id.ShouldBe(new TransactionId(3, 9));
        var command = coordinator.RequestsOf<CommandNewTxn>().ShouldHaveSingleItem();
        command.TcId.ShouldBe(3UL);
        command.TxnTtlMillis.ShouldBe(200UL);
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Fact]
    public async Task AddPublishPartitionToTxn_GivenReadyHandler_ShouldSendCommandWithTransactionIdAndPartition()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        await uut.AddPublishPartitionToTxn(new TransactionId(2, 3), "persistent://public/default/test-partition-0", Current.CancellationToken);

        //Assert
        var command = coordinator.RequestsOf<CommandAddPartitionToTxn>().ShouldHaveSingleItem();
        command.TxnidMostBits.ShouldBe(2UL);
        command.TxnidLeastBits.ShouldBe(3UL);
        command.Partitions.ShouldBe(["persistent://public/default/test-partition-0"]);
    }

    [Fact]
    public async Task AddSubscriptionToTxn_GivenReadyHandler_ShouldSendCommandWithTransactionIdAndSubscription()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        await uut.AddSubscriptionToTxn(new TransactionId(2, 3), "persistent://public/default/test", "my-subscription", Current.CancellationToken);

        //Assert
        var command = coordinator.RequestsOf<CommandAddSubscriptionToTxn>().ShouldHaveSingleItem();
        command.TxnidMostBits.ShouldBe(2UL);
        command.TxnidLeastBits.ShouldBe(3UL);
        var subscription = command.Subscription.ShouldHaveSingleItem();
        subscription.Topic.ShouldBe("persistent://public/default/test");
        subscription.Subscription_.ShouldBe("my-subscription");
    }

    [Theory]
    [InlineData(TxnAction.Commit)]
    [InlineData(TxnAction.Abort)]
    public async Task EndTxn_GivenReadyHandler_ShouldSendCommandWithTransactionIdAndAction(TxnAction action)
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        await uut.EndTxn(new TransactionId(2, 3), action, Current.CancellationToken);

        //Assert
        var command = coordinator.RequestsOf<CommandEndTxn>().ShouldHaveSingleItem();
        command.TxnidMostBits.ShouldBe(2UL);
        command.TxnidLeastBits.ShouldBe(3UL);
        command.TxnAction.ShouldBe(action);
    }

    [Fact]
    public async Task Request_GivenHandlerNotConnectedYet_ShouldBeSentAfterConnect()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => TransactionResponses.NewTxn(0, 11) };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        var pending = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();
        await WaitFor.Until(() => uut.PendingRequestsCount == 1, "the request is pending");
        coordinator.Requests.ShouldBeEmpty();

        //Act
        uut.Start();
        var id = await pending.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        id.ShouldBe(new TransactionId(0, 11));
        coordinator.RequestsOf<CommandNewTxn>().Count.ShouldBe(1);
    }

    // Upstream: TransactionMetaStoreHandler#handleNewTxnResponse (TransactionCoordinatorNotFound)
    [Fact]
    public async Task Request_GivenTransactionCoordinatorNotFoundTwiceThenSuccess_ShouldRetryWithBackoffAndSucceed()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, number) => number <= 2 ? TransactionResponses.NewTxn(ServerError.TransactionCoordinatorNotFound, "not found") : TransactionResponses.NewTxn(0, 7) };
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        await using var uut = CreateHandler(pool);
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var stopwatch = Stopwatch.StartNew();

        //Act
        var id = await uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        stopwatch.Elapsed.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(200)); // The retries wait at least 100 milliseconds and 200 milliseconds (minus the jitter)
        id.ShouldBe(new TransactionId(0, 7));
        coordinator.RequestsOf<CommandNewTxn>().Count.ShouldBe(3);
        coordinator.ConnectRequests.Count.ShouldBeGreaterThanOrEqualTo(2);
        await pool.Received(Quantity.Within(2, int.MaxValue)).FindConnectionForTopic(AssignPartitionTopic, Arg.Any<CancellationToken>());
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Theory]
    [InlineData(Request.NewTransaction)]
    [InlineData(Request.AddPublishPartitionToTxn)]
    [InlineData(Request.AddSubscriptionToTxn)]
    [InlineData(Request.EndTxn)]
    public async Task Request_GivenTransactionCoordinatorNotFound_ShouldRetryEveryRequestType(Request request)
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (command, number) => number == 1 ? TransactionResponses.Failure(command, ServerError.TransactionCoordinatorNotFound, "not found") : TransactionResponses.Success(command) };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => Send(uut, request).WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        coordinator.Requests.Count.ShouldBe(2);
        coordinator.Requests[1].ShouldBeSameAs(coordinator.Requests[0]);
    }

    [Theory]
    [InlineData(Request.NewTransaction, ServerError.InvalidTxnStatus, typeof(InvalidTransactionStatusException))]
    [InlineData(Request.AddPublishPartitionToTxn, ServerError.InvalidTxnStatus, typeof(InvalidTransactionStatusException))]
    [InlineData(Request.AddSubscriptionToTxn, ServerError.InvalidTxnStatus, typeof(InvalidTransactionStatusException))]
    [InlineData(Request.EndTxn, ServerError.InvalidTxnStatus, typeof(InvalidTransactionStatusException))]
    [InlineData(Request.EndTxn, ServerError.TransactionNotFound, typeof(TransactionNotFoundException))]
    [InlineData(Request.EndTxn, ServerError.NotAllowedError, typeof(TransactionCoordinatorClientException))]
    [InlineData(Request.EndTxn, ServerError.TransactionConflict, typeof(TransactionCoordinatorClientException))]
    [InlineData(Request.EndTxn, ServerError.UnknownError, typeof(TransactionCoordinatorClientException))]
    public async Task Request_GivenErrorResponse_ShouldThrowTheExceptionMappedByTheHandler(Request request, ServerError error, Type expectedException)
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (command, _) => TransactionResponses.Failure(command, error, "the message") };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => Send(uut, request).WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldNotBeNull().GetType().ShouldBe(expectedException);
        exception.Message.ShouldBe("the message");
        coordinator.Requests.Count.ShouldBe(1);
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Fact]
    public async Task Request_GivenGenericErrorResponse_ShouldThrowTheExceptionMappedByTheClient()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => TransactionResponses.Error(ServerError.NotAllowedError, "Transactions are not enabled.") };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.EndTxn(_transactionId, TxnAction.Commit, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<NotAllowedException>();
        uut.PendingRequestsCount.ShouldBe(0);
    }

    [Fact]
    public async Task Request_GivenResponseOfAnotherType_ShouldThrowUnexpectedResponseException()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => TransactionResponses.AddPartitionToTxn() };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<UnexpectedResponseException>();
        uut.PendingRequestsCount.ShouldBe(0);
    }

    // Upstream: TransactionMetaStoreHandler#run (timeout sweep)
    [Fact]
    public async Task Request_GivenNoResponseWithinOperationTimeout_ShouldThrowOperationTimeoutException()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator), operationTimeout: TimeSpan.FromMilliseconds(300));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        exception.Message.ShouldStartWith("Create new transaction 0 failed due to timeout. connection: ");
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
        coordinator.PendingRequestCount.ShouldBe(0);
    }

    [Fact]
    public async Task Request_GivenNoResponseWithinOperationTimeout_ShouldNameTheRequestInTheException()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator), operationTimeout: TimeSpan.FromMilliseconds(300));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        var partition = await Record.ExceptionAsync(() => uut.AddPublishPartitionToTxn(_transactionId, "my-topic", Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        var subscription = await Record.ExceptionAsync(() => uut.AddSubscriptionToTxn(_transactionId, "my-topic", "my-subscription", Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        var end = await Record.ExceptionAsync(() => uut.EndTxn(_transactionId, TxnAction.Abort, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        partition.ShouldBeOfType<OperationTimeoutException>().Message.ShouldStartWith("Add partition [my-topic] to TXN (1,1) failed due to timeout.");
        subscription.ShouldBeOfType<OperationTimeoutException>().Message.ShouldStartWith("Add subscription [my-topic my-subscription] to TXN (1,1) failed due to timeout.");
        end.ShouldBeOfType<OperationTimeoutException>().Message.ShouldStartWith("End [ABORT] TXN (1,1) failed due to timeout.");
    }

    // Upstream: TransactionMetaStoreHandlerTest#testTimeoutReleasesCancelledPendingRequests
    [Fact]
    public async Task Request_GivenCancelledRequestAndNoResponse_ShouldBeReleasedByTheTimeoutSweep()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator), operationTimeout: TimeSpan.FromSeconds(1));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        using var cts = new CancellationTokenSource();
        var commit = uut.EndTxn(_transactionId, TxnAction.Commit, cts.Token).AsTask();
        await cts.CancelAsync();

        //Act
        await WaitFor.Until(() => coordinator.Requests.Count == 1, "the request is sent");

        //Assert
        await WaitFor.Until(() => uut.PendingRequestsCount == 0 && uut.AvailableRequestPermits == MaxPendingOperations, "the request is released");
        await Should.ThrowAsync<OperationCanceledException>(commit);
    }

    [Fact]
    public async Task Request_GivenConnectionLostBeforeResponse_ShouldResendOnNewConnection()
    {
        //Arrange
        var first = new FakeCoordinator { Respond = (_, _) => null };
        var second = new FakeCoordinator { Respond = (_, _) => TransactionResponses.NewTxn(0, 42) };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, first, second));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var pending = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();
        await WaitFor.Until(() => first.Requests.Count == 1, "the request is sent");

        //Act
        first.Drop();
        var id = await pending.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        id.ShouldBe(new TransactionId(0, 42));
        second.Requests.ShouldHaveSingleItem().ShouldBeSameAs(first.Requests.ShouldHaveSingleItem());
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Fact]
    public async Task Request_GivenReconnectWhileRequestIsInFlight_ShouldNotSendItAgainOnTheSameConnection()
    {
        //Arrange
        var coordinator = new FakeCoordinator
        {
            Respond = (command, number) => command switch
            {
                CommandNewTxn => null,
                _ when number == 1 => TransactionResponses.Failure(command, ServerError.TransactionCoordinatorNotFound, "not found"),
                _ => TransactionResponses.Success(command)
            }
        };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var newTransaction = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();
        await WaitFor.Until(() => coordinator.RequestsOf<CommandNewTxn>().Count == 1, "the new transaction request is sent");

        //Act
        var addPartition = uut.AddPublishPartitionToTxn(_transactionId, "my-topic", Current.CancellationToken).AsTask(); // The coordinator is not found, which makes the handler reconnect
        await WaitFor.Until(() => coordinator.RegistrationCount == 2, "the handler has reconnected");
        await addPartition.WaitAsync(_timeout, Current.CancellationToken);
        var request = coordinator.RequestsOf<CommandNewTxn>().ShouldHaveSingleItem();
        coordinator.Answer(request, TransactionResponses.NewTxn(0, 5));
        var id = await newTransaction.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        id.ShouldBe(new TransactionId(0, 5));
        coordinator.RequestsOf<CommandNewTxn>().Count.ShouldBe(1);
        coordinator.RequestsOf<CommandAddPartitionToTxn>().Count.ShouldBe(2);
    }

    [Fact]
    public async Task Request_GivenTransactionCoordinatorNotFoundThreeTimes_ShouldSendOncePerResponse()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (command, number) => number <= 3 ? TransactionResponses.Failure(command, ServerError.TransactionCoordinatorNotFound, "not found") : TransactionResponses.Success(command) };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        await uut.EndTxn(_transactionId, TxnAction.Commit, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        coordinator.RequestsOf<CommandEndTxn>().Count.ShouldBe(4); // One send for each response
    }

    // Upstream: TransactionMetaStoreHandler#canSendRequest (Semaphore(1000))
    [Fact]
    public async Task Request_Given1000PendingOps_ShouldWaitForPermit()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var pending = Enumerable.Range(0, MaxPendingOperations).Select(i => uut.EndTxn(new TransactionId(0, (ulong) i), TxnAction.Commit, Current.CancellationToken).AsTask()).ToArray();
        await WaitFor.Until(() => coordinator.Requests.Count == MaxPendingOperations, "1000 requests are sent");
        uut.AvailableRequestPermits.ShouldBe(0);

        //Act
        var waiting = uut.EndTxn(new TransactionId(0, MaxPendingOperations), TxnAction.Commit, Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(200), Current.CancellationToken);
        var sentWhileWaiting = coordinator.Requests.Count;
        coordinator.Answer(coordinator.Requests[0], TransactionResponses.EndTxn());
        await pending[0].WaitAsync(_timeout, Current.CancellationToken);
        await WaitFor.Until(() => coordinator.Requests.Count == MaxPendingOperations + 1, "the request that waited for a permit is sent");

        //Assert
        sentWhileWaiting.ShouldBe(MaxPendingOperations);
        waiting.IsCompleted.ShouldBeFalse();
        uut.PendingRequestsCount.ShouldBe(MaxPendingOperations);
        uut.AvailableRequestPermits.ShouldBe(0);

        coordinator.Answer(coordinator.Requests[MaxPendingOperations], TransactionResponses.EndTxn());
        await waiting.WaitAsync(_timeout, Current.CancellationToken);

        await uut.DisposeAsync();
        await Task.WhenAll(pending.Skip(1).Select(task => Record.ExceptionAsync(() => task).AsTask()));
    }

    [Fact]
    public async Task DisposeAsync_GivenCallerWaitingForPermit_ShouldFailItWithPulsarClientClosedException()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var pending = Enumerable.Range(0, MaxPendingOperations).Select(i => uut.EndTxn(new TransactionId(0, (ulong) i), TxnAction.Commit, Current.CancellationToken).AsTask()).ToArray();
        await WaitFor.Until(() => uut.AvailableRequestPermits == 0, "all the permits are taken");
        var waiting = uut.EndTxn(new TransactionId(0, MaxPendingOperations), TxnAction.Commit, Current.CancellationToken).AsTask();

        //Act
        await uut.DisposeAsync();

        //Assert
        await Should.ThrowAsync<PulsarClientClosedException>(waiting.WaitAsync(_timeout, Current.CancellationToken));
        foreach (var task in pending)
            await Should.ThrowAsync<PulsarClientClosedException>(task.WaitAsync(_timeout, Current.CancellationToken));
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    // Upstream: TransactionMetaStoreHandlerTest#testCloseFailsAndReleasesPendingRequests
    [Fact]
    public async Task DisposeAsync_GivenPendingEndTxnRequest_ShouldFailItWithPulsarClientClosedExceptionAndReleaseIt()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var commit = uut.EndTxn(_transactionId, TxnAction.Commit, Current.CancellationToken).AsTask();
        await WaitFor.Until(() => coordinator.Requests.Count == 1, "the request is sent");
        var request = coordinator.RequestsOf<CommandEndTxn>().ShouldHaveSingleItem();

        //Assert
        request.TxnAction.ShouldBe(TxnAction.Commit);
        request.TxnidMostBits.ShouldBe(1UL);
        request.TxnidLeastBits.ShouldBe(1UL);
        uut.PendingRequestsCount.ShouldBe(1);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations - 1);
        commit.IsCompleted.ShouldBeFalse();

        //Act
        await uut.DisposeAsync();

        //Assert
        await Should.ThrowAsync<PulsarClientClosedException>(commit.WaitAsync(_timeout, Current.CancellationToken));
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Closed);
        coordinator.PendingRequestCount.ShouldBe(0);
        coordinator.RemovedRegistrations.ShouldBe([1UL]);
    }

    [Fact]
    public async Task DisposeAsync_GivenPendingOps_ShouldFailThemAndReleasePermits()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var pending = new[]
        {
            Send(uut, Request.NewTransaction),
            Send(uut, Request.AddPublishPartitionToTxn),
            Send(uut, Request.AddSubscriptionToTxn),
            Send(uut, Request.EndTxn)
        };
        await WaitFor.Until(() => uut.PendingRequestsCount == 4, "all the requests are pending");

        //Act
        await uut.DisposeAsync();

        //Assert
        foreach (var task in pending)
        {
            var exception = await Record.ExceptionAsync(() => task.WaitAsync(_timeout, Current.CancellationToken));
            exception.ShouldBeOfType<PulsarClientClosedException>().Message.ShouldBe("Could not get response from transaction meta store when the transaction meta store has already close.");
        }

        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    // Upstream: TransactionMetaStoreHandlerTest#testCloseReleasesCancelledPendingRequests
    [Fact]
    public async Task DisposeAsync_GivenCancelledPendingRequest_ShouldReleaseIt()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        using var cts = new CancellationTokenSource();
        var commit = uut.EndTxn(_transactionId, TxnAction.Commit, cts.Token).AsTask();
        await WaitFor.Until(() => coordinator.Requests.Count == 1, "the request is sent");
        uut.PendingRequestsCount.ShouldBe(1);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations - 1);

        //Act
        await cts.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(commit.WaitAsync(_timeout, Current.CancellationToken));
        var pendingAfterCancel = uut.PendingRequestsCount;
        await uut.DisposeAsync();

        //Assert
        pendingAfterCancel.ShouldBe(1); // The request is still pending, and it still holds its permit
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    // Upstream: TransactionMetaStoreHandlerTest#testClientCloseFailsPendingRequestsQueuedBehindBusyExecutor
    [Fact]
    public async Task DisposeAsync_GivenConnectionPoolDisposedRightAfter_ShouldStillFailAndReleasePendingRequest()
    {
        //Arrange
        var coordinator = new FakeCoordinator { Respond = (_, _) => null };
        var pool = FakeCoordinator.CreatePool(1, coordinator);
        var uut = CreateHandler(pool);
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        var commit = uut.EndTxn(_transactionId, TxnAction.Commit, Current.CancellationToken).AsTask();
        await WaitFor.Until(() => coordinator.Requests.Count == 1, "the request is sent");

        //Act
        var disposal = uut.DisposeAsync().AsTask();
        await pool.DisposeAsync();
        await disposal;

        //Assert
        await Should.ThrowAsync<PulsarClientClosedException>(commit.WaitAsync(_timeout, Current.CancellationToken));
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Fact]
    public async Task DisposeAsync_GivenHandlerNeverConnected_ShouldFailPendingRequestAndFaultConnected()
    {
        //Arrange
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(callInfo => new ValueTask<IConnection>(WaitFor.Forever<IConnection>(callInfo.Arg<CancellationToken>())));
        var uut = CreateHandler(pool);
        var pending = uut.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask();
        uut.Start();
        await WaitFor.Until(() => pool.ReceivedCalls().Any(), "the handler looks up the coordinator");

        //Act
        await uut.DisposeAsync();

        //Assert
        await Should.ThrowAsync<PulsarClientClosedException>(pending.WaitAsync(_timeout, Current.CancellationToken));
        await Should.ThrowAsync<PulsarClientClosedException>(uut.Connected.WaitAsync(_timeout, Current.CancellationToken));
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Closed);
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Fact]
    public async Task DisposeAsync_GivenHandlerNotStarted_ShouldCloseHandler()
    {
        //Arrange
        var uut = CreateHandler(Substitute.For<IConnectionPool>());

        //Act
        await uut.DisposeAsync();

        //Assert
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Closed);
        await Should.ThrowAsync<PulsarClientClosedException>(uut.Connected.WaitAsync(_timeout, Current.CancellationToken));
    }

    [Fact]
    public async Task DisposeAsync_GivenCalledTwice_ShouldBeNoOp()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        await uut.DisposeAsync();
        var exception = await Record.ExceptionAsync(() => uut.DisposeAsync().AsTask());

        //Assert
        exception.ShouldBeNull();
        coordinator.RemovedRegistrations.ShouldBe([1UL]);
    }

    [Theory]
    [InlineData(Request.NewTransaction)]
    [InlineData(Request.AddPublishPartitionToTxn)]
    [InlineData(Request.AddSubscriptionToTxn)]
    [InlineData(Request.EndTxn)]
    public async Task Request_GivenDisposedHandler_ShouldThrowPulsarClientClosedExceptionAndSendNothing(Request request)
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        await uut.DisposeAsync();

        //Act
        var exception = await Record.ExceptionAsync(() => Send(uut, request).WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<PulsarClientClosedException>().Message.ShouldBe("Could not send the request to the transaction meta store: the client is already closed.");
        coordinator.Requests.ShouldBeEmpty();
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    // Upstream: TransactionMetaStoreHandlerTest#testClientCloseFailsRequestsQueuedBeforeBeingSent
    [Fact]
    public async Task EndTxn_GivenClosedHandler_ShouldThrowMetaStoreHandlerNotReadyExceptionAndSendNothing()
    {
        //Arrange
        var coordinator = new FakeCoordinator { RespondToConnect = (_, _) => TransactionResponses.Error(ServerError.NotAllowedError, "Transactions are not enabled.") };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await Should.ThrowAsync<NotAllowedException>(uut.Connected.WaitAsync(_timeout, Current.CancellationToken)); // The handler is closed, but not disposed

        //Act
        var exception = await Record.ExceptionAsync(() => uut.EndTxn(_transactionId, TxnAction.Commit, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<MetaStoreHandlerNotReadyException>().Message.ShouldBe("Transaction meta store handler for tcId 0 is closing or closed.");
        coordinator.Requests.ShouldBeEmpty();
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    // Upstream: TransactionClientConnectTest#testPulsarClientCloseThenCloseTcClient
    [Fact]
    public async Task NewTransaction_GivenHandlerClosedAfterReconnect_ShouldThrowMetaStoreHandlerNotReadyException()
    {
        //Arrange
        var first = new FakeCoordinator { Respond = (_, _) => TransactionResponses.NewTxn(0, 0) };
        var second = new FakeCoordinator { RespondToConnect = (_, _) => TransactionResponses.Error(ServerError.NotAllowedError, "Transactions are not enabled.") };
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, first, second));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        await uut.NewTransaction(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Act
        first.Drop();
        await uut.State.OnStateChangeTo(TransactionMetaStoreHandlerState.Closed, Current.CancellationToken);
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromSeconds(10), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        uut.CurrentState.ShouldBe(TransactionMetaStoreHandlerState.Closed);
        exception.ShouldBeOfType<MetaStoreHandlerNotReadyException>();
    }

    [Fact]
    public async Task Request_GivenCallerCancellationBeforeSend_ShouldThrowOperationCanceledExceptionAndSendNothing()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = CreateHandler(FakeCoordinator.CreatePool(1, coordinator));
        uut.Start();
        await uut.Connected.WaitAsync(_timeout, Current.CancellationToken);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction(TimeSpan.FromMinutes(1), cts.Token).AsTask());

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        coordinator.Requests.ShouldBeEmpty();
        uut.PendingRequestsCount.ShouldBe(0);
        uut.AvailableRequestPermits.ShouldBe(MaxPendingOperations);
    }

    [Theory]
    [InlineData(ServerError.TransactionCoordinatorNotFound, typeof(TransactionCoordinatorNotFoundException))]
    [InlineData(ServerError.InvalidTxnStatus, typeof(InvalidTransactionStatusException))]
    [InlineData(ServerError.TransactionNotFound, typeof(TransactionNotFoundException))]
    [InlineData(ServerError.NotAllowedError, typeof(TransactionCoordinatorClientException))]
    [InlineData(ServerError.TransactionConflict, typeof(TransactionCoordinatorClientException))]
    [InlineData(ServerError.UnknownError, typeof(TransactionCoordinatorClientException))]
    public void GetExceptionByServerError_GivenServerError_ShouldReturnMatchingException(ServerError error, Type expectedException)
    {
        //Act
        var exception = TransactionMetaStoreHandler.GetExceptionByServerError(error, "the message");

        //Assert
        exception.GetType().ShouldBe(expectedException);
        exception.Message.ShouldBe("the message");
    }

    private static TransactionMetaStoreHandler CreateHandler(IConnectionPool pool, ulong tcId = 0, TimeSpan? operationTimeout = null)
        => new(tcId, tcId == 0 ? AssignPartitionTopic : $"persistent://pulsar/system/transaction_coordinator_assign-partition-{tcId}", pool, operationTimeout ?? TimeSpan.FromSeconds(30));

    private static Task Send(ITransactionMetaStoreHandler handler, Request request)
        => request switch
        {
            Request.NewTransaction => handler.NewTransaction(TimeSpan.FromMinutes(1), Current.CancellationToken).AsTask(),
            Request.AddPublishPartitionToTxn => handler.AddPublishPartitionToTxn(_transactionId, "my-topic", Current.CancellationToken).AsTask(),
            Request.AddSubscriptionToTxn => handler.AddSubscriptionToTxn(_transactionId, "my-topic", "my-subscription", Current.CancellationToken).AsTask(),
            _ => handler.EndTxn(_transactionId, TxnAction.Commit, Current.CancellationToken).AsTask()
        };
}
