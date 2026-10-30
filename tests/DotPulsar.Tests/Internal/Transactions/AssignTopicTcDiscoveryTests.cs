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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class AssignTopicTcDiscoveryTests
{
    private static readonly TimeSpan _operationTimeout = TimeSpan.FromSeconds(30);

    // Upstream: TransactionClientConnectTest#testNotEnableTransactionInBroker (AssignTopicTcDiscovery#start)
    [Fact]
    public async Task Start_GivenZeroPartitions_ShouldThrowTransactionCoordinatorClientException()
    {
        //Arrange
        var pool = CreatePool(0);
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(pool, _operationTimeout, factory.Create);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientException>().Message.ShouldBe("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized");
        factory.Handlers.ShouldBeEmpty();
        uut.Handlers.ShouldBeEmpty();
        await pool.DidNotReceive().FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Start_GivenSixteenPartitions_ShouldCreateHandlerPerPartitionWithAssignTopicNames()
    {
        //Arrange
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(16), _operationTimeout, factory.Create);

        //Act
        await uut.Start(Current.CancellationToken);

        //Assert
        factory.Topics.Count.ShouldBe(16);
        for (var i = 0; i < 16; ++i)
        {
            factory.Topics[(ulong) i].ShouldBe($"persistent://pulsar/system/transaction_coordinator_assign-partition-{i}");
            factory.Handlers[i].Received(1).Start();
        }

        uut.Handlers.ShouldBe(factory.Handlers);
    }

    [Fact]
    public async Task Start_GivenNoHandlerFactory_ShouldCreateTransactionMetaStoreHandlers()
    {
        //Arrange
        var coordinator = new FakeCoordinator();
        await using var uut = new AssignTopicTcDiscovery(FakeCoordinator.CreatePool(2, coordinator), _operationTimeout);

        //Act
        await uut.Start(Current.CancellationToken);

        //Assert
        uut.Handlers.Count.ShouldBe(2);
        uut.Handlers.ShouldAllBe(handler => handler is TransactionMetaStoreHandler && handler.CurrentState == TransactionMetaStoreHandlerState.Ready);
        coordinator.ConnectRequests.Select(request => request.TcId).Order().ShouldBe([0UL, 1UL]);
    }

    [Fact]
    public async Task Start_GivenHandlersNotConnected_ShouldWaitForEveryHandler()
    {
        //Arrange
        var factory = new HandlerFactory();
        var connected = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var uut = new AssignTopicTcDiscovery(CreatePool(3), _operationTimeout, (tcId, topic) => factory.Create(tcId, topic, tcId == 2 ? connected.Task : Task.CompletedTask));

        //Act
        var start = uut.Start(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedBeforeLastHandlerIsConnected = start.IsCompleted;
        connected.SetResult(null);
        await start.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        isCompletedBeforeLastHandlerIsConnected.ShouldBeFalse();
    }

    [Fact]
    public async Task Start_GivenHandlerConnectFails_ShouldThrow()
    {
        //Arrange
        var error = new AuthorizationException("denied");
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(2), _operationTimeout, (tcId, topic) => factory.Create(tcId, topic, tcId == 1 ? Task.FromException(error) : Task.CompletedTask));

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeSameAs(error);
    }

    [Fact]
    public async Task Start_GivenCancellationWhileWaitingForHandlers_ShouldThrowOperationCanceledException()
    {
        //Arrange
        var factory = new HandlerFactory();
        using var cts = new CancellationTokenSource();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(1), _operationTimeout, (tcId, topic) => factory.Create(tcId, topic, new TaskCompletionSource<object?>().Task));
        var start = uut.Start(cts.Token).AsTask();

        //Act
        await cts.CancelAsync();
        var exception = await Record.ExceptionAsync(() => start.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken));

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
    }

    // Upstream: AssignTopicTcDiscovery#nextHandler
    [Fact]
    public async Task NextHandler_GivenSixteenHandlers_ShouldStartAtIndexOneAndRoundRobin()
    {
        //Arrange
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(16), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        var handlers = Enumerable.Range(0, 33).Select(_ => uut.NextHandler()).ToArray();

        //Assert
        for (var i = 0; i < handlers.Length; ++i)
            handlers[i].ShouldBeSameAs(factory.Handlers[(i + 1) % 16]);
    }

    [Fact]
    public async Task NextHandler_GivenOneHandler_ShouldAlwaysReturnIt()
    {
        //Arrange
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(1), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        var handlers = Enumerable.Range(0, 5).Select(_ => uut.NextHandler()).ToArray();

        //Assert
        handlers.ShouldAllBe(handler => ReferenceEquals(handler, factory.Handlers[0]));
    }

    [Fact]
    public async Task NextHandler_GivenNotStarted_ShouldReturnNull()
    {
        //Arrange
        await using var uut = new AssignTopicTcDiscovery(CreatePool(1), _operationTimeout, new HandlerFactory().Create);

        //Act
        var handler = uut.NextHandler();

        //Assert
        handler.ShouldBeNull();
    }

    [Fact]
    public async Task NextHandler_GivenConcurrentCallers_ShouldDistributeEvenly()
    {
        //Arrange
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(4), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        var handlers = await Task.WhenAll(Enumerable.Range(0, 400).Select(_ => Task.Run(() => uut.NextHandler())));

        //Assert
        foreach (var handler in factory.Handlers)
            handlers.Count(h => ReferenceEquals(h, handler)).ShouldBe(100);
    }

    // Upstream: AssignTopicTcDiscovery#handlerForCoordinator
    [Fact]
    public async Task HandlerForCoordinator_GivenKnownId_ShouldReturnHandlerOfThatCoordinator()
    {
        //Arrange
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(3), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        var handler = uut.HandlerForCoordinator(2);

        //Assert
        handler.ShouldBeSameAs(factory.Handlers[2]);
    }

    [Fact]
    public async Task HandlerForCoordinator_GivenUnknownId_ShouldReturnNull()
    {
        //Arrange
        var factory = new HandlerFactory();
        await using var uut = new AssignTopicTcDiscovery(CreatePool(3), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        var handler = uut.HandlerForCoordinator(3);
        var maxHandler = uut.HandlerForCoordinator(ulong.MaxValue);

        //Assert
        handler.ShouldBeNull();
        maxHandler.ShouldBeNull();
    }

    [Fact]
    public async Task HandlerForCoordinator_GivenNotStarted_ShouldReturnNull()
    {
        //Arrange
        await using var uut = new AssignTopicTcDiscovery(CreatePool(1), _operationTimeout, new HandlerFactory().Create);

        //Act
        var handler = uut.HandlerForCoordinator(0);

        //Assert
        handler.ShouldBeNull();
    }

    // Upstream: AssignTopicTcDiscovery#close
    [Fact]
    public async Task DisposeAsync_ShouldDisposeAllHandlers()
    {
        //Arrange
        var factory = new HandlerFactory();
        var uut = new AssignTopicTcDiscovery(CreatePool(3), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.DisposeAsync();

        //Assert
        foreach (var handler in factory.Handlers)
            await handler.Received(1).DisposeAsync();

        uut.Handlers.ShouldBeEmpty();
        uut.NextHandler().ShouldBeNull();
        uut.HandlerForCoordinator(0).ShouldBeNull();
    }

    [Fact]
    public async Task DisposeAsync_GivenHandlerThatFailsToDispose_ShouldStillDisposeTheOtherHandlers()
    {
        //Arrange
        var factory = new HandlerFactory();
        var uut = new AssignTopicTcDiscovery(CreatePool(3), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);
        factory.Handlers[0].DisposeAsync().Returns(_ => ValueTask.FromException(new InvalidOperationException()));

        //Act
        var exception = await Record.ExceptionAsync(() => uut.DisposeAsync().AsTask());

        //Assert
        exception.ShouldBeNull();
        await factory.Handlers[1].Received(1).DisposeAsync();
        await factory.Handlers[2].Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_GivenStartFailed_ShouldDisposeTheHandlersThatWereCreated()
    {
        //Arrange
        var factory = new HandlerFactory();
        var uut = new AssignTopicTcDiscovery(CreatePool(2), _operationTimeout, (tcId, topic) => factory.Create(tcId, topic, Task.FromException(new AuthorizationException("denied"))));
        _ = await Record.ExceptionAsync(() => uut.Start(Current.CancellationToken).AsTask());

        //Act
        await uut.DisposeAsync();

        //Assert
        factory.Handlers.Count.ShouldBe(2);
        foreach (var handler in factory.Handlers)
            await handler.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_GivenCalledTwice_ShouldDisposeHandlersOnce()
    {
        //Arrange
        var factory = new HandlerFactory();
        var uut = new AssignTopicTcDiscovery(CreatePool(1), _operationTimeout, factory.Create);
        await uut.Start(Current.CancellationToken);

        //Act
        await uut.DisposeAsync();
        await uut.DisposeAsync();

        //Assert
        await factory.Handlers[0].Received(1).DisposeAsync();
    }

    private static IConnectionPool CreatePool(uint partitions)
    {
        var pool = Substitute.For<IConnectionPool>();
        pool.GetNumberOfPartitions(Constants.TransactionCoordinatorAssignTopic, Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(partitions));
        return pool;
    }

    private sealed class HandlerFactory
    {
        public List<ITransactionMetaStoreHandler> Handlers { get; } = [];

        public Dictionary<ulong, string> Topics { get; } = [];

        public ITransactionMetaStoreHandler Create(ulong tcId, string topic)
            => Create(tcId, topic, Task.CompletedTask);

        public ITransactionMetaStoreHandler Create(ulong tcId, string topic, Task connected)
        {
            var handler = Substitute.For<ITransactionMetaStoreHandler>();
            handler.Connected.Returns(connected);
            Handlers.Add(handler);
            Topics[tcId] = topic;
            return handler;
        }
    }
}
