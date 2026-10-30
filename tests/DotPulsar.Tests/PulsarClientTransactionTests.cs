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

namespace DotPulsar.Tests;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class PulsarClientTransactionTests
{
    private static readonly Uri _serviceUrl = new("pulsar://localhost:6650");
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    // Additional coverage
    [Fact]
    public async Task Build_GivenEnableTransaction_ShouldExposeNotStartedTransactionCoordinatorClient()
    {
        //Arrange
        await using var uut = (PulsarClient) PulsarClient.Builder().ServiceUrl(_serviceUrl).EnableTransaction(true).Build();

        //Act
        var transactionCoordinatorClient = uut.TransactionCoordinatorClient;

        //Assert
        var client = transactionCoordinatorClient.ShouldBeOfType<TransactionCoordinatorClient>();
        client.State.ShouldBe(TransactionCoordinatorClientState.None);
    }

    // Additional coverage
    [Fact]
    public async Task Build_GivenTransactionsNotEnabled_ShouldHaveNullTransactionCoordinatorClient()
    {
        //Arrange
        await using var uut = (PulsarClient) PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();

        //Act
        var transactionCoordinatorClient = uut.TransactionCoordinatorClient;

        //Assert
        transactionCoordinatorClient.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task Build_GivenTransactionsExplicitlyDisabledAfterEnabled_ShouldHaveNullTransactionCoordinatorClient()
    {
        //Arrange
        await using var uut = (PulsarClient) PulsarClient.Builder().ServiceUrl(_serviceUrl).EnableTransaction(true).EnableTransaction(false).Build();

        //Act
        var transactionCoordinatorClient = uut.TransactionCoordinatorClient;

        //Assert
        transactionCoordinatorClient.ShouldBeNull();
    }

    // Additional coverage
    [Theory]
    [InlineData(0)]
    [InlineData(-1000)]
    public void Build_GivenZeroOrNegativeOperationTimeout_ShouldThrowConfigurationException(int milliseconds)
    {
        //Arrange
        var uut = PulsarClient.Builder().ServiceUrl(_serviceUrl).EnableTransaction(true).OperationTimeout(TimeSpan.FromMilliseconds(milliseconds));

        //Act
        var exception = Record.Exception(uut.Build);

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("OperationTimeout must be greater than zero");
    }

    // Additional coverage
    [Fact]
    public void EnableTransaction_ShouldReturnTheSameBuilder()
    {
        //Arrange
        var uut = PulsarClient.Builder();

        //Act
        var result = uut.EnableTransaction(true);

        //Assert
        result.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_GivenTransactionsNotEnabled_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var uut = CreateClient(null);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.CreateTransaction(new TransactionOptions(), Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("Transactions are not enabled");
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_ShouldStartTheCoordinatorClientAndCreateTransactionWithIdAndTimeoutFromTheCoordinator()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.NewTransaction(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(2, 7)));
        await using var uut = CreateClient(tcClient);

        //Act
        var transaction = await uut.CreateTransaction(new TransactionOptions { Timeout = TimeSpan.FromSeconds(30) }, Current.CancellationToken);

        //Assert
        transaction.ShouldBeOfType<Transaction>();
        transaction.Id.ShouldBe(new TransactionId(2, 7));
        transaction.CurrentState.ShouldBe(TransactionState.Open);
        Received.InOrder(() =>
        {
            tcClient.EnsureStarted(Current.CancellationToken);
            tcClient.NewTransaction(TimeSpan.FromSeconds(30), Current.CancellationToken);
        });
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_GivenDefaultOptions_ShouldCreateTransactionWithSixtySecondsInMilliseconds()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        await using var uut = CreateClient(tcClient);

        //Act
        _ = await uut.NewTransaction().Create(Current.CancellationToken);

        //Assert
        await tcClient.Received(1).NewTransaction(TimeSpan.FromMilliseconds(60000), Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_ShouldReturnTransactionThatEndsWithTheCoordinatorClient()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.NewTransaction(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(0, 9)));
        await using var uut = CreateClient(tcClient);
        var committed = await uut.NewTransaction().Create(Current.CancellationToken);
        var aborted = await uut.NewTransaction().Create(Current.CancellationToken);

        //Act
        await committed.Commit(Current.CancellationToken);
        await aborted.Abort(Current.CancellationToken);

        //Assert
        committed.CurrentState.ShouldBe(TransactionState.Committed);
        aborted.CurrentState.ShouldBe(TransactionState.Aborted);
        await tcClient.Received(1).Commit(new TransactionId(0, 9), CancellationToken.None);
        await tcClient.Received(1).Abort(new TransactionId(0, 9), CancellationToken.None);
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_GivenFailedStartThenCoordinatorAvailable_ShouldSucceedOnRetry()
    {
        //Arrange
        var handler = Substitute.For<ITransactionMetaStoreHandler>();
        handler.NewTransaction(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(0, 1)));
        var failing = Substitute.For<ITcDiscovery>();
        failing.Start(Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new TransactionCoordinatorClientException("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized")));
        var working = Substitute.For<ITcDiscovery>();
        working.Start(Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);
        working.NextHandler().Returns(handler);
        var discoveries = new Queue<ITcDiscovery>(new[] { failing, working });
        var tcClient = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), TimeSpan.FromSeconds(30), () => discoveries.Dequeue());
        await using var uut = CreateClient(tcClient);

        //Act
        var first = await Record.ExceptionAsync(() => uut.NewTransaction().Create(Current.CancellationToken).AsTask());
        var second = await uut.NewTransaction().Create(Current.CancellationToken);

        //Assert
        first.ShouldBeOfType<TransactionCoordinatorClientException>();
        second.Id.ShouldBe(new TransactionId(0, 1));
        tcClient.State.ShouldBe(TransactionCoordinatorClientState.Ready);
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_GivenConcurrentCalls_ShouldStartTheCoordinatorClientOnce()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = Substitute.For<ITransactionMetaStoreHandler>();
        handler.NewTransaction(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(0, 1)));
        var discovery = Substitute.For<ITcDiscovery>();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        discovery.NextHandler().Returns(handler);
        var factoryCalls = 0;
        var tcClient = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), TimeSpan.FromSeconds(30), () =>
        {
            Interlocked.Increment(ref factoryCalls);
            return discovery;
        });
        await using var uut = CreateClient(tcClient);
        var creations = Enumerable.Range(0, 10).Select(_ => Task.Run(() => uut.NewTransaction().Create(Current.CancellationToken).AsTask())).ToArray();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);

        //Act
        start.SetResult(null);
        var transactions = await Task.WhenAll(creations).WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        transactions.Length.ShouldBe(10);
        factoryCalls.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public async Task CreateTransaction_GivenCancellationWhileStarting_ShouldThrowOperationCanceledExceptionAndNotCancelTheStart()
    {
        //Arrange
        var start = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = Substitute.For<ITransactionMetaStoreHandler>();
        handler.NewTransaction(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(0, 1)));
        var discovery = Substitute.For<ITcDiscovery>();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(new ValueTask(start.Task));
        discovery.NextHandler().Returns(handler);
        var tcClient = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), TimeSpan.FromSeconds(30), () => discovery);
        await using var uut = CreateClient(tcClient);
        using var cts = new CancellationTokenSource();
        var cancelled = uut.NewTransaction().Create(cts.Token).AsTask();

        //Act
        await cts.CancelAsync();
        var exception = await Record.ExceptionAsync(() => cancelled.WaitAsync(_timeout, Current.CancellationToken));
        start.SetResult(null);
        var transaction = await uut.NewTransaction().Create(Current.CancellationToken);

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        transaction.Id.ShouldBe(new TransactionId(0, 1));
        await discovery.Received(1).Start(Arg.Any<CancellationToken>());
    }

    // Upstream: TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose
    [Fact]
    public async Task CreateTransaction_GivenDisposedClient_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var tcClient = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), TimeSpan.FromSeconds(30), () => Substitute.For<ITcDiscovery>());
        var uut = CreateClient(tcClient);
        await uut.DisposeAsync();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.NewTransaction().Create(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>();
        uut.TransactionCoordinatorClient.ShouldBeSameAs(tcClient);
        tcClient.State.ShouldBe(TransactionCoordinatorClientState.Closed);
    }

    // Upstream: TransactionClientCloseTest#testEndTransactionAfterClientClose
    [Fact]
    public async Task Commit_GivenClientDisposedAfterCreatingTransactions_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var handler = Substitute.For<ITransactionMetaStoreHandler>();
        handler.NewTransaction(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<TransactionId>(new TransactionId(0, 1)));
        var discovery = Substitute.For<ITcDiscovery>();
        discovery.Start(Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);
        discovery.NextHandler().Returns(handler);
        discovery.HandlerForCoordinator(0).Returns(handler);
        var tcClient = new TransactionCoordinatorClient(Substitute.For<IConnectionPool>(), TimeSpan.FromSeconds(30), () => discovery);
        var uut = CreateClient(tcClient);
        var transactionToCommit = await uut.NewTransaction().Create(Current.CancellationToken);
        var transactionToAbort = await uut.NewTransaction().Create(Current.CancellationToken);

        //Act
        await uut.DisposeAsync();
        var commitException = await Record.ExceptionAsync(() => transactionToCommit.Commit(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        var abortException = await Record.ExceptionAsync(() => transactionToAbort.Abort(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        commitException.ShouldBeOfType<TransactionCoordinatorClientStateException>();
        abortException.ShouldBeOfType<TransactionCoordinatorClientStateException>();
        transactionToCommit.CurrentState.ShouldBe(TransactionState.Committing);
        transactionToAbort.CurrentState.ShouldBe(TransactionState.Aborting);
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenTransactionCoordinatorClient_ShouldDisposeItBeforeConnectionPool()
    {
        //Arrange
        var connectionPool = Substitute.For<IConnectionPool>();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = PulsarClientFactory.CreatePulsarClient(connectionPool, Substitute.For<IHandleException>(), _serviceUrl, tcClient);

        //Act
        await uut.DisposeAsync();

        //Assert
        Received.InOrder(() =>
        {
            tcClient.DisposeAsync();
            connectionPool.DisposeAsync();
        });
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenTransactionCoordinatorClientThatThrows_ShouldStillDisposeConnectionPool()
    {
        //Arrange
        var connectionPool = Substitute.For<IConnectionPool>();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.DisposeAsync().Returns(_ => ValueTask.FromException(new InvalidOperationException()));
        var uut = PulsarClientFactory.CreatePulsarClient(connectionPool, Substitute.For<IHandleException>(), _serviceUrl, tcClient);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.DisposeAsync().AsTask());

        //Assert
        exception.ShouldBeNull();
        await connectionPool.Received(1).DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenCalledTwice_ShouldDisposeTransactionCoordinatorClientOnce()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateClient(tcClient);

        //Act
        await uut.DisposeAsync();
        await uut.DisposeAsync();

        //Assert
        await tcClient.Received(1).DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenConsumer_ShouldDisposeConsumerBeforeTransactionCoordinatorClient()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var connectionPool = Substitute.For<IConnectionPool>();
        connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(callInfo => new ValueTask<IConnection>(WaitFor.Forever<IConnection>(callInfo.Arg<CancellationToken>())));
        var uut = PulsarClientFactory.CreatePulsarClient(connectionPool, new DefaultExceptionHandler(), _serviceUrl, tcClient);
        var consumer = uut.NewConsumer(Schema.String).SubscriptionName("subscription").Topic("persistent://public/default/topic").Create();
        var wasConsumerClosedWhenTransactionCoordinatorClientWasDisposed = false;
        tcClient.DisposeAsync().Returns(_ =>
        {
            wasConsumerClosedWhenTransactionCoordinatorClientWasDisposed = consumer.State.IsFinalState();
            return ValueTask.CompletedTask;
        });

        //Act
        await uut.DisposeAsync();

        //Assert
        wasConsumerClosedWhenTransactionCoordinatorClientWasDisposed.ShouldBeTrue();
    }

    private static PulsarClient CreateClient(ITransactionCoordinatorClient? tcClient)
        => PulsarClientFactory.CreatePulsarClient(Substitute.For<IConnectionPool>(), Substitute.For<IHandleException>(), _serviceUrl, tcClient);
}
