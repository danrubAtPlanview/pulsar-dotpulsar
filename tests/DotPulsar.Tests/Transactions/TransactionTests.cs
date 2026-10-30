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

namespace DotPulsar.Tests.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal.Transactions;
using System.Net;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    [Fact]
    public async Task Create_GivenTransactionsEnabled_ShouldCreateOpenTransactionKnownByCoordinator()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);
        _testOutputHelper.WriteLine($"The metadata of the transaction {transaction.Id} is {metadata}");

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Open);
        transaction.State.IsFinalState().ShouldBeFalse();
        transaction.Id.MostSigBits.ShouldBe(0UL);
        metadata.GetProperty("txnId").GetString().ShouldBe(transaction.Id.ToString());
        metadata.GetProperty("status").GetString().ShouldBe("OPEN");
        metadata.GetProperty("owner").GetString().ShouldNotBeNullOrEmpty();
        metadata.GetProperty("timeoutAt").GetInt64().ShouldBe(300000);

        await transaction.Abort(_cts.Token);
    }

    [Fact]
    public async Task Create_GivenTimeoutOfOneMinute_ShouldTellTheCoordinatorTheTimeoutInMilliseconds()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transaction = await client.NewTransaction().Create(_cts.Token);
        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);

        //Assert
        metadata.GetProperty("timeoutAt").GetInt64().ShouldBe(60000);

        await transaction.Abort(_cts.Token);
    }

    [Fact]
    public async Task Create_GivenSeveralTransactions_ShouldCreateTransactionsWithDistinctIds()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transactions = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token).AsTask()));

        //Assert
        transactions.Select(transaction => transaction.Id).Distinct().Count().ShouldBe(20);
        transactions.ShouldAllBe(transaction => transaction.CurrentState == TransactionState.Open);

        await Task.WhenAll(transactions.Select(transaction => transaction.Abort(_cts.Token).AsTask()));
    }

    [Fact]
    public async Task Commit_GivenEmptyTransaction_ShouldCommitTransaction()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

        //Act
        await transaction.Commit(_cts.Token);
        var status = await _fixture.GetTransactionMetadataStatusCode(transaction.Id, _cts.Token);

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
        transaction.State.IsFinalState().ShouldBeTrue();
        status.ShouldBe(HttpStatusCode.NotFound); // The broker removes a transaction that has ended
    }

    [Fact]
    public async Task Abort_GivenEmptyTransaction_ShouldAbortTransaction()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

        //Act
        await transaction.Abort(_cts.Token);
        var status = await _fixture.GetTransactionMetadataStatusCode(transaction.Id, _cts.Token);

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
        transaction.State.IsFinalState().ShouldBeTrue();
        status.ShouldBe(HttpStatusCode.NotFound); // The broker removes a transaction that has ended
    }

    [Fact]
    public async Task Commit_GivenCommittedTransaction_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        await transaction.Commit(_cts.Token);

        //Act
        var commitException = await Record.ExceptionAsync(() => transaction.Commit(_cts.Token).AsTask());
        var abortException = await Record.ExceptionAsync(() => transaction.Abort(_cts.Token).AsTask());

        //Assert
        commitException.ShouldBeOfType<InvalidTransactionStatusException>();
        abortException.ShouldBeOfType<InvalidTransactionStatusException>();
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    [Fact]
    public async Task Create_GivenShortTimeout_ShouldBecomeTimedOutAndRejectCommitAndAbort()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        var state = await transaction.State.OnStateChangeTo(TransactionState.TimedOut, _cts.Token);
        var commitException = await Record.ExceptionAsync(() => transaction.Commit(_cts.Token).AsTask());
        var abortException = await Record.ExceptionAsync(() => transaction.Abort(_cts.Token).AsTask());

        //Assert
        state.ShouldBe(TransactionState.TimedOut);
        transaction.CurrentState.ShouldBe(TransactionState.TimedOut);
        commitException.ShouldBeOfType<InvalidTransactionStatusException>();
        abortException.ShouldBeOfType<InvalidTransactionStatusException>();
    }

    // The steps that need a producer are ported with the transactional producer
    // Upstream: TransactionTest#testGetTxnState
    [Fact]
    public async Task CurrentState_GivenEmptyTransactions_ShouldReportEachState()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var timedOut = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        var stateBeforeTimeout = timedOut.CurrentState;
        await timedOut.State.OnStateChangeTo(TransactionState.TimedOut, _cts.Token);

        var aborted = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(3)).Create(_cts.Token);
        await aborted.Abort(_cts.Token);

        var committed = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(3)).Create(_cts.Token);
        await committed.Commit(_cts.Token);

        var endedByBroker = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        await _fixture.AbortTransaction(endedByBroker.Id, _cts.Token);
        var commitException = await Record.ExceptionAsync(() => endedByBroker.Commit(_cts.Token).AsTask());

        //Assert
        stateBeforeTimeout.ShouldBe(TransactionState.Open);
        timedOut.CurrentState.ShouldBe(TransactionState.TimedOut);
        aborted.CurrentState.ShouldBe(TransactionState.Aborted);
        committed.CurrentState.ShouldBe(TransactionState.Committed);
        commitException.ShouldBeOfType<TransactionNotFoundException>();
        endedByBroker.CurrentState.ShouldBe(TransactionState.Error);
    }

    // Upstream: TransactionBuilderTest#checkClientEnableTransactionConfiguration
    [Fact]
    public async Task Create_GivenClientWithTransactionsDisabled_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = _fixture.CreateClient(enableTransaction: false);

        //Act
        var exception = await Record.ExceptionAsync(() => client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token).AsTask());

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("Transactions are not enabled");
        ((PulsarClient) client).TransactionCoordinatorClient.ShouldBeNull();
    }

    [Fact]
    public async Task Create_GivenNewClient_ShouldStartTheCoordinatorClientWithTheFirstTransaction()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transactionCoordinatorClient = ((PulsarClient) client).TransactionCoordinatorClient!;
        var stateBefore = transactionCoordinatorClient.State;

        //Act
        var transaction = await client.NewTransaction().Create(_cts.Token);

        //Assert
        stateBefore.ShouldBe(TransactionCoordinatorClientState.None);
        transactionCoordinatorClient.State.ShouldBe(TransactionCoordinatorClientState.Ready);

        await transaction.Abort(_cts.Token);
    }

    public void Dispose()
        => _cts.Dispose();
}
