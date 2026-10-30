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

using DotPulsar.Exceptions;
using DotPulsar.Internal.Transactions;
using System.Net;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionCoordinatorClientTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionCoordinatorClientTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // The broker removes a transaction from the transaction metadata store when it has been committed or aborted. Its status can therefore only be read
    // through the admin API while it is open, and the transaction coordinator no longer knows the transaction afterwards
    // Upstream: TransactionCoordinatorClientTest#testClientStart, TransactionCoordinatorClientTest#testNewTxn, TransactionCoordinatorClientTest#testCommitAndAbort
    [Fact]
    public async Task NewTransaction_GivenStartedClient_ShouldCommitAndAbortTransactionsById()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("transaction-coordinator-client");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient();
        var uut = (TransactionCoordinatorClient) ((PulsarClient) client).TransactionCoordinatorClient!;

        //Act
        await uut.EnsureStarted(_cts.Token);
        var startAgain = await Record.ExceptionAsync(() => uut.Start(_cts.Token).AsTask());
        var transactionToCommit = await uut.NewTransaction(TimeSpan.FromMinutes(1), _cts.Token);
        var transactionToAbort = await uut.NewTransaction(TimeSpan.FromMinutes(1), _cts.Token);
        await uut.AddPublishPartitionToTxn(transactionToCommit, topic, _cts.Token);
        await uut.AddPublishPartitionToTxn(transactionToAbort, topic, _cts.Token);
        var metadataToCommit = await _fixture.GetTransactionMetadata(transactionToCommit, _cts.Token);
        var metadataToAbort = await _fixture.GetTransactionMetadata(transactionToAbort, _cts.Token);
        _testOutputHelper.WriteLine($"The metadata of the transaction {transactionToCommit} is {metadataToCommit}");
        _testOutputHelper.WriteLine($"The metadata of the transaction {transactionToAbort} is {metadataToAbort}");
        await uut.Commit(transactionToCommit, _cts.Token);
        await uut.Abort(transactionToAbort, _cts.Token);
        var statusOfCommitted = await _fixture.GetTransactionMetadataStatusCode(transactionToCommit, _cts.Token);
        var statusOfAborted = await _fixture.GetTransactionMetadataStatusCode(transactionToAbort, _cts.Token);
        var abortAfterCommit = await Record.ExceptionAsync(() => uut.Abort(transactionToCommit, _cts.Token).AsTask());
        var commitAfterAbort = await Record.ExceptionAsync(() => uut.Commit(transactionToAbort, _cts.Token).AsTask());

        //Assert
        startAgain.ShouldBeOfType<TransactionCoordinatorClientStateException>();
        uut.State.ShouldBe(TransactionCoordinatorClientState.Ready);
        transactionToCommit.MostSigBits.ShouldBe(0UL);
        transactionToAbort.MostSigBits.ShouldBe(0UL);
        transactionToAbort.ShouldNotBe(transactionToCommit);
        metadataToCommit.GetProperty("txnId").GetString().ShouldBe(transactionToCommit.ToString());
        metadataToCommit.GetProperty("status").GetString().ShouldBe("OPEN");
        metadataToCommit.GetProperty("producedPartitions").TryGetProperty(topic, out _).ShouldBeTrue();
        metadataToAbort.GetProperty("txnId").GetString().ShouldBe(transactionToAbort.ToString());
        metadataToAbort.GetProperty("status").GetString().ShouldBe("OPEN");
        metadataToAbort.GetProperty("producedPartitions").TryGetProperty(topic, out _).ShouldBeTrue();
        statusOfCommitted.ShouldBe(HttpStatusCode.NotFound);
        statusOfAborted.ShouldBe(HttpStatusCode.NotFound);
        abortAfterCommit.ShouldBeOfType<TransactionNotFoundException>();
        commitAfterAbort.ShouldBeOfType<TransactionNotFoundException>();

        foreach (var handler in uut.Handlers.OfType<TransactionMetaStoreHandler>())
        {
            handler.PendingRequestsCount.ShouldBe(0);
            handler.AvailableRequestPermits.ShouldBe(1000);
        }
    }

    public void Dispose()
        => _cts.Dispose();
}
