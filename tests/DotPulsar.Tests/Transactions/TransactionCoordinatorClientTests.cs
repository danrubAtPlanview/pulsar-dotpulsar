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
using DotPulsar.Internal;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

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
        await using var pool = CreateConnectionPool();
        await using var uut = new TransactionCoordinatorClient(pool, TimeSpan.FromSeconds(30));

        //Act
        await uut.EnsureStarted(_cts.Token);
        var startAgain = await Record.ExceptionAsync(() => uut.Start(_cts.Token).AsTask());
        var transactionToCommit = await uut.NewTransaction(TimeSpan.FromMinutes(1), _cts.Token);
        var transactionToAbort = await uut.NewTransaction(TimeSpan.FromMinutes(1), _cts.Token);
        await uut.AddPublishPartitionToTxn(transactionToCommit, topic, _cts.Token);
        await uut.AddPublishPartitionToTxn(transactionToAbort, topic, _cts.Token);
        var metadataToCommit = await GetTransactionMetadata(transactionToCommit);
        var metadataToAbort = await GetTransactionMetadata(transactionToAbort);
        await uut.Commit(transactionToCommit, _cts.Token);
        await uut.Abort(transactionToAbort, _cts.Token);
        var statusOfCommitted = await GetTransactionMetadataStatusCode(transactionToCommit);
        var statusOfAborted = await GetTransactionMetadataStatusCode(transactionToAbort);
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

    private async Task<JsonElement> GetTransactionMetadata(TransactionId transactionId)
    {
        var metadata = await _fixture.GetTransactionMetadata((long) transactionId.MostSigBits, (long) transactionId.LeastSigBits, _cts.Token);
        _testOutputHelper.WriteLine($"The metadata of the transaction {transactionId} is {metadata}");
        return metadata;
    }

    private Task<HttpStatusCode> GetTransactionMetadataStatusCode(TransactionId transactionId)
        => _fixture.GetStatusCode($"/admin/v3/transactions/transactionMetadata/{transactionId.MostSigBits}/{transactionId.LeastSigBits}", _cts.Token);

    private ConnectionPool CreateConnectionPool()
    {
        var commandConnect = new CommandConnect
        {
            ProtocolVersion = Constants.ProtocolVersion,
            ClientVersion = $"{Constants.ClientName} {Constants.ClientVersion}",
            FeatureFlags = new FeatureFlags { SupportsAuthRefresh = true }
        };

        var connector = new Connector(new DefaultRemoteCertificateValidator(null, true, false), new X509Certificate2Collection(), true);
        return new ConnectionPool(commandConnect, _fixture.ServiceUrl, connector, EncryptionPolicy.EnforceUnencrypted, TimeSpan.FromSeconds(60), null, TimeSpan.FromSeconds(30), _fixture.Authentication);
    }
}
