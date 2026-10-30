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
using DotPulsar.Extensions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Transactions;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionClientCloseTests : IDisposable
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;

    public TransactionClientCloseTests(TransactionFixture fixture)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
    }

    // Upstream: TransactionClientCloseTest#testEndTransactionAfterClientClose
    [Fact]
    public async Task Commit_GivenClientDisposedAfterCreatingTransactions_ShouldThrowTransactionCoordinatorClientStateException()
    {
        //Arrange
        var client = _fixture.CreateClient();
        var transactionToCommit = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var transactionToAbort = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);

        //Act
        await client.DisposeAsync();

        //Assert
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(transactionToCommit.Commit(_cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(transactionToAbort.Abort(_cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
    }

    // Upstream: TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Requests_GivenDisposedClient_ShouldThrowTransactionCoordinatorClientStateException(bool isStarted)
    {
        //Arrange
        var client = _fixture.CreateClient();
        var tcClient = ((PulsarClient) client).TransactionCoordinatorClient!;
        if (isStarted)
            await tcClient.EnsureStarted(_cts.Token);

        //Act
        await client.DisposeAsync();

        //Assert
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(client.NewTransaction().Create(_cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
        await AssertRequestsFail(tcClient);
        tcClient.State.ShouldBe(TransactionCoordinatorClientState.Closed);
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task AssertRequestsFail(ITransactionCoordinatorClient tcClient)
    {
        var transactionId = new TransactionId(0, 0);

        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(tcClient.NewTransaction(TimeSpan.FromMilliseconds(60000), _cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(tcClient.AddPublishPartitionToTxn(transactionId, "topic", _cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(tcClient.AddSubscriptionToTxn(transactionId, "topic", "subscription", _cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(tcClient.Commit(transactionId, _cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
        await Should.ThrowAsync<TransactionCoordinatorClientStateException>(tcClient.Abort(transactionId, _cts.Token).AsTask().WaitAsync(_timeout, _cts.Token));
    }
}
