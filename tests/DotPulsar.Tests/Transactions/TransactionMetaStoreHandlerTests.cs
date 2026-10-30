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

using DotPulsar.Extensions;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionMetaStoreHandlerTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;

    public TransactionMetaStoreHandlerTests(TransactionFixture fixture)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
    }

    // Unloading the assign partition makes the broker remove the transaction meta store. The coordinator is not found until the handler has connected again,
    // which makes the broker load the transaction meta store and recover the transactions that were open
    // Additional coverage (TransactionMetaStoreAssignmentTest#testTransactionMetaStoreUnload needs the internals of the broker)
    [Fact]
    public async Task Create_GivenTransactionCoordinatorUnloaded_ShouldRetryUntilTheCoordinatorIsLoadedAgain()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var open = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

        //Act
        await _fixture.UnloadTopic($"{TransactionFixture.TransactionCoordinatorAssignTopic}-partition-0", _cts.Token);
        var created = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        await open.Commit(_cts.Token);
        await created.Abort(_cts.Token);

        //Assert
        open.CurrentState.ShouldBe(TransactionState.Committed);
        created.CurrentState.ShouldBe(TransactionState.Aborted);
        created.Id.ShouldNotBe(open.Id);
    }

    public void Dispose()
        => _cts.Dispose();
}
