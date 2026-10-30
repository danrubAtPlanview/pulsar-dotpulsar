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
using DotPulsar.Extensions;

// The reconnect tests of the Java class TransactionClientConnectTest are unit tests (TransactionCoordinatorClientTests), because they let the broker answer that the
// transaction coordinator is not found. These tests drop the connection to a real broker. They are not in TransactionClientConnectTests, because that class runs on the
// broker of the IntegrationFixture, which has no transaction coordinator, and a class can only be in one collection.
[Collection("TransactionToxiproxy"), Trait("Category", "Integration")]
public sealed class TransactionClientReconnectTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionToxiproxyFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionClientReconnectTests(TransactionToxiproxyFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Additional coverage
    [Fact]
    public async Task NewTransaction_GivenConnectionDroppedAndRestored_ShouldSucceed()
    {
        //Arrange
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var firstTransaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        await firstTransaction.Commit(_cts.Token);
        Task<ITransaction> creating;

        //Act
        await using (await _fixture.DisableThePulsarConnection())
        {
            creating = client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token).AsTask();
            await Task.Delay(TimeSpan.FromSeconds(3), _cts.Token);
            creating.IsCompleted.ShouldBeFalse("The transaction can not be created while the connection to the broker is dropped");
        }

        var transaction = await creating.WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        await transaction.Commit(_cts.Token);

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    public void Dispose()
        => _cts.Dispose();
}
