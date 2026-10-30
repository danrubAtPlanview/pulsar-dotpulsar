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
using DotPulsar.Internal.Transactions;

// The broker of the integration fixture does not have a transaction coordinator
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class TransactionClientConnectTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;

    public TransactionClientConnectTests(IntegrationFixture fixture)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        _fixture = fixture;
    }

    // Upstream: TransactionClientConnectTest#testNotEnableTransactionInBroker
    [Fact]
    public async Task Create_GivenBrokerWithoutTransactionCoordinator_ShouldThrowTransactionCoordinatorClientExceptionAndRetryTheStartNextTime()
    {
        //Arrange
        await using var client = PulsarClient.Builder().Authentication(_fixture.Authentication).ServiceUrl(_fixture.ServiceUrl).EnableTransaction(true).Build();
        var transactionCoordinatorClient = ((PulsarClient) client).TransactionCoordinatorClient!;

        //Act
        var first = await Record.ExceptionAsync(() => client.NewTransaction().Create(_cts.Token).AsTask());
        var stateAfterFirst = transactionCoordinatorClient.State;
        var second = await Record.ExceptionAsync(() => client.NewTransaction().Create(_cts.Token).AsTask());

        //Assert
        first.ShouldBeOfType<TransactionCoordinatorClientException>().Message.ShouldBe("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized");
        stateAfterFirst.ShouldBe(TransactionCoordinatorClientState.None);
        second.ShouldBeOfType<TransactionCoordinatorClientException>();
        transactionCoordinatorClient.State.ShouldBe(TransactionCoordinatorClientState.None);
    }

    public void Dispose()
        => _cts.Dispose();
}
