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

// The broker of the TransactionMaxActiveFixture allows two active transactions at a time
[Collection("TransactionMaxActive"), Trait("Category", "Integration")]
public sealed class TransactionCoordinatorConfigTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionMaxActiveFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionCoordinatorConfigTests(TransactionMaxActiveFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TransactionCoordinatorConfigTest#testMaxActiveTxn
    [Fact]
    public async Task Create_GivenMaxActiveTransactionsReached_ShouldTimeOutUntilTransactionsEnd()
    {
        //Arrange
        await using var client = _fixture.CreateConfiguredClient(builder => builder.OperationTimeout(TimeSpan.FromSeconds(3)), _testOutputHelper.Log);

        // new two txn will not reach max active txns
        var commitTransaction = await NewTransaction(client);
        var abortTransaction = await NewTransaction(client);

        //Act
        // new the third txn will timeout, broker will return any response
        var thirdWhileTwoAreActive = await Record.ExceptionAsync(async () => await NewTransaction(client));

        //Assert
        thirdWhileTwoAreActive.ShouldBeOfType<OperationTimeoutException>();

        //Act
        // release active txn
        await commitTransaction.Commit(_cts.Token);
        await abortTransaction.Abort(_cts.Token);

        // two txn end, can continue new txn
        _ = await NewTransaction(client);
        _ = await NewTransaction(client);

        // reach max active txns again
        // new the third txn will timeout, broker will return any response
        var thirdWhileTwoAreActiveAgain = await Record.ExceptionAsync(async () => await NewTransaction(client));

        //Assert
        thirdWhileTwoAreActiveAgain.ShouldBeOfType<OperationTimeoutException>();
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task<ITransaction> NewTransaction(IPulsarClient client)
        => await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
}
