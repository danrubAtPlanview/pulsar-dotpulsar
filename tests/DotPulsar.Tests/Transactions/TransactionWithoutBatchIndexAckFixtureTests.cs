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

[Collection("TransactionWithoutBatchIndexAck"), Trait("Category", "Integration")]
public sealed class TransactionWithoutBatchIndexAckFixtureTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionWithoutBatchIndexAckFixture _fixture;

    public TransactionWithoutBatchIndexAckFixtureTests(TransactionWithoutBatchIndexAckFixture fixture)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
    }

    [Fact]
    public async Task TransactionWithoutBatchIndexAckFixture_ShouldDisableBatchIndexAcknowledgment()
    {
        //Arrange
        const int coordinatorId = 0;

        //Act
        var configuration = await _fixture.GetRuntimeConfiguration(_cts.Token);
        var stats = await _fixture.GetCoordinatorStats(coordinatorId, _cts.Token);

        //Assert
        configuration.GetProperty("acknowledgmentAtBatchIndexLevelEnabled").GetString().ShouldBe("false");
        configuration.GetProperty("transactionCoordinatorEnabled").GetString().ShouldBe("true");
        stats.GetProperty("state").GetString().ShouldBe("Ready");
    }

    public void Dispose() => _cts.Dispose();
}
