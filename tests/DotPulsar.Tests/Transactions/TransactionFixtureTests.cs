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
public sealed class TransactionFixtureTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionFixtureTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Additional coverage
    [Fact]
    public async Task TransactionFixture_ShouldHaveTransactionCoordinatorAssignTopic()
    {
        //Arrange
        const int coordinatorId = 0;

        //Act
        var partitions = await _fixture.GetPartitionedTopicMetadata(TransactionFixture.TransactionCoordinatorAssignTopic, _cts.Token);
        var stats = await _fixture.GetCoordinatorStats(coordinatorId, _cts.Token);

        //Assert
        partitions.GetProperty("partitions").GetInt32().ShouldBeGreaterThanOrEqualTo(1);
        stats.GetProperty("state").GetString().ShouldBe("Ready");
    }

    // Additional coverage
    [Fact]
    public async Task TransactionFixture_ShouldApplyTheBrokerConfiguration()
    {
        //Arrange
        const string transactionBufferSnapshotMaxTransactionCount = "2";

        //Act
        var configuration = await _fixture.GetRuntimeConfiguration(_cts.Token);

        //Assert
        configuration.GetProperty("transactionCoordinatorEnabled").GetString().ShouldBe("true");
        configuration.GetProperty("brokerDeduplicationEnabled").GetString().ShouldBe("true");
        configuration.GetProperty("acknowledgmentAtBatchIndexLevelEnabled").GetString().ShouldBe("true");
        configuration.GetProperty("transactionBufferSnapshotMaxTransactionCount").GetString().ShouldBe(transactionBufferSnapshotMaxTransactionCount);
    }

    // Additional coverage
    [Fact]
    public async Task CreateClient_GivenTopicInTransactionNamespace_ShouldSendAndReceiveMessage()
    {
        //Arrange
        const string content = "test-message";
        var topicName = TransactionFixture.CreateTopicName("fixture-test");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String).Topic(topicName).Create();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .Topic(topicName)
            .Create();

        //Act
        var messageId = await producer.Send(content, _cts.Token);
        var message = await consumer.Receive(_cts.Token);

        //Assert
        message.MessageId.ShouldBe(messageId);
        message.Value().ShouldBe(content);
    }

    // Additional coverage
    [Fact]
    public async Task ReceiveOrNull_GivenNoMessage_ShouldReturnNull()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .Topic(topicName)
            .Create();

        //Act
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);

        //Assert
        message.ShouldBeNull();
    }

    public void Dispose() => _cts.Dispose();
}
