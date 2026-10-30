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

namespace DotPulsar.Tests.Prerequisites;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;

// The broker has acknowledgmentAtBatchIndexLevelEnabled=true. The batches are produced by TransactionFixtureBase.ProduceBatchedMessages.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class BatchMessageIndexAckTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public BatchMessageIndexAckTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // TODO(WP7): Remove the skip. The test needs negative acknowledgment (negativeAcknowledge with a redelivery delay of 2 seconds),
    // and then redeliverUnacknowledgedMessages. The expected mark delete position is the position of the first batch.
    // Upstream: BatchMessageIndexAckTest#testBatchMessageIndexAckForSharedSubscription
    [Theory(Skip = "Requires negative acknowledgment (WP7)")]
    [InlineData(true)]
    [InlineData(false)]
    public Task Acknowledge_GivenBatchIndexAckEnabledAndSharedSubscription_ShouldRedeliverOnlyNegativelyAcknowledgedMessages(bool ackReceiptEnabled)
    {
        _ = ackReceiptEnabled;
        return Task.CompletedTask;
    }

    // Upstream: BatchMessageIndexAckTest#testBatchMessageIndexAckForExclusiveSubscription
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AcknowledgeCumulative_GivenBatchIndexAckEnabledAndExclusiveSubscription_ShouldRedeliverOnlyTheUnacknowledgedBatchIndexes(bool ackReceiptEnabled)
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-exclusive");
        await _fixture.CreateTopic(topicName, _cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var consumer = CreateConsumer(client, topicName, subscription, ackReceiptEnabled, true);
        await _fixture.ProduceBatchedMessages(topicName, 1, numberOfMessages, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i == 49)
                await consumer.AcknowledgeCumulative(message, _cts.Token);
        }

        // Wait for the acknowledgment to be sent
        await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        await consumer.DisposeAsync();
        await using var resubscribedConsumer = CreateConsumer(client, topicName, subscription, false, false);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < 50; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        var moreMessage = await TransactionFixture.ReceiveOrNull(resubscribedConsumer, TimeSpan.FromSeconds(1), _cts.Token);

        await _fixture.ProduceBatchedMessages(topicName, 1, 50, _cts.Token);
        for (var i = 0; i < 50; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        //Assert
        received.Take(50).Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(50, 50));
        moreMessage.ShouldBeNull();

        // The flow permits must still work after the client has skipped the acknowledged batch indexes
        received.Count.ShouldBe(100);
    }

    // Upstream: BatchMessageIndexAckTest#testDoNotRecycleAckSetMultipleTimes
    [Fact]
    public async Task AcknowledgeCumulative_GivenEveryMessageOfBatchesAcknowledgedOneByOne_ShouldNotThrowAndAcknowledgeEverything()
    {
        //Arrange
        const int numberOfBatches = 3;
        const int batchSize = 10;
        const int numberOfMessages = numberOfBatches * batchSize;
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-recycle");
        await _fixture.CreateTopic(topicName, _cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, subscription, false, true);
        await _fixture.ProduceBatchedMessages(topicName, numberOfBatches, batchSize, _cts.Token);

        //Act
        var act = async () =>
        {
            for (var i = 0; i < numberOfMessages; i++)
            {
                await consumer.AcknowledgeCumulative(await consumer.Receive(_cts.Token), _cts.Token);
                await Task.Delay(2, _cts.Token);
            }
        };

        //Assert
        await Should.NotThrowAsync(act);
        await WaitForBacklog(topicName, subscription, 0);
    }

    [Fact]
    public async Task Acknowledge_GivenBatchIndexAckEnabledAndEveryOtherMessageAcknowledged_ShouldRedeliverOnlyTheUnacknowledgedMessages()
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-every-other");
        await _fixture.CreateTopic(topicName, _cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var consumer = CreateConsumer(client, topicName, subscription, true, true);
        await _fixture.ProduceBatchedMessages(topicName, 1, numberOfMessages, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i % 2 == 0)
                await consumer.Acknowledge(message, _cts.Token);
        }

        await consumer.DisposeAsync();
        await using var resubscribedConsumer = CreateConsumer(client, topicName, subscription, false, false);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < 50; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        var moreMessage = await TransactionFixture.ReceiveOrNull(resubscribedConsumer, TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        received.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, 50).Select(i => (i * 2) + 1));
        moreMessage.ShouldBeNull();
    }

    [Fact]
    public async Task Acknowledge_GivenBatchIndexAckDisabledAndEveryOtherMessageAcknowledged_ShouldRedeliverTheWholeBatch()
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-disabled");
        await _fixture.CreateTopic(topicName, _cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var consumer = CreateConsumer(client, topicName, subscription, true, false);
        await _fixture.ProduceBatchedMessages(topicName, 1, numberOfMessages, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i % 2 == 0)
                await consumer.Acknowledge(message, _cts.Token);
        }

        await consumer.DisposeAsync();
        await using var resubscribedConsumer = CreateConsumer(client, topicName, subscription, false, false);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        //Assert
        received.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, numberOfMessages));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Acknowledge_GivenAllMessagesOfBatchAcknowledgedOneByOne_ShouldAcknowledgeTheBatch(bool batchIndexAckEnabled)
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-all");
        await _fixture.CreateTopic(topicName, _cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, subscription, true, batchIndexAckEnabled);
        await _fixture.ProduceBatchedMessages(topicName, 1, numberOfMessages, _cts.Token);

        //Act
        for (var i = 0; i < numberOfMessages; i++)
        {
            await consumer.Acknowledge(await consumer.Receive(_cts.Token), _cts.Token);
        }

        //Assert
        await WaitForBacklog(topicName, subscription, 0);
    }

    [Fact]
    public async Task AcknowledgeCumulative_GivenPartiallyAcknowledgedBatchAndBatchIndexAckDisabled_ShouldAcknowledgeThePreviousBatches()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-cumulative-disabled");
        await _fixture.CreateTopic(topicName, _cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var consumer = CreateConsumer(client, topicName, subscription, true, false);
        await _fixture.ProduceBatchedMessages(topicName, 3, 10, _cts.Token);

        for (var i = 0; i < 30; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            // The message with batch index 4 in the second batch
            if (i == 14)
                await consumer.AcknowledgeCumulative(message, _cts.Token);
        }

        await consumer.DisposeAsync();
        await using var resubscribedConsumer = CreateConsumer(client, topicName, subscription, false, false);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < 20; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        var moreMessage = await TransactionFixture.ReceiveOrNull(resubscribedConsumer, TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        received.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, 20).Select(i => i % 10));
        received.Select(message => message.MessageId.EntryId).Distinct().Count().ShouldBe(2);
        moreMessage.ShouldBeNull();
    }

    [Fact]
    public async Task Receive_GivenBatches_ShouldReturnMessageIdsWithTopicAndConsecutiveBatchIndexes()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-topic");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, CreateSubscriptionName(), false, false);
        await _fixture.ProduceBatchedMessages(topicName, 2, 10, _cts.Token);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < 20; i++)
        {
            received.Add(await consumer.Receive(_cts.Token));
        }

        //Assert
        received.ShouldAllBe(message => message.MessageId.Topic == topicName);
        received.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, 20).Select(i => i % 10));
    }

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

    private IConsumer<byte[]> CreateConsumer(IPulsarClient client, string topicName, string subscription, bool ackReceiptEnabled, bool batchIndexAckEnabled)
        => client.NewConsumer(Schema.ByteArray)
        .AckReceiptEnabled(ackReceiptEnabled)
        .BatchIndexAckEnabled(batchIndexAckEnabled)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .MessagePrefetchCount(100)
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(subscription)
        .Topic(topicName)
        .Create();

    private async Task WaitForBacklog(string topicName, string subscription, long expected)
    {
        var backlog = long.MaxValue;

        for (var attempt = 0; attempt < 20 && backlog != expected; attempt++)
        {
            var stats = await _fixture.GetTopicStats(topicName, _cts.Token);
            backlog = stats.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();

            if (backlog != expected)
                await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
        }

        backlog.ShouldBe(expected);
    }

    public void Dispose() => _cts.Dispose();
}
