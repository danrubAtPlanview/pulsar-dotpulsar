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

// The broker has acknowledgmentAtBatchIndexLevelEnabled=true. The batches are produced by TransactionFixtureBase.ProduceBatchedMessages, and by a producer that batches in the test of the shared subscription.
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

    // Upstream: BatchMessageIndexAckTest#testBatchMessageIndexAckForSharedSubscription
    // The upstream test receives a message in the else branch of the loop that it discards, before it receives the message that it negatively acknowledges (negativeAcknowledge(consumer.receive())).
    // The port does the same, so that the numbers of messages that are received and acknowledged are the numbers of the upstream test. The messages that were discarded stay unacknowledged, and are
    // redelivered by the negative acknowledgments of the messages of the batch.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Acknowledge_GivenBatchIndexAckEnabledAndSharedSubscription_ShouldRedeliverOnlyNegativelyAcknowledgedMessages(bool ackReceiptEnabled)
    {
        //Arrange
        const int numberOfMessages = 100;
        const string subscriptionName = "sub";
        var topicName = TransactionFixture.CreateTopicName("batch-index-ack-shared");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.Int32)
            .AckReceiptEnabled(ackReceiptEnabled)
            .BatchIndexAckEnabled(true) // The default of the Java consumer
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(numberOfMessages)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(2))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.Int32)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            receipts.Add(await SendAsync(producer, i));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Act
        var acked = new List<MessageId>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await Receive(consumer);

            if (i % 2 == 0)
            {
                await consumer.Acknowledge(message, _cts.Token);
                acked.Add(message.MessageId);
            }
            else
            {
                await consumer.NegativeAcknowledge(await Receive(consumer), _cts.Token);
            }
        }

        var received = new List<MessageId>();
        for (var i = 0; i < 50; i++)
        {
            received.Add((await Receive(consumer)).MessageId);
        }

        foreach (var messageId in received)
        {
            await consumer.Acknowledge(messageId, _cts.Token);
        }

        await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        var moreMessage = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);

        //Assert
        received.Count.ShouldBe(50);
        acked.Intersect(received).ShouldBeEmpty();
        moreMessage.ShouldBeNull();

        // The mark delete position was changed to the position of the batch
        var stats = await _fixture.GetInternalStats(topicName, _cts.Token);
        stats.GetProperty("cursors").GetProperty(subscriptionName).GetProperty("markDeletePosition").GetString().ShouldBe($"{received[0].LedgerId}:{received[0].EntryId}");

        receipts.Clear();
        for (var i = 0; i < 50; i++)
        {
            receipts.Add(await SendAsync(producer, i));
        }

        await producer.Flush(_cts.Token);
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        for (var i = 0; i < 50; i++)
        {
            received.Add((await Receive(consumer)).MessageId);
        }

        // Ensure that the flow permits work well since the client skipped the acknowledged batch indexes, the broker also needs to handle the available permits
        received.Count.ShouldBe(100);
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

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync<TMessage>(IProducer<TMessage> producer, TMessage value)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

        await producer.SendChannel.Send(new MessageMetadata(), value, messageId =>
        {
            _ = receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }

    private async Task<IMessage<TMessage>> Receive<TMessage>(IConsumer<TMessage> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(15), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

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
