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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Internal;

[Collection("Integration"), Trait("Category", "Integration")]
public sealed class NegativeAcknowledgeTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public NegativeAcknowledgeTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: NegativeAcksTest#testNegativeAcks
    // The columns are batching, partitions, subscription type, negative acknowledge redelivery delay in milliseconds and ack timeout in milliseconds.
    // The upstream producer sends 20 messages without waiting and flushes. The batching rows close a batch at 10 messages, or when the producer is flushed.
    // The first 10 messages are negatively acknowledged by message, and the second 10 by message id.
    [Theory]
    [InlineData(false, false, SubscriptionType.Shared, 100, 0)]
    [InlineData(false, false, SubscriptionType.Shared, 100, 1000)]
    [InlineData(false, false, SubscriptionType.Shared, 0, 0)]
    [InlineData(false, false, SubscriptionType.Shared, 0, 1000)]
    [InlineData(false, false, SubscriptionType.Failover, 100, 0)]
    [InlineData(false, false, SubscriptionType.Failover, 100, 1000)]
    [InlineData(false, false, SubscriptionType.Failover, 0, 0)]
    [InlineData(false, false, SubscriptionType.Failover, 0, 1000)]
    [InlineData(false, true, SubscriptionType.Shared, 100, 0)]
    [InlineData(false, true, SubscriptionType.Shared, 100, 1000)]
    [InlineData(false, true, SubscriptionType.Shared, 0, 0)]
    [InlineData(false, true, SubscriptionType.Shared, 0, 1000)]
    [InlineData(false, true, SubscriptionType.Failover, 100, 0)]
    [InlineData(false, true, SubscriptionType.Failover, 100, 1000)]
    [InlineData(false, true, SubscriptionType.Failover, 0, 0)]
    [InlineData(false, true, SubscriptionType.Failover, 0, 1000)]
    [InlineData(true, false, SubscriptionType.Shared, 100, 0)]
    [InlineData(true, false, SubscriptionType.Shared, 100, 1000)]
    [InlineData(true, false, SubscriptionType.Shared, 0, 0)]
    [InlineData(true, false, SubscriptionType.Shared, 0, 1000)]
    [InlineData(true, false, SubscriptionType.Failover, 100, 0)]
    [InlineData(true, false, SubscriptionType.Failover, 100, 1000)]
    [InlineData(true, false, SubscriptionType.Failover, 0, 0)]
    [InlineData(true, false, SubscriptionType.Failover, 0, 1000)]
    [InlineData(true, true, SubscriptionType.Shared, 100, 0)]
    [InlineData(true, true, SubscriptionType.Shared, 100, 1000)]
    [InlineData(true, true, SubscriptionType.Shared, 0, 0)]
    [InlineData(true, true, SubscriptionType.Shared, 0, 1000)]
    [InlineData(true, true, SubscriptionType.Failover, 100, 0)]
    [InlineData(true, true, SubscriptionType.Failover, 100, 1000)]
    [InlineData(true, true, SubscriptionType.Failover, 0, 0)]
    [InlineData(true, true, SubscriptionType.Failover, 0, 1000)]
    public async Task NegativeAcknowledge_GivenVariations_ShouldRedeliverAllNegativelyAcknowledgedMessages(bool batching, bool usePartitions, SubscriptionType subscriptionType, int negativeAcknowledgeDelayMilliseconds, int ackTimeoutMilliseconds)
    {
        //Arrange
        const int numberOfMessages = 10;
        var topicName = usePartitions ? await _fixture.CreatePartitionedTopic(2, _cts.Token) : await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .AckTimeout(TimeSpan.FromMilliseconds(ackTimeoutMilliseconds))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(negativeAcknowledgeDelayMilliseconds))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(subscriptionType)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .EnableBatching(batching)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var sentMessages = new HashSet<string>();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages * 2; i++)
        {
            var value = $"test-{i}";
            sentMessages.Add(value);
            receipts.Add(await SendAsync(producer, value));
        }

        await producer.Flush(_cts.Token);
        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Any(messageId => messageId.BatchIndex >= 0).ShouldBe(batching);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await ReceiveMessage(consumer);
            await consumer.NegativeAcknowledge(message, _cts.Token);
        }

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await ReceiveMessage(consumer);
            await consumer.NegativeAcknowledge(message.MessageId, _cts.Token);
        }

        var trackedAfterNegativeAcknowledge = ((Consumer<string>) consumer).UnAckedMessageCount;

        //Act
        var receivedMessages = new HashSet<string>();
        for (var i = 0; i < numberOfMessages * 2; i++)
        {
            var message = await ReceiveMessage(consumer);
            receivedMessages.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        trackedAfterNegativeAcknowledge.ShouldBe(0);
        receivedMessages.ShouldBe(sentMessages, true);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromMilliseconds(500), _cts.Token)).ShouldBeNull();
    }

    // Upstream: NegativeAcksTest#testNegativeAcksWithBatch
    // The producer batches two messages (BatchingMaxMessages of 2, and a publish delay of 1 hour, so the batch is sent because it is full). The first message is negatively acknowledged, and the second
    // is negatively acknowledged 2 seconds later. The redelivery delay is 3 seconds, so the first message must be redelivered 1 second later, and not 3 seconds after the second negative acknowledge.
    [Fact]
    public async Task NegativeAcknowledge_GivenTwoMessagesInSameBatchNackedTwoSecondsApart_ShouldRedeliverFirstWithinTwoSeconds()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(3))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("sub1")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(2)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Send two messages in the same batch
        var receipts = new List<Task<MessageId>> { await SendAsync(producer, "test-0"), await SendAsync(producer, "test-1") };
        await producer.Flush(_cts.Token);
        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Select(messageId => messageId.EntryId).Distinct().Count().ShouldBe(1);

        //Act
        // Negatively acknowledge the first message, wait for 2 seconds, and negatively acknowledge the second message
        await consumer.NegativeAcknowledge(await ReceiveMessage(consumer), _cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2), _cts.Token);
        await consumer.NegativeAcknowledge(await ReceiveMessage(consumer), _cts.Token);

        //Assert
        // Now 2 seconds have passed, so the first message is redelivered 1 second later
        var redelivered = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);
        redelivered.ShouldNotBeNull();
    }

    // Upstream: NegativeAcksTest#testNegativeAcksWithBatchAckEnabled
    // Ten messages are sent as one batch, all are negatively acknowledged, and all are received again and acknowledged, after which no more messages are received.
    [Fact]
    public async Task NegativeAcknowledge_GivenTenBatchedMessages_ShouldRedeliverAllAndThenNoMore()
    {
        //Arrange
        const int numberOfMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .BatchIndexAckEnabled(true) // The default of the Java consumer
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(1))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("sub1")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var sentMessages = new HashSet<string>();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var value = $"test-{i}";
            sentMessages.Add(value);
            receipts.Add(await SendAsync(producer, value));
        }

        await producer.Flush(_cts.Token);
        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Select(messageId => messageId.EntryId).Distinct().Count().ShouldBe(1);

        for (var i = 0; i < numberOfMessages; i++)
        {
            await consumer.NegativeAcknowledge(await ReceiveMessage(consumer), _cts.Token);
        }

        //Act
        // All the messages should be received again
        var receivedMessages = new HashSet<string>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await ReceiveMessage(consumer);
            receivedMessages.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        receivedMessages.ShouldBe(sentMessages, true);

        // There should be no more messages
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromMilliseconds(100), _cts.Token)).ShouldBeNull();
    }

    [Fact]
    public async Task NegativeAcknowledge_GivenMessageId_ShouldRedeliverAfterTheDelayAndNotBefore()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(3))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        _ = await producer.Send("test", _cts.Token);
        var message = await ReceiveMessage(consumer);

        //Act
        await consumer.NegativeAcknowledge(message.MessageId, _cts.Token);
        var earlyRedelivery = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token);
        var redelivery = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);

        //Assert
        earlyRedelivery.ShouldBeNull();
        redelivery.ShouldNotBeNull();
        redelivery.Value().ShouldBe("test");
        redelivery.RedeliveryCount.ShouldBe(1U);
    }

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync(IProducer<string> producer, string value)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

        await producer.SendChannel.Send(new MessageMetadata(), value, messageId =>
        {
            _ = receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }

    private async Task<IMessage<string>> ReceiveMessage(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();
}
