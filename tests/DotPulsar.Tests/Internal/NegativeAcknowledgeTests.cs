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
using System.Diagnostics;
using System.Text.Json;

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

    // Upstream: NegativeAcksTest#testNegativeAcksWithBackoff
    // The columns are batching, partitions, subscription type, and the minimum and maximum delay of the backoff in milliseconds. The upstream data provider lists the rows with a minimum delay of 0 twice; they are listed once.
    // The 10 messages are received and negatively acknowledged 5 times, and then received once more. Upstream a batching row negatively acknowledges only the last message of a round, which is enough when the messages
    // are in one batch; here every message of a partitioned row is negatively acknowledged too, because the producer sends them to both partitions, so there are two batches (the tracker takes the first message of an entry
    // and ignores the others). Upstream negatively acknowledges every message of a row without batching right after it is received; here a round receives all its 10 messages first. With a delay of 0 a message is
    // redelivered at once, and on a partitioned topic it can then arrive before a message of the same round that was not received yet, so a round receives one message twice and misses another (seen in the rows with a
    // minimum delay of 0 of a partitioned topic). On a 'Shared' subscription the broker counts the redeliveries, so the delay grows with the backoff, and the whole takes at least the sum of the delays (with 50 ms for
    // the granularity of the timers). On 'Failover' the count stays 0.
    [Theory]
    [InlineData(false, false, SubscriptionType.Shared, 100, 1000)]
    [InlineData(false, false, SubscriptionType.Failover, 100, 1000)]
    [InlineData(false, true, SubscriptionType.Shared, 100, 1000)]
    [InlineData(false, true, SubscriptionType.Failover, 100, 1000)]
    [InlineData(true, false, SubscriptionType.Shared, 100, 1000)]
    [InlineData(true, false, SubscriptionType.Failover, 100, 1000)]
    [InlineData(true, true, SubscriptionType.Shared, 100, 1000)]
    [InlineData(true, true, SubscriptionType.Failover, 100, 1000)]
    [InlineData(false, false, SubscriptionType.Shared, 0, 1000)]
    [InlineData(false, false, SubscriptionType.Failover, 0, 1000)]
    [InlineData(false, true, SubscriptionType.Shared, 0, 1000)]
    [InlineData(false, true, SubscriptionType.Failover, 0, 1000)]
    [InlineData(true, false, SubscriptionType.Shared, 0, 1000)]
    [InlineData(true, false, SubscriptionType.Failover, 0, 1000)]
    [InlineData(true, true, SubscriptionType.Shared, 0, 1000)]
    [InlineData(true, true, SubscriptionType.Failover, 0, 1000)]
    public async Task NegativeAcknowledge_GivenRedeliveryBackoff_ShouldRedeliverAllMessagesAfterTheDelaysOfTheBackoff(bool batching, bool usePartitions, SubscriptionType subscriptionType, int minimumDelayMilliseconds, int maximumDelayMilliseconds)
    {
        //Arrange
        const int numberOfMessages = 10;
        const int numberOfRedeliveries = 5;
        var backoff = new MultiplierRedeliveryBackoff(TimeSpan.FromMilliseconds(minimumDelayMilliseconds), TimeSpan.FromMilliseconds(maximumDelayMilliseconds));
        var negativeAcknowledgeEveryMessage = !batching || usePartitions;
        var topicName = usePartitions ? await _fixture.CreatePartitionedTopic(2, _cts.Token) : await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryBackoff(backoff)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(subscriptionType)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(30))
            .EnableBatching(batching)
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
        sentIds.Any(messageId => messageId.BatchIndex >= 0).ShouldBe(batching);

        //Act
        var expectedTotalDelay = TimeSpan.Zero;
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < numberOfRedeliveries; i++)
        {
            var round = new List<IMessage<string>>();
            for (var j = 0; j < numberOfMessages; j++)
                round.Add(await ReceiveMessage(consumer));

            if (negativeAcknowledgeEveryMessage)
            {
                foreach (var message in round)
                    await consumer.NegativeAcknowledge(message, _cts.Token);
            }
            else
            {
                await consumer.NegativeAcknowledge(round[^1], _cts.Token);
            }

            expectedTotalDelay += backoff.Next((uint) i);
        }

        var receivedMessages = new HashSet<string>();
        var redeliveryCounts = new List<uint>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await ReceiveMessage(consumer);
            receivedMessages.Add(message.Value());
            redeliveryCounts.Add(message.RedeliveryCount);
            await consumer.Acknowledge(message, _cts.Token);
        }

        stopwatch.Stop();

        //Assert
        receivedMessages.ShouldBe(sentMessages, true);
        if (subscriptionType == SubscriptionType.Shared)
        {
            redeliveryCounts.ShouldAllBe(count => count == numberOfRedeliveries);
            stopwatch.Elapsed.ShouldBeGreaterThanOrEqualTo(expectedTotalDelay - TimeSpan.FromMilliseconds(50));
        }

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

    // Upstream: NegativeAcksTest#testMultiTopicConsumerConcurrentRedeliverAndReceive
    // The invocation count of 5 upstream is 5 rows. Upstream waits until the 10 messages are in the incoming queue of the consumer, and redelivers on the internal thread of the consumer after the messages have waited
    // for a second, to make the race of issue #18491 likely. The queue is not visible here, and DotPulsar does not ask the broker for messages before the application starts to receive, so one message is received
    // to start the consumer, and then the test waits until the broker has sent all the messages to the consumer, and for a second more. The redelivery brings back all 10 messages, including the one that was received.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task RedeliverUnacknowledgedMessages_GivenMessagesInReceiveQueueOfConsumerOfPartitionedTopic_ShouldReceiveEveryMessageAgain(int invocation)
    {
        //Arrange
        const int receiverQueueSize = 10;
        const string subscription = "sub";
        var topicName = await _fixture.CreatePartitionedTopic(2, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.Int32)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(receiverQueueSize)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.Int32)
            .EnableBatching(false)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

        for (var i = 0; i < receiverQueueSize; i++)
        {
            _ = await producer.Send(i, _cts.Token);
        }

        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token)).ShouldNotBeNull();
        await WaitUntil(async () => await GetMessagesOut(topicName, subscription) == receiverQueueSize);
        await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);

        //Act
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        var receivedMessages = new HashSet<int>();
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            _ = receivedMessages.Add(message.Value());
        }

        //Assert
        receivedMessages.Count.ShouldBe(10, $"invocation {invocation}");
    }

    // Additional coverage
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

    // The number of messages that the broker has sent to the consumers of the subscription of the partitioned topic
    private async Task<long> GetMessagesOut(string topicName, string subscription)
    {
        using var adminClient = _fixture.CreateAdminClient();
        var separator = topicName.IndexOf("://", StringComparison.Ordinal);
        using var response = await adminClient.GetAsync($"/admin/v2/{topicName[..separator]}/{topicName[(separator + 3)..]}/partitioned-stats", _cts.Token);
        response.EnsureSuccessStatusCode();
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return document.RootElement.GetProperty("subscriptions").TryGetProperty(subscription, out var statistics) ? statistics.GetProperty("msgOutCounter").GetInt64() : 0;
    }

    // Java Awaitility.await().until(...): at most 10 seconds
    private async Task WaitUntil(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);

        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met within 10 seconds");

            await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
        }
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
