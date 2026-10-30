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
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

// The dead letter tests of Java DeadLetterTopicTest, except the retry letter topic (not supported).
// The consumers never acknowledge the messages, and the ack timeout of 1 second redelivers them until they are sent to the dead letter topic.
// The test of the batch index acknowledgment (testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound) needs a broker that has acknowledgmentAtBatchIndexLevelEnabled, which is not the
// case for the broker of the IntegrationFixture, so it is in DeadLetterTopicBatchIndexAckTests.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class DeadLetterTopicTests : IDisposable
{
    private const string SubscriptionName = "my-subscription";
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public DeadLetterTopicTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithMessageKey
    [Fact]
    public async Task Receive_GivenMessagesWithKeyRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithKey()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            await producer.NewMessage().Key("test-key").Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Count.ShouldBe(sendMessages);
        deadLettered.ShouldAllBe(message => message.Key == "test-key" && !message.HasBase64EncodedKey);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithBinaryMessageKey
    [Fact]
    public async Task Receive_GivenMessagesWithBinaryKeyRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithKeyBytes()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 100;
        var key = Encoding.UTF8.GetBytes("binaryKey");
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            await producer.NewMessage().KeyBytes(key).Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Count.ShouldBe(sendMessages);
        deadLettered.ShouldAllBe(message => message.HasBase64EncodedKey && message.KeyBytes!.SequenceEqual(key));
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicMessagesWithOrderingKey
    [Fact]
    public async Task Receive_GivenMessagesWithOrderingKeyRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithOrderingKey()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 100;
        var orderingKey = Encoding.UTF8.GetBytes("ordering-key");
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            await producer.NewMessage().OrderingKey(orderingKey).Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Count.ShouldBe(sendMessages);
        deadLettered.ShouldAllBe(message => message.HasOrderingKey && message.OrderingKey!.SequenceEqual(orderingKey));
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicMessagesWithEventTime
    [Fact]
    public async Task Receive_GivenMessagesWithEventTimeRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithEventTime()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 100;
        const ulong eventTime = 1_700_000_000_000;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            await producer.NewMessage().EventTime(eventTime).Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Count.ShouldBe(sendMessages);
        deadLettered.ShouldAllBe(message => message.HasEventTime && message.EventTime == eventTime);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithProducerName
    [Fact]
    public async Task Receive_GivenConsumerName_ShouldSendToDeadLetterTopicWithJavaCompatibleProducerName()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount, "my-consumer");
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        var expected = new Regex($"^{Regex.Escape(topicName)}-{SubscriptionName}-my-consumer-[a-zA-Z0-9]{{5}}-DLQ$");
        deadLettered.Count.ShouldBe(sendMessages);
        deadLettered.ShouldAllBe(message => expected.IsMatch(message.ProducerName));
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopic
    // The rows are produceLargeMessages false (the producer batches, and the messages are "Hello Pulsar [i]") and true (the producer chunks, and the messages are 10 KiB). The upstream producer batches,
    // but it sends each message with a blocking send, so a batch has a single message. The messages of the second row are smaller than the maximum message size of the broker, so they are not split.
    // The backlog of the subscription is asserted too, because a consumer that is created after the first one is closed does not receive the messages that the first one did not acknowledge.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Receive_GivenMessagesRedeliveredPastMaxRedeliverCount_ShouldSendAllToDeadLetterTopicAndAcknowledgeOriginals(bool produceLargeMessages)
    {
        //Arrange
        const int maxRedeliverCount = 2;
        const int sendMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(100)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var newClient = CreateClient();
        await using var deadLetterConsumer = newClient.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .Topic($"{topicName}-{SubscriptionName}-DLQ")
            .Create();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .EnableBatching(!produceLargeMessages)
            .EnableChunking(produceLargeMessages)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var messageContent = new Dictionary<int, string>();
        for (var i = 0; i < sendMessages; i++)
        {
            var data = produceLargeMessages ? CreateMessagePayload(1024 * 10) : $"Hello Pulsar [{i}]";
            _ = await producer.NewMessage().Key(i.ToString()).Send(Encoding.UTF8.GetBytes(data), _cts.Token);
            messageContent[i] = data;
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1), TimeSpan.FromSeconds(5));
        var deadLettered = new List<IMessage<byte[]>>();
        for (var i = 0; i < sendMessages; i++)
        {
            var message = await Receive(deadLetterConsumer, TimeSpan.FromSeconds(5));
            await deadLetterConsumer.Acknowledge(message, _cts.Token);
            deadLettered.Add(message);
        }

        var backlog = await GetBacklog(topicName, SubscriptionName);
        await deadLetterConsumer.DisposeAsync();
        await consumer.DisposeAsync();
        await using var checkConsumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        var checkMessage = await TransactionFixture.ReceiveOrNull(checkConsumer, TimeSpan.FromSeconds(3), _cts.Token);

        //Assert
        deadLettered.ShouldAllBe(message => Encoding.UTF8.GetString(message.Value()) == messageContent[int.Parse(message.Key!)]);
        deadLettered.Select(message => int.Parse(message.Key!)).ToHashSet().ShouldBe(messageContent.Keys.ToHashSet(), true);
        checkMessage.ShouldBeNull();
        backlog.ShouldBe(0);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicHasOriginalInfo
    // Additional coverage: 100 messages and a maximum redelivery count of 3 instead of 10 messages and 1. The upstream producer batches, but it sends each message with a blocking send, so a batch has
    // a single message. The batching row sends the messages without waiting for the broker, in batches of 10 messages, so the dead letter topic receives whole batches.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Receive_GivenMessagesRedeliveredPastMaxRedeliverCount_ShouldSendAllToDeadLetterTopicWithOriginalInfoAndAcknowledgeOriginals(bool batching)
    {
        //Arrange
        const int maxRedeliverCount = 3;
        const int sendMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName, batching, 10);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var receipts = new Dictionary<string, Task<MessageId>>();
        for (var i = 0; i < sendMessages; i++)
        {
            var value = $"Hello Pulsar [{i}]";
            receipts[value] = await SendAsync(producer, value);
        }

        await producer.Flush(_cts.Token);
        await Task.WhenAll(receipts.Values).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        var messageIds = receipts.ToDictionary(receipt => receipt.Key, receipt => receipt.Value.Result);

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Select(message => message.Value()).ToHashSet().ShouldBe(messageIds.Keys.ToHashSet(), true);
        messageIds.Values.Select(messageId => messageId.BatchIndex >= 0).Distinct().ShouldBe([batching]);

        foreach (var message in deadLettered)
        {
            var sentId = messageIds[message.Value()];
            message.Properties["REAL_TOPIC"].ShouldBe(topicName);
            message.Properties["REAL_SUBSCRIPTION"].ShouldBe(SubscriptionName);
            message.Properties["ORIGIN_MESSAGE_ID"].ShouldBe(FormatMessageId(sentId));
            message.Properties["ORIGIN_MESSAGE_IDY_TIME"].ShouldBe(FormatMessageId(sentId));
        }

        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
        (await GetBacklog(topicName, SubscriptionName)).ShouldBe(0);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicByCustomTopicName
    [Fact]
    public async Task Receive_GivenCustomDeadLetterTopic_ShouldSendToCustomTopic()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var customTopicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, new DeadLetterPolicy(maxRedeliverCount) { DeadLetterTopic = customTopicName });
        await using var deadLetterConsumer = CreateConsumerOfTopic(client, customTopicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Count.ShouldBe(sendMessages);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterWithoutConsumerReceiveImmediately
    [Fact]
    public async Task Receive_GivenConsumerNotReceivingForFiveSeconds_ShouldNotSendToDeadLetterTopic()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await Task.Delay(TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        (await TransactionFixture.ReceiveOrNull(deadLetterConsumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldBeNull();
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithInitialSubscription
    [Fact]
    public async Task Receive_GivenInitialSubscriptionName_ShouldCreateSubscriptionOnDeadLetterTopic()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 10;
        const string initialSubscriptionName = "init-sub";
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var deadLetterTopicName = $"{topicName}-{SubscriptionName}-DLQ";
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName, new DeadLetterPolicy(maxRedeliverCount) { InitialSubscriptionName = initialSubscriptionName });
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.Send($"Hello Pulsar [{i}]", _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var subscriptions = await GetSubscriptions(deadLetterTopicName);

        //Assert
        subscriptions.ShouldContain(initialSubscriptionName);
    }

    // Upstream: DeadLetterTopicTest#testMultipleSameNameConsumersToDeadLetterTopic
    // The message listeners of the upstream consumers only count the messages, and never acknowledge them. Here, a receive loop does the same.
    [Fact]
    public async Task Receive_GivenThreeConsumersWithSameNameNeverAcknowledging_ShouldDeliverEachMessageMaxRedeliverPlusOneTimesAndDeadLetterEachOnce()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int messageCount = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer1 = CreateNeverAcknowledgingConsumer(client, topicName, maxRedeliverCount, "my-consumer");
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var consumer2 = CreateNeverAcknowledgingConsumer(client, topicName, maxRedeliverCount, "my-consumer");
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var consumer3 = CreateNeverAcknowledgingConsumer(client, topicName, maxRedeliverCount, "my-consumer");
        await consumer3.StateChangedTo(ConsumerState.Active, _cts.Token);
        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var totalReceived = new ReceivedCounter();
        var receiving = Task.WhenAll(CountReceivedMessages(consumer1, totalReceived, receiveCts.Token), CountReceivedMessages(consumer2, totalReceived, receiveCts.Token), CountReceivedMessages(consumer3, totalReceived, receiveCts.Token));
        await using var producer = CreateProducer(client, topicName);

        for (var i = 0; i < messageCount; i++)
        {
            _ = await producer.Send($"Message [{i}]", _cts.Token);
        }

        await using var deadLetterConsumer = CreateDeadLetterConsumerOf(client, Schema.String, $"{topicName}-DLQ", "my-subscription-DuplicatedMessage-DLQ");

        //Act
        var totalInDeadLetter = 0;
        while (await TransactionFixture.ReceiveOrNull(deadLetterConsumer, TimeSpan.FromSeconds(10), _cts.Token) is { } message)
        {
            await deadLetterConsumer.Acknowledge(message, _cts.Token);
            totalInDeadLetter++;
        }

        await receiveCts.CancelAsync();
        await receiving;

        //Assert
        totalReceived.Value.ShouldBe(messageCount * (maxRedeliverCount + 1));
        totalInDeadLetter.ShouldBe(messageCount);
    }

    // Upstream: DeadLetterTopicTest#testDuplicatedMessageSendToDeadLetterTopic
    // The message listeners of the upstream consumers only count the messages, and never acknowledge them. Here, a receive loop does the same.
    [Fact]
    public async Task Receive_GivenThreeConsumersNeverAcknowledging_ShouldNotSendDuplicatesToDeadLetterTopic()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int messageCount = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer1 = CreateNeverAcknowledgingConsumer(client, topicName, maxRedeliverCount, null);
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var consumer2 = CreateNeverAcknowledgingConsumer(client, topicName, maxRedeliverCount, null);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var consumer3 = CreateNeverAcknowledgingConsumer(client, topicName, maxRedeliverCount, null);
        await consumer3.StateChangedTo(ConsumerState.Active, _cts.Token);
        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var totalReceived = new ReceivedCounter();
        var receiving = Task.WhenAll(CountReceivedMessages(consumer1, totalReceived, receiveCts.Token), CountReceivedMessages(consumer2, totalReceived, receiveCts.Token), CountReceivedMessages(consumer3, totalReceived, receiveCts.Token));
        await using var producer = CreateProducer(client, topicName);

        for (var i = 0; i < messageCount; i++)
        {
            _ = await producer.Send($"Message [{i}]", _cts.Token);
        }

        await using var deadLetterConsumer = CreateDeadLetterConsumerOf(client, Schema.String, $"{topicName}-DLQ", "my-subscription-DuplicatedMessage-DLQ");

        //Act
        var totalInDeadLetter = 0;
        while (await TransactionFixture.ReceiveOrNull(deadLetterConsumer, TimeSpan.FromSeconds(10), _cts.Token) is { } message)
        {
            await deadLetterConsumer.Acknowledge(message, _cts.Token);
            totalInDeadLetter++;
        }

        await receiveCts.CancelAsync();
        await receiving;

        //Assert
        totalReceived.Value.ShouldBe(messageCount * (maxRedeliverCount + 1));
        totalInDeadLetter.ShouldBe(messageCount);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicUnderPartitionedTopicWithKeyShareType
    // The upstream KeySharedPolicy autoSplitHashRange is the default of DotPulsar. The message has no key and is published to one of the partitions, so it is sent to the dead letter topic of that partition.
    // The backlog of the subscription is asserted too, because a consumer that is created after the first one is closed does not receive the messages that the first one did not acknowledge.
    [Fact]
    public async Task Receive_GivenPartitionedTopicWithKeySharedSubscription_ShouldSendToPerPartitionDeadLetterTopics()
    {
        //Arrange
        const int maxRedeliverCount = 2;
        const int sendMessages = 1;
        var topicName = await _fixture.CreatePartitionedTopic(2, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(100)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.KeyShared)
            .Topic(topicName)
            .Create();
        await using var deadLetterConsumer0 = CreateDeadLetterConsumerOf(client, Schema.ByteArray, $"{topicName}-partition-0-{SubscriptionName}-DLQ", SubscriptionName);
        await using var deadLetterConsumer1 = CreateDeadLetterConsumerOf(client, Schema.ByteArray, $"{topicName}-partition-1-{SubscriptionName}-DLQ", SubscriptionName);
        await using var producer = client.NewProducer(Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topicName).Create();

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.Send(Encoding.UTF8.GetBytes($"Hello Pulsar [{i}]"), _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var totalInDeadLetter = 0;
        do
        {
            var message = await TransactionFixture.ReceiveOrNull(deadLetterConsumer0, TimeSpan.FromSeconds(3), _cts.Token);

            if (message is null)
                break;

            await deadLetterConsumer0.Acknowledge(message, _cts.Token);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        do
        {
            var message = await TransactionFixture.ReceiveOrNull(deadLetterConsumer1, TimeSpan.FromSeconds(3), _cts.Token);

            if (message is null)
                break;

            await deadLetterConsumer1.Acknowledge(message, _cts.Token);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        var backlog = await GetBacklog(topicName, SubscriptionName, true);
        await deadLetterConsumer0.DisposeAsync();
        await deadLetterConsumer1.DisposeAsync();
        await consumer.DisposeAsync();
        await using var checkConsumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.KeyShared)
            .Topic(topicName)
            .Create();
        var checkMessage = await TransactionFixture.ReceiveOrNull(checkConsumer, TimeSpan.FromSeconds(3), _cts.Token);

        //Assert
        totalInDeadLetter.ShouldBe(sendMessages);
        checkMessage.ShouldBeNull();
        backlog.ShouldBe(0);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithInitialSubscriptionAndMultiConsumers
    // Each consumer creates its dead letter producer after its final redelivery, so the dead letter topic has two publishers.
    [Fact]
    public async Task Receive_GivenTwoConsumersWithInitialSubscriptionName_ShouldCreateOneDeadLetterProducerPerConsumer()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int sendMessages = 100;
        const string initialSubscriptionName = "init-sub";
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var deadLetterTopicName = $"{topicName}-{SubscriptionName}-DLQ";
        await using var client = CreateClient();
        await using var consumer = CreateConsumerWithInitialSubscription(client, topicName, maxRedeliverCount, initialSubscriptionName);
        await using var otherConsumer = CreateConsumerWithInitialSubscription(client, topicName, maxRedeliverCount, initialSubscriptionName);
        await using var newClient = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topicName).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await otherConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.Send(Encoding.UTF8.GetBytes($"Hello Pulsar [{i}]"), _cts.Token);
        }

        //Act
        var totalReceived = new ReceivedCounter();
        await Task.WhenAll(ReceiveForDeadLetterQueue(consumer, totalReceived, sendMessages, maxRedeliverCount), ReceiveForDeadLetterQueue(otherConsumer, totalReceived, sendMessages, maxRedeliverCount))
            .WaitAsync(TimeSpan.FromSeconds(10), _cts.Token);
        await Eventually(async () =>
        {
            (await GetTopics(topicName)).ShouldContain(deadLetterTopicName);
            (await GetJson($"{ToAdminPath(deadLetterTopicName)}/subscriptions")).EnumerateArray().Select(element => element.GetString()).ShouldContain(initialSubscriptionName);
            (await GetJson($"{ToAdminPath(deadLetterTopicName)}/stats")).GetProperty("publishers").GetArrayLength().ShouldBe(2);
        }, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
        await using var deadLetterConsumer = newClient.NewConsumer(Schema.ByteArray)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(initialSubscriptionName)
            .Topic(deadLetterTopicName)
            .Create();
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages, TimeSpan.FromSeconds(10));

        foreach (var message in deadLettered)
        {
            await deadLetterConsumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        deadLettered.Count.ShouldBe(sendMessages);
        totalReceived.Value.ShouldBe(sendMessages * (maxRedeliverCount + 1));
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithMaxUnackedMessagesBlocking
    // The upstream negative acknowledge redelivery delay of 1 millisecond is below the minimum of DotPulsar, so it is 100 milliseconds. The broker sends the consumer no more than the maximum number of
    // unacknowledged messages, so the messages must be negative acknowledged, or dead lettered, before the broker sends it more.
    [Fact]
    public async Task NegativeAcknowledge_GivenMaxUnackedMessagesPolicy_ShouldEventuallySendAllMessagesToDeadLetterTopic()
    {
        //Arrange
        const int maxRedeliverCount = 3;
        const int maxUnackedMessages = 100;
        const int sendMessages = 1000;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var deadLetterTopicName = $"{topicName}-{SubscriptionName}-DLQ";
        await SetTopicPolicy(topicName, "maxUnackedMessagesOnConsumer", maxUnackedMessages);
        await SetTopicPolicy(topicName, "maxUnackedMessagesOnSubscription", maxUnackedMessages);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount) { DeadLetterTopic = deadLetterTopicName })
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(200)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(1))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var deadLetterConsumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .Topic(deadLetterTopicName)
            .Create();
        await using var producer = client.NewProducer(Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topicName).Create();

        for (var i = 0; i < sendMessages; i++)
        {
            _ = await producer.NewMessage().Send(Encoding.UTF8.GetBytes($"Hello Pulsar [{i}]"), _cts.Token);
        }

        //Act
        var receivedMessages = new HashSet<string>();
        for (var totalReceived = 0; totalReceived < sendMessages * (maxRedeliverCount + 1); totalReceived++)
        {
            var message = await Receive(consumer);
            _ = receivedMessages.Add(Encoding.UTF8.GetString(message.Value()));
            await consumer.NegativeAcknowledge(message, _cts.Token);
        }

        var deadLettered = new List<IMessage<byte[]>>();
        for (var i = 0; i < sendMessages; i++)
        {
            var message = await Receive(deadLetterConsumer);
            await deadLetterConsumer.Acknowledge(message, _cts.Token);
            deadLettered.Add(message);
        }

        //Assert
        receivedMessages.Count.ShouldBe(sendMessages);
        deadLettered.Select(message => Encoding.UTF8.GetString(message.Value())).ToHashSet().ShouldBe(receivedMessages, true);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithPoolMessages (redeliver the ids of the messages that were received in the last round, the data provider enableBatching)
    // The batching row has all 5 messages in one batch (BatchingMaxMessages of 5), so the whole batch is a candidate.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RedeliverUnacknowledgedMessages_GivenCandidatesFromLastDelivery_ShouldSendAllToDeadLetterTopic(bool enableBatching)
    {
        //Arrange
        const int maxRedeliverCount = 2;
        const int sendMessages = 5;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName, enableBatching, sendMessages);
        await using var consumer = client.NewConsumer(Schema.String)
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < sendMessages; i++)
        {
            receipts.Add(await SendAsync(producer, $"Hello Pulsar [{i}]"));
        }

        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Select(messageId => messageId.BatchIndex >= 0).Distinct().ShouldBe([enableBatching]);

        //Act
        for (var round = 0; round <= maxRedeliverCount; round++)
        {
            var received = new List<MessageId>();
            for (var i = 0; i < sendMessages; i++)
            {
                var message = await Receive(consumer);
                message.RedeliveryCount.ShouldBe((uint) round);
                received.Add(message.MessageId);
            }

            await consumer.RedeliverUnacknowledgedMessages(received, _cts.Token);
        }

        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.Select(message => message.Value()).ToHashSet().ShouldBe(Enumerable.Range(0, sendMessages).Select(i => $"Hello Pulsar [{i}]").ToHashSet(), true);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
        (await GetBacklog(topicName, SubscriptionName)).ShouldBe(0);
    }

    // Upstream: DeadLetterTopicTest#testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId
    // The map of the dead letter candidates cannot be observed, so the test checks that the whole batch is sent to the dead letter topic when the id of one message of the batch is redelivered,
    // and that redelivering the same id again does not send the batch a second time. The unit tests of BatchHandler and DeadLetterHandler cover the registration and the removal of the entry.
    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenBatchMessageIdOfDeadLetterCandidate_ShouldSendWholeBatchToDeadLetterTopicOnce()
    {
        //Arrange
        const int batchSize = 5;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName, true, batchSize);
        await using var consumer = client.NewConsumer(Schema.String)
            .DeadLetterPolicy(new DeadLetterPolicy(1))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < batchSize; i++)
        {
            receipts.Add(await SendAsync(producer, $"message-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        // First delivery: the redelivery count is 0, which is below the maximum, so nothing is registered yet
        for (var i = 0; i < batchSize; i++)
        {
            _ = await Receive(consumer);
        }

        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);

        // Second delivery: the redelivery count reaches the maximum, so the whole batch is registered as a candidate
        var messages = new List<IMessage<string>>();
        for (var i = 0; i < batchSize; i++)
        {
            messages.Add(await Receive(consumer));
        }

        // The id of a single message of the batch, which still carries its batch index
        var batchMessageId = messages[0].MessageId;

        //Act
        await consumer.RedeliverUnacknowledgedMessages([batchMessageId], _cts.Token);
        var deadLettered = await ReceiveAll(deadLetterConsumer, batchSize);
        await consumer.RedeliverUnacknowledgedMessages([batchMessageId], _cts.Token);

        //Assert
        batchMessageId.BatchIndex.ShouldBeGreaterThanOrEqualTo(0);
        messages.ShouldAllBe(message => message.RedeliveryCount == 1);
        deadLettered.Select(message => message.Value()).ToHashSet().ShouldBe(Enumerable.Range(0, batchSize).Select(i => $"message-{i}").ToHashSet(), true);
        (await TransactionFixture.ReceiveOrNull(deadLetterConsumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();
        (await GetBacklog(topicName, SubscriptionName)).ShouldBe(0);
    }

    // Upstream: ConsumerImpl#initDeadLetterProducerIfNeeded (enableChunking(true)), C++ MessageChunkingTest#testChunkedMessageDLQ
    // A message that the consumer assembled from chunks is larger than the maximum message size of the broker, so the dead letter producer must split it in chunks again.
    [Fact]
    public async Task Receive_GivenChunkedMessageRedeliveredPastMaxRedeliverCount_ShouldSendItToDeadLetterTopicInChunks()
    {
        //Arrange
        const int maxRedeliverCount = 1;
        const int messageSize = 12_000_000; // The maximum message size of the broker is 5 MiB
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String).EnableChunking(true).StateChangedHandler(_testOutputHelper.Log).Topic(topicName).Create();
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var value = new string('c', messageSize);
        var messageId = await producer.Send(value, _cts.Token);

        //Act
        await ReceiveWithoutAcknowledging(consumer, maxRedeliverCount + 1);
        var deadLettered = await Receive(deadLetterConsumer);

        //Assert
        messageId.FirstChunkMessageId.ShouldNotBeNull();
        deadLettered.Value().ShouldBe(value);
        deadLettered.MessageId.FirstChunkMessageId.ShouldNotBeNull();
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
        (await GetBacklog(topicName, SubscriptionName)).ShouldBe(0);
    }

    private async Task<long> GetBacklog(string topicName, string subscription, bool partitioned = false)
    {
        using var adminClient = _fixture.CreateAdminClient();
        var path = ToAdminPath(topicName);
        var statsPath = partitioned ? $"{path}/partitioned-stats" : $"{path}/stats";

        for (var attempt = 0; attempt < 10; attempt++)
        {
            using var response = await adminClient.GetAsync(statsPath, _cts.Token);
            response.EnsureSuccessStatusCode();
            using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var backlog = document.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();

            if (backlog == 0)
                return 0;

            await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        }

        using var lastResponse = await adminClient.GetAsync(statsPath, _cts.Token);
        using var lastDocument = JsonDocument.Parse(await lastResponse.Content.ReadAsStringAsync());
        return lastDocument.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();
    }

    private async Task<List<string>> GetSubscriptions(string topicName)
    {
        using var adminClient = _fixture.CreateAdminClient();
        var path = ToAdminPath(topicName);

        for (var attempt = 0; attempt < 10; attempt++)
        {
            using var response = await adminClient.GetAsync($"{path}/subscriptions", _cts.Token);

            if (response.IsSuccessStatusCode)
            {
                using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                var subscriptions = document.RootElement.EnumerateArray().Select(element => element.GetString()!).ToList();

                if (subscriptions.Count > 0)
                    return subscriptions;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        }

        return [];
    }

    private static string ToAdminPath(string topicName)
    {
        // persistent://public/default/name => /admin/v2/persistent/public/default/name
        var separator = topicName.IndexOf("://", StringComparison.Ordinal);
        return $"/admin/v2/{topicName[..separator]}/{topicName[(separator + 3)..]}";
    }

    private async Task<JsonElement> GetJson(string path)
    {
        using var adminClient = _fixture.CreateAdminClient();
        using var response = await adminClient.GetAsync(path, _cts.Token);
        response.EnsureSuccessStatusCode();
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return document.RootElement.Clone();
    }

    private async Task<string> GetText(string path)
    {
        using var adminClient = _fixture.CreateAdminClient();
        using var response = await adminClient.GetAsync(path, _cts.Token);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadAsStringAsync()).Trim();
    }

    // The topics of the namespace of the topic
    private async Task<List<string>> GetTopics(string topicName)
    {
        var path = ToAdminPath(topicName);
        return (await GetJson(path[..path.LastIndexOf('/')])).EnumerateArray().Select(element => element.GetString()!).ToList();
    }

    // The number of the topic policy is the body of the request. The policy is read back before the test goes on, because the broker applies it when it has read it from the system topic.
    private async Task SetTopicPolicy(string topicName, string policy, int value)
    {
        var path = $"{ToAdminPath(topicName)}/{policy}";
        using var adminClient = _fixture.CreateAdminClient();
        using var content = new StringContent(value.ToString(), Encoding.UTF8, "application/json");
        using var response = await adminClient.PostAsync(path, content, _cts.Token);
        response.EnsureSuccessStatusCode();
        await Eventually(async () => (await GetText(path)).ShouldBe(value.ToString()), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(500));
    }

    // Java Awaitility.await().atMost(...).pollInterval(...).untilAsserted(...)
    private async Task Eventually(Func<Task> assertion, TimeSpan atMost, TimeSpan pollInterval)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await assertion();
                return;
            }
            catch (Exception exception) when ((exception is ShouldAssertException || exception is HttpRequestException) && stopwatch.Elapsed < atMost)
            {
                await Task.Delay(pollInterval, _cts.Token);
            }
        }
    }

    private static string CreateMessagePayload(int size)
        => string.Create(size, 0, static (span, _) =>
        {
            for (var i = 0; i < span.Length; ++i)
            {
                span[i] = (char) ('0' + Random.Shared.Next(10));
            }
        });

    // The message listener of the upstream tests, which counts the messages and never acknowledges them
    private static async Task CountReceivedMessages<TMessage>(IConsumer<TMessage> consumer, ReceivedCounter totalReceived, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var _ in consumer.Messages(cancellationToken))
            {
                totalReceived.Increment();
            }
        }
        catch (OperationCanceledException)
        {
            // The test has stopped the receiving
        }
    }

    // Upstream: DeadLetterTopicTest#consumerReceiveForDLQ
    private async Task ReceiveForDeadLetterQueue<TMessage>(IConsumer<TMessage> consumer, ReceivedCounter totalReceived, int sendMessages, int maxRedeliverCount)
    {
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token) is not null)
        {
            totalReceived.Increment();

            if (totalReceived.Value >= sendMessages * (maxRedeliverCount + 1))
                break;
        }
    }

    // The consumers of the upstream tests that have a message listener, with the ack timeout and the negative acknowledge redelivery delay of 1001 milliseconds of the upstream
    private IConsumer<string> CreateNeverAcknowledgingConsumer(IPulsarClient client, string topicName, uint maxRedeliverCount, string? consumerName)
    {
        var builder = client.NewConsumer(Schema.String)
            .AckTimeout(TimeSpan.FromMilliseconds(1001))
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount) { DeadLetterTopic = $"{topicName}-DLQ" })
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(100)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(1001))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("my-subscription-DuplicatedMessage")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName);

        if (consumerName is not null)
            builder.ConsumerName(consumerName);

        return builder.Create();
    }

    private IConsumer<byte[]> CreateConsumerWithInitialSubscription(IPulsarClient client, string topicName, uint maxRedeliverCount, string initialSubscriptionName)
        => client.NewConsumer(Schema.ByteArray)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount) { InitialSubscriptionName = initialSubscriptionName })
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(20)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();

    // The default of the Java consumer is the exclusive subscription type
    private IConsumer<TMessage> CreateDeadLetterConsumerOf<TMessage>(IPulsarClient client, ISchema<TMessage> schema, string topicName, string subscriptionName)
        => client.NewConsumer(schema)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .Topic(topicName)
            .Create();

    private sealed class ReceivedCounter
    {
        private int _value;

        public int Value => Volatile.Read(ref _value);

        public void Increment() => Interlocked.Increment(ref _value);
    }

    private async Task ReceiveWithoutAcknowledging<TMessage>(IConsumer<TMessage> consumer, int count, TimeSpan? timeout = null)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await Receive(consumer, timeout);
        }
    }

    private async Task<IMessage<TMessage>> Receive<TMessage>(IConsumer<TMessage> consumer, TimeSpan? timeout = null)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, timeout ?? TimeSpan.FromSeconds(30), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private async Task<List<IMessage<TMessage>>> ReceiveAll<TMessage>(IConsumer<TMessage> consumer, int count, TimeSpan? timeout = null)
    {
        var messages = new List<IMessage<TMessage>>();

        for (var i = 0; i < count; i++)
        {
            messages.Add(await Receive(consumer, timeout));
        }

        return messages;
    }

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topicName, uint maxRedeliverCount, string? consumerName = null)
        => CreateConsumer(client, topicName, new DeadLetterPolicy(maxRedeliverCount), consumerName);

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topicName, DeadLetterPolicy deadLetterPolicy, string? consumerName = null)
        => CreateConsumer(client, topicName, SubscriptionType.Shared, deadLetterPolicy, consumerName);

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topicName, SubscriptionType subscriptionType, DeadLetterPolicy? deadLetterPolicy, string? consumerName = null)
    {
        var builder = client.NewConsumer(Schema.String)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(subscriptionType)
            .Topic(topicName);

        if (deadLetterPolicy is not null)
            builder.DeadLetterPolicy(deadLetterPolicy);

        if (consumerName is not null)
            builder.ConsumerName(consumerName);

        return builder.Create();
    }

    private IConsumer<string> CreateDeadLetterConsumer(IPulsarClient client, string topicName)
        => CreateConsumerOfTopic(client, $"{topicName}-{SubscriptionName}-DLQ");

    private IConsumer<string> CreateConsumerOfTopic(IPulsarClient client, string topicName)
        => client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("dead-letter-subscription")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();

    private IProducer<string> CreateProducer(IPulsarClient client, string topicName)
        => client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

    // A batch is closed when it has the maximum number of messages or after 1 second, whichever comes first
    private IProducer<string> CreateProducer(IPulsarClient client, string topicName, bool enableBatching, uint batchingMaxMessages)
        => client.NewProducer(Schema.String)
            .BatchingMaxMessages(batchingMaxMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .EnableBatching(enableBatching)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

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

    // The Java MessageId.toString() of a message id, which has the batch index only for the id of a message in a batch
    private static string FormatMessageId(MessageId messageId)
        => messageId.BatchIndex < 0
            ? $"{messageId.LedgerId}:{messageId.EntryId}:{messageId.Partition}"
            : $"{messageId.LedgerId}:{messageId.EntryId}:{messageId.Partition}:{messageId.BatchIndex}";

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();
}
