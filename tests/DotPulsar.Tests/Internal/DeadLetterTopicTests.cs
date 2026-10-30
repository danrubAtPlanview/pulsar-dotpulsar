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

    // Upstream: DeadLetterTopicTest#testDeadLetterTopic (produceLargeMessages = false, the producer batches)
    // Upstream: DeadLetterTopicTest#testDeadLetterTopicHasOriginalInfo
    // The upstream producer batches, but it sends each message with a blocking send, so a batch has a single message. The batching row sends the messages without waiting for the broker,
    // in batches of 10 messages, so the dead letter topic receives whole batches.
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

    // Upstream: DeadLetterTopicTest#testDeadLetterTopic (produceLargeMessages = true)
    // The upstream messages are 10 KiB, which is below the maximum message size of the broker, so they are not split, but the producer has chunking enabled.
    [Fact]
    public async Task Receive_GivenMessagesFromProducerWithChunkingRedeliveredPastMaxRedeliverCount_ShouldSendAllToDeadLetterTopic()
    {
        //Arrange
        const int maxRedeliverCount = 2;
        const int sendMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String).EnableChunking(true).StateChangedHandler(_testOutputHelper.Log).Topic(topicName).Create();
        await using var consumer = CreateConsumer(client, topicName, maxRedeliverCount);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var values = new Dictionary<string, string>();
        for (var i = 0; i < sendMessages; i++)
        {
            var value = new string((char) ('a' + i % 26), 1024 * 10) + i;
            values[i.ToString()] = value;
            _ = await producer.NewMessage().Key(i.ToString()).Send(value, _cts.Token);
        }

        //Act
        await ReceiveWithoutAcknowledging(consumer, sendMessages * (maxRedeliverCount + 1));
        var deadLettered = await ReceiveAll(deadLetterConsumer, sendMessages);

        //Assert
        deadLettered.ShouldAllBe(message => values[message.Key!] == message.Value());
        deadLettered.Select(message => message.Key).ToHashSet().ShouldBe(values.Keys.ToHashSet(), true);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
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

    private async Task<long> GetBacklog(string topicName, string subscription)
    {
        using var adminClient = _fixture.CreateAdminClient();
        var path = ToAdminPath(topicName);

        for (var attempt = 0; attempt < 10; attempt++)
        {
            using var response = await adminClient.GetAsync($"{path}/stats", _cts.Token);
            response.EnsureSuccessStatusCode();
            using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var backlog = document.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();

            if (backlog == 0)
                return 0;

            await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        }

        using var lastResponse = await adminClient.GetAsync($"{path}/stats", _cts.Token);
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

    private async Task ReceiveWithoutAcknowledging(IConsumer<string> consumer, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await Receive(consumer);
        }
    }

    private async Task<IMessage<string>> Receive(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(30), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private async Task<List<IMessage<string>>> ReceiveAll(IConsumer<string> consumer, int count)
    {
        var messages = new List<IMessage<string>>();

        for (var i = 0; i < count; i++)
        {
            messages.Add(await Receive(consumer));
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
