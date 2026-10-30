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

namespace DotPulsar.Tests;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Tests.Internal;
using DotPulsar.Tests.Internal.Chunking;
using System.Text.Json;

// The consumer and reader side of Java MessageChunkingTest. The messages that are split in chunks are produced by the perf tool of the broker image, and the chunks of the tests
// that need to control them are sent one by one with ChunkTestHelper.SendSingleChunk. TODO(WP8-producer): produce the large messages with IProducer chunking, and port the rest of
// MessageChunkingTest and MessageChunkingSharedTest, which need the producer and a broker with a small maximum message size.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class MessageChunkingTests : IDisposable
{
    // The default maximum message size of the broker is 5 MiB, so this is 3 chunks
    private const int ChunkedMessageSize = 12_000_000;

    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public MessageChunkingTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: MessageChunkingTest#testLargeMessage
    [Fact]
    public async Task Receive_GivenMessageThatWasChunkedByAnotherClient_ShouldDeliverItAssembledWithTheIdsOfItsChunks()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);

        //Act
        var message = await Receive(consumer);

        //Assert
        message.Data.Length.ShouldBe(ChunkedMessageSize);
        message.MessageId.FirstChunkMessageId.ShouldNotBeNull();
        message.MessageId.FirstChunkMessageId.CompareTo(message.MessageId).ShouldBeNegative();
        message.MessageId.GetChunkMessageIds()!.Length.ShouldBe(3);
    }

    // Upstream: MessageChunkingTest#testLargeMessage (the broker has no backlog left when every chunk is acknowledged)
    [Fact]
    public async Task Acknowledge_GivenChunkedMessages_ShouldAcknowledgeEveryChunk()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive, subscription);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);
        var messages = await ReceiveAll(consumer);

        //Act
        foreach (var message in messages)
        {
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        messages.ShouldNotBeEmpty();
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // Upstream: MessageChunkingTest#testLargeMessage (a cumulative acknowledgment of the last chunk acknowledges all the chunks before it)
    [Fact]
    public async Task AcknowledgeCumulative_GivenChunkedMessages_ShouldAcknowledgeEveryChunk()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive, subscription);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);
        var messages = await ReceiveAll(consumer);

        //Act
        await consumer.AcknowledgeCumulative(messages[^1], _cts.Token);

        //Assert
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // Upstream: MessageChunkingTest#testLargeMessageAckTimeOut
    [Fact]
    public async Task Receive_GivenChunkedMessageThatIsNotAcknowledgedBeforeTheAckTimeout_ShouldDeliverItAgain()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Shared, configure: builder => builder.AckTimeout(TimeSpan.FromSeconds(2)));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);
        var first = await Receive(consumer);

        //Act
        var redelivered = await ReceiveUntil(consumer, message => message.MessageId == first.MessageId, 10);

        //Assert
        redelivered.Data.Length.ShouldBe(ChunkedMessageSize);
        redelivered.MessageId.FirstChunkMessageId.ShouldBe(first.MessageId.FirstChunkMessageId);
        redelivered.MessageId.GetChunkMessageIds()!.Length.ShouldBe(3);
    }

    // Upstream: MessageChunkingSharedTest#testNegativeAckChunkedMessage
    [Fact]
    public async Task NegativeAcknowledge_GivenChunkedMessage_ShouldDeliverItAgain()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Shared, configure: builder => builder.NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(1)));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);
        var first = await Receive(consumer);

        //Act
        await consumer.NegativeAcknowledge(first, _cts.Token);
        var redelivered = await ReceiveUntil(consumer, message => message.MessageId == first.MessageId, 10);

        //Assert
        redelivered.Data.Length.ShouldBe(ChunkedMessageSize);
        redelivered.MessageId.GetChunkMessageIds()!.Length.ShouldBe(3);
    }

    // Upstream: MessageChunkingTest#testSeekChunkMessages (the reader that is inclusive)
    [Fact]
    public async Task Receive_GivenReaderThatStartsAtChunkedMessageId_ShouldDeliverTheMessage()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);
        var expected = await Receive(consumer);
        await using var reader = client.NewReader(Schema.ByteArray).StartMessageId(expected.MessageId).Topic(topic).StateChangedHandler(_testOutputHelper.Log).Create();

        //Act
        var actual = await Receive(reader);

        //Assert
        actual.MessageId.ShouldBe(expected.MessageId);
        actual.Data.Length.ShouldBe(ChunkedMessageSize);
        actual.MessageId.FirstChunkMessageId.ShouldBe(expected.MessageId.FirstChunkMessageId);
    }

    // Upstream: MessageChunkingTest#testSeekChunkMessages (the consumer that is inclusive)
    [Fact]
    public async Task Seek_GivenChunkedMessageId_ShouldDeliverTheMessageAgain()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, ChunkedMessageSize, _cts.Token);
        var first = await Receive(consumer);
        await consumer.Acknowledge(first, _cts.Token);

        //Act
        await consumer.Seek(first.MessageId, _cts.Token);
        var actual = await ReceiveUntil(consumer, message => message.MessageId == first.MessageId, 10);

        //Assert
        actual.Data.Length.ShouldBe(ChunkedMessageSize);
        actual.MessageId.FirstChunkMessageId.ShouldBe(first.MessageId.FirstChunkMessageId);
    }

    // Upstream: MessageChunkingTest#testMaxPendingChunkMessages
    [Fact]
    public async Task Receive_GivenMaxPendingChunkedMessageExceededAndAutoAckOldest_ShouldAcknowledgeTheOldestMessageAndNotDeliverIt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic);
        await using var consumer = CreateStringConsumer(client, topic, subscription, builder => builder.MaxPendingChunkedMessage(1).AutoAckOldestChunkedMessageOnQueueFull(true));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        await ChunkTestHelper.SendSingleChunk(producer, "0", 0, 3, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "0", 1, 3, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "1", 0, 2, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "1", 1, 2, _cts.Token);
        var message = await Receive(consumer);
        await consumer.Acknowledge(message, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "0", 1, 3, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "0", 2, 3, _cts.Token);

        //Assert
        message.Value().ShouldBe("chunk-1-0|chunk-1-1|");
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token)).ShouldBeNull();
        await WaitUntil(async () => (await GetSubscriptionStats(topic, subscription)).GetProperty("nonContiguousDeletedMessagesRanges").GetInt64() == 0);
    }

    // Upstream: MessageChunkingTest#testResendChunkMessagesWithoutAckHole
    [Fact]
    public async Task Acknowledge_GivenFirstChunkThatWasSentAgain_ShouldLeaveNoAckHole()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic);
        await using var consumer = CreateStringConsumer(client, topic, subscription);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "0", 0, 2, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "0", 0, 2, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "0", 1, 2, _cts.Token);

        //Act
        var message = await Receive(consumer);
        await consumer.Acknowledge(message, _cts.Token);

        //Assert
        message.Value().ShouldBe("chunk-0-0|chunk-0-1|");
        await WaitUntil(async () =>
        {
            var stats = await GetSubscriptionStats(topic, subscription);
            return stats.GetProperty("msgBacklog").GetInt64() == 0 && stats.GetProperty("nonContiguousDeletedMessagesRanges").GetInt64() == 0;
        });
    }

    // Upstream: MessageChunkingTest#testOrphanedLastChunkDoesNotStallDispatch
    [Fact]
    public async Task Receive_GivenTwelveOrphanedLastChunks_ShouldStillDeliverTheNextMessage()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic);
        await using var consumer = CreateStringConsumer(client, topic, CreateSubscriptionName(), builder => builder.MessagePrefetchCount(4).ExpireTimeOfIncompleteChunkedMessage(TimeSpan.FromSeconds(1)));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < 12; ++i)
        {
            await ChunkTestHelper.SendSingleChunk(producer, $"torn-{i}", 1, 2, _cts.Token);
        }

        //Act
        await ChunkTestHelper.SendSingleChunk(producer, "live", 0, 2, _cts.Token);
        await ChunkTestHelper.SendSingleChunk(producer, "live", 1, 2, _cts.Token);
        var message = await Receive(consumer, TimeSpan.FromSeconds(15));

        //Assert
        message.Value().ShouldBe("chunk-live-0|chunk-live-1|");
    }

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

    private IConsumer<byte[]> CreateConsumer(IPulsarClient client, string topic, SubscriptionType subscriptionType, string? subscription = null, Action<IConsumerBuilder<byte[]>>? configure = null)
    {
        var builder = client.NewConsumer(Schema.ByteArray)
            .AckReceiptEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription ?? CreateSubscriptionName())
            .SubscriptionType(subscriptionType)
            .Topic(topic);

        configure?.Invoke(builder);
        return builder.Create();
    }

    private IConsumer<string> CreateStringConsumer(IPulsarClient client, string topic, string subscription, Action<IConsumerBuilder<string>>? configure = null)
    {
        var builder = client.NewConsumer(Schema.String)
            .AckReceiptEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .Topic(topic);

        configure?.Invoke(builder);
        return builder.Create();
    }

    private IProducer<string> CreateProducer(IPulsarClient client, string topic)
        => client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private async Task<IMessage<TMessage>> Receive<TMessage>(IReceive<IMessage<TMessage>> receiver, TimeSpan? timeout = null)
    {
        var message = await TransactionFixture.ReceiveOrNull(receiver, timeout ?? TimeSpan.FromSeconds(60), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    // The perf tool can send a message or two more than it was asked to
    private async Task<List<IMessage<byte[]>>> ReceiveAll(IConsumer<byte[]> consumer)
    {
        var messages = new List<IMessage<byte[]>> { await Receive(consumer) };

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token) is { } message)
        {
            messages.Add(message);
        }

        return messages;
    }

    private async Task<IMessage<byte[]>> ReceiveUntil(IConsumer<byte[]> consumer, Func<IMessage<byte[]>, bool> predicate, int maximumNumberOfMessages)
    {
        for (var i = 0; i < maximumNumberOfMessages; ++i)
        {
            var message = await Receive(consumer);

            if (predicate(message))
                return message;
        }

        throw new Exception($"The message was not received within {maximumNumberOfMessages} messages");
    }

    private async Task<JsonElement> GetSubscriptionStats(string topic, string subscription)
    {
        using var httpClient = _fixture.CreateAdminClient();
        using var response = await httpClient.GetAsync($"/admin/v2/persistent/{topic.Replace("persistent://", string.Empty)}/stats", _cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(_cts.Token);
        using var json = await JsonDocument.ParseAsync(stream, cancellationToken: _cts.Token);
        return json.RootElement.GetProperty("subscriptions").GetProperty(subscription).Clone();
    }

    private async Task<long> GetBacklog(string topic, string subscription)
        => (await GetSubscriptionStats(topic, subscription)).GetProperty("msgBacklog").GetInt64();

    private async Task WaitUntil(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);

        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met within 30 seconds");

            await Task.Delay(TimeSpan.FromMilliseconds(200), _cts.Token);
        }
    }
}
