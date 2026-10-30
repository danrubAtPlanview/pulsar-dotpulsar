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
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Tests.Internal;
using DotPulsar.Tests.Internal.Chunking;
using System.Collections.Concurrent;
using System.Text.Json;

// Java MessageChunkingTest and MessageChunkingSharedTest, with the default maximum message size of the broker (5 MiB). The messages are split in chunks of a size that is set by ChunkMaxMessageSize or
// by the size of the message, and the chunks of the tests that need to control them are sent one by one with ChunkTestHelper.SendSingleChunk. The tests that need a broker with a small maximum message
// size are in MessageChunkingSmallMessageSizeTests.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class MessageChunkingTests : IDisposable
{
    // The default maximum message size of the broker is 5 MiB, so this is 3 chunks
    private const int ChunkedMessageSize = 12_000_000;

    // The size of the payload of the chunks of the producers of the tests that are not about the maximum message size of the broker
    private const int ChunkMaxMessageSize = 100;

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

    // Upstream: MessageChunkingSharedTest#testSingleConsumer
    [Fact]
    public async Task Receive_GivenChunkCountsAboveAndBelowMessagePrefetchCount_ShouldReceiveAllMessagesInOrder()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateChunkingProducer(client, topic);
        await using var consumer = CreateStringConsumer(client, topic, CreateSubscriptionName(), builder => builder.SubscriptionType(SubscriptionType.Shared).MessagePrefetchCount(5));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var values = new[] { CreateChunkedMessage(1), CreateChunkedMessage(10), CreateChunkedMessage(4) }; // The second has more chunks than the prefetch count, and the third has fewer

        //Act
        foreach (var value in values)
        {
            _ = await producer.Send(value, _cts.Token);
        }

        var received = new List<string>();
        for (var i = 0; i < values.Length; ++i)
        {
            var message = await Receive(consumer, TimeSpan.FromSeconds(10));
            received.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        received.ShouldBe(values);
    }

    // Upstream: MessageChunkingSharedTest#testMultiConsumers
    // The upstream consumers have a receiver queue of 5, which is a little more than the 4 chunks of a message. The broker hands the chunks to a consumer as long as it has permits, and the rest
    // of the message goes to the other consumer, where it can not be completed (the chunks stay unacknowledged and the message is never delivered). The Java client gives the permits back
    // as soon as it has received two chunks, and DotPulsar gives them back in steps of half the queue, so the queue is large enough for every chunk of the test here (followup 0015).
    [Fact]
    public async Task Receive_GivenTwoSharedConsumers_ShouldDeliverEveryChunkedMessageToOneOfThem()
    {
        //Arrange
        const int prefetchCount = 40;
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producer = CreateChunkingProducer(client, topic);
        await using var consumer1 = CreateStringConsumer(client, topic, subscription, builder => builder.SubscriptionType(SubscriptionType.Shared).MessagePrefetchCount(prefetchCount));
        await using var consumer2 = CreateStringConsumer(client, topic, subscription, builder => builder.SubscriptionType(SubscriptionType.Shared).MessagePrefetchCount(prefetchCount));
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        var values = Enumerable.Range(0, 10).Select(_ => CreateChunkedMessage(4)).ToHashSet();
        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var received1 = new ConcurrentQueue<string>();
        var received2 = new ConcurrentQueue<string>();
        var receiving = Task.WhenAll(ReceiveUntilCancelled(consumer1, received1, receiveCts.Token), ReceiveUntilCancelled(consumer2, received2, receiveCts.Token));

        // The first flow of a consumer is sent by its first Receive (Java sends it when the consumer subscribes), so the messages are sent when the broker has the permits of both consumers.
        // Otherwise the consumer that is ready gets the first messages, and the other one may get none.
        await WaitUntil(async () => HaveConsumersPermits(await GetSubscriptionStats(topic, subscription), 2, prefetchCount));

        //Act
        foreach (var value in values)
        {
            _ = await producer.Send(value, _cts.Token);
        }

        await WaitUntil(() => Task.FromResult(received1.Count + received2.Count >= values.Count));
        await receiveCts.CancelAsync();
        await receiving;

        //Assert
        (received1.Count + received2.Count).ShouldBe(values.Count);
        received1.ShouldNotBeEmpty();
        received2.ShouldNotBeEmpty();
        received1.Concat(received2).ToHashSet().ShouldBe(values, true);
    }

    // Upstream: MessageChunkingSharedTest#testInterleavedChunks
    // The chunks are published by two producers that are named like the producers of the upstream test, one chunk at a time. A message is delivered when its last chunk has been received.
    [Fact]
    public async Task Receive_GivenInterleavedChunksOfTwoProducers_ShouldDeliverTheMessagesInTheOrderTheyAreCompleted()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producerA = CreateByteProducer(client, topic, "A");
        await using var producerB = CreateByteProducer(client, topic, "B");
        var consumer1 = CreateConsumer(client, topic, SubscriptionType.Shared, subscription, builder => builder.ConsumerName("consumer-1"));
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        _ = await ChunkTestHelper.SendChunk(producerA, "A", 0, null, null, _cts.Token);
        _ = await ChunkTestHelper.SendChunk(producerA, "A", 1, 0, 3, _cts.Token);
        _ = await ChunkTestHelper.SendChunk(producerA, "A", 1, 1, 3, _cts.Token);
        _ = await ChunkTestHelper.SendChunk(producerB, "B", 0, null, null, _cts.Token);
        _ = await ChunkTestHelper.SendChunk(producerB, "B", 1, 0, 2, _cts.Token);
        var first = new List<string> { await ReceiveProducerAndSequenceId(consumer1), await ReceiveProducerAndSequenceId(consumer1) };
        _ = await ChunkTestHelper.SendChunk(producerB, "B", 1, 1, 2, _cts.Token);
        first.Add(await ReceiveProducerAndSequenceId(consumer1));
        _ = await ChunkTestHelper.SendChunk(producerA, "A", 1, 2, 3, _cts.Token);
        first.Add(await ReceiveProducerAndSequenceId(consumer1));
        await using var consumer2 = CreateConsumer(client, topic, SubscriptionType.Shared, subscription, builder => builder.ConsumerName("consumer-2"));
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer1.DisposeAsync();
        var second = new List<string>();
        for (var i = 0; i < 4; ++i)
        {
            second.Add(await ReceiveProducerAndSequenceId(consumer2)); // Nothing was acknowledged, so all the messages are delivered to the other consumer
        }

        //Assert
        first.ShouldBe(["A-0", "B-0", "B-1", "A-1"]);
        second.ShouldBe(["A-0", "B-0", "B-1", "A-1"]);
    }

    // Upstream: MessageChunkingTest#testRepeatedSeekDoesNotRetainFilteredChunkMessageIds (inclusive = true; the ids of the chunks are carried by the id of the message, so nothing is kept by the consumer)
    [Fact]
    public async Task Seek_GivenIdOfEachChunkedMessage_ShouldDeliverTheMessagesFromThatMessageAndTheMarkerThatIsNotChunked()
    {
        //Arrange
        const int totalMessages = 4;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateChunkingProducer(client, topic);
        var messageIds = new List<MessageId>();
        for (var i = 0; i < totalMessages; ++i)
        {
            var messageId = await producer.Send(CreateRepeatedMessage(i), _cts.Token);
            messageId.FirstChunkMessageId.ShouldNotBeNull();
            messageIds.Add(messageId);
        }

        var markerId = await producer.Send("marker", _cts.Token);
        markerId.FirstChunkMessageId.ShouldBeNull();
        await using var consumer = CreateStringConsumer(client, topic, CreateSubscriptionName());
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act and assert
        for (var boundary = 0; boundary < totalMessages; ++boundary)
        {
            await consumer.Seek(messageIds[boundary], _cts.Token);

            for (var i = boundary; i < totalMessages; ++i)
            {
                var message = await Receive(consumer);
                message.MessageId.ShouldBe(messageIds[i]);
                message.Value().ShouldBe(CreateRepeatedMessage(i));
                await consumer.Acknowledge(message, _cts.Token);
            }

            var marker = await Receive(consumer);
            marker.MessageId.ShouldBe(markerId);
            await consumer.Acknowledge(marker, _cts.Token);
        }
    }

    // Upstream: MessageChunkingTest#testLargeMessageOnNonPersistentTopicIsSentOnceWithoutChunking
    [Fact]
    public async Task Send_GivenLargeMessageOnNonPersistentTopic_ShouldSendItOnceWithoutChunking()
    {
        //Arrange
        var topic = $"non-persistent://public/default/{Guid.NewGuid():N}";
        await using var client = CreateClient();
        await using var consumer = CreateStringConsumer(client, topic, CreateSubscriptionName());
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = CreateChunkingProducer(client, topic, 100); // Small enough that the payload would otherwise be split in several chunks
        var payload = new string('a', 500);
        var receiving = await StartReceiving(consumer);

        //Act
        var messageId = await producer.Send(payload, _cts.Token);

        //Assert
        messageId.FirstChunkMessageId.ShouldBeNull();
        (await receiving).ShouldNotBeNull().Value().ShouldBe(payload);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull("the payload was published more than once");
    }

    // Upstream: MessageChunkingTest#testLargeMessageOnNonPersistentTopicAboveBrokerLimitIsRejectedLocally (the maximum message size of the broker is 5 MiB here, and the same checks apply as without chunking)
    [Fact]
    public async Task Send_GivenNonPersistentMessageAboveBrokerLimit_ShouldThrowTooLargeMessageExceptionAndKeepTheProducerUsable()
    {
        //Arrange
        const int maxMessageSize = 5 * 1024 * 1024;
        var topic = $"non-persistent://public/default/{Guid.NewGuid():N}";
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topic).EnableChunking(true).Create();
        var receiving = await StartReceiving(consumer);

        //Act
        var act = async () => await producer.Send(new byte[maxMessageSize + 10 * 1024 + 1024], _cts.Token);

        //Assert
        await act.ShouldThrowAsync<TooLargeMessageException>();
        _ = await producer.Send([1, 2, 3], _cts.Token);
        (await receiving).ShouldNotBeNull().Value().ShouldBe([1, 2, 3]);
    }

    // Upstream: BatchMessageTest#testSendOverSizeMessage (the producer does not chunk, so the send of a message that is larger than the maximum message size fails)
    [Fact]
    public async Task Send_GivenMessageAboveBrokerLimitAndChunkingNotEnabled_ShouldThrowTooLargeMessageExceptionAndKeepTheProducerUsable()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, SubscriptionType.Exclusive);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topic).Create();

        //Act
        var act = async () => await producer.Send(new byte[6 * 1024 * 1024], _cts.Token);

        //Assert
        await act.ShouldThrowAsync<TooLargeMessageException>();
        _ = await producer.Send([1, 2, 3], _cts.Token);
        (await Receive(consumer, TimeSpan.FromSeconds(10))).Value().ShouldBe([1, 2, 3]);
    }

    // Upstream: MessageChunkingTest#testLargeMessage (the size of the chunks is limited by the producer, and the messages are compressed)
    [Theory]
    [InlineData(CompressionType.None)]
    [InlineData(CompressionType.Lz4)]
    [InlineData(CompressionType.Zlib)]
    public async Task Send_GivenMessagesAboveChunkMaxMessageSize_ShouldDeliverThemAssembledAndAcknowledgeEveryChunk(CompressionType compressionType)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateStringConsumer(client, topic, subscription);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String).StateChangedHandler(_testOutputHelper.Log).Topic(topic).CompressionType(compressionType).EnableChunking(true).ChunkMaxMessageSize(100).Create();
        var values = Enumerable.Range(0, 5).Select(i => CreateRandomDigits(i * 1000)).ToList();

        //Act
        var messageIds = new List<MessageId>();
        foreach (var value in values)
        {
            messageIds.Add(await producer.Send(value, _cts.Token));
        }

        var received = new List<IMessage<string>>();
        for (var i = 0; i < values.Count; ++i)
        {
            received.Add(await Receive(consumer, TimeSpan.FromSeconds(10)));
        }

        foreach (var message in received)
        {
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        received.Select(message => message.Value()).ShouldBe(values);
        received.Select(message => message.MessageId).ShouldBe(messageIds);
        received.Skip(1).ShouldAllBe(message => message.MessageId.FirstChunkMessageId != null);
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // Upstream: TransactionConsumeTest#testAckChunkMessage (the id that the producer returns is the id of a chunked message, and it acknowledges every chunk)
    [Fact]
    public async Task Acknowledge_GivenIdThatTheProducerReturnedForAChunkedMessage_ShouldAcknowledgeEveryChunk()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateStringConsumer(client, topic, subscription);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String).StateChangedHandler(_testOutputHelper.Log).Topic(topic).EnableChunking(true).Create();
        var messageId = await producer.Send(new string('a', ChunkedMessageSize), _cts.Token);
        var message = await Receive(consumer);

        //Act
        await consumer.Acknowledge(messageId, _cts.Token);

        //Assert
        messageId.FirstChunkMessageId.ShouldNotBeNull();
        messageId.FirstChunkMessageId.ShouldNotBe(messageId);
        message.MessageId.ShouldBe(messageId);
        message.MessageId.FirstChunkMessageId.ShouldBe(messageId.FirstChunkMessageId);
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    [Fact]
    public async Task Send_GivenChunkedMessagesAndPartitionedTopic_ShouldDeliverEveryMessageAssembled()
    {
        //Arrange
        const int numberOfMessages = 6;
        var topic = await _fixture.CreatePartitionedTopic(3, _cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateChunkingProducer(client, topic);
        await using var consumer = CreateStringConsumer(client, topic, CreateSubscriptionName(), builder => builder.SubscriptionType(SubscriptionType.Shared));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var values = Enumerable.Range(0, numberOfMessages).Select(_ => CreateChunkedMessage(4)).ToList();

        //Act
        var messageIds = new List<MessageId>();
        foreach (var value in values)
        {
            messageIds.Add(await producer.Send(value, _cts.Token));
        }

        var received = new List<IMessage<string>>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            received.Add(await Receive(consumer, TimeSpan.FromSeconds(10)));
        }

        //Assert
        received.Select(message => message.Value()).Order().ShouldBe(values.Order());
        messageIds.Select(messageId => messageId.Partition).Distinct().Count().ShouldBe(3);
        messageIds.ShouldAllBe(messageId => messageId.FirstChunkMessageId != null && messageId.FirstChunkMessageId.Partition == messageId.Partition);
    }

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

    // The consumer sends its permits to the broker when it starts to receive, and the broker does not keep a message of a non-persistent topic for a consumer that has none
    private async Task<Task<IMessage<TMessage>?>> StartReceiving<TMessage>(IReceive<IMessage<TMessage>> receiver)
    {
        var receiving = TransactionFixture.ReceiveOrNull(receiver, TimeSpan.FromSeconds(10), _cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        return receiving;
    }

    private static string CreateRepeatedMessage(int number)
        => string.Concat(Enumerable.Repeat($"message-{number}", 30));

    // Upstream: MessageChunkingSharedTest#createChunkedMessage
    // A message that is split in 'numberOfChunks' chunks by a producer that has the ChunkMaxMessageSize of the tests
    private static string CreateChunkedMessage(int numberOfChunks)
        => CreateRandomLetters((numberOfChunks - 1) * ChunkMaxMessageSize + ChunkMaxMessageSize / 10);

    private static string CreateRandomLetters(int length)
        => string.Create(length, 0, static (span, _) =>
        {
            for (var i = 0; i < span.Length; ++i)
            {
                span[i] = (char) ('a' + Random.Shared.Next(26));
            }
        });

    private static string CreateRandomDigits(int length)
        => string.Create(length, 0, static (span, _) =>
        {
            for (var i = 0; i < span.Length; ++i)
            {
                span[i] = (char) ('0' + Random.Shared.Next(10));
            }
        });

    private IProducer<string> CreateChunkingProducer(IPulsarClient client, string topic, int chunkMaxMessageSize = ChunkMaxMessageSize)
        => client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .EnableChunking(true)
            .ChunkMaxMessageSize(chunkMaxMessageSize)
            .Create();

    private IProducer<byte[]> CreateByteProducer(IPulsarClient client, string topic, string name)
        => client.NewProducer(Schema.ByteArray)
            .ProducerName(name)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .EnableChunking(true)
            .Create();

    private async Task<string> ReceiveProducerAndSequenceId(IConsumer<byte[]> consumer)
    {
        var message = await Receive(consumer, TimeSpan.FromSeconds(10));
        return $"{message.ProducerName}-{message.SequenceId}";
    }

    private static async Task ReceiveUntilCancelled(IConsumer<string> consumer, ConcurrentQueue<string> received, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in consumer.Messages(cancellationToken))
            {
                received.Enqueue(message.Value());
                await consumer.Acknowledge(message, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // The messages have been received
        }
    }

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

    // The messages that are received until there are none for 5 seconds
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

    private static bool HaveConsumersPermits(JsonElement subscriptionStats, int consumers, int permits)
    {
        var availablePermits = subscriptionStats.GetProperty("consumers").EnumerateArray().Select(consumer => consumer.GetProperty("availablePermits").GetInt32()).ToList();
        return availablePermits.Count == consumers && availablePermits.All(available => available >= permits);
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
