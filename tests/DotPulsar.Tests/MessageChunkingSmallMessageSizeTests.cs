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
using ProtocolMetadata = DotPulsar.Internal.PulsarApi.MessageMetadata;
using System.Security.Cryptography;
using System.Text.Json;

// The tests of Java MessageChunkingTest that set the maximum message size of the broker to a small value, so the producer splits the messages that the tests send in chunks
[Collection("Chunking"), Trait("Category", "Integration")]
public sealed class MessageChunkingSmallMessageSizeTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly ChunkingFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public MessageChunkingSmallMessageSizeTests(ChunkingFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: MessageChunkingTest#testLargeMessage (the data provider ackReceiptEnabledWithMaxMessageSize; the producer limits the chunks if the chunk size is not 0)
    [Theory]
    [InlineData(true, 40)]
    [InlineData(true, 0)]
    [InlineData(false, 40)]
    [InlineData(false, 0)]
    public async Task Send_GivenMessagesAboveMaxMessageSize_ShouldDeliverThemAssembledInOrderAndAcknowledgeEveryChunk(bool ackReceiptEnabled, int chunkMaxMessageSize)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .AckReceiptEnabled(ackReceiptEnabled)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .Topic(topic)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String)
            .ChunkMaxMessageSize(chunkMaxMessageSize)
            .CompressionType(CompressionType.Lz4)
            .EnableChunking(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        var values = Enumerable.Range(1, 5).Select(i => CreateRandomDigits(i * 100)).ToList();

        //Act
        var messageIds = new List<MessageId>();
        foreach (var value in values)
        {
            messageIds.Add(await producer.Send(value, _cts.Token));
        }

        var received = new List<IMessage<string>>();
        for (var i = 0; i < values.Count; ++i)
        {
            received.Add(await Receive(consumer));
        }

        foreach (var message in received)
        {
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        received.Select(message => message.Value()).ShouldBe(values);
        received.Select(message => message.MessageId).ShouldBe(messageIds);
        messageIds.ShouldAllBe(messageId => (object?) messageId.FirstChunkMessageId != null);
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // Upstream: MessageChunkingTest#testChunkingWithOrderingKey
    [Fact]
    public async Task Receive_GivenChunkedMessageWithOrderingKey_ShouldPreserveDataAndOrderingKey()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(CreateSubscriptionName())
            .Topic(topic)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .EnableChunking(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        var data = RandomNumberGenerator.GetBytes(200);
        var orderingKey = RandomNumberGenerator.GetBytes(50);

        //Act
        var messageId = await producer.NewMessage().OrderingKey(orderingKey).Send(data, _cts.Token);
        var message = await Receive(consumer);

        //Assert
        messageId.FirstChunkMessageId.ShouldNotBeNull();
        message.Value().ShouldBe(data);
        message.OrderingKey.ShouldBe(orderingKey);
    }

    // Upstream: MessageChunkingTest#testChunkSize
    // The size of the metadata is not known by the test upstream, which uses 32. It is the size of the metadata that the producer sends, which the test builds in the same way.
    [Fact]
    public async Task Send_GivenSizesUpToMaxMessageSize_ShouldChunkOnlyAbovePayloadChunkSize()
    {
        //Arrange
        const string producerName = "test-producer";
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String)
            .EnableChunking(true)
            .ProducerName(producerName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        var metadataSize = new ProtocolMetadata { ProducerName = producerName, SequenceId = 0, PublishTime = (ulong) DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() }.CalculateSize();
        var payloadChunkSize = ChunkingFixture.MaxMessageSize - metadataSize;

        //Act
        var isChunked = new List<bool>();
        for (var size = 1; size <= ChunkingFixture.MaxMessageSize; ++size)
        {
            var messageId = await producer.Send(CreateRandomDigits(size), _cts.Token);
            isChunked.Add((object?) messageId.FirstChunkMessageId != null);
        }

        //Assert
        isChunked.ShouldBe(Enumerable.Range(1, ChunkingFixture.MaxMessageSize).Select(size => size > payloadChunkSize));
        isChunked[payloadChunkSize - 1].ShouldBeFalse();
        isChunked[payloadChunkSize].ShouldBeTrue();
    }

    // Upstream: MessageChunkingTest#testBlockIfQueueFullWhenChunking
    // A chunked message takes one place in the queue of the producer, so it can not wait for room that only its own chunks can make
    [Fact]
    public async Task Send_GivenMaxPendingMessagesSmallerThanNumberOfChunks_ShouldNotDeadlock()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String)
            .EnableChunking(true)
            .MaxPendingMessages(3)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();

        //Act
        var large = producer.Send(CreateRandomDigits(200), _cts.Token).AsTask();
        await large.WaitAsync(TimeSpan.FromSeconds(5), _cts.Token);
        var concurrent = Enumerable.Range(0, 3).Select(_ => Task.Run(async () => await producer.Send(CreateRandomDigits(100), _cts.Token), _cts.Token));
        var sent = await Task.WhenAll(concurrent).WaitAsync(TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        (await large).FirstChunkMessageId.ShouldNotBeNull();
        sent.ShouldAllBe(messageId => (object?) messageId.FirstChunkMessageId != null);
    }

    // Upstream: MessageChunkingTest#testLargeMessageOnNonPersistentTopicAboveBrokerLimitIsRejectedLocally
    [Fact]
    public async Task Send_GivenNonPersistentMessageAboveBrokerLimit_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        var topic = $"non-persistent://public/default/{Guid.NewGuid():N}";
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .EnableChunking(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        var payload = RandomNumberGenerator.GetBytes(ChunkingFixture.MaxMessageSize + 10 * 1024 + 1024); // Well above the maximum message size plus the padding of the frame of the broker

        //Act
        var act = async () => await producer.Send(payload, _cts.Token);

        //Assert
        var exception = await act.ShouldThrowAsync<TooLargeMessageException>();
        exception.MaxMessageSize.ShouldBe(ChunkingFixture.MaxMessageSize);
    }

    // Upstream: BatchMessageTest#testSendOverSizeMessage
    [Fact]
    public async Task Send_GivenMessageAboveMaxMessageSizeAndChunkingNotEnabled_ShouldThrowTooLargeMessageExceptionAndKeepTheProducerUsable()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(CreateSubscriptionName())
            .Topic(topic)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String).StateChangedHandler(_testOutputHelper.Log).Topic(topic).Create();

        //Act
        var act = async () => await producer.Send(CreateRandomDigits(200), _cts.Token);

        //Assert
        await act.ShouldThrowAsync<TooLargeMessageException>();
        _ = await producer.Send("hello", _cts.Token);
        (await Receive(consumer)).Value().ShouldBe("hello");
    }

    // Upstream: ProducerImpl#sendAsync (the metadata of the message is larger than the maximum message size, so no payload fits in a chunk)
    [Fact]
    public async Task Send_GivenMetadataLargerThanMaxMessageSize_ShouldThrowTooLargeMessageExceptionAndKeepTheProducerUsable()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(CreateSubscriptionName())
            .Topic(topic)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String).EnableChunking(true).StateChangedHandler(_testOutputHelper.Log).Topic(topic).Create();

        //Act
        var act = async () => await producer.NewMessage().Property("large-metadata", new string('y', 400)).Send("hello", _cts.Token);

        //Assert
        await act.ShouldThrowAsync<TooLargeMessageException>();
        _ = await producer.Send("hello again", _cts.Token);
        (await Receive(consumer)).Value().ShouldBe("hello again");
    }

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

    private static string CreateRandomDigits(int length)
        => string.Create(length, 0, static (span, _) =>
        {
            for (var i = 0; i < span.Length; ++i)
            {
                span[i] = (char) ('0' + RandomNumberGenerator.GetInt32(10));
            }
        });

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private async Task<IMessage<TMessage>> Receive<TMessage>(IReceive<IMessage<TMessage>> receiver)
    {
        var message = await TransactionFixture.ReceiveOrNull(receiver, TimeSpan.FromSeconds(30), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private async Task<long> GetBacklog(string topic, string subscription)
    {
        using var httpClient = _fixture.CreateAdminClient();
        using var response = await httpClient.GetAsync($"/admin/v2/persistent/{topic.Replace("persistent://", string.Empty)}/stats", _cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(_cts.Token);
        using var json = await JsonDocument.ParseAsync(stream, cancellationToken: _cts.Token);
        return json.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();
    }

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
