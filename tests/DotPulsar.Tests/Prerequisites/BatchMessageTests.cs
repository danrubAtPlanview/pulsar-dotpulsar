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
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using System.Security.Cryptography;
using System.Text.Json;

// Upstream: BatchMessageTest
// The backlog of the subscription is the number of entries that were written, so it is read from the admin API to count the batches.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class BatchMessageTests : IDisposable
{
    private const string _subscription = "my-subscription";
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public BatchMessageTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: BatchMessageTest#testSimpleBatchProducerWithFixedBatchSize (the data provider codecAndContainerBuilder)
    [Theory]
    [InlineData(CompressionType.None, BatcherType.Default)]
    [InlineData(CompressionType.Lz4, BatcherType.Default)]
    [InlineData(CompressionType.Zlib, BatcherType.Default)]
    [InlineData(CompressionType.None, BatcherType.KeyBased)]
    [InlineData(CompressionType.Lz4, BatcherType.KeyBased)]
    [InlineData(CompressionType.Zlib, BatcherType.KeyBased)]
    public async Task Send_GivenFixedBatchSizeOf25And50Messages_ShouldCreateTwoEntries(CompressionType compressionType, BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 50;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .CompressionType(compressionType)
            .BatcherType(batcherType)
            .BatchingMaxMessages(25)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        var messageIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        (await GetBacklog(topic)).ShouldBe(2);
        messageIds.Select(messageId => messageId.EntryId).Distinct().Count().ShouldBe(2);
        await AssertReceived(consumer, numberOfMessages);
    }

    // Upstream: BatchMessageTest#testSimpleBatchProducerWithFixedBatchBytes
    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public async Task Send_GivenFixedBatchBytes_ShouldCreateEntriesOfTenMessages(BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 50;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(batcherType)
            .BatchingMaxMessages(0)
            .BatchingMaxBytes(100)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i:D5}")); // 9 bytes
        }

        var messageIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        messageIds.GroupBy(messageId => messageId.EntryId).OrderBy(entry => entry.Key).Select(entry => entry.Count()).ShouldBe([11, 11, 11, 11, 6]); // The twelfth message of 9 bytes does not fit in 100 bytes
        (await GetBacklog(topic)).ShouldBe(5);
        (await AssertReceived(consumer, numberOfMessages, false)).ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"msg-{i:D5}"));
    }

    // Upstream: BatchMessageTest#testSimpleBatchProducerWithFixedBatchTime
    [Fact]
    public async Task Send_GivenFixedBatchTime_ShouldCreateEntriesByTime()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatchingMaxMessages(1000)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(500)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < 5; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.Delay(TimeSpan.FromSeconds(2), _cts.Token);

        for (var i = 5; i < 10; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        (await GetBacklog(topic)).ShouldBe(2);
        await AssertReceived(consumer, 10);
    }

    // Upstream: BatchMessageTest#testSimpleBatchProducerWithFixedBatchSizeAndTime
    [Fact]
    public async Task Send_GivenFixedBatchSizeAndTime_ShouldCloseTheBatchBySizeOrTime()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatchingMaxMessages(5)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(500)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < 7; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        (await GetBacklog(topic)).ShouldBe(2);
        await AssertReceived(consumer, 7);
    }

    // Upstream: BatchMessageTest#testBatchProducerWithLargeMessage
    [Fact]
    public async Task Send_GivenMessageLargerThanBatchingMaxBytes_ShouldSendItAloneAndBatchTheOthers()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatchingMaxBytes(1024)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(500)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < 5; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        receipts.Add(await SendAsync(producer, "large-" + new string('x', 2048)));

        for (var i = 6; i < 11; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        var messageIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        (await GetBacklog(topic)).ShouldBe(3);
        messageIds[5].BatchIndex.ShouldBe(-1);
        messageIds.Take(5).Select(messageId => messageId.BatchIndex).ShouldBe(Enumerable.Range(0, 5));
        messageIds.Skip(6).Select(messageId => messageId.BatchIndex).ShouldBe(Enumerable.Range(0, 5));
        var received = await AssertReceived(consumer, 11, false);
        received[5].ShouldStartWith("large-");
    }

    // Upstream: BatchMessageTest#testSimpleBatchProducerConsumer
    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public async Task Send_GivenBatchedMessages_ShouldDeliverThemWithTheMessageIdsOfTheProducer(BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(batcherType)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        var messageIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        messageIds.Select(messageId => messageId.BatchIndex).ShouldBe(Enumerable.Range(0, numberOfMessages));
        messageIds.Select(messageId => (messageId.LedgerId, messageId.EntryId)).Distinct().Count().ShouldBe(1);
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
            message.ShouldNotBeNull();
            message.Value().ShouldBe($"msg-{i}");
            (message.MessageId.LedgerId, message.MessageId.EntryId, message.MessageId.BatchIndex).ShouldBe((messageIds[i].LedgerId, messageIds[i].EntryId, messageIds[i].BatchIndex));
            await consumer.Acknowledge(message, _cts.Token);
        }
    }

    // Upstream: BatchMessageTest#testSimpleBatchSyncProducerWithFixedBatchSize (the data provider containerBuilder)
    // Java expects 2 entries for 10 messages with a batch size of 5, but each synchronous send waits for the broker, so every message goes as an entry of its own and the backlog is 10.
    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public async Task Send_GivenFixedBatchSizeAndSynchronousSends_ShouldCreateOneEntryPerMessage(BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(batcherType)
            .BatchingMaxMessages(numberOfMessages / 2)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1)));

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            _ = await producer.Send($"my-message-{i}", _cts.Token);
        }

        //Assert
        (await GetBacklog(topic)).ShouldBe(10);
        (await AssertReceived(consumer, numberOfMessages, false)).ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"my-message-{i}"));
    }

    // Upstream: BatchMessageTest#testSimpleBatchProducerConsumer1kMessages
    [Fact]
    public async Task Send_GivenThousandMessagesAndBatching_ShouldDeliverAllInOrder()
    {
        //Arrange
        const int numberOfMessages = 1000;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxMessages(100));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(60), _cts.Token);

        //Assert
        await AssertReceived(consumer, numberOfMessages);
    }

    // Upstream: BatchMessageTest#testOrderingOfKeyBasedBatchMessageContainer
    [Fact]
    public async Task Send_GivenKeyBasedBatcherAndKeySharedConsumers_ShouldPreserveOrderPerKey()
    {
        //Arrange
        const int numberOfMessages = 200;
        const int numberOfKeys = 5;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        var consumers = new List<IConsumer<string>>();

        for (var i = 0; i < 2; ++i)
        {
            var consumer = client.NewConsumer(Schema.String)
                .Topic(topic)
                .InitialPosition(SubscriptionInitialPosition.Earliest)
                .StateChangedHandler(_testOutputHelper.Log)
                .SubscriptionName(_subscription)
                .SubscriptionType(SubscriptionType.KeyShared)
                .Create();
            await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
            consumers.Add(consumer);
        }

        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(BatcherType.KeyBased)
            .BatchingMaxMessages(20)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, i.ToString(), $"key-{i % numberOfKeys}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        var received = await Task.WhenAll(consumers.Select(consumer => ReceiveAll(consumer)));

        //Assert
        received.Sum(messages => messages.Count).ShouldBe(numberOfMessages);

        foreach (var messages in received)
        {
            foreach (var group in messages.GroupBy(message => message.Key))
            {
                group.Select(message => int.Parse(message.Value())).ShouldBe(group.Select(message => int.Parse(message.Value())).Order(), $"The messages of the key {group.Key} are out of order");
            }
        }

        received[0].Select(message => message.Key).Intersect(received[1].Select(message => message.Key)).ShouldBeEmpty("A key is only delivered to one consumer");

        foreach (var consumer in consumers)
            await consumer.DisposeAsync();
    }

    // Upstream: BatchMessageTest#testBatchSendOneMessage
    [Fact]
    public async Task Send_GivenSingleMessageInBatch_ShouldSendItUnbatched()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100)));

        //Act
        var messageId = await producer.NewMessage().Property("key1", "value1").Send("my-message", _cts.Token);
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);

        //Assert
        messageId.BatchIndex.ShouldBe(-1);
        message.ShouldNotBeNull();
        message.Value().ShouldBe("my-message");
        message.MessageId.BatchIndex.ShouldBe(-1);
        message.SequenceId.ShouldBe(0UL);
        message.Properties["key1"].ShouldBe("value1");
    }

    // Upstream: BatchMessageTest#testRetrieveSequenceIdGenerated
    [Fact]
    public async Task Send_GivenGeneratedSequenceIds_ShouldDeliverThemWithTheMessages()
    {
        //Arrange
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxMessages(numberOfMessages).BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
            message.ShouldNotBeNull();
            message.SequenceId.ShouldBe((ulong) i);
        }
    }

    // Upstream: BatchMessageTest#testRetrieveSequenceIdSpecify
    [Fact]
    public async Task Send_GivenExplicitSequenceIds_ShouldDeliverThemWithTheMessages()
    {
        //Arrange
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxMessages(numberOfMessages).BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}", sequenceId: (ulong) (i + 100)));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
            message.ShouldNotBeNull();
            message.SequenceId.ShouldBe((ulong) (i + 100));
        }
    }

    // Upstream: BatchMessageTest#testSendOverSizeMessage (the data provider codecAndContainerBuilder)
    // The message is random, so it is larger than the maximum message size of the broker (5 MiB) also when it is compressed. The Java test accepts that the send does not fail.
    [Theory]
    [InlineData(CompressionType.None, BatcherType.Default)]
    [InlineData(CompressionType.Lz4, BatcherType.Default)]
    [InlineData(CompressionType.Zlib, BatcherType.Default)]
    [InlineData(CompressionType.None, BatcherType.KeyBased)]
    [InlineData(CompressionType.Lz4, BatcherType.KeyBased)]
    [InlineData(CompressionType.Zlib, BatcherType.KeyBased)]
    public async Task Send_GivenMessageLargerThanMaxMessageSize_ShouldFailTheSendAndNotFaultTheProducer(CompressionType compressionType, BatcherType batcherType)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .CompressionType(compressionType)
            .BatcherType(batcherType)
            .BatchingMaxMessages(2)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1)));
        var tooLarge = Convert.ToBase64String(RandomNumberGenerator.GetBytes(8 * 1024 * 1024));

        //Act
        var act = async () => await producer.Send(tooLarge, _cts.Token);

        //Assert
        await act.ShouldThrowAsync<TooLargeMessageException>();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < 10; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        producer.State.IsFinalState().ShouldBeFalse(); // A faulted producer is in a final state
        await AssertReceived(consumer, 10);
    }

    // Additional coverage
    [Fact]
    public async Task Flush_GivenLongPublishDelay_ShouldPersistTheMessagesAtOnce()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxPublishDelay(TimeSpan.FromHours(1)));
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < 5; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        //Act
        await producer.Flush(_cts.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        receipts.ShouldAllBe(receipt => receipt.IsCompletedSuccessfully);
        (await GetBacklog(topic)).ShouldBe(1);
        await AssertReceived(consumer, 5);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenBatchingAndPartitionedTopic_ShouldDeliverAllMessages()
    {
        //Arrange
        const int numberOfMessages = 60;
        var topic = await _fixture.CreatePartitionedTopic(3, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100)));
        var receipts = new List<Task<MessageId>>();

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await producer.Flush(_cts.Token);
        var messageIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        messageIds.Select(messageId => messageId.Partition).Distinct().Count().ShouldBe(3);
        var received = await ReceiveAll(consumer);
        received.Select(message => message.Value()).Order().ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"msg-{i}").Order());
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenConnectionDownWhileMessagesWait_ShouldBatchAndSendThemAfterReconnect()
    {
        //Arrange
        const int numberOfMessages = 20;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder.BatchingMaxMessages(10).BatchingMaxPublishDelay(TimeSpan.FromSeconds(30)));
        var receipts = new List<Task<MessageId>>();

        //Act
        await using (await _fixture.DisableThePulsarConnection())
        {
            await producer.StateChangedTo(ProducerState.Disconnected, _cts.Token);

            for (var i = 0; i < numberOfMessages; ++i)
            {
                receipts.Add(await SendAsync(producer, $"msg-{i}"));
            }
        }

        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var messageIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(60), _cts.Token);

        //Assert
        messageIds.Select(messageId => messageId.EntryId).Distinct().Count().ShouldBe(2);
        await AssertReceived(consumer, numberOfMessages);
    }

    // Upstream: BatchMessageTest#testOutOfOrderAcksForBatchMessage
    // The 4 batches have the messages 0-9, 10-19, 20-29 and 30-39. The cumulative acknowledgment of the message 8 leaves a hole at the message 9, and the cumulative acknowledgment of the message 14
    // acknowledges the first batch (also for the broker), and the individual acknowledgments of the messages 15-19 complete the second batch.
    [Fact]
    public async Task AcknowledgeCumulative_GivenOutOfOrderAcksForBatchMessages_ShouldAcknowledgeTheBatchesWhenAllTheirMessagesAreAcknowledged()
    {
        //Arrange
        const int numberOfMessages = 40;
        const int numberOfMessagesInBatch = numberOfMessages / 4;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatchingMaxMessages(numberOfMessagesInBatch)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        (await GetBacklog(topic)).ShouldBe(numberOfMessages / numberOfMessagesInBatch);
        var individualAcks = Enumerable.Range(15, 5).ToHashSet();
        IMessage<string>? lastUnackedMessage = null;

        //Act
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await ReceiveMessage(consumer);

            if (i == 8)
            {
                await consumer.AcknowledgeCumulative(message, _cts.Token);
            }
            else if (i == 9)
            {
                // Do not acknowledge the message
            }
            else if (i == 14)
            {
                // Should acknowledge the first batch on the broker
                await consumer.AcknowledgeCumulative(message, _cts.Token);
                await WaitForBacklog(topic, 3);
            }
            else if (individualAcks.Contains(i))
            {
                await consumer.Acknowledge(message, _cts.Token);
            }
            else
            {
                lastUnackedMessage = message;
            }
        }

        await WaitForBacklog(topic, 2);
        await consumer.AcknowledgeCumulative(lastUnackedMessage!, _cts.Token);

        //Assert
        await WaitForBacklog(topic, 0);
    }

    // Upstream: BatchMessageTest#testNonBatchCumulativeAckAfterBatchPublish
    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public async Task AcknowledgeCumulative_GivenNonBatchMessageAfterBatchPublish_ShouldAcknowledgeTheBatchAndTheMessage(BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(batcherType)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        await using var noBatchProducer = client.NewProducer(Schema.String).Topic(topic).StateChangedHandler(_testOutputHelper.Log).Create();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        _ = await noBatchProducer.Send("nobatch", _cts.Token);
        (await GetBacklog(topic)).ShouldBe(2);

        //Act
        IMessage<string>? lastUnackedMessage = null;
        for (var i = 0; i <= numberOfMessages; ++i)
        {
            lastUnackedMessage = await ReceiveMessage(consumer);
        }

        await consumer.AcknowledgeCumulative(lastUnackedMessage!, _cts.Token);

        //Assert
        await WaitForBacklog(topic, 0);
    }

    // Upstream: BatchMessageTest#testBatchAndNonBatchCumulativeAcks
    // The batch producer and the producer without batching send messages alternately, so the entries of the batches (of 5 messages) and the entries of single messages are mixed.
    // Every second message is acknowledged cumulatively, and the last message that was not acknowledged is acknowledged cumulatively at the end.
    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public async Task AcknowledgeCumulative_GivenBatchAndNonBatchMessages_ShouldAcknowledgeEverything(BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 50;
        const int numberOfMessagesInBatch = numberOfMessages / 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(batcherType)
            .BatchingMaxMessages(numberOfMessagesInBatch)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        await using var noBatchProducer = client.NewProducer(Schema.String).Topic(topic).StateChangedHandler(_testOutputHelper.Log).Create();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages / 2; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
            receipts.Add(await SendAsync(noBatchProducer, $"nobatch-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        (await GetBacklog(topic)).ShouldBe(((numberOfMessages / 2) / numberOfMessagesInBatch) + (numberOfMessages / 2));

        //Act
        IMessage<string>? lastUnackedMessage = null;
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await ReceiveMessage(consumer);

            if (i % 2 == 0)
                lastUnackedMessage = message;
            else
                await consumer.AcknowledgeCumulative(message, _cts.Token);
        }

        await consumer.AcknowledgeCumulative(lastUnackedMessage!, _cts.Token);

        //Assert
        await WaitForBacklog(topic, 0);
    }

    // Upstream: BatchMessageTest#testConcurrentBatchMessageAck
    // Verifies that acknowledging the messages of a batch is thread-safe. The Java test ignores the failure of a receive or an acknowledge, and here it fails the test.
    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public async Task Acknowledge_GivenConcurrentAcksOfTheMessagesOfABatch_ShouldAcknowledgeAllMessages(BatcherType batcherType)
    {
        //Arrange
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatcherType(batcherType)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5)));
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"my-message-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(_subscription)
            .SubscriptionType(SubscriptionType.Shared)
            .Create();

        //Act
        var acks = Enumerable.Range(0, numberOfMessages).Select(_ => Task.Run(async () =>
        {
            var message = await ReceiveMessage(consumer);
            await consumer.Acknowledge(message, _cts.Token);
        }, _cts.Token)).ToList();
        await Task.WhenAll(acks).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        // Wait for the acknowledgments to be received by the broker
        await WaitForConsumerUnackedMessages(topic, 0);
        await WaitForBacklog(topic, 0);
    }

    // Upstream: BatchMessageTest#testBatchMessageDispatchingAccordingToPermits
    // Java reads the number of messages in the receiver queue of the consumers. The queue of a DotPulsar consumer is not accessible, so the number of messages that the broker dispatched to each consumer
    // is read from the topic stats. Both consumers must have been sent messages, and no more than the permits that they gave to the broker: the permits are a number of messages and not of batches.
    // A DotPulsar consumer gives the broker permits when it starts to receive, and the first permits are half of the prefetch count, so the prefetch count of 20 gives the 10 permits of the Java consumer.
    [Fact]
    public async Task Receive_GivenTwoSharedConsumersAndBatches_ShouldDispatchMessagesAccordingToPermits()
    {
        //Arrange
        const int numberOfMessages = 1000;
        const int batchMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer1 = CreateSharedConsumer(client, topic, batchMessages * 2);
        await using var consumer2 = CreateSharedConsumer(client, topic, batchMessages * 2);
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receive1 = consumer1.Receive(_cts.Token).AsTask();
        var receive2 = consumer2.Receive(_cts.Token).AsTask();
        await using var producer = await CreateProducer(client, topic, builder => builder
            .BatchingMaxMessages(batchMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(500)));
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"my-message-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(60), _cts.Token);

        //Act
        // Java waits 3 seconds. A consumer may have to subscribe again when the broker fails to create the ledger of its cursor, which was seen for a broker that had just started.
        var messagesSent = await WaitForConsumersToHaveMessages(topic, 2, TimeSpan.FromSeconds(10));

        //Assert
        messagesSent.Count.ShouldBe(2);
        messagesSent.ShouldAllBe(count => count > 0 && count <= batchMessages * 2);
        _ = await receive1.WaitAsync(TimeSpan.FromSeconds(5), _cts.Token);
        _ = await receive2.WaitAsync(TimeSpan.FromSeconds(5), _cts.Token);
    }

    // Additional coverage
    // The first 50 messages are acknowledged by the broker. Then the receipts of the broker are lost, so the producer sends the other messages, and the broker stores them, but the producer never gets
    // the receipts. When the connection is closed, the producer reconnects and sends the batches that it has no receipt for again, with the same sequence ids. The broker deduplicates them, so the topic
    // contains each message once and in order.
    // The broker deduplicates by the name of the producer. A producer without a name sends the name that the broker generated when it reconnects, like the Java producer does, so the broker takes it for
    // the producer that it had. The test runs for a producer with a name and for one without.
    [Theory]
    [InlineData(null)]
    [InlineData("deduplication-producer")]
    public async Task Send_GivenBatchingAndLostReceipts_ShouldResendWithoutDuplicates(string? producerName)
    {
        //Arrange
        const int numberOfMessages = 500;
        const int numberOfAcknowledgedMessages = 50;
        var topic = await CreateTopicWithDeduplication();
        await using var relay = await LossyRelay.Start(_fixture.ServiceUrl, _cts.Token);
        await using var producerClient = PulsarClient
            .Builder()
            .Authentication(_fixture.Authentication)
            .ExceptionHandler(_testOutputHelper.Log)
            .ServiceUrl(relay.ServiceUrl)
            .Build();
        var producerBuilder = producerClient.NewProducer(Schema.String)
            .BatchingMaxMessages(10)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(50))
            .EnableBatching(true)
            .MaxPendingMessages(numberOfMessages * 2)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic);

        if (producerName is not null)
            producerBuilder = producerBuilder.ProducerName(producerName);

        await using var producer = producerBuilder.Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < numberOfAcknowledgedMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        relay.SwallowBrokerToClient();

        for (var i = numberOfAcknowledgedMessages; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        // The broker has stored all the messages, and the producer has not received a receipt
        await WaitUntilAsync(async () => await GetMessagesPublished(topic) >= numberOfMessages);
        receipts.Skip(numberOfAcknowledgedMessages).ShouldAllBe(receipt => !receipt.IsCompleted);

        //Act
        relay.DropConnections();
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(60), _cts.Token);

        //Assert
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topic);
        var received = new List<string>();

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token) is { } message)
        {
            received.Add(message.Value());
        }

        received.ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"msg-{i}"));
    }

    public void Dispose()
        => _cts.Dispose();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private async Task<IConsumer<string>> CreateConsumer(IPulsarClient client, string topic)
    {
        var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(_subscription)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        return consumer;
    }

    private async Task<IProducer<string>> CreateProducer(IPulsarClient client, string topic, Func<IProducerBuilder<string>, IProducerBuilder<string>> configure)
    {
        var producer = configure(client.NewProducer(Schema.String).Topic(topic).EnableBatching(true).StateChangedHandler(_testOutputHelper.Log)).Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        return producer;
    }

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync(IProducer<string> producer, string value, string? key = null, ulong sequenceId = 0)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);
        var metadata = new MessageMetadata { SequenceId = sequenceId };

        if (key is not null)
            metadata.Key = key;

        await producer.SendChannel.Send(metadata, value, messageId =>
        {
            _ = receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }

    private async Task<List<string>> AssertReceived(IConsumer<string> consumer, int numberOfMessages, bool compareValues = true)
    {
        var received = new List<string>();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
            message.ShouldNotBeNull($"The message {i} was not received");
            received.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        if (compareValues)
            received.ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"msg-{i}"));

        return received;
    }

    private async Task<List<IMessage<string>>> ReceiveAll(IConsumer<string> consumer)
    {
        var messages = new List<IMessage<string>>();

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token) is { } message)
        {
            messages.Add(message);
        }

        return messages;
    }

    // The Java consumer without a subscription type is exclusive, and this one is shared with a receiver queue of the number of messages
    private IConsumer<string> CreateSharedConsumer(IPulsarClient client, string topic, uint prefetchCount)
        => client.NewConsumer(Schema.String)
        .Topic(topic)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .MessagePrefetchCount(prefetchCount)
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(_subscription)
        .SubscriptionType(SubscriptionType.Shared)
        .Create();

    private async Task<IMessage<string>> ReceiveMessage(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    // Java Awaitility: until the condition is true (at most 10 seconds, polling every 100 milliseconds)
    private async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
        }

        condition().ShouldBeTrue();
    }

    private async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (!await condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
        }

        (await condition()).ShouldBeTrue();
    }

    // The number of messages that the producers have published to the topic
    private async Task<long> GetMessagesPublished(string topic)
    {
        using var stats = await GetStats(topic);
        return stats.RootElement.GetProperty("msgInCounter").GetInt64();
    }

    private async Task WaitForBacklog(string topic, long expected)
    {
        var backlog = await GetBacklog(topic);
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (backlog != expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
            backlog = await GetBacklog(topic);
        }

        backlog.ShouldBe(expected);
    }

    // Java: getStats(topic).getSubscriptions().get(subscription).getConsumers().get(0).getUnackedMessages()
    private async Task WaitForConsumerUnackedMessages(string topic, long expected)
    {
        var unacked = long.MaxValue;
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (unacked != expected && DateTime.UtcNow < deadline)
        {
            using var stats = await GetStats(topic);
            unacked = stats.RootElement.GetProperty("subscriptions").GetProperty(_subscription).GetProperty("consumers")[0].GetProperty("unackedMessages").GetInt64();

            if (unacked != expected)
                await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
        }

        unacked.ShouldBe(expected);
    }

    // The number of messages that the broker has sent to each consumer of the subscription, as soon as all the consumers have been sent messages
    private async Task<List<long>> WaitForConsumersToHaveMessages(string topic, int numberOfConsumers, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        List<long> messagesSent;

        while (true)
        {
            using var stats = await GetStats(topic);
            messagesSent = stats.RootElement.GetProperty("subscriptions").GetProperty(_subscription).GetProperty("consumers").EnumerateArray().Select(consumer => consumer.GetProperty("msgOutCounter").GetInt64()).ToList();

            if (messagesSent.Count == numberOfConsumers && messagesSent.All(count => count > 0))
                return messagesSent;

            if (DateTime.UtcNow >= deadline)
                return messagesSent;

            await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
        }
    }

    // The deduplication of the broker is enabled for the namespace, which is created for the topic
    private async Task<string> CreateTopicWithDeduplication()
    {
        var ns = $"public/deduplication-{Guid.NewGuid():N}";
        await ExecInPulsar($"bin/pulsar-admin namespaces create {ns}");
        await ExecInPulsar($"bin/pulsar-admin namespaces set-deduplication {ns} --enable");
        var topic = $"persistent://{ns}/{Guid.NewGuid():N}";
        await _fixture.CreateTopic(topic, _cts.Token);
        return topic;
    }

    private async Task ExecInPulsar(string command)
    {
        var result = await _fixture.ExecInPulsar(command, _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"The command '{command}' failed: {result.Stderr}");
    }

    private async Task<JsonDocument> GetStats(string topic)
    {
        using var admin = _fixture.CreateAdminClient();
        using var response = await admin.GetAsync($"/admin/v2/{topic.Replace("://", "/")}/stats", _cts.Token);
        _ = response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(_cts.Token));
    }

    private async Task<long> GetBacklog(string topic)
    {
        using var admin = _fixture.CreateAdminClient();
        using var response = await admin.GetAsync($"/admin/v2/{topic.Replace("://", "/")}/stats", _cts.Token);
        _ = response.EnsureSuccessStatusCode();
        using var stats = JsonDocument.Parse(await response.Content.ReadAsStringAsync(_cts.Token));
        return stats.RootElement.GetProperty("subscriptions").GetProperty(_subscription).GetProperty("msgBacklog").GetInt64();
    }
}
