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

    private async Task<long> GetBacklog(string topic)
    {
        using var admin = _fixture.CreateAdminClient();
        using var response = await admin.GetAsync($"/admin/v2/{topic.Replace("://", "/")}/stats", _cts.Token);
        _ = response.EnsureSuccessStatusCode();
        using var stats = JsonDocument.Parse(await response.Content.ReadAsStringAsync(_cts.Token));
        return stats.RootElement.GetProperty("subscriptions").GetProperty(_subscription).GetProperty("msgBacklog").GetInt64();
    }
}
