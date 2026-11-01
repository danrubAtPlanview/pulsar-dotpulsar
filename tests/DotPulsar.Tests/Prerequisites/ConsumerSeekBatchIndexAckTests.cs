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
using DotPulsar.Tests.Internal;

// The broker has acknowledgmentAtBatchIndexLevelEnabled=true, which it needs to use the ack set of a seek. The tests with a broker that has it set to false are in
// ConsumerSeekWithoutBatchIndexAckTests.
// ConsumerSeekBatchIndexAckTest of the upstream has one test, testSeekReplaysBatchAfterSuccessWhileDisconnected, with the rows partialBatchAck = false and true.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class ConsumerSeekBatchIndexAckTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public ConsumerSeekBatchIndexAckTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: ConsumerSeekBatchIndexAckTest#testSeekReplaysBatchAfterSuccessWhileDisconnected
    // The batch of two messages is received, and the first message is acknowledged in the row partialBatchAck = true. The seek to the earliest message must replay the batch with both its messages.
    // The upstream test also pauses the consumer, delays its reconnect after the seek (which the broker causes), and groups the acknowledgments for an hour, so that it can check that a seek that
    // succeeds while the consumer is disconnected does not let the acknowledgments that were grouped before it reach the broker after it. The consumer of DotPulsar can not be paused, does not group
    // its acknowledgments (the first message is acknowledged at once), and the reconnect of it can not be delayed, so the test can not do that. It does the rest of the test, which is the
    // replay of the batch with both its messages.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Seek_GivenBatchWithFirstMessageAcknowledgedOrNot_ShouldReplayAllMessagesOfTheBatch(bool partialBatchAck)
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-ack");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, "seek-batch-index-ack", true);
        await using var producer = CreateProducer(client, topicName, 2);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receipts = await Send(producer, 2);
        var firstId = await receipts[0];
        var secondId = await receipts[1];
        var first = await Receive(consumer);
        var second = await Receive(consumer);

        if (partialBatchAck)
            await consumer.Acknowledge(first, _cts.Token);

        //Act
        await consumer.Seek(MessageId.Earliest, _cts.Token);
        var replayedFirst = await Receive(consumer);
        var replayedSecond = await Receive(consumer);

        //Assert
        firstId.BatchIndex.ShouldBe(0);
        secondId.BatchIndex.ShouldBe(1);
        (firstId.LedgerId, firstId.EntryId).ShouldBe((secondId.LedgerId, secondId.EntryId));
        first.Value().ShouldBe("0");
        second.Value().ShouldBe("1");
        replayedFirst.Value().ShouldBe("0");
        replayedSecond.Value().ShouldBe("1");
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();
    }

    // Additional coverage
    // The batch is replayed from the message that the id is of, so the messages of the batch before it are not delivered again. The id is the one of a message that was received, and the consumer has
    // batch index acknowledgment enabled or not, because the ack set of a seek does not depend on it. The batch sizes are around the 64 bits of a word of the ack set.
    [Theory]
    [InlineData(5, 0, true)]
    [InlineData(5, 1, true)]
    [InlineData(5, 2, true)]
    [InlineData(5, 4, true)]
    [InlineData(5, 2, false)]
    [InlineData(64, 63, true)]
    [InlineData(65, 64, true)]
    [InlineData(100, 70, true)]
    [InlineData(100, 99, false)]
    public async Task Seek_GivenMessageIdOfBatchMessage_ShouldReplayTheBatchFromThatMessage(int batchSize, int seekIndex, bool batchIndexAckEnabled)
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, $"subscription-{Guid.NewGuid():N}", batchIndexAckEnabled);
        await using var producer = CreateProducer(client, topicName, (uint) batchSize);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        _ = await Send(producer, batchSize);
        var received = await ReceiveAll(consumer, batchSize);

        //Act
        await consumer.Seek(received[seekIndex].MessageId, _cts.Token);
        var replayed = await ReceiveAll(consumer, batchSize - seekIndex);
        var moreMessage = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        received.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, batchSize));
        replayed.Select(message => message.Value()).ShouldBe(Enumerable.Range(seekIndex, batchSize - seekIndex).Select(i => i.ToString()));
        replayed.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(seekIndex, batchSize - seekIndex));
        moreMessage.ShouldBeNull();
    }

    // Additional coverage
    // The next batches are replayed whole, and the permits of the consumer keep working, because the broker does not charge the messages of the batch that it does not deliver.
    [Fact]
    public async Task Seek_GivenMessageIdOfBatchMessageAndFurtherBatches_ShouldReplayTheRestOfTheBatchAndTheNextBatches()
    {
        //Arrange
        const int batchSize = 5;
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-next");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.String)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(3)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .Topic(topicName)
            .Create();
        await using var producer = CreateProducer(client, topicName, batchSize);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        _ = await Send(producer, 2 * batchSize);
        var received = await ReceiveAll(consumer, 2 * batchSize);

        //Act
        await consumer.Seek(received[2].MessageId, _cts.Token);
        var replayed = await ReceiveAll(consumer, (2 * batchSize) - 2);
        var moreMessage = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        replayed.Select(message => message.Value()).ShouldBe(Enumerable.Range(2, (2 * batchSize) - 2).Select(i => i.ToString()));
        moreMessage.ShouldBeNull();
    }

    // Upstream: ReaderImpl#seekAsync(MessageId) (the seek of a reader is the seek of its consumer)
    [Fact]
    public async Task Seek_GivenReaderAndMessageIdOfBatchMessage_ShouldReplayTheBatchFromThatMessage()
    {
        //Arrange
        const int batchSize = 5;
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-reader");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = CreateProducer(client, topicName, batchSize);
        _ = await Send(producer, batchSize);
        await using var reader = client.NewReader(Schema.String)
            .StartMessageId(MessageId.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        var received = await ReceiveAll(reader, batchSize);

        //Act
        await reader.Seek(received[3].MessageId, _cts.Token);
        var replayed = await ReceiveAll(reader, batchSize - 3);
        var moreMessage = await TransactionFixture.ReceiveOrNull(reader, TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        replayed.Select(message => message.Value()).ShouldBe(["3", "4"]);
        moreMessage.ShouldBeNull();
    }

    // Additional coverage
    // The id that the producer returns for a message of a batch has its batch index, but not the size of the batch, like the id that the Java producer returns (the Java client does not send an ack set
    // for it either, because its batch size is 0, and it drops the messages before the batch index itself, which DotPulsar does not do). So the whole batch is replayed.
    [Fact]
    public async Task Seek_GivenMessageIdThatTheProducerReturnedForBatchMessage_ShouldReplayTheWholeBatch()
    {
        //Arrange
        const int batchSize = 5;
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-sent");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, $"subscription-{Guid.NewGuid():N}", true);
        await using var producer = CreateProducer(client, topicName, batchSize);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receipts = await Send(producer, batchSize);
        var sentId = await receipts[2];
        _ = await ReceiveAll(consumer, batchSize);

        //Act
        await consumer.Seek(sentId, _cts.Token);
        var replayed = await ReceiveAll(consumer, batchSize);

        //Assert
        sentId.BatchIndex.ShouldBe(2);
        sentId.GetBatchSize().ShouldBe(0);
        replayed.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, batchSize));
    }

    // Additional coverage
    // The batch size is not part of the text of an id, so an id that was parsed has none, and the whole batch is replayed.
    [Fact]
    public async Task Seek_GivenParsedMessageIdOfBatchMessage_ShouldReplayTheWholeBatch()
    {
        //Arrange
        const int batchSize = 5;
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-parsed");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, $"subscription-{Guid.NewGuid():N}", true);
        await using var producer = CreateProducer(client, topicName, batchSize);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        _ = await Send(producer, batchSize);
        var received = await ReceiveAll(consumer, batchSize);
        MessageId.TryParse(received[2].MessageId.ToString(), out var parsed).ShouldBeTrue();

        //Act
        await consumer.Seek(parsed, _cts.Token);
        var replayed = await ReceiveAll(consumer, batchSize);

        //Assert
        parsed.BatchIndex.ShouldBe(2);
        replayed.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, batchSize));
    }

    // Additional coverage
    // A message that is not in a batch is seeked to as before, and the message itself is the first that is replayed.
    [Fact]
    public async Task Seek_GivenMessageIdOfMessageThatIsNotInBatch_ShouldReplayFromThatMessage()
    {
        //Arrange
        const int numberOfMessages = 5;
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-no-batch");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = CreateConsumer(client, topicName, $"subscription-{Guid.NewGuid():N}", true);
        await using var producer = client.NewProducer(Schema.String).StateChangedHandler(_testOutputHelper.Log).Topic(topicName).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            _ = await producer.Send(i.ToString(), _cts.Token);
        }

        var received = await ReceiveAll(consumer, numberOfMessages);

        //Act
        await consumer.Seek(received[3].MessageId, _cts.Token);
        var replayed = await ReceiveAll(consumer, numberOfMessages - 3);

        //Assert
        received[3].MessageId.BatchIndex.ShouldBe(-1);
        replayed.Select(message => message.Value()).ShouldBe(["3", "4"]);
    }

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topicName, string subscriptionName, bool batchIndexAckEnabled)
        => client.NewConsumer(Schema.String)
            .BatchIndexAckEnabled(batchIndexAckEnabled)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .Topic(topicName)
            .Create();

    // The producer closes a batch when it has the number of messages, and the time window of a batch is so long that it is never closed by it
    private IProducer<string> CreateProducer(IPulsarClient client, string topicName, uint batchSize)
        => client.NewProducer(Schema.String)
            .BatchingMaxMessages(batchSize)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message. The producer is flushed at the end.
    private async Task<List<Task<MessageId>>> Send(IProducer<string> producer, int count)
    {
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < count; i++)
        {
            var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

            await producer.SendChannel.Send(new MessageMetadata(), i.ToString(), messageId =>
            {
                _ = receipt.TrySetResult(messageId);
                return ValueTask.CompletedTask;
            }, _cts.Token);

            receipts.Add(receipt.Task);
        }

        await producer.Flush(_cts.Token);
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        return receipts;
    }

    private async Task<IMessage<string>> Receive(IReceive<IMessage<string>> receiver)
    {
        var message = await TransactionFixture.ReceiveOrNull(receiver, TimeSpan.FromSeconds(10), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private async Task<List<IMessage<string>>> ReceiveAll(IReceive<IMessage<string>> receiver, int count)
    {
        var messages = new List<IMessage<string>>();

        for (var i = 0; i < count; i++)
        {
            messages.Add(await Receive(receiver));
        }

        return messages;
    }
}
