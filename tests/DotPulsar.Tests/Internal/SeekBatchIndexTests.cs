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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;

// The command that a seek to the id of a message in a batch sends: the entry, and an ack set that has the messages before the message acknowledged (upstream ConsumerImpl#seekAsync(MessageId))
[Trait("Category", "Unit")]
public sealed class SeekBatchIndexTests
{
    private const string Topic = "persistent://public/default/test";

    // Upstream: ConsumerImpl#seekAsync(MessageId) (the bits [0, batchSize) are set, and the bits [0, batchIndex) are cleared)
    [Theory]
    [InlineData(5, 0, new long[] { 0b11111 })]
    [InlineData(5, 1, new long[] { 0b11110 })]
    [InlineData(5, 2, new long[] { 0b11100 })]
    [InlineData(5, 4, new long[] { 0b10000 })]
    [InlineData(1, 0, new long[] { 1 })]
    [InlineData(64, 0, new long[] { -1 })]
    [InlineData(64, 1, new long[] { -2 })]
    [InlineData(64, 63, new long[] { long.MinValue })]
    [InlineData(65, 63, new long[] { long.MinValue, 1 })]
    [InlineData(65, 64, new long[] { 0, 1 })]
    [InlineData(100, 70, new long[] { 0, 0xFFFFFFFC0 })]
    [InlineData(128, 127, new long[] { 0, long.MinValue })]
    [InlineData(200, 0, new long[] { -1, -1, -1, 0xFF })]
    public void ToSeekMessageIdData_GivenMessageIdOfBatchMessage_ShouldHaveAckSetWithTheMessagesBeforeItAcknowledged(int batchSize, int batchIndex, long[] expected)
    {
        //Arrange
        var messageId = MessageIdInternals.Create(1, 2, 3, batchIndex, Topic, batchSize);

        //Act
        var actual = messageId.ToSeekMessageIdData();

        //Assert
        actual.LedgerId.ShouldBe(1UL);
        actual.EntryId.ShouldBe(2UL);
        actual.AckSet.ShouldBeAckSet(expected);
        actual.HasBatchSize.ShouldBeFalse();
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId) (the bits [0, batchSize) are set, and the bits [0, batchIndex) are cleared, which are all the bits if the index is not in the batch)
    [Theory]
    [InlineData(5, 5)]
    [InlineData(5, 100)]
    public void ToSeekMessageIdData_GivenBatchIndexThatIsNotInTheBatch_ShouldHaveNoAckSet(int batchSize, int batchIndex)
    {
        //Arrange
        var messageId = MessageIdInternals.Create(1, 2, 3, batchIndex, Topic, batchSize);

        //Act
        var actual = messageId.ToSeekMessageIdData();

        //Assert
        actual.AckSet.Count.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId) (isBatch is a batch index of at least 0 and a batch size above 0)
    [Theory]
    [InlineData(-1, 5)]
    [InlineData(-1, 0)]
    [InlineData(0, 0)]
    [InlineData(3, 0)]
    public void ToSeekMessageIdData_GivenMessageIdThatIsNotOfBatchMessage_ShouldHaveNoAckSet(int batchIndex, int batchSize)
    {
        //Arrange
        var messageId = MessageIdInternals.Create(1, 2, 3, batchIndex, Topic, batchSize);

        //Act
        var actual = messageId.ToSeekMessageIdData();

        //Assert
        actual.LedgerId.ShouldBe(1UL);
        actual.EntryId.ShouldBe(2UL);
        actual.AckSet.Count.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId) (the ids of the constants are not batch ids)
    [Fact]
    public void ToSeekMessageIdData_GivenEarliestAndLatest_ShouldHaveNoAckSet()
    {
        //Act & Assert
        MessageId.Earliest.ToSeekMessageIdData().AckSet.Count.ShouldBe(0);
        MessageId.Latest.ToSeekMessageIdData().AckSet.Count.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId) (a chunked message is seeked to by its first chunk, and with an empty ack set)
    [Fact]
    public void ToSeekMessageIdData_GivenChunkedMessageId_ShouldBeTheFirstChunkWithNoAckSet()
    {
        //Arrange
        var messageId = MessageIdInternals.CreateChunked(Topic, ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11));

        //Act
        var actual = messageId.ToSeekMessageIdData();

        //Assert
        actual.EntryId.ShouldBe(10UL);
        actual.AckSet.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void ToSeekMessageIdData_GivenMessageIdOfBatchMessage_ShouldNotChangeTheMessageId()
    {
        //Arrange
        var messageId = MessageIdInternals.Create(1, 2, 3, 4, Topic, 10);

        //Act
        var first = messageId.ToSeekMessageIdData();
        var second = messageId.ToSeekMessageIdData();

        //Assert
        first.ShouldNotBeSameAs(second);
        first.AckSet.ShouldBeAckSet(0b1111110000);
        second.AckSet.ShouldBeAckSet(0b1111110000);
        messageId.BatchIndex.ShouldBe(4);
        messageId.GetBatchSize().ShouldBe(10);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId)
    [Fact]
    public async Task SubConsumerSeek_GivenMessageIdOfBatchMessage_ShouldSendTheEntryWithAckSetOfTheMessagesBeforeItAcknowledged()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubConsumer(channel);

        //Act
        await uut.Seek(MessageIdInternals.Create(1, 12, 0, 2, Topic, 5), Current.CancellationToken);

        //Assert
        var messageId = sent.ShouldHaveSingleItem().MessageId;
        messageId.LedgerId.ShouldBe(1UL);
        messageId.EntryId.ShouldBe(12UL);
        messageId.AckSet.ShouldBeAckSet(0b11100);
    }

    // Additional coverage
    [Fact]
    public async Task SubConsumerSeek_GivenMessageIdThatIsNotOfBatchMessage_ShouldSendTheEntryWithoutAckSet()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubConsumer(channel);

        //Act
        await uut.Seek(new MessageId(1, 12, 0, -1, Topic), Current.CancellationToken);

        //Assert
        var messageId = sent.ShouldHaveSingleItem().MessageId;
        messageId.EntryId.ShouldBe(12UL);
        messageId.AckSet.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task SubConsumerSeek_GivenPublishTime_ShouldNotSendAMessageId()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubConsumer(channel);

        //Act
        await uut.Seek(1234UL, Current.CancellationToken);

        //Assert
        var command = sent.ShouldHaveSingleItem();
        command.MessagePublishTime.ShouldBe(1234UL);
        command.MessageId.ShouldBeNull();
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId) (ReaderImpl#seekAsync(MessageId) seeks the consumer of the reader)
    [Fact]
    public async Task SubReaderSeek_GivenMessageIdOfBatchMessage_ShouldSendTheEntryWithAckSetOfTheMessagesBeforeItAcknowledged()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubReader(channel);

        //Act
        await uut.Seek(MessageIdInternals.Create(1, 12, 0, 70, Topic, 100), Current.CancellationToken);

        //Assert
        var messageId = sent.ShouldHaveSingleItem().MessageId;
        messageId.EntryId.ShouldBe(12UL);
        messageId.AckSet.ShouldBeAckSet(0, 0xFFFFFFFC0);
    }

    // Additional coverage
    [Fact]
    public async Task SubReaderSeek_GivenMessageIdThatIsNotOfBatchMessage_ShouldSendTheEntryWithoutAckSet()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubReader(channel);

        //Act
        await uut.Seek(new MessageId(1, 12, 0, -1, Topic), Current.CancellationToken);

        //Assert
        sent.ShouldHaveSingleItem().MessageId.AckSet.Count.ShouldBe(0);
    }

    // Upstream: ConsumerSeekBatchIndexAckTest#testSeekReplaysBatchAfterSuccessWhileDisconnected (the part about the partial batch ack, without the disconnection: the batch that was partially
    // acknowledged before the seek is replayed with both its messages, and it is tracked from its first message, and not as if its first message was acknowledged. The acknowledgments are not
    // grouped here, so a stale acknowledgment can not be sent after the seek. The tracker of the batch is gone after the seek, and it is replaced when the entry is delivered again anyway.)
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ChannelSend_GivenSeekAfterPartialBatchAck_ShouldTrackTheReplayedBatchFromItsFirstMessage(bool partialBatchAck)
    {
        //Arrange
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        var batchHandler = new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, null);
        var queue = new AsyncQueue<MessagePackage>();
        var acks = new List<CommandAck>();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        connection.Send(Arg.Any<CommandSeek>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess() });
        await using var channel = new ConsumerChannel<byte[]>(1, 100, queue, connection, batchHandler, messageFactory, [], BatchTestData.Topic, false, true, null, null, null, ChunkTestData.CreateAssembler(), null, null, ConsumerCryptoFailureAction.Fail);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 2));
        var first = await channel.Receive(Current.CancellationToken);
        var second = await channel.Receive(Current.CancellationToken);

        if (partialBatchAck)
            await channel.Send(CreateAck(first), Current.CancellationToken);

        //Act
        await channel.Send(new CommandSeek { MessageId = MessageId.Earliest.ToSeekMessageIdData() }, Current.CancellationToken);
        acks.Clear();
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 2));
        var replayedFirst = await channel.Receive(Current.CancellationToken);
        var replayedSecond = await channel.Receive(Current.CancellationToken);
        await channel.Send(CreateAck(replayedSecond), Current.CancellationToken);

        //Assert
        first.MessageId.BatchIndex.ShouldBe(0);
        second.MessageId.BatchIndex.ShouldBe(1);
        replayedFirst.MessageId.BatchIndex.ShouldBe(0);
        replayedSecond.MessageId.BatchIndex.ShouldBe(1);

        // The replayed message 1 is acknowledged alone: message 0 is still not acknowledged, so the ack set has its bit, and the entry is not acknowledged as a whole
        var ack = acks.ShouldHaveSingleItem();
        ack.MessageId.ShouldHaveSingleItem().AckSet.ShouldBeAckSet(0b01);
    }

    private static CommandAck CreateAck(params DotPulsar.Abstractions.IMessage<byte[]>[] messages)
    {
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };

        foreach (var message in messages)
        {
            command.MessageId.Add(BatchTestData.ToMessageIdData(message.MessageId));
        }

        return command;
    }

    private static SubConsumer<byte[]> CreateSubConsumer(IConsumerChannel<byte[]> channel, bool seekToBatchIndex = true)
    {
        var correlationId = Guid.NewGuid();
        var eventRegister = Substitute.For<IRegisterEvent>();
        SubConsumer<byte[]>? subConsumer = null;
        subConsumer = new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            SubscriptionType.Exclusive,
            Topic,
            eventRegister,
            channel,
            new Executor(correlationId, eventRegister, new DefaultExceptionHandler()),
            Substitute.For<IState<ConsumerState>>(),
            Substitute.For<IConsumerChannelFactory<byte[]>>(),
            false,
            TimeSpan.FromSeconds(30),
            new ConsumerEpoch(),
            null,
            new NegativeAcknowledgeTracker(TimeSpan.FromMinutes(1), (ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct)),
            null,
            ChunkTestData.CreateAssembler(),
            null,
            seekToBatchIndex: seekToBatchIndex);

        return subConsumer;
    }

    private static SubReader<byte[]> CreateSubReader(IConsumerChannel<byte[]> channel, bool seekToBatchIndex = true)
    {
        var correlationId = Guid.NewGuid();
        var eventRegister = Substitute.For<IRegisterEvent>();

        return new SubReader<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            Topic,
            eventRegister,
            channel,
            new Executor(correlationId, eventRegister, new DefaultExceptionHandler()),
            Substitute.For<IState<ReaderState>>(),
            Substitute.For<IConsumerChannelFactory<byte[]>>(),
            ChunkTestData.CreateAssembler(),
            null,
            seekToBatchIndex: seekToBatchIndex);
    }

    // Additional coverage (decision 0010: SeekToBatchIndex is the switch, and false restores the previous behaviour)
    [Fact]
    public void ToSeekMessageIdData_GivenNotSeekToBatchIndex_ShouldHaveNoAckSet()
    {
        //Arrange
        var messageId = MessageIdInternals.Create(1, 2, 3, 2, Topic, 5);

        //Act
        var actual = messageId.ToSeekMessageIdData(false);

        //Assert
        actual.LedgerId.ShouldBe(1UL);
        actual.EntryId.ShouldBe(2UL);
        actual.AckSet.ShouldBeEmpty();
    }

    // Additional coverage (decision 0010)
    [Fact]
    public async Task SubConsumerSeek_GivenNotSeekToBatchIndex_ShouldSendTheEntryWithoutAckSet()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubConsumer(channel, false);

        //Act
        await uut.Seek(MessageIdInternals.Create(1, 12, 0, 2, Topic, 5), Current.CancellationToken);

        //Assert
        var messageId = sent.ShouldHaveSingleItem().MessageId;
        messageId.EntryId.ShouldBe(12UL);
        messageId.AckSet.ShouldBeEmpty();
    }

    // Additional coverage (decision 0010)
    [Fact]
    public async Task SubReaderSeek_GivenNotSeekToBatchIndex_ShouldSendTheEntryWithoutAckSet()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateSubReader(channel, false);

        //Act
        await uut.Seek(MessageIdInternals.Create(1, 12, 0, 2, Topic, 5), Current.CancellationToken);

        //Assert
        var messageId = sent.ShouldHaveSingleItem().MessageId;
        messageId.EntryId.ShouldBe(12UL);
        messageId.AckSet.ShouldBeEmpty();
    }
}
