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

using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;

[Trait("Category", "Unit")]
public sealed class ConsumerChannelTests
{
    [Fact]
    public async Task Send_GivenEntireProducerBatchInOneAck_ShouldAcknowledgeBatch()
    {
        // Arrange
        var (batchHandler, messageFactory, first, second) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(ToProtocolId(first.MessageId));
        command.MessageId.Add(ToProtocolId(second.MessageId));

        // Act
        await channel.Send(command, Current.CancellationToken);

        // Assert
        await connection.Received(1).Send(
            Arg.Is<CommandAck>(ack => ack.MessageId.Count == 1
                && ack.MessageId[0].LedgerId == first.MessageId.LedgerId
                && ack.MessageId[0].EntryId == first.MessageId.EntryId
                && ack.MessageId[0].BatchIndex == -1),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenIncompleteBatchBeforeUnbatchedMessage_ShouldAcknowledgeUnbatchedMessage()
    {
        // Arrange
        var (batchHandler, messageFactory, first, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        var unbatched = new MessageIdData { LedgerId = 1, EntryId = 3, Partition = 0, BatchIndex = -1 };
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(ToProtocolId(first.MessageId));
        command.MessageId.Add(unbatched);

        // Act
        await channel.Send(command, Current.CancellationToken);

        // Assert
        await connection.Received(1).Send(
            Arg.Is<CommandAck>(ack => ack.MessageId.Count == 1
                && ack.MessageId[0].LedgerId == unbatched.LedgerId
                && ack.MessageId[0].EntryId == unbatched.EntryId
                && ack.MessageId[0].BatchIndex == -1),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenUnbatchedMessageBeforeEntireProducerBatch_ShouldAcknowledgeBoth()
    {
        // Arrange
        var (batchHandler, messageFactory, first, second) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        var unbatched = new MessageIdData { LedgerId = 1, EntryId = 3, Partition = 0, BatchIndex = -1 };
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(unbatched);
        command.MessageId.Add(ToProtocolId(first.MessageId));
        command.MessageId.Add(ToProtocolId(second.MessageId));

        // Act
        await channel.Send(command, Current.CancellationToken);

        // Assert
        await connection.Received(1).Send(
            Arg.Is<CommandAck>(ack => ack.MessageId.Count == 2
                && ack.MessageId[0].EntryId == unbatched.EntryId
                && ack.MessageId[1].EntryId == first.MessageId.EntryId
                && ack.MessageId[1].BatchIndex == -1),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenUnbatchedMessages_ShouldAcknowledgeAllInOneCommand()
    {
        // Arrange
        var (batchHandler, messageFactory, _, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(new MessageIdData { LedgerId = 1, EntryId = 3, Partition = 0, BatchIndex = -1 });
        command.MessageId.Add(new MessageIdData { LedgerId = 1, EntryId = 4, Partition = 0, BatchIndex = -1 });

        // Act
        await channel.Send(command, Current.CancellationToken);

        // Assert
        await connection.Received(1).Send(
            Arg.Is<CommandAck>(ack => ack.MessageId.Count == 2
                && ack.MessageId[0].EntryId == 3
                && ack.MessageId[1].EntryId == 4),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenEmptyAcknowledgements_ShouldNotSend()
    {
        // Arrange
        var (batchHandler, messageFactory, _, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        // Act
        await channel.Send(new CommandAck { AckType = CommandAck.Types.AckType.Individual }, Current.CancellationToken);

        // Assert
        await connection.DidNotReceive().Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenPartialBatch_ShouldWaitForRemainingMessageBeforeAcknowledging()
    {
        // Arrange
        var (batchHandler, messageFactory, first, second) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        var firstCommand = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        firstCommand.MessageId.Add(ToProtocolId(first.MessageId));
        var secondCommand = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        secondCommand.MessageId.Add(ToProtocolId(second.MessageId));

        // Act
        await channel.Send(firstCommand, Current.CancellationToken);
        await connection.DidNotReceive().Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        await channel.Send(secondCommand, Current.CancellationToken);

        // Assert
        await connection.Received(1).Send(
            Arg.Is<CommandAck>(ack => ack.MessageId.Count == 1
                && ack.MessageId[0].EntryId == first.MessageId.EntryId
                && ack.MessageId[0].BatchIndex == -1),
            Arg.Any<CancellationToken>());
    }

    private static (BatchHandler<byte[]> Handler, MessageFactory<byte[]> Factory, DotPulsar.Abstractions.IMessage<byte[]> First, DotPulsar.Abstractions.IMessage<byte[]> Second) CreateBatch()
    {
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 2);
        messages.Count.ShouldBe(2);
        messages[0].MessageId.BatchIndex.ShouldBe(0);
        messages[1].MessageId.BatchIndex.ShouldBe(1);

        return (batchHandler, messageFactory, messages[0], messages[1]);
    }

    [Fact]
    public async Task Send_GivenAckReceiptEnabledAndProtocol19_ShouldSendWithResponseAndReturnResponse()
    {
        // Arrange
        var (batchHandler, messageFactory, _, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse() };
        connection.SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(response);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, true);

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(new MessageIdData { LedgerId = 1, EntryId = 5, Partition = 0, BatchIndex = -1 });

        // Act
        var actual = await channel.Send(command, Current.CancellationToken);

        // Assert
        actual.ShouldBeSameAs(response);
        await connection.Received(1).SendWithResponse(Arg.Is<CommandAck>(ack => ack.ConsumerId == 1), Arg.Any<CancellationToken>());
        await connection.DidNotReceive().Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenAckReceiptEnabledAndProtocol14_ShouldSendWithoutRequestIdAndReturnNull()
    {
        // Arrange
        var (batchHandler, messageFactory, _, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(14);
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, true);

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual, RequestId = 7 };
        command.MessageId.Add(new MessageIdData { LedgerId = 1, EntryId = 5, Partition = 0, BatchIndex = -1 });

        // Act
        var actual = await channel.Send(command, Current.CancellationToken);

        // Assert
        actual.ShouldBeNull();
        await connection.Received(1).Send(Arg.Is<CommandAck>(ack => !ack.HasRequestId), Arg.Any<CancellationToken>());
        await connection.DidNotReceive().SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenAckReceiptDisabled_ShouldClearRequestIdOnPooledCommand()
    {
        // Arrange
        var (batchHandler, messageFactory, _, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, false);

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual, RequestId = 7 };
        command.MessageId.Add(new MessageIdData { LedgerId = 1, EntryId = 5, Partition = 0, BatchIndex = -1 });

        // Act
        var actual = await channel.Send(command, Current.CancellationToken);

        // Assert
        actual.ShouldBeNull();
        command.HasRequestId.ShouldBeFalse();
        await connection.Received(1).Send(command, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenAckReceiptEnabledAndPartialBatch_ShouldNotSendAndReturnNull()
    {
        // Arrange
        var (batchHandler, messageFactory, first, _) = CreateBatch();
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, true);

        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(ToProtocolId(first.MessageId));

        // Act
        var actual = await channel.Send(command, Current.CancellationToken);

        // Assert
        actual.ShouldBeNull();
        await connection.DidNotReceive().SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        await connection.DidNotReceive().Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenPartialBatchAndBatchIndexAckEnabled_ShouldSendEntryWithAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var command = CreateAck(CommandAck.Types.AckType.Individual, messages[2]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId.Count.ShouldBe(1);
        var messageId = sent[0].MessageId[0];
        messageId.LedgerId.ShouldBe(1UL);
        messageId.EntryId.ShouldBe(2UL);
        messageId.BatchIndex.ShouldBe(-1);
        messageId.HasBatchSize.ShouldBeFalse();
        messageId.AckSet.ShouldBeAckSet(0b11011L);
        sent[0].ConsumerId.ShouldBe(1UL);
    }

    [Fact]
    public async Task Send_GivenTwoIndexesOfSameBatchInOneBulkAckAndBatchIndexAckEnabled_ShouldSendOneEntryWithMergedAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var command = CreateAck(CommandAck.Types.AckType.Individual, messages[1], messages[3]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b10101L);
    }

    [Fact]
    public async Task Send_GivenBulkAckOfWholeBatchAndBatchIndexAckEnabled_ShouldSendOneEntryWithoutAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var command = CreateAck(CommandAck.Types.AckType.Individual, messages[0], messages[1], messages[2]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].AckSet.Count.ShouldBe(0);
    }

    [Fact]
    public async Task Send_GivenIndividualAcksOfAllIndexesAndBatchIndexAckEnabled_ShouldSendAckSetsAndThenEntryWithoutAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);

        //Act
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[1]), Current.CancellationToken);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[2]), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(3);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b110L);
        sent[1].MessageId[0].AckSet.ShouldBeAckSet(0b100L);
        sent[2].MessageId[0].AckSet.Count.ShouldBe(0);
        sent.ShouldAllBe(ack => ack.MessageId[0].EntryId == 2 && ack.MessageId[0].BatchIndex == -1);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfPartialBatchAndBatchIndexAckDisabled_ShouldAckPreviousEntryCumulatively()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = CreateAck(CommandAck.Types.AckType.Cumulative, messages[2]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].LedgerId.ShouldBe(1UL);
        sent[0].MessageId[0].EntryId.ShouldBe(1UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
        sent[0].MessageId[0].AckSet.Count.ShouldBe(0);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfPartialBatchAndBatchIndexAckDisabled_ShouldAcknowledgeEntryWhenRemainingMessagesAreAcknowledged()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Cumulative, messages[2]), Current.CancellationToken);
        sent.Clear();

        //Act
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[3]), Current.CancellationToken);
        var sentBeforeLastAck = sent.Count;
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[4]), Current.CancellationToken);

        //Assert
        sentBeforeLastAck.ShouldBe(0);
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfPartialBatchAndBatchIndexAckEnabled_ShouldSendCumulativeAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var command = CreateAck(CommandAck.Types.AckType.Cumulative, messages[2]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11000L);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckAfterIndividualAckOfLaterIndexAndBatchIndexAckEnabled_ShouldKeepTheIndividualAckInTheAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[4]), Current.CancellationToken);
        sent.Clear();

        //Act
        await channel.Send(CreateAck(CommandAck.Types.AckType.Cumulative, messages[1]), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b01100L);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfLastIndexOfBatch_ShouldAcknowledgeEntryCumulativelyWithoutAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var command = CreateAck(CommandAck.Types.AckType.Cumulative, messages[4]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
        sent[0].MessageId[0].AckSet.Count.ShouldBe(0);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfFirstEntryInLedgerAndBatchIndexAckDisabled_ShouldNotSend()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 0, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = CreateAck(CommandAck.Types.AckType.Cumulative, messages[2]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.ShouldBeEmpty();
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfLaterEntry_ShouldRemoveTrackersOfEarlierEntries()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var first = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        var second = BatchTestData.AddBatch(batchHandler, 1, 3, 3);
        var third = BatchTestData.AddBatch(batchHandler, 1, 4, 3);
        var otherLedger = BatchTestData.AddBatch(batchHandler, 2, 0, 3);
        var connection = CreateConnection(out _);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, first[0]), Current.CancellationToken);
        var command = CreateAck(CommandAck.Types.AckType.Cumulative, third[1]);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 3, 0), out _).ShouldBeFalse();
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 4, 0), out _).ShouldBeTrue();
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(2, 0, 0), out _).ShouldBeTrue();
        second.Count.ShouldBe(3);
        otherLedger.Count.ShouldBe(3);
    }

    [Fact]
    public async Task Send_GivenCumulativeAckOfUnbatchedMessage_ShouldRemoveTrackersUpToThatEntry()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        _ = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        _ = BatchTestData.AddBatch(batchHandler, 1, 3, 3);
        _ = BatchTestData.AddBatch(batchHandler, 1, 4, 3);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Cumulative };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 3));

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId[0].EntryId.ShouldBe(3UL);
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 3, 0), out _).ShouldBeFalse();
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 4, 0), out _).ShouldBeTrue();
    }

    [Fact]
    public async Task Send_GivenCompletedBatch_ShouldRemoveItsTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 2);
        var connection = CreateConnection(out _);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        //Act
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[1]), Current.CancellationToken);

        //Assert
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenBatchedIdWithoutTracker_ShouldAcknowledgeWholeEntry()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(BatchTestData.CreateBatchedId(1, 9, 3));

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].LedgerId.ShouldBe(1UL);
        sent[0].MessageId[0].EntryId.ShouldBe(9UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
    }

    [Fact]
    public async Task Send_GivenCompletedBatch_ShouldNotLeakBrokerMessageIdDataIntoPooledCommand()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 1);
        var connection = CreateConnection(out _);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var pooled = new MessageIdData();
        pooled.MapFrom(messages[0].MessageId);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(pooled);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        command.MessageId.Count.ShouldBe(1);
        command.MessageId[0].ShouldBeSameAs(pooled);
        pooled.EntryId.ShouldBe(2UL);
        pooled.BatchIndex.ShouldBe(-1);
    }

    [Fact]
    public async Task Send_GivenCommandThatWasAlreadySent_ShouldSendItUnchangedWhenRetried()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);
        var command = CreateAck(CommandAck.Types.AckType.Individual, messages[2]);
        await channel.Send(command, Current.CancellationToken);

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(2);
        sent[1].MessageId.Count.ShouldBe(1);
        sent[1].MessageId[0].EntryId.ShouldBe(2UL);
        sent[1].MessageId[0].AckSet.ShouldBeAckSet(0b11011L);
    }

    [Fact]
    public async Task Send_GivenBatchIndexAckEnabledAndAckReceiptEnabled_ShouldSendAckSetWithResponse()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse() };
        connection.SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(response);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, true, true);

        //Act
        var actual = await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);

        //Assert
        actual.ShouldBeSameAs(response);
        await connection.Received(1).SendWithResponse(Arg.Is<CommandAck>(ack => ack.MessageId[0].AckSet.SequenceEqual(new long[] { 0b11110 })), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenBatchOfMoreThan64MessagesAndBatchIndexAckEnabled_ShouldSendMultipleWords()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 130);
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true);

        //Act
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[100]), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(-1L, unchecked((long) ~(1UL << 36)), 0b11L);
    }

    [Fact]
    public async Task SendTransactional_GivenIndividualAckOfBatchIndex2Of5_ShouldSendBatchSize5AndAckSetWithOnlyIndex2Cleared()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, true, true);
        var command = CreateTransactionalAck(CommandAck.Types.AckType.Individual, messages[2]);

        //Act
        var actual = await channel.SendTransactional(command, Current.CancellationToken);

        //Assert
        actual.AckResponse.ShouldNotBeNull();
        sent.Count.ShouldBe(1);
        sent[0].ConsumerId.ShouldBe(1UL);
        sent[0].TxnidMostBits.ShouldBe(7UL);
        sent[0].TxnidLeastBits.ShouldBe(8UL);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].LedgerId.ShouldBe(1UL);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].HasBatchIndex.ShouldBeFalse();
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11011L);
    }

    [Fact]
    public async Task SendTransactional_GivenCumulativeAckOfBatchIndex2Of5_ShouldSendAckSetForIndexes3And4AndMutateTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var plainConnection = CreateConnection(out var plainSent);
        await using var plainChannel = CreateChannel(plainConnection, batchHandler, messageFactory);
        var command = CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, messages[2]);

        //Act
        await channel.SendTransactional(command, Current.CancellationToken);
        await plainChannel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[3]), Current.CancellationToken);
        var plainSentAfterIndex3 = plainSent.Count;
        await plainChannel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[4]), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].HasBatchIndex.ShouldBeFalse();
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11000L);
        plainSentAfterIndex3.ShouldBe(0);
        plainSent.Count.ShouldBe(1);
        plainSent[0].MessageId[0].EntryId.ShouldBe(2UL);
        plainSent[0].MessageId[0].BatchIndex.ShouldBe(-1);
    }

    [Fact]
    public async Task SendTransactional_GivenNonBatchedId_ShouldSendNoAckSetNorBatchSize()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual, TxnidMostBits = 7, TxnidLeastBits = 8 };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 2));

        //Act
        await channel.SendTransactional(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].AckSet.Count.ShouldBe(0);
        sent[0].MessageId[0].HasBatchSize.ShouldBeFalse();
    }

    [Fact]
    public async Task SendTransactional_GivenBatchIndexAckDisabled_ShouldStillSendAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, false, false);

        //Act
        await channel.SendTransactional(CreateTransactionalAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);

        //Assert
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11110L);
    }

    [Fact]
    public async Task SendTransactional_GivenIndividualAckOfBatchIndex_ShouldNotChangeTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        var connection = CreateTransactionalConnection(out _);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var plainConnection = CreateConnection(out var plainSent);
        await using var plainChannel = CreateChannel(plainConnection, batchHandler, messageFactory);
        await channel.SendTransactional(CreateTransactionalAck(CommandAck.Types.AckType.Individual, messages[1]), Current.CancellationToken);

        //Act
        await plainChannel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);
        await plainChannel.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[2]), Current.CancellationToken);

        //Assert
        plainSent.ShouldBeEmpty();
        batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeTrue();
    }

    [Fact]
    public async Task SendTransactional_GivenBatchOfMoreThan64Messages_ShouldSendMultipleWords()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 130);
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);

        //Act
        await channel.SendTransactional(CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, messages[70]), Current.CancellationToken);

        //Assert
        sent[0].MessageId[0].BatchSize.ShouldBe(130);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0L, unchecked((long) ~((1UL << 7) - 1)), 0b11L);
    }

    [Fact]
    public async Task SendTransactional_GivenBatchedIdWithoutTrackerButWithBatchSize_ShouldUseBatchSizeOfId()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var messageId = BatchTestData.CreateBatchedId(1, 2, 2);
        messageId.BatchSize = 5;
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual, TxnidMostBits = 7, TxnidLeastBits = 8 };
        command.MessageId.Add(messageId);

        //Act
        await channel.SendTransactional(command, Current.CancellationToken);

        //Assert
        sent[0].MessageId[0].HasBatchIndex.ShouldBeFalse();
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11011L);
    }

    [Fact]
    public async Task SendTransactional_GivenBatchedIdWithoutTrackerAndBatchSize_ShouldSendWholeEntry()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual, TxnidMostBits = 7, TxnidLeastBits = 8 };
        command.MessageId.Add(BatchTestData.CreateBatchedId(1, 2, 2));

        //Act
        await channel.SendTransactional(command, Current.CancellationToken);

        //Assert
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].HasBatchIndex.ShouldBeFalse();
        sent[0].MessageId[0].HasBatchSize.ShouldBeFalse();
        sent[0].MessageId[0].AckSet.Count.ShouldBe(0);
    }

    [Fact]
    public async Task SendTransactional_GivenMultipleIdsOfSameBatch_ShouldSendOneMessageIdDataPerId()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = CreateTransactionalAck(CommandAck.Types.AckType.Individual, messages[1], messages[3]);

        //Act
        await channel.SendTransactional(command, Current.CancellationToken);

        //Assert
        sent[0].MessageId.Count.ShouldBe(2);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11101L);
        sent[0].MessageId[1].AckSet.ShouldBeAckSet(0b10111L);
    }

    [Fact]
    public async Task SendTransactional_GivenCommandThatWasAlreadySent_ShouldSendItUnchangedWhenRetried()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, messages[2]);
        await channel.SendTransactional(command, Current.CancellationToken);

        //Act
        await channel.SendTransactional(command, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(2);
        sent[1].MessageId[0].BatchSize.ShouldBe(5);
        sent[1].MessageId[0].AckSet.ShouldBeAckSet(0b11000L);
    }

    [Fact]
    public async Task SendTransactional_GivenProtocolVersionWithoutAckReceipt_ShouldThrowNotSupportedExceptionAndNotChangeTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(16);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory);
        var command = CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, messages[2]);

        //Act
        var exception = await Record.ExceptionAsync(async () => await channel.SendTransactional(command, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<NotSupportedException>();
        await connection.DidNotReceive().SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        command.MessageId[0].BatchIndex.ShouldBe(2);
        command.MessageId[0].AckSet.Count.ShouldBe(0);
    }

    // A cumulative acknowledgment of a transaction clears the bits of the tracker. When the transaction is aborted, the broker delivers the entry again.
    [Fact]
    public async Task Send_GivenRedeliveryAfterTransactionalCumulativeAck_ShouldAcknowledgeEntryOnlyWhenAllIndexesAreAcknowledgedAgain()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var queue = new AsyncQueue<MessagePackage>();
        var connection = CreateTransactionalConnection(out var transactionalSent);
        var plainSent = new List<CommandAck>();
        connection.Send(Arg.Do<CommandAck>(ack => plainSent.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var delivered = await ReceiveMessages(channel, 5);
        await channel.SendTransactional(CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, delivered[2]), Current.CancellationToken);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var redelivered = await ReceiveMessages(channel, 5);

        //Act
        var entryAcksAfterEachAck = new List<int>();
        foreach (var batchIndex in new[] { 3, 4, 0, 1, 2 })
        {
            await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, redelivered[batchIndex]), Current.CancellationToken);
            entryAcksAfterEachAck.Add(plainSent.Count);
        }

        //Assert
        transactionalSent.Count.ShouldBe(1);
        entryAcksAfterEachAck.ShouldBeSequence(0, 0, 0, 0, 1);
        plainSent[0].MessageId[0].LedgerId.ShouldBe(1UL);
        plainSent[0].MessageId[0].EntryId.ShouldBe(2UL);
        plainSent[0].MessageId[0].HasBatchIndex.ShouldBeFalse();
        plainSent[0].MessageId[0].AckSet.Count.ShouldBe(0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Send_GivenRedeliveryOfPartiallyAcknowledgedBatch_ShouldAcknowledgeEntryOnlyWhenAllDeliveredIndexesAreAcknowledged(bool batchIndexAckEnabled)
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var queue = new AsyncQueue<MessagePackage>();
        var connection = CreateConnection(out var sent);
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: batchIndexAckEnabled, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var delivered = await ReceiveMessages(channel, 5);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, delivered[0]), Current.CancellationToken);
        await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, delivered[1]), Current.CancellationToken);

        // The broker only knows about the indexes that were sent to it, which is none when the batch index acknowledgment is disabled
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5, batchIndexAckEnabled ? BatchTestData.CreateAckSet(0b11100) : null));
        var redelivered = await ReceiveMessages(channel, batchIndexAckEnabled ? 3 : 5);
        sent.Clear();

        //Act
        var entryAcks = new List<int>();
        foreach (var message in Enumerable.Reverse(redelivered))
        {
            await channel.Send(CreateAck(CommandAck.Types.AckType.Individual, message), Current.CancellationToken);
            entryAcks.Add(sent.Count(ack => ack.MessageId[0].AckSet.Count == 0));
        }

        //Assert
        redelivered.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(batchIndexAckEnabled ? [2, 3, 4] : [0, 1, 2, 3, 4]);
        entryAcks.ShouldBeSequence(batchIndexAckEnabled ? [0, 0, 1] : [0, 0, 0, 0, 1]);
    }

    [Fact]
    public async Task Receive_GivenEntryWithAllIndexesAcked_ShouldContinueToNextEntryAndKeepFlowAccounting()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, queue: queue, prefetchCount: 4);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 3, BatchTestData.CreateAckSet(0)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 3)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 4)));

        //Act
        var second = await channel.Receive(Current.CancellationToken);
        var third = await channel.Receive(Current.CancellationToken);
        var flowsAfterTwoMessages = connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow);
        var fourth = await channel.Receive(Current.CancellationToken);

        //Assert
        second.MessageId.EntryId.ShouldBe(2UL);
        third.MessageId.EntryId.ShouldBe(3UL);
        fourth.MessageId.EntryId.ShouldBe(4UL);
        flowsAfterTwoMessages.ShouldBe(1);
        connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow).ShouldBe(2);
    }

    [Fact]
    public async Task Receive_GivenCompactedOutMessagesInBatch_ShouldCountThemAsReceivedForFlow()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, queue: queue, prefetchCount: 4);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 3, null, 0, 1));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));

        //Act
        var first = await channel.Receive(Current.CancellationToken);
        var flowsAfterFirstMessage = connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow);
        var second = await channel.Receive(Current.CancellationToken);

        //Assert
        first.MessageId.BatchIndex.ShouldBe(2);
        second.MessageId.EntryId.ShouldBe(2UL);
        flowsAfterFirstMessage.ShouldBe(1);
        connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow).ShouldBe(2);
    }

    [Fact]
    public async Task Receive_GivenBrokerAckSet_ShouldDeliverOnlyUnacknowledgedIndexes()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 5, BatchTestData.CreateAckSet(0b10110)));

        //Act
        var received = new List<DotPulsar.Abstractions.IMessage<byte[]>>();
        for (var i = 0; i < 3; ++i)
        {
            received.Add(await channel.Receive(Current.CancellationToken));
        }

        //Assert
        received.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(1, 2, 4);
    }

    [Fact]
    public async Task Receive_GivenBatch_ShouldReturnMessageIdsWithTopic()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var channel = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 2));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));

        //Act
        var first = await channel.Receive(Current.CancellationToken);
        var second = await channel.Receive(Current.CancellationToken);
        var unbatched = await channel.Receive(Current.CancellationToken);

        //Assert
        first.MessageId.Topic.ShouldBe(BatchTestData.Topic);
        second.MessageId.Topic.ShouldBe(BatchTestData.Topic);
        unbatched.MessageId.Topic.ShouldBe(BatchTestData.Topic);
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages
    [Fact]
    public async Task Send_GivenRedeliverAllWithPrefetchedMessages_ShouldPurgeQueueAndRefundPermitsAfterTheRedeliverCommand()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 3)));

        //Act
        await uut.Send(new CommandRedeliverUnacknowledgedMessages(), Current.CancellationToken);

        //Assert
        var commands = GetRedeliverAndFlowCommands(connection);
        commands.Count.ShouldBe(2);
        commands[0].ShouldBeOfType<CommandRedeliverUnacknowledgedMessages>().ConsumerId.ShouldBe(1UL);
        commands[1].ShouldBeOfType<CommandFlow>().MessagePermits.ShouldBe(3U);
        queue.RemoveAll().ShouldBeEmpty();
    }

    [Fact]
    public async Task Send_GivenRedeliverAllWithPrefetchedBatch_ShouldRefundOnePermitForEveryMessageOfTheBatch()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 5));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));

        //Act
        await uut.Send(new CommandRedeliverUnacknowledgedMessages(), Current.CancellationToken);

        //Assert
        GetRedeliverAndFlowCommands(connection).OfType<CommandFlow>().Single().MessagePermits.ShouldBe(6U);
    }

    [Fact]
    public async Task Send_GivenRedeliverAllWithMessagesUnpackedFromBatch_ShouldDropThemAndRefundThem()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 5));
        var delivered = await ReceiveMessages(uut, 1);

        //Act
        await uut.Send(new CommandRedeliverUnacknowledgedMessages(), Current.CancellationToken);

        //Assert
        delivered[0].MessageId.BatchIndex.ShouldBe(0);
        GetRedeliverAndFlowCommands(connection).OfType<CommandFlow>().Last().MessagePermits.ShouldBe(4U);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        var next = await uut.Receive(Current.CancellationToken);
        next.MessageId.EntryId.ShouldBe(2UL);
    }

    [Fact]
    public async Task Send_GivenRedeliverAllWithNothingPrefetched_ShouldNotSendFlow()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory);

        //Act
        await uut.Send(new CommandRedeliverUnacknowledgedMessages(), Current.CancellationToken);

        //Assert
        GetRedeliverAndFlowCommands(connection).Single().ShouldBeOfType<CommandRedeliverUnacknowledgedMessages>();
    }

    [Fact]
    public async Task Send_GivenSelectiveRedeliver_ShouldPurgeOnlyMatchingEntries()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 3)));
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 4), 3));
        var command = new CommandRedeliverUnacknowledgedMessages();
        command.MessageIds.Add(BatchTestData.CreateEntryId(1, 2));
        command.MessageIds.Add(BatchTestData.CreateEntryId(1, 4));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        var commands = GetRedeliverAndFlowCommands(connection);
        commands.Count.ShouldBe(2);
        commands[0].ShouldBeSameAs(command);
        commands[1].ShouldBeOfType<CommandFlow>().MessagePermits.ShouldBe(4U);
        var received = await ReceiveMessages(uut, 2);
        received.Select(message => message.MessageId.EntryId).ShouldBeSequence(1UL, 3UL);
    }

    [Fact]
    public async Task Send_GivenSelectiveRedeliverOfEntryWithBatchMessagesNotYetDelivered_ShouldDropThem()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 1), 4));
        _ = await ReceiveMessages(uut, 1);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        var command = new CommandRedeliverUnacknowledgedMessages();
        command.MessageIds.Add(BatchTestData.CreateEntryId(1, 1));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        GetRedeliverAndFlowCommands(connection).OfType<CommandFlow>().Last().MessagePermits.ShouldBe(3U);
        var next = await uut.Receive(Current.CancellationToken);
        next.MessageId.EntryId.ShouldBe(2UL);
    }

    [Fact]
    public async Task Send_GivenSelectiveRedeliverOfEntriesThatAreNotPrefetched_ShouldNotSendFlow()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1)));
        var command = new CommandRedeliverUnacknowledgedMessages();
        command.MessageIds.Add(BatchTestData.CreateEntryId(1, 2));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        GetRedeliverAndFlowCommands(connection).Single().ShouldBeSameAs(command);
        (await uut.Receive(Current.CancellationToken)).MessageId.EntryId.ShouldBe(1UL);
    }

    // A transactional cumulative acknowledgment clears the bits of the tracker, and the messages that were not delivered yet are dropped by the redelivery.
    // The broker delivers the entry again, which replaces the tracker, so the entry is only acknowledged when all of its messages are acknowledged again.
    [Fact]
    public async Task Send_GivenRedeliverAllAfterTransactionalCumulativeAck_ShouldAcknowledgeEntryOnlyWhenAllIndexesAreAcknowledgedAgain()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var queue = new AsyncQueue<MessagePackage>();
        var connection = CreateTransactionalConnection(out _);
        var plainSent = new List<CommandAck>();
        connection.Send(Arg.Do<CommandAck>(ack => plainSent.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var delivered = await ReceiveMessages(uut, 3);
        await uut.SendTransactional(CreateTransactionalAck(CommandAck.Types.AckType.Cumulative, delivered[2]), Current.CancellationToken);
        await uut.Send(new CommandRedeliverUnacknowledgedMessages(), Current.CancellationToken);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var redelivered = await ReceiveMessages(uut, 5);

        //Act
        var entryAcksAfterEachAck = new List<int>();
        foreach (var batchIndex in new[] { 3, 4, 0, 1, 2 })
        {
            await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, redelivered[batchIndex]), Current.CancellationToken);
            entryAcksAfterEachAck.Add(plainSent.Count);
        }

        //Assert
        redelivered.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(0, 1, 2, 3, 4);
        entryAcksAfterEachAck.ShouldBeSequence(0, 0, 0, 0, 1);
    }

    // Upstream: ConsumerBase#isValidConsumerEpoch
    [Fact]
    public async Task Receive_GivenStaleConsumerEpoch_ShouldSkipMessage()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var consumerEpoch = new ConsumerEpoch();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, consumerEpoch: consumerEpoch);
        consumerEpoch.Increment();
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), consumerEpoch: 0));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2), consumerEpoch: 1));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(2UL);
    }

    [Fact]
    public async Task Receive_GivenMessageWithoutConsumerEpoch_ShouldNotSkipMessage()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var consumerEpoch = new ConsumerEpoch();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, consumerEpoch: consumerEpoch);
        consumerEpoch.Increment();
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), consumerEpoch: -1));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(1UL);
    }

    [Fact]
    public async Task Receive_GivenConsumerEpochNewerThanTheChannel_ShouldNotSkipMessage()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, consumerEpoch: new ConsumerEpoch());
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), consumerEpoch: 3));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(1UL);
    }

    [Fact]
    public async Task Receive_GivenNoConsumerEpoch_ShouldNotSkipMessage()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), consumerEpoch: 0));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(1UL);
    }

    [Fact]
    public async Task Receive_GivenStaleBatchEntry_ShouldCountEveryMessageOfTheEntryAsReceivedForFlow()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var consumerEpoch = new ConsumerEpoch();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, prefetchCount: 6, consumerEpoch: consumerEpoch);
        consumerEpoch.Increment();
        queue.Enqueue(BatchTestData.CreatePackage(BatchTestData.CreateEntryId(1, 1), BatchTestData.CreateMetadata(3), BatchTestData.CreateBatchPayload(3), null, 0));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2), consumerEpoch: 1));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(2UL);
        connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow).ShouldBe(2);
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#addIndividualAcknowledgment
    [Fact]
    public async Task Send_GivenIndividualAckOfEntry_ShouldRemoveItFromTrackerAndKeepTheOthers()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out _);
        using var tracker = CreateTracker();
        tracker.Add(new MessageId(1, 3, 0, -1));
        tracker.Add(new MessageId(1, 4, 0, -1));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, unAckedMessageTracker: tracker);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 3));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(1);
        tracker.Remove(1, 4).ShouldBeTrue();
    }

    [Fact]
    public async Task Send_GivenIndividualAckOfCompletedBatch_ShouldRemoveTheEntryFromTrackerOnlyWhenTheLastMessageIsAcknowledged()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out _);
        using var tracker = CreateTracker();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        tracker.Add(messages[0].MessageId);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, unAckedMessageTracker: tracker);

        //Act
        await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);
        var trackedAfterFirstMessage = tracker.Count;
        await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[1]), Current.CancellationToken);
        var trackedAfterSecondMessage = tracker.Count;
        await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[2]), Current.CancellationToken);

        //Assert
        trackedAfterFirstMessage.ShouldBe(1);
        trackedAfterSecondMessage.ShouldBe(1);
        tracker.Count.ShouldBe(0);
    }

    [Fact]
    public async Task Send_GivenIndividualAckOfBatchMessageWithBatchIndexAckEnabled_ShouldNotRemoveTheEntryFromTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out var sent);
        using var tracker = CreateTracker();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        tracker.Add(messages[0].MessageId);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true, unAckedMessageTracker: tracker);

        //Act
        await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[1]), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId[0].AckSet.Count.ShouldBe(1);
        tracker.Count.ShouldBe(1);
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#doCumulativeAck
    [Fact]
    public async Task Send_GivenCumulativeAck_ShouldRemoveAllEntriesUpToAndIncludingTheEntryFromTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out _);
        using var tracker = CreateTracker();
        for (ulong entryId = 1; entryId <= 4; ++entryId)
        {
            tracker.Add(new MessageId(1, entryId, 0, -1));
        }

        await using var uut = CreateChannel(connection, batchHandler, messageFactory, unAckedMessageTracker: tracker);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Cumulative };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 3));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(1);
        tracker.Remove(1, 4).ShouldBeTrue();
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#doCumulativeAck
    // The acknowledgment of a batch that is not completely acknowledged is not sent when the entry before it is not known, but the tracker is still updated
    [Fact]
    public async Task Send_GivenCumulativeAckOfPartialBatchThatIsNotSent_ShouldRemoveEntriesFromTracker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out var sent);
        using var tracker = CreateTracker();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 0, 3);
        tracker.Add(messages[0].MessageId);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, unAckedMessageTracker: tracker);

        //Act
        await uut.Send(CreateAck(CommandAck.Types.AckType.Cumulative, messages[1]), Current.CancellationToken);

        //Assert
        sent.ShouldBeEmpty();
        tracker.Count.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#messageReceived (possibleSendToDeadLetterTopicMessages)
    [Fact]
    public async Task Receive_GivenNonBatchedMessageBelowMaxRedeliverCount_ShouldNotRegisterCandidate()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out var exceeded);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, deadLetterHandler: deadLetterHandler);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), redeliveryCount: 2));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(1UL);
        deadLetterHandler.Count.ShouldBe(0);
        exceeded.ShouldBeEmpty();
    }

    [Fact]
    public async Task Receive_GivenNonBatchedMessageWithMaxRedeliverCount_ShouldRegisterCandidateAndReturnTheMessage()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out var exceeded);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, deadLetterHandler: deadLetterHandler);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), redeliveryCount: 3));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(1UL);
        deadLetterHandler.Contains(1, 1).ShouldBeTrue();
        exceeded.ShouldBeEmpty();
    }

    [Fact]
    public async Task Receive_GivenNonBatchedMessageAboveMaxRedeliverCount_ShouldSkipItAndRequestRedelivery()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out var exceeded);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, deadLetterHandler: deadLetterHandler);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), redeliveryCount: 4));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(2UL);
        deadLetterHandler.Contains(1, 1).ShouldBeTrue();
        exceeded.Select(messageId => (messageId.LedgerId, messageId.EntryId, messageId.BatchIndex)).ShouldBeSequence((1UL, 1UL, -1));
    }

    [Fact]
    public async Task Receive_GivenNonBatchedMessageAboveMaxRedeliverCount_ShouldCountItsPermitAsReceived()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue, prefetchCount: 4, deadLetterHandler: deadLetterHandler);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), redeliveryCount: 4));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 3)));

        //Act
        _ = await uut.Receive(Current.CancellationToken);
        _ = await uut.Receive(Current.CancellationToken);

        //Assert
        connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow).ShouldBe(2);
    }

    [Fact]
    public async Task Receive_GivenNoDeadLetterHandler_ShouldReturnMessageWithHighRedeliveryCount()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var queue = new AsyncQueue<MessagePackage>();
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, queue: queue);
        queue.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), redeliveryCount: 100));

        //Act
        var actual = await uut.Receive(Current.CancellationToken);

        //Assert
        actual.RedeliveryCount.ShouldBe(100U);
    }

    [Fact]
    public async Task Send_GivenIndividualAckOfRegisteredEntry_ShouldRemoveCandidate()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out _);
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        deadLetterHandler.Register(1, 3, BatchTestData.CreateCandidate(1, 3));
        deadLetterHandler.Register(1, 4, BatchTestData.CreateCandidate(1, 4));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, deadLetterHandler: deadLetterHandler);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 3));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        deadLetterHandler.Contains(1, 3).ShouldBeFalse();
        deadLetterHandler.Contains(1, 4).ShouldBeTrue();
    }

    [Fact]
    public async Task Send_GivenCumulativeAck_ShouldNotRemoveCandidate()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out _);
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        deadLetterHandler.Register(1, 3, BatchTestData.CreateCandidate(1, 3));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, deadLetterHandler: deadLetterHandler);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Cumulative };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 3));

        //Act
        await uut.Send(command, Current.CancellationToken);

        //Assert
        deadLetterHandler.Contains(1, 3).ShouldBeTrue();
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse
    [Fact]
    public async Task SendTransactional_GivenAckOfRegisteredEntry_ShouldNotRemoveCandidate()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateTransactionalConnection(out _);
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        deadLetterHandler.Register(1, 3, BatchTestData.CreateCandidate(1, 3));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, deadLetterHandler: deadLetterHandler);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual, TxnidMostBits = 7, TxnidLeastBits = 8 };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 3));

        //Act
        await uut.SendTransactional(command, Current.CancellationToken);

        //Assert
        deadLetterHandler.Contains(1, 3).ShouldBeTrue();
    }

    [Fact]
    public async Task Send_GivenIndividualAckOfBatchMessageWithBatchIndexAckEnabled_ShouldNotRemoveCandidateUntilTheBatchIsAcknowledged()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = CreateConnection(out _);
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 2);
        deadLetterHandler.Register(1, 2, BatchTestData.CreateCandidate(1, 2));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, batchIndexAckEnabled: true, deadLetterHandler: deadLetterHandler);

        //Act
        await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[0]), Current.CancellationToken);
        var containedAfterFirstMessage = deadLetterHandler.Contains(1, 2);
        await uut.Send(CreateAck(CommandAck.Types.AckType.Individual, messages[1]), Current.CancellationToken);

        //Assert
        containedAfterFirstMessage.ShouldBeTrue();
        deadLetterHandler.Contains(1, 2).ShouldBeFalse();
    }

    // Upstream: ConsumerImpl#seekAsyncInternal
    [Fact]
    public async Task Send_GivenSeek_ShouldClearCandidates()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandSeek>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess() });
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        deadLetterHandler.Register(1, 3, BatchTestData.CreateCandidate(1, 3));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, deadLetterHandler: deadLetterHandler);

        //Act
        await uut.Send(new CommandSeek { MessagePublishTime = 1 }, Current.CancellationToken);

        //Assert
        deadLetterHandler.Count.ShouldBe(0);
    }

    [Fact]
    public async Task Send_GivenRedeliverAll_ShouldNotClearCandidates()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var connection = Substitute.For<IConnection>();
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        deadLetterHandler.Register(1, 3, BatchTestData.CreateCandidate(1, 3));
        await using var uut = CreateChannel(connection, batchHandler, messageFactory, deadLetterHandler: deadLetterHandler);

        //Act
        await uut.Send(new CommandRedeliverUnacknowledgedMessages(), Current.CancellationToken);

        //Assert
        deadLetterHandler.Contains(1, 3).ShouldBeTrue();
    }

    private static UnAckedMessageTracker CreateTracker()
        => new(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1), (_, _) => default);

    private static List<object> GetRedeliverAndFlowCommands(IConnection connection)
        => connection.ReceivedCalls()
            .Select(call => call.GetArguments().FirstOrDefault())
            .Where(argument => argument is CommandRedeliverUnacknowledgedMessages or CommandFlow)
            .Select(argument => argument!)
            .ToList();

    private static async Task<List<DotPulsar.Abstractions.IMessage<byte[]>>> ReceiveMessages(ConsumerChannel<byte[]> channel, int count)
    {
        var messages = new List<DotPulsar.Abstractions.IMessage<byte[]>>();

        for (var i = 0; i < count; ++i)
        {
            messages.Add(await channel.Receive(Current.CancellationToken));
        }

        return messages;
    }

    private static (BatchHandler<byte[]> Handler, MessageFactory<byte[]> Factory) CreateBatchHandler()
    {
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        return (new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, null), messageFactory);
    }

    private static IConnection CreateConnection(out List<CommandAck> sent)
    {
        var acks = new List<CommandAck>();
        sent = acks;
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return connection;
    }

    private static IConnection CreateTransactionalConnection(out List<CommandAck> sent)
    {
        var acks = new List<CommandAck>();
        sent = acks;
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse() };
        connection.SendWithResponse(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(response);
        return connection;
    }

    private static CommandAck CreateAck(CommandAck.Types.AckType ackType, params DotPulsar.Abstractions.IMessage<byte[]>[] messages)
    {
        var command = new CommandAck { AckType = ackType };

        foreach (var message in messages)
        {
            command.MessageId.Add(ToProtocolId(message.MessageId));
        }

        return command;
    }

    private static CommandAck CreateTransactionalAck(CommandAck.Types.AckType ackType, params DotPulsar.Abstractions.IMessage<byte[]>[] messages)
    {
        var command = CreateAck(ackType, messages);
        command.TxnidMostBits = 7;
        command.TxnidLeastBits = 8;
        return command;
    }

    private static ConsumerChannel<byte[]> CreateChannel(
        IConnection connection,
        BatchHandler<byte[]> batchHandler,
        MessageFactory<byte[]> messageFactory,
        bool ackReceiptEnabled = false,
        bool batchIndexAckEnabled = false,
        AsyncQueue<MessagePackage>? queue = null,
        uint prefetchCount = 100,
        ConsumerEpoch? consumerEpoch = null,
        UnAckedMessageTracker? unAckedMessageTracker = null,
        DeadLetterHandler<byte[]>? deadLetterHandler = null,
        ChunkedMessageAssembler? chunkAssembler = null)
        => new(1, prefetchCount, queue ?? new AsyncQueue<MessagePackage>(), connection, batchHandler, messageFactory, [], BatchTestData.Topic, ackReceiptEnabled, batchIndexAckEnabled, consumerEpoch, unAckedMessageTracker, deadLetterHandler, chunkAssembler ?? ChunkTestData.CreateAssembler(), null, null, ConsumerCryptoFailureAction.Fail);

    private static MessageIdData ToProtocolId(MessageId messageId)
        => new()
        {
            LedgerId = messageId.LedgerId,
            EntryId = messageId.EntryId,
            Partition = messageId.Partition,
            BatchIndex = messageId.BatchIndex
        };
}
