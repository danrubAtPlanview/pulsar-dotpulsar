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
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class ProducerChannelTests
{
    [Fact]
    public async Task Send_GivenTransactionalThenNonTransactionalMetadata_ShouldClearTxnIdsOnPooledCommand()
    {
        //Arrange
        var written = new List<SentMessage>();
        var uut = CreateChannel(written);
        var transactionalMetadata = new MessageMetadata { SequenceId = 1, TxnidMostBits = 0, TxnidLeastBits = 0 };
        var plainMetadata = new MessageMetadata { SequenceId = 2 };

        //Act
        await uut.Send(transactionalMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await uut.Send(plainMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Count.ShouldBe(2);
        ReferenceEquals(written[0].Package, written[1].Package).ShouldBeTrue("the test only means something if the pooled command is reused");
        written[0].CommandIsTransactional.ShouldBeTrue("the ids are zero, but the broker handles the message as transactional");
        written[1].CommandHasTxnidMostBits.ShouldBeFalse();
        written[1].CommandHasTxnidLeastBits.ShouldBeFalse();
        written[1].MetadataIsTransactional.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenNonTransactionalThenTransactionalMetadata_ShouldSetTxnIdsOnPooledCommand()
    {
        //Arrange
        var written = new List<SentMessage>();
        var uut = CreateChannel(written);
        var plainMetadata = new MessageMetadata { SequenceId = 1 };
        var transactionalMetadata = new MessageMetadata { SequenceId = 2, TxnidMostBits = 3, TxnidLeastBits = 4 };

        //Act
        await uut.Send(plainMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await uut.Send(transactionalMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        ReferenceEquals(written[0].Package, written[1].Package).ShouldBeTrue("the test only means something if the pooled command is reused");
        written[0].CommandIsTransactional.ShouldBeFalse();
        written[1].CommandTxnidMostBits.ShouldBe(3UL);
        written[1].CommandTxnidLeastBits.ShouldBe(4UL);
        written[1].MetadataTxnidMostBits.ShouldBe(3UL);
        written[1].MetadataTxnidLeastBits.ShouldBe(4UL);
    }

    [Fact]
    public async Task Send_GivenBatchMetadata_ShouldSetNumMessagesAndHighestSequenceIdOnCommand()
    {
        //Arrange
        var commands = new List<CommandSend>();
        var uut = CreateChannel(commands);
        var batchMetadata = new MessageMetadata { SequenceId = 10, HighestSequenceId = 14, NumMessagesInBatch = 5 };

        //Act
        await uut.Send(batchMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        commands.Count.ShouldBe(1);
        commands[0].SequenceId.ShouldBe(10UL);
        commands[0].NumMessages.ShouldBe(5);
        commands[0].HighestSequenceId.ShouldBe(14UL);
    }

    [Fact]
    public async Task Send_GivenSingleMessageAfterBatch_ShouldResetNumMessagesAndHighestSequenceId()
    {
        //Arrange
        var commands = new List<CommandSend>();
        var uut = CreateChannel(commands);
        var batchMetadata = new MessageMetadata { SequenceId = 10, HighestSequenceId = 14, NumMessagesInBatch = 5 };
        var singleMetadata = new MessageMetadata { SequenceId = 15 };

        //Act
        await uut.Send(batchMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await uut.Send(singleMetadata, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        commands.Count.ShouldBe(2);
        commands[1].SequenceId.ShouldBe(15UL);
        commands[1].NumMessages.ShouldBe(1);
        commands[1].HasHighestSequenceId.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenMessageThatIsNotBatched_ShouldSendOneMessage()
    {
        //Arrange
        var commands = new List<CommandSend>();
        var uut = CreateChannel(commands);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        commands[0].NumMessages.ShouldBe(1);
        commands[0].HasHighestSequenceId.ShouldBeFalse();
    }

    [Fact]
    public void MaxMessageSize_GivenConnectionWithMaxMessageSize_ShouldReturnIt()
    {
        //Arrange
        var connection = Substitute.For<IConnection>();
        connection.MaxMessageSize.Returns(1234);
        var uut = new ProducerChannel(1, "producer", connection, null, null);

        //Act
        var actual = uut.MaxMessageSize;

        //Assert
        actual.ShouldBe(1234);
    }

    private static ProducerChannel CreateChannel(List<CommandSend> commands)
    {
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            // The command is pooled and reused, so what is checked is copied
            commands.Add(callInfo.Arg<SendPackage>().Command!.Clone());
            return Task.CompletedTask;
        });

        return new ProducerChannel(1, "producer", connection, null, null);
    }

    private static ProducerChannel CreateChannel(List<SentMessage> written)
    {
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            // The package is pooled and reused, so what is checked is copied
            var package = callInfo.Arg<SendPackage>();
            var command = package.Command!;
            var metadata = package.Metadata!;
            written.Add(new SentMessage(
                command.ProducerId,
                command.SequenceId,
                package,
                command.HasTxnidMostBits,
                command.TxnidMostBits,
                command.HasTxnidLeastBits,
                command.TxnidLeastBits,
                metadata.HasTxnidMostBits,
                metadata.TxnidMostBits,
                metadata.HasTxnidLeastBits,
                metadata.TxnidLeastBits));
            return Task.CompletedTask;
        });

        return new ProducerChannel(1, "producer", connection, null, null);
    }
}
