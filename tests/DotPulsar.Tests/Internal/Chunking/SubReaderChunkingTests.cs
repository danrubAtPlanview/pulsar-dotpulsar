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

namespace DotPulsar.Tests.Internal.Chunking;

using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class SubReaderChunkingTests
{
    private const string Topic = "persistent://public/default/test";

    // Upstream: ConsumerImpl#seekAsync(MessageId)
    [Fact]
    public async Task Seek_GivenChunkedMessageId_ShouldSeekToTheFirstChunk()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateUut(channel, ChunkTestData.CreateAssembler());

        //Act
        await uut.Seek(MessageIdInternals.CreateChunked(Topic, ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11)), Current.CancellationToken);

        //Assert
        sent.Single().MessageId.EntryId.ShouldBe(10UL);
    }

    [Fact]
    public async Task Seek_GivenMessageIdThatIsNotChunked_ShouldSeekToIt()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var sent = new List<CommandSeek>();
        channel.Send(Arg.Do<CommandSeek>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        await using var uut = CreateUut(channel, ChunkTestData.CreateAssembler());

        //Act
        await uut.Seek(new MessageId(1, 12, 0, -1, Topic), Current.CancellationToken);

        //Assert
        sent.Single().MessageId.EntryId.ShouldBe(12UL);
    }

    // Upstream: ConsumerImpl#closeConsumerTasks
    [Fact]
    public async Task DisposeAsync_ShouldDisposeTheChunkAssembler()
    {
        //Arrange
        var assembler = ChunkTestData.CreateAssembler();
        var uut = CreateUut(Substitute.For<IConsumerChannel<byte[]>>(), assembler);

        //Act
        await uut.DisposeAsync();

        //Assert
        FeedTwoChunks(assembler).ShouldBeNull();
    }

    [Fact]
    public async Task ChannelFaulted_ShouldDisposeTheChunkAssembler()
    {
        //Arrange
        var assembler = ChunkTestData.CreateAssembler();
        await using var uut = CreateUut(Substitute.For<IConsumerChannel<byte[]>>(), assembler);

        //Act
        await uut.ChannelFaulted(new Exception("fault"));

        //Assert
        FeedTwoChunks(assembler).ShouldBeNull();
    }

    private static AssembledChunkedMessage? FeedTwoChunks(ChunkedMessageAssembler assembler)
    {
        List<MessageIdData> acks = [];
        List<MessageIdData> track = [];
        assembler.Process(ChunkTestData.CreateEntryId(1, 1), ChunkTestData.CreateChunkMetadata("a", 0, 2), new ReadOnlySequence<byte>([1]), acks, track);
        return assembler.Process(ChunkTestData.CreateEntryId(1, 2), ChunkTestData.CreateChunkMetadata("a", 1, 2), new ReadOnlySequence<byte>([2]), acks, track);
    }

    private static SubReader<byte[]> CreateUut(IConsumerChannel<byte[]> channel, ChunkedMessageAssembler assembler)
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
            assembler);
    }
}
