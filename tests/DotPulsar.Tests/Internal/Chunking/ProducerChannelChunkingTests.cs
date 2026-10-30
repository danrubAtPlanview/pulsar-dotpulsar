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

using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

// The planning and the sending of chunks of ProducerChannel, with a connection that records what it is asked to send
[Trait("Category", "Unit")]
public sealed class ProducerChannelChunkingTests
{
    private const string _producerName = "test-producer";
    private const int _maxMessageSize = 1000;

    // Upstream: ProducerImpl#sendAsync
    [Fact]
    public void PlanChunking_GivenPayloadAboveMaxMessageSize_ShouldSplitItInTheNumberOfChunksThatFitTheMaxMessageSize()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());
        var metadata = new MessageMetadata { SequenceId = 7 };
        var payload = CreatePayload(5000);

        //Act
        var state = uut.PlanChunking(metadata, payload, 0);

        //Assert
        state.ShouldNotBeNull();
        var chunkSize = _maxMessageSize - GetMetadataSizeBeforeChunking(state);
        state.ChunkSize.ShouldBe(chunkSize);
        state.TotalChunks.ShouldBe((5000 + chunkSize - 1) / chunkSize);
        state.TotalChunks.ShouldBeGreaterThan(1);
        state.Payload.ToArray().ShouldBe(payload.ToArray());
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenPayload_ShouldSetTheFieldsOfTheChunksOnACopyOfTheMetadata()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());
        var metadata = new MessageMetadata { SequenceId = 7 };
        var payload = CreatePayload(5000);

        //Act
        var state = uut.PlanChunking(metadata, payload, 0);

        //Assert
        state.ShouldNotBeNull();
        state.Metadata.ShouldNotBeSameAs(metadata);
        state.Metadata.SequenceId.ShouldBe(7UL);
        state.Metadata.ProducerName.ShouldBe(_producerName);
        state.Metadata.HasPublishTime.ShouldBeTrue();
        state.Metadata.Uuid.ShouldBe($"{_producerName}-7");
        state.Metadata.NumChunksFromMsg.ShouldBe(state.TotalChunks);
        state.Metadata.TotalChunkMsgSize.ShouldBe(5000);
        metadata.HasUuid.ShouldBeFalse("the metadata of the caller is not changed");
        metadata.HasProducerName.ShouldBeFalse("the metadata of the caller is not changed");
        metadata.HasPublishTime.ShouldBeFalse("the metadata of the caller is not changed");
        metadata.HasNumChunksFromMsg.ShouldBeFalse("the metadata of the caller is not changed");
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenChunkMaxMessageSizeSmallerThanWhatFits_ShouldUseIt()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());

        //Act
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(5000), 100);

        //Assert
        state.ShouldNotBeNull();
        state.ChunkSize.ShouldBe(100);
        state.TotalChunks.ShouldBe(50);
    }

    // Additional coverage
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(_maxMessageSize)]
    [InlineData(_maxMessageSize * 2)]
    public void PlanChunking_GivenChunkMaxMessageSizeThatIsNotSmallerThanWhatFits_ShouldUseWhatFits(int chunkMaxMessageSize)
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());

        //Act
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(5000), chunkMaxMessageSize);

        //Assert
        state.ShouldNotBeNull();
        state.ChunkSize.ShouldBe(_maxMessageSize - GetMetadataSizeBeforeChunking(state));
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenMetadataThatDoesNotFitInTheMaxMessageSize_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>(), 10);

        //Act
        var act = () => uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(100), 0);

        //Assert
        act.ShouldThrow<TooLargeMessageException>().MaxMessageSize.ShouldBe(10);
    }

    // Upstream: MessageChunkingTest#sendSingleChunk (the chunk fields are set by the caller)
    [Fact]
    public void PlanChunking_GivenChunkFieldsOfTheCaller_ShouldReturnNullSoTheMessageIsSentAsItIs()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());
        var metadata = new MessageMetadata { SequenceId = 1, Uuid = "caller", ChunkId = 0, NumChunksFromMsg = 2, TotalChunkMsgSize = 100 };

        //Act
        var state = uut.PlanChunking(metadata, CreatePayload(5000), 0);

        //Assert
        state.ShouldBeNull();
        metadata.Uuid.ShouldBe("caller");
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenPayloadThatFitsInOneFrame_ShouldReturnOneChunkWithoutChunkFields()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());

        //Act
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(10), 0);

        //Assert
        state.ShouldNotBeNull();
        state.TotalChunks.ShouldBe(1);
        state.IsChunked.ShouldBeFalse();
        state.Metadata.HasUuid.ShouldBeFalse();
        state.Metadata.HasNumChunksFromMsg.ShouldBeFalse();
        state.Metadata.HasTotalChunkMsgSize.ShouldBeFalse();
        state.Metadata.ProducerName.ShouldBe(_producerName);
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenEmptyPayload_ShouldReturnOneChunk()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());

        //Act
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, ReadOnlySequence<byte>.Empty, 0);

        //Assert
        state.ShouldNotBeNull();
        state.TotalChunks.ShouldBe(1);
    }

    // Upstream: ProducerImpl#serializeAndSendMessage (a message is compressed before it is split, and is not compressed per chunk)
    [Fact]
    public void PlanChunking_GivenCompression_ShouldCompressOnceAndSplitTheCompressedPayload()
    {
        //Arrange
        var compressions = 0;
        var factory = new CompressorFactory(CompressionType.Zlib, () => new Compressor(data =>
        {
            compressions++;
            return data.Slice(0, data.Length / 2);
        }));
        var uut = CreateChannel(new List<Written>(), compressorFactory: factory);

        //Act
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(5000), 0);

        //Assert
        state.ShouldNotBeNull();
        compressions.ShouldBe(1);
        state.Payload.Length.ShouldBe(2500);
        state.Metadata.Compression.ShouldBe(CompressionType.Zlib);
        state.Metadata.UncompressedSize.ShouldBe(5000u);
        state.Metadata.TotalChunkMsgSize.ShouldBe(2500, "the total size of the chunks is the size of the compressed payload");
        state.TotalChunks.ShouldBe((2500 + state.ChunkSize - 1) / state.ChunkSize);
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenMetadataThatIsCompressedAlready_ShouldNotCompressAgain()
    {
        //Arrange
        var compressions = 0;
        var factory = new CompressorFactory(CompressionType.Zlib, () => new Compressor(data =>
        {
            compressions++;
            return data;
        }));
        var uut = CreateChannel(new List<Written>(), compressorFactory: factory);
        var metadata = new MessageMetadata { SequenceId = 1, Compression = CompressionType.Lz4, UncompressedSize = 5000 };

        //Act
        var state = uut.PlanChunking(metadata, CreatePayload(2000), 0);

        //Assert
        state.ShouldNotBeNull();
        compressions.ShouldBe(0);
        state.Metadata.Compression.ShouldBe(CompressionType.Lz4);
        state.Metadata.UncompressedSize.ShouldBe(5000u);
    }

    // Additional coverage
    [Fact]
    public void PlanChunking_GivenTransactionalMetadata_ShouldKeepTheTransactionIds()
    {
        //Arrange
        var uut = CreateChannel(new List<Written>());
        var metadata = new MessageMetadata { SequenceId = 1, TxnidMostBits = 3, TxnidLeastBits = 4 };

        //Act
        var state = uut.PlanChunking(metadata, CreatePayload(5000), 0);

        //Assert
        state.ShouldNotBeNull();
        state.Metadata.TxnidMostBits.ShouldBe(3UL);
        state.Metadata.TxnidLeastBits.ShouldBe(4UL);
    }

    // Upstream: ProducerImpl#serializeAndSendMessage
    [Fact]
    public async Task SendChunk_GivenEveryChunkOfAMessage_ShouldWriteTheSlicesOfThePayloadWithTheSameSequenceIdAndTheirOwnChunkId()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        var payload = CreatePayload(5000);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 100)!;

        //Act
        for (var chunkId = 0; chunkId < state.TotalChunks; ++chunkId)
        {
            await uut.SendChunk(state, chunkId, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        }

        //Assert
        written.Count.ShouldBe(50);
        written.Select(x => x.Command.SequenceId).Distinct().ShouldBe([7UL]);
        written.Select(x => x.Metadata.ChunkId).ShouldBe(Enumerable.Range(0, 50));
        written.Select(x => x.Metadata.Uuid).Distinct().ShouldBe([$"{_producerName}-7"]);
        written.ShouldAllBe(x => x.Metadata.NumChunksFromMsg == 50 && x.Metadata.TotalChunkMsgSize == 5000);
        written.ShouldAllBe(x => x.Command.IsChunk);
        written.ShouldAllBe(x => x.SkipSizeCheck);
        written.ShouldAllBe(x => x.Command.NumMessages == 1);
        written.SelectMany(x => x.Payload).ToArray().ShouldBe(payload.ToArray());
    }

    // Additional coverage
    [Fact]
    public async Task SendChunk_GivenLastChunk_ShouldWriteTheRestOfThePayload()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(250), 100)!;

        //Act
        await uut.SendChunk(state, 2, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        state.TotalChunks.ShouldBe(3);
        written.Single().Payload.Length.ShouldBe(50);
        written.Single().Metadata.ChunkId.ShouldBe(2);
    }

    // Additional coverage
    [Fact]
    public async Task SendChunk_GivenTransactionalMessage_ShouldSetTheTransactionIdsOnTheCommandOfEveryChunk()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1, TxnidMostBits = 3, TxnidLeastBits = 4 }, CreatePayload(500), 100)!;

        //Act
        for (var chunkId = 0; chunkId < state.TotalChunks; ++chunkId)
        {
            await uut.SendChunk(state, chunkId, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        }

        //Assert
        written.Count.ShouldBe(5);
        written.ShouldAllBe(x => x.Command.TxnidMostBits == 3 && x.Command.TxnidLeastBits == 4);
        written.ShouldAllBe(x => x.Metadata.TxnidMostBits == 3 && x.Metadata.TxnidLeastBits == 4);
    }

    // Additional coverage
    [Fact]
    public async Task SendChunk_GivenMessageThatIsNotSplit_ShouldWriteItWithoutChunkFieldsAndWithoutCheckingTheSize()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(10), 0)!;

        //Act
        await uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Command.IsChunk.ShouldBeFalse();
        actual.Metadata.HasChunkId.ShouldBeFalse();
        actual.Metadata.HasNumChunksFromMsg.ShouldBeFalse();
        actual.SkipSizeCheck.ShouldBeTrue();
        actual.Payload.Length.ShouldBe(10);
    }

    // Upstream: Commands#newSend (the command is pooled, so what a chunk sets on it must not leak into the next send)
    [Fact]
    public async Task Send_GivenPlainMessageAfterChunk_ShouldClearIsChunkAndCheckTheSize()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(500), 100)!;
        await uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 2 }, CreatePayload(10), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Count.ShouldBe(2);
        written[0].Command.IsChunk.ShouldBeTrue();
        written[1].Command.IsChunk.ShouldBeFalse();
        written[1].Command.HasIsChunk.ShouldBeFalse("the frame of a message that is not a chunk is the same as before");
        written[1].SkipSizeCheck.ShouldBeFalse();
        written[1].Metadata.HasChunkId.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task SendChunk_GivenChunkAfterPlainMessage_ShouldSetIsChunkAndSkipTheSizeCheck()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        await uut.Send(new MessageMetadata { SequenceId = 1 }, CreatePayload(10), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 2 }, CreatePayload(500), 100)!;

        //Act
        await uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written[0].Command.IsChunk.ShouldBeFalse();
        written[0].SkipSizeCheck.ShouldBeFalse();
        written[1].Command.IsChunk.ShouldBeTrue();
        written[1].SkipSizeCheck.ShouldBeTrue();
    }

    // Upstream: MessageChunkingTest#sendSingleChunk
    [Fact]
    public async Task Send_GivenChunkFieldsOfTheCaller_ShouldSetIsChunkAndSkipTheSizeCheck()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);
        var metadata = new MessageMetadata { SequenceId = 1, Uuid = "caller", ChunkId = 0, NumChunksFromMsg = 2, TotalChunkMsgSize = 100 };

        //Act
        await uut.Send(metadata, CreatePayload(10), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Single().Command.IsChunk.ShouldBeTrue();
        written.Single().SkipSizeCheck.ShouldBeTrue();
        written.Single().Metadata.Uuid.ShouldBe("caller");
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenPlainMessage_ShouldNotSetIsChunkOrSkipTheSizeCheck()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, CreatePayload(10), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Single().Command.HasIsChunk.ShouldBeFalse();
        written.Single().SkipSizeCheck.ShouldBeFalse();
    }

    // The size of the metadata that the chunk size is based on, which is the metadata without the chunk fields
    private static int GetMetadataSizeBeforeChunking(ChunkedSendState state)
    {
        var metadata = state.Metadata.Clone();
        metadata.ClearUuid();
        metadata.ClearNumChunksFromMsg();
        metadata.ClearTotalChunkMsgSize();
        metadata.ClearChunkId();
        return metadata.CalculateSize();
    }

    private static ReadOnlySequence<byte> CreatePayload(int length)
    {
        var payload = new byte[length];
        for (var i = 0; i < length; ++i)
        {
            payload[i] = (byte) (i % 251);
        }

        return new ReadOnlySequence<byte>(payload);
    }

    private static ProducerChannel CreateChannel(List<Written> written, int maxMessageSize = _maxMessageSize, ICompressorFactory? compressorFactory = null)
    {
        var connection = Substitute.For<IConnection>();
        connection.MaxMessageSize.Returns(maxMessageSize);
        connection.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            // The package, the command and the metadata are reused, so what is checked is copied
            var package = callInfo.Arg<SendPackage>();
            written.Add(new Written(package.Command!.Clone(), package.Metadata!.Clone(), package.Payload.ToArray(), package.SkipSizeCheck));
            return Task.CompletedTask;
        });

        return new ProducerChannel(1, _producerName, connection, compressorFactory, null, null, ProducerCryptoFailureAction.Fail);
    }

    private sealed record Written(CommandSend Command, MessageMetadata Metadata, byte[] Payload, bool SkipSizeCheck);
}
