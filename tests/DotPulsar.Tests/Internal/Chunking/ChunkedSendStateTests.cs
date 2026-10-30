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
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class ChunkedSendStateTests
{
    private const int MaxMessageSize = 1000;

    // Upstream: ProducerImpl#sendAsync (L669), ProducerImpl.cc#getNumOfChunks
    [Theory]
    [InlineData(0, 0, 1)]
    [InlineData(0, 1, 1)]
    [InlineData(1, -1, 1)]
    [InlineData(1, 0, 1)]
    [InlineData(1, 1, 2)]
    [InlineData(2, -1, 2)]
    [InlineData(2, 0, 2)]
    [InlineData(2, 1, 3)]
    [InlineData(3, 0, 3)]
    [InlineData(10, 0, 10)]
    [InlineData(10, 1, 11)]
    public void Plan_GivenPayloadLengthAroundMultiplesOfChunkSize_ShouldSplitIntoCeilDivChunks(int multiple, int extra, int expectedChunks)
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var chunkSize = MaxMessageSize - metadata.CalculateSize();
        var payload = ChunkTestData.CreatePayload((chunkSize * multiple) + extra);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 0);

        //Assert
        uut.TotalChunks.ShouldBe(expectedChunks);
        uut.ChunkSize.ShouldBe(chunkSize);
        uut.IsChunked.ShouldBe(expectedChunks > 1);
    }

    [Fact]
    public void Plan_GivenEmptyPayload_ShouldReturnSingleChunkWithEmptyPayload()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();

        //Act
        var uut = ChunkedSendState.Plan(metadata, ReadOnlySequence<byte>.Empty, MaxMessageSize, 0, 0);

        //Assert
        uut.TotalChunks.ShouldBe(1);
        uut.GetChunk(0).Length.ShouldBe(0);
    }

    [Fact]
    public void Plan_GivenSingleChunk_ShouldNotSetChunkFields()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(10);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 0);

        //Assert
        uut.Metadata.HasUuid.ShouldBeFalse();
        uut.Metadata.HasNumChunksFromMsg.ShouldBeFalse();
        uut.Metadata.HasTotalChunkMsgSize.ShouldBeFalse();
        uut.Metadata.HasChunkId.ShouldBeFalse();
        uut.Metadata.IsChunk().ShouldBeFalse();
        uut.ChunkMessageIds.ShouldBeEmpty();
    }

    // Upstream: ProducerImpl#sendAsync (L720), ProducerImpl#serializeAndSendMessage (L843-L848), ProducerImpl.cc#sendAsyncWithStatsUpdate (L611-L618)
    [Fact]
    public void Plan_GivenMultipleChunks_ShouldSetChunkFields()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata("test-producer", 42);
        var chunkSize = MaxMessageSize - metadata.CalculateSize();
        var payload = ChunkTestData.CreatePayload((chunkSize * 3) + 1);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 0);

        //Assert
        uut.TotalChunks.ShouldBe(4);
        uut.Metadata.Uuid.ShouldBe("test-producer-42");
        uut.Metadata.NumChunksFromMsg.ShouldBe(4);
        uut.Metadata.TotalChunkMsgSize.ShouldBe(payload.Length);
        uut.Metadata.ChunkId.ShouldBe(0);
        uut.Metadata.IsChunk().ShouldBeTrue();
        uut.ChunkMessageIds.Length.ShouldBe(4);
    }

    [Fact]
    public void Plan_GivenMultipleChunks_ShouldKeepTheSequenceIdAndTheOtherFieldsOfTheMetadata()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata("test-producer", 42);
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 12345;
        metadata.PartitionKey = "key";
        var payload = ChunkTestData.CreatePayload(MaxMessageSize * 2);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 0);

        //Assert
        uut.Metadata.SequenceId.ShouldBe(42UL);
        uut.Metadata.ProducerName.ShouldBe("test-producer");
        uut.Metadata.PublishTime.ShouldBe(1_700_000_000_000UL);
        uut.Metadata.Compression.ShouldBe(CompressionType.Lz4);
        uut.Metadata.UncompressedSize.ShouldBe(12345u);
        uut.Metadata.PartitionKey.ShouldBe("key");
    }

    [Fact]
    public void Plan_GivenLargestSequenceId_ShouldWriteTheSequenceIdInTheUuidAsAnUnsignedNumber()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata("p", ulong.MaxValue);
        var payload = ChunkTestData.CreatePayload(MaxMessageSize * 2);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 0);

        //Assert
        uut.Metadata.Uuid.ShouldBe("p-18446744073709551615");
    }

    [Fact]
    public void Plan_GivenOneByteChunks_ShouldCreateOneChunkPerByte()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(10);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 1, 0);

        //Assert
        uut.TotalChunks.ShouldBe(10);
        uut.ChunkSize.ShouldBe(1);
        for (var chunkId = 0; chunkId < 10; ++chunkId)
            uut.GetChunk(chunkId).ToArray().ShouldBe([payload[chunkId]]);
    }

    // Upstream: ProducerImpl#setChunkMaxMessageSize (L354-L358), ProducerImpl#sendAsync (L668)
    [Theory]
    [InlineData(100, 100)]
    [InlineData(1, 1)]
    [InlineData(100000, -1)]
    [InlineData(0, -1)]
    [InlineData(-1, -1)]
    public void Plan_GivenChunkMaxMessageSize_ShouldUseTheSmallerOfItAndTheMaxMessageSizeMinusTheMetadata(int chunkMaxMessageSize, int expectedChunkSize)
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var limit = MaxMessageSize - metadata.CalculateSize();
        var payload = ChunkTestData.CreatePayload(MaxMessageSize * 3);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, chunkMaxMessageSize, 0);

        //Assert
        uut.ChunkSize.ShouldBe(expectedChunkSize == -1 ? limit : expectedChunkSize);
    }

    [Fact]
    public void Plan_GivenEncryptionOverhead_ShouldSubtractItFromTheChunkSize()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var limit = MaxMessageSize - metadata.CalculateSize();
        var payload = ChunkTestData.CreatePayload(MaxMessageSize * 3);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 16);

        //Assert
        uut.ChunkSize.ShouldBe(limit - 16);
    }

    [Fact]
    public void Plan_GivenEncryptionOverheadAndSmallChunkMaxMessageSize_ShouldNotSubtractTheOverheadFromChunkMaxMessageSize()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(MaxMessageSize);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 100, 16);

        //Assert
        uut.ChunkSize.ShouldBe(100);
    }

    // Upstream: ProducerImpl#sendAsync (L657-L667)
    [Fact]
    public void Plan_GivenMaxMessageSizeSmallerThanTheMetadata_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var metadataSize = metadata.CalculateSize();
        var payload = ChunkTestData.CreatePayload(100);

        //Act
        var exception = Should.Throw<TooLargeMessageException>(() => ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), metadataSize - 1, 0, 0));

        //Assert
        exception.MessageSize.ShouldBe(metadataSize);
        exception.MaxMessageSize.ShouldBe(metadataSize - 1);
    }

    [Fact]
    public void Plan_GivenMaxMessageSizeEqualToTheMetadata_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(100);
        var maxMessageSize = metadata.CalculateSize();

        //Act
        var act = () => ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), maxMessageSize, 0, 0);

        //Assert
        Should.Throw<TooLargeMessageException>(act);
    }

    [Fact]
    public void Plan_GivenMetadataAndEncryptionOverheadFillingTheMaxMessageSize_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(100);
        var maxMessageSize = metadata.CalculateSize() + 16;

        //Act
        var exception = Should.Throw<TooLargeMessageException>(() => ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), maxMessageSize, 0, 16));

        //Assert
        exception.MessageSize.ShouldBe(maxMessageSize);
    }

    [Fact]
    public void Plan_GivenMaxMessageSizeOneLargerThanTheMetadata_ShouldCreateOneByteChunks()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(5);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), metadata.CalculateSize() + 1, 0, 0);

        //Assert
        uut.ChunkSize.ShouldBe(1);
        uut.TotalChunks.ShouldBe(5);
    }

    [Fact]
    public void Plan_GivenPayloadLargerThanInt32MaxValue_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        var megabyte = new byte[1024 * 1024];
        var segments = new byte[2049][];
        Array.Fill(segments, megabyte);
        var payload = ChunkTestData.CreateSequence(segments);

        //Act
        var act = () => ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), payload, MaxMessageSize, 0, 0);

        //Assert
        Should.Throw<TooLargeMessageException>(act);
    }

    [Fact]
    public void Plan_GivenSixMegabytesAndTheDefaultMaxMessageSize_ShouldCreateTwoChunks()
    {
        //Arrange
        const int defaultMaxMessageSize = 5 * 1024 * 1024;
        var metadata = ChunkTestData.CreateMetadata();
        var metadataSize = metadata.CalculateSize();
        var payload = ChunkTestData.CreatePayload(6000 * 1000);

        //Act
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), defaultMaxMessageSize, 0, 0);

        //Assert
        uut.TotalChunks.ShouldBe(2);
        uut.GetChunk(0).Length.ShouldBe(defaultMaxMessageSize - metadataSize);
        uut.GetChunk(1).Length.ShouldBe(payload.Length - (defaultMaxMessageSize - metadataSize));
    }

    // Upstream: ProducerImpl#serializeAndSendMessage (L836-L837)
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(51)]
    [InlineData(100)]
    [InlineData(1234)]
    public void GetChunk_GivenAnyChunkOfPayload_ShouldReturnConsecutiveSlicesThatJoinToThePayload(int length)
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(length);
        var uut = ChunkedSendState.Plan(metadata, new ReadOnlySequence<byte>(payload), MaxMessageSize, 50, 0);

        //Act
        var joined = new List<byte>();
        for (var chunkId = 0; chunkId < uut.TotalChunks; ++chunkId)
        {
            var chunk = uut.GetChunk(chunkId);
            if (chunkId < uut.TotalChunks - 1)
                chunk.Length.ShouldBe(50);
            joined.AddRange(chunk.ToArray());
        }

        //Assert
        joined.ShouldBe(payload);
    }

    [Fact]
    public void GetChunk_GivenPayloadInSeveralSegments_ShouldReturnTheRightBytes()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = ChunkTestData.CreatePayload(1000);
        var sequence = ChunkTestData.CreateSegmentedSequence(payload, 7, 100, 1, 333, 59);
        var uut = ChunkedSendState.Plan(metadata, sequence, MaxMessageSize, 64, 0);

        //Act
        var joined = new List<byte>();
        for (var chunkId = 0; chunkId < uut.TotalChunks; ++chunkId)
            joined.AddRange(uut.GetChunk(chunkId).ToArray());

        //Assert
        uut.TotalChunks.ShouldBe(16);
        joined.ShouldBe(payload);
    }

    [Fact]
    public void GetChunk_GivenSingleChunk_ShouldReturnTheWholePayload()
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(10);
        var uut = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), MaxMessageSize, 0, 0);

        //Act
        var actual = uut.GetChunk(0);

        //Assert
        actual.ToArray().ShouldBe(payload);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(4)]
    [InlineData(5)]
    public void GetChunk_GivenChunkIdOutsideTheChunks_ShouldThrowArgumentOutOfRangeException(int chunkId)
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(40);
        var uut = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), MaxMessageSize, 10, 0);

        //Act
        var act = () => { _ = uut.GetChunk(chunkId); };

        //Assert
        uut.TotalChunks.ShouldBe(4);
        Should.Throw<ArgumentOutOfRangeException>(act);
    }

    [Fact]
    public void GetChunk_GivenPayloadSplitInManyChunks_ShouldNotAllocate()
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(1024 * 1024);
        var uut = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), MaxMessageSize, 100, 0);
        var length = 0L;
        for (var chunkId = 0; chunkId < uut.TotalChunks; ++chunkId)
            length += uut.GetChunk(chunkId).Length;

        //Act
        var allocated = AllocationMeasurement.SmallestBytesOfCurrentThread(() =>
        {
            length = 0;
            for (var chunkId = 0; chunkId < uut.TotalChunks; ++chunkId)
                length += uut.GetChunk(chunkId).Length;
        });

        //Assert
        length.ShouldBe(payload.Length);
        allocated.ShouldBe(0);
    }

    [Fact]
    public void Plan_GivenTenMegabytesInOneMegabyteChunks_ShouldNotCopyThePayload()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();
        var payload = new ReadOnlySequence<byte>(new byte[10 * 1024 * 1024]);
        var maxMessageSize = 1024 * 1024 + metadata.CalculateSize();
        _ = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), payload, maxMessageSize, 0, 0);

        //Act
        var before = GC.GetAllocatedBytesForCurrentThread();
        var uut = ChunkedSendState.Plan(metadata, payload, maxMessageSize, 0, 0);
        var length = 0L;
        for (var chunkId = 0; chunkId < uut.TotalChunks; ++chunkId)
            length += uut.GetChunk(chunkId).Length;
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        //Assert
        length.ShouldBe(payload.Length);
        uut.TotalChunks.ShouldBe(10);
        allocated.ShouldBeLessThan(1024);
    }

    // Upstream: ProducerImpl#ackReceived (L1590-L1606)
    [Fact]
    public void RecordReceipt_GivenReceiptsForAllChunks_ShouldCompleteOnTheLastChunkAndKeepTheOrder()
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(40);
        var uut = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), MaxMessageSize, 10, 0);
        var ids = new[] { ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11), ChunkTestData.CreateEntryId(1, 12), ChunkTestData.CreateEntryId(2, 0) };

        //Act
        var completed = ids.Select(uut.RecordReceipt).ToArray();

        //Assert
        completed.ShouldBe([false, false, false, true]);
        uut.ReceiptedChunks.ShouldBe(4);
        uut.ChunkMessageIds.ShouldBe(ids);
        uut.FirstChunkMessageId.ShouldBeSameAs(ids[0]);
        uut.LastChunkMessageId.ShouldBeSameAs(ids[3]);
    }

    [Fact]
    public void RecordReceipt_GivenPartialReceipts_ShouldTellHowManyChunksToSendAfterAReconnect()
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(40);
        var uut = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), MaxMessageSize, 10, 0);

        //Act
        uut.RecordReceipt(ChunkTestData.CreateEntryId(1, 10));
        uut.RecordReceipt(ChunkTestData.CreateEntryId(1, 11));

        //Assert
        uut.ReceiptedChunks.ShouldBe(2);
        uut.Metadata.Uuid.ShouldBe("producer-7");
        uut.GetChunk(uut.ReceiptedChunks).ToArray().ShouldBe(payload.AsSpan(20, 10).ToArray());
    }
}
