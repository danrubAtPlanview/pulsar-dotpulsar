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

using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;

// Splits messages like a producer does, serializes the metadata of the chunks, and assembles the chunks like a consumer does
[Trait("Category", "Unit")]
public sealed class ChunkingRoundTripTests
{
    // Additional coverage
    [Theory]
    [InlineData(1000, 1000, 100)]
    [InlineData(1001, 1000, 100)]
    [InlineData(999, 1000, 100)]
    [InlineData(50, 1000, 1)]
    [InlineData(123457, 100000, 1000)]
    [InlineData(6000 * 1000, 5 * 1024 * 1024, 0)]
    public void Process_GivenChunksOfAPlannedMessage_ShouldAssembleTheOriginalPayload(int length, int maxMessageSize, int chunkMaxMessageSize)
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(length);
        var state = ChunkedSendState.Plan(ChunkTestData.CreateMetadata("producer", 3), new ReadOnlySequence<byte>(payload), maxMessageSize, chunkMaxMessageSize, 0);
        state.IsChunked.ShouldBeTrue();
        using var uut = CreateUut();
        var chunks = CreateWireChunks(state);
        AssembledChunkedMessage? actual = null;

        //Act
        for (var chunkId = 0; chunkId < chunks.Count; ++chunkId)
        {
            actual = Process(uut, chunks[chunkId], (ulong) chunkId + 100);
            if (chunkId < chunks.Count - 1)
                actual.ShouldBeNull();
        }

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.ToArray().ShouldBe(payload);
        actual.Value.ChunkMessageIds.Length.ShouldBe(state.TotalChunks);
        actual.Value.FirstChunkMessageId.EntryId.ShouldBe(100UL);
        actual.Value.LastChunkMessageId.EntryId.ShouldBe(100UL + (ulong) state.TotalChunks - 1);
        uut.PendingCount.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Process_GivenChunksOfAPlannedMessage_ShouldSeeEveryChunkAsAChunkAndNotAsABatch()
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(500);
        var state = ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), 1000, 100, 0);

        //Act
        var chunks = CreateWireChunks(state);

        //Assert
        chunks.Count.ShouldBe(5);
        chunks.ShouldAllBe(chunk => chunk.Metadata.IsChunkedMessage() && chunk.Metadata.IsChunk());
        chunks.Select(chunk => chunk.Metadata.ChunkId).ShouldBe([0, 1, 2, 3, 4]);
        chunks.Select(chunk => chunk.Metadata.Uuid).Distinct().ShouldBe(["producer-7"]);
        chunks.Select(chunk => chunk.Metadata.SequenceId).Distinct().ShouldBe([7UL]);
        chunks.Select(chunk => chunk.Metadata.NumChunksFromMsg).Distinct().ShouldBe([5]);
        chunks.Select(chunk => chunk.Metadata.TotalChunkMsgSize).Distinct().ShouldBe([500]);
    }

    // Upstream: MessageChunkingSharedTest#testInterleavedChunks
    [Fact]
    public void Process_GivenChunksOfTwoPlannedMessagesFromDifferentProducers_ShouldAssembleBothWhenTheirChunksAreInterleaved()
    {
        //Arrange
        var payloadA = ChunkTestData.CreatePayload(1050);
        var payloadB = ChunkTestData.CreatePayload(777);
        var chunksA = CreateWireChunks(ChunkedSendState.Plan(ChunkTestData.CreateMetadata("A", 1), new ReadOnlySequence<byte>(payloadA), 1000, 100, 0));
        var chunksB = CreateWireChunks(ChunkedSendState.Plan(ChunkTestData.CreateMetadata("B", 1), new ReadOnlySequence<byte>(payloadB), 1000, 100, 0));
        using var uut = CreateUut();
        var random = new Random(4711);
        var nextA = 0;
        var nextB = 0;
        ulong entry = 0;
        var completed = new List<(string Uuid, byte[] Payload)>();

        //Act
        while (nextA < chunksA.Count || nextB < chunksB.Count)
        {
            var useA = nextB == chunksB.Count || (nextA < chunksA.Count && random.Next(2) == 0);
            var chunk = useA ? chunksA[nextA++] : chunksB[nextB++];
            var message = Process(uut, chunk, ++entry);
            if (message is not null)
                completed.Add((chunk.Metadata.Uuid, message.Value.Payload.ToArray()));
        }

        //Assert
        completed.Count.ShouldBe(2);
        completed.Single(message => message.Uuid == "A-1").Payload.ShouldBe(payloadA);
        completed.Single(message => message.Uuid == "B-1").Payload.ShouldBe(payloadB);
        uut.PendingCount.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Process_GivenChunksOfTheSamePlannedMessageSentTwice_ShouldAssembleOnceWithTheIdsOfTheLastFirstChunk()
    {
        //Arrange
        var payload = ChunkTestData.CreatePayload(450);
        var chunks = CreateWireChunks(ChunkedSendState.Plan(ChunkTestData.CreateMetadata(), new ReadOnlySequence<byte>(payload), 1000, 100, 0));
        using var uut = CreateUut();
        var acks = new List<MessageIdData>();
        ulong entry = 0;
        Process(uut, chunks[0], ++entry, acks);
        Process(uut, chunks[1], ++entry, acks);

        //Act
        AssembledChunkedMessage? actual = null;
        foreach (var chunk in chunks)
            actual = Process(uut, chunk, ++entry, acks);

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.ToArray().ShouldBe(payload);
        acks.Select(id => id.EntryId).ShouldBe([1UL, 2UL]);
        actual.Value.ChunkMessageIds.Select(id => id.EntryId).ShouldBe([3UL, 4UL, 5UL, 6UL, 7UL]);
    }

    private static ChunkedMessageAssembler CreateUut()
        => new(10, false, TimeSpan.FromMinutes(1), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), null);

    // What the consumer sees on the wire: the metadata is serialized and parsed again
    private static List<WireChunk> CreateWireChunks(ChunkedSendState state)
    {
        var chunks = new List<WireChunk>();

        for (var chunkId = 0; chunkId < state.TotalChunks; ++chunkId)
        {
            state.Metadata.ChunkId = chunkId;
            var metadata = MessageMetadata.Parser.ParseFrom(state.Metadata.ToByteArray());
            chunks.Add(new WireChunk(metadata, state.GetChunk(chunkId).ToArray()));
        }

        return chunks;
    }

    private static AssembledChunkedMessage? Process(ChunkedMessageAssembler uut, WireChunk chunk, ulong entry, List<MessageIdData>? acks = null)
        => uut.Process(ChunkTestData.CreateEntryId(1, entry), chunk.Metadata, new ReadOnlySequence<byte>(chunk.Payload), acks ?? [], []);

    private sealed class WireChunk
    {
        public WireChunk(MessageMetadata metadata, byte[] payload)
        {
            Metadata = metadata;
            Payload = payload;
        }

        public MessageMetadata Metadata { get; }
        public byte[] Payload { get; }
    }
}
