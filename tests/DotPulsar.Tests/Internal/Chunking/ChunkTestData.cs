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

using DotPulsar.Internal.PulsarApi;
using System.Buffers;

public static class ChunkTestData
{
    public static MessageMetadata CreateMetadata(string producerName = "producer", ulong sequenceId = 7)
        => new()
        {
            ProducerName = producerName,
            SequenceId = sequenceId,
            PublishTime = 1_700_000_000_000
        };

    // The metadata of an entry that is a chunk of a chunked message, like the helper of the Java tests creates it
    public static MessageMetadata CreateChunkMetadata(string uuid, int chunkId, int numberOfChunks, int totalChunkMsgSize = 100, ulong publishTime = 1_000_000)
        => new()
        {
            ProducerName = "producer",
            SequenceId = 1,
            PublishTime = publishTime,
            Uuid = uuid,
            ChunkId = chunkId,
            NumChunksFromMsg = numberOfChunks,
            TotalChunkMsgSize = totalChunkMsgSize
        };

    public static MessageIdData CreateEntryId(ulong ledgerId, ulong entryId)
        => new() { LedgerId = ledgerId, EntryId = entryId };

    // The payload is a repeating pattern, so that a chunk that is in the wrong place is detected
    public static byte[] CreatePayload(int length)
    {
        var payload = new byte[length];
        for (var i = 0; i < length; ++i)
            payload[i] = (byte) ((i * 31) + (i >> 8));
        return payload;
    }

    public static ReadOnlySequence<byte> CreateSequence(params byte[][] segments)
    {
        if (segments.Length == 0)
            return ReadOnlySequence<byte>.Empty;

        var first = new Segment(segments[0], 0);
        var last = first;

        for (var i = 1; i < segments.Length; ++i)
            last = last.Append(segments[i]);

        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    // Splits the bytes in segments with the sizes, and the last segment gets what is left
    public static ReadOnlySequence<byte> CreateSegmentedSequence(byte[] bytes, params int[] segmentSizes)
    {
        var segments = new List<byte[]>();
        var offset = 0;

        foreach (var size in segmentSizes)
        {
            var length = Math.Min(size, bytes.Length - offset);
            segments.Add(bytes.AsSpan(offset, length).ToArray());
            offset += length;
        }

        if (offset < bytes.Length)
            segments.Add(bytes.AsSpan(offset).ToArray());

        return CreateSequence(segments.ToArray());
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory, RunningIndex + Memory.Length);
            Next = next;
            return next;
        }
    }
}
