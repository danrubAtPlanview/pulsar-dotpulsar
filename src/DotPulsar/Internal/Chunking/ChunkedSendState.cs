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

namespace DotPulsar.Internal.Chunking;

using DotPulsar.Exceptions;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Globalization;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerImpl.java (sendAsync, serializeAndSendMessage, ChunkedMessageCtx), apache/pulsar-client-cpp@f71d7e9 lib/ProducerImpl.cc (sendAsyncWithStatsUpdate, getNumOfChunks)
public sealed class ChunkedSendState
{
    // Not thread safe. The dispatcher and the receipt handler of the sub producer are serialized by the send queue.
    public ChunkedSendState(MessageMetadata metadata, ReadOnlySequence<byte> payload, int totalChunks, int chunkSize)
    {
        Metadata = metadata;
        Payload = payload;
        TotalChunks = totalChunks;
        ChunkSize = chunkSize;
        ReceiptedChunks = 0;
        ChunkMessageIds = totalChunks > 1 ? new MessageIdData[totalChunks] : [];
    }

    // The metadata of the message. It is private to the state. The chunk fields are set, and the chunk id is changed by the sender before each chunk is written.
    public MessageMetadata Metadata { get; }

    // The full payload, after compression and before encryption
    public ReadOnlySequence<byte> Payload { get; }

    // The number of chunks. A value of 1 means that the message is not chunked, and that the chunk fields of the metadata are not set.
    public int TotalChunks { get; }

    // The number of payload bytes in every chunk, except possibly the last
    public int ChunkSize { get; }

    public bool IsChunked => TotalChunks > 1;

    // The number of chunks that the broker has acknowledged. A chunk that is not acknowledged, is sent again after a reconnect.
    public int ReceiptedChunks { get; private set; }

    // The id of the entry of every chunk that is acknowledged, in chunk order. It is empty if the message is not chunked.
    public MessageIdData[] ChunkMessageIds { get; }

    public MessageIdData FirstChunkMessageId => ChunkMessageIds[0];

    public MessageIdData LastChunkMessageId => ChunkMessageIds[TotalChunks - 1];

    // Upstream: ProducerImpl#sendAsync (L642-L670), ProducerImpl#serializeAndSendMessage (L833-L849)
    // Splits a message into chunks. The metadata must be a copy that belongs to the caller, with the sequence id, the producer name and the compression set,
    // and the payload must be the compressed payload. The state takes over the metadata, and sets the chunk fields on it if there is more than one chunk.
    // The size of the metadata (before the chunk fields are set) and the encryptionOverhead (the bytes that encryption adds to every chunk) are
    // subtracted from the max message size, like Java does. The chunkMaxMessageSize is ignored if it is zero or less.
    public static ChunkedSendState Plan(MessageMetadata metadata, ReadOnlySequence<byte> payload, int maxMessageSize, int chunkMaxMessageSize, int encryptionOverhead)
    {
        var length = payload.Length;
        if (length > int.MaxValue)
            throw new TooLargeMessageException(int.MaxValue, maxMessageSize);

        var metadataSize = metadata.CalculateSize();
        var chunkSize = maxMessageSize - metadataSize - encryptionOverhead;
        if (chunkSize <= 0)
            throw new TooLargeMessageException(metadataSize + encryptionOverhead, maxMessageSize);

        if (chunkMaxMessageSize > 0)
            chunkSize = Math.Min(chunkMaxMessageSize, chunkSize);

        var totalChunks = (int) ((Math.Max(1, length) + chunkSize - 1) / chunkSize);

        if (totalChunks > 1)
        {
            metadata.Uuid = string.Concat(metadata.ProducerName, "-", metadata.SequenceId.ToString(CultureInfo.InvariantCulture));
            metadata.NumChunksFromMsg = totalChunks;
            metadata.TotalChunkMsgSize = (int) length;
            metadata.ChunkId = 0;
        }

        return new ChunkedSendState(metadata, payload, totalChunks, chunkSize);
    }

    // A view of the payload of the chunk. Nothing is copied.
    public ReadOnlySequence<byte> GetChunk(int chunkId)
    {
        if ((uint) chunkId >= (uint) TotalChunks)
            throw new ArgumentOutOfRangeException(nameof(chunkId));

        if (TotalChunks == 1)
            return Payload;

        var start = (long) chunkId * ChunkSize;
        return Payload.Slice(start, Math.Min(ChunkSize, Payload.Length - start));
    }

    // Upstream: ProducerImpl#ackReceived (L1590-L1606)
    // Stores the id of the entry of the next chunk that the broker has acknowledged. Returns true when the last chunk has been acknowledged.
    public bool RecordReceipt(MessageIdData messageId)
    {
        ChunkMessageIds[ReceiptedChunks++] = messageId;
        return ReceiptedChunks == TotalChunks;
    }
}
