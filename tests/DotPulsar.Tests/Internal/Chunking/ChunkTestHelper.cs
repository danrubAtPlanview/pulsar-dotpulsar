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
using System.Reflection;
using ProtocolMetadata = DotPulsar.Internal.PulsarApi.MessageMetadata;

public static class ChunkTestHelper
{
    // The metadata of a message is internal, and the library has no InternalsVisibleTo
    private static readonly PropertyInfo _metadata = typeof(DotPulsar.MessageMetadata).GetProperty("Metadata", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("MessageMetadata.Metadata was not found");

    // Upstream: MessageChunkingTest#sendSingleChunk
    // Sends one entry that says that it is chunk 'chunkId' of the message 'uuid'. The value is "chunk-{uuid}-{chunkId}|", so the value of the assembled message is
    // the values of its chunks one after the other. The producer sends the chunk fields as they are, because they are set by the caller.
    public static ValueTask<MessageId> SendSingleChunk(IProducer<string> producer, string uuid, int chunkId, int totalChunks, CancellationToken cancellationToken)
    {
        var metadata = new DotPulsar.MessageMetadata();
        var protocolMetadata = (ProtocolMetadata) _metadata.GetValue(metadata)!;
        protocolMetadata.Uuid = uuid;
        protocolMetadata.ChunkId = chunkId;
        protocolMetadata.NumChunksFromMsg = totalChunks;
        protocolMetadata.TotalChunkMsgSize = 100;

        return producer.Send(metadata, $"chunk-{uuid}-{chunkId}|", cancellationToken);
    }

    // Upstream: MessageChunkingSharedTest#sendChunk, #sendNonChunk
    // Sends one entry of the message that is identified by the name of the producer and the sequence id, as chunk 'chunkId' of 'numChunks', or as a message that is not chunked
    // if there are no chunk fields. The uuid is "{producerName}-{sequenceId}", and the total size is the number of chunks, because the payload of a chunk is one byte.
    public static ValueTask<MessageId> SendChunk(IProducer<byte[]> producer, string producerName, ulong sequenceId, int? chunkId, int? numChunks, CancellationToken cancellationToken)
    {
        var metadata = new DotPulsar.MessageMetadata { SequenceId = sequenceId };

        if (chunkId is not null && numChunks is not null)
        {
            var protocolMetadata = (ProtocolMetadata) _metadata.GetValue(metadata)!;
            protocolMetadata.Uuid = $"{producerName}-{sequenceId}";
            protocolMetadata.ChunkId = chunkId.Value;
            protocolMetadata.NumChunksFromMsg = numChunks.Value;
            protocolMetadata.TotalChunkMsgSize = numChunks.Value;
        }

        return producer.Send(metadata, [(byte) 'a'], cancellationToken);
    }
}
