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
}
