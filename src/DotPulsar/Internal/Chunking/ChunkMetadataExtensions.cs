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

using DotPulsar.Internal.PulsarApi;

// Upstream: apache/pulsar@f3846f4 pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java (newSend), apache/pulsar-client-cpp@f71d7e9 lib/ConsumerImpl.cc (messageReceived)
public static class ChunkMetadataExtensions
{
    // The value of CommandSend.is_chunk for a send with the metadata. Java only checks the total size, so it is true for every chunk of a chunked message.
    public static bool IsChunk(this MessageMetadata metadata)
        => metadata.TotalChunkMsgSize > 1;

    // Whether the entry of the metadata is a chunk of a chunked message. A batch can never be a chunk.
    public static bool IsChunkedMessage(this MessageMetadata metadata)
        => metadata.NumChunksFromMsg > 1 && !metadata.HasNumMessagesInBatch;
}
