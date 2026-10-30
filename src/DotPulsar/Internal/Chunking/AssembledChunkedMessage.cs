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
using System.Buffers;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java (messageReceived, chunkedMessageIds), apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ChunkMessageIdImpl.java
public readonly struct AssembledChunkedMessage
{
    // The chunkMessageIds are owned by the message from now on. All of them are set and in chunk order.
    public AssembledChunkedMessage(ReadOnlySequence<byte> payload, MessageIdData[] chunkMessageIds)
    {
        Payload = payload;
        ChunkMessageIds = chunkMessageIds;
    }

    // The concatenated payload of the chunks. It is still compressed, because a chunked message is decompressed after it has been assembled.
    public ReadOnlySequence<byte> Payload { get; }

    // The id of the entry of every chunk, in chunk order
    public MessageIdData[] ChunkMessageIds { get; }

    public MessageIdData FirstChunkMessageId => ChunkMessageIds[0];

    public MessageIdData LastChunkMessageId => ChunkMessageIds[ChunkMessageIds.Length - 1];
}
