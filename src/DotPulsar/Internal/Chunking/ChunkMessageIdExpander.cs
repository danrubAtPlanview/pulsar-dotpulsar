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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/PersistentAcknowledgmentsGroupingTracker.java (flushAsync, newImmediateAckAndFlush), apache/pulsar-client-cpp@f71d7e9 lib/ConsumerImpl.cc (ChunkMessageIdImpl)
public static class ChunkMessageIdExpander
{
    // Whether the ids of the chunks are the ids of a chunked message. A message that was not chunked has no chunk ids, or only one.
    public static bool IsChunked(MessageIdData[]? chunkMessageIds)
        => chunkMessageIds is { Length: > 1 };

    // Adds the id of every chunk to the destination, if the ids are the ids of a chunked message, and returns true. Otherwise nothing is added and false is returned,
    // and the id of the message itself must be used. This is what an individual acknowledgement, a negative acknowledgement and a redelivery must do.
    // A cumulative acknowledgement must not use it, because acknowledging the last chunk (which is the id of the message) also acknowledges all the chunks before it.
    public static bool TryExpand(MessageIdData[]? chunkMessageIds, ICollection<MessageIdData> destination)
    {
        if (!IsChunked(chunkMessageIds))
            return false;

        foreach (var chunkMessageId in chunkMessageIds!)
            destination.Add(chunkMessageId);

        return true;
    }
}
