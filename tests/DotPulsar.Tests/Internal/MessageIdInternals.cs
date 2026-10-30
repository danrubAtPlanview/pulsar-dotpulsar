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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Internal.PulsarApi;
using System.Reflection;

// The batch size and the chunk ids of a MessageId are internal, and the library has no InternalsVisibleTo
public static class MessageIdInternals
{
    private static readonly PropertyInfo _batchSize = typeof(MessageId).GetProperty("BatchSize", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("MessageId.BatchSize was not found");

    private static readonly PropertyInfo _chunkMessageIds = typeof(MessageId).GetProperty("ChunkMessageIds", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("MessageId.ChunkMessageIds was not found");

    public static int GetBatchSize(this MessageId messageId)
        => (int) _batchSize.GetValue(messageId)!;

    public static MessageIdData[]? GetChunkMessageIds(this MessageId messageId)
        => (MessageIdData[]?) _chunkMessageIds.GetValue(messageId);

    // The id of a chunked message, like the consumer creates it: the last chunk gives the ledger, entry and partition, and the batch index is -1
    public static MessageId CreateChunked(string topic, params MessageIdData[] chunkMessageIds)
    {
        var first = chunkMessageIds[0];
        var last = chunkMessageIds[^1];
        var firstChunkMessageId = new MessageId(first.LedgerId, first.EntryId, first.Partition, -1, topic);

        return Create(last.LedgerId, last.EntryId, last.Partition, -1, topic, firstChunkMessageId, chunkMessageIds);
    }

    public static MessageId Create(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic, MessageId? firstChunkMessageId, MessageIdData[]? chunkMessageIds)
        => (MessageId) Activator.CreateInstance(
            typeof(MessageId),
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            [ledgerId, entryId, partition, batchIndex, topic, firstChunkMessageId, chunkMessageIds],
            null)!;

    public static MessageId Create(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic, int batchSize)
        => (MessageId) Activator.CreateInstance(
            typeof(MessageId),
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            [ledgerId, entryId, partition, batchIndex, topic, batchSize],
            null)!;
}
