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

namespace DotPulsar.Internal.Extensions;

using DotPulsar.Internal.PulsarApi;

public static class MessageIdDataExtensions
{
    public static MessageId ToMessageId(this MessageIdData messageIdData, string topic)
        => new(messageIdData.LedgerId, messageIdData.EntryId, messageIdData.Partition, messageIdData.BatchIndex, topic);

    // Upstream: ConsumerImpl#seekAsync(MessageId)
    // The message id of a seek is the entry that the broker redelivers from. For a chunked message that is its first chunk, because the broker redelivers entries.
    // For a message that is in a batch, the ack set has the bits of the messages before it cleared (acknowledged), so the broker redelivers the batch from the message.
    // Like upstream, this does not depend on the batch index acknowledgment of the consumer, but the broker only uses the ack set when it has 'acknowledgmentAtBatchIndexLevelEnabled'.
    // Without it the broker ignores the ack set, and redelivers the batch from its first message.
    public static MessageIdData ToSeekMessageIdData(this MessageId messageId)
    {
        var firstChunkMessageId = messageId.FirstChunkMessageId;

        if (firstChunkMessageId is not null)
            return firstChunkMessageId.ToMessageIdData();

        var messageIdData = messageId.ToMessageIdData();

        // The messages [0, batchIndex) are acknowledged, and the ones from the message to the end of the batch are not
        if (messageId.BatchIndex >= 0 && messageId.BatchSize > 0)
            AckSetWriter.WriteCumulative(messageIdData.AckSet, messageId.BatchSize, messageId.BatchIndex - 1);

        return messageIdData;
    }

    public static void MapFrom(this MessageIdData destination, MessageId source)
    {
        destination.LedgerId = source.LedgerId;
        destination.EntryId = source.EntryId;
        destination.Partition = source.Partition;
        destination.BatchIndex = source.BatchIndex;
        if (destination.AckSet.Count != 0)
            destination.AckSet.Clear();

        if (destination.HasBatchSize)
            destination.ClearBatchSize();

        if (destination.FirstChunkMessageId is not null)
            destination.FirstChunkMessageId = null;
    }
}
