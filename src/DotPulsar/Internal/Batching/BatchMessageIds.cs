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

namespace DotPulsar.Internal.Batching;

using DotPulsar.Internal.PulsarApi;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerImpl.java (OpSendMsg#setMessageId)
public static class BatchMessageIds
{
    // The id of message 'batchIndex' of an entry that holds 'batchSize' messages, from the id in the send receipt.
    // One message is sent as it is, so it keeps the batch index of the receipt. The messages of a batch get their index in the batch.
    // The id does not carry the size of the batch, like the id that the Java producer creates.
    public static MessageId Create(MessageIdData receiptMessageId, int partition, string topic, int batchSize, int batchIndex)
        => new(receiptMessageId.LedgerId, receiptMessageId.EntryId, partition, batchSize > 1 ? batchIndex : receiptMessageId.BatchIndex, topic);
}
