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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageContainerBase.java
// A container is not thread safe. It is used by one dispatcher: add messages, check the limits, create the batches, clear.
public interface IBatchMessageContainer<TItem> where TItem : class, IBatchItem
{
    int NumMessagesInBatch { get; }

    // The sum of the (uncompressed) payload sizes of the messages in the container
    long CurrentBatchSize { get; }

    bool IsEmpty { get; }

    // The sequence id of the last message that was added. 0 if the container is empty.
    ulong HighestSequenceId { get; }

    // The messages in the order they are sent. For a key based container this is the order after CreateBatches.
    // The list is valid until Clear() is called.
    IReadOnlyList<TItem> Items { get; }

    bool HaveEnoughSpace(TItem item, int maxMessageSize);
    bool HasSameTxn(TItem item);
    bool HasSameSchema(TItem item);

    // HaveEnoughSpace && HasSameSchema && HasSameTxn
    bool CanAddToCurrentBatch(TItem item, int maxMessageSize);

    // Returns true if the container is full after adding the message
    bool Add(TItem item, int maxMessageSize);

    bool IsBatchFull(int maxMessageSize);

    // The time left before the first message has waited for the maximum publish delay. Zero if the delay has elapsed.
    // Timeout.InfiniteTimeSpan if the container is empty.
    TimeSpan GetRemainingPublishDelay();

    // The container is not empty and is full or the maximum publish delay has elapsed
    bool ShouldFlush(int maxMessageSize);

    // Appends the groups of Items that are sent as one entry each, in the order they are sent. Does not clear the container.
    void CreateBatches(List<PreparedBatchGroup> destination, int maxMessageSize);

    void Clear();
}
