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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/DefaultBatcherBuilder.java, pulsar-client/src/main/java/org/apache/pulsar/client/impl/KeyBasedBatcherBuilder.java
public static class BatchMessageContainerFactory
{
    // The 'clock' (monotonic, only used for the maximum publish delay) is a test seam.
    // 'maxNumMessagesInBatch' = 0 means no limit on the number of messages. 'maxBytesInBatch' = 0 means that the maximum message size is used.
    public static IBatchMessageContainer<TItem> Create<TItem>(
        BatcherType batcherType,
        uint maxNumMessagesInBatch,
        uint maxBytesInBatch,
        TimeSpan maxPublishDelay,
        PreparedBatchPool? batchPool = null,
        Func<TimeSpan>? clock = null) where TItem : class, IBatchItem
        => batcherType switch
        {
            BatcherType.Default => new BatchMessageContainer<TItem>(maxNumMessagesInBatch, maxBytesInBatch, maxPublishDelay, batchPool, clock),
            BatcherType.KeyBased => new KeyBasedBatchMessageContainer<TItem>(maxNumMessagesInBatch, maxBytesInBatch, maxPublishDelay, batchPool, clock),
            _ => throw new ArgumentOutOfRangeException(nameof(batcherType), batcherType, "The batcher type is not supported")
        };
}
