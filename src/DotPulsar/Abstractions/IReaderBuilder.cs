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

namespace DotPulsar.Abstractions;

/// <summary>
/// A reader building abstraction.
/// </summary>
public interface IReaderBuilder<TMessage>
{
    /// <summary>
    /// Number of messages that will be prefetched. The default is 1000.
    /// </summary>
    IReaderBuilder<TMessage> MessagePrefetchCount(uint count);

    /// <summary>
    /// Whether to read from the compacted topic. The default is 'false'.
    /// </summary>
    IReaderBuilder<TMessage> ReadCompacted(bool readCompacted);

    /// <summary>
    /// Set the reader name. This is optional.
    /// </summary>
    IReaderBuilder<TMessage> ReaderName(string name);

    /// <summary>
    /// The initial reader position is set to the specified message id. This is required.
    /// </summary>
    IReaderBuilder<TMessage> StartMessageId(MessageId messageId);

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    IReaderBuilder<TMessage> StateChangedHandler(IHandleStateChanged<ReaderStateChanged> handler);

    /// <summary>
    /// Set the subscription name for this reader. This is optional.
    /// </summary>
    IReaderBuilder<TMessage> SubscriptionName(string name);

    /// <summary>
    /// Set the subscription role prefix for this reader. The default is 'Reader'. This is optional.
    /// </summary>
    IReaderBuilder<TMessage> SubscriptionRolePrefix(string subscriptionRolePrefix);

    /// <summary>
    /// Set the topic for this reader. This is required.
    /// </summary>
    IReaderBuilder<TMessage> Topic(string topic);

    /// <summary>
    /// Set the max number of chunked messages that are being assembled at the same time. When it is exceeded, the oldest is dropped. 0 means no limit. The default is 10.
    /// </summary>
    IReaderBuilder<TMessage> MaxPendingChunkedMessage(uint maxPendingChunkedMessage);

    /// <summary>
    /// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
    /// </summary>
    IReaderBuilder<TMessage> AutoAckOldestChunkedMessageOnQueueFull(bool autoAckOldestChunkedMessageOnQueueFull);

    /// <summary>
    /// Set the time after which an incomplete chunked message is dropped. TimeSpan.Zero disables expiry. The default is 1 minute.
    /// </summary>
    IReaderBuilder<TMessage> ExpireTimeOfIncompleteChunkedMessage(TimeSpan expireTimeOfIncompleteChunkedMessage);

    /// <summary>
    /// Create the reader.
    /// </summary>
    IReader<TMessage> Create();
}
