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

using System.Text.RegularExpressions;

/// <summary>
/// A consumer building abstraction.
/// </summary>
public interface IConsumerBuilder<TMessage>
{
    /// <summary>
    /// Set the consumer name. This is optional.
    /// </summary>
    IConsumerBuilder<TMessage> ConsumerName(string name);

    /// <summary>
    /// Set initial position for the subscription. The default is 'Latest'.
    /// </summary>
    IConsumerBuilder<TMessage> InitialPosition(SubscriptionInitialPosition initialPosition);

    /// <summary>
    /// Number of messages that will be prefetched. The default is 1000.
    /// </summary>
    IConsumerBuilder<TMessage> MessagePrefetchCount(uint count);

    /// <summary>
    /// Set the priority level for the shared subscription consumer. The default is 0.
    /// </summary>
    IConsumerBuilder<TMessage> PriorityLevel(int priorityLevel);

    /// <summary>
    /// Whether to read from the compacted topic. The default is 'false'.
    /// </summary>
    IConsumerBuilder<TMessage> ReadCompacted(bool readCompacted);

    /// <summary>
    /// Determines which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. The default is 'Persistent'.
    /// </summary>
    IConsumerBuilder<TMessage> RegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode);

    /// <summary>
    /// Whether to replicate the subscription's state across clusters (when using geo-replication). The default is 'false'.
    /// </summary>
    IConsumerBuilder<TMessage> ReplicateSubscriptionState(bool replicateSubscriptionState);

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    IConsumerBuilder<TMessage> StateChangedHandler(IHandleStateChanged<ConsumerStateChanged> handler);

    /// <summary>
    /// Set the subscription name for this consumer. This is required.
    /// </summary>
    IConsumerBuilder<TMessage> SubscriptionName(string name);

    /// <summary>
    /// Add/Set a property key/value on the subscription. This is optional.
    /// </summary>
    IConsumerBuilder<TMessage> SubscriptionProperty(string key, string value);

    /// <summary>
    /// Set the subscription type for this consumer. The default is 'Exclusive'.
    /// </summary>
    IConsumerBuilder<TMessage> SubscriptionType(SubscriptionType type);

    /// <summary>
    /// Set the topic for this consumer. This, or setting multiple topics or a topic pattern, is required.
    /// </summary>
    IConsumerBuilder<TMessage> Topic(string topic);

    /// <summary>
    /// Set the topics for this consumer. This, or setting a single topic or a topic pattern, is required.
    /// </summary>
    IConsumerBuilder<TMessage> Topics(IEnumerable<string> topics);

    /// <summary>
    /// Specify a pattern for topics that this consumer subscribes to. This, or setting a single topic or multiple topics, is required.
    /// </summary>
    IConsumerBuilder<TMessage> TopicsPattern(Regex topicsPattern);

    /// <summary>
    /// Whether to allow out-of-order delivery on key_shared subscriptions. The default is 'false'.
    /// </summary>
    IConsumerBuilder<TMessage> AllowOutOfOrderDeliver(bool allowOutOfOrderDeliver);

    /// <summary>
    /// Whether acknowledgments wait for a receipt from the broker. The default is 'false'.
    /// </summary>
    IConsumerBuilder<TMessage> AckReceiptEnabled(bool ackReceiptEnabled);

    /// <summary>
    /// Whether partial acknowledgments of batched messages are sent to the broker using batch index acknowledgment. The default is 'false'.
    /// </summary>
    IConsumerBuilder<TMessage> BatchIndexAckEnabled(bool batchIndexAckEnabled);

    /// <summary>
    /// Set the timeout for unacknowledged messages. Must be zero (disabled) or at least 1 second. The default is 'TimeSpan.Zero' (disabled).
    /// </summary>
    IConsumerBuilder<TMessage> AckTimeout(TimeSpan ackTimeout);

    /// <summary>
    /// Set the granularity of the ack timeout redelivery. Must be at least 100 milliseconds. The default is 1 second.
    /// </summary>
    IConsumerBuilder<TMessage> AckTimeoutTickTime(TimeSpan tickTime);

    /// <summary>
    /// Set the backoff that delays the redelivery of the messages that are not acknowledged within the ack timeout. This is optional, and it has no effect without an ack timeout.
    /// </summary>
    IConsumerBuilder<TMessage> AckTimeoutRedeliveryBackoff(IRedeliveryBackoff redeliveryBackoff);

    /// <summary>
    /// Set the dead letter policy. This is optional.
    /// </summary>
    IConsumerBuilder<TMessage> DeadLetterPolicy(DeadLetterPolicy deadLetterPolicy);

    /// <summary>
    /// Set the delay before negatively acknowledged messages are redelivered. A delay below 100 milliseconds is raised to 100 milliseconds. The default is 1 minute.
    /// </summary>
    IConsumerBuilder<TMessage> NegativeAcknowledgeRedeliveryDelay(TimeSpan delay);

    /// <summary>
    /// Set the backoff that decides how long to wait before a negatively acknowledged message is redelivered, instead of the redelivery delay. This is optional.
    /// </summary>
    IConsumerBuilder<TMessage> NegativeAcknowledgeRedeliveryBackoff(IRedeliveryBackoff redeliveryBackoff);

    /// <summary>
    /// Set the max number of chunked messages that are being assembled at the same time. When it is exceeded, the oldest is dropped. 0 means no limit. The default is 10.
    /// </summary>
    IConsumerBuilder<TMessage> MaxPendingChunkedMessage(uint maxPendingChunkedMessage);

    /// <summary>
    /// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
    /// </summary>
    IConsumerBuilder<TMessage> AutoAckOldestChunkedMessageOnQueueFull(bool autoAckOldestChunkedMessageOnQueueFull);

    /// <summary>
    /// Set the time after which an incomplete chunked message is dropped and its chunks are acknowledged. TimeSpan.Zero disables expiry. The default is 1 minute.
    /// </summary>
    IConsumerBuilder<TMessage> ExpireTimeOfIncompleteChunkedMessage(TimeSpan expireTimeOfIncompleteChunkedMessage);

    /// <summary>
    /// Set the crypto key reader used to read the private keys that decrypt the messages. This is optional.
    /// </summary>
    IConsumerBuilder<TMessage> CryptoKeyReader(ICryptoKeyReader cryptoKeyReader);

    /// <summary>
    /// Set the action to take when a message cannot be decrypted. The default is 'Fail'.
    /// </summary>
    IConsumerBuilder<TMessage> CryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction);

    /// <summary>
    /// Create the consumer.
    /// </summary>
    IConsumer<TMessage> Create();
}
