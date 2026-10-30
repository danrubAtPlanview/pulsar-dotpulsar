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

namespace DotPulsar;

using DotPulsar.Abstractions;
using System.Text.RegularExpressions;

/// <summary>
/// The consumer building options.
/// </summary>
public sealed class ConsumerOptions<TMessage>
{
    /// <summary>
    /// The default of whether acknowledgments wait for a receipt from the broker.
    /// </summary>
    public static readonly bool DefaultAckReceiptEnabled = false;

    /// <summary>
    /// The default of whether to acknowledge individual messages inside a batch with batch index acknowledgment.
    /// </summary>
    public static readonly bool DefaultBatchIndexAckEnabled = false;

    /// <summary>
    /// The default crypto failure action.
    /// </summary>
    public static readonly ConsumerCryptoFailureAction DefaultCryptoFailureAction = ConsumerCryptoFailureAction.Fail;

    /// <summary>
    /// The default ack timeout.
    /// </summary>
    public static readonly TimeSpan DefaultAckTimeout = TimeSpan.Zero;

    /// <summary>
    /// The default ack timeout tick time.
    /// </summary>
    public static readonly TimeSpan DefaultAckTimeoutTickTime = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The default initial position.
    /// </summary>
    public static readonly SubscriptionInitialPosition DefaultInitialPosition = SubscriptionInitialPosition.Latest;

    /// <summary>
    /// The default of whether to acknowledge the oldest pending chunked message when the queue of pending chunked messages is full.
    /// </summary>
    public static readonly bool DefaultAutoAckOldestChunkedMessageOnQueueFull = false;

    /// <summary>
    /// The default expire time of incomplete chunked messages.
    /// </summary>
    public static readonly TimeSpan DefaultExpireTimeOfIncompleteChunkedMessage = TimeSpan.FromMinutes(1);

    /// <summary>
    /// The default max number of pending chunked messages.
    /// </summary>
    public static readonly uint DefaultMaxPendingChunkedMessage = 10;

    /// <summary>
    /// The default message prefetch count.
    /// </summary>
    public static readonly uint DefaultMessagePrefetchCount = 1000;

    /// <summary>
    /// The default delay before negatively acknowledged messages are redelivered.
    /// </summary>
    public static readonly TimeSpan DefaultNegativeAcknowledgeRedeliveryDelay = TimeSpan.FromMinutes(1);

    /// <summary>
    /// The default priority level.
    /// </summary>
    public static readonly int DefaultPriorityLevel = 0;

    /// <summary>
    /// The default of whether to read compacted.
    /// </summary>
    public static readonly bool DefaultReadCompacted = false;

    /// <summary>
    /// The default regex subscription mode.
    /// </summary>
    public static readonly RegexSubscriptionMode DefaultRegexSubscriptionMode = DotPulsar.RegexSubscriptionMode.Persistent;

    /// <summary>
    /// The default of whether to replicate the subscription's state.
    /// </summary>
    public static readonly bool DefaultReplicateSubscriptionState = false;

    /// <summary>
    /// The default subscription type.
    /// </summary>
    public static readonly SubscriptionType DefaultSubscriptionType = SubscriptionType.Exclusive;

    private readonly HashSet<string> _topics;

    private ConsumerOptions(string subscriptionName, ISchema<TMessage> schema, string topic, Regex? topicsPattern, IEnumerable<string> topics)
    {
        AckReceiptEnabled = DefaultAckReceiptEnabled;
        AckTimeout = DefaultAckTimeout;
        AckTimeoutTickTime = DefaultAckTimeoutTickTime;
        AutoAckOldestChunkedMessageOnQueueFull = DefaultAutoAckOldestChunkedMessageOnQueueFull;
        BatchIndexAckEnabled = DefaultBatchIndexAckEnabled;
        CryptoFailureAction = DefaultCryptoFailureAction;
        ExpireTimeOfIncompleteChunkedMessage = DefaultExpireTimeOfIncompleteChunkedMessage;
        MaxPendingChunkedMessage = DefaultMaxPendingChunkedMessage;
        InitialPosition = DefaultInitialPosition;
        NegativeAcknowledgeRedeliveryDelay = DefaultNegativeAcknowledgeRedeliveryDelay;
        PriorityLevel = DefaultPriorityLevel;
        MessagePrefetchCount = DefaultMessagePrefetchCount;
        ReadCompacted = DefaultReadCompacted;
        RegexSubscriptionMode = DefaultRegexSubscriptionMode;
        ReplicateSubscriptionState = DefaultReplicateSubscriptionState;
        SubscriptionType = DefaultSubscriptionType;
        SubscriptionProperties = [];
        SubscriptionName = subscriptionName;
        Schema = schema;
        Topic = topic;
        _topics = [];
        foreach (var t in topics)
        {
            _topics.Add(t);
        }
        TopicsPattern = topicsPattern;
    }

    /// <summary>
    /// Initializes a new instance using the specified subscription name, topic and schema.
    /// </summary>
    public ConsumerOptions(string subscriptionName, string topic, ISchema<TMessage> schema)
        : this(subscriptionName, schema, topic, null, Array.Empty<string>()) { }

    /// <summary>
    /// Initializes a new instance using the specified subscription name, topics and schema.
    /// </summary>
    public ConsumerOptions(string subscriptionName, IEnumerable<string> topics, ISchema<TMessage> schema)
        : this(subscriptionName, schema, string.Empty, null, topics) { }

    /// <summary>
    /// Initializes a new instance using the specified subscription name, topics pattern and schema.
    /// </summary>
    public ConsumerOptions(string subscriptionName, Regex topicsPattern, ISchema<TMessage> schema)
        : this(subscriptionName, schema, string.Empty, topicsPattern, Array.Empty<string>()) { }

    /// <summary>
    /// Set the consumer name. This is optional.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Set initial position for the subscription. The default is 'Latest'.
    /// </summary>
    public SubscriptionInitialPosition InitialPosition { get; set; }

    /// <summary>
    /// Number of messages that will be prefetched. The default is 1000.
    /// </summary>
    public uint MessagePrefetchCount { get; set; }

    /// <summary>
    /// Set the priority level for the shared subscription consumer. The default is 0.
    /// </summary>
    public int PriorityLevel { get; set; }

    /// <summary>
    /// Whether to read from the compacted topic. The default is 'false'.
    /// </summary>
    public bool ReadCompacted { get; set; }

    /// <summary>
    /// Determines which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. The default is 'Persistent'.
    /// </summary>
    public RegexSubscriptionMode RegexSubscriptionMode { get; set; }

    /// <summary>
    /// Whether to replicate the subscription's state across clusters (when using geo-replication). The default is 'false'.
    /// </summary>
    public bool ReplicateSubscriptionState { get; set; }

    /// <summary>
    /// Set the schema. This is required.
    /// </summary>
    public ISchema<TMessage> Schema { get; set; }

    /// <summary>
    /// Register a state changed handler. This is optional.
    /// </summary>
    public IHandleStateChanged<ConsumerStateChanged>? StateChangedHandler { get; set; }

    /// <summary>
    /// Set the subscription name for this consumer. This is required.
    /// </summary>
    public string SubscriptionName { get; set; }

    /// <summary>
    /// Add/Set the subscription's properties. This is optional.
    /// </summary>
    public Dictionary<string, string> SubscriptionProperties { get; set; }

    /// <summary>
    /// Set the subscription type for this consumer. The default is 'Exclusive'.
    /// </summary>
    public SubscriptionType SubscriptionType { get; set; }

    /// <summary>
    /// Set the topic for this consumer. This, or setting multiple topics or a topic pattern, is required.
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// Set the topics for this consumer. This, or setting a single topic or a topic pattern, is required.
    /// </summary>
    public IEnumerable<string> Topics
    {
        get => _topics;
        set
        {
            _topics.Clear();

            foreach (var topic in value)
            {
                _topics.Add(topic);
            }
        }
    }

    /// <summary>
    /// Specify a pattern for topics that this consumer subscribes to. This, or setting a single topic or multiple topics, is required.
    /// </summary>
    public Regex? TopicsPattern { get; set; }

    /// <summary>
    /// Allow out-of-order delivery on key_shared subscriptions. The default is 'false'.
    /// </summary>
    /// <remarks>
    /// https://pulsar.apache.org/docs/3.3.x/concepts-messaging/#preserving-order-of-processing
    /// </remarks>
    public bool AllowOutOfOrderDeliver { get; set; }

    /// <summary>
    /// Whether acknowledgments wait for a receipt from the broker, so that acknowledgment errors are reported to the caller. Transactional acknowledgments always wait for a receipt. The default is 'false'.
    /// </summary>
    public bool AckReceiptEnabled { get; set; }

    /// <summary>
    /// Whether partial acknowledgments of batched messages are sent to the broker using batch index acknowledgment (requires the broker setting 'acknowledgmentAtBatchIndexLevelEnabled'). Transactional acknowledgments always use batch index acknowledgment. The default is 'false'.
    /// </summary>
    public bool BatchIndexAckEnabled { get; set; }

    /// <summary>
    /// Set the timeout for unacknowledged messages. Must be zero (disabled) or at least 1 second. The default is 'TimeSpan.Zero' (disabled).
    /// </summary>
    /// <remarks>
    /// When enabled, a received message that is not acknowledged within the timeout is redelivered (possibly to another consumer on a shared subscription).
    /// On subscriptions that are not 'Shared' or 'KeyShared' an expiry redelivers all unacknowledged messages.
    /// The timeout is ignored for non-persistent topics.
    /// </remarks>
    public TimeSpan AckTimeout { get; set; }

    /// <summary>
    /// Set the granularity of the ack timeout redelivery. Must be at least 100 milliseconds. The default is 1 second.
    /// </summary>
    /// <remarks>
    /// A higher tick time reduces the memory overhead of tracking messages when the ack timeout is large (e.g. 1 hour).
    /// </remarks>
    public TimeSpan AckTimeoutTickTime { get; set; }

    /// <summary>
    /// Set the dead letter policy. This is optional.
    /// </summary>
    /// <remarks>
    /// Only has an effect for 'Shared' and 'KeyShared' subscriptions.
    /// </remarks>
    public DeadLetterPolicy? DeadLetterPolicy { get; set; }

    /// <summary>
    /// Set the delay before negatively acknowledged messages are redelivered. A delay below 100 milliseconds is raised to 100 milliseconds. The default is 1 minute.
    /// </summary>
    public TimeSpan NegativeAcknowledgeRedeliveryDelay { get; set; }

    /// <summary>
    /// The max number of chunked messages that are being assembled at the same time. When it is exceeded, the oldest is dropped. 0 means no limit. The default is 10.
    /// </summary>
    /// <remarks>
    /// A chunked message is assembled by the consumer that receives all of its chunks. With a 'Shared' subscription the broker can hand some chunks of a message to another consumer
    /// of the subscription, for example when the permits of a consumer run out in the middle of a message, which is likely when 'MessagePrefetchCount' is not much larger than the number
    /// of chunks of a message. Neither consumer can then complete that message, and the chunks that it holds are not delivered as a message.
    /// Use a 'MessagePrefetchCount' that is well above the largest number of chunks of a message, or an 'Exclusive' or 'Failover' subscription.
    /// </remarks>
    public uint MaxPendingChunkedMessage { get; set; }

    /// <summary>
    /// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
    /// </summary>
    public bool AutoAckOldestChunkedMessageOnQueueFull { get; set; }

    /// <summary>
    /// The time after which an incomplete chunked message is dropped and its chunks are acknowledged. TimeSpan.Zero disables expiry. The default is 1 minute.
    /// </summary>
    public TimeSpan ExpireTimeOfIncompleteChunkedMessage { get; set; }

    /// <summary>
    /// Set the crypto key reader used to read the private keys that decrypt the messages. This is optional.
    /// </summary>
    /// <remarks>
    /// Without a crypto key reader the messages that are encrypted cannot be decrypted, and the 'CryptoFailureAction' is taken for them.
    /// </remarks>
    public ICryptoKeyReader? CryptoKeyReader { get; set; }

    /// <summary>
    /// Set the action to take when a message cannot be decrypted. The default is 'Fail'.
    /// </summary>
    public ConsumerCryptoFailureAction CryptoFailureAction { get; set; }
}
