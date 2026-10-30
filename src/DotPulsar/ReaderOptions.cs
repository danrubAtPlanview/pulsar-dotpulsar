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

/// <summary>
/// The reader building options.
/// </summary>
public sealed class ReaderOptions<TMessage>
{
    /// <summary>
    /// The default of whether to acknowledge the oldest pending chunked message when the queue of pending chunked messages is full.
    /// </summary>
    public static readonly bool DefaultAutoAckOldestChunkedMessageOnQueueFull = false;

    /// <summary>
    /// The default crypto failure action.
    /// </summary>
    public static readonly ConsumerCryptoFailureAction DefaultCryptoFailureAction = ConsumerCryptoFailureAction.Fail;

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
    /// The default of whether to read compacted.
    /// </summary>
    public static readonly bool DefaultReadCompacted = false;

    /// <summary>
    /// The default subscription role prefix.
    /// </summary>
    public static readonly string DefaultSubscriptionRolePrefix = "Reader";

    /// <summary>
    /// Initializes a new instance using the specified startMessageId and topic.
    /// </summary>
    public ReaderOptions(MessageId startMessageId, string topic, ISchema<TMessage> schema)
    {
        AutoAckOldestChunkedMessageOnQueueFull = DefaultAutoAckOldestChunkedMessageOnQueueFull;
        CryptoFailureAction = DefaultCryptoFailureAction;
        ExpireTimeOfIncompleteChunkedMessage = DefaultExpireTimeOfIncompleteChunkedMessage;
        MaxPendingChunkedMessage = DefaultMaxPendingChunkedMessage;
        MessagePrefetchCount = DefaultMessagePrefetchCount;
        ReadCompacted = DefaultReadCompacted;
        StartMessageId = startMessageId;
        SubscriptionRolePrefix = DefaultSubscriptionRolePrefix;
        Topic = topic;
        Schema = schema;
    }

    /// <summary>
    /// Number of messages that will be prefetched. The default is 1000.
    /// </summary>
    public uint MessagePrefetchCount { get; set; }

    /// <summary>
    /// Whether to read from the compacted topic. The default is 'false'.
    /// </summary>
    public bool ReadCompacted { get; set; }

    /// <summary>
    /// Set the reader name. This is optional.
    /// </summary>
    public string? ReaderName { get; set; }

    /// <summary>
    /// Set the schema. This is required.
    /// </summary>
    public ISchema<TMessage> Schema { get; set; }

    /// <summary>
    /// The initial reader position is set to the specified message id. This is required.
    /// </summary>
    public MessageId StartMessageId { get; set; }

    /// <summary>
    /// Register a state changed handler. This is optional.
    /// </summary>
    public IHandleStateChanged<ReaderStateChanged>? StateChangedHandler { get; set; }

    /// <summary>
    /// Set the subscription name for this reader. This is optional.
    /// </summary>
    public string? SubscriptionName { get; set; }

    /// <summary>
    /// Set the subscription role prefix for this reader. The default is 'Reader'. This is optional.
    /// </summary>
    public string SubscriptionRolePrefix { get; set; }

    /// <summary>
    /// Set the topic for this reader. This is required.
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// The max number of chunked messages that are being assembled at the same time. When it is exceeded, the oldest is dropped. 0 means no limit. The default is 10.
    /// </summary>
    public uint MaxPendingChunkedMessage { get; set; }

    /// <summary>
    /// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
    /// </summary>
    public bool AutoAckOldestChunkedMessageOnQueueFull { get; set; }

    /// <summary>
    /// The time after which an incomplete chunked message is dropped. TimeSpan.Zero disables expiry. The default is 1 minute.
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
    /// <remarks>
    /// A reader does not acknowledge messages and cannot have a message redelivered, so a message that 'Fail' does not deliver is skipped.
    /// </remarks>
    public ConsumerCryptoFailureAction CryptoFailureAction { get; set; }
}
