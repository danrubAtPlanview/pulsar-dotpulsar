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

namespace DotPulsar.Extensions;

using DotPulsar.Abstractions;
using DotPulsar.Internal;

/// <summary>
/// Extensions for IConsumerBuilder.
/// </summary>
public static class ConsumerBuilderExtensions
{
    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IConsumerBuilder<TMessage> StateChangedHandler<TMessage>(
        this IConsumerBuilder<TMessage> builder,
        Action<ConsumerStateChanged> handler)
    {
        void forwarder(ConsumerStateChanged consumerStateChanged, CancellationToken _) => handler(consumerStateChanged);
        builder.StateChangedHandler(new ActionStateChangedHandler<ConsumerStateChanged>(forwarder, default));
        return builder;
    }

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IConsumerBuilder<TMessage> StateChangedHandler<TMessage>(
        this IConsumerBuilder<TMessage> builder,
        Action<ConsumerStateChanged, CancellationToken> handler,
        CancellationToken cancellationToken = default)
    {
        builder.StateChangedHandler(new ActionStateChangedHandler<ConsumerStateChanged>(handler, cancellationToken));
        return builder;
    }

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IConsumerBuilder<TMessage> StateChangedHandler<TMessage>(
        this IConsumerBuilder<TMessage> builder,
        Func<ConsumerStateChanged, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        builder.StateChangedHandler(new FuncStateChangedHandler<ConsumerStateChanged>(handler, cancellationToken));
        return builder;
    }

    /// <summary>
    /// Set the topics for this consumer. This, or setting a single topic, is required.
    /// </summary>
    public static IConsumerBuilder<TMessage> Topics<TMessage>(
        this IConsumerBuilder<TMessage> builder,
        params string[] topics)
    {
        builder.Topics(topics);
        return builder;
    }

    /// <summary>
    /// Use the DefaultCryptoKeyReader with a private key URI that is always used to decrypt.
    /// </summary>
    public static IConsumerBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(
        this IConsumerBuilder<TMessage> builder,
        string privateKey)
        => builder.CryptoKeyReader(new DefaultCryptoKeyReader(null, privateKey));

    /// <summary>
    /// Use the DefaultCryptoKeyReader with private key URIs by key name.
    /// </summary>
    public static IConsumerBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(
        this IConsumerBuilder<TMessage> builder,
        IReadOnlyDictionary<string, string> privateKeys)
        => builder.CryptoKeyReader(new DefaultCryptoKeyReader(null, null, null, privateKeys));

    /// <summary>
    /// Whether acknowledgments wait for a receipt from the broker. The default is 'false'.
    /// </summary>
    public static IConsumerBuilder<TMessage> AckReceiptEnabled<TMessage>(this IConsumerBuilder<TMessage> builder, bool ackReceiptEnabled)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(AckReceiptEnabled)).AckReceiptEnabled(ackReceiptEnabled);

    /// <summary>
    /// Whether partial acknowledgments of batched messages are sent to the broker using batch index acknowledgment. The default is 'false'.
    /// </summary>
    public static IConsumerBuilder<TMessage> BatchIndexAckEnabled<TMessage>(this IConsumerBuilder<TMessage> builder, bool batchIndexAckEnabled)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(BatchIndexAckEnabled)).BatchIndexAckEnabled(batchIndexAckEnabled);

    /// <summary>
    /// Set the timeout for unacknowledged messages. Must be zero (disabled) or at least 1 second. The default is 'TimeSpan.Zero' (disabled).
    /// </summary>
    public static IConsumerBuilder<TMessage> AckTimeout<TMessage>(this IConsumerBuilder<TMessage> builder, TimeSpan ackTimeout)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(AckTimeout)).AckTimeout(ackTimeout);

    /// <summary>
    /// Set the granularity of the ack timeout redelivery. Must be at least 100 milliseconds. The default is 1 second.
    /// </summary>
    public static IConsumerBuilder<TMessage> AckTimeoutTickTime<TMessage>(this IConsumerBuilder<TMessage> builder, TimeSpan tickTime)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(AckTimeoutTickTime)).AckTimeoutTickTime(tickTime);

    /// <summary>
    /// Set the backoff that delays the redelivery of the messages that are not acknowledged within the ack timeout. This is optional, and it has no effect without an ack timeout.
    /// </summary>
    public static IConsumerBuilder<TMessage> AckTimeoutRedeliveryBackoff<TMessage>(this IConsumerBuilder<TMessage> builder, IRedeliveryBackoff redeliveryBackoff)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(AckTimeoutRedeliveryBackoff)).AckTimeoutRedeliveryBackoff(redeliveryBackoff);

    /// <summary>
    /// Set the dead letter policy. This is optional.
    /// </summary>
    public static IConsumerBuilder<TMessage> DeadLetterPolicy<TMessage>(this IConsumerBuilder<TMessage> builder, DeadLetterPolicy deadLetterPolicy)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(DeadLetterPolicy)).DeadLetterPolicy(deadLetterPolicy);

    /// <summary>
    /// Set the delay before negatively acknowledged messages are redelivered. A delay below 100 milliseconds is raised to 100 milliseconds. The default is 1 minute.
    /// </summary>
    public static IConsumerBuilder<TMessage> NegativeAcknowledgeRedeliveryDelay<TMessage>(this IConsumerBuilder<TMessage> builder, TimeSpan delay)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(NegativeAcknowledgeRedeliveryDelay)).NegativeAcknowledgeRedeliveryDelay(delay);

    /// <summary>
    /// Set the backoff that decides how long to wait before a negatively acknowledged message is redelivered, instead of the redelivery delay. This is optional.
    /// </summary>
    public static IConsumerBuilder<TMessage> NegativeAcknowledgeRedeliveryBackoff<TMessage>(this IConsumerBuilder<TMessage> builder, IRedeliveryBackoff redeliveryBackoff)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(NegativeAcknowledgeRedeliveryBackoff)).NegativeAcknowledgeRedeliveryBackoff(redeliveryBackoff);

    /// <summary>
    /// Set the max number of chunked messages that are being assembled at the same time. When it is exceeded, the oldest is dropped. 0 means no limit. The default is 10.
    /// </summary>
    public static IConsumerBuilder<TMessage> MaxPendingChunkedMessage<TMessage>(this IConsumerBuilder<TMessage> builder, uint maxPendingChunkedMessage)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(MaxPendingChunkedMessage)).MaxPendingChunkedMessage(maxPendingChunkedMessage);

    /// <summary>
    /// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
    /// </summary>
    public static IConsumerBuilder<TMessage> AutoAckOldestChunkedMessageOnQueueFull<TMessage>(this IConsumerBuilder<TMessage> builder, bool autoAckOldestChunkedMessageOnQueueFull)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(AutoAckOldestChunkedMessageOnQueueFull)).AutoAckOldestChunkedMessageOnQueueFull(autoAckOldestChunkedMessageOnQueueFull);

    /// <summary>
    /// Whether 'IConsumer.RedeliverUnacknowledgedMessages' with an empty list of message ids does nothing. The default is 'true'. With 'false' an empty list redelivers all unacknowledged messages of a consumer of one topic, as it did before DotPulsar had this option.
    /// </summary>
    public static IConsumerBuilder<TMessage> RedeliverNothingForNoMessageIds<TMessage>(this IConsumerBuilder<TMessage> builder, bool redeliverNothingForNoMessageIds)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(RedeliverNothingForNoMessageIds)).RedeliverNothingForNoMessageIds(redeliverNothingForNoMessageIds);

    /// <summary>
    /// Whether a seek to the MessageId of a message in a batch makes the broker redeliver the batch from that message, so the messages of the batch before it are not redelivered. This needs the broker setting 'acknowledgmentAtBatchIndexLevelEnabled'. The default is 'true'. With 'false' the seek ignores the position in the batch, and the broker redelivers the whole batch, as it did before DotPulsar had this option.
    /// </summary>
    public static IConsumerBuilder<TMessage> SeekToBatchIndex<TMessage>(this IConsumerBuilder<TMessage> builder, bool seekToBatchIndex)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(SeekToBatchIndex)).SeekToBatchIndex(seekToBatchIndex);

    /// <summary>
    /// Whether the chunks of a chunked message are delivered as one message. The default is 'true'. With 'false' every chunk is delivered as a message, as it was before DotPulsar had this option, and 'MaxPendingChunkedMessage', 'AutoAckOldestChunkedMessageOnQueueFull' and 'ExpireTimeOfIncompleteChunkedMessage' have no effect.
    /// </summary>
    public static IConsumerBuilder<TMessage> AssembleChunkedMessages<TMessage>(this IConsumerBuilder<TMessage> builder, bool assembleChunkedMessages)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(AssembleChunkedMessages)).AssembleChunkedMessages(assembleChunkedMessages);

    /// <summary>
    /// Set the time after which an incomplete chunked message is dropped and its chunks are acknowledged. TimeSpan.Zero disables expiry. The default is 1 minute.
    /// </summary>
    public static IConsumerBuilder<TMessage> ExpireTimeOfIncompleteChunkedMessage<TMessage>(this IConsumerBuilder<TMessage> builder, TimeSpan expireTimeOfIncompleteChunkedMessage)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(ExpireTimeOfIncompleteChunkedMessage)).ExpireTimeOfIncompleteChunkedMessage(expireTimeOfIncompleteChunkedMessage);

    /// <summary>
    /// Set the crypto key reader used to read the private keys that decrypt the messages. This is optional.
    /// </summary>
    public static IConsumerBuilder<TMessage> CryptoKeyReader<TMessage>(this IConsumerBuilder<TMessage> builder, ICryptoKeyReader cryptoKeyReader)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(CryptoKeyReader)).CryptoKeyReader(cryptoKeyReader);

    /// <summary>
    /// Set the action to take when a message cannot be decrypted. The default is 'Fail'.
    /// </summary>
    public static IConsumerBuilder<TMessage> CryptoFailureAction<TMessage>(this IConsumerBuilder<TMessage> builder, ConsumerCryptoFailureAction cryptoFailureAction)
        => ExtendedApi.Require<ConsumerBuilder<TMessage>>(builder, nameof(builder), nameof(CryptoFailureAction)).CryptoFailureAction(cryptoFailureAction);
}
