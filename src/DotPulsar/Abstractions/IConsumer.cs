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
/// A consumer abstraction.
/// </summary>
public interface IConsumer : IGetLastMessageIds, ISeek, IStateHolder<ConsumerState>, IAsyncDisposable
{
    /// <summary>
    /// Acknowledge the consumption of a single message using the MessageId.
    /// </summary>
    ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge the consumption of a single message using the MessageId as part of a transaction.
    /// </summary>
    ValueTask Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge the consumption of multiple messages using the MessageIds.
    /// </summary>
    ValueTask Acknowledge(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge the consumption of multiple messages using the MessageIds as part of a transaction.
    /// </summary>
    ValueTask Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge the consumption of all the messages in the topic up to and including the provided MessageId.
    /// </summary>
    ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge the consumption of all the messages in the topic up to and including the provided MessageId as part of a transaction.
    /// </summary>
    /// <remarks>
    /// Cumulative acknowledgment is not allowed for subscriptions of type 'Shared' and 'KeyShared'.
    /// </remarks>
    ValueTask AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>
    /// The consumer's service url.
    /// </summary>
    Uri ServiceUrl { get; }

    /// <summary>
    /// The consumer's subscription name.
    /// </summary>
    string SubscriptionName { get; }

    /// <summary>
    /// The consumer's subscription type.
    /// </summary>
    SubscriptionType SubscriptionType { get; }

    /// <summary>
    /// The consumer's topic.
    /// </summary>
    string Topic { get; }

    /// <summary>
    /// Unsubscribe the consumer.
    /// </summary>
    ValueTask Unsubscribe(CancellationToken cancellationToken = default);

    /// <summary>
    /// Redeliver the pending messages that were pushed to this consumer that are not yet acknowledged.
    /// </summary>
    /// <remarks>
    /// Nothing is redelivered if there are no message ids. On subscriptions that are not 'Shared' or 'KeyShared' all unacknowledged messages are redelivered.
    /// </remarks>
    ValueTask RedeliverUnacknowledgedMessages(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Redeliver all pending messages that were pushed to this consumer that are not yet acknowledged.
    /// </summary>
    ValueTask RedeliverUnacknowledgedMessages(CancellationToken cancellationToken = default);

    /// <summary>
    /// Negatively acknowledge a single message using the MessageId, so it will be redelivered after the configured negative acknowledge redelivery delay.
    /// </summary>
    ValueTask NegativeAcknowledge(MessageId messageId, CancellationToken cancellationToken = default);
}
