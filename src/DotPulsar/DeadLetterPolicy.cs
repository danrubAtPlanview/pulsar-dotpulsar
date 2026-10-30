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

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/DeadLetterPolicy.java, apache/pulsar-client-cpp@f71d7e9 include/pulsar/DeadLetterPolicy.h
/// <summary>
/// The dead letter policy for a consumer.
/// </summary>
/// <remarks>
/// When a message has been redelivered more than 'MaxRedeliverCount' times, it is sent to the dead letter topic and acknowledged automatically.
/// The redelivery count is only tracked by the broker for 'Shared' and 'KeyShared' subscriptions, so the policy only has an effect for these subscription types.
/// The dead letter producer does not encrypt, like the one of the Java client. A message that was end-to-end encrypted is sent to the dead letter topic decrypted and without its encryption metadata, so protect the dead letter topic as the topic of the messages.
/// </remarks>
public sealed class DeadLetterPolicy
{
    /// <summary>
    /// Initializes a new instance using the specified max redeliver count.
    /// </summary>
    public DeadLetterPolicy(uint maxRedeliverCount)
        => MaxRedeliverCount = maxRedeliverCount;

    /// <summary>
    /// The maximum number of times a message will be redelivered before being sent to the dead letter topic. Must be greater than 0.
    /// </summary>
    public uint MaxRedeliverCount { get; set; }

    /// <summary>
    /// The name of the dead letter topic. The default is '{TopicName}-{SubscriptionName}-DLQ'.
    /// </summary>
    /// <remarks>
    /// For a partitioned topic the default is computed per partition, e.g. 'persistent://public/default/my-topic-partition-0-my-subscription-DLQ'.
    /// </remarks>
    public string? DeadLetterTopic { get; set; }

    /// <summary>
    /// The name of the subscription to create on the dead letter topic when the dead letter producer is created. This is optional.
    /// </summary>
    /// <remarks>
    /// If this is set and the broker does not allow automatic subscription creation, the dead letter producer will fail to be created.
    /// </remarks>
    public string? InitialSubscriptionName { get; set; }
}
