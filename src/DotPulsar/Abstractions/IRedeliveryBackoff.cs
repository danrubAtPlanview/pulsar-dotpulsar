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

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/RedeliveryBackoff.java
/// <summary>
/// A policy that decides how long to wait before a message is redelivered, given the number of times it has been redelivered.
/// </summary>
/// <remarks>
/// An implementation is called by the consumer, while the consumer is tracking its unacknowledged messages. It must be thread-safe, fast, and it should not throw.
/// If it throws, the message is redelivered without a delay.
/// A consumer that loses its connection is not delayed by the policy, because the broker redelivers all its unacknowledged messages at once when the consumer reconnects.
/// </remarks>
public interface IRedeliveryBackoff
{
    /// <summary>
    /// The delay before a message is redelivered.
    /// </summary>
    /// <param name="redeliveryCount">The number of times the message has been redelivered, which is 0 for a message that has only been delivered once.</param>
    TimeSpan Next(uint redeliveryCount);
}
