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

namespace DotPulsar.Internal;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/util/RetryMessageUtil.java
public static class RetryMessageUtil
{
    public const string RealTopic = "REAL_TOPIC";
    public const string RealSubscription = "REAL_SUBSCRIPTION";
    public const string OriginMessageId = "ORIGIN_MESSAGE_ID";

    // The key of the deprecated property literally is 'ORIGIN_MESSAGE_IDY_TIME'. It still holds the same value as 'ORIGIN_MESSAGE_ID'.
    public const string OriginMessageIdDeprecated = "ORIGIN_MESSAGE_IDY_TIME";

    public const string DeadLetterTopicSuffix = "-DLQ";

    public static string GetDeadLetterTopic(string topic, string subscriptionName)
        => $"{topic}-{subscriptionName}{DeadLetterTopicSuffix}";

    // The message id as the upstream MessageId.toString(), which differs from MessageId.ToString() by not including the topic
    // Upstream: MessageIdImpl#toString, BatchMessageIdImpl#toString
    public static string FormatMessageId(MessageId messageId)
        => messageId.BatchIndex < 0
            ? $"{messageId.LedgerId}:{messageId.EntryId}:{messageId.Partition}"
            : $"{messageId.LedgerId}:{messageId.EntryId}:{messageId.Partition}:{messageId.BatchIndex}";
}
