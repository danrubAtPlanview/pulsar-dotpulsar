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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Internal;

[Trait("Category", "Unit")]
public sealed class RetryMessageUtilTests
{
    // Upstream: RetryMessageUtil#getDLQTopic
    [Theory]
    [InlineData("persistent://public/default/t", "sub", "persistent://public/default/t-sub-DLQ")]
    [InlineData("persistent://public/default/t-partition-0", "sub", "persistent://public/default/t-partition-0-sub-DLQ")]
    [InlineData("t", "s", "t-s-DLQ")]
    public void GetDeadLetterTopic_GivenTopicAndSubscription_ShouldAppendSubscriptionAndSuffix(string topic, string subscriptionName, string expected)
    {
        //Arrange & Act
        var actual = RetryMessageUtil.GetDeadLetterTopic(topic, subscriptionName);

        //Assert
        actual.ShouldBe(expected);
    }

    // Additional coverage
    [Fact]
    public void FormatMessageId_GivenNonBatched_ShouldReturnLedgerEntryPartition()
    {
        //Arrange
        var messageId = new MessageId(1, 2, -1, -1);

        //Act
        var actual = RetryMessageUtil.FormatMessageId(messageId);

        //Assert
        actual.ShouldBe("1:2:-1");
    }

    // Additional coverage
    [Fact]
    public void FormatMessageId_GivenBatched_ShouldAppendBatchIndex()
    {
        //Arrange
        var messageId = new MessageId(1, 2, 0, 3);

        //Act
        var actual = RetryMessageUtil.FormatMessageId(messageId);

        //Assert
        actual.ShouldBe("1:2:0:3");
    }

    // Additional coverage
    [Fact]
    public void FormatMessageId_GivenMessageIdWithTopic_ShouldNotIncludeTheTopic()
    {
        //Arrange
        var messageId = new MessageId(1, 2, 0, 3, "persistent://public/default/my-topic");

        //Act
        var actual = RetryMessageUtil.FormatMessageId(messageId);

        //Assert
        actual.ShouldBe("1:2:0:3");
    }

    // Upstream: RetryMessageUtil
    [Fact]
    public void PropertyNames_ShouldMatchTheUpstreamConstants()
    {
        //Arrange & Act & Assert
        RetryMessageUtil.RealTopic.ShouldBe("REAL_TOPIC");
        RetryMessageUtil.RealSubscription.ShouldBe("REAL_SUBSCRIPTION");
        RetryMessageUtil.OriginMessageId.ShouldBe("ORIGIN_MESSAGE_ID");
        RetryMessageUtil.OriginMessageIdDeprecated.ShouldBe("ORIGIN_MESSAGE_IDY_TIME");
    }
}
