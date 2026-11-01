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

namespace DotPulsar.Tests.Extensions;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Internal.Abstractions;

[Trait("Category", "Unit")]
public sealed class ConsumerExtensionsTests
{
    private static readonly MessageId _firstId = new(1, 2, 0, -1, "topic");
    private static readonly MessageId _secondId = new(1, 3, 0, -1, "topic");

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenMessageAndTransaction_ShouldAcknowledgeTheMessageIdAsPartOfTheTransaction()
    {
        //Arrange
        var consumer = Substitute.For<IConsumer, IExtendedConsumer>();
        var transaction = Substitute.For<ITransaction>();

        //Act
        await consumer.Acknowledge(CreateMessage(_firstId), transaction, Current.CancellationToken);

        //Assert
        await ((IExtendedConsumer) consumer).Received(1).Acknowledge(_firstId, transaction, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenMessagesAndTransaction_ShouldAcknowledgeTheMessageIdsAsPartOfTheTransaction()
    {
        //Arrange
        var consumer = Substitute.For<IConsumer, IExtendedConsumer>();
        var transaction = Substitute.For<ITransaction>();
        var messages = new[] { CreateMessage(_firstId), CreateMessage(_secondId) };

        //Act
        await consumer.Acknowledge(messages, transaction, Current.CancellationToken);

        //Assert
        await ((IExtendedConsumer) consumer).Received(1).Acknowledge(Arg.Is<IEnumerable<MessageId>>(messageIds => messageIds.SequenceEqual(new[] { _firstId, _secondId })), transaction, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenMessageAndTransaction_ShouldAcknowledgeTheMessageIdCumulativelyAsPartOfTheTransaction()
    {
        //Arrange
        var consumer = Substitute.For<IConsumer, IExtendedConsumer>();
        var transaction = Substitute.For<ITransaction>();

        //Act
        await consumer.AcknowledgeCumulative(CreateMessage(_firstId), transaction, Current.CancellationToken);

        //Assert
        await ((IExtendedConsumer) consumer).Received(1).AcknowledgeCumulative(_firstId, transaction, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenMessages_ShouldAcknowledgeTheMessageIds()
    {
        //Arrange
        var consumer = Substitute.For<IConsumer, IExtendedConsumer>();
        var messages = new[] { CreateMessage(_firstId), CreateMessage(_secondId) };

        //Act
        await consumer.Acknowledge(messages, Current.CancellationToken);

        //Assert
        await consumer.Received(1).Acknowledge(Arg.Is<IEnumerable<MessageId>>(messageIds => messageIds.SequenceEqual(new[] { _firstId, _secondId })), Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenMessage_ShouldAcknowledgeTheMessageIdWithoutTransaction()
    {
        //Arrange
        var consumer = Substitute.For<IConsumer, IExtendedConsumer>();

        //Act
        await consumer.Acknowledge(CreateMessage(_firstId), Current.CancellationToken);

        //Assert
        await consumer.Received(1).Acknowledge(_firstId, Current.CancellationToken);
        await consumer.DidNotReceive().Acknowledge(Arg.Any<MessageId>(), Arg.Any<ITransaction>(), Arg.Any<CancellationToken>());
    }

    private static IMessage CreateMessage(MessageId messageId)
    {
        var message = Substitute.For<IMessage>();
        message.MessageId.Returns(messageId);
        return message;
    }
}
