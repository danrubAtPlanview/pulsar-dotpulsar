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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;

// The public interfaces have no new members, because that breaks every implementation of them outside of DotPulsar. The new calls are extension methods with the same names, which call a capability interface that
// the implementations of DotPulsar have, or the builder that DotPulsar creates. An implementation that DotPulsar did not create does not have them, and the extension methods say so.
[Trait("Category", "Unit")]
public sealed class ExtendedApiTests
{
    private static readonly MessageId _messageId = new(1, 2, 0, -1);

    // Additional coverage
    [Fact]
    public void Acknowledge_GivenConsumerThatDotPulsarDidNotCreate_ShouldThrowNotSupportedExceptionThatNamesTheMember()
    {
        //Arrange
        var consumer = Substitute.For<IConsumer>();
        var transaction = Substitute.For<ITransaction>();

        //Act
        var acknowledge = Record.Exception(() => { _ = consumer.Acknowledge(_messageId, transaction, Current.CancellationToken); });
        var acknowledgeMany = Record.Exception(() => { _ = consumer.Acknowledge([_messageId], transaction, Current.CancellationToken); });
        var acknowledgeCumulative = Record.Exception(() => { _ = consumer.AcknowledgeCumulative(_messageId, transaction, Current.CancellationToken); });
        var negativeAcknowledge = Record.Exception(() => { _ = consumer.NegativeAcknowledge(_messageId, Current.CancellationToken); });
        var negativeAcknowledgeWithCount = Record.Exception(() => { _ = consumer.NegativeAcknowledge(_messageId, 2, Current.CancellationToken); });

        //Assert
        acknowledge.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'Acknowledge'");
        acknowledgeMany.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'Acknowledge'");
        acknowledgeCumulative.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'AcknowledgeCumulative'");
        negativeAcknowledge.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'NegativeAcknowledge'");
        negativeAcknowledgeWithCount.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'NegativeAcknowledge'");
    }

    // Additional coverage
    [Fact]
    public void Flush_GivenProducerThatDotPulsarDidNotCreate_ShouldThrowNotSupportedException()
    {
        //Arrange
        var producer = Substitute.For<IProducer>();

        //Act
        var actual = Record.Exception(() => { _ = producer.Flush(Current.CancellationToken); });

        //Assert
        actual.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'Flush'");
    }

    // Additional coverage
    [Fact]
    public void CreateTransaction_GivenClientThatDotPulsarDidNotCreate_ShouldThrowNotSupportedException()
    {
        //Arrange
        var client = Substitute.For<IPulsarClient>();

        //Act
        var actual = Record.Exception(() => { _ = client.CreateTransaction(new TransactionOptions(), Current.CancellationToken); });

        //Assert
        actual.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'CreateTransaction'");
    }

    // Additional coverage
    [Fact]
    public void Builders_GivenBuildersThatDotPulsarDidNotCreate_ShouldThrowNotSupportedException()
    {
        //Arrange
        var clientBuilder = Substitute.For<IPulsarClientBuilder>();
        var consumerBuilder = Substitute.For<IConsumerBuilder<string>>();
        var producerBuilder = Substitute.For<IProducerBuilder<string>>();
        var readerBuilder = Substitute.For<IReaderBuilder<string>>();
        var messageBuilder = Substitute.For<IMessageBuilder<string>>();

        //Act
        var enableTransaction = Record.Exception(() => clientBuilder.EnableTransaction(true));
        var ackTimeout = Record.Exception(() => consumerBuilder.AckTimeout(TimeSpan.FromSeconds(5)));
        var enableBatching = Record.Exception(() => producerBuilder.EnableBatching(true));
        var cryptoFailureAction = Record.Exception(() => readerBuilder.CryptoFailureAction(ConsumerCryptoFailureAction.Discard));
        var transaction = Record.Exception(() => messageBuilder.Transaction(Substitute.For<ITransaction>()));

        //Assert
        enableTransaction.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'EnableTransaction'");
        ackTimeout.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'AckTimeout'");
        enableBatching.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'EnableBatching'");
        cryptoFailureAction.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'CryptoFailureAction'");
        transaction.ShouldBeOfType<NotSupportedException>().Message.ShouldContain("'Transaction'");
    }

    // Additional coverage
    [Fact]
    public void NegativeAcknowledge_GivenNullConsumer_ShouldThrowArgumentNullException()
    {
        //Act
        var actual = Record.Exception(() => { _ = ConsumerExtensions.NegativeAcknowledge(null!, _messageId, Current.CancellationToken); });

        //Assert
        actual.ShouldBeOfType<ArgumentNullException>().ParamName.ShouldBe("consumer");
    }

    // Additional coverage
    [Fact]
    public void GetEncryptionContext_GivenMessageThatDotPulsarDidNotCreate_ShouldReturnNull()
    {
        //Arrange
        var message = Substitute.For<IMessage>();

        //Act
        var actual = message.GetEncryptionContext();

        //Assert
        actual.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void GetEncryptionContext_GivenMessageThatIsExtended_ShouldReadItsEncryptionContext()
    {
        //Arrange
        var message = Substitute.For<IMessage, IExtendedMessage>();

        //Act
        var actual = message.GetEncryptionContext();

        //Assert
        actual.ShouldBeNull();
        _ = ((IExtendedMessage) message).Received(1).EncryptionContext;
    }

    // Additional coverage
    [Fact]
    public void Implementations_GivenTheOnesThatDotPulsarCreates_ShouldHaveTheCapabilityInterfaces()
    {
        typeof(IExtendedConsumer).IsAssignableFrom(typeof(Consumer<byte[]>)).ShouldBeTrue();
        typeof(IExtendedConsumer).IsAssignableFrom(typeof(SubConsumer<byte[]>)).ShouldBeTrue();
        typeof(IExtendedProducer).IsAssignableFrom(typeof(Producer<byte[]>)).ShouldBeTrue();
        typeof(IExtendedPulsarClient).IsAssignableFrom(typeof(PulsarClient)).ShouldBeTrue();
        typeof(IExtendedMessage).IsAssignableFrom(typeof(Message<byte[]>)).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Builders_GivenTheOnesThatDotPulsarCreates_ShouldAcceptTheExtensionMethods()
    {
        //Arrange
        var clientBuilder = PulsarClient.Builder();

        //Act
        var actual = clientBuilder.EnableTransaction(true).OperationTimeout(TimeSpan.FromSeconds(5));

        //Assert
        actual.ShouldBeSameAs(clientBuilder);
    }
}
