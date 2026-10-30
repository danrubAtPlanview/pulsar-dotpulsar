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
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using ProducerAccessMode = DotPulsar.Internal.PulsarApi.ProducerAccessMode;

[Trait("Category", "Unit")]
public sealed class ProducerChannelFactoryTests
{
    // Additional coverage
    [Fact]
    public async Task Create_GivenTransactionsEnabled_ShouldSetTxnEnabled()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, true);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        var command = connection.CommandProducers.Single();
        command.HasTxnEnabled.ShouldBeTrue();
        command.TxnEnabled.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenTransactionsDisabled_ShouldNotSetTxnEnabled()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        connection.CommandProducers.Single().HasTxnEnabled.ShouldBeFalse();
    }

    // Upstream: ProducerImpl#connectionOpened (CommandProducer encrypted)
    [Fact]
    public async Task Create_GivenMessageCrypto_ShouldSetEncryptedOnCommandProducer()
    {
        //Arrange
        using var messageCrypto = CreateMessageCrypto();
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false, messageCrypto);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        var command = connection.CommandProducers.Single();
        command.HasEncrypted.ShouldBeTrue();
        command.Encrypted.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenNoMessageCrypto_ShouldNotSetEncrypted()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        connection.CommandProducers.Single().HasEncrypted.ShouldBeFalse("the bytes that a client without encryption sends do not change");
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenMessageCryptoAndTheProducerIsCreatedAgain_ShouldSetEncryptedAgain()
    {
        //Arrange
        using var messageCrypto = CreateMessageCrypto();
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false, messageCrypto);

        //Act
        await uut.Create(Current.CancellationToken);
        await uut.Create(Current.CancellationToken);

        //Assert
        connection.CommandProducers.Count.ShouldBe(2);
        connection.CommandProducers.ShouldAllBe(command => command.Encrypted);
    }

    // The message crypto belongs to the sub producer, and the channel that the factory creates is replaced when the sub producer reconnects, so every channel gets the same one
    // Additional coverage
    [Theory]
    [InlineData(ProducerCryptoFailureAction.Fail)]
    [InlineData(ProducerCryptoFailureAction.Send)]
    public async Task Create_GivenMessageCrypto_ShouldCreateChannelsThatEncryptWithIt(ProducerCryptoFailureAction cryptoFailureAction)
    {
        //Arrange
        using var messageCrypto = CreateMessageCrypto();
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false, messageCrypto, cryptoFailureAction);
        var payload = new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 });

        //Act
        var first = await uut.Create(Current.CancellationToken);
        var second = await uut.Create(Current.CancellationToken);
        await first.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await second.Send(new MessageMetadata { SequenceId = 2 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        connection.Sent.Count.ShouldBe(2);
        connection.Sent.ShouldAllBe(sent => sent.Payload.Length == 3 + MessageCrypto.TagLength && sent.Metadata.EncryptionKeys.Count == 1);
        await first.DisposeAsync();
        await second.DisposeAsync();
        Record.Exception(() => messageCrypto.Encrypt(new MessageMetadata(), payload)).ShouldBeNull("a channel does not dispose the message crypto");
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenFailureActionSend_ShouldCreateChannelsThatSendTheMessageUnencryptedIfEncryptionFails()
    {
        //Arrange
        using var messageCrypto = new MessageCrypto(true);
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false, messageCrypto, ProducerCryptoFailureAction.Send);
        var channel = await uut.Create(Current.CancellationToken);

        //Act
        await channel.Send(new MessageMetadata { SequenceId = 1 }, new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 }), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        connection.Sent.Single().Payload.ShouldBe([1, 2, 3]);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenFailureActionFail_ShouldCreateChannelsThatThrowIfEncryptionFails()
    {
        //Arrange
        using var messageCrypto = new MessageCrypto(true);
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false, messageCrypto, ProducerCryptoFailureAction.Fail);
        var channel = await uut.Create(Current.CancellationToken);

        //Act
        var exception = await Record.ExceptionAsync(() => channel.Send(new MessageMetadata { SequenceId = 1 }, new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 }), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<DotPulsar.Exceptions.CryptoException>();
        connection.Sent.ShouldBeEmpty();
    }

    private static MessageCrypto CreateMessageCrypto()
    {
        var messageCrypto = new MessageCrypto(true);
        messageCrypto.AddPublicKeyCipher(["rsa"], new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa")));
        return messageCrypto;
    }

    private static ProducerChannelFactory CreateFactory(FakeProducerConnection connection, bool transactionsEnabled, MessageCrypto? messageCrypto = null, ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.Fail)
        => new(
            Guid.NewGuid(),
            Substitute.For<IRegisterEvent>(),
            connection.Pool,
            "persistent://public/default/test",
            null,
            ProducerAccessMode.Shared,
            DotPulsar.Schema.ByteArray.SchemaInfo,
            null,
            null,
            transactionsEnabled,
            null,
            messageCrypto,
            cryptoFailureAction);

    // Upstream: ProducerBuilderImpl#initialSubscriptionName
    [Fact]
    public async Task Create_GivenInitialSubscriptionName_ShouldSetItOnCommandProducer()
    {
        //Arrange
        var commands = new List<CommandProducer>();
        var uut = CreateFactory("my-initial-subscription", commands);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        commands.Single().HasInitialSubscriptionName.ShouldBeTrue();
        commands.Single().InitialSubscriptionName.ShouldBe("my-initial-subscription");
    }

    // Additional coverage
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task Create_GivenNoInitialSubscriptionName_ShouldNotSetItOnCommandProducer(string? initialSubscriptionName)
    {
        //Arrange
        var commands = new List<CommandProducer>();
        var uut = CreateFactory(initialSubscriptionName, commands);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        commands.Single().HasInitialSubscriptionName.ShouldBeFalse();
    }

    private static ProducerChannelFactory CreateFactory(string? initialSubscriptionName, List<CommandProducer> commands)
    {
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Do<CommandProducer>(command => commands.Add(command.Clone())), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(new ProducerResponse(1, "producer", 0)));
        var connectionPool = Substitute.For<IConnectionPool>();
        connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));

        return new ProducerChannelFactory(
            Guid.NewGuid(),
            Substitute.For<IRegisterEvent>(),
            connectionPool,
            "persistent://public/default/test",
            null,
            ProducerAccessMode.Shared,
            DotPulsar.Schema.ByteArray.SchemaInfo,
            null,
            null,
            false,
            initialSubscriptionName,
            null,
            ProducerCryptoFailureAction.Fail);
    }
}
