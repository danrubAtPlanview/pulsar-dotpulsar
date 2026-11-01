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

    // Upstream: ProducerImpl#connectionOpened (Commands.newProducer(..., producerName, ..., epoch, userProvidedProducerName, ...))
    // The producer has no name when it connects for the first time, and the broker generates one. The broker is told that the name is not the user's, also then, because older brokers (for example 3.1.0) do not replace a registration if the name is.
    [Fact]
    public async Task Create_GivenNoProducerNameAndTheFirstConnect_ShouldNotSetNameAndShouldSayThatTheNameIsNotUserProvided()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        var command = connection.CommandProducers.Single();
        command.HasProducerName.ShouldBeFalse();
        command.HasUserProvidedProducerName.ShouldBeTrue();
        command.UserProvidedProducerName.ShouldBeFalse();
        command.HasEpoch.ShouldBeTrue();
        command.Epoch.ShouldBe(0ul);
        command.HasProducerId.ShouldBeTrue();
    }

    // Upstream: ProducerImpl#connectionOpened (this.producerName = producerName of the first response), ConnectionHandler#switchClientCnx (epoch)
    // The broker deduplicates the messages that a producer sends again after a reconnect by the name of the producer, and it replaces the producer of an earlier connection by the one of a later connection
    // if the id is the same and the epoch is larger, so a producer without a name is the same producer for the broker only if it sends its generated name, its id and a larger epoch again.
    [Fact]
    public async Task Create_GivenNoProducerNameAndTheProducerIsCreatedAgain_ShouldSendTheGeneratedNameWithTheSameProducerIdAndALargerEpoch()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);

        //Act
        await uut.Create(Current.CancellationToken);
        await uut.Create(Current.CancellationToken);
        await uut.Create(Current.CancellationToken);

        //Assert
        var commands = connection.CommandProducers;
        commands.Count.ShouldBe(3);
        commands[0].HasProducerName.ShouldBeFalse();
        const string generatedName = "producer-1"; // The answer that the fake connection gives to the first command
        commands[1].ProducerName.ShouldBe(generatedName);
        commands[2].ProducerName.ShouldBe(generatedName, "the name that the broker generated the first time is sent, and not the name of an answer to a command that had a name");
        commands.ShouldAllBe(command => command.HasUserProvidedProducerName && !command.UserProvidedProducerName);
        commands.Select(command => command.ProducerId).Distinct().Count().ShouldBe(1);
        commands[1].Epoch.ShouldBeGreaterThan(commands[0].Epoch);
        commands[2].Epoch.ShouldBeGreaterThan(commands[1].Epoch);
    }

    // Additional coverage
    // The messages of the channel carry the name of the producer, and the broker deduplicates by it, so a channel that is created again must carry the same name as the first one
    [Fact]
    public async Task Create_GivenNoProducerNameAndTheProducerIsCreatedAgain_ShouldCreateChannelsThatSendTheSameProducerName()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);
        var payload = new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 });

        //Act
        var first = await uut.Create(Current.CancellationToken);
        var second = await uut.Create(Current.CancellationToken);
        await first.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await second.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var sent = connection.Sent;
        sent.Count.ShouldBe(2);
        sent[0].Metadata.ProducerName.ShouldBe("producer-1");
        sent[1].Metadata.ProducerName.ShouldBe("producer-1");
        sent[1].ProducerId.ShouldBe(sent[0].ProducerId);
    }

    // Additional coverage
    // A connect that fails did not give the producer a name, so the next one does not send one, and the epoch grows with the attempt
    [Fact]
    public async Task Create_GivenNoProducerNameAndTheFirstConnectFails_ShouldNotSendANameThatWasNeverGenerated()
    {
        //Arrange
        var commands = new List<CommandProducer>();
        var connection = Substitute.For<IConnection>();
        var attempts = 0;
        connection.Send(Arg.Any<CommandProducer>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            commands.Add(callInfo.Arg<CommandProducer>().Clone());
            return ++attempts == 1 ? Task.FromException<ProducerResponse>(new DotPulsar.Exceptions.ProducerBusyException("busy")) : Task.FromResult(new ProducerResponse(1, "generated", 0));
        });
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        var uut = new ProducerChannelFactory(Guid.NewGuid(), Substitute.For<IRegisterEvent>(), pool, "persistent://public/default/test", null, ProducerAccessMode.Shared, DotPulsar.Schema.ByteArray.SchemaInfo, null, null, false, null, null, ProducerCryptoFailureAction.Fail);

        //Act
        _ = await Record.ExceptionAsync(() => uut.Create(Current.CancellationToken));
        await uut.Create(Current.CancellationToken);
        await uut.Create(Current.CancellationToken);

        //Assert
        commands.Count.ShouldBe(3);
        commands[0].HasProducerName.ShouldBeFalse();
        commands[1].HasProducerName.ShouldBeFalse("the first attempt failed, so the broker has not told the producer a name");
        commands[2].ProducerName.ShouldBe("generated");
        commands.Select(command => command.Epoch).ShouldBe([0ul, 1ul, 2ul]);
        commands.Select(command => command.ProducerId).Distinct().Count().ShouldBe(1);
    }

    // Upstream: ProducerImpl#ProducerImpl (userProvidedProducerName = StringUtils.isNotBlank(producerName))
    // A producer that the user names is sent as it always was: the name, and neither the epoch nor the field that says that the name is not the user's
    [Fact]
    public async Task Create_GivenProducerNameAndTheProducerIsCreatedAgain_ShouldSendTheNameAndNeitherEpochNorUserProvidedProducerName()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false, producerName: "my-producer");

        //Act
        await uut.Create(Current.CancellationToken);
        await uut.Create(Current.CancellationToken);

        //Assert
        var commands = connection.CommandProducers;
        commands.Count.ShouldBe(2);
        commands.ShouldAllBe(command => command.ProducerName == "my-producer");
        commands.ShouldAllBe(command => !command.HasUserProvidedProducerName && command.UserProvidedProducerName, "the field has its default, which is true, as it was sent before");
        commands.ShouldAllBe(command => !command.HasEpoch);
    }

    // Additional coverage
    // The id is the producer's own and is given out once, so it is the same on every connection, and no two producers share it
    [Fact]
    public async Task Create_GivenSeveralProducers_ShouldGiveEveryProducerItsOwnProducerIdThatStaysTheSame()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var factories = Enumerable.Range(0, 10).Select(i => CreateFactory(connection, false, producerName: i % 2 == 0 ? null : $"named-{i}")).ToArray();

        //Act
        foreach (var factory in factories)
            await factory.Create(Current.CancellationToken);

        foreach (var factory in factories)
            await factory.Create(Current.CancellationToken);

        //Assert
        var commands = connection.CommandProducers;
        commands.Count.ShouldBe(20);
        commands.Take(10).Select(command => command.ProducerId).Distinct().Count().ShouldBe(10);
        commands.Skip(10).Select(command => command.ProducerId).ShouldBe(commands.Take(10).Select(command => command.ProducerId));
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenTheProducerIdOfTheCommand_ShouldCreateAChannelThatSendsWithThatProducerId()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);
        var channel = await uut.Create(Current.CancellationToken);

        //Act
        await channel.Send(new MessageMetadata { SequenceId = 1 }, new ReadOnlySequence<byte>(new byte[] { 1 }), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        connection.Sent.Single().ProducerId.ShouldBe(connection.CommandProducers.Single().ProducerId);
    }

    private static MessageCrypto CreateMessageCrypto()
    {
        var messageCrypto = new MessageCrypto(true);
        messageCrypto.AddPublicKeyCipher(["rsa"], new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa")));
        return messageCrypto;
    }

    private static ProducerChannelFactory CreateFactory(FakeProducerConnection connection, bool transactionsEnabled, MessageCrypto? messageCrypto = null, ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.Fail, string? producerName = null)
        => new(
            Guid.NewGuid(),
            Substitute.For<IRegisterEvent>(),
            connection.Pool,
            "persistent://public/default/test",
            producerName,
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
