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

using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using System.Text;

[Trait("Category", "Unit")]
public sealed class ConsumerChannelFactoryTests
{
    // Upstream: ConsumerImpl#connectionOpened
    [Fact]
    public async Task Create_GivenConsumerEpoch_ShouldSubscribeWithTheEpoch()
    {
        //Arrange
        var consumerEpoch = new ConsumerEpoch();
        consumerEpoch.Increment();
        consumerEpoch.Increment();
        var subscriptions = new List<CommandSubscribe>();
        var uut = CreateFactory(CreateConnection(subscriptions), CommandSubscribe.Types.SubType.Failover, consumerEpoch);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        subscriptions.Single().HasConsumerEpoch.ShouldBeTrue();
        subscriptions.Single().ConsumerEpoch.ShouldBe(2UL);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenNoConsumerEpoch_ShouldNotSubscribeWithAnEpoch()
    {
        //Arrange
        var subscriptions = new List<CommandSubscribe>();
        var redeliveries = new List<CommandRedeliverUnacknowledgedMessages>();
        var uut = CreateFactory(CreateConnection(subscriptions, redeliveries), CommandSubscribe.Types.SubType.Exclusive, null);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        subscriptions.Single().HasConsumerEpoch.ShouldBeFalse();
        redeliveries.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenEpochUnchangedWhileSubscribing_ShouldNotSendRedeliver()
    {
        //Arrange
        var redeliveries = new List<CommandRedeliverUnacknowledgedMessages>();
        var uut = CreateFactory(CreateConnection(redeliveries: redeliveries), CommandSubscribe.Types.SubType.Failover, new ConsumerEpoch());

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        redeliveries.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenEpochIncreasedWhileSubscribing_ShouldSendRedeliverWithTheNewEpochBeforeAnyFlow()
    {
        //Arrange
        var consumerEpoch = new ConsumerEpoch();
        var subscriptions = new List<CommandSubscribe>();
        var redeliveries = new List<CommandRedeliverUnacknowledgedMessages>();
        var connection = CreateConnection(subscriptions, redeliveries, _ => consumerEpoch.Increment());
        var uut = CreateFactory(connection, CommandSubscribe.Types.SubType.Failover, consumerEpoch);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        subscriptions.Single().ConsumerEpoch.ShouldBe(0UL);
        redeliveries.Count.ShouldBe(1);
        redeliveries[0].ConsumerId.ShouldBe(7UL);
        redeliveries[0].ConsumerEpoch.ShouldBe(1UL);
        redeliveries[0].MessageIds.Count.ShouldBe(0);
        await connection.DidNotReceive().Send(Arg.Any<CommandFlow>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Theory]
    [InlineData(CommandSubscribe.Types.SubType.Exclusive, 2UL)]
    [InlineData(CommandSubscribe.Types.SubType.Failover, 2UL)]
    [InlineData(CommandSubscribe.Types.SubType.Shared, 1UL)]
    [InlineData(CommandSubscribe.Types.SubType.KeyShared, 1UL)]
    public async Task Create_GivenSubscriptionType_ShouldOnlySkipStaleMessagesIfSubscriptionHasSingleActiveConsumer(CommandSubscribe.Types.SubType subscriptionType, ulong expectedEntryId)
    {
        //Arrange
        var consumerEpoch = new ConsumerEpoch();
        consumerEpoch.Increment();
        IChannel? brokerChannel = null;
        var connection = CreateConnection(onSubscribe: channel => brokerChannel = channel);
        var uut = CreateFactory(connection, subscriptionType, consumerEpoch);
        await using var channel = await uut.Create(Current.CancellationToken);
        brokerChannel!.Received(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 1), consumerEpoch: 0));
        brokerChannel.Received(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2), consumerEpoch: 1));

        //Act
        var actual = await channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(expectedEntryId);
    }

    // Upstream: ConsumerImpl#connectionOpened
    [Fact]
    public async Task Create_GivenDeadLetterHandler_ShouldClearTheCandidatesBeforeSubscribing()
    {
        //Arrange
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        deadLetterHandler.Register(1, 2, BatchTestData.CreateCandidate(1, 2));
        var candidatesWhenSubscribing = -1;
        var connection = CreateConnection(onSubscribe: _ => candidatesWhenSubscribing = deadLetterHandler.Count);
        var uut = CreateFactory(connection, CommandSubscribe.Types.SubType.Shared, new ConsumerEpoch(), null, deadLetterHandler);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        candidatesWhenSubscribing.ShouldBe(0);
        deadLetterHandler.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenTrackerAndDeadLetterHandler_ShouldGiveThemToTheChannel()
    {
        //Arrange
        using var tracker = new UnAckedMessageTracker(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1), (_, _) => default);
        tracker.Add(new MessageId(1, 2, 0, -1));
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(3, out _);
        var connection = CreateConnection();
        connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var uut = CreateFactory(connection, CommandSubscribe.Types.SubType.Shared, new ConsumerEpoch(), tracker, deadLetterHandler);
        await using var channel = await uut.Create(Current.CancellationToken);
        deadLetterHandler.Register(1, 2, BatchTestData.CreateCandidate(1, 2));
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        command.MessageId.Add(BatchTestData.CreateEntryId(1, 2));

        //Act
        await channel.Send(command, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(0);
        deadLetterHandler.Count.ShouldBe(0);
    }

    // The chunks of a message can arrive on different channels, because the broker delivers the unacknowledged entries again after a reconnect
    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Create_GivenTwoChannels_ShouldAssembleTheChunksThatArriveOnDifferentChannelsIntoOneMessage()
    {
        //Arrange
        var channels = new List<IChannel>();
        var uut = CreateFactory(CreateConnection(onSubscribe: channels.Add), CommandSubscribe.Types.SubType.Exclusive, null);
        await using var first = await uut.Create(Current.CancellationToken);
        await using var second = await uut.Create(Current.CancellationToken);
        channels[0].Received(ChunkTestData.CreateChunkPackage("a", 0, 2, 1));
        channels[0].Received(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        _ = await first.Receive(Current.CancellationToken);

        //Act
        channels[1].Received(ChunkTestData.CreateChunkPackage("a", 1, 2, 3));
        var actual = await second.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(3UL);
        actual.MessageId.FirstChunkMessageId!.EntryId.ShouldBe(1UL);
    }

    // Upstream: ConsumerImpl#ConsumerImpl (msgCrypto)
    // The messages of every channel of the sub-consumer are decrypted by the same MessageCrypto, which keeps the data keys that it has read
    [Fact(Timeout = 30_000)]
    public async Task Create_GivenMessageCryptoAndCryptoKeyReader_ShouldDecryptTheMessagesOfTheChannel()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        using var consumer = new MessageCrypto(false);
        var channels = new List<IChannel>();
        var uut = CreateFactory(CreateConnection(onSubscribe: channels.Add), CommandSubscribe.Types.SubType.Exclusive, null, messageCrypto: consumer, cryptoKeyReader: CreateCryptoKeyReader());
        await using var channel = await uut.Create(Current.CancellationToken);
        channels[0].Received(CreateEncryptedPackage(producer, 1, "Hello"));

        //Act
        var actual = await channel.Receive(Current.CancellationToken);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("Hello");
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
    }

    // Upstream: ConsumerImpl#handleCryptoFailure
    [Fact(Timeout = 30_000)]
    public async Task Create_GivenCryptoFailureAction_ShouldGiveItToTheChannel()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        var acks = new List<CommandAck>();
        var channels = new List<IChannel>();
        var connection = CreateConnection(onSubscribe: channels.Add);
        connection.Send(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var uut = CreateFactory(connection, CommandSubscribe.Types.SubType.Exclusive, null, cryptoFailureAction: ConsumerCryptoFailureAction.Discard);
        await using var channel = await uut.Create(Current.CancellationToken);
        channels[0].Received(CreateEncryptedPackage(producer, 1, "Hello"));
        channels[0].Received(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));

        //Act
        var actual = await channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(2UL);
        acks.ShouldHaveSingleItem().ValidationError.ShouldBe(CommandAck.Types.ValidationError.DecryptionError);
    }

    private static MessageCrypto CreateProducerCrypto()
    {
        var producer = new MessageCrypto(true);
        producer.AddPublicKeyCipher(["key"], new InMemoryCryptoKeyReader().WithPublicKey("key", CryptoTestKeys.Public("ClientRsa")));
        return producer;
    }

    private static ICryptoKeyReader CreateCryptoKeyReader()
        => new InMemoryCryptoKeyReader().WithPrivateKey("key", CryptoTestKeys.Private("ClientRsa"));

    private static MessagePackage CreateEncryptedPackage(MessageCrypto producer, ulong entryId, string text)
    {
        var metadata = new MessageMetadata { ProducerName = "producer", SequenceId = 1, PublishTime = 1 };
        var encrypted = producer.Encrypt(metadata, new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(text)));
        return BatchTestData.CreatePackage(BatchTestData.CreateEntryId(1, entryId), metadata, encrypted.ToArray());
    }

    private static ConsumerChannelFactory<byte[]> CreateFactory(IConnection connection, CommandSubscribe.Types.SubType subscriptionType, ConsumerEpoch? consumerEpoch, UnAckedMessageTracker? tracker = null, DeadLetterHandler<byte[]>? deadLetterHandler = null, MessageCrypto? messageCrypto = null, ICryptoKeyReader? cryptoKeyReader = null, ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.Fail)
    {
        var connectionPool = Substitute.For<IConnectionPool>();
        connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        var subscribe = new CommandSubscribe { Topic = BatchTestData.Topic, Subscription = "subscription", SubType = subscriptionType };

        return new ConsumerChannelFactory<byte[]>(
            Guid.NewGuid(),
            Substitute.For<IRegisterEvent>(),
            connectionPool,
            subscribe,
            100,
            new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, null),
            messageFactory,
            [],
            BatchTestData.Topic,
            false,
            false,
            consumerEpoch,
            tracker,
            deadLetterHandler,
            ChunkTestData.CreateAssembler(),
            messageCrypto,
            cryptoKeyReader,
            cryptoFailureAction);
    }

    private static IConnection CreateConnection(List<CommandSubscribe>? subscriptions = null, List<CommandRedeliverUnacknowledgedMessages>? redeliveries = null, Action<IChannel>? onSubscribe = null)
    {
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandSubscribe>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            subscriptions?.Add(callInfo.Arg<CommandSubscribe>().Clone());
            onSubscribe?.Invoke(callInfo.Arg<IChannel>());
            return Task.FromResult(new SubscribeResponse(7));
        });
        connection.Send(Arg.Do<CommandRedeliverUnacknowledgedMessages>(command => redeliveries?.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return connection;
    }
}
