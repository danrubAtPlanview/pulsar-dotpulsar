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
using DotPulsar.Internal.PulsarApi;
using ProducerAccessMode = DotPulsar.Internal.PulsarApi.ProducerAccessMode;

// What the broker is told about a producer when it connects again: the id, the name and the epoch. The broker deduplicates by the name, and it replaces the registration that an earlier connection has by the
// one of a later connection if the id is the same and the epoch is larger.
[Trait("Category", "Unit")]
public sealed class ProducerIdentityTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _guard = TimeSpan.FromSeconds(10);

    // Upstream: ProducerImpl#connectionOpened (the same producerId, the generated producerName, and the next epoch after a reconnect)
    [Fact(Timeout = 30_000)]
    public async Task Connectivity_GivenProducerWithoutNameAndConnectionLost_ShouldConnectAgainWithTheSameIdAndGeneratedNameAndALargerEpoch()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var uut = await CreateConnectedProducer(connection);
        _ = await uut.Send(new DotPulsar.MessageMetadata(), [1], Current.CancellationToken);

        //Act
        await Reconnect(connection, 0, 2);
        await uut.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken).AsTask().WaitAsync(_guard, Current.CancellationToken);
        _ = await uut.Send(new DotPulsar.MessageMetadata(), [2], Current.CancellationToken).AsTask().WaitAsync(_guard, Current.CancellationToken);

        //Assert
        var commands = connection.CommandProducers;
        commands.Count.ShouldBe(2);
        commands[0].HasProducerName.ShouldBeFalse();
        commands[1].ProducerName.ShouldBe("producer-1");
        commands[1].HasUserProvidedProducerName.ShouldBeTrue();
        commands[1].UserProvidedProducerName.ShouldBeFalse();
        commands[1].ProducerId.ShouldBe(commands[0].ProducerId);
        commands[1].Epoch.ShouldBeGreaterThan(commands[0].Epoch);
        var sent = connection.Sent;
        sent.Count.ShouldBe(2);
        sent[1].ProducerId.ShouldBe(sent[0].ProducerId);
        sent[1].Metadata.ProducerName.ShouldBe(sent[0].Metadata.ProducerName);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Connectivity_GivenProducerWithNameAndConnectionLost_ShouldConnectAgainWithTheSameNameAndNoEpochAndTheDefaultOfUserProvidedProducerName()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var uut = await CreateConnectedProducer(connection, "my-producer");

        //Act
        await Reconnect(connection, 0, 2);

        //Assert
        var commands = connection.CommandProducers;
        commands.ShouldAllBe(command => command.ProducerName == "my-producer");
        commands.ShouldAllBe(command => !command.HasEpoch && !command.HasUserProvidedProducerName);
        commands[1].ProducerId.ShouldBe(commands[0].ProducerId);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Connectivity_GivenProducerWithoutNameThatConnectsSeveralTimes_ShouldSendAGrowingEpoch()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var uut = await CreateConnectedProducer(connection);

        //Act
        await Reconnect(connection, 0, 2);
        await Reconnect(connection, 1, 3);
        await Reconnect(connection, 2, 4);

        //Assert
        var commands = connection.CommandProducers;
        commands.Select(command => command.Epoch).ShouldBe([0ul, 1ul, 2ul, 3ul]);
        commands.Skip(1).ShouldAllBe(command => command.ProducerName == "producer-1");
        commands.Select(command => command.ProducerId).Distinct().Count().ShouldBe(1);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Connectivity_GivenPartitionedTopic_ShouldGiveEveryPartitionItsOwnIdAndNameThatStayTheSameWhenOnePartitionConnectsAgain()
    {
        //Arrange
        var connection = new FakeProducerConnection(3);
        await using var uut = await CreateConnectedProducer(connection);
        var first = connection.CommandProducers;
        var secondPartition = first.Single(command => command.Topic.EndsWith("-partition-1", StringComparison.Ordinal));
        var secondPartitionChannel = connection.Channels[first.ToList().IndexOf(secondPartition)];

        //Act
        secondPartitionChannel.Disconnected();
        await WaitForCommands(connection, 4);

        //Assert
        first.Count.ShouldBe(3);
        first.Select(command => command.ProducerId).Distinct().Count().ShouldBe(3, "every partition has its own id");
        first.ShouldAllBe(command => !command.HasProducerName);
        var commands = connection.CommandProducers;
        commands.Count.ShouldBe(4);
        var again = commands[3];
        again.Topic.ShouldBe(secondPartition.Topic);
        again.ProducerId.ShouldBe(secondPartition.ProducerId);
        again.Epoch.ShouldBeGreaterThan(secondPartition.Epoch);
        again.ProducerName.ShouldBe(connection.GeneratedNames[again.Topic]);
        again.UserProvidedProducerName.ShouldBeFalse();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Connectivity_GivenPartitionedTopicAndEveryPartitionConnectsAgain_ShouldSendTheNameOfEveryPartitionThatItWasGivenAndNotTheNameOfAnotherPartition()
    {
        //Arrange
        var connection = new FakeProducerConnection(3);
        await using var uut = await CreateConnectedProducer(connection);
        var channels = connection.Channels;

        //Act
        foreach (var channel in channels)
            channel.Disconnected();

        await WaitForCommands(connection, 6);

        //Assert
        var commands = connection.CommandProducers;
        commands.Count.ShouldBe(6);
        connection.GeneratedNames.Values.Distinct().Count().ShouldBe(3, "the broker gave every partition its own name");
        commands.Take(3).Select(command => command.ProducerId).OrderBy(id => id).ShouldBe(commands.Skip(3).Select(command => command.ProducerId).OrderBy(id => id));

        foreach (var again in commands.Skip(3))
        {
            again.ProducerName.ShouldBe(connection.GeneratedNames[again.Topic]);
            again.ProducerId.ShouldBe(commands.First(command => command.Topic == again.Topic).ProducerId);
            again.Epoch.ShouldBe(1ul);
        }
    }

    // Additional coverage
    // A producer, a consumer and another producer on one connection, with ids that are the same number, still get their own channels
    [Fact]
    public async Task ChannelManager_GivenProducersAndConsumerWithTheSameNumberAsId_ShouldKeepTheirChannelsApart()
    {
        //Arrange
        using var uut = new ChannelManager();
        var consumer = CreateChannel();
        var firstProducer = CreateChannel();
        var secondProducer = CreateChannel();
        var subscribe = new CommandSubscribe { Topic = _topic, Subscription = "subscription" };
        var consumerResponse = uut.Outgoing(subscribe, consumer);
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = subscribe.RequestId } });
        var consumerId = (await consumerResponse).ConsumerId;
        var firstProducerCommand = new CommandProducer { Topic = _topic, ProducerId = consumerId };
        var secondProducerCommand = new CommandProducer { Topic = _topic, ProducerId = consumerId + 1 };
        var firstProducerResponse = uut.Outgoing(firstProducerCommand, firstProducer);
        uut.Incoming(CreateProducerSuccess(firstProducerCommand.RequestId));
        _ = await firstProducerResponse;
        var secondProducerResponse = uut.Outgoing(secondProducerCommand, secondProducer);
        uut.Incoming(CreateProducerSuccess(secondProducerCommand.RequestId));
        _ = await secondProducerResponse;

        //Act
        uut.Incoming(new CommandMessage { ConsumerId = consumerId, MessageId = new MessageIdData { LedgerId = 1, EntryId = 1 } }, new System.Buffers.ReadOnlySequence<byte>([1]));
        uut.Outgoing(new CommandSend { ProducerId = consumerId, SequenceId = 1 }, new TaskCompletionSource<BaseCommand>());
        uut.Outgoing(new CommandSend { ProducerId = consumerId + 1, SequenceId = 1 }, new TaskCompletionSource<BaseCommand>());

        //Assert
        consumer.Received(1).Received(Arg.Any<MessagePackage>());
        consumer.DidNotReceive().SenderLock();
        firstProducer.DidNotReceive().Received(Arg.Any<MessagePackage>());
        firstProducer.Received(1).SenderLock();
        secondProducer.DidNotReceive().Received(Arg.Any<MessagePackage>());
        secondProducer.Received(1).SenderLock();
    }

    // Additional coverage
    // The producer has the id of its command and the consumer has the id that the lookup of consumers gave it. When the connection closes, every channel is told.
    [Fact]
    public async Task ChannelManager_GivenProducerAndConsumerOnTheConnection_ShouldTellBothWhenTheConnectionIsDisposed()
    {
        //Arrange
        var uut = new ChannelManager();
        var consumer = CreateChannel();
        var producer = CreateChannel();
        var subscribe = new CommandSubscribe { Topic = _topic, Subscription = "subscription" };
        var consumerResponse = uut.Outgoing(subscribe, consumer);
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = subscribe.RequestId } });
        _ = await consumerResponse;
        var producerCommand = new CommandProducer { Topic = _topic, ProducerId = ProducerIds.Next() };
        var producerResponse = uut.Outgoing(producerCommand, producer);
        uut.Incoming(CreateProducerSuccess(producerCommand.RequestId));
        _ = await producerResponse;

        //Act
        uut.Dispose();

        //Assert
        consumer.Received(1).Disconnected();
        producer.Received(1).Disconnected();
    }

    // Additional coverage
    // The commands that the connection writes are the ones that the broker reads, so what the producer tells the broker is checked on the wire, with a broker that gives a producer without a name a name of its own
    [Fact]
    public async Task Create_GivenProducerWithoutNameAndARealConnection_ShouldWriteTheIdTheNameAndTheEpochOfTheCommands()
    {
        //Arrange
        var nextName = 0;
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.Producer
            ? new BaseCommand
            {
                Type = BaseCommand.Types.Type.ProducerSuccess,
                ProducerSuccess = new CommandProducerSuccess { RequestId = command.Producer.RequestId, ProducerName = command.Producer.HasProducerName ? command.Producer.ProducerName : $"generated-{++nextName}" }
            }
            : null);
        await using var connection = Connection.Connect(stream, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        var uut = new ProducerChannelFactory(Guid.NewGuid(), Substitute.For<IRegisterEvent>(), pool, _topic, null, ProducerAccessMode.Shared, DotPulsar.Schema.ByteArray.SchemaInfo, null, null, false, null, null, ProducerCryptoFailureAction.Fail);

        //Act
        await uut.Create(Current.CancellationToken);
        await uut.Create(Current.CancellationToken);

        //Assert
        var commands = stream.Sent.Where(command => command.Type == BaseCommand.Types.Type.Producer).Select(command => command.Producer).ToList();
        commands.Count.ShouldBe(2);
        commands[0].HasProducerName.ShouldBeFalse();
        commands[0].UserProvidedProducerName.ShouldBeFalse();
        commands[0].Epoch.ShouldBe(0ul);
        commands[1].ProducerName.ShouldBe("generated-1");
        commands[1].UserProvidedProducerName.ShouldBeFalse();
        commands[1].Epoch.ShouldBe(1ul);
        commands[1].ProducerId.ShouldBe(commands[0].ProducerId);
        commands[1].RequestId.ShouldNotBe(commands[0].RequestId);
    }

    private static IChannel CreateChannel()
    {
        var channel = Substitute.For<IChannel>();
        channel.SenderLock().Returns(Substitute.For<IDisposable>());
        return channel;
    }

    private static BaseCommand CreateProducerSuccess(ulong requestId)
        => new() { Type = BaseCommand.Types.Type.ProducerSuccess, ProducerSuccess = new CommandProducerSuccess { RequestId = requestId, ProducerName = "producer" } };

    private static async Task<Producer<byte[]>> CreateConnectedProducer(FakeProducerConnection connection, string? producerName = null)
    {
        var options = new ProducerOptions<byte[]>(_topic, DotPulsar.Schema.ByteArray) { ProducerName = producerName };
        var producer = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, false);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken).AsTask().WaitAsync(_guard, Current.CancellationToken);
        return producer;
    }

    // The connection is lost, which is what the connection tells the channel of the producer when it is disposed
    private static async Task Reconnect(FakeProducerConnection connection, int channelIndex, int expectedNumberOfCommands)
    {
        connection.Channels[channelIndex].Disconnected();
        await WaitForCommands(connection, expectedNumberOfCommands);
    }

    private static async Task WaitForCommands(FakeProducerConnection connection, int expectedNumberOfCommands)
    {
        var deadline = DateTime.UtcNow + _guard;

        while (connection.CommandProducers.Count < expectedNumberOfCommands && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }

        connection.CommandProducers.Count.ShouldBeGreaterThanOrEqualTo(expectedNumberOfCommands);
    }
}
