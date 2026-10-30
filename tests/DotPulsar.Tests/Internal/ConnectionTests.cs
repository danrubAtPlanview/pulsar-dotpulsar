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

using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class ConnectionTests
{
    private static readonly TimeSpan _keepAliveInterval = TimeSpan.FromMinutes(5);

    [Fact]
    public void ProtocolVersion_GivenDefaults_ShouldAdvertiseProtocolVersion19()
    {
        //Assert
        Constants.ProtocolVersion.ShouldBe(19);
        Constants.ProtocolVersionAckReceipt.ShouldBe(17);
        Constants.TransactionCoordinatorAssignTopic.ShouldBe("persistent://pulsar/system/transaction_coordinator_assign");
    }

    [Fact]
    public async Task Send_GivenCommandTcClientConnectRequest_ShouldSendRequestAndReturnResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.TcClientConnectRequest
            ? new BaseCommand { Type = BaseCommand.Types.Type.TcClientConnectResponse, TcClientConnectResponse = new CommandTcClientConnectResponse { RequestId = command.TcClientConnectRequest.RequestId } }
            : null);
        await using var uut = Connect(stream);

        //Act
        var response = await uut.Send(new CommandTcClientConnectRequest { TcId = 3 }, Current.CancellationToken);

        //Assert
        response.Type.ShouldBe(BaseCommand.Types.Type.TcClientConnectResponse);
        var sent = stream.Sent.Single(c => c.Type == BaseCommand.Types.Type.TcClientConnectRequest);
        sent.TcClientConnectRequest.TcId.ShouldBe(3ul);
        sent.TcClientConnectRequest.RequestId.ShouldBe(response.TcClientConnectResponse.RequestId);
    }

    [Fact]
    public async Task Send_GivenCommandNewTxn_ShouldSendRequestAndReturnResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.NewTxn
            ? new BaseCommand { Type = BaseCommand.Types.Type.NewTxnResponse, NewTxnResponse = new CommandNewTxnResponse { RequestId = command.NewTxn.RequestId, TxnidMostBits = 3, TxnidLeastBits = 7 } }
            : null);
        await using var uut = Connect(stream);

        //Act
        var response = await uut.Send(new CommandNewTxn { TcId = 3, TxnTtlMillis = 60000 }, Current.CancellationToken);

        //Assert
        response.NewTxnResponse.TxnidMostBits.ShouldBe(3ul);
        response.NewTxnResponse.TxnidLeastBits.ShouldBe(7ul);
        var sent = stream.Sent.Single(c => c.Type == BaseCommand.Types.Type.NewTxn);
        sent.NewTxn.TxnTtlMillis.ShouldBe(60000ul);
    }

    [Fact]
    public async Task Send_GivenCommandAddPartitionToTxn_ShouldSendRequestAndReturnResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.AddPartitionToTxn
            ? new BaseCommand { Type = BaseCommand.Types.Type.AddPartitionToTxnResponse, AddPartitionToTxnResponse = new CommandAddPartitionToTxnResponse { RequestId = command.AddPartitionToTxn.RequestId } }
            : null);
        await using var uut = Connect(stream);
        var request = new CommandAddPartitionToTxn { TxnidMostBits = 1, TxnidLeastBits = 2 };
        request.Partitions.Add("persistent://public/default/topic");

        //Act
        var response = await uut.Send(request, Current.CancellationToken);

        //Assert
        response.Type.ShouldBe(BaseCommand.Types.Type.AddPartitionToTxnResponse);
        var sent = stream.Sent.Single(c => c.Type == BaseCommand.Types.Type.AddPartitionToTxn);
        sent.AddPartitionToTxn.Partitions.ShouldBe(["persistent://public/default/topic"]);
    }

    [Fact]
    public async Task Send_GivenCommandAddSubscriptionToTxn_ShouldSendRequestAndReturnResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.AddSubscriptionToTxn
            ? new BaseCommand { Type = BaseCommand.Types.Type.AddSubscriptionToTxnResponse, AddSubscriptionToTxnResponse = new CommandAddSubscriptionToTxnResponse { RequestId = command.AddSubscriptionToTxn.RequestId } }
            : null);
        await using var uut = Connect(stream);
        var request = new CommandAddSubscriptionToTxn { TxnidMostBits = 1, TxnidLeastBits = 2 };
        request.Subscription.Add(new Subscription { Topic = "persistent://public/default/topic", Subscription_ = "sub" });

        //Act
        var response = await uut.Send(request, Current.CancellationToken);

        //Assert
        response.Type.ShouldBe(BaseCommand.Types.Type.AddSubscriptionToTxnResponse);
        stream.Sent.Single(c => c.Type == BaseCommand.Types.Type.AddSubscriptionToTxn).AddSubscriptionToTxn.Subscription.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Send_GivenCommandEndTxn_ShouldSendRequestAndReturnResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.EndTxn
            ? new BaseCommand { Type = BaseCommand.Types.Type.EndTxnResponse, EndTxnResponse = new CommandEndTxnResponse { RequestId = command.EndTxn.RequestId } }
            : null);
        await using var uut = Connect(stream);

        //Act
        var response = await uut.Send(new CommandEndTxn { TxnidMostBits = 1, TxnidLeastBits = 2, TxnAction = TxnAction.Abort }, Current.CancellationToken);

        //Assert
        response.Type.ShouldBe(BaseCommand.Types.Type.EndTxnResponse);
        stream.Sent.Single(c => c.Type == BaseCommand.Types.Type.EndTxn).EndTxn.TxnAction.ShouldBe(TxnAction.Abort);
    }

    [Fact]
    public async Task SendWithResponse_GivenCommandAckForRegisteredConsumer_ShouldSendRequestIdAndReturnResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type switch
        {
            BaseCommand.Types.Type.Subscribe => new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = command.Subscribe.RequestId } },
            BaseCommand.Types.Type.Ack => new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse { ConsumerId = command.Ack.ConsumerId, RequestId = command.Ack.RequestId } },
            _ => null
        });
        await using var uut = Connect(stream);
        var subscribe = await uut.Send(new CommandSubscribe { Topic = "persistent://public/default/topic", Subscription = "sub" }, Substitute.For<IChannel>(), Current.CancellationToken);
        var ack = new CommandAck { ConsumerId = subscribe.ConsumerId, AckType = CommandAck.Types.AckType.Individual };

        //Act
        var response = await uut.SendWithResponse(ack, Current.CancellationToken);

        //Assert
        response.Type.ShouldBe(BaseCommand.Types.Type.AckResponse);
        var sent = stream.Sent.Single(c => c.Type == BaseCommand.Types.Type.Ack);
        sent.Ack.HasRequestId.ShouldBeTrue();
        sent.Ack.RequestId.ShouldBe(response.AckResponse.RequestId);
    }

    [Fact]
    public async Task SendWithResponse_GivenCancelledToken_ShouldCancelAndIgnoreLateResponse()
    {
        //Arrange
        var stream = new FakePulsarStream(command => command.Type == BaseCommand.Types.Type.Subscribe
            ? new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = command.Subscribe.RequestId } }
            : null);
        await using var uut = Connect(stream);
        var subscribe = await uut.Send(new CommandSubscribe { Topic = "persistent://public/default/topic", Subscription = "sub" }, Substitute.For<IChannel>(), Current.CancellationToken);
        var ack = new CommandAck { ConsumerId = subscribe.ConsumerId, AckType = CommandAck.Types.AckType.Individual };
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.SendWithResponse(ack, cts.Token));
        stream.Push(new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse { ConsumerId = subscribe.ConsumerId, RequestId = ack.RequestId } });

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        await Task.Delay(100, Current.CancellationToken);
        uut.State.IsFinalState().ShouldBeFalse();
    }

    [Fact]
    public async Task SendWithResponse_GivenConsumerThatIsNotRegistered_ShouldThrowOperationCanceledException()
    {
        //Arrange
        await using var uut = Connect(new FakePulsarStream());

        //Act
        var exception = await Record.ExceptionAsync(() => uut.SendWithResponse(new CommandAck { ConsumerId = 0 }, Current.CancellationToken));

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
    }

    [Fact]
    public async Task Connection_GivenAuthChallenge_ShouldRespondWithProtocolVersionAndClientVersion()
    {
        //Arrange
        var stream = new FakePulsarStream();
        await using var uut = Connect(stream);

        //Act
        stream.Push(new BaseCommand { Type = BaseCommand.Types.Type.AuthChallenge, AuthChallenge = new CommandAuthChallenge() });
        var response = await WaitFor(() => stream.Sent.FirstOrDefault(c => c.Type == BaseCommand.Types.Type.AuthResponse));

        //Assert
        response.AuthResponse.ProtocolVersion.ShouldBe(Constants.ProtocolVersion);
        response.AuthResponse.ClientVersion.ShouldBe($"{Constants.ClientName} {Constants.ClientVersion}");
    }

    [Fact]
    public async Task AddTransactionMetaStoreHandler_GivenHandler_ShouldNotCloseConnectionOnInactivity()
    {
        //Arrange
        await using var uut = Connect(new FakePulsarStream(), TimeSpan.FromMilliseconds(200));
        var handler = Substitute.For<ITransactionMetaStoreHandler>();

        //Act
        _ = uut.AddTransactionMetaStoreHandler(handler);
        await Task.Delay(TimeSpan.FromMilliseconds(800), Current.CancellationToken);

        //Assert
        uut.State.IsFinalState().ShouldBeFalse();
    }

    [Fact]
    public async Task RemoveTransactionMetaStoreHandler_GivenLastHandler_ShouldCloseConnectionOnInactivity()
    {
        //Arrange
        await using var uut = Connect(new FakePulsarStream(), TimeSpan.FromMilliseconds(200));
        var handlerId = uut.AddTransactionMetaStoreHandler(Substitute.For<ITransactionMetaStoreHandler>());

        //Act
        uut.RemoveTransactionMetaStoreHandler(handlerId);
        var state = await uut.State.OnStateChangeTo(ConnectionState.Disconnected, Current.CancellationToken);

        //Assert
        state.ShouldBe(ConnectionState.Disconnected);
    }

    [Fact]
    public async Task DisposeAsync_GivenTransactionMetaStoreHandler_ShouldNotifyConnectionClosed()
    {
        //Arrange
        var uut = Connect(new FakePulsarStream());
        var handler = Substitute.For<ITransactionMetaStoreHandler>();
        _ = uut.AddTransactionMetaStoreHandler(handler);

        //Act
        await uut.DisposeAsync();

        //Assert
        handler.Received(1).ConnectionClosed();
    }

    [Fact]
    public async Task AddTransactionMetaStoreHandler_GivenDisposedConnection_ShouldThrowConnectionDisposedException()
    {
        //Arrange
        var uut = Connect(new FakePulsarStream());
        await uut.DisposeAsync();

        //Act
        var exception = Record.Exception(() => uut.AddTransactionMetaStoreHandler(Substitute.For<ITransactionMetaStoreHandler>()));

        //Assert
        exception.ShouldBeOfType<ConnectionDisposedException>();
    }

    // Upstream: ProducerImpl#getMessageSizeError
    [Fact]
    public async Task Send_GivenMessageLargerThanMaxMessageSize_ShouldThrowTooLargeMessageExceptionAndNotWriteIt()
    {
        //Arrange
        var stream = CreateStreamOfProducer();
        await using var uut = Connect(stream);
        uut.MaxMessageSize = 200;
        var package = await CreateSendPackage(uut, 300);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(package, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        var tooLarge = exception.ShouldBeOfType<TooLargeMessageException>();
        tooLarge.MaxMessageSize.ShouldBe(200);
        tooLarge.MessageSize.ShouldBeGreaterThan(300);
        stream.Sent.ShouldNotContain(command => command.Type == BaseCommand.Types.Type.Send);
    }

    [Fact]
    public async Task Send_GivenMessageThatFitsInMaxMessageSize_ShouldWriteIt()
    {
        //Arrange
        var stream = CreateStreamOfProducer();
        await using var uut = Connect(stream);
        uut.MaxMessageSize = 1000;
        var package = await CreateSendPackage(uut, 300);

        //Act
        await uut.Send(package, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        stream.Sent.ShouldContain(command => command.Type == BaseCommand.Types.Type.Send);
    }

    // Upstream: ProducerImpl#getMessageSizeError (it is not checked when chunking is enabled)
    [Fact]
    public async Task Send_GivenPackageThatSkipsTheSizeCheck_ShouldWriteMessageLargerThanMaxMessageSize()
    {
        //Arrange
        var stream = CreateStreamOfProducer();
        await using var uut = Connect(stream);
        uut.MaxMessageSize = 200;
        var package = await CreateSendPackage(uut, 300);
        package.SkipSizeCheck = true;

        //Act
        await uut.Send(package, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        stream.Sent.ShouldContain(command => command.Type == BaseCommand.Types.Type.Send);
    }

    private static FakePulsarStream CreateStreamOfProducer()
        => new(command => command.Type == BaseCommand.Types.Type.Producer
            ? new BaseCommand { Type = BaseCommand.Types.Type.ProducerSuccess, ProducerSuccess = new CommandProducerSuccess { RequestId = command.Producer.RequestId, ProducerName = "producer" } }
            : null);

    // The producer is registered with the connection, because a message is only sent for a producer that the connection knows
    private static async Task<SendPackage> CreateSendPackage(Connection connection, int payloadLength)
    {
        var channel = Substitute.For<IChannel>();
        channel.SenderLock().Returns(Substitute.For<IDisposable>());
        var command = new CommandProducer { Topic = "persistent://public/default/test" };
        _ = await connection.Send(command, channel, Current.CancellationToken);

        return new SendPackage
        {
            Command = new CommandSend { ProducerId = command.ProducerId, SequenceId = 1, NumMessages = 1 },
            Metadata = new MessageMetadata { SequenceId = 1, ProducerName = "producer", PublishTime = 1 },
            Payload = new ReadOnlySequence<byte>(new byte[payloadLength])
        };
    }

    private static Connection Connect(IPulsarStream stream, TimeSpan? closeOnInactiveInterval = null)
        => Connection.Connect(stream, null, _keepAliveInterval, closeOnInactiveInterval ?? TimeSpan.FromMinutes(5));

    private static async Task<BaseCommand> WaitFor(Func<BaseCommand?> find)
    {
        var timeout = DateTime.UtcNow.AddSeconds(10);

        while (DateTime.UtcNow < timeout)
        {
            var command = find();
            if (command is not null)
                return command;

            await Task.Delay(10, Current.CancellationToken);
        }

        throw new TimeoutException("The expected command was never sent");
    }
}
