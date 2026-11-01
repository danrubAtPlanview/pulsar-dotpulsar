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
using DotPulsar.Internal.PulsarApi;

// A producer has the same id on every connection, so the channel manager uses the id of the command. The channel that an earlier connection attempt of the producer left on the connection is replaced by the
// channel of the next one, and what happens to the earlier channel afterwards must not reach the new one.
[Trait("Category", "Unit")]
public sealed class ChannelManagerProducerIdTests
{
    private const string _topic = "persistent://public/default/test";

    // Upstream: ProducerImpl#connectionOpened (cnx.registerProducer(producerId, this))
    [Fact]
    public async Task Outgoing_GivenCommandWithProducerId_ShouldRegisterTheChannelWithThatId()
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = CreateChannel();
        var command = new CommandProducer { Topic = _topic, ProducerId = 1_000_000 };

        //Act
        var response = await Connect(uut, command, channel);

        //Assert
        response.ProducerId.ShouldBe(1_000_000ul);
        command.ProducerId.ShouldBe(1_000_000ul);
        uut.Outgoing(new CommandSend { ProducerId = 1_000_000 }, new TaskCompletionSource<BaseCommand>());
        channel.Received(1).SenderLock();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenCommandsWithoutProducerId_ShouldGiveEveryChannelAnotherId()
    {
        //Arrange
        using var uut = new ChannelManager();
        var first = new CommandProducer { Topic = _topic };
        var second = new CommandProducer { Topic = _topic };

        //Act
        var firstResponse = await Connect(uut, first, CreateChannel());
        var secondResponse = await Connect(uut, second, CreateChannel());

        //Assert
        firstResponse.ProducerId.ShouldBe(0ul);
        secondResponse.ProducerId.ShouldBe(1ul);
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenCommandsWithProducerIds_ShouldKeepTheChannelsApart()
    {
        //Arrange
        using var uut = new ChannelManager();
        var first = CreateChannel();
        var second = CreateChannel();
        var third = CreateChannel();
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 7 }, first);
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 8 }, second);
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 4_000_000_000 }, third);

        //Act
        uut.Outgoing(new CommandSend { ProducerId = 8 }, new TaskCompletionSource<BaseCommand>());
        uut.Outgoing(new CommandSend { ProducerId = 4_000_000_000 }, new TaskCompletionSource<BaseCommand>());

        //Assert
        first.DidNotReceive().SenderLock();
        second.Received(1).SenderLock();
        third.Received(1).SenderLock();
    }

    // Upstream: ProducerImpl#connectionOpened (cnx.registerProducer(producerId, this))
    [Fact]
    public async Task Outgoing_GivenProducerIdOfChannelFromEarlierAttempt_ShouldReplaceTheChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var earlier = CreateChannel();
        var later = CreateChannel();
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12 }, earlier);

        //Act
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12, Epoch = 1 }, later);
        uut.Outgoing(new CommandSend { ProducerId = 12 }, new TaskCompletionSource<BaseCommand>());

        //Assert
        earlier.DidNotReceive().SenderLock();
        later.Received(1).SenderLock();
        earlier.DidNotReceive().Disconnected();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenErrorForTheEarlierChannelAfterTheProducerConnectedAgain_ShouldKeepTheNewChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var earlier = CreateChannel();
        var later = CreateChannel();
        var earlierCommand = new CommandProducer { Topic = _topic, ProducerId = 12 };
        var earlierResponse = uut.Outgoing(earlierCommand, earlier);
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12, Epoch = 1 }, later);

        //Act
        uut.Incoming(CreateError(earlierCommand.RequestId, ServerError.ProducerBusy));
        var exception = await Record.ExceptionAsync(() => earlierResponse);
        uut.Outgoing(new CommandSend { ProducerId = 12 }, new TaskCompletionSource<BaseCommand>());

        //Assert
        exception.ShouldBeOfType<ProducerBusyException>();
        later.Received(1).SenderLock();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenErrorForTheChannelThatHasTheId_ShouldRemoveTheChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = CreateChannel();
        var command = new CommandProducer { Topic = _topic, ProducerId = 12 };
        var response = uut.Outgoing(command, channel);

        //Act
        uut.Incoming(CreateError(command.RequestId, ServerError.ProducerBusy));
        _ = await Record.ExceptionAsync(() => response);
        var exception = Record.Exception(() => uut.Outgoing(new CommandSend { ProducerId = 12 }, new TaskCompletionSource<BaseCommand>()));

        //Assert
        exception.ShouldBeOfType<OperationCanceledException>();
        channel.DidNotReceive().SenderLock();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenCloseProducerThatIsAnsweredAfterTheProducerConnectedAgain_ShouldKeepTheNewChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var earlier = CreateChannel();
        var later = CreateChannel();
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12 }, earlier);
        var close = new CommandCloseProducer { ProducerId = 12 };
        var closeResponse = uut.Outgoing(close);
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12, Epoch = 1 }, later);

        //Act
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = close.RequestId } });
        _ = await closeResponse;
        await Task.Delay(200, Current.CancellationToken); // The channel would be removed by a continuation of the response, so a test that gives it time can only miss a wrong removal, never fail by chance
        uut.Outgoing(new CommandSend { ProducerId = 12 }, new TaskCompletionSource<BaseCommand>());

        //Assert
        later.Received(1).SenderLock();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenCloseProducerThatIsAnswered_ShouldRemoveTheChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = CreateChannel();
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12 }, channel);
        var close = new CommandCloseProducer { ProducerId = 12 };
        var closeResponse = uut.Outgoing(close);
        var inactive = uut.State.OnStateChangeTo(ChannelManagerState.Inactive, Current.CancellationToken).AsTask();

        //Act
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = close.RequestId } });
        _ = await closeResponse;
        var state = await inactive.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken); // The channel was the only one, so the connection is inactive when it is removed

        //Assert
        state.ShouldBe(ChannelManagerState.Inactive);
        Record.Exception(() => uut.Outgoing(new CommandSend { ProducerId = 12 }, new TaskCompletionSource<BaseCommand>())).ShouldBeOfType<OperationCanceledException>();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenAdditionalResponseThatFailsForTheEarlierChannelAfterTheProducerConnectedAgain_ShouldNotDisconnectTheEarlierChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var earlier = CreateChannel();
        var later = CreateChannel();
        var earlierCommand = new CommandProducer { Topic = _topic, ProducerId = 12 };
        var earlierResponse = uut.Outgoing(earlierCommand, earlier);
        uut.Incoming(CreateProducerSuccess(earlierCommand.RequestId, ready: false)); // The producer waits for the exclusive access
        _ = await earlierResponse;
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 12, Epoch = 1 }, later);

        //Act
        uut.Incoming(CreateError(earlierCommand.RequestId, ServerError.ProducerFenced));
        await Task.Delay(200, Current.CancellationToken); // The channel would be disconnected by a continuation of the response, so a test that gives it time can only miss a wrong disconnect, never fail by chance

        //Assert
        earlier.Received(1).WaitingForExclusive();
        earlier.DidNotReceive().Disconnected();
        later.DidNotReceive().Disconnected();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenAdditionalResponseThatFailsForTheChannelThatHasTheId_ShouldDisconnectTheChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = CreateChannel();
        var command = new CommandProducer { Topic = _topic, ProducerId = 12 };
        var response = uut.Outgoing(command, channel);
        uut.Incoming(CreateProducerSuccess(command.RequestId, ready: false));
        _ = await response;

        //Act
        uut.Incoming(CreateError(command.RequestId, ServerError.ProducerFenced));
        await WaitUntil(() => channel.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(IChannel.Disconnected)));

        //Assert
        channel.Received(1).Disconnected();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenAdditionalResponseThatIsReady_ShouldConnectTheChannel()
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = CreateChannel();
        var command = new CommandProducer { Topic = _topic, ProducerId = 12 };
        var response = uut.Outgoing(command, channel);
        uut.Incoming(CreateProducerSuccess(command.RequestId, ready: false));
        _ = await response;

        //Act
        uut.Incoming(CreateProducerSuccess(command.RequestId, ready: true));
        await WaitUntil(() => channel.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(IChannel.Connected)));

        //Assert
        channel.Received(1).Connected();
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenCloseProducerForProducerId_ShouldRemoveTheChannelAndTellItThatTheServerClosedIt()
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = CreateChannel();
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 1_000_000 }, channel);

        //Act
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.CloseProducer, CloseProducer = new CommandCloseProducer { ProducerId = 1_000_000, RequestId = 99 } });

        //Assert
        channel.Received(1).ClosedByServer();
        Record.Exception(() => uut.Outgoing(new CommandSend { ProducerId = 1_000_000 }, new TaskCompletionSource<BaseCommand>())).ShouldBeOfType<OperationCanceledException>();
    }

    // Additional coverage
    [Fact]
    public void Incoming_GivenCloseProducerForUnknownProducerId_ShouldNotThrow()
    {
        //Arrange
        using var uut = new ChannelManager();

        //Act
        var exception = Record.Exception(() => uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.CloseProducer, CloseProducer = new CommandCloseProducer { ProducerId = 1_000_000, RequestId = 99 } }));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task Dispose_GivenChannelsWithProducerIds_ShouldDisconnectEveryChannel()
    {
        //Arrange
        var uut = new ChannelManager();
        var first = CreateChannel();
        var second = CreateChannel();
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 3 }, first);
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 1_000_000 }, second);

        //Act
        uut.Dispose();

        //Assert
        first.Received(1).Disconnected();
        second.Received(1).Disconnected();
    }

    // Additional coverage
    [Fact]
    public async Task State_GivenChannelWithProducerId_ShouldBeActiveUntilTheChannelIsRemoved()
    {
        //Arrange
        using var uut = new ChannelManager();
        var active = uut.State.OnStateChangeTo(ChannelManagerState.Active, Current.CancellationToken).AsTask();

        //Act
        _ = await Connect(uut, new CommandProducer { Topic = _topic, ProducerId = 1_000_000 }, CreateChannel());
        var activeState = await active.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);
        var close = new CommandCloseProducer { ProducerId = 1_000_000 };
        var closeResponse = uut.Outgoing(close);
        var inactive = uut.State.OnStateChangeTo(ChannelManagerState.Inactive, Current.CancellationToken).AsTask();
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = close.RequestId } });
        _ = await closeResponse;
        var inactiveState = await inactive.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        activeState.ShouldBe(ChannelManagerState.Active);
        inactiveState.ShouldBe(ChannelManagerState.Inactive);
    }

    private static IChannel CreateChannel()
    {
        var channel = Substitute.For<IChannel>();
        channel.SenderLock().Returns(Substitute.For<IDisposable>());
        return channel;
    }

    // The broker answers the command, and the channel manager answers the caller when it has done what it does with the answer
    private static async Task<ProducerResponse> Connect(ChannelManager channelManager, CommandProducer command, IChannel channel)
    {
        var response = channelManager.Outgoing(command, channel);
        channelManager.Incoming(CreateProducerSuccess(command.RequestId));
        return await response;
    }

    private static BaseCommand CreateProducerSuccess(ulong requestId, bool ready = true)
        => new() { Type = BaseCommand.Types.Type.ProducerSuccess, ProducerSuccess = new CommandProducerSuccess { RequestId = requestId, ProducerName = "producer", ProducerReady = ready } };

    private static BaseCommand CreateError(ulong requestId, ServerError error)
        => new() { Type = BaseCommand.Types.Type.Error, Error = new CommandError { RequestId = requestId, Error = error, Message = "error" } };

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }

        condition().ShouldBeTrue();
    }
}
