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
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

// The ids of consumers and of the handlers of the transaction meta store are small indexes that the client gives out. A command of the broker with another id is ignored: it must neither throw
// in the loop that reads the commands of the connection, nor reach the consumer that has the id after the cast of the id to an int (4294967296 becomes 0).
[Trait("Category", "Unit")]
public sealed class ChannelManagerConsumerIdTests
{
    private const string _topic = "persistent://public/default/test";

    // Additional coverage (followup 0005 item 4)
    [Theory]
    [InlineData(1ul)]
    [InlineData(42ul)]
    [InlineData(4294967296ul)]
    [InlineData(ulong.MaxValue)]
    public async Task Incoming_GivenMessageForConsumerIdThatWasNeverGivenOut_ShouldNotReachTheConsumer(ulong consumerId)
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = Substitute.For<IChannel>();
        _ = await Subscribe(uut, channel);

        //Act
        uut.Incoming(new CommandMessage { ConsumerId = consumerId }, new ReadOnlySequence<byte>());

        //Assert
        channel.DidNotReceive().Received(Arg.Any<MessagePackage>());
    }

    // Additional coverage (followup 0005 item 4)
    [Theory]
    [InlineData(42ul)]
    [InlineData(4294967296ul)]
    public async Task Incoming_GivenCommandsForConsumerIdThatWasNeverGivenOut_ShouldKeepTheConsumer(ulong consumerId)
    {
        //Arrange
        using var uut = new ChannelManager();
        var channel = Substitute.For<IChannel>();
        _ = await Subscribe(uut, channel);

        //Act
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.ReachedEndOfTopic, ReachedEndOfTopic = new CommandReachedEndOfTopic { ConsumerId = consumerId } });
        uut.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.CloseConsumer, CloseConsumer = new CommandCloseConsumer { ConsumerId = consumerId } });

        //Assert
        channel.DidNotReceive().ReachedEndOfTopic();
        channel.DidNotReceive().ClosedByServer();
        uut.Dispose();
        channel.Received(1).Disconnected();
    }

    // Additional coverage (followup 0005 item 4)
    [Theory]
    [InlineData(42ul)]
    [InlineData(4294967296ul)]
    public void RemoveTransactionMetaStoreHandler_GivenIdThatWasNeverGivenOut_ShouldKeepTheHandler(ulong handlerId)
    {
        //Arrange
        using var uut = new ChannelManager();
        var handler = Substitute.For<ITransactionMetaStoreHandler>();
        _ = uut.AddTransactionMetaStoreHandler(handler);

        //Act
        uut.RemoveTransactionMetaStoreHandler(handlerId);

        //Assert
        uut.Dispose();
        handler.Received(1).ConnectionClosed();
    }

    private static async Task<SubscribeResponse> Subscribe(ChannelManager channelManager, IChannel channel)
    {
        var command = new CommandSubscribe { Topic = _topic, Subscription = "subscription" };
        var response = channelManager.Outgoing(command, channel);
        channelManager.Incoming(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess { RequestId = command.RequestId } });
        return await response;
    }
}
