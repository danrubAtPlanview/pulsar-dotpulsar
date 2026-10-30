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

namespace DotPulsar.Tests.Transactions;

using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

// Additional coverage: the relay is the test double of the Java mock that does not acknowledge a message (TransactionEndToEndTest#testSendTxnMessageTimeout)
[Trait("Category", "Unit")]
public sealed class BlackHoleRelayTests
{
    // Additional coverage
    [Fact]
    public async Task SwallowSendCommands_GivenFramesThatAreCutInPieces_ShouldForwardEveryFrameButTheSendCommands()
    {
        //Arrange
        using var broker = new TcpListener(IPAddress.Loopback, 0);
        broker.Start();
        await using var relay = await BlackHoleRelay.Start(new Uri($"pulsar://127.0.0.1:{((IPEndPoint) broker.LocalEndpoint).Port}"), Current.CancellationToken);
        using var client = new TcpClient();
        await client.ConnectAsync(relay.ServiceUrl.Host, relay.ServiceUrl.Port, Current.CancellationToken);
        using var brokerSide = await broker.AcceptTcpClientAsync(Current.CancellationToken);
        var ping = CreateFrame(new BaseCommand { Type = BaseCommand.Types.Type.Ping, Ping = new CommandPing() });
        var pong = CreateFrame(new BaseCommand { Type = BaseCommand.Types.Type.Pong, Pong = new CommandPong() });
        var send = CreateFrame(new BaseCommand { Type = BaseCommand.Types.Type.Send, Send = new CommandSend { ProducerId = 1, SequenceId = 1 } }, new byte[] { 14, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        var clientStream = client.GetStream();
        var brokerStream = brokerSide.GetStream();
        var forwardedBeforeSwallowing = ping.Concat(send).ToArray();
        var forwardedAfterSwallowing = pong.Concat(ping).ToArray();

        //Act
        await clientStream.WriteAsync(forwardedBeforeSwallowing, Current.CancellationToken);
        var actualBeforeSwallowing = new byte[forwardedBeforeSwallowing.Length];
        await brokerStream.ReadExactlyAsync(actualBeforeSwallowing, Current.CancellationToken);
        relay.SwallowSendCommands();
        await clientStream.WriteAsync(pong.Concat(send.Take(6)).ToArray(), Current.CancellationToken);
        await clientStream.WriteAsync(send.Skip(6).Take(10).ToArray(), Current.CancellationToken);
        await clientStream.WriteAsync(send.Skip(16).Concat(send).Concat(ping).ToArray(), Current.CancellationToken);

        //Assert
        var actualAfterSwallowing = new byte[forwardedAfterSwallowing.Length];
        await brokerStream.ReadExactlyAsync(actualAfterSwallowing, Current.CancellationToken);
        actualBeforeSwallowing.ShouldBe(forwardedBeforeSwallowing);
        actualAfterSwallowing.ShouldBe(forwardedAfterSwallowing);
    }

    // Additional coverage
    [Fact]
    public async Task Pump_GivenBytesFromTheBroker_ShouldForwardThemToTheClientWhileSwallowingSendCommands()
    {
        //Arrange
        using var broker = new TcpListener(IPAddress.Loopback, 0);
        broker.Start();
        await using var relay = await BlackHoleRelay.Start(new Uri($"pulsar://127.0.0.1:{((IPEndPoint) broker.LocalEndpoint).Port}"), Current.CancellationToken);
        using var client = new TcpClient();
        await client.ConnectAsync(relay.ServiceUrl.Host, relay.ServiceUrl.Port, Current.CancellationToken);
        using var brokerSide = await broker.AcceptTcpClientAsync(Current.CancellationToken);
        relay.SwallowSendCommands();
        var receipt = CreateFrame(new BaseCommand { Type = BaseCommand.Types.Type.SendReceipt, SendReceipt = new CommandSendReceipt { ProducerId = 1, SequenceId = 1 } });

        //Act
        await brokerSide.GetStream().WriteAsync(receipt, Current.CancellationToken);

        //Assert
        var actual = new byte[receipt.Length];
        await client.GetStream().ReadExactlyAsync(actual, Current.CancellationToken);
        actual.ShouldBe(receipt);
    }

    // A frame is the size of the rest of the frame, the size of the command, the command and what follows it
    private static byte[] CreateFrame(BaseCommand command, byte[]? afterCommand = null)
    {
        var commandBytes = command.ToByteArray();
        afterCommand ??= [];
        var frame = new byte[8 + commandBytes.Length + afterCommand.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(0, 4), (uint) (4 + commandBytes.Length + afterCommand.Length));
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(4, 4), (uint) commandBytes.Length);
        commandBytes.CopyTo(frame, 8);
        afterCommand.CopyTo(frame, 8 + commandBytes.Length);
        return frame;
    }
}
