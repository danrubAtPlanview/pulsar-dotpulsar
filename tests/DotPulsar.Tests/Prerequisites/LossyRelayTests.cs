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

namespace DotPulsar.Tests.Prerequisites;

using System.Net;
using System.Net.Sockets;

[Trait("Category", "Unit")]
public sealed class LossyRelayTests
{
    // Additional coverage
    [Fact]
    public async Task DropClientConnections_GivenConnection_ShouldCloseTheSideOfTheClientAndKeepTheSideOfTheBrokerOpen()
    {
        //Arrange
        using var broker = new TcpListener(IPAddress.Loopback, 0);
        broker.Start();
        await using var relay = await LossyRelay.Start(new Uri($"pulsar://127.0.0.1:{((IPEndPoint) broker.LocalEndpoint).Port}"), Current.CancellationToken);
        using var client = new TcpClient();
        await client.ConnectAsync(relay.ServiceUrl.Host, relay.ServiceUrl.Port, Current.CancellationToken);
        using var brokerSide = await broker.AcceptTcpClientAsync(Current.CancellationToken);
        var brokerRead = brokerSide.GetStream().ReadAsync(new byte[1], Current.CancellationToken).AsTask();

        //Act
        relay.DropClientConnections();

        //Assert
        var read = await Record.ExceptionAsync(async () => (await client.GetStream().ReadAsync(new byte[1], Current.CancellationToken)).ShouldBe(0, "the client sees the connection end"));
        (read is null or IOException or ObjectDisposedException or SocketException).ShouldBeTrue();
        var brokerSideIsOpen = await Task.WhenAny(brokerRead, Task.Delay(TimeSpan.FromMilliseconds(500), Current.CancellationToken)) != brokerRead;
        brokerSideIsOpen.ShouldBeTrue("the broker does not see the connection end");
        await brokerSide.GetStream().WriteAsync(new byte[] { 1 }, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task DropClientConnections_GivenConnectionAfterTheDrop_ShouldRelayBothWays()
    {
        //Arrange
        using var broker = new TcpListener(IPAddress.Loopback, 0);
        broker.Start();
        await using var relay = await LossyRelay.Start(new Uri($"pulsar://127.0.0.1:{((IPEndPoint) broker.LocalEndpoint).Port}"), Current.CancellationToken);
        using var first = new TcpClient();
        await first.ConnectAsync(relay.ServiceUrl.Host, relay.ServiceUrl.Port, Current.CancellationToken);
        using var firstBrokerSide = await broker.AcceptTcpClientAsync(Current.CancellationToken);
        relay.DropClientConnections();
        using var second = new TcpClient();
        await second.ConnectAsync(relay.ServiceUrl.Host, relay.ServiceUrl.Port, Current.CancellationToken);
        using var secondBrokerSide = await broker.AcceptTcpClientAsync(Current.CancellationToken);

        //Act
        await second.GetStream().WriteAsync(new byte[] { 1, 2 }, Current.CancellationToken);
        await secondBrokerSide.GetStream().WriteAsync(new byte[] { 3, 4 }, Current.CancellationToken);

        //Assert
        var atBroker = new byte[2];
        await secondBrokerSide.GetStream().ReadExactlyAsync(atBroker, Current.CancellationToken);
        var atClient = new byte[2];
        await second.GetStream().ReadExactlyAsync(atClient, Current.CancellationToken);
        atBroker.ShouldBe([1, 2]);
        atClient.ShouldBe([3, 4]);
    }
}
