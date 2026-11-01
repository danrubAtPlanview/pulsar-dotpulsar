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

// A TCP relay between a client and a broker that can lose what the broker sends. After SwallowBrokerToClient, the relay reads what the broker sends and throws it away, but it keeps both sockets open
// and still delivers what the client sends. The broker acts on what it receives, and the client never gets the answers, like the receipts of a producer. DropConnections closes the sockets, and the
// connections that the client opens after that are normal again. DropClientConnections closes only the sockets towards the client, so the client sees the connection close, and the broker does not:
// like a network that is cut, the broker keeps the connection and the producers and consumers on it until it finds out that the other end is gone.
public sealed class LossyRelay : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Uri _brokerServiceUrl;
    private readonly CancellationTokenSource _cts;
    private readonly List<TcpClient> _sockets;
    private readonly List<TcpClient> _clientSockets;
    private readonly List<Task> _tasks;
    private Task? _acceptTask;
    private volatile bool _swallow;

    private LossyRelay(Uri brokerServiceUrl)
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _brokerServiceUrl = brokerServiceUrl;
        _cts = new CancellationTokenSource();
        _sockets = [];
        _clientSockets = [];
        _tasks = [];
        _swallow = false;
    }

    public Uri ServiceUrl => new($"pulsar://127.0.0.1:{((IPEndPoint) _listener.LocalEndpoint).Port}");

    public static Task<LossyRelay> Start(Uri brokerServiceUrl, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var relay = new LossyRelay(brokerServiceUrl);
        relay._listener.Start();
        relay._acceptTask = Task.Run(() => relay.Accept(relay._cts.Token), CancellationToken.None);
        return Task.FromResult(relay);
    }

    // From now on the broker is read, but what it sends does not reach the client. The client can still send to the broker.
    public void SwallowBrokerToClient()
        => _swallow = true;

    // Closes the connections of the client, and the connections that are opened later are not swallowed
    public void DropConnections()
    {
        _swallow = false;

        lock (_sockets)
        {
            foreach (var socket in _sockets)
            {
                socket.Dispose();
            }

            _sockets.Clear();
            _clientSockets.Clear();
        }
    }

    // Closes the sockets towards the client, and keeps the sockets towards the broker open. What the broker sends on them is not read any more, so it does not get an answer to a ping either.
    // The connections that are opened later are not swallowed.
    public void DropClientConnections()
    {
        _swallow = false;

        lock (_sockets)
        {
            foreach (var socket in _clientSockets)
            {
                socket.Dispose();
            }

            _clientSockets.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
        DropConnections();

        try
        {
            await Task.WhenAll(_tasks.Append(_acceptTask ?? Task.CompletedTask)).WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch
        {
            // The pumps end with an exception when their sockets are closed
        }

        _cts.Dispose();
    }

    private async Task Accept(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;

            try
            {
                client = await _listener.AcceptTcpClientAsync(cancellationToken);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            // The client is known before the broker is connected, so that a drop that follows the accept of the broker cannot miss it
            lock (_sockets)
            {
                _sockets.Add(client);
                _clientSockets.Add(client);
            }

            var broker = new TcpClient();
            await broker.ConnectAsync(_brokerServiceUrl.Host, _brokerServiceUrl.Port, cancellationToken);

            lock (_sockets)
            {
                if (!_sockets.Contains(client))
                {
                    // DropConnections ran in between
                    broker.Dispose();
                    continue;
                }

                _sockets.Add(broker);
                _tasks.Add(Pump(client, broker, false, cancellationToken));
                _tasks.Add(Pump(broker, client, true, cancellationToken));
            }
        }
    }

    private async Task Pump(TcpClient from, TcpClient to, bool isBrokerToClient, CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];

        try
        {
            var source = from.GetStream();
            var destination = to.GetStream();

            while (!cancellationToken.IsCancellationRequested)
            {
                var read = await source.ReadAsync(buffer, cancellationToken);

                if (read == 0)
                    return;

                if (isBrokerToClient && _swallow)
                    continue;

                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            }
        }
        catch
        {
            // The sockets are closed when the connections are dropped
        }
    }
}
