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

using System.Net;
using System.Net.Sockets;

// A TCP relay between a client and a broker. After SwallowClientToBroker, the relay reads what the client sends and throws it away, but it keeps both sockets open.
// The client then believes that it is connected, and the broker never answers. That is what a mocked connection of a producer does in the Java tests.
public sealed class BlackHoleRelay : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Uri _brokerServiceUrl;
    private readonly CancellationTokenSource _cts;
    private readonly List<TcpClient> _sockets;
    private readonly List<Task> _tasks;
    private Task? _acceptTask;
    private volatile bool _swallow;

    private BlackHoleRelay(Uri brokerServiceUrl)
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _brokerServiceUrl = brokerServiceUrl;
        _cts = new CancellationTokenSource();
        _sockets = [];
        _tasks = [];
        _swallow = false;
    }

    public Uri ServiceUrl => new($"pulsar://127.0.0.1:{((IPEndPoint) _listener.LocalEndpoint).Port}");

    public static Task<BlackHoleRelay> Start(Uri brokerServiceUrl, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var relay = new BlackHoleRelay(brokerServiceUrl);
        relay._listener.Start();
        relay._acceptTask = Task.Run(() => relay.Accept(relay._cts.Token), CancellationToken.None);
        return Task.FromResult(relay);
    }

    // From now on the client is read, but what it sends does not reach the broker. The broker can still send to the client.
    public void SwallowClientToBroker()
        => _swallow = true;

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();

        lock (_sockets)
        {
            foreach (var socket in _sockets)
            {
                socket.Dispose();
            }
        }

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

            var broker = new TcpClient();
            await broker.ConnectAsync(_brokerServiceUrl.Host, _brokerServiceUrl.Port, cancellationToken);

            lock (_sockets)
            {
                _sockets.Add(client);
                _sockets.Add(broker);
                _tasks.Add(Pump(client, broker, true, cancellationToken));
                _tasks.Add(Pump(broker, client, false, cancellationToken));
            }
        }
    }

    private async Task Pump(TcpClient from, TcpClient to, bool isClientToBroker, CancellationToken cancellationToken)
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

                if (isClientToBroker && _swallow)
                    continue;

                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            }
        }
        catch
        {
            // The sockets are closed when the relay is disposed
        }
    }
}
