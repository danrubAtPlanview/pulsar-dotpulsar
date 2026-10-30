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
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

// A TCP relay between a client and a broker. After SwallowSendCommands, the relay reads the frames that the client sends and throws away the ones that send a message, but it keeps both sockets open.
// The client then believes that it is connected, and the broker never answers a send. That is what a mocked connection of a producer does in the Java tests: the mock answers every request, so a producer
// can be closed, but a message that is sent is never acknowledged. Everything else that the client sends reaches the broker, so a producer that is closed does not wait for a broker that does not answer.
public sealed class BlackHoleRelay : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Uri _brokerServiceUrl;
    private readonly CancellationTokenSource _cts;
    private readonly List<TcpClient> _sockets;
    private readonly List<Task> _tasks;
    private Task? _acceptTask;
    private volatile bool _swallowSends;

    private BlackHoleRelay(Uri brokerServiceUrl)
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _brokerServiceUrl = brokerServiceUrl;
        _cts = new CancellationTokenSource();
        _sockets = [];
        _tasks = [];
        _swallowSends = false;
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

    // From now on the commands of the client that send a message do not reach the broker. The broker can still send to the client.
    public void SwallowSendCommands()
        => _swallowSends = true;

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
        var frames = new List<byte>();

        try
        {
            var source = from.GetStream();
            var destination = to.GetStream();

            while (!cancellationToken.IsCancellationRequested)
            {
                var read = await source.ReadAsync(buffer, cancellationToken);

                if (read == 0)
                    return;

                if (!isClientToBroker)
                {
                    await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                    continue;
                }

                frames.AddRange(new ArraySegment<byte>(buffer, 0, read));
                await ForwardCompleteFrames(frames, destination, cancellationToken);
            }
        }
        catch
        {
            // The sockets are closed when the relay is disposed
        }
    }

    // A frame is the size of the rest of the frame (4 bytes), the size of the command (4 bytes), the command, and the metadata and the payload of a message
    private async Task ForwardCompleteFrames(List<byte> frames, NetworkStream destination, CancellationToken cancellationToken)
    {
        var offset = 0;

        while (frames.Count - offset >= 8)
        {
            var frameSize = 4 + (int) BinaryPrimitives.ReadUInt32BigEndian(frames.GetRange(offset, 4).ToArray());

            if (frames.Count - offset < frameSize)
                break;

            var frame = frames.GetRange(offset, frameSize).ToArray();
            var commandSize = (int) BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(4, 4));
            var command = BaseCommand.Parser.ParseFrom(frame, 8, commandSize);
            offset += frameSize;

            if (_swallowSends && command.Type == BaseCommand.Types.Type.Send)
                continue;

            await destination.WriteAsync(frame, cancellationToken);
        }

        frames.RemoveRange(0, offset);
    }
}
