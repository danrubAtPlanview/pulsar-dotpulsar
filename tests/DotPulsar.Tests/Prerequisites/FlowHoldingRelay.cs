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

using DotPulsar.Internal.PulsarApi;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

// A TCP relay between a client and a broker that can hold back the flow commands of the client. It stands in for consumer.pause() of the Java client, which stops the consumer from asking
// the broker for more messages. The relay reads the frames of the client, and after HoldFlowsAfter(n) it passes the next n flow commands and keeps the ones that follow, until ReleaseFlows()
// sends them. Everything else that the client sends, and everything that the broker sends, is passed as it is.
public sealed class FlowHoldingRelay : IAsyncDisposable
{
    private const int FrameSizeFieldLength = 4;
    private const int CommandSizeFieldLength = 4;

    private readonly TcpListener _listener;
    private readonly Uri _brokerServiceUrl;
    private readonly CancellationTokenSource _cts;
    private readonly List<TcpClient> _sockets;
    private readonly List<Task> _tasks;
    private readonly List<(byte[] Frame, Connection Connection)> _held;
    private readonly object _lock;
    private Task? _acceptTask;
    private bool _isHolding;
    private int _flowsToPass;

    private FlowHoldingRelay(Uri brokerServiceUrl)
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _brokerServiceUrl = brokerServiceUrl;
        _cts = new CancellationTokenSource();
        _sockets = [];
        _tasks = [];
        _held = [];
        _lock = new();
        _isHolding = false;
        _flowsToPass = 0;
    }

    public Uri ServiceUrl => new($"pulsar://127.0.0.1:{((IPEndPoint) _listener.LocalEndpoint).Port}");

    // The number of flow commands that are held back
    public int HeldFlows
    {
        get
        {
            lock (_lock)
                return _held.Count;
        }
    }

    public static Task<FlowHoldingRelay> Start(Uri brokerServiceUrl, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var relay = new FlowHoldingRelay(brokerServiceUrl);
        relay._listener.Start();
        relay._acceptTask = Task.Run(() => relay.Accept(relay._cts.Token), CancellationToken.None);
        return Task.FromResult(relay);
    }

    // The next 'numberOfFlows' flow commands of the client are passed, and the ones after those are held back
    public void HoldFlowsAfter(int numberOfFlows)
    {
        lock (_lock)
        {
            _isHolding = true;
            _flowsToPass = numberOfFlows;
        }
    }

    // Sends the flow commands that were held back, in the order that the client sent them, and passes the flow commands that follow
    public async Task ReleaseFlows(CancellationToken cancellationToken)
    {
        (byte[] Frame, Connection Connection)[] held;

        lock (_lock)
        {
            _isHolding = false;
            held = _held.ToArray();
            _held.Clear();
        }

        foreach (var (frame, connection) in held)
        {
            await connection.Write(frame, cancellationToken);
        }
    }

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

            _sockets.Clear();
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
            var toBroker = new Connection(broker.GetStream());

            lock (_sockets)
            {
                _sockets.Add(client);
                _sockets.Add(broker);
                _tasks.Add(PumpFromClient(client, toBroker, cancellationToken));
                _tasks.Add(PumpFromBroker(broker, client, cancellationToken));
            }
        }
    }

    private async Task PumpFromBroker(TcpClient broker, TcpClient client, CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];

        try
        {
            var source = broker.GetStream();
            var destination = client.GetStream();

            while (!cancellationToken.IsCancellationRequested)
            {
                var read = await source.ReadAsync(buffer, cancellationToken);

                if (read == 0)
                    return;

                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            }
        }
        catch
        {
            // The sockets are closed when the relay is disposed
        }
    }

    private async Task PumpFromClient(TcpClient client, Connection toBroker, CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];
        var pending = new List<byte>();

        try
        {
            var source = client.GetStream();

            while (!cancellationToken.IsCancellationRequested)
            {
                var read = await source.ReadAsync(buffer, cancellationToken);

                if (read == 0)
                    return;

                for (var i = 0; i < read; ++i)
                {
                    pending.Add(buffer[i]);
                }

                while (TryTakeFrame(pending, out var frame))
                {
                    if (IsFlow(frame) && ShouldHold(frame, toBroker))
                        continue;

                    await toBroker.Write(frame, cancellationToken);
                }
            }
        }
        catch
        {
            // The sockets are closed when the relay is disposed
        }
    }

    private bool ShouldHold(byte[] frame, Connection connection)
    {
        lock (_lock)
        {
            if (!_isHolding)
                return false;

            if (_flowsToPass > 0)
            {
                --_flowsToPass;
                return false;
            }

            _held.Add((frame, connection));
            return true;
        }
    }

    // A frame is the size of the frame (not counting this field), the size of the command, and the command, and it may be followed by a payload
    private static bool TryTakeFrame(List<byte> pending, out byte[] frame)
    {
        frame = [];

        if (pending.Count < FrameSizeFieldLength)
            return false;

        Span<byte> size = stackalloc byte[FrameSizeFieldLength];
        for (var i = 0; i < FrameSizeFieldLength; ++i)
        {
            size[i] = pending[i];
        }

        var frameLength = FrameSizeFieldLength + (int) BinaryPrimitives.ReadUInt32BigEndian(size);

        if (pending.Count < frameLength)
            return false;

        frame = pending.GetRange(0, frameLength).ToArray();
        pending.RemoveRange(0, frameLength);
        return true;
    }

    private static bool IsFlow(byte[] frame)
    {
        var commandSize = (int) BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(FrameSizeFieldLength, CommandSizeFieldLength));
        return BaseCommand.Parser.ParseFrom(frame, FrameSizeFieldLength + CommandSizeFieldLength, commandSize).Type == BaseCommand.Types.Type.Flow;
    }

    // The writes of the pump and of ReleaseFlows are not mixed
    private sealed class Connection
    {
        private readonly Stream _stream;
        private readonly SemaphoreSlim _writeLock;

        public Connection(Stream stream)
        {
            _stream = stream;
            _writeLock = new SemaphoreSlim(1, 1);
        }

        public async Task Write(byte[] frame, CancellationToken cancellationToken)
        {
            await _writeLock.WaitAsync(cancellationToken);

            try
            {
                await _stream.WriteAsync(frame, cancellationToken);
            }
            finally
            {
                _ = _writeLock.Release();
            }
        }
    }
}
