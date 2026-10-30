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
using System.Collections.Concurrent;

/// <summary>
/// An in-memory broker side of a connection. Every command the client sends is recorded and optionally answered.
/// </summary>
public sealed class FakePulsarStream : IPulsarStream
{
    private readonly System.Threading.Channels.Channel<ReadOnlySequence<byte>> _incoming;
    private readonly Func<BaseCommand, BaseCommand?> _respond;
    private readonly ConcurrentQueue<BaseCommand> _sent;
    private readonly ConcurrentQueue<byte[]> _sentFrames;

    public FakePulsarStream(Func<BaseCommand, BaseCommand?>? respond = null)
    {
        _incoming = System.Threading.Channels.Channel.CreateUnbounded<ReadOnlySequence<byte>>();
        _respond = respond ?? (_ => null);
        _sent = new ConcurrentQueue<BaseCommand>();
        _sentFrames = new ConcurrentQueue<byte[]>();
    }

    public IReadOnlyCollection<BaseCommand> Sent => _sent.ToArray();

    // The bytes of every frame that the client sent, including the size of the frame
    public IReadOnlyCollection<byte[]> SentFrames => _sentFrames.ToArray();

    public Task Send(ReadOnlySequence<byte> sequence)
    {
        var bytes = sequence.ToArray();
        _sentFrames.Enqueue(bytes);
        var commandSize = (int) System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(4, 4));
        var command = BaseCommand.Parser.ParseFrom(bytes, 8, commandSize);
        _sent.Enqueue(command);

        var response = _respond(command);
        if (response is not null)
            Push(response);

        return Task.CompletedTask;
    }

    public void Push(BaseCommand command)
    {
        var serialized = Serializer.Serialize(command).ToArray();
        _ = _incoming.Writer.TryWrite(new ReadOnlySequence<byte>(serialized, 4, serialized.Length - 4));
    }

    public async IAsyncEnumerable<ReadOnlySequence<byte>> Frames([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var frame in _incoming.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return frame;
    }

    public ValueTask DisposeAsync()
    {
        _ = _incoming.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
