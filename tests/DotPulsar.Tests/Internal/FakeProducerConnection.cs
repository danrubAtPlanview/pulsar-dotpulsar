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
using System.Collections.Concurrent;

// A connection and a connection pool that accept producers and messages, so a producer can be tested without a broker.
public sealed class FakeProducerConnection
{
    private readonly object _lock = new();
    private readonly ConcurrentQueue<SentMessage> _sent;
    private readonly ConcurrentQueue<CommandProducer> _commandProducers;
    private readonly Queue<(SentMessage Message, TaskCompletionSource<BaseCommand> Response)> _heldResponses;
    private readonly uint _numberOfPartitions;
    private ulong _nextProducerId;
    private ulong _nextEntryId;
    private bool _holdResponses;

    public FakeProducerConnection(uint numberOfPartitions = 0)
    {
        _sent = new ConcurrentQueue<SentMessage>();
        _commandProducers = new ConcurrentQueue<CommandProducer>();
        _heldResponses = new Queue<(SentMessage, TaskCompletionSource<BaseCommand>)>();
        _numberOfPartitions = numberOfPartitions;
        _nextProducerId = 0;
        _nextEntryId = 0;
        _holdResponses = false;

        Connection = Substitute.For<IConnection>();
        Connection.Send(Arg.Any<CommandProducer>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo => CreateProducer(callInfo.Arg<CommandProducer>(), callInfo.Arg<IChannel>()));
        Connection.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()).Returns(callInfo => Write(callInfo.Arg<SendPackage>(), callInfo.Arg<TaskCompletionSource<BaseCommand>>()));

        Pool = Substitute.For<IConnectionPool>();
        Pool.GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => GetNumberOfPartitions());
        Pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => new ValueTask<IConnection>(Connection));
    }

    public IConnection Connection { get; }
    public IConnectionPool Pool { get; }

    // The task the connection pool awaits before it tells the number of partitions. Null means that it does not wait.
    public Task? NumberOfPartitionsGate { get; set; }

    // Decides how a message is answered. By default every message gets a receipt.
    public Func<SentMessage, BaseCommand?>? Respond { get; set; }

    public IReadOnlyList<SentMessage> Sent => _sent.ToArray();
    public IReadOnlyList<CommandProducer> CommandProducers => _commandProducers.ToArray();

    // The responses of the messages written from now on are not delivered until ReleaseResponses is called.
    public void HoldResponses()
    {
        lock (_lock)
        {
            _holdResponses = true;
        }
    }

    public void ReleaseResponses()
    {
        (SentMessage Message, TaskCompletionSource<BaseCommand> Response)[] held;

        lock (_lock)
        {
            _holdResponses = false;
            held = _heldResponses.ToArray();
            _heldResponses.Clear();
        }

        foreach (var (message, response) in held)
            response.TrySetResult(CreateResponse(message));
    }

    public static BaseCommand CreateReceipt(SentMessage message, ulong entryId)
        => new()
        {
            Type = BaseCommand.Types.Type.SendReceipt,
            SendReceipt = new CommandSendReceipt
            {
                ProducerId = message.ProducerId,
                SequenceId = message.SequenceId,
                MessageId = new MessageIdData { LedgerId = 1, EntryId = entryId }
            }
        };

    public static BaseCommand CreateSendError(SentMessage message, ServerError error, string errorMessage)
        => new()
        {
            Type = BaseCommand.Types.Type.SendError,
            SendError = new CommandSendError
            {
                ProducerId = message.ProducerId,
                SequenceId = message.SequenceId,
                Error = error,
                Message = errorMessage
            }
        };

    private async ValueTask<uint> GetNumberOfPartitions()
    {
        if (NumberOfPartitionsGate is not null)
            await NumberOfPartitionsGate.ConfigureAwait(false);

        return _numberOfPartitions;
    }

    private Task<ProducerResponse> CreateProducer(CommandProducer command, IChannel channel)
    {
        _commandProducers.Enqueue(command.Clone());
        channel.Connected();
        return Task.FromResult(new ProducerResponse(Interlocked.Increment(ref _nextProducerId), $"producer-{_nextProducerId}", 0));
    }

    private Task Write(SendPackage package, TaskCompletionSource<BaseCommand> response)
    {
        // The package is pooled and reused for the next message, so everything that is checked is copied
        var command = package.Command!;
        var metadata = package.Metadata!;
        var message = new SentMessage(
            command.ProducerId,
            command.SequenceId,
            package,
            command.HasTxnidMostBits,
            command.TxnidMostBits,
            command.HasTxnidLeastBits,
            command.TxnidLeastBits,
            metadata.HasTxnidMostBits,
            metadata.TxnidMostBits,
            metadata.HasTxnidLeastBits,
            metadata.TxnidLeastBits)
        {
            NumMessages = command.NumMessages,
            NumMessagesInBatch = metadata.HasNumMessagesInBatch ? metadata.NumMessagesInBatch : 1
        };

        _sent.Enqueue(message);

        lock (_lock)
        {
            if (_holdResponses)
            {
                _heldResponses.Enqueue((message, response));
                return Task.CompletedTask;
            }
        }

        response.TrySetResult(CreateResponse(message));
        return Task.CompletedTask;
    }

    private BaseCommand CreateResponse(SentMessage message)
        => Respond?.Invoke(message) ?? CreateReceipt(message, Interlocked.Increment(ref _nextEntryId));
}

public sealed record SentMessage(
    ulong ProducerId,
    ulong SequenceId,
    SendPackage Package,
    bool CommandHasTxnidMostBits,
    ulong CommandTxnidMostBits,
    bool CommandHasTxnidLeastBits,
    ulong CommandTxnidLeastBits,
    bool MetadataHasTxnidMostBits,
    ulong MetadataTxnidMostBits,
    bool MetadataHasTxnidLeastBits,
    ulong MetadataTxnidLeastBits)
{
    public bool CommandIsTransactional => CommandHasTxnidMostBits && CommandHasTxnidLeastBits;
    public bool MetadataIsTransactional => MetadataHasTxnidMostBits && MetadataHasTxnidLeastBits;

    // The number of messages of the command, and the number of messages in the batch of the metadata. One for a message that is not a batch.
    public int NumMessages { get; init; }
    public int NumMessagesInBatch { get; init; }
}
