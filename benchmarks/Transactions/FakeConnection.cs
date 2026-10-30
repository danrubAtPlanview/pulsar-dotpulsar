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

namespace Transactions;

using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;

// A connection that accepts producers and answers every message with a receipt, so that no broker is needed.
// Nothing but what a producer uses is supported. NSubstitute is not used, because its proxies would allocate and pollute the numbers.
public sealed class FakeConnection : IConnection
{
    private readonly StateManager<ConnectionState> _state;
    private readonly bool _completeOnThreadPool;
    private readonly bool _serializeFrames;

    // The frame of a message is serialized like a connection does it when serializeFrames is true. It is the work that batching saves.
    public FakeConnection(bool completeOnThreadPool, bool serializeFrames = false)
    {
        _state = new StateManager<ConnectionState>(ConnectionState.Connected, ConnectionState.Disconnected, ConnectionState.Closed);
        _completeOnThreadPool = completeOnThreadPool;
        _serializeFrames = serializeFrames;
    }

    public IState<ConnectionState> State => _state;
    public int MaxMessageSize => int.MaxValue;
    public int ProtocolVersion => 19;

    public Task<ProducerResponse> Send(CommandProducer command, IChannel channel, CancellationToken cancellationToken)
    {
        channel.Connected();
        return Task.FromResult(new ProducerResponse(1, "producer", 0));
    }

    public Task Send(SendPackage command, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
    {
        if (_serializeFrames)
            _ = Serializer.Serialize(command.Command!.AsBaseCommand(), command.Metadata!, command.Payload);

        var sequenceId = command.Command!.SequenceId;

        var receipt = new BaseCommand
        {
            Type = BaseCommand.Types.Type.SendReceipt,
            SendReceipt = new CommandSendReceipt
            {
                ProducerId = command.Command.ProducerId,
                SequenceId = sequenceId,
                MessageId = new MessageIdData { LedgerId = 1, EntryId = sequenceId }
            }
        };

        if (_completeOnThreadPool)
            _ = ThreadPool.UnsafeQueueUserWorkItem(static state => state.Tcs.TrySetResult(state.Receipt), (Tcs: responseTcs, Receipt: receipt), false);
        else
            _ = responseTcs.TrySetResult(receipt);

        return Task.CompletedTask;
    }

    public Task<BaseCommand> Send(CommandCloseProducer command, CancellationToken cancellationToken)
        => Task.FromResult(new BaseCommand());

    public ValueTask DisposeAsync()
        => default;

    public Task<SubscribeResponse> Send(CommandSubscribe command, IChannel channel, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandPing command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandPong command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandAck command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandFlow command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandUnsubscribe command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandConnect command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandLookupTopic command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandSeek command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetLastMessageId command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandCloseConsumer command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetOrCreateSchema command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandPartitionedTopicMetadata command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetTopicsOfNamespace command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> SendWithResponse(CommandAck command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandTcClientConnectRequest command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandNewTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandAddPartitionToTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandAddSubscriptionToTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandEndTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public ulong AddTransactionMetaStoreHandler(ITransactionMetaStoreHandler handler) => throw new NotSupportedException();
    public void RemoveTransactionMetaStoreHandler(ulong handlerId) => throw new NotSupportedException();
}
