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
using DotPulsar.Internal.PulsarApi;

// A connection that accepts consumers and answers every acknowledgment, so that no broker is needed.
// Nothing but what a consumer uses to acknowledge is supported. NSubstitute is not used, because its proxies would allocate and pollute the numbers.
// An acknowledgment that asks for a receipt gets one task completion source, which is what the request/response handler of the real connection needs for the awaiter
// (the real connection also adds a dictionary entry and the request key). The fake has no more allocations than that, so the allocated bytes of the benchmarks can be compared.
public sealed class FakeConsumerConnection : IConnection
{
    private static readonly BaseCommand _ackResponse = new()
    {
        Type = BaseCommand.Types.Type.AckResponse,
        AckResponse = new CommandAckResponse { ConsumerId = 1 }
    };

    private readonly StateManager<ConnectionState> _state;
    private readonly bool _completeOnThreadPool;

    public FakeConsumerConnection(bool completeOnThreadPool)
    {
        _state = new StateManager<ConnectionState>(ConnectionState.Connected, ConnectionState.Disconnected, ConnectionState.Closed);
        _completeOnThreadPool = completeOnThreadPool;
    }

    public IState<ConnectionState> State => _state;
    public int MaxMessageSize => int.MaxValue;
    public int ProtocolVersion => 19;

    public Task<SubscribeResponse> Send(CommandSubscribe command, IChannel channel, CancellationToken cancellationToken)
    {
        channel.Connected();
        return Task.FromResult(new SubscribeResponse(1));
    }

    // An acknowledgment that needs no receipt
    public Task Send(CommandAck command, CancellationToken cancellationToken)
        => Task.CompletedTask;

    // An acknowledgment that needs a receipt, completed directly or via the thread pool, as the receipt of the broker arrives on another thread
    public Task<BaseCommand> SendWithResponse(CommandAck command, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);

        if (_completeOnThreadPool)
            _ = ThreadPool.UnsafeQueueUserWorkItem(static state => state.TrySetResult(_ackResponse), tcs, false);
        else
            _ = tcs.TrySetResult(_ackResponse);

        return tcs.Task;
    }

    public Task Send(CommandFlow command, CancellationToken cancellationToken)
        => Task.CompletedTask;

    public Task<BaseCommand> Send(CommandCloseConsumer command, CancellationToken cancellationToken)
        => Task.FromResult(new BaseCommand());

    public ValueTask DisposeAsync()
        => default;

    public Task<ProducerResponse> Send(CommandProducer command, IChannel channel, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandPing command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandPong command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandUnsubscribe command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandConnect command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandLookupTopic command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandSeek command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetLastMessageId command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandCloseProducer command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(SendPackage command, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetOrCreateSchema command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandPartitionedTopicMetadata command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetTopicsOfNamespace command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandTcClientConnectRequest command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandNewTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandAddPartitionToTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandAddSubscriptionToTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandEndTxn command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public ulong AddTransactionMetaStoreHandler(ITransactionMetaStoreHandler handler) => throw new NotSupportedException();
    public void RemoveTransactionMetaStoreHandler(ulong handlerId) => throw new NotSupportedException();
}
