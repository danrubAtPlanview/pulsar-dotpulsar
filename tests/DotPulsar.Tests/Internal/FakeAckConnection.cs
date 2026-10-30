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

using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;

// A connection that answers every acknowledgment, so that the allocations of a consumer and of its channel can be measured. A mocked connection records its calls, which allocates.
// The receipt arrives at once, or on the thread pool as the receipt of a broker does. The latter allocates a task completion source for every receipt, which is what the real connection needs too.
public sealed class FakeAckConnection : IConnection
{
    private static readonly BaseCommand _ackResponse = new()
    {
        Type = BaseCommand.Types.Type.AckResponse,
        AckResponse = new CommandAckResponse { ConsumerId = 1, RequestId = 1 }
    };

    private static readonly Task<BaseCommand> _receipt = Task.FromResult(_ackResponse);

    private readonly StateManager<ConnectionState> _state = new(ConnectionState.Connected, ConnectionState.Disconnected, ConnectionState.Closed);
    private readonly bool _completeOnThreadPool;

    public FakeAckConnection(bool completeOnThreadPool = false)
    {
        _completeOnThreadPool = completeOnThreadPool;
    }

    public IState<ConnectionState> State => _state;
    public int MaxMessageSize => int.MaxValue;
    public int ProtocolVersion => 19;

    public Task<BaseCommand> SendWithResponse(CommandAck command, CancellationToken cancellationToken)
    {
        if (!_completeOnThreadPool)
            return _receipt;

        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = ThreadPool.UnsafeQueueUserWorkItem(static state => state.TrySetResult(_ackResponse), tcs, false);
        return tcs.Task;
    }

    public Task Send(CommandAck command, CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync() => default;

    public Task<ProducerResponse> Send(CommandProducer command, IChannel channel, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<SubscribeResponse> Send(CommandSubscribe command, IChannel channel, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandPing command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandPong command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandFlow command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task Send(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandUnsubscribe command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandConnect command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandLookupTopic command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandSeek command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandGetLastMessageId command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandCloseProducer command, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task<BaseCommand> Send(CommandCloseConsumer command, CancellationToken cancellationToken) => throw new NotSupportedException();
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
