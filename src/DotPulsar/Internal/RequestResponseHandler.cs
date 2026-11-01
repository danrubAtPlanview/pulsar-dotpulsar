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

namespace DotPulsar.Internal;

using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Requests;

public sealed class RequestResponseHandler : IDisposable
{
    private readonly ConnectRequest _connectRequest;
    private readonly RequestId _requestId;
    private readonly Awaiter<IRequest, BaseCommand> _requests;
    private readonly EnumLookup<BaseCommand.Types.Type, Func<BaseCommand, IRequest>> _getResponseIdentifier;
    private readonly Dictionary<(ulong ProducerId, ulong SequenceId), Queue<TaskCompletionSource<BaseCommand>>> _chunkRequests;
    private int _chunkRequestCount;

    public RequestResponseHandler()
    {
        _connectRequest = new ConnectRequest();
        _requestId = new RequestId();
        _requests = new Awaiter<IRequest, BaseCommand>();
        _chunkRequests = [];

        _getResponseIdentifier = new EnumLookup<BaseCommand.Types.Type, Func<BaseCommand, IRequest>>(cmd => null!);
        _getResponseIdentifier.Set(BaseCommand.Types.Type.Connected, cmd => _connectRequest);
        _getResponseIdentifier.Set(BaseCommand.Types.Type.SendError, cmd => new SendRequest(cmd.SendError.ProducerId, cmd.SendError.SequenceId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.SendReceipt, cmd => new SendRequest(cmd.SendReceipt.ProducerId, cmd.SendReceipt.SequenceId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.ProducerSuccess, cmd => StandardRequest.WithRequestId(cmd.ProducerSuccess.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.CloseConsumer, cmd => StandardRequest.WithConsumerId(cmd.CloseConsumer.RequestId, cmd.CloseConsumer.ConsumerId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.CloseProducer, cmd => StandardRequest.WithProducerId(cmd.CloseProducer.RequestId, cmd.CloseProducer.ProducerId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.LookupResponse, cmd => StandardRequest.WithRequestId(cmd.LookupTopicResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.PartitionedMetadataResponse, cmd => StandardRequest.WithRequestId(cmd.PartitionMetadataResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.GetTopicsOfNamespaceResponse, cmd => StandardRequest.WithRequestId(cmd.GetTopicsOfNamespaceResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.GetLastMessageIdResponse, cmd => StandardRequest.WithRequestId(cmd.GetLastMessageIdResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.GetOrCreateSchemaResponse, cmd => StandardRequest.WithRequestId(cmd.GetOrCreateSchemaResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.Success, cmd => StandardRequest.WithRequestId(cmd.Success.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.AckResponse, cmd => cmd.AckResponse.HasRequestId ? StandardRequest.WithRequestId(cmd.AckResponse.RequestId) : null!);
        _getResponseIdentifier.Set(BaseCommand.Types.Type.TcClientConnectResponse, cmd => StandardRequest.WithRequestId(cmd.TcClientConnectResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.NewTxnResponse, cmd => StandardRequest.WithRequestId(cmd.NewTxnResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.AddPartitionToTxnResponse, cmd => StandardRequest.WithRequestId(cmd.AddPartitionToTxnResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.AddSubscriptionToTxnResponse, cmd => StandardRequest.WithRequestId(cmd.AddSubscriptionToTxnResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.EndTxnResponse, cmd => StandardRequest.WithRequestId(cmd.EndTxnResponse.RequestId));
        _getResponseIdentifier.Set(BaseCommand.Types.Type.Error, cmd => _requestId.IsPastInitialId() ? StandardRequest.WithRequestId(cmd.Error.RequestId) : _connectRequest);
    }

    public void Dispose()
    {
        _requests.Dispose();
        CancelChunkRequests(null);
    }

    public Task<BaseCommand> Outgoing(CommandProducer command)
    {
        // The chunks are matched to the answers of the broker by their order. A producer has the same id on every connection, so when it registers again the chunk requests that this connection still holds for its id
        // belong to an earlier channel of it. The broker has answered them, or it never will (it replaced or closed the producer), and one that is left would take the answer to a chunk of the next channel.
        if (Volatile.Read(ref _chunkRequestCount) != 0)
            CancelChunkRequests(command.ProducerId);

        command.RequestId = _requestId.FetchNext();
        var request = StandardRequest.WithProducerId(command.RequestId, command.ProducerId);
        return _requests.CreateTask(request);
    }

    public Task<BaseCommand> ExpectAdditionalResponse(CommandProducer command)
    {
        var request = StandardRequest.WithProducerId(command.RequestId, command.ProducerId);
        return _requests.CreateTask(request);
    }

    public Task<BaseCommand> Outgoing(CommandCloseProducer command)
    {
        command.RequestId = _requestId.FetchNext();
        var request = StandardRequest.WithProducerId(command.RequestId, command.ProducerId);
        return _requests.CreateTask(request);
    }

    public Task<BaseCommand> Outgoing(CommandSubscribe command)
    {
        command.RequestId = _requestId.FetchNext();
        var request = StandardRequest.WithConsumerId(command.RequestId, command.ConsumerId);
        return _requests.CreateTask(request);
    }

    public Task<BaseCommand> Outgoing(CommandUnsubscribe command)
    {
        command.RequestId = _requestId.FetchNext();
        var request = StandardRequest.WithConsumerId(command.RequestId, command.ConsumerId);
        return _requests.CreateTask(request);
    }

    public Task<BaseCommand> Outgoing(CommandCloseConsumer command)
    {
        command.RequestId = _requestId.FetchNext();
        var request = StandardRequest.WithConsumerId(command.RequestId, command.ConsumerId);
        return _requests.CreateTask(request);
    }

    public void Outgoing(CommandSend command, TaskCompletionSource<BaseCommand> tcs)
    {
        // Upstream: ProducerImpl#ackReceived
        // The chunks of a message are sent with the same sequence id, so they can not be told apart by the response. The broker answers them in the order they were sent, so they are answered in that order.
        if (command.IsChunk)
        {
            lock (_chunkRequests)
            {
                var key = (command.ProducerId, command.SequenceId);

                if (!_chunkRequests.TryGetValue(key, out var queue))
                {
                    queue = new Queue<TaskCompletionSource<BaseCommand>>();
                    _chunkRequests.Add(key, queue);
                    Volatile.Write(ref _chunkRequestCount, _chunkRequests.Count);
                }

                queue.Enqueue(tcs);
            }

            return;
        }

        var request = new SendRequest(command.ProducerId, command.SequenceId);
        _requests.AddTaskCompletionSource(request, tcs);

        // Upstream: Producer#MessagePublishContext.completed
        // The broker answers with the sequence id of the batch in a receipt, but with the highest sequence id of the batch in an error.
        // Both keys are therefore registered, and the one that was not used is removed when the request is completed.
        if (!command.HasHighestSequenceId || command.HighestSequenceId == command.SequenceId)
            return;

        var highestRequest = new SendRequest(command.ProducerId, command.HighestSequenceId);
        _requests.AddTaskCompletionSource(highestRequest, tcs);

        _ = tcs.Task.ContinueWith(
            static (_, state) =>
            {
                var (requests, tcs, request, highestRequest) = ((Awaiter<IRequest, BaseCommand>, TaskCompletionSource<BaseCommand>, IRequest, IRequest)) state!;
                requests.Remove(request, tcs);
                requests.Remove(highestRequest, tcs);
            },
            (_requests, tcs, (IRequest) request, (IRequest) highestRequest),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    public Task<BaseCommand> Outgoing(CommandGetOrCreateSchema command)
    {
        command.RequestId = _requestId.FetchNext();
        var request = StandardRequest.WithRequestId(command.RequestId);
        return _requests.CreateTask(request);
    }

    public Task<BaseCommand> Outgoing(CommandConnect _)
        => _requests.CreateTask(new ConnectRequest());

    public Task<BaseCommand> Outgoing(CommandLookupTopic command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandPartitionedTopicMetadata command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandGetTopicsOfNamespace command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandSeek command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithConsumerId(command.RequestId, command.ConsumerId, BaseCommand.Types.Type.Seek));
    }

    public Task<BaseCommand> Outgoing(CommandGetLastMessageId command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> OutgoingWithResponse(CommandAck command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandTcClientConnectRequest command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandNewTxn command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandAddPartitionToTxn command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandAddSubscriptionToTxn command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public Task<BaseCommand> Outgoing(CommandEndTxn command)
    {
        command.RequestId = _requestId.FetchNext();
        return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
    }

    public void Cancel(ulong requestId)
        => _requests.Cancel(StandardRequest.WithRequestId(requestId));

    public void Incoming(BaseCommand command)
    {
        if (Volatile.Read(ref _chunkRequestCount) != 0 && TryCompleteChunkRequest(command))
            return;

        var identifier = _getResponseIdentifier.Get(command.Type)(command);

        if (identifier is not null)
            _requests.SetResult(identifier, command);
    }

    public void Incoming(CommandCloseConsumer command)
    {
        var requests = _requests.Keys;
        foreach (var request in requests)
        {
            if (request.SenderIsConsumer(command.ConsumerId))
            {
                if (request.IsCommandType(BaseCommand.Types.Type.Seek))
                    _requests.SetResult(request, new BaseCommand { Type = BaseCommand.Types.Type.Success });
                else
                    _requests.Cancel(request);
            }
        }
    }

    public void Incoming(CommandCloseProducer command)
    {
        var requests = _requests.Keys;
        foreach (var request in requests)
        {
            if (request.SenderIsProducer(command.ProducerId))
                _requests.Cancel(request);
        }

        CancelChunkRequests(command.ProducerId);
    }

    // Completes the oldest request for the chunk that the broker has answered. Returns false if the command does not answer a chunk.
    private bool TryCompleteChunkRequest(BaseCommand command)
    {
        (ulong, ulong) key;

        if (command.Type == BaseCommand.Types.Type.SendReceipt)
            key = (command.SendReceipt.ProducerId, command.SendReceipt.SequenceId);
        else if (command.Type == BaseCommand.Types.Type.SendError)
            key = (command.SendError.ProducerId, command.SendError.SequenceId);
        else
            return false;

        TaskCompletionSource<BaseCommand> tcs;

        lock (_chunkRequests)
        {
            if (!_chunkRequests.TryGetValue(key, out var queue))
                return false;

            tcs = queue.Dequeue();

            if (queue.Count == 0)
            {
                _ = _chunkRequests.Remove(key);
                Volatile.Write(ref _chunkRequestCount, _chunkRequests.Count);
            }
        }

        _ = tcs.TrySetResult(command);
        return true;
    }

    // Cancels the requests of the chunks of the producer, or of every producer if the id is null
    private void CancelChunkRequests(ulong? producerId)
    {
        List<TaskCompletionSource<BaseCommand>> canceled = [];

        lock (_chunkRequests)
        {
            foreach (var key in _chunkRequests.Keys.ToArray())
            {
                if (producerId is not null && key.ProducerId != producerId)
                    continue;

                canceled.AddRange(_chunkRequests[key]);
                _ = _chunkRequests.Remove(key);
            }

            Volatile.Write(ref _chunkRequestCount, _chunkRequests.Count);
        }

        foreach (var tcs in canceled)
            _ = tcs.TrySetCanceled();
    }
}
