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

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf.Collections;

public sealed class ConsumerChannel<TMessage> : IConsumerChannel<TMessage>
{
    private readonly ulong _id;
    private readonly AsyncQueue<MessagePackage> _queue;
    private readonly IConnection _connection;
    private readonly BatchHandler<TMessage> _batchHandler;
    private readonly CommandFlow _cachedCommandFlow;
    private readonly IMessageFactory<TMessage> _messageFactory;
    private readonly IDecompress?[] _decompressors;
    private readonly AsyncLock _lock;
    private readonly string _topic;
    private readonly bool _ackReceiptEnabled;
    private readonly bool _batchIndexAckEnabled;
    private uint _sendWhenZero;
    private bool _firstFlow;

    public ConsumerChannel(
        ulong id,
        uint messagePrefetchCount,
        AsyncQueue<MessagePackage> queue,
        IConnection connection,
        BatchHandler<TMessage> batchHandler,
        IMessageFactory<TMessage> messageFactory,
        IEnumerable<IDecompressorFactory> decompressorFactories,
        string topic,
        bool ackReceiptEnabled,
        bool batchIndexAckEnabled)
    {
        _id = id;
        _queue = queue;
        _connection = connection;
        _batchHandler = batchHandler;
        _messageFactory = messageFactory;
        _topic = topic;
        _ackReceiptEnabled = ackReceiptEnabled;
        _batchIndexAckEnabled = batchIndexAckEnabled;

        _decompressors = new IDecompress[5];

        foreach (var decompressorFactory in decompressorFactories)
        {
            _decompressors[(int) decompressorFactory.CompressionType] = decompressorFactory.Create();
        }

        _lock = new AsyncLock();

        _cachedCommandFlow = new CommandFlow
        {
            ConsumerId = id,
            MessagePermits = messagePrefetchCount
        };

        _sendWhenZero = 0;
        _firstFlow = true;
    }

    public async ValueTask<IMessage<TMessage>> Receive(CancellationToken cancellationToken)
    {
        using (await _lock.Lock(cancellationToken).ConfigureAwait(false))
        {
            while (true)
            {
                if (_sendWhenZero == 0)
                    await SendFlow(cancellationToken).ConfigureAwait(false);

                _sendWhenZero--;
                try
                {
                    var message = _batchHandler.GetNext();

                    if (message is not null)
                        return message;

                    var messagePackage = await _queue.Dequeue(cancellationToken).ConfigureAwait(false);

                    if (!messagePackage.ValidateMagicNumberAndChecksum())
                    {
                        await RejectPackage(messagePackage, CommandAck.Types.ValidationError.ChecksumMismatch, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    var metadataSize = messagePackage.GetMetadataSize();
                    var metadata = messagePackage.ExtractMetadata(metadataSize);
                    var data = messagePackage.ExtractData(metadataSize);

                    if (metadata.Compression != CompressionType.None)
                    {
                        var decompressor = _decompressors[(int) metadata.Compression];
                        if (decompressor is null)
                            throw new CompressionException($"Support for {metadata.Compression} compression was not found");

                        try
                        {
                            data = decompressor.Decompress(data, (int) metadata.UncompressedSize);
                        }
                        catch
                        {
                            await RejectPackage(messagePackage, CommandAck.Types.ValidationError.DecompressionError, cancellationToken).ConfigureAwait(false);
                            continue;
                        }
                    }

                    var messageId = messagePackage.MessageId;
                    var redeliveryCount = messagePackage.RedeliveryCount;

                    if (metadata.HasNumMessagesInBatch)
                    {
                        IMessage<TMessage>? batchMessage;
                        int chargedButSkipped;

                        try
                        {
                            batchMessage = _batchHandler.Add(messageId, messagePackage.AckSet, redeliveryCount, metadata, data, out chargedButSkipped);
                        }
                        catch
                        {
                            await RejectPackage(messagePackage, CommandAck.Types.ValidationError.BatchDeSerializeError, cancellationToken).ConfigureAwait(false);
                            continue;
                        }

                        // Nothing was received if every message of the batch was skipped
                        if (batchMessage is null)
                            _sendWhenZero++;

                        // Upstream: ConsumerImpl#receiveIndividualMessagesFromBatch (increaseAvailablePermits)
                        // The broker has deducted permits for the messages that were skipped, so they count as received
                        if (chargedButSkipped > 0)
                            _sendWhenZero = (uint) chargedButSkipped >= _sendWhenZero ? 0 : _sendWhenZero - (uint) chargedButSkipped;

                        if (batchMessage is null)
                            continue;

                        return batchMessage;
                    }

                    return _messageFactory.Create(messageId.ToMessageId(_topic), redeliveryCount, data, metadata);
                }
                catch (Exception e) when (e is not CompressionException)
                {
                    // Undo decrementing since we didn't actually receive anything
                    _sendWhenZero++;
                    throw;
                }
            }
        }
    }

    public async Task<BaseCommand?> Send(CommandAck command, CancellationToken cancellationToken)
    {
        if (!PrepareAck(command))
            return null;

        command.ConsumerId = _id;

        if (_ackReceiptEnabled && _connection.ProtocolVersion >= Constants.ProtocolVersionAckReceipt)
            return await _connection.SendWithResponse(command, cancellationToken).ConfigureAwait(false);

        command.ClearRequestId();
        await _connection.Send(command, cancellationToken).ConfigureAwait(false);
        return null;
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#addAcknowledgment (C# deviation: the acknowledgments are not grouped)
    // The message ids of the command are changed in place, because they belong to the command and may be pooled with it.
    // An id that is kept by this method and has a batch index is the entry of a batch. Its batch index is cleared at the end.
    private bool PrepareAck(CommandAck command)
    {
        var messageIds = command.MessageId;

        if (messageIds.Count == 0)
            return false;

        var cumulative = command.AckType == CommandAck.Types.AckType.Cumulative;
        var keep = 0;
        var hasBatchedIds = false;

        for (var i = 0; i < messageIds.Count; ++i)
        {
            var messageId = messageIds[i];

            if (messageId.BatchIndex < 0)
            {
                if (cumulative)
                    _batchHandler.PruneUpTo(messageId);
            }
            else
            {
                hasBatchedIds = true;

                if (!(cumulative ? PrepareCumulativeAck(messageId) : PrepareIndividualAck(messageId, messageIds, keep)))
                    continue;
            }

            messageIds[keep++] = messageId;
        }

        if (keep == 0)
            return false;

        while (messageIds.Count > keep)
            messageIds.RemoveAt(messageIds.Count - 1);

        if (hasBatchedIds)
        {
            for (var i = 0; i < keep; ++i)
            {
                if (messageIds[i].BatchIndex >= 0)
                    messageIds[i].ClearBatchIndex();
            }
        }

        return true;
    }

    // Returns whether the message id is to be sent
    private bool PrepareIndividualAck(MessageIdData messageId, RepeatedField<MessageIdData> keptMessageIds, int keptCount)
    {
        var tracker = _batchHandler.AcknowledgeIndividual(messageId, out var isComplete);
        var kept = FindEntry(keptMessageIds, keptCount, messageId);

        if (kept is not null)
        {
            // Another message of the same batch is already part of the command. A kept id that still has a batch index was created from a tracker.
            if (kept.BatchIndex >= 0)
            {
                if (tracker is null || isComplete)
                    kept.AckSet.Clear();
                else if (_batchIndexAckEnabled)
                    _batchHandler.CopyAckSetTo(tracker, kept.AckSet);
            }

            return false;
        }

        // A batch that is not tracked is acknowledged as a whole
        if (tracker is null)
        {
            messageId.ClearBatchIndex();
            return true;
        }

        if (isComplete)
        {
            messageId.AckSet.Clear();
            return true;
        }

        if (!_batchIndexAckEnabled)
            return false;

        _batchHandler.CopyAckSetTo(tracker, messageId.AckSet);
        return true;
    }

    // Returns whether the message id is to be sent
    private bool PrepareCumulativeAck(MessageIdData messageId)
    {
        var tracker = _batchHandler.AcknowledgeCumulative(messageId, out var isComplete);

        if (tracker is null || isComplete)
        {
            messageId.AckSet.Clear();
            messageId.ClearBatchIndex();
            return true;
        }

        if (_batchIndexAckEnabled)
        {
            _batchHandler.CopyAckSetTo(tracker, messageId.AckSet);
            messageId.ClearBatchIndex();
            return true;
        }

        // Only the entries before the batch are completely acknowledged
        if (messageId.EntryId == 0)
            return false;

        messageId.EntryId--;
        messageId.AckSet.Clear();
        messageId.ClearBatchIndex();
        return true;
    }

    private static MessageIdData? FindEntry(RepeatedField<MessageIdData> messageIds, int count, MessageIdData messageId)
    {
        for (var i = count - 1; i >= 0; --i)
        {
            var candidate = messageIds[i];

            if (candidate.LedgerId == messageId.LedgerId && candidate.EntryId == messageId.EntryId && candidate.Partition == messageId.Partition)
                return candidate;
        }

        return null;
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse
    // The batch index is not enough for the broker, so a message in a batch is acknowledged using the ack set of the batch.
    // This is done regardless of BatchIndexAckEnabled, and the trackers are only changed by a cumulative acknowledgment.
    public async Task<BaseCommand> SendTransactional(CommandAck command, CancellationToken cancellationToken)
    {
        if (_connection.ProtocolVersion < Constants.ProtocolVersionAckReceipt)
            throw new NotSupportedException("The broker does not support acknowledgment receipts (protocol version < 17)");

        var cumulative = command.AckType == CommandAck.Types.AckType.Cumulative;

        for (var i = 0; i < command.MessageId.Count; ++i)
        {
            var messageId = command.MessageId[i];

            if (messageId.BatchIndex < 0)
                continue;

            var batchSize = GetBatchSize(messageId);

            if (batchSize > 0)
            {
                if (cumulative)
                {
                    _batchHandler.AcknowledgeCumulative(messageId, out _);
                    AckSetWriter.WriteCumulative(messageId.AckSet, batchSize, messageId.BatchIndex);
                }
                else
                {
                    AckSetWriter.WriteIndividual(messageId.AckSet, batchSize, messageId.BatchIndex);
                }

                messageId.BatchSize = batchSize;
            }

            messageId.ClearBatchIndex();
        }

        command.ConsumerId = _id;
        return await _connection.SendWithResponse(command, cancellationToken).ConfigureAwait(false);
    }

    private int GetBatchSize(MessageIdData messageId)
    {
        if (_batchHandler.TryGetBatchSize(messageId, out var batchSize))
            return batchSize;

        return messageId.HasBatchSize ? messageId.BatchSize : 0;
    }

    public async Task Send(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken)
    {
        command.ConsumerId = _id;
        await _connection.Send(command, cancellationToken).ConfigureAwait(false);
    }

    public async Task Send(CommandUnsubscribe command, CancellationToken cancellationToken)
    {
        command.ConsumerId = _id;
        var response = await _connection.Send(command, cancellationToken).ConfigureAwait(false);
        response.Expect(BaseCommand.Types.Type.Success);
    }

    public async Task Send(CommandSeek command, CancellationToken cancellationToken)
    {
        command.ConsumerId = _id;
        var response = await _connection.Send(command, cancellationToken).ConfigureAwait(false);
        response.Expect(BaseCommand.Types.Type.Success);
        _batchHandler.Clear();
    }

    public async Task<MessageId> Send(CommandGetLastMessageId command, CancellationToken cancellationToken)
    {
        command.ConsumerId = _id;
        var response = await _connection.Send(command, cancellationToken).ConfigureAwait(false);
        response.Expect(BaseCommand.Types.Type.GetLastMessageIdResponse);
        var messageIdData = response.GetLastMessageIdResponse.LastMessageId;
        if (messageIdData.LedgerId == MessageId.Earliest.LedgerId &&
            messageIdData.EntryId == MessageId.Earliest.EntryId &&
            messageIdData.Partition == MessageId.Earliest.Partition &&
            messageIdData.BatchIndex == MessageId.Earliest.BatchIndex)
            return MessageId.Earliest;
        return messageIdData.ToMessageId(_topic);
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Dispose();

        for (var i = 0; i < _decompressors.Length; ++i)
        {
            _decompressors[i]?.Dispose();
        }

        await _lock.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask SendFlow(CancellationToken cancellationToken)
    {
        await _connection.Send(_cachedCommandFlow, cancellationToken).ConfigureAwait(false);

        if (_firstFlow)
        {
            _cachedCommandFlow.MessagePermits = (uint) Math.Ceiling(_cachedCommandFlow.MessagePermits * 0.5);
            _firstFlow = false;
        }

        _sendWhenZero = _cachedCommandFlow.MessagePermits;
    }

    private async Task RejectPackage(MessagePackage messagePackage, CommandAck.Types.ValidationError validationErrorType, CancellationToken cancellationToken)
    {
        var ack = new CommandAck
        {
            AckType = CommandAck.Types.AckType.Individual,
            ValidationError = validationErrorType
        };

        ack.MessageId.Add(messagePackage.MessageId);

        if (!PrepareAck(ack))
            return;

        ack.ConsumerId = _id;
        await _connection.Send(ack, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask ClosedByClient(CancellationToken cancellationToken)
    {
        try
        {
            var closeConsumer = new CommandCloseConsumer { ConsumerId = _id };
            await _connection.Send(closeConsumer, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }
}
