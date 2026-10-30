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
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf.Collections;
using System.Buffers;

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
    private readonly ConsumerEpoch? _consumerEpoch;
    private readonly UnAckedMessageTracker? _unAckedMessageTracker;
    private readonly DeadLetterHandler<TMessage>? _deadLetterHandler;
    private readonly ChunkedMessageAssembler _chunkAssembler;
    private readonly MessageCrypto? _messageCrypto;
    private readonly ICryptoKeyReader? _cryptoKeyReader;
    private readonly ConsumerCryptoFailureAction _cryptoFailureAction;
    private readonly List<MessageIdData> _chunkAcks;
    private readonly List<MessageIdData> _chunkTrack;
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
        bool batchIndexAckEnabled,
        ConsumerEpoch? consumerEpoch,
        UnAckedMessageTracker? unAckedMessageTracker,
        DeadLetterHandler<TMessage>? deadLetterHandler,
        ChunkedMessageAssembler chunkAssembler,
        MessageCrypto? messageCrypto,
        ICryptoKeyReader? cryptoKeyReader,
        ConsumerCryptoFailureAction cryptoFailureAction)
    {
        _id = id;
        _queue = queue;
        _connection = connection;
        _batchHandler = batchHandler;
        _messageFactory = messageFactory;
        _topic = topic;
        _ackReceiptEnabled = ackReceiptEnabled;
        _batchIndexAckEnabled = batchIndexAckEnabled;
        _consumerEpoch = consumerEpoch;
        _unAckedMessageTracker = unAckedMessageTracker;
        _deadLetterHandler = deadLetterHandler;
        _chunkAssembler = chunkAssembler;
        _messageCrypto = messageCrypto;
        _cryptoKeyReader = cryptoKeyReader;
        _cryptoFailureAction = cryptoFailureAction;
        _chunkAcks = [];
        _chunkTrack = [];

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

                    if (IsStale(messagePackage))
                    {
                        // Upstream: ConsumerImpl#messageReceived (increaseAvailablePermits)
                        // The broker has deducted permits for every message of the entry, and one of them was counted when the entry was dequeued
                        var skipped = (uint) GetPermitCount(messagePackage) - 1;
                        _sendWhenZero = skipped >= _sendWhenZero ? 0 : _sendWhenZero - skipped;
                        continue;
                    }

                    if (!messagePackage.ValidateMagicNumberAndChecksum())
                    {
                        await RejectPackage(messagePackage, CommandAck.Types.ValidationError.ChecksumMismatch, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    var metadataSize = messagePackage.GetMetadataSize();
                    var metadata = messagePackage.ExtractMetadata(metadataSize);
                    var data = messagePackage.ExtractData(metadataSize);
                    var messageId = messagePackage.MessageId;
                    MessageIdData[]? chunkMessageIds = null;
                    var isUndecryptable = false;

                    // Upstream: ConsumerImpl#decryptPayloadIfNeeded, ConsumerImpl#handleCryptoFailure
                    // Every entry is decrypted before anything else is done with it, because every chunk is encrypted on its own, and the assembler needs the decrypted chunk,
                    // since total_chunk_msg_size is the size of the payload before it was encrypted.
                    if (metadata.EncryptionKeys.Count > 0)
                    {
                        if (_messageCrypto is not null && _messageCrypto.TryDecrypt(metadata, data, _cryptoKeyReader!, out var decrypted))
                            data = decrypted;
                        else
                        {
                            CountBatchAsReceived(metadata);

                            switch (_cryptoFailureAction)
                            {
                                // Upstream: ConsumerImpl#messageReceived (isMessageUndecryptable)
                                // The payload is delivered as it is, so it is neither decompressed nor unpacked if it is a batch. A chunk is not delivered, because the chunks of a
                                // message can not be assembled as long as they are encrypted. Each has its own IV, and its size is 16 bytes more than the size that total_chunk_msg_size
                                // has for it. Upstream fails to assemble them (the buffer of the message overflows), so a chunk is handled as if the action was 'Fail'.
                                case ConsumerCryptoFailureAction.Consume when !metadata.IsChunkedMessage():
                                    isUndecryptable = true;
                                    break;
                                case ConsumerCryptoFailureAction.Discard:
                                    await RejectPackage(messagePackage, CommandAck.Types.ValidationError.DecryptionError, cancellationToken).ConfigureAwait(false);
                                    continue;
                                default:
                                    // Neither acknowledged nor delivered. The ack timeout redelivers it, or the broker does when the consumer reconnects.
                                    _unAckedMessageTracker?.Add(messageId.ToMessageId(_topic));
                                    continue;
                            }
                        }
                    }

                    // Upstream: ConsumerImpl#messageReceived (chunk branch)
                    // The chunks are assembled before the message is decompressed. The metadata of the last chunk is the metadata of the message.
                    if (metadata.IsChunkedMessage())
                    {
                        var assembled = ProcessChunk(messageId, metadata, data);

                        if (assembled is null)
                            continue;

                        data = assembled.Value.Payload;
                        chunkMessageIds = assembled.Value.ChunkMessageIds;
                    }

                    if (!isUndecryptable && metadata.Compression != CompressionType.None)
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
                            // Upstream: ConsumerImpl#messageReceived (decompression failure of a chunked message)
                            // The last chunk is rejected like any other corrupted entry. The chunks before it can never be part of a delivered message, so they are acknowledged.
                            if (chunkMessageIds is not null)
                                AcknowledgeChunks(chunkMessageIds, chunkMessageIds.Length - 1);

                            await RejectPackage(messagePackage, CommandAck.Types.ValidationError.DecompressionError, cancellationToken).ConfigureAwait(false);
                            continue;
                        }
                    }

                    var redeliveryCount = messagePackage.RedeliveryCount;

                    // Upstream: ConsumerImpl#createEncryptionContext
                    // The context is only created for the message that is delivered. For a chunked message, it is the context of the last chunk.
                    var encryptionContext = metadata.EncryptionKeys.Count > 0 ? EncryptionContextFactory.Create(metadata, isUndecryptable) : null;

                    if (!isUndecryptable && metadata.HasNumMessagesInBatch)
                    {
                        IMessage<TMessage>? batchMessage;
                        int chargedButSkipped;

                        try
                        {
                            batchMessage = _batchHandler.Add(messageId, messagePackage.AckSet, redeliveryCount, metadata, data, out chargedButSkipped, encryptionContext);
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

                    var receivedMessage = _messageFactory.Create(chunkMessageIds is null ? messageId.ToMessageId(_topic) : CreateChunkedMessageId(chunkMessageIds), redeliveryCount, data, metadata, null, encryptionContext);

                    // Upstream: ConsumerImpl#messageReceived (possibleSendToDeadLetterTopicMessages)
                    // A message that was delivered more often than allowed is not delivered. The redelivery sends it to the dead letter topic.
                    // Its permit is counted as received, like the permits of the messages that are rejected.
                    if (_deadLetterHandler is not null && redeliveryCount >= _deadLetterHandler.MaxRedeliverCount)
                    {
                        _deadLetterHandler.Register(messageId.LedgerId, messageId.EntryId, receivedMessage);

                        if (redeliveryCount > _deadLetterHandler.MaxRedeliverCount)
                        {
                            _deadLetterHandler.OnRedeliveryCountExceeded(receivedMessage.MessageId);
                            continue;
                        }
                    }

                    return receivedMessage;
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
        if (command.AckType == CommandAck.Types.AckType.Cumulative && command.MessageId.Count > 0)
            _unAckedMessageTracker?.RemoveMessagesTill(command.MessageId[0].LedgerId, command.MessageId[0].EntryId);

        if (!PrepareAck(command))
            return null;

        if (_unAckedMessageTracker is not null || _deadLetterHandler is not null)
            RemoveAcknowledgedEntries(command);

        command.ConsumerId = _id;

        if (_ackReceiptEnabled && _connection.ProtocolVersion >= Constants.ProtocolVersionAckReceipt)
            return await _connection.SendWithResponse(command, cancellationToken).ConfigureAwait(false);

        command.ClearRequestId();
        await _connection.Send(command, cancellationToken).ConfigureAwait(false);
        return null;
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#addIndividualAcknowledgment
    // An entry is acknowledged as a whole when the message id of the command has no ack set. It is then no longer waited for, and it is no longer a dead letter candidate.
    // A cumulative acknowledgment removes the entries from the tracker before the command is prepared, and it does not remove dead letter candidates.
    private void RemoveAcknowledgedEntries(CommandAck command)
    {
        if (command.AckType != CommandAck.Types.AckType.Individual || (_unAckedMessageTracker is null && _deadLetterHandler is null))
            return;

        foreach (var messageId in command.MessageId)
        {
            if (messageId.AckSet.Count != 0)
                continue;

            _unAckedMessageTracker?.Remove(messageId.LedgerId, messageId.EntryId);
            _deadLetterHandler?.Remove(messageId.LedgerId, messageId.EntryId);
        }
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#addAcknowledgment (C# deviation: the acknowledgments are not grouped)
    // The message ids of the command are changed in place, because they belong to the command and may be pooled with it.
    // An id that is kept by this method and has a batch index is the entry of a batch. Its batch index is cleared at the end.
    private bool PrepareAck(CommandAck command)
    {
        var messageIds = command.MessageId;

        if (messageIds.Count == 0)
            return false;

        // The common acknowledgment of one entry that is not a batch has nothing to prepare
        if (messageIds.Count == 1 && command.AckType == CommandAck.Types.AckType.Individual && messageIds[0].BatchIndex < 0)
            return true;

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

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages
    // The messages that were received from the broker, but not yet delivered, are dropped. The broker delivers them again, so they do not count as received,
    // and the permits that the broker has deducted for them are given back.
    public async Task Send(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken)
    {
        command.ConsumerId = _id;

        var permits = command.MessageIds.Count == 0 ? PurgeAll() : PurgeEntries(command.MessageIds);

        await _connection.Send(command, cancellationToken).ConfigureAwait(false);

        if (permits > 0)
            await _connection.Send(new CommandFlow { ConsumerId = _id, MessagePermits = (uint) permits }, cancellationToken).ConfigureAwait(false);
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
        _deadLetterHandler?.Clear();
        _chunkAssembler.Clear();
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

    // Upstream: ConsumerImpl#messageReceived (increaseAvailablePermits)
    // The broker has deducted a permit for every message of a batch, so a batch that is not unpacked counts as received once for every message in it.
    // One of them was counted when the entry was dequeued.
    private void CountBatchAsReceived(MessageMetadata metadata)
    {
        if (!metadata.HasNumMessagesInBatch)
            return;

        var skipped = (uint) Math.Max(metadata.NumMessagesInBatch - 1, 0);
        _sendWhenZero = skipped >= _sendWhenZero ? 0 : _sendWhenZero - skipped;
    }

    // Upstream: ConsumerBase#isValidConsumerEpoch
    // Only a consumer that has a single active consumer on the broker has a consumer epoch, and a message without an epoch is always valid
    private bool IsStale(MessagePackage messagePackage)
        => _consumerEpoch is not null && messagePackage.ConsumerEpoch != -1 && messagePackage.ConsumerEpoch < _consumerEpoch.Value;

    // The queue and the batch handler have their own locks, and the receive lock must not be taken, because Receive may wait for a message while holding it.
    // A message that Receive has dequeued before the purge may still be delivered.
    private int PurgeAll()
    {
        var permits = _batchHandler.ClearQueuedMessages();

        foreach (var messagePackage in _queue.RemoveAll())
        {
            permits += GetPermitCount(messagePackage);
        }

        return permits;
    }

    // Only the ledger and the entry are compared, so the messages of a batch are dropped together
    private int PurgeEntries(RepeatedField<MessageIdData> messageIds)
    {
        var entries = new HashSet<(ulong LedgerId, ulong EntryId)>();

        foreach (var messageId in messageIds)
        {
            entries.Add((messageId.LedgerId, messageId.EntryId));
        }

        var permits = _batchHandler.RemoveQueuedMessages(entries);

        foreach (var messagePackage in _queue.RemoveWhere(messagePackage => entries.Contains((messagePackage.MessageId.LedgerId, messagePackage.MessageId.EntryId))))
        {
            permits += GetPermitCount(messagePackage);
        }

        return permits;
    }

    // The broker deducts one permit for every message of a batch
    private static int GetPermitCount(MessagePackage messagePackage)
    {
        try
        {
            var metadata = messagePackage.ExtractMetadata(messagePackage.GetMetadataSize());
            return metadata.HasNumMessagesInBatch ? Math.Max(metadata.NumMessagesInBatch, 1) : 1;
        }
        catch
        {
            return 1;
        }
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

    // Upstream: ConsumerImpl#processMessageChunk
    // The assembler asks for acknowledgments of the chunks that will never be part of a delivered message, and it hands back the chunks that are tracked by
    // the ack timeout (they are redelivered if the ack timeout is enabled). Returns the message if the chunk completed it.
    // Nothing is awaited, because every local variable of Receive that is used after an await is kept in its state machine, which is allocated for every message.
    private AssembledChunkedMessage? ProcessChunk(MessageIdData messageId, MessageMetadata metadata, ReadOnlySequence<byte> data)
    {
        _chunkAcks.Clear();
        _chunkTrack.Clear();

        var assembled = _chunkAssembler.Process(messageId, metadata, data, _chunkAcks, _chunkTrack);

        if (_unAckedMessageTracker is not null)
        {
            foreach (var chunkId in _chunkTrack)
            {
                _unAckedMessageTracker.Add(chunkId.ToMessageId(_topic));
            }
        }

        if (_chunkAcks.Count > 0)
            AcknowledgeChunks(_chunkAcks, _chunkAcks.Count);

        return assembled;
    }

    // Upstream: ConsumerImpl#acknowledgeChunkedMessageIds
    // The first 'count' ids are acknowledged individually, in the background and without waiting for a receipt. Like Java, a failure is ignored,
    // because the broker redelivers the chunks if the consumer reconnects.
    private void AcknowledgeChunks(IReadOnlyList<MessageIdData> chunkMessageIds, int count)
    {
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };

        for (var i = 0; i < count; ++i)
        {
            command.MessageId.Add(chunkMessageIds[i]);
        }

        RemoveAcknowledgedEntries(command);
        command.ConsumerId = _id;
        _ = SendIgnoringFailure(command);
    }

    private async Task SendIgnoringFailure(CommandAck command)
    {
        try
        {
            await _connection.Send(command, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }

    // Upstream: ChunkMessageIdImpl
    // The id of a chunked message is the id of its last chunk, and it knows the ids of all the chunks
    private MessageId CreateChunkedMessageId(MessageIdData[] chunkMessageIds)
    {
        var first = chunkMessageIds[0];
        var last = chunkMessageIds[chunkMessageIds.Length - 1];
        var firstChunkMessageId = new MessageId(first.LedgerId, first.EntryId, first.Partition, -1, _topic);
        return new MessageId(last.LedgerId, last.EntryId, last.Partition, -1, _topic, firstChunkMessageId, chunkMessageIds);
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
