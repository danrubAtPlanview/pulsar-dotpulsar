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

using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using Microsoft.Extensions.ObjectPool;
using System.Buffers;

public sealed class ProducerChannel : IProducerChannel
{
    private readonly ObjectPool<SendPackage> _sendPackagePool;
    private readonly ulong _id;
    private readonly string _name;
    private readonly IConnection _connection;
    private readonly ICompressorFactory? _compressorFactory;
    private readonly byte[]? _schemaVersion;
    private readonly MessageCrypto? _messageCrypto;
    private readonly ProducerCryptoFailureAction _cryptoFailureAction;

    // The message crypto is owned by the sub producer, because it is used by every channel that the sub producer has, so the channel does not dispose it
    public ProducerChannel(
        ulong id,
        string name,
        IConnection connection,
        ICompressorFactory? compressorFactory,
        byte[]? schemaVersion,
        MessageCrypto? messageCrypto,
        ProducerCryptoFailureAction cryptoFailureAction)
    {
        var sendPackagePolicy = new DefaultPooledObjectPolicy<SendPackage>();
        _sendPackagePool = new DefaultObjectPool<SendPackage>(sendPackagePolicy);
        _id = id;
        _name = name;
        _connection = connection;
        _compressorFactory = compressorFactory;
        _schemaVersion = schemaVersion;
        _messageCrypto = messageCrypto;
        _cryptoFailureAction = cryptoFailureAction;
    }

    public int MaxMessageSize
        => _connection.MaxMessageSize;

    public async ValueTask ClosedByClient(CancellationToken cancellationToken)
    {
        try
        {
            var closeProducer = new CommandCloseProducer { ProducerId = _id };
            await _connection.Send(closeProducer, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }

    public ValueTask DisposeAsync() => new();

    public async Task Send(MessageMetadata metadata, ReadOnlySequence<byte> payload, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
    {
        var sendPackage = _sendPackagePool.Get();
        var resetSchema = false;
        var resetCompression = false;
        var resetEncryption = false;

        try
        {
            metadata.PublishTime = (ulong) DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            metadata.ProducerName = _name;

            if (!metadata.HasSchemaVersion && _schemaVersion is not null)
            {
                metadata.SchemaVersion = ByteString.CopyFrom(_schemaVersion);
                resetSchema = true;
            }

            // The size of a frame that carries a chunk is not checked, because the frame is larger than the maximum message size by the fields of the chunk
            PrepareCommand(sendPackage, metadata, metadata.NumChunksFromMsg > 1);

            if (_compressorFactory is null || metadata.Compression != CompressionType.None)
                sendPackage.Payload = payload;
            else
            {
                metadata.Compression = _compressorFactory.CompressionType;
                metadata.UncompressedSize = (uint) payload.Length;
                using var compressor = _compressorFactory.Create();
                sendPackage.Payload = compressor.Compress(payload);
                resetCompression = true;
            }

            // Upstream: ProducerImpl#serializeAndSendMessage, BatchMessageContainerImpl#buildAndEncryptBatchPayload
            // The payload is encrypted after it has been compressed. A batch is encrypted as a whole, so it has one IV. A message that is split in chunks is encrypted in SendChunk, one chunk at a time.
            if (_messageCrypto is not null)
                resetEncryption = Encrypt(sendPackage, metadata);

            await _connection.Send(sendPackage, responseTcs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (resetSchema)
                metadata.ClearSchemaVersion();

            if (resetCompression)
            {
                metadata.Compression = CompressionType.None;
                metadata.UncompressedSize = 0;
            }

            if (resetEncryption)
                ResetEncryption(metadata);

            _sendPackagePool.Return(sendPackage);
        }
    }

    // Upstream: ProducerImpl#sendAsync, #updateMessageMetadata
    // Prepares a message to be sent by a producer with chunking enabled. The metadata of the message is copied, so it is not changed, and the message is compressed once and not each time it is sent.
    // The state that is returned says how many chunks the message is split in, which is one if it fits in a frame. Null is returned for a message that has chunk fields already, which is sent as it is.
    public ChunkedSendState? PlanChunking(MessageMetadata metadata, ReadOnlySequence<byte> payload, int chunkMaxMessageSize)
    {
        if (metadata.HasNumChunksFromMsg)
            return null;

        var plannedMetadata = metadata.Clone();
        plannedMetadata.PublishTime = (ulong) DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        plannedMetadata.ProducerName = _name;

        if (!plannedMetadata.HasSchemaVersion && _schemaVersion is not null)
            plannedMetadata.SchemaVersion = ByteString.CopyFrom(_schemaVersion);

        if (_compressorFactory is not null && plannedMetadata.Compression == CompressionType.None)
        {
            plannedMetadata.Compression = _compressorFactory.CompressionType;
            plannedMetadata.UncompressedSize = (uint) payload.Length;
            using var compressor = _compressorFactory.Create();
            payload = compressor.Compress(payload);
        }

        // Upstream: ProducerImpl#sendAsync
        // Every chunk is encrypted, and that adds the tag and the encryption fields of the metadata to it, so the size of the chunks is reduced by that. Java does not do this, and relies on the padding of the broker.
        var encryptionOverhead = _messageCrypto?.GetMetadataOverhead() ?? 0;
        return ChunkedSendState.Plan(plannedMetadata, payload, MaxMessageSize, chunkMaxMessageSize, encryptionOverhead);
    }

    // Upstream: ProducerImpl#serializeAndSendMessage
    // Sends one chunk of a message. The metadata and the payload of the message are already complete, so they are not changed, except for the id of the chunk. The size of the frame is not checked.
    public async Task SendChunk(ChunkedSendState state, int chunkId, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
    {
        var sendPackage = _sendPackagePool.Get();
        var metadata = state.Metadata;
        var resetEncryption = false;

        try
        {
            if (state.IsChunked)
                metadata.ChunkId = chunkId;

            PrepareCommand(sendPackage, metadata, true);
            sendPackage.Payload = state.GetChunk(chunkId);

            // Upstream: ProducerImpl#serializeAndSendMessage
            // Every chunk is encrypted on its own, with its own IV, and the metadata of the message is shared by the chunks. The metadata is serialized when the chunk is written, so the encryption fields of the chunk are removed after that.
            if (_messageCrypto is not null)
                resetEncryption = Encrypt(sendPackage, metadata);

            await _connection.Send(sendPackage, responseTcs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (resetEncryption)
                ResetEncryption(metadata);

            _sendPackagePool.Return(sendPackage);
        }
    }

    // Upstream: ProducerImpl#encryptMessage
    // Returns true if the payload is encrypted, which sets the encryption fields of the metadata. If it can not be, then the exception is thrown, unless the action is to send the message unencrypted, which returns false.
    private bool Encrypt(SendPackage sendPackage, MessageMetadata metadata)
    {
        try
        {
            sendPackage.Payload = _messageCrypto!.Encrypt(metadata, sendPackage.Payload);
            return true;
        }
        catch (CryptoException) when (_cryptoFailureAction == ProducerCryptoFailureAction.Send)
        {
            return false;
        }
    }

    // The metadata is pooled, or shared by the chunks and by the sends of a message that is sent again, so the fields that encryption has set can not be left on it
    private static void ResetEncryption(MessageMetadata metadata)
    {
        metadata.EncryptionKeys.Clear();
        metadata.ClearEncryptionParam();
    }

    private void PrepareCommand(SendPackage sendPackage, MessageMetadata metadata, bool skipSizeCheck)
    {
        sendPackage.Command ??= new CommandSend { ProducerId = _id };

        sendPackage.Command.SequenceId = metadata.SequenceId;

        // Upstream: Commands#newSend
        // The command is pooled, so these are always set or cleared, and the values of an earlier send can never leak into this one.
        sendPackage.Command.NumMessages = metadata.HasNumMessagesInBatch && metadata.NumMessagesInBatch > 1 ? metadata.NumMessagesInBatch : 1;

        if (metadata.HasHighestSequenceId)
            sendPackage.Command.HighestSequenceId = metadata.HighestSequenceId;
        else
            sendPackage.Command.ClearHighestSequenceId();

        if (metadata.IsChunk())
            sendPackage.Command.IsChunk = true;
        else
            sendPackage.Command.ClearIsChunk();

        // The command is pooled, and the broker handles a message as transactional if both of the ids are set, even if they are zero.
        // Both ids are therefore always either copied from the metadata or cleared, so the ids of an earlier message can never leak into this one.
        if (metadata.HasTxnidMostBits && metadata.HasTxnidLeastBits)
        {
            sendPackage.Command.TxnidMostBits = metadata.TxnidMostBits;
            sendPackage.Command.TxnidLeastBits = metadata.TxnidLeastBits;
        }
        else
        {
            sendPackage.Command.ClearTxnidMostBits();
            sendPackage.Command.ClearTxnidLeastBits();
        }

        sendPackage.Metadata = metadata;
        sendPackage.SkipSizeCheck = skipSizeCheck;
    }
}
