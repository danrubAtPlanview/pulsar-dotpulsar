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
using DotPulsar.Internal.Chunking;
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

    public ProducerChannel(
        ulong id,
        string name,
        IConnection connection,
        ICompressorFactory? compressorFactory,
        byte[]? schemaVersion)
    {
        var sendPackagePolicy = new DefaultPooledObjectPolicy<SendPackage>();
        _sendPackagePool = new DefaultObjectPool<SendPackage>(sendPackagePolicy);
        _id = id;
        _name = name;
        _connection = connection;
        _compressorFactory = compressorFactory;
        _schemaVersion = schemaVersion;
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

        try
        {
            metadata.PublishTime = (ulong) DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            metadata.ProducerName = _name;

            if (metadata.SchemaVersion is null && _schemaVersion is not null)
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
                sendPackage.Metadata.Compression = _compressorFactory.CompressionType;
                sendPackage.Metadata.UncompressedSize = (uint) payload.Length;
                using var compressor = _compressorFactory.Create();
                sendPackage.Payload = compressor.Compress(payload);
                resetCompression = true;
            }

            // TODO(WP9-producer): Encrypt sendPackage.Payload here, after it has been compressed. A message that is split in chunks is encrypted in SendChunk, one chunk at a time.

            await _connection.Send(sendPackage, responseTcs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (resetSchema)
                metadata.SchemaVersion = null;

            if (resetCompression)
            {
                metadata.Compression = CompressionType.None;
                metadata.UncompressedSize = 0;
            }

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

        if (plannedMetadata.SchemaVersion is null && _schemaVersion is not null)
            plannedMetadata.SchemaVersion = ByteString.CopyFrom(_schemaVersion);

        if (_compressorFactory is not null && plannedMetadata.Compression == CompressionType.None)
        {
            plannedMetadata.Compression = _compressorFactory.CompressionType;
            plannedMetadata.UncompressedSize = (uint) payload.Length;
            using var compressor = _compressorFactory.Create();
            payload = compressor.Compress(payload);
        }

        // TODO(WP9-producer): The bytes that encryption adds to every chunk must be subtracted from the size of the chunks here, by passing them as the last argument
        return ChunkedSendState.Plan(plannedMetadata, payload, MaxMessageSize, chunkMaxMessageSize, 0);
    }

    // Upstream: ProducerImpl#serializeAndSendMessage
    // Sends one chunk of a message. The metadata and the payload of the message are already complete, so they are not changed, except for the id of the chunk. The size of the frame is not checked.
    public async Task SendChunk(ChunkedSendState state, int chunkId, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
    {
        var sendPackage = _sendPackagePool.Get();

        try
        {
            var metadata = state.Metadata;

            if (state.IsChunked)
                metadata.ChunkId = chunkId;

            PrepareCommand(sendPackage, metadata, true);
            sendPackage.Payload = state.GetChunk(chunkId);

            // TODO(WP9-producer): Encrypt sendPackage.Payload here, which is the chunk. Every chunk is encrypted on its own, with its own encryption parameters in the metadata, and the metadata is serialized before the chunk is sent.

            await _connection.Send(sendPackage, responseTcs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendPackagePool.Return(sendPackage);
        }
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
