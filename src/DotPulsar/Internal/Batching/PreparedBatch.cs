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

namespace DotPulsar.Internal.Batching;

using DotPulsar.Internal.PulsarApi;
using System.Buffers;

// A batch of more than one message: the batch metadata and the uncompressed payload. Compression is applied when the batch is sent.
public sealed class PreparedBatch
{
    private readonly PreparedBatchPool _pool;

    private int _isReturned;
    private int _isWriting;
    private int _isReturnRequested;

    public PreparedBatch(PreparedBatchPool pool)
    {
        _pool = pool;
        _isReturned = 0;
        _isWriting = 0;
        _isReturnRequested = 0;
        Buffer = new PooledBufferWriter();
        Metadata = new MessageMetadata();
        Count = 0;
        SequenceId = 0;
        HighestSequenceId = 0;
    }

    public PooledBufferWriter Buffer { get; }
    public MessageMetadata Metadata { get; }
    public int Count { get; set; }

    // The sequence id of the batch (the lowest, or the highest for key based batching)
    public ulong SequenceId { get; set; }

    public ulong HighestSequenceId { get; set; }

    public bool IsReturned => Volatile.Read(ref _isReturned) != 0;

    public ReadOnlySequence<byte> Payload => new(Buffer.WrittenMemory);

    public void Reset()
    {
        Buffer.Reset();
        Count = 0;
        SequenceId = 0;
        HighestSequenceId = 0;

        Metadata.Properties.Clear();
        Metadata.ReplicateTo.Clear();
        Metadata.EncryptionKeys.Clear();
        Metadata.CompactedBatchIndexes.Clear();
        Metadata.ClearProducerName();
        Metadata.ClearSequenceId();
        Metadata.ClearPublishTime();
        Metadata.ClearReplicatedFrom();
        Metadata.ClearPartitionKey();
        Metadata.ClearCompression();
        Metadata.ClearUncompressedSize();
        Metadata.ClearNumMessagesInBatch();
        Metadata.ClearEventTime();
        Metadata.ClearEncryptionAlgo();
        Metadata.ClearEncryptionParam();
        Metadata.ClearSchemaVersion();
        Metadata.ClearPartitionKeyB64Encoded();
        Metadata.ClearOrderingKey();
        Metadata.ClearDeliverAtTime();
        Metadata.ClearMarkerType();
        Metadata.ClearTxnidLeastBits();
        Metadata.ClearTxnidMostBits();
        Metadata.ClearHighestSequenceId();
        Metadata.ClearNullValue();
        Metadata.ClearUuid();
        Metadata.ClearNumChunksFromMsg();
        Metadata.ClearTotalChunkMsgSize();
        Metadata.ClearChunkId();
        Metadata.ClearNullPartitionKey();
        Metadata.ClearSchemaId();
    }

    public void MarkRented()
    {
        Volatile.Write(ref _isWriting, 0);
        Volatile.Write(ref _isReturnRequested, 0);
        Volatile.Write(ref _isReturned, 0);
    }

    // The dispatcher writes the batch to the channel, which changes the metadata of the batch while it writes and resets it afterwards. A receipt can be processed before the write has returned, so the batch
    // is not given back to the pool while it is written: a Return that comes during the write is done by EndWrite. Both sides write their own flag and then read the flag of the other, with a full fence
    // in between, so at least one of them sees the other, and Return does nothing for a batch that has been given back already.
    public void BeginWrite()
        => _ = Interlocked.Exchange(ref _isWriting, 1);

    public void EndWrite()
    {
        _ = Interlocked.Exchange(ref _isWriting, 0);

        if (Volatile.Read(ref _isReturnRequested) != 0)
            GiveBack();
    }

    // Resets the batch and gives it back to the pool, when it is not being written (otherwise when the write ends). Does nothing if it has already been returned.
    public void Return()
    {
        _ = Interlocked.Exchange(ref _isReturnRequested, 1);

        if (Volatile.Read(ref _isWriting) == 0)
            GiveBack();
    }

    private void GiveBack()
    {
        if (Interlocked.Exchange(ref _isReturned, 1) != 0)
            return;

        _pool.Return(this);
    }
}
