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

    public PreparedBatch(PreparedBatchPool pool)
    {
        _pool = pool;
        _isReturned = 0;
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
        => Volatile.Write(ref _isReturned, 0);

    // Resets the batch and gives it back to the pool. Does nothing if it has already been returned.
    public void Return()
    {
        if (Interlocked.Exchange(ref _isReturned, 1) != 0)
            return;

        _pool.Return(this);
    }
}
