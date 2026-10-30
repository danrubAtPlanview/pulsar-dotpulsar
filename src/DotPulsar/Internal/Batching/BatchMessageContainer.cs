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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageContainerImpl.java, apache/pulsar-client-cpp@f71d7e9 lib/BatchMessageContainer.cc
public sealed class BatchMessageContainer<TItem> : AbstractBatchMessageContainer<TItem> where TItem : class, IBatchItem
{
    private const int ShrinkCoolingOffPeriod = 10;

    private readonly PreparedBatchPool _batchPool;
    private readonly BatchPayloadWriter _payloadWriter;
    private readonly List<TItem> _items;

    private int _consecutiveShrinkTime;

    // The 'clock' (monotonic, only used for the maximum publish delay) is a test seam.
    // 'maxNumMessagesInBatch' = 0 means no limit on the number of messages. 'maxBytesInBatch' = 0 means that the maximum message size is used.
    // The 'payloadWriter' can be shared by containers that are used by the same dispatcher.
    public BatchMessageContainer(
        uint maxNumMessagesInBatch,
        uint maxBytesInBatch,
        TimeSpan maxPublishDelay,
        PreparedBatchPool? batchPool = null,
        Func<TimeSpan>? clock = null,
        BatchPayloadWriter? payloadWriter = null)
        : base(maxNumMessagesInBatch, maxBytesInBatch, maxPublishDelay, clock)
    {
        _batchPool = batchPool ?? PreparedBatchPool.Shared;
        _payloadWriter = payloadWriter ?? new BatchPayloadWriter();
        _items = new List<TItem>(InitialMessagesNum);
        _consecutiveShrinkTime = 0;
        MaxBatchSize = InitialBatchBufferSize;
        MaxMessagesNum = InitialMessagesNum;
        LowestSequenceId = 0;
    }

    // The largest uncompressed size of a batch. Used as the capacity hint for the buffer of the next batch.
    public int MaxBatchSize { get; private set; }

    public int MaxMessagesNum { get; private set; }

    // The sequence id of the batch. The sequence id of the first message, unless it has been set by the caller.
    public ulong LowestSequenceId { get; set; }

    public override IReadOnlyList<TItem> Items => _items;

    // Upstream: BatchMessageContainerImpl#add
    public override bool Add(TItem item, int maxMessageSize)
    {
        if (IsEmpty)
            LowestSequenceId = item.Metadata.SequenceId;

        MessageAdded(item);
        _items.Add(item);

        return IsBatchFull(maxMessageSize);
    }

    // Upstream: BatchMessageContainerImpl#hasSameSchema
    public override bool HasSameSchema(TItem item)
    {
        if (IsEmpty)
            return true;

        var first = _items[0].Metadata;
        var metadata = item.Metadata;

        if (first.HasSchemaId && metadata.HasSchemaId)
            return first.SchemaId.Equals(metadata.SchemaId) && first.SchemaVersion.Equals(metadata.SchemaVersion);

        if (!first.HasSchemaVersion)
            return !metadata.HasSchemaVersion;

        return metadata.HasSchemaVersion && first.SchemaVersion.Equals(metadata.SchemaVersion);
    }

    public override void CreateBatches(List<PreparedBatchGroup> destination, int maxMessageSize)
    {
        if (!IsEmpty)
            destination.Add(CreateBatch(0, maxMessageSize));
    }

    // Upstream: BatchMessageContainerImpl#createOpSendMsg, #getCompressedBatchMetadataAndPayload (C# deviation: the payload is compressed when the batch is sent)
    // One message is not a batch: it is sent as it is. Several messages become a batch. 'start' is the index of the first message in the Items of the caller.
    public PreparedBatchGroup CreateBatch(int start, int maxMessageSize)
    {
        var count = _items.Count;
        PreparedBatchGroup group;

        if (count == 1)
        {
            var item = _items[0];
            var sequenceId = item.Metadata.SequenceId;
            UpdateMaxBatchSize((int) Math.Min(item.Data.Length, int.MaxValue));
            group = new PreparedBatchGroup(start, 1, null, sequenceId, sequenceId);
        }
        else
        {
            var batch = _batchPool.Get();

            try
            {
                batch.Buffer.Initialize(Math.Min(MaxBatchSize, maxMessageSize));

                for (var i = 0; i < count; ++i)
                {
                    var item = _items[i];
                    _payloadWriter.Write(batch.Buffer, item.Metadata, item.Data);
                }

                InitializeMetadata(batch, count);
            }
            catch
            {
                batch.Return();
                throw;
            }

            UpdateMaxBatchSize(batch.Buffer.WrittenCount);
            group = new PreparedBatchGroup(start, count, batch, batch.SequenceId, batch.HighestSequenceId);
        }

        MaxMessagesNum = Math.Max(MaxMessagesNum, NumMessagesInBatch);

        return group;
    }

    // Upstream: BatchMessageContainerImpl#updateMaxBatchSize
    public void UpdateMaxBatchSize(int uncompressedSize)
    {
        if (uncompressedSize > MaxBatchSize)
        {
            MaxBatchSize = uncompressedSize;
            _consecutiveShrinkTime = 0;
            return;
        }

        var shrank = MaxBatchSize - (MaxBatchSize >> 2);

        if (uncompressedSize > shrank)
        {
            _consecutiveShrinkTime = 0;
            return;
        }

        if (_consecutiveShrinkTime <= ShrinkCoolingOffPeriod)
        {
            ++_consecutiveShrinkTime;
            return;
        }

        MaxBatchSize = shrank;
        _consecutiveShrinkTime = 0;
    }

    // Upstream: BatchMessageContainerImpl#clear
    public override void Clear()
    {
        _items.Clear();
        ResetState();
        LowestSequenceId = 0;
    }

    // Upstream: Commands#initBatchMessageMetadata, BatchMessageContainerImpl#createOpSendMsg
    // The producer name and the publish time are set when the batch is sent
    private void InitializeMetadata(PreparedBatch batch, int count)
    {
        var first = _items[0].Metadata;
        var metadata = batch.Metadata;

        metadata.SequenceId = LowestSequenceId;

        if (first.HasPartitionKey)
        {
            metadata.PartitionKey = first.PartitionKey;
            metadata.PartitionKeyB64Encoded = first.PartitionKeyB64Encoded;
        }

        if (first.HasOrderingKey)
            metadata.OrderingKey = first.OrderingKey;

        if (first.HasReplicatedFrom)
            metadata.ReplicatedFrom = first.ReplicatedFrom;

        var replicateTo = first.ReplicateTo;
        for (var i = 0; i < replicateTo.Count; ++i)
        {
            metadata.ReplicateTo.Add(replicateTo[i]);
        }

        if (first.HasSchemaVersion)
            metadata.SchemaVersion = first.SchemaVersion;

        if (first.HasSchemaId)
            metadata.SchemaId = first.SchemaId;

        metadata.NumMessagesInBatch = count;
        metadata.HighestSequenceId = HighestSequenceId;
        CopyTransactionTo(metadata);

        batch.Count = count;
        batch.SequenceId = LowestSequenceId;
        batch.HighestSequenceId = HighestSequenceId;
    }
}
