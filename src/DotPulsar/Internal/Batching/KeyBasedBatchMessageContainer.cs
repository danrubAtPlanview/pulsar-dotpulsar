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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageKeyBasedContainer.java, apache/pulsar-client-cpp@f71d7e9 lib/BatchMessageKeyBasedContainer.cc
public sealed class KeyBasedBatchMessageContainer<TItem> : AbstractBatchMessageContainer<TItem> where TItem : class, IBatchItem
{
    private const int MaximumFreeContainers = 1024;

    private readonly uint _maxNumMessagesInBatch;
    private readonly uint _maxBytesInBatch;
    private readonly TimeSpan _maxPublishDelay;
    private readonly PreparedBatchPool _batchPool;
    private readonly Func<TimeSpan>? _clock;
    private readonly BatchPayloadWriter _payloadWriter;
    private readonly Dictionary<BatchKey, BatchMessageContainer<TItem>> _batches;
    private readonly Stack<BatchMessageContainer<TItem>> _freeContainers;
    private readonly List<BatchMessageContainer<TItem>> _sortedContainers;
    private readonly List<TItem> _orderedItems;

    // The 'clock' (monotonic, only used for the maximum publish delay) is a test seam.
    // 'maxNumMessagesInBatch' = 0 means no limit on the number of messages. 'maxBytesInBatch' = 0 means that the maximum message size is used.
    // The limits apply to all messages in the container, not to the messages of one key.
    public KeyBasedBatchMessageContainer(
        uint maxNumMessagesInBatch,
        uint maxBytesInBatch,
        TimeSpan maxPublishDelay,
        PreparedBatchPool? batchPool = null,
        Func<TimeSpan>? clock = null)
        : base(maxNumMessagesInBatch, maxBytesInBatch, maxPublishDelay, clock)
    {
        _maxNumMessagesInBatch = maxNumMessagesInBatch;
        _maxBytesInBatch = maxBytesInBatch;
        _maxPublishDelay = maxPublishDelay;
        _batchPool = batchPool ?? PreparedBatchPool.Shared;
        _clock = clock;
        _payloadWriter = new BatchPayloadWriter();
        _batches = [];
        _freeContainers = new Stack<BatchMessageContainer<TItem>>();
        _sortedContainers = [];
        _orderedItems = new List<TItem>(InitialMessagesNum);
    }

    // The messages in the order they are sent. Only valid after CreateBatches and until Clear.
    public override IReadOnlyList<TItem> Items => _orderedItems;

    // Upstream: BatchMessageKeyBasedContainer#add
    public override bool Add(TItem item, int maxMessageSize)
    {
        var key = new BatchKey(item.Metadata);

        if (!_batches.TryGetValue(key, out var batch))
        {
            batch = _freeContainers.Count > 0
                ? _freeContainers.Pop()
                : new BatchMessageContainer<TItem>(_maxNumMessagesInBatch, _maxBytesInBatch, _maxPublishDelay, _batchPool, _clock, _payloadWriter);

            _batches.Add(key, batch);
        }

        batch.Add(item, maxMessageSize);
        MessageAdded(item);

        return IsBatchFull(maxMessageSize);
    }

    // Upstream: BatchMessageKeyBasedContainer#hasSameSchema
    public override bool HasSameSchema(TItem item)
        => !_batches.TryGetValue(new BatchKey(item.Metadata), out var batch) || batch.HasSameSchema(item);

    // Upstream: BatchMessageKeyBasedContainer#createOpSendMsgs
    // The sequence ids of the messages of a key are not ordered compared to the other keys, for example:
    // key A has 0, 3, 4 and key B has 1, 2. The batches are sent in the order B (1, 2) and A (0, 3, 4), which the broker would
    // see as a sequence id going backwards for A. So the sequence id of a batch is its highest sequence id, and
    // the batches are sent ordered by that. The groups are appended to the destination in that order.
    public override void CreateBatches(List<PreparedBatchGroup> destination, int maxMessageSize)
    {
        var initialCount = destination.Count;

        _sortedContainers.Clear();
        _orderedItems.Clear();

        foreach (var batch in _batches.Values)
        {
            batch.LowestSequenceId = batch.HighestSequenceId;
            _sortedContainers.Add(batch);
        }

        _sortedContainers.Sort(static (x, y) => x.LowestSequenceId.CompareTo(y.LowestSequenceId));

        try
        {
            for (var i = 0; i < _sortedContainers.Count; ++i)
            {
                var batch = _sortedContainers[i];
                destination.Add(batch.CreateBatch(_orderedItems.Count, maxMessageSize));

                var items = batch.Items;
                for (var j = 0; j < items.Count; ++j)
                {
                    _orderedItems.Add(items[j]);
                }
            }
        }
        catch
        {
            // The batches that were built never reach the caller
            for (var i = initialCount; i < destination.Count; ++i)
            {
                destination[i].Batch?.Return();
            }

            destination.RemoveRange(initialCount, destination.Count - initialCount);
            _orderedItems.Clear();
            throw;
        }
    }

    // Upstream: BatchMessageKeyBasedContainer#clear
    public override void Clear()
    {
        foreach (var batch in _batches.Values)
        {
            batch.Clear();

            if (_freeContainers.Count < MaximumFreeContainers)
                _freeContainers.Push(batch);
        }

        _batches.Clear();
        _sortedContainers.Clear();
        _orderedItems.Clear();
        ResetState();
    }
}
