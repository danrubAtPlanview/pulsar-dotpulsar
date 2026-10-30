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
using System.Diagnostics;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/AbstractBatchMessageContainer.java
public abstract class AbstractBatchMessageContainer<TItem> : IBatchMessageContainer<TItem> where TItem : class, IBatchItem
{
    protected const int InitialBatchBufferSize = 1024;
    protected const int InitialMessagesNum = 32;

    private static readonly double _timestampToTicks;

    private readonly uint _maxNumMessagesInBatch;
    private readonly uint _maxBytesInBatch;
    private readonly TimeSpan _maxPublishDelay;
    private readonly Func<TimeSpan> _clock;

    private TimeSpan _firstAddedTimestamp;
    private bool _hasTransaction;
    private ulong _transactionMostBits;
    private ulong _transactionLeastBits;

    static AbstractBatchMessageContainer()
        => _timestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;

    // The 'clock' (monotonic, only used for the maximum publish delay) is a test seam.
    // 'maxNumMessagesInBatch' = 0 means no limit on the number of messages. 'maxBytesInBatch' = 0 means that the maximum message size is used.
    protected AbstractBatchMessageContainer(uint maxNumMessagesInBatch, uint maxBytesInBatch, TimeSpan maxPublishDelay, Func<TimeSpan>? clock)
    {
        _maxNumMessagesInBatch = maxNumMessagesInBatch;
        _maxBytesInBatch = maxBytesInBatch;
        _maxPublishDelay = maxPublishDelay;
        _clock = clock ?? GetElapsedTime;
        _firstAddedTimestamp = TimeSpan.Zero;
        _hasTransaction = false;
        _transactionMostBits = 0;
        _transactionLeastBits = 0;
        NumMessagesInBatch = 0;
        CurrentBatchSize = 0;
        HighestSequenceId = 0;
    }

    public int NumMessagesInBatch { get; private set; }
    public long CurrentBatchSize { get; private set; }

    // The sequence id of the last message that was added
    public ulong HighestSequenceId { get; private set; }

    public bool IsEmpty => NumMessagesInBatch == 0;

    public abstract IReadOnlyList<TItem> Items { get; }

    public abstract bool HasSameSchema(TItem item);
    public abstract bool Add(TItem item, int maxMessageSize);
    public abstract void CreateBatches(List<PreparedBatchGroup> destination, int maxMessageSize);
    public abstract void Clear();

    // Upstream: AbstractBatchMessageContainer#haveEnoughSpace
    public bool HaveEnoughSpace(TItem item, int maxMessageSize)
    {
        var messageSize = item.Data.Length;

        return (
            (_maxBytesInBatch == 0 && (messageSize + CurrentBatchSize) <= maxMessageSize) ||
            (_maxBytesInBatch > 0 && (messageSize + CurrentBatchSize) <= _maxBytesInBatch)
        ) && (_maxNumMessagesInBatch == 0 || NumMessagesInBatch < _maxNumMessagesInBatch);
    }

    // Upstream: AbstractBatchMessageContainer#isBatchFull
    public bool IsBatchFull(int maxMessageSize)
        => (_maxBytesInBatch > 0 && CurrentBatchSize >= _maxBytesInBatch) ||
           (_maxBytesInBatch == 0 && CurrentBatchSize >= maxMessageSize) ||
           (_maxNumMessagesInBatch > 0 && NumMessagesInBatch >= _maxNumMessagesInBatch);

    // Upstream: AbstractBatchMessageContainer#hasSameTxn
    // A pure query. The transaction identity of the container is adopted from the first message that is added.
    public bool HasSameTxn(TItem item)
    {
        if (IsEmpty)
            return true;

        var metadata = item.Metadata;
        var messageHasTransaction = metadata.HasTxnidMostBits && metadata.HasTxnidLeastBits;

        if (messageHasTransaction != _hasTransaction)
            return false;

        if (!messageHasTransaction)
            return true;

        return _transactionMostBits == metadata.TxnidMostBits && _transactionLeastBits == metadata.TxnidLeastBits;
    }

    // Upstream: ProducerImpl#canAddToCurrentBatch
    public bool CanAddToCurrentBatch(TItem item, int maxMessageSize)
        => HaveEnoughSpace(item, maxMessageSize) && HasSameSchema(item) && HasSameTxn(item);

    public TimeSpan GetRemainingPublishDelay()
    {
        if (IsEmpty)
            return Timeout.InfiniteTimeSpan;

        var remaining = _maxPublishDelay - (_clock() - _firstAddedTimestamp);

        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    public bool ShouldFlush(int maxMessageSize)
        => !IsEmpty && (IsBatchFull(maxMessageSize) || GetRemainingPublishDelay() == TimeSpan.Zero);

    // Called when a message has been added. The first message adopts the transaction identity of the container.
    protected void MessageAdded(TItem item)
    {
        if (IsEmpty)
        {
            _firstAddedTimestamp = _clock();

            // Both are assigned unconditionally, so a plain first message resets the transaction identity
            var metadata = item.Metadata;
            _hasTransaction = metadata.HasTxnidMostBits && metadata.HasTxnidLeastBits;
            _transactionMostBits = _hasTransaction ? metadata.TxnidMostBits : 0;
            _transactionLeastBits = _hasTransaction ? metadata.TxnidLeastBits : 0;
        }

        ++NumMessagesInBatch;
        CurrentBatchSize += item.Data.Length;
        HighestSequenceId = item.Metadata.SequenceId;
    }

    protected void ResetState()
    {
        _firstAddedTimestamp = TimeSpan.Zero;
        _hasTransaction = false;
        _transactionMostBits = 0;
        _transactionLeastBits = 0;
        NumMessagesInBatch = 0;
        CurrentBatchSize = 0;
        HighestSequenceId = 0;
    }

    // Copies the identity of the transaction of the container to the metadata of a batch. Does nothing if the container is not transactional.
    protected void CopyTransactionTo(MessageMetadata metadata)
    {
        if (!_hasTransaction)
            return;

        metadata.TxnidMostBits = _transactionMostBits;
        metadata.TxnidLeastBits = _transactionLeastBits;
    }

    private static TimeSpan GetElapsedTime()
        => TimeSpan.FromTicks((long) (Stopwatch.GetTimestamp() * _timestampToTicks));
}
