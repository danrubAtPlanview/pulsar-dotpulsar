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
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf.Collections;
using System.Buffers;

public sealed class BatchHandler<TMessage>
{
    private readonly object _lock;
    private readonly bool _trackBatches;
    private readonly IMessageFactory<TMessage> _messageFactory;
    private readonly string _topic;
    private readonly Queue<IMessage<TMessage>> _messages;
    private readonly Dictionary<EntryKey, BatchAckTracker> _trackers;
    private readonly List<EntryKey> _removals;

    public BatchHandler(bool trackBatches, IMessageFactory<TMessage> messageFactory, string topic)
    {
        _lock = new object();
        _trackBatches = trackBatches;
        _messageFactory = messageFactory;
        _topic = topic;
        _messages = new Queue<IMessage<TMessage>>();
        _trackers = [];
        _removals = [];
    }

    // Upstream: ConsumerImpl#receiveIndividualMessagesFromBatch
    // Returns the first message that is to be delivered, or null if every message of the batch was skipped.
    // chargedButSkipped is the number of messages that were skipped although the broker has deducted permits for them.
    public IMessage<TMessage>? Add(MessageIdData messageId, RepeatedField<long>? brokerAckSet, uint redeliveryCount, MessageMetadata metadata, ReadOnlySequence<byte> data, out int chargedButSkipped)
    {
        chargedButSkipped = 0;

        var numberOfMessages = metadata.NumMessagesInBatch;
        var filterAcknowledged = brokerAckSet is not null && brokerAckSet.Count > 0;
        var tracker = _trackBatches && numberOfMessages > 0 ? new BatchAckTracker(messageId, numberOfMessages, brokerAckSet) : null;
        var messages = ArrayPool<IMessage<TMessage>>.Shared.Rent(numberOfMessages);
        var deliverable = 0;

        try
        {
            long index = 0;

            for (var i = 0; i < numberOfMessages; ++i)
            {
                // The messages that are skipped must still be parsed, so the position in the data is moved to the next message
                var singleMetadataSize = data.ReadUInt32(index, true);
                index += 4;
                var singleMetadata = SingleMessageMetadata.Parser.ParseFrom(data.Slice(index, singleMetadataSize));
                index += singleMetadataSize;

                if (filterAcknowledged && IsAcknowledgedByBroker(brokerAckSet!, i))
                {
                    index += (uint) singleMetadata.PayloadSize;
                    continue;
                }

                if (singleMetadata.CompactedOut)
                {
                    tracker?.AckIndividual(i);
                    ++chargedButSkipped;
                    index += (uint) singleMetadata.PayloadSize;
                    continue;
                }

                var singleMessageId = new MessageId(messageId.LedgerId, messageId.EntryId, messageId.Partition, i, _topic, numberOfMessages);
                messages[deliverable++] = _messageFactory.Create(singleMessageId, redeliveryCount, data.Slice(index, singleMetadata.PayloadSize), metadata, singleMetadata);
                index += (uint) singleMetadata.PayloadSize;
            }

            lock (_lock)
            {
                if (tracker is not null)
                    Track(tracker);

                for (var i = 0; i < deliverable; ++i)
                {
                    _messages.Enqueue(messages[i]);
                }

                return _messages.Count == 0 ? null : _messages.Dequeue();
            }
        }
        finally
        {
            ArrayPool<IMessage<TMessage>>.Shared.Return(messages, true);
        }
    }

    public IMessage<TMessage>? GetNext()
    {
        lock (_lock)
            return _messages.Count == 0 ? null : _messages.Dequeue();
    }

    public void Clear()
    {
        lock (_lock)
        {
            _messages.Clear();
            _trackers.Clear();
        }
    }

    public bool TryGetBatchSize(MessageIdData messageId, out int batchSize)
    {
        lock (_lock)
        {
            if (_trackers.TryGetValue(new EntryKey(messageId), out var tracker))
            {
                batchSize = tracker.BatchSize;
                return true;
            }
        }

        batchSize = 0;
        return false;
    }

    public bool IsTracked(MessageIdData messageId)
    {
        lock (_lock)
            return _trackers.ContainsKey(new EntryKey(messageId));
    }

    // Upstream: MessageIdAdvUtils#acknowledge (individual)
    // Returns null if the batch is not tracked. The tracker is removed when all messages of the batch are acknowledged.
    public BatchAckTracker? AcknowledgeIndividual(MessageIdData messageId, out bool isComplete)
    {
        lock (_lock)
        {
            var key = new EntryKey(messageId);

            if (!_trackers.TryGetValue(key, out var tracker))
            {
                isComplete = false;
                return null;
            }

            isComplete = tracker.AckIndividual(messageId.BatchIndex);

            if (isComplete)
                _trackers.Remove(key);

            return tracker;
        }
    }

    // Upstream: MessageIdAdvUtils#acknowledge (cumulative)
    // Returns null if the batch is not tracked. The trackers of the earlier entries are removed,
    // and so is the tracker of the batch when all messages of the batch are acknowledged.
    public BatchAckTracker? AcknowledgeCumulative(MessageIdData messageId, out bool isComplete)
    {
        lock (_lock)
        {
            RemoveTrackers(messageId.LedgerId, messageId.EntryId, messageId.Partition, false);

            var key = new EntryKey(messageId);

            if (!_trackers.TryGetValue(key, out var tracker))
            {
                isComplete = false;
                return null;
            }

            isComplete = tracker.AckCumulative(messageId.BatchIndex);

            if (isComplete)
                _trackers.Remove(key);

            return tracker;
        }
    }

    // Removes the trackers of the entries up to and including the entry of the message id
    public void PruneUpTo(MessageIdData messageId)
    {
        lock (_lock)
            RemoveTrackers(messageId.LedgerId, messageId.EntryId, messageId.Partition, true);
    }

    public void CopyAckSetTo(BatchAckTracker tracker, RepeatedField<long> target)
    {
        lock (_lock)
            tracker.CopyAckSetTo(target);
    }

    private static bool IsAcknowledgedByBroker(RepeatedField<long> brokerAckSet, int batchIndex)
    {
        var word = batchIndex >> 6;
        return word >= brokerAckSet.Count || (brokerAckSet[word] & (1L << (batchIndex & 63))) == 0;
    }

    // Must be called while holding the lock
    // Upstream: ConsumerImpl#receiveIndividualMessagesFromBatch (every delivery of a batch gets its own BatchMessageAcker)
    // When an entry is delivered again, the tracker of the earlier delivery is replaced and never merged with the new one. The broker
    // delivers the entry again with its own view of what is acknowledged, and what the client has cleared in the old tracker may not
    // have been acknowledged (a transactional cumulative acknowledgment clears bits, and the transaction can then be aborted).
    // Merging them would complete the batch too early, so that messages that were not acknowledged again are lost.
    private void Track(BatchAckTracker tracker)
    {
        if (tracker.IsEmpty)
            _trackers.Remove(tracker.Key);
        else
            _trackers[tracker.Key] = tracker;
    }

    // Must be called while holding the lock
    private void RemoveTrackers(ulong ledgerId, ulong entryId, int partition, bool includeEntry)
    {
        if (_trackers.Count == 0)
            return;

        _removals.Clear();

        foreach (var key in _trackers.Keys)
        {
            if (key.Partition != partition)
                continue;

            var comparison = key.LedgerId != ledgerId ? key.LedgerId.CompareTo(ledgerId) : key.EntryId.CompareTo(entryId);

            if (comparison < 0 || (includeEntry && comparison == 0))
                _removals.Add(key);
        }

        for (var i = 0; i < _removals.Count; ++i)
        {
            _trackers.Remove(_removals[i]);
        }
    }
}
