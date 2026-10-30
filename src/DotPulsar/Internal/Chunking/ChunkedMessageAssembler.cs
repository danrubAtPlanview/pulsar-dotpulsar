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

namespace DotPulsar.Internal.Chunking;

using DotPulsar.Internal.PulsarApi;
using System.Buffers;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java (ChunkedMessageCtx, doProcessMessageChunk, removeOldestPendingChunkedMessage, removeExpireIncompleteChunkedMessages, removeChunkMessage), apache/pulsar-client-cpp@f71d7e9 lib/ConsumerImpl.cc (processMessageChunk)
public sealed class ChunkedMessageAssembler : IDisposable
{
    private readonly object _lock;
    private readonly uint _maxPendingChunkedMessage;
    private readonly bool _autoAckOldestChunkedMessageOnQueueFull;
    private readonly long _expireTimeOfIncompleteChunkedMessageMs;
    private readonly Func<long> _utcNowMs;
    private readonly Func<IReadOnlyList<MessageIdData>, ValueTask>? _acknowledgeExpired;
    private readonly Dictionary<string, Context> _contexts;
    private readonly List<MessageIdData> _expired;
    private Context? _oldest;
    private Context? _newest;
    private Timer? _expiryTimer;
    private int _isSweeping;
    private int _isDisposed;

    // The assembler is shared by the receive loop and an expiry timer, and all state is guarded by one lock. A zero maxPendingChunkedMessage means no limit,
    // and a zero expireTimeOfIncompleteChunkedMessage disables expiry. The utcNowMs is the clock, in milliseconds since the Unix epoch.
    // If acknowledgeExpired is set, a timer is started when the first chunk is processed, and it hands the ids of the chunks of the expired messages
    // to acknowledgeExpired (fire and forget, errors are ignored). If it is null there is no timer, and the owner must call RemoveExpired itself.
    public ChunkedMessageAssembler(
        uint maxPendingChunkedMessage,
        bool autoAckOldestChunkedMessageOnQueueFull,
        TimeSpan expireTimeOfIncompleteChunkedMessage,
        Func<long> utcNowMs,
        Func<IReadOnlyList<MessageIdData>, ValueTask>? acknowledgeExpired)
    {
        _lock = new object();
        _maxPendingChunkedMessage = maxPendingChunkedMessage;
        _autoAckOldestChunkedMessageOnQueueFull = autoAckOldestChunkedMessageOnQueueFull;
        _expireTimeOfIncompleteChunkedMessageMs = (long) expireTimeOfIncompleteChunkedMessage.TotalMilliseconds;
        _utcNowMs = utcNowMs;
        _acknowledgeExpired = acknowledgeExpired;
        _contexts = [];
        _expired = [];
        _oldest = null;
        _newest = null;
        _expiryTimer = null;
        _isSweeping = 0;
        _isDisposed = 0;
    }

    // The number of incomplete chunked messages
    public int PendingCount
    {
        get
        {
            lock (_lock)
                return _contexts.Count;
        }
    }

    // The number of incomplete chunked messages in the queue that keeps them in the order they were started. It always equals PendingCount.
    public int QueueCount
    {
        get
        {
            lock (_lock)
            {
                var count = 0;
                for (var context = _oldest; context is not null; context = context.Newer)
                    ++count;
                return count;
            }
        }
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk
    // Processes a chunk of a chunked message (see IsChunkedMessage). The chunkPayload must be the decrypted payload of the chunk, and it must stay valid
    // until the message is delivered, because the chunks are linked together and not copied. Returns the message when the chunk completes it, otherwise null.
    // The caller must acknowledge every id that is added to acksToSend, and track every id that is added to idsToTrack (only if ack timeout is enabled),
    // after the call. The lists are not cleared. Every id that is added to a list is the id of a chunk that will never be part of a delivered message,
    // which is either the chunk that was passed in, or chunks of messages that were dropped because of it.
    public AssembledChunkedMessage? Process(MessageIdData messageId, MessageMetadata metadata, ReadOnlySequence<byte> chunkPayload, List<MessageIdData> acksToSend, List<MessageIdData> idsToTrack)
    {
        lock (_lock)
        {
            if (_isDisposed != 0)
                return null;

            EnsureExpiryTimer();

            var chunkId = metadata.ChunkId;
            var numberOfChunks = metadata.NumChunksFromMsg;
            _contexts.TryGetValue(metadata.Uuid, out var context);

            if (numberOfChunks < 2 || chunkId < 0 || chunkId >= numberOfChunks)
            {
                // The chunk is not a chunk of a message with more than one chunk, so it can not be used for anything
                if (context is not null)
                    Remove(context);

                Discard(messageId, metadata, acksToSend, idsToTrack);
                return null;
            }

            if (chunkId == 0)
            {
                if (context is not null)
                {
                    // The first chunk is received again. If it is a redelivery of the same entry, then the broker will redeliver the other chunks as well and
                    // nothing is acknowledged. If it is another entry, then the chunks that were received belong to a corrupted message that will never be
                    // delivered. They are acknowledged here to avoid an ack hole.
                    if (!Contains(context.ChunkIds, messageId))
                        AddChunkIds(context.ChunkIds, acksToSend);

                    Remove(context);
                }

                if (_maxPendingChunkedMessage > 0 && _contexts.Count >= _maxPendingChunkedMessage)
                    RemoveOldestPending(acksToSend, idsToTrack);

                context = new Context(metadata.Uuid, numberOfChunks, metadata.TotalChunkMsgSize, _utcNowMs());
                _contexts[context.Uuid] = context;
                Enqueue(context);
            }

            if (context is null || chunkId != context.LastChunkId + 1)
            {
                if (context is not null && chunkId <= context.LastChunkId)
                {
                    // A duplicated chunk is filtered. It is only acknowledged if it is not a redelivery of an entry that was already received.
                    if (!Contains(context.ChunkIds, messageId))
                        acksToSend.Add(messageId);

                    return null;
                }

                // The first chunk was lost, or a chunk in the middle
                if (context is not null)
                    Remove(context);

                Discard(messageId, metadata, acksToSend, idsToTrack);
                return null;
            }

            var length = chunkPayload.Length;
            if (context.WrittenBytes + length > context.TotalChunkMsgSize)
            {
                // The chunk does not fit in the total size of the message. Java throws, which would leave the chunks unacknowledged and the context in the map.
                // The message can never be completed, so the chunks are acknowledged.
                AddChunkIds(context.ChunkIds, acksToSend);
                acksToSend.Add(messageId);
                Remove(context);
                return null;
            }

            context.ChunkIds[chunkId] = messageId;
            context.Append(chunkPayload);
            context.LastChunkId = chunkId;

            if (chunkId != context.TotalChunks - 1)
                return null;

            Remove(context);
            return new AssembledChunkedMessage(context.BuildPayload(), context.ChunkIds);
        }
    }

    // Upstream: ConsumerImpl#removeExpireIncompleteChunkedMessages
    // Removes the incomplete chunked messages that were started more than the expire time ago, and adds the ids of their chunks to acksToSend.
    // It does nothing if expiry is disabled. The caller must acknowledge the ids.
    public void RemoveExpired(List<MessageIdData> acksToSend)
    {
        if (_expireTimeOfIncompleteChunkedMessageMs <= 0)
            return;

        lock (_lock)
        {
            var now = _utcNowMs();

            // The queue is ordered by when the messages were started, so nothing behind the first message that is not expired is expired
            while (_oldest is not null && now > _oldest.ReceivedTimeMs + _expireTimeOfIncompleteChunkedMessageMs)
                RemoveChunkMessage(_oldest, acksToSend);
        }
    }

    // Drops all incomplete chunked messages without acknowledging them, which is what to do when the consumer is repositioned
    public void Clear()
    {
        lock (_lock)
            ClearContexts();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        lock (_lock)
        {
            _expiryTimer?.Dispose();
            _expiryTimer = null;
            ClearContexts();
        }
    }

    private void ClearContexts()
    {
        _contexts.Clear();
        _oldest = null;
        _newest = null;
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk (L1920-L1926)
    // A chunk that can not be used. It is acknowledged if it is old enough that the broker's redelivery can not be expected to complete the message,
    // otherwise it is tracked, so it is redelivered if the consumer has an ack timeout.
    private void Discard(MessageIdData messageId, MessageMetadata metadata, List<MessageIdData> acksToSend, List<MessageIdData> idsToTrack)
    {
        if (_expireTimeOfIncompleteChunkedMessageMs > 0 && _utcNowMs() > (long) metadata.PublishTime + _expireTimeOfIncompleteChunkedMessageMs)
            acksToSend.Add(messageId);
        else
            idsToTrack.Add(messageId);
    }

    // Upstream: ConsumerImpl#removeOldestPendingChunkedMessage
    private void RemoveOldestPending(List<MessageIdData> acksToSend, List<MessageIdData> idsToTrack)
    {
        if (_oldest is not null)
            RemoveChunkMessage(_oldest, _autoAckOldestChunkedMessageOnQueueFull ? acksToSend : idsToTrack);
    }

    // Upstream: ConsumerImpl#removeChunkMessage
    private void RemoveChunkMessage(Context context, List<MessageIdData> destination)
    {
        Remove(context);
        AddChunkIds(context.ChunkIds, destination);
        context.Release();
    }

    private void Remove(Context context)
    {
        _contexts.Remove(context.Uuid);

        if (context.Older is null)
            _oldest = context.Newer;
        else
            context.Older.Newer = context.Newer;

        if (context.Newer is null)
            _newest = context.Older;
        else
            context.Newer.Older = context.Older;

        context.Older = null;
        context.Newer = null;
    }

    private void Enqueue(Context context)
    {
        context.Older = _newest;

        if (_newest is null)
            _oldest = context;
        else
            _newest.Newer = context;

        _newest = context;
    }

    private static void AddChunkIds(MessageIdData[] chunkIds, List<MessageIdData> destination)
    {
        foreach (var chunkId in chunkIds)
        {
            if (chunkId is not null)
                destination.Add(chunkId);
        }
    }

    private static bool Contains(MessageIdData[] chunkIds, MessageIdData messageId)
    {
        foreach (var chunkId in chunkIds)
        {
            if (chunkId is not null && chunkId.LedgerId == messageId.LedgerId && chunkId.EntryId == messageId.EntryId)
                return true;
        }

        return false;
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk (L1787-L1796)
    private void EnsureExpiryTimer()
    {
        if (_expiryTimer is not null || _acknowledgeExpired is null || _expireTimeOfIncompleteChunkedMessageMs <= 0)
            return;

        var period = TimeSpan.FromMilliseconds(Math.Min(_expireTimeOfIncompleteChunkedMessageMs, int.MaxValue));
        _expiryTimer = new Timer(static state => ((ChunkedMessageAssembler) state!).RemoveExpiredAndAcknowledge(), this, period, period);
    }

    private void RemoveExpiredAndAcknowledge()
    {
        if (_isDisposed != 0 || Interlocked.CompareExchange(ref _isSweeping, 1, 0) != 0)
            return;

        try
        {
            RemoveExpired(_expired);

            if (_expired.Count == 0)
                return;

            var messageIds = _expired.ToArray();
            _expired.Clear();
            _ = AcknowledgeExpired(messageIds);
        }
        finally
        {
            Volatile.Write(ref _isSweeping, 0);
        }
    }

    private async Task AcknowledgeExpired(MessageIdData[] messageIds)
    {
        try
        {
            await _acknowledgeExpired!(messageIds).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Like Java, a failed acknowledgement is ignored. The broker redelivers the chunks when the consumer reconnects.
        }
    }

    // Upstream: ConsumerImpl#ChunkedMessageCtx
    private sealed class Context
    {
        private Segment? _first;
        private Segment? _last;

        public Context(string uuid, int totalChunks, int totalChunkMsgSize, long receivedTimeMs)
        {
            Uuid = uuid;
            TotalChunks = totalChunks;
            TotalChunkMsgSize = totalChunkMsgSize;
            ReceivedTimeMs = receivedTimeMs;
            ChunkIds = new MessageIdData[totalChunks];
            LastChunkId = -1;
            WrittenBytes = 0;
            Older = null;
            Newer = null;
            _first = null;
            _last = null;
        }

        public string Uuid { get; }
        public int TotalChunks { get; }
        public int TotalChunkMsgSize { get; }
        public long ReceivedTimeMs { get; }

        // The id of the entry of every chunk that is received. The elements of the chunks that are not received are null.
        public MessageIdData[] ChunkIds { get; }

        public int LastChunkId { get; set; }
        public long WrittenBytes { get; private set; }

        // The neighbours in the queue of the incomplete messages. The older ones were started before this one.
        public Context? Older { get; set; }
        public Context? Newer { get; set; }

        public void Append(ReadOnlySequence<byte> payload)
        {
            foreach (var memory in payload)
            {
                if (memory.IsEmpty)
                    continue;

                if (_last is null)
                {
                    _first = new Segment(memory, 0);
                    _last = _first;
                }
                else
                    _last = _last.Append(memory);
            }

            WrittenBytes += payload.Length;
        }

        public ReadOnlySequence<byte> BuildPayload()
            => _first is null || _last is null
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(_first, 0, _last, _last.Memory.Length);

        // Lets go of the payload, so the memory can be collected even if something still refers to the context
        public void Release()
        {
            _first = null;
            _last = null;
        }
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory, RunningIndex + Memory.Length);
            Next = next;
            return next;
        }
    }
}
