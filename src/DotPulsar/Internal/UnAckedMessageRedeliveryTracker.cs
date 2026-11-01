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
using System.Diagnostics;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/UnAckedMessageRedeliveryTracker.java
// The tracker of an ack timeout with a redelivery backoff. It waits for the ack timeout exactly as UnAckedMessageTracker does, with the same buckets and ticks. An entry that
// expires is not redelivered at once: it waits for the delay that the backoff returns for the redelivery count of the entry, and then it is redelivered by the next tick.
// An entry that is acknowledged, negatively acknowledged or cleared while it waits for the delay is not redelivered.
public sealed class UnAckedMessageRedeliveryTracker : IUnAckedMessageTracker
{
    private readonly object _lock;
    private readonly Dictionary<(ulong LedgerId, ulong EntryId), int> _index;
    private readonly Dictionary<(ulong LedgerId, ulong EntryId), (MessageId MessageId, uint RedeliveryCount)>[] _buckets;
    private readonly Dictionary<(ulong LedgerId, ulong EntryId), (MessageId MessageId, TimeSpan DueTime)> _ackTimeoutMessages;
    private readonly Dictionary<(ulong LedgerId, ulong EntryId), uint> _localRedeliveryCounts;
    private readonly List<(ulong LedgerId, ulong EntryId)> _scratch;
    private readonly IRedeliveryBackoff _backoff;
    private readonly bool _countRedeliveriesLocally;
    private readonly TimeSpan _tickTime;
    private readonly Func<MessageId[], CancellationToken, ValueTask> _redeliver;
    private readonly Func<TimeSpan> _clock;
    private readonly Func<Action, IOneShotTimer> _timerFactory;
    private readonly CancellationTokenSource _cts;
    private readonly CancellationToken _cancellationToken;
    private IOneShotTimer? _timer;
    private int _head;
    private int _isDisposed;

    // The 'redeliver' delegate receives the message ids that are due, outside of the lock, and a token that is cancelled when the tracker is disposed.
    // The redeliveries are counted by the tracker when 'countRedeliveriesLocally' is true, which is for the subscriptions of which the broker does not count them.
    // The 'clock' (monotonic) and 'timerFactory' are test seams. The tick is the smaller of 'tickTime' and 'ackTimeout', and there is one more bucket than ticks in the ack timeout.
    public UnAckedMessageRedeliveryTracker(
        TimeSpan ackTimeout,
        TimeSpan tickTime,
        IRedeliveryBackoff backoff,
        bool countRedeliveriesLocally,
        Func<MessageId[], CancellationToken, ValueTask> redeliver,
        Func<TimeSpan>? clock = null,
        Func<Action, IOneShotTimer>? timerFactory = null)
    {
        var ackTimeoutMs = (long) ackTimeout.TotalMilliseconds;
        var tickMs = Math.Min((long) tickTime.TotalMilliseconds, ackTimeoutMs);

        if (tickMs <= 0)
            throw new ArgumentOutOfRangeException(nameof(tickTime), tickTime, "The ack timeout and the tick time must be at least one millisecond");

        var bucketCount = Math.Ceiling((double) ackTimeoutMs / tickMs) + 1;

        if (bucketCount > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(ackTimeout), ackTimeout, "The ack timeout is too large compared to the tick time");

        _lock = new();
        _index = [];
        _buckets = new Dictionary<(ulong LedgerId, ulong EntryId), (MessageId MessageId, uint RedeliveryCount)>[(int) bucketCount];
        _ackTimeoutMessages = [];
        _localRedeliveryCounts = [];
        _scratch = [];
        _backoff = backoff;
        _countRedeliveriesLocally = countRedeliveriesLocally;
        _tickTime = TimeSpan.FromMilliseconds(tickMs);
        _redeliver = redeliver;
        _clock = clock ?? CreateClock();
        _timerFactory = timerFactory ?? CreateTimer;
        _cts = new CancellationTokenSource();
        _cancellationToken = _cts.Token;
        _timer = null;
        _head = 0;
        _isDisposed = 0;

        for (var i = 0; i < _buckets.Length; ++i)
            _buckets[i] = [];
    }

    // The entries that wait for the ack timeout and the entries that wait for the delay of the backoff
    public int Count
    {
        get
        {
            lock (_lock)
                return _index.Count + _ackTimeoutMessages.Count;
        }
    }

    public bool IsEmpty
    {
        get
        {
            lock (_lock)
                return _index.Count == 0 && _ackTimeoutMessages.Count == 0;
        }
    }

    // The first firing is one tick after the start
    public void Start()
    {
        lock (_lock)
        {
            if (_timer is not null || _isDisposed != 0)
                return;

            _timer = _timerFactory(OnTimer);
            _timer.Schedule(_tickTime);
        }
    }

    public bool Add(MessageId messageId)
        => Add(messageId, 0);

    // The batch index is discarded, so a batch message is tracked by its entry. An entry that is already tracked is not moved to a newer bucket, and keeps its redelivery count.
    // Upstream: UnAckedMessageRedeliveryTracker#add (an entry that waits for the delay of the backoff can be tracked again, as upstream does)
    public bool Add(MessageId messageId, uint redeliveryCount)
    {
        var key = (messageId.LedgerId, messageId.EntryId);

        lock (_lock)
        {
            if (_index.ContainsKey(key))
                return false;

            var newest = (_head + _buckets.Length - 1) % _buckets.Length;
            _buckets[newest].Add(key, (messageId, redeliveryCount));
            _index.Add(key, newest);
            return true;
        }
    }

    public bool Remove(MessageId messageId)
        => Remove(messageId.LedgerId, messageId.EntryId);

    // Upstream: UnAckedMessageRedeliveryTracker#remove
    // The redelivery count that the tracker kept for the entry is forgotten. Upstream does not remove an entry from the ones that wait for the delay if it was in a bucket too, and this does.
    public bool Remove(ulong ledgerId, ulong entryId)
    {
        var key = (ledgerId, entryId);

        lock (_lock)
        {
            var removed = false;

            if (_index.TryGetValue(key, out var bucket))
            {
                _index.Remove(key);
                removed = _buckets[bucket].Remove(key);
            }

            _localRedeliveryCounts.Remove(key);
            return _ackTimeoutMessages.Remove(key) || removed;
        }
    }

    // Remove every tracked entry up to and including the entry of the message id. Partition and batch index are ignored.
    public int RemoveMessagesTill(MessageId messageId)
        => RemoveMessagesTill(messageId.LedgerId, messageId.EntryId);

    // Upstream: UnAckedMessageRedeliveryTracker#removeMessagesTill
    public int RemoveMessagesTill(ulong ledgerId, ulong entryId)
    {
        lock (_lock)
        {
            _scratch.Clear();

            foreach (var pair in _index)
            {
                if (IsUpTo(pair.Key, ledgerId, entryId))
                    _scratch.Add(pair.Key);
            }

            var removed = _scratch.Count;

            for (var i = 0; i < removed; ++i)
            {
                var key = _scratch[i];
                _buckets[_index[key]].Remove(key);
                _index.Remove(key);
            }

            _scratch.Clear();

            foreach (var pair in _ackTimeoutMessages)
            {
                if (IsUpTo(pair.Key, ledgerId, entryId))
                    _scratch.Add(pair.Key);
            }

            removed += _scratch.Count;

            for (var i = 0; i < _scratch.Count; ++i)
                _ackTimeoutMessages.Remove(_scratch[i]);

            _scratch.Clear();

            foreach (var pair in _localRedeliveryCounts)
            {
                if (IsUpTo(pair.Key, ledgerId, entryId))
                    _scratch.Add(pair.Key);
            }

            for (var i = 0; i < _scratch.Count; ++i)
                _localRedeliveryCounts.Remove(_scratch[i]);

            _scratch.Clear();
            return removed;
        }
    }

    // Upstream: UnAckedMessageRedeliveryTracker#clear
    // The redelivery counts that the tracker kept are not cleared, because a consumer of a subscription that is not Shared or KeyShared clears the tracker every time it redelivers.
    public void Clear()
    {
        lock (_lock)
        {
            _index.Clear();
            _ackTimeoutMessages.Clear();

            for (var i = 0; i < _buckets.Length; ++i)
                _buckets[i].Clear();
        }
    }

    // How many times the tracker has counted that the entry expired, which is the number of times that it was redelivered by the tracker, unless the tracker is not counting
    // Upstream: UnAckedMessageRedeliveryTracker#getLocalRedeliveryCount
    public uint GetLocalRedeliveryCount(ulong ledgerId, ulong entryId)
    {
        lock (_lock)
            return _localRedeliveryCounts.TryGetValue((ledgerId, entryId), out var count) ? count : 0;
    }

    // Expire the oldest bucket, which then becomes the newest one, and return the entries that are due. Returns null if there are none.
    // Every entry of the bucket gets a due time, which is now plus the delay of the backoff for the redelivery count of the entry. An entry without a delay is due at once.
    // Upstream: UnAckedMessageRedeliveryTracker#TimerTask.run, #triggerRedelivery
    public MessageId[]? Tick()
    {
        lock (_lock)
        {
            var oldest = _buckets[_head];
            _head = (_head + 1) % _buckets.Length;

            if (oldest.Count == 0 && _ackTimeoutMessages.Count == 0)
                return null;

            var now = _clock();

            foreach (var pair in oldest)
            {
                _index.Remove(pair.Key);
                _ackTimeoutMessages[pair.Key] = (pair.Value.MessageId, GetDueTime(pair.Key, pair.Value.RedeliveryCount, now));
            }

            oldest.Clear();
            return TakeDue(now);
        }
    }

    // Stops the timer and clears the tracker. Add, Remove and Tick still work afterwards (as the upstream close()), but nothing is redelivered.
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        IOneShotTimer? timer;

        lock (_lock)
        {
            timer = _timer;
            _timer = null;
        }

        _cts.Cancel();
        timer?.Dispose();
        Clear();

        lock (_lock)
            _localRedeliveryCounts.Clear();

        _cts.Dispose();
    }

    private static bool IsUpTo((ulong LedgerId, ulong EntryId) key, ulong ledgerId, ulong entryId)
        => key.LedgerId < ledgerId || (key.LedgerId == ledgerId && key.EntryId <= entryId);

    private static Func<TimeSpan> CreateClock()
    {
        var timestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;
        return () => TimeSpan.FromTicks((long) (Stopwatch.GetTimestamp() * timestampToTicks));
    }

    private static IOneShotTimer CreateTimer(Action callback)
        => new OneShotTimer(callback);

    // Upstream: UnAckedMessageRedeliveryTracker#addAckTimeoutMessages
    // The redelivery count is the one that the broker sent with the message. When the tracker counts the redeliveries, it is the number of times the entry has expired before this time.
    private TimeSpan GetDueTime((ulong LedgerId, ulong EntryId) key, uint redeliveryCount, TimeSpan now)
    {
        if (_countRedeliveriesLocally)
        {
            _ = _localRedeliveryCounts.TryGetValue(key, out redeliveryCount);
            _localRedeliveryCounts[key] = redeliveryCount + 1;
        }

        TimeSpan delay;

        try
        {
            delay = _backoff.Next(redeliveryCount);
        }
        catch
        {
            // The policy is code of the application, and the tick must not fail because of it
            delay = TimeSpan.Zero;
        }

        // A delay that would overflow means that the entry is never redelivered
        if (delay <= TimeSpan.Zero)
            return now;

        return delay >= TimeSpan.MaxValue - now ? TimeSpan.MaxValue : now + delay;
    }

    private MessageId[]? TakeDue(TimeSpan now)
    {
        _scratch.Clear();

        foreach (var pair in _ackTimeoutMessages)
        {
            if (pair.Value.DueTime <= now)
                _scratch.Add(pair.Key);
        }

        if (_scratch.Count == 0)
            return null;

        var due = new MessageId[_scratch.Count];

        for (var i = 0; i < due.Length; ++i)
        {
            due[i] = _ackTimeoutMessages[_scratch[i]].MessageId;
            _ackTimeoutMessages.Remove(_scratch[i]);
        }

        _scratch.Clear();
        return due;
    }

    // Upstream: UnAckedMessageRedeliveryTracker#TimerTask.run
    private void OnTimer()
    {
        MessageId[]? due;

        lock (_lock)
        {
            if (_isDisposed != 0 || _timer is null)
                return;

            try
            {
                due = Tick();
            }
            finally
            {
                _timer.Schedule(_tickTime);
            }
        }

        if (due is not null)
            _ = Redeliver(due);
    }

    private async Task Redeliver(MessageId[] due)
    {
        try
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            await _redeliver(due, _cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }
}
