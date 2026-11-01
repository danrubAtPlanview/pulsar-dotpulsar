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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/UnAckedMessageTracker.java, apache/pulsar-client-cpp@f71d7e9 lib/UnAckedMessageTrackerEnabled.cc
public sealed class UnAckedMessageTracker : IUnAckedMessageTracker
{
    private readonly object _lock;
    private readonly Dictionary<(ulong LedgerId, ulong EntryId), int> _index;
    private readonly Dictionary<(ulong LedgerId, ulong EntryId), MessageId>[] _buckets;
    private readonly List<(ulong LedgerId, ulong EntryId)> _scratch;
    private readonly TimeSpan _tickTime;
    private readonly Func<MessageId[], CancellationToken, ValueTask> _redeliver;
    private readonly Func<Action, IOneShotTimer> _timerFactory;
    private readonly CancellationTokenSource _cts;
    private readonly CancellationToken _cancellationToken;
    private IOneShotTimer? _timer;
    private int _head;
    private int _isDisposed;

    // The 'redeliver' delegate receives the expired message ids outside of the lock, and a token that is cancelled when the tracker is disposed.
    // The 'timerFactory' is a test seam. The tick is the smaller of 'tickTime' and 'ackTimeout', and there is one more bucket than ticks in the ack timeout.
    public UnAckedMessageTracker(
        TimeSpan ackTimeout,
        TimeSpan tickTime,
        Func<MessageId[], CancellationToken, ValueTask> redeliver,
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
        _buckets = new Dictionary<(ulong LedgerId, ulong EntryId), MessageId>[(int) bucketCount];
        _scratch = [];
        _tickTime = TimeSpan.FromMilliseconds(tickMs);
        _redeliver = redeliver;
        _timerFactory = timerFactory ?? CreateTimer;
        _cts = new CancellationTokenSource();
        _cancellationToken = _cts.Token;
        _timer = null;
        _head = 0;
        _isDisposed = 0;

        for (var i = 0; i < _buckets.Length; ++i)
            _buckets[i] = [];
    }

    public int Count
    {
        get
        {
            lock (_lock)
                return _index.Count;
        }
    }

    public bool IsEmpty
    {
        get
        {
            lock (_lock)
                return _index.Count == 0;
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

    // The batch index is discarded, so a batch message is tracked by its entry. An entry that is already tracked is not moved to a newer bucket.
    public bool Add(MessageId messageId)
    {
        var key = (messageId.LedgerId, messageId.EntryId);

        lock (_lock)
        {
            if (_index.ContainsKey(key))
                return false;

            var newest = (_head + _buckets.Length - 1) % _buckets.Length;
            _buckets[newest].Add(key, messageId);
            _index.Add(key, newest);
            return true;
        }
    }

    // The redelivery count is not used, like the 'add(MessageId, int)' of the upstream tracker
    public bool Add(MessageId messageId, uint redeliveryCount)
        => Add(messageId);

    public bool Remove(MessageId messageId)
        => Remove(messageId.LedgerId, messageId.EntryId);

    public bool Remove(ulong ledgerId, ulong entryId)
    {
        var key = (ledgerId, entryId);

        lock (_lock)
        {
            if (!_index.TryGetValue(key, out var bucket))
                return false;

            _index.Remove(key);
            return _buckets[bucket].Remove(key);
        }
    }

    // Remove every tracked entry up to and including the entry of the message id. Partition and batch index are ignored.
    public int RemoveMessagesTill(MessageId messageId)
        => RemoveMessagesTill(messageId.LedgerId, messageId.EntryId);

    public int RemoveMessagesTill(ulong ledgerId, ulong entryId)
    {
        lock (_lock)
        {
            _scratch.Clear();

            foreach (var pair in _index)
            {
                var key = pair.Key;

                if (key.LedgerId < ledgerId || (key.LedgerId == ledgerId && key.EntryId <= entryId))
                    _scratch.Add(key);
            }

            var removed = _scratch.Count;

            for (var i = 0; i < removed; ++i)
            {
                var key = _scratch[i];
                _buckets[_index[key]].Remove(key);
                _index.Remove(key);
            }

            _scratch.Clear();
            return removed;
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            _index.Clear();

            for (var i = 0; i < _buckets.Length; ++i)
                _buckets[i].Clear();
        }
    }

    // Expire the oldest bucket, which then becomes the newest one. Returns the expired message ids, or null if there were none.
    public MessageId[]? Tick()
    {
        lock (_lock)
        {
            var oldest = _buckets[_head];
            MessageId[]? expired = null;

            if (oldest.Count > 0)
            {
                expired = new MessageId[oldest.Count];
                var i = 0;

                foreach (var pair in oldest)
                {
                    expired[i++] = pair.Value;
                    _index.Remove(pair.Key);
                }

                oldest.Clear();
            }

            _head = (_head + 1) % _buckets.Length;
            return expired;
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
        _cts.Dispose();
    }

    private static IOneShotTimer CreateTimer(Action callback)
        => new OneShotTimer(callback);

    // Upstream: UnAckedMessageTracker#TimerTask.run
    private void OnTimer()
    {
        MessageId[]? expired;

        lock (_lock)
        {
            if (_isDisposed != 0 || _timer is null)
                return;

            try
            {
                expired = Tick();
            }
            finally
            {
                _timer.Schedule(_tickTime);
            }
        }

        if (expired is not null)
            _ = Redeliver(expired);
    }

    private async Task Redeliver(MessageId[] expired)
    {
        try
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            await _redeliver(expired, _cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Ignore
        }
    }
}
