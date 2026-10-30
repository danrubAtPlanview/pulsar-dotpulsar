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
using System.Diagnostics;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/NegativeAcksTracker.java, apache/pulsar-client-cpp@f71d7e9 lib/NegativeAcksTracker.cc
public sealed class NegativeAcknowledgeTracker : IDisposable
{
    public const int PrecisionBitCount = 8;

    public static readonly TimeSpan MinimumDelay = TimeSpan.FromMilliseconds(100);

    private readonly object _lock;
    private readonly long _delayMs;
    private readonly Func<MessageId[], CancellationToken, ValueTask> _redeliver;
    private readonly Func<TimeSpan> _clock;
    private readonly Func<Action, IOneShotTimer> _timerFactory;
    private readonly HashSet<(ulong LedgerId, ulong EntryId)> _pending;
    private readonly SortedDictionary<long, List<MessageId>> _buckets;
    private readonly List<long> _dueBuckets;
    private readonly CancellationTokenSource _cts;
    private readonly CancellationToken _cancellationToken;
    private IOneShotTimer? _timer;
    private bool _isScheduled;
    private int _isDisposed;

    // The 'redeliver' delegate receives the entry-level message ids that are due, outside of the lock, and a token that is cancelled when the tracker is disposed.
    // The 'clock' (monotonic) and 'timerFactory' are test seams. A delay below the minimum is raised to the minimum.
    public NegativeAcknowledgeTracker(
        TimeSpan delay,
        Func<MessageId[], CancellationToken, ValueTask> redeliver,
        Func<TimeSpan>? clock = null,
        Func<Action, IOneShotTimer>? timerFactory = null)
    {
        _lock = new();
        _delayMs = Math.Max((long) delay.TotalMilliseconds, (long) MinimumDelay.TotalMilliseconds);
        _redeliver = redeliver;
        _clock = clock ?? CreateClock();
        _timerFactory = timerFactory ?? CreateTimer;
        _pending = [];
        _buckets = [];
        _dueBuckets = [];
        _cts = new CancellationTokenSource();
        _cancellationToken = _cts.Token;
        _timer = null;
        _isScheduled = false;
        _isDisposed = 0;
    }

    public int Count
    {
        get
        {
            lock (_lock)
                return _pending.Count;
        }
    }

    // Upstream: NegativeAcksTracker#trimLowerBit
    public static long TrimLowerBits(long timestamp, int bits)
        => timestamp & (-1L << bits);

    // The batch index is discarded, so all the messages of a batch are redelivered with their entry. Returns false if the entry is already pending.
    // Upstream: NegativeAcksTracker#add (C# deviation: an entry that is already pending is not added again, so it is redelivered once and keeps its first deadline)
    public bool Add(MessageId messageId)
    {
        lock (_lock)
        {
            if (_isDisposed != 0)
                return false;

            if (!_pending.Add((messageId.LedgerId, messageId.EntryId)))
                return false;

            var dueTimestamp = TrimLowerBits(GetTimestamp() + _delayMs, PrecisionBitCount);

            if (!_buckets.TryGetValue(dueTimestamp, out var ids))
            {
                ids = [];
                _buckets.Add(dueTimestamp, ids);
            }

            ids.Add(ToEntryLevel(messageId));

            if (!_isScheduled)
                Schedule(_delayMs);

            return true;
        }
    }

    // Stops the timer and clears the tracker. Nothing is accepted or redelivered afterwards.
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        IOneShotTimer? timer;

        lock (_lock)
        {
            timer = _timer;
            _timer = null;
            _isScheduled = false;
            _pending.Clear();
            _buckets.Clear();
        }

        _cts.Cancel();
        timer?.Dispose();
        _cts.Dispose();
    }

    private static Func<TimeSpan> CreateClock()
    {
        var timestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;
        return () => TimeSpan.FromTicks((long) (Stopwatch.GetTimestamp() * timestampToTicks));
    }

    private static IOneShotTimer CreateTimer(Action callback)
        => new OneShotTimer(callback);

    private static MessageId ToEntryLevel(MessageId messageId)
        => messageId.BatchIndex < 0 ? messageId : new MessageId(messageId.LedgerId, messageId.EntryId, messageId.Partition, -1, messageId.Topic);

    private long GetTimestamp()
        => (long) _clock().TotalMilliseconds;

    // The timer is only scheduled while there are pending entries
    private void Schedule(long delayMs)
    {
        _timer ??= _timerFactory(OnTimer);
        _timer.Schedule(TimeSpan.FromMilliseconds(delayMs));
        _isScheduled = true;
    }

    // Upstream: NegativeAcksTracker#triggerRedelivery
    private void OnTimer()
    {
        MessageId[]? due = null;

        lock (_lock)
        {
            if (_isDisposed != 0)
                return;

            _isScheduled = false;

            if (_buckets.Count == 0)
                return;

            var timestamp = GetTimestamp();
            var count = 0;
            _dueBuckets.Clear();

            foreach (var bucket in _buckets)
            {
                if (bucket.Key > timestamp)
                    break;

                _dueBuckets.Add(bucket.Key);
                count += bucket.Value.Count;
            }

            if (count > 0)
            {
                due = new MessageId[count];
                var index = 0;

                for (var i = 0; i < _dueBuckets.Count; ++i)
                {
                    var ids = _buckets[_dueBuckets[i]];

                    for (var j = 0; j < ids.Count; ++j)
                    {
                        var messageId = ids[j];
                        due[index++] = messageId;
                        _pending.Remove((messageId.LedgerId, messageId.EntryId));
                    }

                    _buckets.Remove(_dueBuckets[i]);
                }
            }

            using var enumerator = _buckets.GetEnumerator();

            if (enumerator.MoveNext())
                Schedule(Math.Max(enumerator.Current.Key - timestamp, 0));
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
