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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/NegativeAcksTracker.java, apache/pulsar-client-cpp@f71d7e9 lib/NegativeAcksTracker.cc
public sealed class NegativeAcknowledgeTracker : IDisposable
{
    public const int PrecisionBitCount = 8;

    public static readonly TimeSpan MinimumDelay = TimeSpan.FromMilliseconds(100);

    private readonly object _lock;
    private readonly long _delayMs;
    private readonly IRedeliveryBackoff? _redeliveryBackoff;
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
    private long _scheduledAtMs;
    private int _isDisposed;

    // The 'redeliver' delegate receives the entry-level message ids that are due, outside of the lock, and a token that is cancelled when the tracker is disposed.
    // The 'clock' (monotonic) and 'timerFactory' are test seams. A delay below the minimum is raised to the minimum.
    // With a 'redeliveryBackoff' the delay of an entry is the one that the policy returns for its redelivery count, instead of 'delay' (and without the minimum, as in Java).
    // Upstream: NegativeAcksTracker#NegativeAcksTracker
    public NegativeAcknowledgeTracker(
        TimeSpan delay,
        Func<MessageId[], CancellationToken, ValueTask> redeliver,
        Func<TimeSpan>? clock = null,
        Func<Action, IOneShotTimer>? timerFactory = null,
        IRedeliveryBackoff? redeliveryBackoff = null)
    {
        _lock = new();
        _delayMs = Math.Max((long) delay.TotalMilliseconds, (long) MinimumDelay.TotalMilliseconds);
        _redeliveryBackoff = redeliveryBackoff;
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
        _scheduledAtMs = 0;
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
    // The 'redeliveryCount' is the number of times that the message has been redelivered ('IMessage.RedeliveryCount'). It only matters with a redelivery backoff.
    // Upstream: NegativeAcksTracker#add (C# deviations: an entry that is already pending is not added again, so it is redelivered once and keeps its first deadline,
    // and the timer is brought forward when an entry is due before the one that it waits for, which can only happen with a redelivery backoff)
    public bool Add(MessageId messageId, uint redeliveryCount = 0)
    {
        // The policy is asked before anything changes, so a policy that throws leaves the tracker as it was and the exception reaches the caller, as in Java
        var delayMs = GetDelayMs(redeliveryCount);

        lock (_lock)
        {
            if (_isDisposed != 0)
                return false;

            if (!_pending.Add((messageId.LedgerId, messageId.EntryId)))
                return false;

            var now = GetTimestamp();
            var dueTimestamp = TrimLowerBits(now + delayMs, PrecisionBitCount);

            if (!_buckets.TryGetValue(dueTimestamp, out var ids))
            {
                ids = [];
                _buckets.Add(dueTimestamp, ids);
            }

            ids.Add(ToEntryLevel(messageId));

            if (!_isScheduled || now + delayMs < _scheduledAtMs)
                Schedule(delayMs, now);

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

    // Upstream: NegativeAcksTracker#add (backoffMs)
    private long GetDelayMs(uint redeliveryCount)
    {
        if (_redeliveryBackoff is null)
            return _delayMs;

        var delay = _redeliveryBackoff.Next(redeliveryCount);
        return delay <= TimeSpan.Zero ? 0 : (long) delay.TotalMilliseconds;
    }

    // The timer is only scheduled while there are pending entries
    private void Schedule(long delayMs, long now)
    {
        _timer ??= _timerFactory(OnTimer);
        _timer.Schedule(TimeSpan.FromMilliseconds(delayMs));
        _isScheduled = true;
        _scheduledAtMs = now + delayMs;
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
                Schedule(Math.Max(enumerator.Current.Key - timestamp, 0), timestamp);
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
