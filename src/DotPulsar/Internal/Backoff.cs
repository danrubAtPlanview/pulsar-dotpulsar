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

using System.Diagnostics;

// Upstream: apache/pulsar@f3846f4 pulsar-common/src/main/java/org/apache/pulsar/common/util/Backoff.java
public sealed class Backoff
{
    public const double DefaultJitterPercent = 10.0;

    private static readonly double _timestampToTicks;
    private static readonly Random _random;

    private readonly TimeSpan _initial;
    private readonly TimeSpan _max;
    private readonly TimeSpan _mandatoryStop;
    private readonly double _jitterPercent;
    private readonly Func<TimeSpan> _clock;
    private readonly Func<double> _nextDouble;

    private TimeSpan _next;
    private TimeSpan _firstBackoffTime;
    private bool _firstBackoffTimeSet;

    static Backoff()
    {
        _timestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;
        _random = new Random();
    }

    // The 'clock' (monotonic, only used for the mandatory stop) and 'nextDouble' (in [0, 1), only used for the jitter)
    // are test seams. A 'jitterPercent' of zero disables the jitter and a 'mandatoryStop' of zero disables the mandatory stop.
    public Backoff(
        TimeSpan initial,
        TimeSpan max,
        TimeSpan mandatoryStop = default,
        double jitterPercent = DefaultJitterPercent,
        Func<TimeSpan>? clock = null,
        Func<double>? nextDouble = null)
    {
        if (jitterPercent < 0 || jitterPercent > 100)
            throw new ArgumentOutOfRangeException(nameof(jitterPercent), jitterPercent, "The jitter percent must be in [0, 100]");

        _initial = initial;
        _max = max;
        _mandatoryStop = mandatoryStop;
        _jitterPercent = jitterPercent;
        _clock = clock ?? GetElapsedTime;
        _nextDouble = nextDouble ?? NextRandomDouble;

        _next = initial;
        _firstBackoffTime = TimeSpan.Zero;
        _firstBackoffTimeSet = false;
        IsMandatoryStopMade = IsAllZero();
    }

    public bool IsMandatoryStopMade { get; private set; }

    public TimeSpan Next()
    {
        var current = _next;
        if (current < _max)
        {
            var doubled = _next + _next;
            _next = doubled < _max ? doubled : _max;
        }

        if (!IsMandatoryStopMade)
        {
            var now = _clock();
            var timeElapsedSinceFirstBackoff = TimeSpan.Zero;
            if (!_firstBackoffTimeSet)
            {
                _firstBackoffTime = now;
                _firstBackoffTimeSet = true;
            }
            else
            {
                timeElapsedSinceFirstBackoff = now - _firstBackoffTime;
            }

            if ((timeElapsedSinceFirstBackoff + current) > _mandatoryStop)
            {
                var remaining = _mandatoryStop - timeElapsedSinceFirstBackoff;
                current = remaining > _initial ? remaining : _initial;
                IsMandatoryStopMade = true;
            }
        }

        var currentMilliseconds = (long) current.TotalMilliseconds;
        if (_jitterPercent > 0 && currentMilliseconds > 0)
        {
            var factor = 1.0 + ((_nextDouble() - 0.5) * (_jitterPercent / 100.0));
            currentMilliseconds = Math.Max(0L, (long) Math.Floor((currentMilliseconds * factor) + 0.5));
        }

        return TimeSpan.FromMilliseconds(currentMilliseconds);
    }

    public void Reset()
    {
        _next = _initial;
        _firstBackoffTime = TimeSpan.Zero;
        _firstBackoffTimeSet = false;
        IsMandatoryStopMade = IsAllZero();
    }

    private bool IsAllZero()
        => _initial == TimeSpan.Zero && _max == TimeSpan.Zero && _mandatoryStop == TimeSpan.Zero;

    private static TimeSpan GetElapsedTime()
        => TimeSpan.FromTicks((long) (Stopwatch.GetTimestamp() * _timestampToTicks));

    private static double NextRandomDouble()
    {
        lock (_random)
            return _random.NextDouble();
    }
}
