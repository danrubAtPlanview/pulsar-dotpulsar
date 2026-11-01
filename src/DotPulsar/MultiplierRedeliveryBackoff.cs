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

namespace DotPulsar;

using DotPulsar.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/MultiplierRedeliveryBackoff.java
/// <summary>
/// A redelivery backoff where the delay is the minimum delay multiplied by the multiplier once for every time the message has been redelivered, and never more than the maximum delay.
/// </summary>
/// <remarks>
/// With a minimum delay of 1 second, a maximum delay of 1 minute and a multiplier of 2, the delays are 1, 2, 4, 8, 16, 32, 60, 60 ... seconds.
/// </remarks>
public sealed class MultiplierRedeliveryBackoff : IRedeliveryBackoff
{
    private readonly TimeSpan _minDelay;
    private readonly TimeSpan _maxDelay;
    private readonly double _multiplier;
    private readonly double _maxMultiplierPow;

    /// <summary>
    /// Initializes a new instance with a minimum delay of 10 seconds, a maximum delay of 10 minutes and a multiplier of 2.
    /// </summary>
    public MultiplierRedeliveryBackoff() : this(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(10))
    {
    }

    /// <summary>
    /// Initializes a new instance using the specified minimum delay, maximum delay and multiplier.
    /// </summary>
    /// <param name="minDelay">The delay of a message that has not been redelivered. Must not be negative.</param>
    /// <param name="maxDelay">The delay that is never exceeded. Must not be less than the minimum delay.</param>
    /// <param name="multiplier">The factor that the delay grows with for every redelivery. Must be greater than 1. The default is 2.</param>
    /// <exception cref="ArgumentOutOfRangeException">A value is outside the range that is described for it.</exception>
    public MultiplierRedeliveryBackoff(TimeSpan minDelay, TimeSpan maxDelay, double multiplier = 2.0)
    {
        if (minDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(minDelay), minDelay, "The minimum delay must not be negative");

        if (maxDelay < minDelay)
            throw new ArgumentOutOfRangeException(nameof(maxDelay), maxDelay, "The maximum delay must not be less than the minimum delay");

        // Written so that NaN is rejected too
        if (!(multiplier > 1))
            throw new ArgumentOutOfRangeException(nameof(multiplier), multiplier, "The multiplier must be greater than 1");

        _minDelay = minDelay;
        _maxDelay = maxDelay;
        _multiplier = multiplier;

        // The number of redeliveries from where the delay is the maximum delay. It is not used when the minimum delay is zero, because all delays are then zero.
        _maxMultiplierPow = minDelay > TimeSpan.Zero
            ? Math.Floor(Math.Log((double) maxDelay.Ticks / minDelay.Ticks) / Math.Log(multiplier)) + 1
            : 0;
    }

    /// <summary>
    /// The delay before a message is redelivered.
    /// </summary>
    public TimeSpan Next(uint redeliveryCount)
    {
        if (redeliveryCount == 0 || _minDelay <= TimeSpan.Zero)
            return _minDelay;

        if (redeliveryCount > _maxMultiplierPow)
            return _maxDelay;

        var ticks = _minDelay.Ticks * Math.Pow(_multiplier, redeliveryCount);
        return ticks >= _maxDelay.Ticks ? _maxDelay : TimeSpan.FromTicks((long) ticks);
    }
}
