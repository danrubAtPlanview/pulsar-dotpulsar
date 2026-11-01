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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Abstractions;

// A backoff that returns the delay of a function for the number of redeliveries that it is asked for, and remembers the numbers
public sealed class FakeRedeliveryBackoff : IRedeliveryBackoff
{
    private readonly Func<uint, TimeSpan> _delay;
    private readonly List<uint> _counts;

    public FakeRedeliveryBackoff(Func<uint, TimeSpan> delay)
    {
        _delay = delay;
        _counts = [];
    }

    public uint[] Counts
    {
        get
        {
            lock (_counts)
                return [.. _counts];
        }
    }

    public TimeSpan Next(uint redeliveryCount)
    {
        lock (_counts)
            _counts.Add(redeliveryCount);

        return _delay(redeliveryCount);
    }
}
