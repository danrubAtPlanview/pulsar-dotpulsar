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

using DotPulsar.Internal.Abstractions;

public sealed class FakeOneShotTimerFactory
{
    private readonly object _lock;
    private readonly List<FakeOneShotTimer> _timers;

    public FakeOneShotTimerFactory()
    {
        _lock = new();
        _timers = [];
    }

    public int Created
    {
        get
        {
            lock (_lock)
                return _timers.Count;
        }
    }

    // The only timer that was created
    public FakeOneShotTimer Timer
    {
        get
        {
            lock (_lock)
                return _timers.Single();
        }
    }

    public IOneShotTimer Create(Action callback)
    {
        var timer = new FakeOneShotTimer(callback);

        lock (_lock)
            _timers.Add(timer);

        return timer;
    }
}
