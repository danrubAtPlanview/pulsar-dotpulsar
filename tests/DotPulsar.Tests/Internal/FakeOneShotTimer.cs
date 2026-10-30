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

// A timer that only fires when the test says so, and that records how it was scheduled
public sealed class FakeOneShotTimer : IOneShotTimer
{
    private readonly object _lock;
    private readonly Action _callback;
    private readonly List<TimeSpan> _scheduled;
    private TimeSpan? _pending;
    private bool _isDisposed;

    public FakeOneShotTimer(Action callback)
    {
        _lock = new();
        _callback = callback;
        _scheduled = [];
        _pending = null;
        _isDisposed = false;
    }

    // Every due time the timer was scheduled with, in order
    public TimeSpan[] Scheduled
    {
        get
        {
            lock (_lock)
                return [.. _scheduled];
        }
    }

    public TimeSpan? Pending
    {
        get
        {
            lock (_lock)
                return _pending;
        }
    }

    public bool IsDisposed
    {
        get
        {
            lock (_lock)
                return _isDisposed;
        }
    }

    public void Schedule(TimeSpan dueTime)
    {
        lock (_lock)
        {
            if (_isDisposed)
                return;

            _scheduled.Add(dueTime);
            _pending = dueTime;
        }
    }

    // Fire the callback as a timer does: only if it is scheduled, and once per schedule
    public void Fire()
    {
        if (!TryFire())
            throw new InvalidOperationException("The timer is not scheduled");
    }

    public bool TryFire()
    {
        lock (_lock)
        {
            if (_pending is null)
                return false;

            _pending = null;
        }

        _callback();
        return true;
    }

    // Fire the callback as a timer does that was already queued when it got disposed
    public void FireWithoutSchedule()
        => _callback();

    public void Dispose()
    {
        lock (_lock)
        {
            _isDisposed = true;
            _pending = null;
        }
    }
}
