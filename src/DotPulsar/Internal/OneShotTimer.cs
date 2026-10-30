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

public sealed class OneShotTimer : IOneShotTimer
{
    private static readonly TimeSpan _maxDueTime = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly Action _callback;
    private readonly Timer _timer;
    private int _isDisposed;

    public OneShotTimer(Action callback)
    {
        _callback = callback;
        _isDisposed = 0;
        _timer = new Timer(static state => ((OneShotTimer) state!).OnTimer(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    public void Schedule(TimeSpan dueTime)
    {
        if (_isDisposed != 0)
            return;

        if (dueTime < TimeSpan.Zero)
            dueTime = TimeSpan.Zero;
        else if (dueTime > _maxDueTime)
            dueTime = _maxDueTime;

        try
        {
            _ = _timer.Change(dueTime, Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
            // Ignore
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _timer.Dispose();
    }

    private void OnTimer()
    {
        if (_isDisposed == 0)
            _callback();
    }
}
