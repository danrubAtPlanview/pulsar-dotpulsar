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

using DotPulsar.Internal;

[Trait("Category", "Unit")]
public sealed class OneShotTimerTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

    // Additional coverage
    [Fact]
    public async Task Schedule_GivenDueTime_ShouldInvokeCallback()
    {
        //Arrange
        var invoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new OneShotTimer(() => invoked.TrySetResult());

        //Act
        uut.Schedule(TimeSpan.FromMilliseconds(10));

        //Assert
        await invoked.Task.WaitAsync(_timeout, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Schedule_GivenNoSchedule_ShouldNotInvokeCallback()
    {
        //Arrange
        var invocations = 0;
        using var uut = new OneShotTimer(() => Interlocked.Increment(ref invocations));

        //Act
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);

        //Assert
        invocations.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task Schedule_GivenInvokedCallback_ShouldNotInvokeItAgainUntilScheduledAgain()
    {
        //Arrange
        var invocations = 0;
        var invoked = new SemaphoreSlim(0);
        using var uut = new OneShotTimer(() =>
        {
            Interlocked.Increment(ref invocations);
            invoked.Release();
        });
        uut.Schedule(TimeSpan.FromMilliseconds(10));
        await invoked.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var invocationsWithoutSchedule = invocations;
        uut.Schedule(TimeSpan.FromMilliseconds(10));
        await invoked.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        invocationsWithoutSchedule.ShouldBe(1);
        invocations.ShouldBe(2);
    }

    // Additional coverage
    [Fact]
    public async Task Schedule_GivenPendingSchedule_ShouldReplaceIt()
    {
        //Arrange
        var invoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new OneShotTimer(() => invoked.TrySetResult());
        uut.Schedule(TimeSpan.FromHours(1));

        //Act
        uut.Schedule(TimeSpan.FromMilliseconds(10));

        //Assert
        await invoked.Task.WaitAsync(_timeout, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Schedule_GivenNegativeDueTime_ShouldInvokeCallbackImmediately()
    {
        //Arrange
        var invoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new OneShotTimer(() => invoked.TrySetResult());

        //Act
        uut.Schedule(TimeSpan.FromSeconds(-5));

        //Assert
        await invoked.Task.WaitAsync(_timeout, Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public void Schedule_GivenDueTimeAboveTheMaximumOfTheTimer_ShouldNotThrow()
    {
        //Arrange
        using var uut = new OneShotTimer(() => { });

        //Act
        var exception = Record.Exception(() => uut.Schedule(TimeSpan.MaxValue));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task Dispose_GivenScheduledTimer_ShouldNotInvokeCallback()
    {
        //Arrange
        var invocations = 0;
        var uut = new OneShotTimer(() => Interlocked.Increment(ref invocations));
        uut.Schedule(TimeSpan.FromMilliseconds(50));

        //Act
        uut.Dispose();
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);

        //Assert
        invocations.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenCalledTwice_ShouldNotThrow()
    {
        //Arrange
        var uut = new OneShotTimer(() => { });

        //Act
        var exception = Record.Exception(() =>
        {
            uut.Dispose();
            uut.Dispose();
        });

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Schedule_GivenDisposedTimer_ShouldNotThrow()
    {
        //Arrange
        var uut = new OneShotTimer(() => { });
        uut.Dispose();

        //Act
        var exception = Record.Exception(() => uut.Schedule(TimeSpan.FromMilliseconds(10)));

        //Assert
        exception.ShouldBeNull();
    }
}
