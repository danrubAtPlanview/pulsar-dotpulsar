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

namespace DotPulsar.Tests.Internal.Transactions;

using System.Diagnostics;

public static class WaitFor
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

    // Poll every 10 milliseconds until the condition is true (Java Awaitility)
    public static async Task Until(Func<bool> condition, string description, TimeSpan? timeout = null)
    {
        var limit = timeout ?? DefaultTimeout;
        var stopwatch = Stopwatch.StartNew();

        while (!condition())
        {
            if (stopwatch.Elapsed > limit)
                throw new TimeoutException($"Gave up waiting until {description}");

            await Task.Delay(TimeSpan.FromMilliseconds(10), Current.CancellationToken);
        }
    }

    // A task that is completed when the token is cancelled, like the tasks of a connection that is waiting for an event
    public static Task<T> Forever<T>(CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
        return completion.Task;
    }
}
