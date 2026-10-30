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

#if !NET6_0_OR_GREATER
namespace DotPulsar.Internal.Extensions;

// Task.WaitAsync(CancellationToken) was introduced in .NET 6
public static class TaskExtensions
{
    public static async Task WaitAsync(this Task task, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || !cancellationToken.CanBeCanceled)
        {
            await task.ConfigureAwait(false);
            return;
        }

        var cancellation = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using (cancellationToken.Register(() => cancellation.TrySetCanceled(cancellationToken)))
        {
            if (await Task.WhenAny(task, cancellation.Task).ConfigureAwait(false) != task)
                await cancellation.Task.ConfigureAwait(false);
        }

        await task.ConfigureAwait(false);
    }

    public static async Task<T> WaitAsync<T>(this Task<T> task, CancellationToken cancellationToken)
    {
        await ((Task) task).WaitAsync(cancellationToken).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }
}
#endif
