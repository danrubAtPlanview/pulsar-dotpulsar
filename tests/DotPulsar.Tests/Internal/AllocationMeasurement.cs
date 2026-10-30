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
using System.Diagnostics;
using System.Reflection;

// Measures the allocations of asynchronous operations that complete at once. The numbers are only meaningful for an optimized build:
// the C# compiler makes the state machine of an async method a class in a Debug build, and that allocates for every call.
public static class AllocationMeasurement
{
    private const int WarmupOperations = 50;
    private const int Operations = 200;
    private const int MaxAttempts = 5;

    public static bool IsOptimized { get; } = typeof(Executor).Assembly.GetCustomAttribute<DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };

    public static void SkipWhenNotOptimized()
        => Assert.SkipWhen(!IsOptimized, "The allocations of async methods are only measured in an optimized build, because the state machines are classes in a Debug build");

    // The pooled buffers and caches of the runtime can be trimmed by a gen 2 collection under memory pressure, and the next use then allocates.
    // A real allocation in the code under test shows up in every attempt, so the smallest of the attempts counts (see the test note of the batching core).
    // The result is the number of bytes per operation, and the measurement stops as soon as an attempt does not allocate more than the target.
    public static async Task<double> BytesPerOperation(Func<ValueTask> operation, double target = 0)
    {
        for (var i = 0; i < WarmupOperations; ++i)
        {
            await operation().ConfigureAwait(false);
        }

        var smallest = double.MaxValue;

        for (var attempt = 0; attempt < MaxAttempts && smallest > target; ++attempt)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();

            for (var i = 0; i < Operations; ++i)
            {
                await operation().ConfigureAwait(false);
            }

            smallest = Math.Min(smallest, (GC.GetAllocatedBytesForCurrentThread() - before) / (double) Operations);
        }

        return smallest;
    }
}
