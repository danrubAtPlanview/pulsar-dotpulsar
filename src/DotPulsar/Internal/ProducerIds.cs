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

/// <summary>
/// Gives out the ids of producers. A producer keeps its id when it connects again, and it is the same on every connection, so an id is never given out twice.
/// The broker keeps the ids of the producers of a connection apart from each other, so ids that are unique in the process are unique on every connection.
/// </summary>
public static class ProducerIds
{
    // Upstream: PulsarClientImpl#newProducerId (the Java client has one per client, and a producer takes its id when it is created)
    private static long _next = -1;

    public static ulong Next()
        => unchecked((ulong) Interlocked.Increment(ref _next));
}
