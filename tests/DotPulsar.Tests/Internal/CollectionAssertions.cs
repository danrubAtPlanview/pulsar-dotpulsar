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

using Google.Protobuf.Collections;

public static class CollectionAssertions
{
    public static void ShouldBeAckSet(this RepeatedField<long> actual, params long[] expected)
        => actual.ShouldBeSequence(expected);

    public static void ShouldBeSequence<T>(this IEnumerable<T> actual, params T[] expected)
    {
        var actualItems = actual.ToArray();

        if (!actualItems.SequenceEqual(expected))
            throw new ShouldAssertException($"Expected [{string.Join(", ", expected)}] but was [{string.Join(", ", actualItems)}]");
    }
}
