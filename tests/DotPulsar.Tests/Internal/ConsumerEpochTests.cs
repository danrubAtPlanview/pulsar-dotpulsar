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
public sealed class ConsumerEpochTests
{
    [Fact]
    public void Value_GivenNewEpoch_ShouldBeZero()
    {
        //Arrange
        var uut = new ConsumerEpoch();

        //Act
        var actual = uut.Value;

        //Assert
        actual.ShouldBe(0);
    }

    [Fact]
    public void Increment_GivenEpoch_ShouldReturnAndStoreTheIncrementedValue()
    {
        //Arrange
        var uut = new ConsumerEpoch();

        //Act
        var first = uut.Increment();
        var second = uut.Increment();

        //Assert
        first.ShouldBe(1);
        second.ShouldBe(2);
        uut.Value.ShouldBe(2);
    }

    [Fact]
    public async Task Increment_GivenConcurrentUse_ShouldNotLoseIncrements()
    {
        //Arrange
        const int threads = 8;
        const int incrementsPerThread = 10000;
        var uut = new ConsumerEpoch();

        //Act
        var tasks = Enumerable.Range(0, threads).Select(thread => Run(() =>
        {
            for (var i = 0; i < incrementsPerThread; ++i)
                _ = uut.Increment();
        })).ToArray();
        await Task.WhenAll(tasks);

        //Assert
        uut.Value.ShouldBe((long) threads * incrementsPerThread);
    }

    private static Task Run(Action action)
        => Task.Run(action, Current.CancellationToken);
}
