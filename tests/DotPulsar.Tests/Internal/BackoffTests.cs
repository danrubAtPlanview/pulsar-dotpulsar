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
public sealed class BackoffTests
{
    private static TimeSpan Milliseconds(long value) => TimeSpan.FromMilliseconds(value);

    // Additional coverage
    [Fact]
    public void Next_GivenNoJitter_ShouldReturnInitialAndThenDoubleUntilMax()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(100), Milliseconds(1000), jitterPercent: 0);

        //Act
        var actual = Enumerable.Range(0, 7).Select(_ => uut.Next()).ToArray();

        //Assert
        actual.ShouldBe([Milliseconds(100), Milliseconds(200), Milliseconds(400), Milliseconds(800), Milliseconds(1000), Milliseconds(1000), Milliseconds(1000)]);
    }

    // Additional coverage
    [Fact]
    public void Next_GivenPerOperationRetryConfiguration_ShouldCapAtSixSeconds()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(100), TimeSpan.FromSeconds(60) / 10, jitterPercent: 0);

        //Act
        var actual = Enumerable.Range(0, 9).Select(_ => uut.Next()).ToArray();

        //Assert
        actual.ShouldBe([Milliseconds(100), Milliseconds(200), Milliseconds(400), Milliseconds(800), Milliseconds(1600), Milliseconds(3200), Milliseconds(6000), Milliseconds(6000), Milliseconds(6000)]);
    }

    // Additional coverage
    [Theory]
    [InlineData(0.0, 950)]
    [InlineData(0.25, 975)]
    [InlineData(0.5, 1000)]
    [InlineData(0.75, 1025)]
    [InlineData(0.999999, 1050)]
    public void Next_GivenJitter_ShouldApplySymmetricJitterOfHalfThePercentage(double random, long expectedMilliseconds)
    {
        //Arrange
        var uut = new Backoff(Milliseconds(1000), Milliseconds(60000), jitterPercent: 10, nextDouble: () => random);

        //Act
        var actual = uut.Next();

        //Assert
        actual.ShouldBe(Milliseconds(expectedMilliseconds));
    }

    // Additional coverage
    [Fact]
    public void Next_GivenJitterPercentIsZero_ShouldNotUseRandom()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(100), Milliseconds(1000), jitterPercent: 0, nextDouble: () => throw new InvalidOperationException("Random should not be used"));

        //Act
        var actual = uut.Next();

        //Assert
        actual.ShouldBe(Milliseconds(100));
    }

    // Additional coverage
    [Fact]
    public void Next_GivenDefaultClockAndRandom_ShouldStayWithinJitterBounds()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(1000), Milliseconds(1000));

        for (var i = 0; i < 100; ++i)
        {
            //Act
            var actual = uut.Next();

            //Assert
            actual.ShouldBeGreaterThanOrEqualTo(Milliseconds(950));
            actual.ShouldBeLessThanOrEqualTo(Milliseconds(1050));
        }
    }

    // Additional coverage
    [Fact]
    public void Next_GivenMandatoryStop_ShouldTruncateDelayOnceAndThenKeepDoubling()
    {
        //Arrange
        var now = TimeSpan.Zero;
        var uut = new Backoff(Milliseconds(100), Milliseconds(60000), Milliseconds(1000), jitterPercent: 0, clock: () => now);
        var actual = new List<TimeSpan>();

        //Act
        foreach (var elapsed in new[] { 0L, 100L, 300L, 700L, 1000L })
        {
            now = Milliseconds(elapsed);
            actual.Add(uut.Next());
        }

        //Assert
        actual.ShouldBe([Milliseconds(100), Milliseconds(200), Milliseconds(400), Milliseconds(300), Milliseconds(1600)]);
        uut.IsMandatoryStopMade.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Next_GivenMandatoryStopNotYetReached_ShouldNotMakeMandatoryStop()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(100), Milliseconds(60000), Milliseconds(1000), jitterPercent: 0, clock: () => TimeSpan.Zero);

        //Act
        _ = uut.Next();

        //Assert
        uut.IsMandatoryStopMade.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Next_GivenRemainingTimeToMandatoryStopBelowInitial_ShouldReturnInitial()
    {
        //Arrange
        var now = TimeSpan.Zero;
        var uut = new Backoff(Milliseconds(100), Milliseconds(60000), Milliseconds(150), jitterPercent: 0, clock: () => now);
        _ = uut.Next();
        now = Milliseconds(100);

        //Act
        var actual = uut.Next();

        //Assert
        actual.ShouldBe(Milliseconds(100));
        uut.IsMandatoryStopMade.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Next_GivenMandatoryStopEqualToInitial_ShouldReturnInitialTwiceAndThenDouble()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(100), Milliseconds(60000), Milliseconds(100), jitterPercent: 0, clock: () => TimeSpan.Zero);

        //Act
        var actual = Enumerable.Range(0, 4).Select(_ => uut.Next()).ToArray();

        //Assert
        actual.ShouldBe([Milliseconds(100), Milliseconds(100), Milliseconds(400), Milliseconds(800)]);
    }

    // Additional coverage
    [Fact]
    public void Next_GivenNoMandatoryStop_ShouldOnlyReadTheClockOnce()
    {
        //Arrange
        var reads = 0;
        var uut = new Backoff(Milliseconds(100), Milliseconds(1000), jitterPercent: 0, clock: () =>
        {
            ++reads;
            return TimeSpan.Zero;
        });

        //Act
        for (var i = 0; i < 5; ++i)
            _ = uut.Next();

        //Assert
        reads.ShouldBe(1);
        uut.IsMandatoryStopMade.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Next_GivenAllZero_ShouldReturnZero()
    {
        //Arrange
        var uut = new Backoff(TimeSpan.Zero, TimeSpan.Zero);

        //Act
        var actual = uut.Next();

        //Assert
        actual.ShouldBe(TimeSpan.Zero);
        uut.IsMandatoryStopMade.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Reset_GivenBackoffHasGrown_ShouldReturnInitialAgain()
    {
        //Arrange
        var uut = new Backoff(Milliseconds(100), Milliseconds(1000), jitterPercent: 0);
        _ = uut.Next();
        _ = uut.Next();
        _ = uut.Next();

        //Act
        uut.Reset();

        //Assert
        uut.Next().ShouldBe(Milliseconds(100));
        uut.Next().ShouldBe(Milliseconds(200));
    }

    // Additional coverage
    [Fact]
    public void Reset_GivenMandatoryStopMade_ShouldRestartMandatoryStopTracking()
    {
        //Arrange
        var now = TimeSpan.Zero;
        var uut = new Backoff(Milliseconds(100), Milliseconds(60000), Milliseconds(150), jitterPercent: 0, clock: () => now);
        _ = uut.Next();
        now = Milliseconds(100);
        _ = uut.Next();
        uut.IsMandatoryStopMade.ShouldBeTrue();

        //Act
        uut.Reset();

        //Assert
        uut.IsMandatoryStopMade.ShouldBeFalse();
        now = Milliseconds(10000);
        uut.Next().ShouldBe(Milliseconds(100));
        uut.IsMandatoryStopMade.ShouldBeFalse();
    }

    // Additional coverage
    [Theory]
    [InlineData(-0.1)]
    [InlineData(100.1)]
    public void Constructor_GivenJitterPercentOutsideZeroToOneHundred_ShouldThrowArgumentOutOfRangeException(double jitterPercent)
    {
        //Arrange & Act
        var exception = Record.Exception(() => new Backoff(Milliseconds(100), Milliseconds(1000), jitterPercent: jitterPercent));

        //Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }
}
