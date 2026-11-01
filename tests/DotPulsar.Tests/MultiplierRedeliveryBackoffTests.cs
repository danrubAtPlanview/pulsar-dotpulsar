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

namespace DotPulsar.Tests;

[Trait("Category", "Unit")]
public sealed class MultiplierRedeliveryBackoffTests
{
    // Upstream: MultiplierRedeliveryBackoffTest#testNext (the first policy: 1 second, 10 minutes and the default multiplier)
    // The assertion for the redelivery count -1 is not ported, because the count of DotPulsar is unsigned.
    [Fact]
    public void Next_GivenMinDelayMaxDelayAndDefaultMultiplier_ShouldDoubleTheDelayUntilMaxDelay()
    {
        //Arrange
        var minDelay = TimeSpan.FromMilliseconds(1000);
        var maxDelay = TimeSpan.FromMilliseconds(1000 * 60 * 10);
        var uut = new MultiplierRedeliveryBackoff(minDelay, maxDelay);

        //Act
        var delays = new[] { 0u, 1u, 4u, 100u }.Select(uut.Next).ToArray();

        //Assert
        delays.ShouldBe([minDelay, minDelay * 2, minDelay * 16, maxDelay]);
    }

    // Upstream: MultiplierRedeliveryBackoffTest#testNext (the second policy: 2 seconds, 100 minutes and the multiplier 5)
    [Fact]
    public void Next_GivenMultiplierOfFive_ShouldMultiplyTheDelayByFiveForEveryRedelivery()
    {
        //Arrange
        var minDelay = TimeSpan.FromMilliseconds(2000);
        var maxDelay = TimeSpan.FromMilliseconds(10000 * 60 * 10);
        var uut = new MultiplierRedeliveryBackoff(minDelay, maxDelay, 5);

        //Act
        var delays = new[] { 0u, 1u, 2u, 4u, 100u }.Select(uut.Next).ToArray();

        //Assert
        delays.ShouldBe([minDelay, minDelay * 5, minDelay * 25, minDelay * 625, maxDelay]);
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenNoArguments_ShouldUseTheDefaultsOfTheJavaClient()
    {
        //Arrange & Act
        var uut = new MultiplierRedeliveryBackoff();

        //Assert
        uut.Next(0).ShouldBe(TimeSpan.FromSeconds(10));
        uut.Next(1).ShouldBe(TimeSpan.FromSeconds(20));
        uut.Next(5).ShouldBe(TimeSpan.FromSeconds(320));
        uut.Next(6).ShouldBe(TimeSpan.FromMinutes(10));
        uut.Next(uint.MaxValue).ShouldBe(TimeSpan.FromMinutes(10));
    }

    // Additional coverage
    [Fact]
    public void Next_GivenCountsAroundWhereMaxDelayIsReached_ShouldNeverExceedMaxDelay()
    {
        //Arrange
        var maxDelay = TimeSpan.FromMilliseconds(600000);
        var uut = new MultiplierRedeliveryBackoff(TimeSpan.FromMilliseconds(1000), maxDelay);

        //Act
        var delays = Enumerable.Range(0, 20).Select(count => uut.Next((uint) count)).ToArray();

        //Assert
        delays.ShouldAllBe(delay => delay <= maxDelay);
        delays.Take(10).ShouldBe(Enumerable.Range(0, 10).Select(count => TimeSpan.FromMilliseconds(1000 * Math.Pow(2, count))));
        delays.Skip(10).ShouldAllBe(delay => delay == maxDelay);
    }

    // Additional coverage
    [Fact]
    public void Next_GivenFractionalMultiplier_ShouldTruncateToWholeTicks()
    {
        //Arrange
        var uut = new MultiplierRedeliveryBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), 1.5);

        //Act & Assert
        uut.Next(0).ShouldBe(TimeSpan.FromSeconds(1));
        uut.Next(1).ShouldBe(TimeSpan.FromMilliseconds(1500));
        uut.Next(2).ShouldBe(TimeSpan.FromMilliseconds(2250));
        uut.Next(3).ShouldBe(TimeSpan.FromMilliseconds(3375));
    }

    // Additional coverage
    [Fact]
    public void Next_GivenZeroMinDelay_ShouldAlwaysReturnZero()
    {
        //Arrange
        var uut = new MultiplierRedeliveryBackoff(TimeSpan.Zero, TimeSpan.FromMinutes(1));

        //Act & Assert
        uut.Next(0).ShouldBe(TimeSpan.Zero);
        uut.Next(1).ShouldBe(TimeSpan.Zero);
        uut.Next(uint.MaxValue).ShouldBe(TimeSpan.Zero);
    }

    // Additional coverage
    [Fact]
    public void Next_GivenMinDelayEqualToMaxDelay_ShouldAlwaysReturnIt()
    {
        //Arrange
        var delay = TimeSpan.FromSeconds(3);
        var uut = new MultiplierRedeliveryBackoff(delay, delay);

        //Act & Assert
        uut.Next(0).ShouldBe(delay);
        uut.Next(1).ShouldBe(delay);
        uut.Next(2).ShouldBe(delay);
        uut.Next(uint.MaxValue).ShouldBe(delay);
    }

    // Additional coverage
    [Fact]
    public void Next_GivenMaxDelayThatIsTheLargestTimeSpan_ShouldNotOverflow()
    {
        //Arrange
        var uut = new MultiplierRedeliveryBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.MaxValue, 1000);

        //Act
        var delays = Enumerable.Range(0, 10).Select(count => uut.Next((uint) count)).ToArray();

        //Assert
        delays.Take(5).ShouldBe(Enumerable.Range(0, 5).Select(count => TimeSpan.FromMilliseconds(Math.Pow(1000, count))));
        delays.Skip(5).ShouldAllBe(delay => delay == TimeSpan.MaxValue);
    }

    // Additional coverage
    [Fact]
    public void Next_GivenMultiplierJustAboveOne_ShouldNotOverflowTheCountThatReachesMaxDelay()
    {
        //Arrange
        var uut = new MultiplierRedeliveryBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.MaxValue, Math.BitIncrement(1.0));

        //Act & Assert
        uut.Next(1).ShouldBe(TimeSpan.FromMilliseconds(1));
        uut.Next(uint.MaxValue).ShouldBe(TimeSpan.FromMilliseconds(1));
    }

    // Additional coverage
    [Theory]
    [InlineData(-1, 1000, 2.0, "minDelay")]
    [InlineData(1000, 999, 2.0, "maxDelay")]
    [InlineData(1000, 2000, 1.0, "multiplier")]
    [InlineData(1000, 2000, 0.0, "multiplier")]
    [InlineData(1000, 2000, -2.0, "multiplier")]
    [InlineData(1000, 2000, double.NaN, "multiplier")]
    public void Constructor_GivenInvalidArgument_ShouldThrowArgumentOutOfRangeException(int minDelayMs, int maxDelayMs, double multiplier, string parameterName)
    {
        //Arrange & Act
        var exception = Should.Throw<ArgumentOutOfRangeException>(() => new MultiplierRedeliveryBackoff(TimeSpan.FromMilliseconds(minDelayMs), TimeSpan.FromMilliseconds(maxDelayMs), multiplier));

        //Assert
        exception.ParamName.ShouldBe(parameterName);
    }
}
