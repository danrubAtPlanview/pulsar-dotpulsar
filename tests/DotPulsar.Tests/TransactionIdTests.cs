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
public sealed class TransactionIdTests
{
    [Fact]
    public void Constructor_GivenMostAndLeast_ShouldStoreBothValues()
    {
        //Arrange & Act
        var uut = new TransactionId(1, 5);

        //Assert
        uut.MostSigBits.ShouldBe(1UL);
        uut.LeastSigBits.ShouldBe(5UL);
    }

    [Fact]
    public void ToString_GivenMostAndLeast_ShouldFormatLikeUpstream()
    {
        //Arrange
        var uut = new TransactionId(1, 5);

        //Act
        var actual = uut.ToString();

        //Assert
        actual.ShouldBe("(1,5)");
    }

    [Fact]
    public void ToString_GivenMaxValues_ShouldFormatAsUnsigned()
    {
        //Arrange
        var uut = new TransactionId(ulong.MaxValue, ulong.MaxValue);

        //Act
        var actual = uut.ToString();

        //Assert
        actual.ShouldBe("(18446744073709551615,18446744073709551615)");
    }

    [Fact]
    public void Default_ShouldBeZeroZero()
    {
        //Arrange & Act
        var uut = default(TransactionId);

        //Assert
        uut.ToString().ShouldBe("(0,0)");
        uut.ShouldBe(new TransactionId(0, 0));
    }

    [Fact]
    public void Equals_GivenSameBits_ShouldBeEqual()
    {
        //Arrange
        var uut = new TransactionId(1, 5);
        var other = new TransactionId(1, 5);

        //Act & Assert
        uut.Equals(other).ShouldBeTrue();
        uut.Equals((object) other).ShouldBeTrue();
        (uut == other).ShouldBeTrue();
        (uut != other).ShouldBeFalse();
        uut.GetHashCode().ShouldBe(other.GetHashCode());
    }

    [Theory]
    [InlineData(2, 5)]
    [InlineData(1, 6)]
    [InlineData(5, 1)]
    [InlineData(0, 0)]
    public void Equals_GivenDifferentBits_ShouldNotBeEqual(ulong mostSigBits, ulong leastSigBits)
    {
        //Arrange
        var uut = new TransactionId(1, 5);
        var other = new TransactionId(mostSigBits, leastSigBits);

        //Act & Assert
        uut.Equals(other).ShouldBeFalse();
        uut.Equals((object) other).ShouldBeFalse();
        (uut == other).ShouldBeFalse();
        (uut != other).ShouldBeTrue();
    }

    [Fact]
    public void Equals_GivenOtherType_ShouldNotBeEqual()
    {
        //Arrange
        var uut = new TransactionId(1, 5);

        //Act & Assert
        uut.Equals(null).ShouldBeFalse();
        uut.Equals("(1,5)").ShouldBeFalse();
    }
}
