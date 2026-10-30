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

namespace DotPulsar.Tests.Internal.Crypto;

using DotPulsar.Internal.Crypto;
using System.Security.Cryptography;

[Trait("Category", "Unit")]
public sealed class Kdf2Sha1Tests
{
    [Fact]
    public void Generate_GivenVectorInput_ShouldReturnExpectedBytes()
    {
        //Arrange
        var sharedSecret = Enumerable.Range(0, 32).Select(i => (byte) i).ToArray();

        //Act
        var actual = Kdf2Sha1.Generate(sharedSecret, 48);

        //Assert
        Convert.ToHexString(actual).ToLowerInvariant().ShouldBe("a98ffb7caef3bd518fb7bc1b6cc89dbdef59cde6e6b6e6b5e405eb5e6ffb080c390c2cd82363d25a79fcf567c36336e9");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(19)]
    [InlineData(20)]
    [InlineData(21)]
    [InlineData(48)]
    [InlineData(100)]
    public void Generate_GivenLength_ShouldBeThePrefixOfTheSha1Blocks(int length)
    {
        //Arrange
        var sharedSecret = new byte[] { 1, 2, 3, 4, 5 };
        var expected = new List<byte>();

        for (var counter = 1; expected.Count < 100; ++counter)
            expected.AddRange(SHA1.HashData(sharedSecret.Concat(new byte[] { 0, 0, 0, (byte) counter }).ToArray()));

        //Act
        var actual = Kdf2Sha1.Generate(sharedSecret, length);

        //Assert
        actual.ShouldBe(expected.Take(length).ToArray());
    }
}
