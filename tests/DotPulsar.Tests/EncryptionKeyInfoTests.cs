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
public sealed class EncryptionKeyInfoTests
{
    [Fact]
    public void Constructor_GivenKeyOnly_ShouldHaveEmptyMetadata()
    {
        //Arrange
        var key = new byte[] { 1, 2, 3 };

        //Act
        var actual = new EncryptionKeyInfo(key);

        //Assert
        actual.Key.ShouldBeSameAs(key);
        actual.Metadata.ShouldBeEmpty();
    }

    [Fact]
    public void Constructor_GivenKeyAndMetadata_ShouldHaveBoth()
    {
        //Arrange
        var key = new byte[] { 1, 2, 3 };
        var metadata = new Dictionary<string, string> { ["version"] = "1.0" };

        //Act
        var actual = new EncryptionKeyInfo(key, metadata);

        //Assert
        actual.Key.ShouldBeSameAs(key);
        actual.Metadata.ShouldBeSameAs(metadata);
    }

    [Fact]
    public void Constructor_GivenNullKey_ShouldThrowArgumentNullException()
    {
        //Act
        var exception = Record.Exception(() => new EncryptionKeyInfo(null!));

        //Assert
        exception.ShouldBeOfType<ArgumentNullException>();
    }
}
