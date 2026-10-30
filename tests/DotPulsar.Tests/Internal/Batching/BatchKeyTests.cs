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

namespace DotPulsar.Tests.Internal.Batching;

using DotPulsar.Internal.Batching;

[Trait("Category", "Unit")]
public sealed class BatchKeyTests
{
    [Fact]
    public void Equals_GivenSamePartitionKey_ShouldReturnTrueAndSameHashCode()
    {
        //Arrange
        var uut = CreateKey(key: "A");
        var other = CreateKey(key: new string('A', 1));

        //Act
        var actual = uut.Equals(other);

        //Assert
        actual.ShouldBeTrue();
        uut.GetHashCode().ShouldBe(other.GetHashCode());
    }

    [Fact]
    public void Equals_GivenOtherPartitionKey_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateKey(key: "A");

        //Act
        var actual = uut.Equals(CreateKey(key: "B"));

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenSameOrderingKeyBytes_ShouldReturnTrueAndSameHashCode()
    {
        //Arrange
        var uut = CreateKey(orderingKey: [1, 2, 3]);
        var other = CreateKey(orderingKey: [1, 2, 3]);

        //Act
        var actual = uut.Equals(other);

        //Assert
        actual.ShouldBeTrue();
        uut.GetHashCode().ShouldBe(other.GetHashCode());
    }

    [Fact]
    public void Equals_GivenOtherOrderingKeyBytes_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateKey(orderingKey: [1, 2, 3]);

        //Act
        var actual = uut.Equals(CreateKey(orderingKey: [1, 2, 4]));

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenOrderingKeyAndPartitionKey_ShouldPreferTheOrderingKey()
    {
        //Arrange
        var uut = CreateKey(key: "A", orderingKey: [1]);

        //Act
        var sameOrderingKeyOtherPartitionKey = uut.Equals(CreateKey(key: "B", orderingKey: [1]));
        var samePartitionKeyWithoutOrderingKey = uut.Equals(CreateKey(key: "A"));

        //Assert
        sameOrderingKeyOtherPartitionKey.ShouldBeTrue();
        samePartitionKeyWithoutOrderingKey.ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenNoKeys_ShouldReturnTrueAndSameHashCode()
    {
        //Arrange
        var uut = CreateKey();
        var other = CreateKey();

        //Act
        var actual = uut.Equals(other);

        //Assert
        actual.ShouldBeTrue();
        uut.GetHashCode().ShouldBe(other.GetHashCode());
    }

    [Fact]
    public void Equals_GivenEmptyPartitionKeyAndNoKey_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateKey(key: "");

        //Act
        var actual = uut.Equals(CreateKey());

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenEmptyOrderingKeyAndNoKey_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateKey(orderingKey: []);

        //Act
        var actual = uut.Equals(CreateKey());

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenObjectThatIsNotABatchKey_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateKey(key: "A");

        //Act
        var actual = uut.Equals("A");

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenBoxedBatchKey_ShouldCompareTheKeys()
    {
        //Arrange
        var uut = CreateKey(key: "A");

        //Act
        var actual = uut.Equals((object) CreateKey(key: "A"));

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void Dictionary_GivenKeysOfDifferentMessages_ShouldFindTheValueOfEqualKeys()
    {
        //Arrange
        var uut = new Dictionary<BatchKey, int>
        {
            [CreateKey(key: "A")] = 1,
            [CreateKey(orderingKey: [1])] = 2,
            [CreateKey()] = 3
        };

        //Act
        var partitionKey = uut[CreateKey(key: "A")];
        var orderingKey = uut[CreateKey(orderingKey: [1])];
        var noKey = uut[CreateKey()];

        //Assert
        partitionKey.ShouldBe(1);
        orderingKey.ShouldBe(2);
        noKey.ShouldBe(3);
        uut.ContainsKey(CreateKey(key: "B")).ShouldBeFalse();
    }

    private static BatchKey CreateKey(string? key = null, byte[]? orderingKey = null)
        => new(TestBatchItem.Create(1, key: key, orderingKey: orderingKey).Metadata);
}
