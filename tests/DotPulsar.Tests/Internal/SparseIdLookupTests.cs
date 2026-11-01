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
public sealed class SparseIdLookupTests
{
    // Additional coverage
    [Fact]
    public void Add_GivenItems_ShouldGiveTheLowestIdThatIsNotUsed()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var second = new object();

        //Act
        var firstId = uut.Add(new object());
        var secondId = uut.Add(second);
        var thirdId = uut.Add(new object());
        _ = uut.Remove(firstId);
        var reusedId = uut.Add(new object());

        //Assert
        firstId.ShouldBe(0ul);
        secondId.ShouldBe(1ul);
        thirdId.ShouldBe(2ul);
        reusedId.ShouldBe(0ul);
        uut[1].ShouldBeSameAs(second);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenIdsThatWereSet_ShouldSkipTheirIds()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        _ = uut.Set(0, new object());
        _ = uut.Set(1, new object());
        _ = uut.Set(3, new object());

        //Act
        var id = uut.Add(new object());

        //Assert
        id.ShouldBe(2ul);
    }

    // Additional coverage
    [Fact]
    public void Set_GivenIdThatIsNotUsed_ShouldAddTheItemAndReturnNull()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var item = new object();

        //Act
        var replaced = uut.Set(ulong.MaxValue, item);

        //Assert
        replaced.ShouldBeNull();
        uut[ulong.MaxValue].ShouldBeSameAs(item);
        uut.IsEmpty().ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Set_GivenIdThatIsUsed_ShouldReplaceTheItemAndReturnIt()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var first = new object();
        var second = new object();
        _ = uut.Set(1_000_000, first);

        //Act
        var replaced = uut.Set(1_000_000, second);

        //Assert
        replaced.ShouldBeSameAs(first);
        uut[1_000_000].ShouldBeSameAs(second);
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenId_ShouldRemoveAndReturnTheItem()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var item = new object();
        _ = uut.Set(5, item);

        //Act
        var removed = uut.Remove(5);

        //Assert
        removed.ShouldBeSameAs(item);
        uut[5].ShouldBeNull();
        uut.IsEmpty().ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenIdThatIsNotUsed_ShouldReturnNullAndNotThrow()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        object? removed = new();

        //Act
        var exception = Record.Exception(() => removed = uut.Remove(1_000_000));

        //Assert
        exception.ShouldBeNull();
        removed.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenItemThatIsTheOneWithTheId_ShouldRemoveIt()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var item = new object();
        _ = uut.Set(7, item);

        //Act
        var removed = uut.Remove(7, item);

        //Assert
        removed.ShouldBeTrue();
        uut[7].ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenItemThatHasBeenReplaced_ShouldKeepTheItemThatReplacedIt()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var replaced = new object();
        var replacement = new object();
        _ = uut.Set(7, replaced);
        _ = uut.Set(7, replacement);

        //Act
        var removed = uut.Remove(7, replaced);

        //Assert
        removed.ShouldBeFalse();
        uut[7].ShouldBeSameAs(replacement);
    }

    // Additional coverage
    [Fact]
    public void Indexer_GivenIdThatIsNotUsed_ShouldReturnNullAndNotThrow()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        _ = uut.Add(new object());
        object? item = new();

        //Act
        var exception = Record.Exception(() => item = uut[1_000_000]);

        //Assert
        exception.ShouldBeNull();
        item.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void RemoveAll_GivenItems_ShouldReturnAllOfThemAndEmptyTheLookup()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        var first = new object();
        var second = new object();
        _ = uut.Set(3, first);
        _ = uut.Set(1_000_000, second);

        //Act
        var removed = uut.RemoveAll();

        //Assert
        removed.ShouldBe([first, second], ignoreOrder: true);
        uut.IsEmpty().ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void IsEmpty_GivenNoItems_ShouldBeTrue()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();

        //Act
        var isEmpty = uut.IsEmpty();

        //Assert
        isEmpty.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task Add_GivenManyThreads_ShouldGiveEveryItemItsOwnId()
    {
        //Arrange
        var uut = new SparseIdLookup<object>();
        const int numberOfItems = 2000;

        //Act
        var ids = await Task.WhenAll(Enumerable.Range(0, numberOfItems).Select(_ => Task.Run(() => uut.Add(new object()))));

        //Assert
        ids.Distinct().Count().ShouldBe(numberOfItems);
        uut.RemoveAll().Length.ShouldBe(numberOfItems);
    }
}
