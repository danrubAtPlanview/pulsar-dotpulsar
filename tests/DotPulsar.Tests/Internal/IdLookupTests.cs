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

// The ids of consumers and of the handlers of the transaction meta store are given out by this lookup, so how it gives out ids is not changed
[Trait("Category", "Unit")]
public sealed class IdLookupTests
{
    // Additional coverage
    [Fact]
    public void Add_GivenItems_ShouldGiveTheLowestIdThatIsNotUsed()
    {
        //Arrange
        var uut = new IdLookup<object>();
        var second = new object();

        //Act
        var firstId = uut.Add(new object());
        var secondId = uut.Add(second);
        var thirdId = uut.Add(new object());
        _ = uut.Remove(firstId);
        var reusedId = uut.Add(new object());
        var nextId = uut.Add(new object());

        //Assert
        firstId.ShouldBe(0ul);
        secondId.ShouldBe(1ul);
        thirdId.ShouldBe(2ul);
        reusedId.ShouldBe(0ul);
        nextId.ShouldBe(3ul);
        uut[1].ShouldBeSameAs(second);
    }

    // Additional coverage
    [Fact]
    public void RemoveAll_GivenItems_ShouldReturnAllOfThemAndEmptyTheLookup()
    {
        //Arrange
        var uut = new IdLookup<object>();
        var first = new object();
        var second = new object();
        _ = uut.Add(first);
        _ = uut.Add(second);

        //Act
        var removed = uut.RemoveAll();

        //Assert
        removed.ShouldBe([first, second]);
        uut.IsEmpty().ShouldBeTrue();
    }

    // Additional coverage (followup 0005 item 4)
    // An id that the lookup never gave out can come from the broker. 4294967296 is 2^32, which a cast to an int turns into 0, so it must not find the item with the id 0.
    [Theory]
    [InlineData(1ul)]
    [InlineData(42ul)]
    [InlineData(2147483648ul)]
    [InlineData(4294967296ul)]
    [InlineData(ulong.MaxValue)]
    public void Indexer_GivenIdThatWasNeverGivenOut_ShouldReturnNull(ulong id)
    {
        //Arrange
        var uut = new IdLookup<object>();
        var item = new object();
        _ = uut.Add(item);

        //Act
        var found = uut[id];

        //Assert
        found.ShouldBeNull();
        uut[0].ShouldBeSameAs(item);
    }

    // Additional coverage (followup 0005 item 4)
    [Theory]
    [InlineData(1ul)]
    [InlineData(42ul)]
    [InlineData(4294967296ul)]
    [InlineData(ulong.MaxValue)]
    public void Remove_GivenIdThatWasNeverGivenOut_ShouldReturnNullAndKeepTheItems(ulong id)
    {
        //Arrange
        var uut = new IdLookup<object>();
        var item = new object();
        _ = uut.Add(item);

        //Act
        var removed = uut.Remove(id);

        //Assert
        removed.ShouldBeNull();
        uut[0].ShouldBeSameAs(item);
        uut.IsEmpty().ShouldBeFalse();
    }

    // Additional coverage (followup 0005 item 4)
    // The lookup replaces its array when it grows, so it must not use the array as the object of its lock: two callers that lock the old and the new array can then both run, and give the same id to two items.
    [Fact]
    public async Task Add_GivenConcurrentCallers_ShouldGiveEveryItemItsOwnId()
    {
        //Arrange
        const int callers = 8;
        const int itemsPerCaller = 400;
        var uut = new IdLookup<object>();
        using var start = new ManualResetEventSlim(false);

        //Act
        var tasks = Enumerable.Range(0, callers).Select(_ => Task.Run(() =>
        {
            start.Wait();
            var ids = new List<(ulong Id, object Item)>();

            for (var i = 0; i < itemsPerCaller; ++i)
            {
                var item = new object();
                ids.Add((uut.Add(item), item));
            }

            return ids;
        })).ToArray();
        start.Set();
        var added = (await Task.WhenAll(tasks)).SelectMany(ids => ids).ToArray();

        //Assert
        added.Select(entry => entry.Id).Distinct().Count().ShouldBe(callers * itemsPerCaller);
        foreach (var (id, item) in added)
            uut[id].ShouldBeSameAs(item);
    }
}
