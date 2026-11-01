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
}
