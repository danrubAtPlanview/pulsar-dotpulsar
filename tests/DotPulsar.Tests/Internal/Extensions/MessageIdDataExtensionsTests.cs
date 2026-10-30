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

namespace DotPulsar.Tests.Internal.Extensions;

using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;

[Trait("Category", "Unit")]
public sealed class MessageIdDataExtensionsTests
{
    [Fact]
    public void MapFrom_GivenMessageId_ShouldCopyTheIdentifiers()
    {
        //Arrange
        var uut = new MessageIdData();

        //Act
        uut.MapFrom(new MessageId(1, 2, 3, 4));

        //Assert
        uut.LedgerId.ShouldBe(1UL);
        uut.EntryId.ShouldBe(2UL);
        uut.Partition.ShouldBe(3);
        uut.BatchIndex.ShouldBe(4);
    }

    [Fact]
    public void MapFrom_GivenPooledInstanceWithAckSet_ShouldResetAckSetAndBatchSize()
    {
        //Arrange
        var uut = new MessageIdData { LedgerId = 9, EntryId = 9, Partition = 9, BatchIndex = 9, BatchSize = 10 };
        uut.AckSet.AddRange([1L, 2L, 3L]);
        uut.FirstChunkMessageId = new MessageIdData { LedgerId = 5, EntryId = 5 };

        //Act
        uut.MapFrom(new MessageId(1, 2, 3, 4));

        //Assert
        uut.AckSet.Count.ShouldBe(0);
        uut.HasBatchSize.ShouldBeFalse();
        uut.FirstChunkMessageId.ShouldBeNull();
        uut.LedgerId.ShouldBe(1UL);
        uut.EntryId.ShouldBe(2UL);
    }
}
