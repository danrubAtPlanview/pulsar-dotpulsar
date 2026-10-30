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
using DotPulsar.Internal.PulsarApi;

[Trait("Category", "Unit")]
public sealed class BatchMessageIdsTests
{
    private const string Topic = "persistent://public/default/test";

    // Additional coverage
    [Fact]
    public void Create_GivenBatchOfThree_ShouldReturnIdWithTheIndexInTheBatch()
    {
        //Arrange
        var receipt = new MessageIdData { LedgerId = 5, EntryId = 6, BatchIndex = -1 };

        //Act
        var actual = Enumerable.Range(0, 3).Select(index => BatchMessageIds.Create(receipt, 2, Topic, 3, index)).ToArray();

        //Assert
        actual.Select(id => id.BatchIndex).ShouldBeSequence(0, 1, 2);
        actual.ShouldAllBe(id => id.LedgerId == 5 && id.EntryId == 6 && id.Partition == 2 && id.Topic == Topic);
        actual[1].ShouldBe(new MessageId(5, 6, 2, 1, Topic));
    }

    // Additional coverage
    [Fact]
    public void Create_GivenSingleMessage_ShouldKeepTheBatchIndexOfTheReceipt()
    {
        //Arrange
        var receipt = new MessageIdData { LedgerId = 5, EntryId = 6, BatchIndex = -1 };

        //Act
        var actual = BatchMessageIds.Create(receipt, 0, Topic, 1, 0);

        //Assert
        actual.ShouldBe(new MessageId(5, 6, 0, -1, Topic));
        actual.BatchIndex.ShouldBe(-1);
    }

    // Additional coverage
    [Fact]
    public void Create_GivenSingleMessageAndReceiptWithoutBatchIndex_ShouldUseTheDefaultBatchIndexOfTheReceipt()
    {
        //Arrange
        var receipt = new MessageIdData { LedgerId = 1, EntryId = 2 };

        //Act
        var actual = BatchMessageIds.Create(receipt, 0, Topic, 1, 0);

        //Assert
        actual.BatchIndex.ShouldBe(receipt.BatchIndex);
    }

    // Additional coverage
    [Fact]
    public void Create_GivenBatchIndexOfALargeBatch_ShouldReturnThatBatchIndex()
    {
        //Arrange
        var receipt = new MessageIdData { LedgerId = 5, EntryId = 6, BatchIndex = -1 };

        //Act
        var actual = BatchMessageIds.Create(receipt, 0, Topic, 5000, 4999);

        //Assert
        actual.BatchIndex.ShouldBe(4999);
    }
}
