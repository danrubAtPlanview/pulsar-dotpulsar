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

namespace DotPulsar.Tests.Internal.Chunking;

using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.PulsarApi;

[Trait("Category", "Unit")]
public sealed class ChunkMessageIdExpanderTests
{
    // Additional coverage
    [Fact]
    public void IsChunked_GivenNull_ShouldBeFalse()
    {
        //Act
        var actual = ChunkMessageIdExpander.IsChunked(null);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void IsChunked_GivenEmptyIds_ShouldBeFalse()
    {
        //Act
        var actual = ChunkMessageIdExpander.IsChunked([]);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void IsChunked_GivenOneId_ShouldBeFalse()
    {
        //Act
        var actual = ChunkMessageIdExpander.IsChunked([ChunkTestData.CreateEntryId(1, 1)]);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void IsChunked_GivenTwoIds_ShouldBeTrue()
    {
        //Act
        var actual = ChunkMessageIdExpander.IsChunked([ChunkTestData.CreateEntryId(1, 1), ChunkTestData.CreateEntryId(1, 2)]);

        //Assert
        actual.ShouldBeTrue();
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#flushAsync (L513-L515), ConsumerImpl#doTransactionAcknowledgeForResponse (L3541-L3567)
    [Fact]
    public void TryExpand_GivenIdsOfChunkedMessage_ShouldAddEveryIdInOrder()
    {
        //Arrange
        var ids = new[] { ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11), ChunkTestData.CreateEntryId(2, 0) };
        var destination = new List<MessageIdData>();

        //Act
        var actual = ChunkMessageIdExpander.TryExpand(ids, destination);

        //Assert
        actual.ShouldBeTrue();
        destination.ShouldBe(ids);
    }

    // Additional coverage
    [Fact]
    public void TryExpand_GivenDestinationOfTheAckCommand_ShouldAddEveryId()
    {
        //Arrange
        var ids = new[] { ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11) };
        var command = new CommandAck();

        //Act
        var actual = ChunkMessageIdExpander.TryExpand(ids, command.MessageId);

        //Assert
        actual.ShouldBeTrue();
        command.MessageId.Count.ShouldBe(2);
        command.MessageId[1].EntryId.ShouldBe(11UL);
    }

    // Additional coverage
    [Fact]
    public void TryExpand_GivenNull_ShouldAddNothing()
    {
        //Arrange
        var destination = new List<MessageIdData>();

        //Act
        var actual = ChunkMessageIdExpander.TryExpand(null, destination);

        //Assert
        actual.ShouldBeFalse();
        destination.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public void TryExpand_GivenOneId_ShouldAddNothing()
    {
        //Arrange
        var destination = new List<MessageIdData>();

        //Act
        var actual = ChunkMessageIdExpander.TryExpand([ChunkTestData.CreateEntryId(1, 1)], destination);

        //Assert
        actual.ShouldBeFalse();
        destination.ShouldBeEmpty();
    }
}
