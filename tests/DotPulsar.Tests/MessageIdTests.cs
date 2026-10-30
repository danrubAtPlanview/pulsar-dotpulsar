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

#nullable disable

namespace DotPulsar.Tests;

using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal;

[Trait("Category", "Unit")]
public class MessageIdTests
{
    [Fact]
    public void CompareTo_GivenTheSameValues_ShouldBeEqual()
    {
        var m1 = new MessageId(1, 2, 3, 4);
        var m2 = new MessageId(1, 2, 3, 4);

        m1.CompareTo(m2).ShouldBe(0);
        (m1 < m2).ShouldBeFalse();
        (m1 > m2).ShouldBeFalse();
        (m1 >= m2).ShouldBeTrue();
        (m1 <= m2).ShouldBeTrue();
    }

    [Fact]
    public void CompareTo_GivenTheSameValuesWithTopic_ShouldBeEqual()
    {
        var m1 = new MessageId(1, 2, 3, 4, "persistent://public/default/my-topic-partition-0");
        var m2 = new MessageId(1, 2, 3, 4, "persistent://public/default/my-topic-partition-0");

        m1.CompareTo(m2).ShouldBe(0);
        (m1 < m2).ShouldBeFalse();
        (m1 > m2).ShouldBeFalse();
        (m1 >= m2).ShouldBeTrue();
        (m1 <= m2).ShouldBeTrue();
    }

    [Fact]
    public void CompareTo_GivenAllNull_ShouldBeEqual()
    {
        MessageId m1 = null;
        MessageId m2 = null;

        (m1 < m2).ShouldBeFalse();
        (m1 > m2).ShouldBeFalse();
        (m1 <= m2).ShouldBeTrue();
        (m1 >= m2).ShouldBeTrue();
    }

    [Fact]
    public void CompareTo_GivenOneNull_ShouldFollowNull()
    {
        var m1 = new MessageId(1, 2, 3, 4);
        MessageId m2 = null;

        m1.CompareTo(m2).ShouldBePositive();
        (m1 < m2).ShouldBeFalse();
        (m1 > m2).ShouldBeTrue();
        (m1 <= m2).ShouldBeFalse();
        (m1 >= m2).ShouldBeTrue();

        (m2 < m1).ShouldBeTrue();
        (m2 > m1).ShouldBeFalse();
        (m2 <= m1).ShouldBeTrue();
        (m2 >= m1).ShouldBeFalse();
    }

    [Theory]
    [InlineData(2, 2, 3, 4)] // LegderId is greater
    [InlineData(1, 3, 3, 4)] // EntryId is greater
    [InlineData(1, 2, 4, 4)] // Partition is greater
    [InlineData(1, 2, 3, 5)] // BatchIndex is greater
    public void CompareTo_GivenCurrentFollowsArgument_ShouldReturnPositive(ulong ledgerId, ulong entryId, int partition, int batchIndex)
    {
        var m1 = new MessageId(ledgerId, entryId, partition, batchIndex);
        var m2 = new MessageId(1, 2, 3, 4);

        m1.CompareTo(m2).ShouldBePositive();
        (m1 > m2).ShouldBeTrue();
        (m1 < m2).ShouldBeFalse();
    }

    [Theory]
    [InlineData(0, 2, 3, 4)] // LegderId is less
    [InlineData(1, 1, 3, 4)] // EntryId is less
    [InlineData(1, 2, 2, 4)] // Partition is less
    [InlineData(1, 2, 3, 3)] // BatchIndex is less
    public void CompareTo_GivenCurrentPrecedesArgument_ShouldReturnNegative(ulong ledgerId, ulong entryId, int partition, int batchIndex)
    {
        var m1 = new MessageId(ledgerId, entryId, partition, batchIndex);
        var m2 = new MessageId(1, 2, 3, 4);

        m1.CompareTo(m2).ShouldBeNegative();
        (m1 < m2).ShouldBeTrue();
        (m1 > m2).ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenTheSameObject_ShouldBeEqual()
    {
        var m1 = new MessageId(1, 2, 3, 4);
        var m2 = m1;

        m1.Equals(m2).ShouldBeTrue();
        (m1 == m2).ShouldBeTrue();
        (m1 != m2).ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenTheSameValues_ShouldBeEqual()
    {
        var m1 = new MessageId(1, 2, 3, 4);
        var m2 = new MessageId(1, 2, 3, 4);

        m1.Equals(m2).ShouldBeTrue();
        (m1 == m2).ShouldBeTrue();
        (m1 != m2).ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenTheSameValuesWithTopic_ShouldBeEqual()
    {
        var m1 = new MessageId(1, 2, 3, 4, "persistent://public/default/my-topic-partition-0");
        var m2 = new MessageId(1, 2, 3, 4, "persistent://public/default/my-topic-partition-0");

        m1.Equals(m2).ShouldBeTrue();
        (m1 == m2).ShouldBeTrue();
        (m1 != m2).ShouldBeFalse();
    }

    [Theory]
    [InlineData(0, 2, 3, 4)] // LegerId not the same
    [InlineData(1, 0, 3, 4)] // EntryId not the same
    [InlineData(1, 2, 0, 4)] // Partition not the same
    [InlineData(1, 2, 3, 0)] // BatchIndex not the same
    public void Equals_GivenDifferentValues_ShouldNotBeEqual(ulong ledgerId, ulong entryId, int partition, int batchIndex)
    {
        var m1 = new MessageId(ledgerId, entryId, partition, batchIndex);
        var m2 = new MessageId(1, 2, 3, 4);

        m1.Equals(m2).ShouldBeFalse();
        (m1 == m2).ShouldBeFalse();
        (m1 != m2).ShouldBeTrue();
    }

    [Theory]
    [InlineData(0, 2, 3, 4, "persistent://public/default/my-topic-partition-1")] // LegerId not the same
    [InlineData(1, 0, 3, 4, "persistent://public/default/my-topic-partition-1")] // EntryId not the same
    [InlineData(1, 2, 0, 4, "persistent://public/default/my-topic-partition-1")] // Partition not the same
    [InlineData(1, 2, 3, 0, "persistent://public/default/my-topic-partition-1")] // BatchIndex not the same
    [InlineData(1, 2, 3, 4, "persistent://public/default/my-topic-partition-0")] // Topic not the same
    public void Equals_GivenDifferentValuesWithTopic_ShouldNotBeEqual(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic)
    {
        var m1 = new MessageId(ledgerId, entryId, partition, batchIndex, topic);
        var m2 = new MessageId(1, 2, 3, 4, "persistent://public/default/my-topic-partition-1");

        m1.Equals(m2).ShouldBeFalse();
        (m1 == m2).ShouldBeFalse();
        (m1 != m2).ShouldBeTrue();
    }

    [Fact]
    public void Equals_GivenAllNull_ShouldBeEqual()
    {
        MessageId m1 = null;
        MessageId m2 = null;

        (m1 == m2).ShouldBeTrue();
        (m1 is null).ShouldBeTrue();
        (m1 != m2).ShouldBeFalse();
    }

    [Fact]
    public void Equals_GivenOneNull_ShouldNotBeEqual()
    {
        var m1 = new MessageId(1, 2, 3, 4);
        MessageId m2 = null;

        (m1 is null).ShouldBeFalse();
        (m1 == m2).ShouldBeFalse();
        m1.Equals(m2).ShouldBeFalse();
        (m1 != m2).ShouldBeTrue();
    }

    [Theory]
    [InlineData("0.0.0.0")] // Wrong separator
    [InlineData("A:0:0:0")] // Ledger id is not a number
    [InlineData("0:A:0:0")] // Entry id is not a number
    [InlineData("0:0:A:0")] // Partition id is not a number
    [InlineData("0:0:0:A")] // Batch index is not a number
    [InlineData(":::")] // Missing all
    [InlineData(":0:0:0")] // Missing ledger id
    [InlineData("0::0:0")] // Missing entry id
    [InlineData("0:0::0")] // Missing partion
    [InlineData("0:0:0:")] // Missing batch index
    public void TryParse_GivenInvalidString_ShouldReturnFalse(string input)
    {
        // Arrange
        var expected = MessageId.Earliest;

        // Act
        var result = MessageId.TryParse(input, out var actual);

        // Assert
        result.ShouldBeFalse();
        actual.ShouldBe(expected);
    }

    [Fact]
    public void TryParse_GivenValidString_ShouldReturnTrue()
    {
        // Arrange
        var expected = new MessageId(1, 2, 3, 4, "topic");

        // Act
        var result = MessageId.TryParse(expected.ToString(), out var actual);

        // Assert
        result.ShouldBeTrue();
        actual.ShouldBe(expected);
    }

    // Upstream: BatchMessageIdImplTest#compareToTest
    [Fact]
    public void CompareTo_GivenBatchIds0000And1111_ShouldOrder()
    {
        //Arrange
        var batchMsgId1 = new MessageId(0, 0, 0, 0);
        var batchMsgId2 = new MessageId(1, 1, 1, 1);

        //Act
        var first = batchMsgId1.CompareTo(batchMsgId2);
        var second = batchMsgId2.CompareTo(batchMsgId1);
        var same = batchMsgId2.CompareTo(batchMsgId2);

        //Assert
        first.ShouldBe(-1);
        second.ShouldBe(1);
        same.ShouldBe(0);
    }

    // Upstream: BatchMessageIdImplTest#hashCodeTest
    [Fact]
    public void GetHashCode_GivenDifferentBatchIds_ShouldDiffer()
    {
        //Arrange
        var batchMsgId1 = new MessageId(0, 0, 0, 0);
        var batchMsgId2 = new MessageId(1, 1, 1, 1);

        //Act
        var hashCode1 = batchMsgId1.GetHashCode();
        var hashCode2 = batchMsgId2.GetHashCode();

        //Assert
        hashCode1.ShouldBe(batchMsgId1.GetHashCode());
        hashCode1.ShouldNotBe(hashCode2);
    }

    // Upstream: BatchMessageIdImplTest#equalsTest
    [Fact]
    public void Equals_GivenBatchAndNonBatchIds_ShouldFollowJavaRules()
    {
        //Arrange
        var batchMsgId1 = new MessageId(0, 0, 0, 0);
        var batchMsgId2 = new MessageId(1, 1, 1, 1);
        var batchMsgId3 = new MessageId(0, 0, 0, 1);
        var batchMsgId4 = new MessageId(0, 0, 0, -1);
        var msgId = new MessageId(0, 0, 0, -1);

        //Assert
        batchMsgId1.Equals(batchMsgId1).ShouldBeTrue();
        batchMsgId2.Equals(batchMsgId1).ShouldBeFalse();
        batchMsgId3.Equals(batchMsgId1).ShouldBeFalse();
        batchMsgId4.Equals(batchMsgId1).ShouldBeFalse();
        msgId.Equals(batchMsgId1).ShouldBeFalse();

        msgId.Equals(msgId).ShouldBeTrue();
        batchMsgId1.Equals(msgId).ShouldBeFalse();
        batchMsgId2.Equals(msgId).ShouldBeFalse();
        batchMsgId3.Equals(msgId).ShouldBeFalse();
        batchMsgId4.Equals(msgId).ShouldBeTrue();
        msgId.Equals(batchMsgId4).ShouldBeTrue();
    }

    // Upstream: BatchMessageIdImplTest#compareToUnbatchedTest
    [Fact]
    public void CompareTo_GivenUnbatchedAndBatchIndex0_ShouldBeLess()
    {
        //Arrange
        var msgId = new MessageId(1, 2, 3, -1);
        var batchMsgId = new MessageId(1, 2, 3, 0);
        var unbatchedBatchMsgId = new MessageId(1, 2, 3, -1);

        //Assert
        msgId.CompareTo(batchMsgId).ShouldBe(-1);
        batchMsgId.CompareTo(msgId).ShouldBe(1);
        msgId.CompareTo(unbatchedBatchMsgId).ShouldBe(0);
        unbatchedBatchMsgId.CompareTo(msgId).ShouldBe(0);
    }

    // Upstream: BatchMessageIdImplTest#equalsUnbatchedTest
    [Fact]
    public void Equals_GivenUnbatchedIds_ShouldCompareCoordinates()
    {
        //Arrange
        var batchMsgId1 = new MessageId(0, 0, 0, -1);
        var batchMsgId2 = new MessageId(1, 1, 1, -1);
        var msgId1 = new MessageId(0, 0, 0, -1);
        var msgId2 = new MessageId(1, 1, 1, -1);

        //Assert
        batchMsgId1.Equals(msgId1).ShouldBeTrue();
        batchMsgId2.Equals(msgId2).ShouldBeTrue();
        batchMsgId1.Equals(msgId2).ShouldBeFalse();
        batchMsgId2.Equals(msgId1).ShouldBeFalse();
    }

    // Upstream: BatchMessageIdImplTest#hashCodeUnbatchedTest
    [Fact]
    public void GetHashCode_GivenEqualUnbatchedIds_ShouldBeEqual()
    {
        //Arrange
        var batchMsgId1 = new MessageId(0, 0, 0, -1);
        var batchMsgId2 = new MessageId(1, 1, 1, -1);
        var msgId1 = new MessageId(0, 0, 0, -1);
        var msgId2 = new MessageId(1, 1, 1, -1);

        //Assert
        batchMsgId1.GetHashCode().ShouldBe(msgId1.GetHashCode());
        batchMsgId2.GetHashCode().ShouldBe(msgId2.GetHashCode());
    }

    [Fact]
    public void BatchSize_ShouldNotTakePartInEqualityOrHashCode()
    {
        //Arrange
        var withoutBatchSize = new MessageId(1, 2, 3, 4, "topic");
        var withBatchSize = MessageIdInternals.Create(1, 2, 3, 4, "topic", 10);

        //Assert
        withBatchSize.GetBatchSize().ShouldBe(10);
        withoutBatchSize.GetBatchSize().ShouldBe(0);
        withBatchSize.Equals(withoutBatchSize).ShouldBeTrue();
        withBatchSize.GetHashCode().ShouldBe(withoutBatchSize.GetHashCode());
        withBatchSize.CompareTo(withoutBatchSize).ShouldBe(0);
        withBatchSize.ToString().ShouldBe(withoutBatchSize.ToString());
    }

    [Fact]
    public void TryParse_GivenValidString_ShouldNotSetBatchSize()
    {
        //Arrange
        var messageId = MessageIdInternals.Create(1, 2, 3, 4, "topic", 10);

        //Act
        MessageId.TryParse(messageId.ToString(), out var actual);

        //Assert
        actual.GetBatchSize().ShouldBe(0);
    }

    [Fact]
    public void FirstChunkMessageId_GivenIdThatIsNotChunked_ShouldBeNull()
    {
        //Act
        var uut = new MessageId(1, 2, 3, 4, "topic");

        //Assert
        uut.FirstChunkMessageId.ShouldBeNull();
        uut.GetChunkMessageIds().ShouldBeNull();
    }

    [Fact]
    public void CreateChunked_GivenChunkIds_ShouldUseTheLastChunkAndNoBatchIndex()
    {
        //Arrange
        var chunkIds = new[] { CreateChunkId(1, 10), CreateChunkId(1, 11), CreateChunkId(2, 3) };

        //Act
        var uut = MessageIdInternals.CreateChunked("topic", chunkIds);

        //Assert
        uut.LedgerId.ShouldBe(2UL);
        uut.EntryId.ShouldBe(3UL);
        uut.Partition.ShouldBe(0);
        uut.BatchIndex.ShouldBe(-1);
        uut.Topic.ShouldBe("topic");
        uut.FirstChunkMessageId.ShouldNotBeNull();
        uut.FirstChunkMessageId.LedgerId.ShouldBe(1UL);
        uut.FirstChunkMessageId.EntryId.ShouldBe(10UL);
        uut.FirstChunkMessageId.BatchIndex.ShouldBe(-1);
        uut.GetChunkMessageIds().ShouldBeSameAs(chunkIds);
    }

    // Upstream: ChunkMessageIdImplTest#compareToTest
    [Fact]
    public void CompareTo_GivenChunkMessageIds_ShouldCompareByLastChunk()
    {
        //Arrange
        var chunkMessageId1 = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(0, 0), CreateChunkId(1, 1));
        var chunkMessageId2 = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(2, 2), CreateChunkId(3, 3));
        var sameLastChunk = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(0, 5), CreateChunkId(1, 1));

        //Assert
        chunkMessageId1.CompareTo(chunkMessageId2).ShouldBeNegative();
        chunkMessageId2.CompareTo(chunkMessageId1).ShouldBePositive();
        chunkMessageId1.CompareTo(sameLastChunk).ShouldBe(0);
        (chunkMessageId1 < chunkMessageId2).ShouldBeTrue();
        chunkMessageId1.CompareTo(new MessageId(1, 1, 0, -1)).ShouldBe(0);
    }

    // Upstream: ChunkMessageIdImplTest#hashCodeTest (C# deviation: equal ids have equal hash codes, so the first chunk is not part of the hash code)
    [Fact]
    public void GetHashCode_GivenDifferentChunkMessageIds_ShouldDiffer()
    {
        //Arrange
        var chunkMessageId1 = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(0, 0), CreateChunkId(1, 1));
        var sameAsChunkMessageId1 = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(0, 0), CreateChunkId(1, 1));
        var chunkMessageId2 = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(2, 2), CreateChunkId(3, 3));

        //Assert
        chunkMessageId1.GetHashCode().ShouldBe(sameAsChunkMessageId1.GetHashCode());
        chunkMessageId1.GetHashCode().ShouldNotBe(chunkMessageId2.GetHashCode());
    }

    // Upstream: ChunkMessageIdImplTest#equalsTest
    [Fact]
    public void Equals_GivenChunkMessageIdAndItsLastChunkId_ShouldBeEqual()
    {
        //Arrange
        var chunkMessageId = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(0, 0), CreateChunkId(1, 1));
        var otherFirstChunk = MessageIdInternals.CreateChunked(string.Empty, CreateChunkId(9, 9), CreateChunkId(1, 1));
        var lastChunkId = new MessageId(1, 1, 0, -1);
        var otherId = new MessageId(1, 2, 0, -1);

        //Assert
        chunkMessageId.Equals(lastChunkId).ShouldBeTrue();
        lastChunkId.Equals(chunkMessageId).ShouldBeTrue();
        (chunkMessageId == otherFirstChunk).ShouldBeTrue();
        chunkMessageId.GetHashCode().ShouldBe(lastChunkId.GetHashCode());
        chunkMessageId.Equals(otherId).ShouldBeFalse();
    }

    [Fact]
    public void ToString_GivenChunkMessageId_ShouldBeTheStringOfTheLastChunkAndParseToAnIdThatIsNotChunked()
    {
        //Arrange
        var uut = MessageIdInternals.CreateChunked("topic", CreateChunkId(0, 0), CreateChunkId(1, 1));

        //Act
        var actual = uut.ToString();
        var parsed = MessageId.TryParse(actual, out var result);

        //Assert
        actual.ShouldBe("1:1:0:-1:topic");
        parsed.ShouldBeTrue();
        result.ShouldBe(uut);
        result.FirstChunkMessageId.ShouldBeNull();
        result.GetChunkMessageIds().ShouldBeNull();
    }

    private static MessageIdData CreateChunkId(ulong ledgerId, ulong entryId)
        => new() { LedgerId = ledgerId, EntryId = entryId, Partition = 0, BatchIndex = -1 };
}
