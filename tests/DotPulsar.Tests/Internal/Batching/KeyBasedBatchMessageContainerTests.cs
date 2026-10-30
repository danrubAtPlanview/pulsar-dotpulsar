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
using Google.Protobuf;

[Trait("Category", "Unit")]
public sealed class KeyBasedBatchMessageContainerTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;

    // Upstream: BatchMessageKeyBasedContainer#createOpSendMsgs
    [Fact]
    public void CreateBatches_GivenInterleavedKeys_ShouldGroupByKeyAndSortByHighestSequenceId()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(0, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(1, key: "B"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "B"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4, key: "A"), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(2);

        groups[0].Start.ShouldBe(0);
        groups[0].Count.ShouldBe(2);
        groups[0].SequenceId.ShouldBe(2ul);
        groups[0].HighestSequenceId.ShouldBe(2ul);
        groups[0].Batch!.Metadata.SequenceId.ShouldBe(2ul);
        groups[0].Batch!.Metadata.HighestSequenceId.ShouldBe(2ul);
        groups[0].Batch!.Metadata.NumMessagesInBatch.ShouldBe(2);
        groups[0].Batch!.Metadata.PartitionKey.ShouldBe("B");
        BatchPayloadReader.Read(groups[0].Batch!).Select(m => m.Metadata.SequenceId).ShouldBeSequence(1ul, 2ul);

        groups[1].Start.ShouldBe(2);
        groups[1].Count.ShouldBe(3);
        groups[1].SequenceId.ShouldBe(4ul);
        groups[1].HighestSequenceId.ShouldBe(4ul);
        groups[1].Batch!.Metadata.SequenceId.ShouldBe(4ul);
        groups[1].Batch!.Metadata.HighestSequenceId.ShouldBe(4ul);
        groups[1].Batch!.Metadata.NumMessagesInBatch.ShouldBe(3);
        groups[1].Batch!.Metadata.PartitionKey.ShouldBe("A");
        BatchPayloadReader.Read(groups[1].Batch!).Select(m => m.Metadata.SequenceId).ShouldBeSequence(0ul, 3ul, 4ul);

        uut.Items.Select(item => item.Metadata.SequenceId).ShouldBeSequence(1ul, 2ul, 0ul, 3ul, 4ul);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenOrderingKey_ShouldPreferOrderingKeyOverPartitionKey()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, key: "A", orderingKey: [1]), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "B", orderingKey: [1]), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4, key: "A", orderingKey: [2]), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Select(g => (g.Count, g.SequenceId)).ShouldBeSequence((2, 2ul), (1, 3ul), (1, 4ul));
        groups[0].Batch!.Metadata.OrderingKey.ToByteArray().ShouldBe([1]);
        groups[0].Batch!.Metadata.PartitionKey.ShouldBe("A");
        uut.Items.Select(item => item.Metadata.SequenceId).ShouldBeSequence(1ul, 2ul, 3ul, 4ul);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenOrderingKeysWithSameBytes_ShouldGroupThemEvenIfTheyAreDifferentInstances()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, orderingKey: [1, 2, 3]), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, orderingKey: [1, 2, 3]), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(1);
        groups[0].Count.ShouldBe(2);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenMessagesWithoutKey_ShouldGroupThemTogether()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(2);
        groups[0].Count.ShouldBe(1);
        groups[0].SequenceId.ShouldBe(2ul);
        groups[1].Count.ShouldBe(3);
        groups[1].SequenceId.ShouldBe(4ul);
        groups[1].Batch!.Metadata.HasPartitionKey.ShouldBeFalse();
        uut.Items.Select(item => item.Metadata.SequenceId).ShouldBeSequence(2ul, 1ul, 3ul, 4ul);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenEmptyPartitionKey_ShouldNotGroupItWithMessagesWithoutKey()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: ""), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(2);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenOneMessageForEachKey_ShouldReturnGroupsWithoutPreparedBatchSortedBySequenceId()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(30, key: "C"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(10, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(20, key: "B"), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Select(g => (g.Start, g.Count, g.SequenceId, g.HighestSequenceId)).ShouldBeSequence((0, 1, 10ul, 10ul), (1, 1, 20ul, 20ul), (2, 1, 30ul, 30ul));
        groups.All(g => g.Batch is null).ShouldBeTrue();
        uut.Items.Select(item => item.Metadata.SequenceId).ShouldBeSequence(10ul, 20ul, 30ul);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenOneMessageForAKey_ShouldKeepTheMetadataOfTheMessage()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        var item = TestBatchItem.Create(10, key: "A");
        var metadataBefore = item.Metadata.Clone();
        uut.Add(item, MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        item.Metadata.ShouldBe(metadataBefore);
        item.Metadata.HasNumMessagesInBatch.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenKeysInAnyOrder_ShouldKeepTheOrderOfTheMessagesOfAKey()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, "a1", key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, "b1", key: "B"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3, "a2", key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4, "b2", key: "B"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(5, "a3", key: "A"), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(2);
        BatchPayloadReader.Read(groups[0].Batch!).Select(m => System.Text.Encoding.UTF8.GetString(m.Payload)).ShouldBeSequence("b1", "b2");
        BatchPayloadReader.Read(groups[1].Batch!).Select(m => System.Text.Encoding.UTF8.GetString(m.Payload)).ShouldBeSequence("a1", "a2", "a3");
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenMoreThan1000MessagesAndManyKeys_ShouldSortAllGroupsAndKeepEveryMessage()
    {
        //Arrange
        const int numberOfMessages = 4000;
        var uut = CreateContainer(maxMessages: 0, maxBytes: 0);
        var groups = new List<PreparedBatchGroup>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            uut.Add(TestBatchItem.Create((ulong) i, key: $"key-{(i * 7) % 300}"), MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(300);
        groups.Sum(g => g.Count).ShouldBe(numberOfMessages);
        for (var i = 1; i < groups.Count; ++i)
        {
            groups[i].SequenceId.ShouldBeGreaterThan(groups[i - 1].SequenceId);
            groups[i].Start.ShouldBe(groups[i - 1].Start + groups[i - 1].Count);
        }

        uut.Items.Count.ShouldBe(numberOfMessages);
        uut.Items.Select(item => item.Metadata.SequenceId).Distinct().Count().ShouldBe(numberOfMessages);
        foreach (var group in groups)
        {
            group.HighestSequenceId.ShouldBe(group.SequenceId);
            var itemsOfGroup = uut.Items.Skip(group.Start).Take(group.Count).ToList();
            itemsOfGroup.Select(item => item.Metadata.PartitionKey).Distinct().Count().ShouldBe(1);
            itemsOfGroup.Max(item => item.Metadata.SequenceId).ShouldBe(group.SequenceId);
        }
    }

    // Upstream: BatchMessageContainerImplTest#testKeyBasedContainerKeepsTransactionalAndPlainMessagesApart
    [Fact]
    public void HasSameTxn_GivenKeyBasedPlainBatchAndTransactionalMessage_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.HasSameTxn(TestBatchItem.Create(2, transaction: (7, 13)));

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void HasSameTxn_GivenKeyBasedTransactionalBatchAndPlainMessage_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, key: "A", transaction: (7, 13)), MaxMessageSize);

        //Act
        var actual = uut.HasSameTxn(TestBatchItem.Create(2, key: "B"));

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void HasSameTxn_GivenKeyBasedMessagesOfOneTransactionWithDifferentKeys_ShouldReturnTrueForSameAndFalseForOtherTransaction()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, key: "A", transaction: (7, 13)), MaxMessageSize);

        //Act
        var sameTransaction = uut.HasSameTxn(TestBatchItem.Create(2, key: "B", transaction: (7, 13)));
        var otherTransaction = uut.HasSameTxn(TestBatchItem.Create(3, key: "A", transaction: (7, 14)));

        //Assert
        sameTransaction.ShouldBeTrue();
        otherTransaction.ShouldBeFalse();
    }

    // Upstream: BatchMessageContainerImplTest#testKeyBasedContainerDropsTheTransactionIdentityOfAFailedFirstAdd (C# deviation: an add cannot fail, so the transaction identity of a cleared container is checked instead)
    [Fact]
    public void HasSameTxn_GivenKeyBasedContainerClearedAfterTransactionalBatch_ShouldTreatNextPlainMessageAsPlain()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, transaction: (7, 13)), MaxMessageSize);
        uut.Clear();
        uut.Add(TestBatchItem.Create(2), MaxMessageSize);

        //Act
        var transactional = uut.HasSameTxn(TestBatchItem.Create(3, transaction: (7, 13)));
        var plain = uut.HasSameTxn(TestBatchItem.Create(4));

        //Assert
        transactional.ShouldBeFalse();
        plain.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenTransactionalMessagesWithDifferentKeys_ShouldCopyTheTransactionToEveryBatch()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, key: "A", transaction: (7, 13)), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "B", transaction: (7, 13)), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3, key: "A", transaction: (7, 13)), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4, key: "B", transaction: (7, 13)), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(2);
        foreach (var group in groups)
        {
            group.Batch!.Metadata.TxnidMostBits.ShouldBe(7ul);
            group.Batch!.Metadata.TxnidLeastBits.ShouldBe(13ul);
        }
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenPlainMessagesAfterTransactionalBatchAndClear_ShouldNotCarryTheTransaction()
    {
        //Arrange
        var uut = CreateContainer(pool: new PreparedBatchPool(8));
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, key: "A", transaction: (7, 13)), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "A", transaction: (7, 13)), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);
        uut.Clear();
        groups[0].Batch!.Return();
        groups.Clear();
        uut.Add(TestBatchItem.Create(3, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4, key: "A"), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(1);
        groups[0].Batch!.Metadata.HasTxnidMostBits.ShouldBeFalse();
        groups[0].Batch!.Metadata.HasTxnidLeastBits.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void HaveEnoughSpace_GivenMaxMessagesReachedAcrossKeys_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 3);
        uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "B"), MaxMessageSize);

        //Act
        var beforeLimit = uut.HaveEnoughSpace(TestBatchItem.Create(3, key: "C"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3, key: "C"), MaxMessageSize);
        var atLimit = uut.HaveEnoughSpace(TestBatchItem.Create(4, key: "A"), MaxMessageSize);

        //Assert
        beforeLimit.ShouldBeTrue();
        atLimit.ShouldBeFalse();
        uut.NumMessagesInBatch.ShouldBe(3);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenMaxBytesReachedAcrossKeys_ShouldReportFull()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 100);

        //Act
        var first = uut.Add(TestBatchItem.CreateWithKey(1, 40, "A"), MaxMessageSize);
        var second = uut.Add(TestBatchItem.CreateWithKey(2, 40, "B"), MaxMessageSize);
        var third = uut.Add(TestBatchItem.CreateWithKey(3, 20, "C"), MaxMessageSize);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeFalse();
        third.ShouldBeTrue();
        uut.CurrentBatchSize.ShouldBe(100);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenMaxMessagesReachedAcrossKeys_ShouldReportFull()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 2);

        //Act
        var first = uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);
        var second = uut.Add(TestBatchItem.Create(2, key: "B"), MaxMessageSize);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void HasSameSchema_GivenOtherSchemaVersionForTheKey_ShouldReturnFalseOnlyForThatKey()
    {
        //Arrange
        var uut = CreateContainer();
        var first = TestBatchItem.Create(1, key: "A");
        first.Metadata.SchemaVersion = ByteString.CopyFrom(1);
        uut.Add(first, MaxMessageSize);
        var otherSchemaSameKey = TestBatchItem.Create(2, key: "A");
        otherSchemaSameKey.Metadata.SchemaVersion = ByteString.CopyFrom(2);
        var otherSchemaOtherKey = TestBatchItem.Create(3, key: "B");
        otherSchemaOtherKey.Metadata.SchemaVersion = ByteString.CopyFrom(2);

        //Act
        var sameKey = uut.HasSameSchema(otherSchemaSameKey);
        var otherKey = uut.HasSameSchema(otherSchemaOtherKey);

        //Assert
        sameKey.ShouldBeFalse();
        otherKey.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void CanAddToCurrentBatch_GivenMessageWithOtherTransaction_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, key: "A", transaction: (7, 13)), MaxMessageSize);

        //Act
        var actual = uut.CanAddToCurrentBatch(TestBatchItem.Create(2, key: "B"), MaxMessageSize);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void ShouldFlush_GivenDelayElapsedSinceTheFirstMessageOfAnyKey_ShouldReturnTrue()
    {
        //Arrange
        var clock = new FakeClock();
        var uut = CreateContainer(maxPublishDelay: TimeSpan.FromMilliseconds(5), clock: clock.Now);
        uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);
        clock.Advance(TimeSpan.FromMilliseconds(3));
        uut.Add(TestBatchItem.Create(2, key: "B"), MaxMessageSize);

        //Act
        var beforeDelay = uut.ShouldFlush(MaxMessageSize);
        clock.Advance(TimeSpan.FromMilliseconds(2));
        var afterDelay = uut.ShouldFlush(MaxMessageSize);

        //Assert
        beforeDelay.ShouldBeFalse();
        afterDelay.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void GetRemainingPublishDelay_GivenEmptyContainer_ShouldReturnInfinite()
    {
        //Arrange
        var uut = CreateContainer();

        //Act
        var actual = uut.GetRemainingPublishDelay();

        //Assert
        actual.ShouldBe(Timeout.InfiniteTimeSpan);
    }

    // Additional coverage
    [Fact]
    public void Items_GivenCreateBatchesNotCalled_ShouldBeEmpty()
    {
        //Arrange
        var uut = CreateContainer();

        //Act
        uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);

        //Assert
        uut.Items.Count.ShouldBe(0);
        uut.NumMessagesInBatch.ShouldBe(1);
        uut.IsEmpty.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void HighestSequenceId_GivenMessagesOfSeveralKeys_ShouldBeTheSequenceIdOfTheLastAddedMessage()
    {
        //Arrange
        var uut = CreateContainer();
        var highestWhenEmpty = uut.HighestSequenceId;

        //Act
        uut.Add(TestBatchItem.Create(5, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(6, key: "B"), MaxMessageSize);
        var afterSecond = uut.HighestSequenceId;
        uut.Add(TestBatchItem.Create(7, key: "A"), MaxMessageSize);
        var afterAdd = uut.HighestSequenceId;
        uut.Clear();

        //Assert
        highestWhenEmpty.ShouldBe(0ul);
        afterSecond.ShouldBe(6ul);
        afterAdd.ShouldBe(7ul);
        uut.HighestSequenceId.ShouldBe(0ul);
    }

    // Additional coverage
    [Fact]
    public void Clear_GivenCreatedBatches_ShouldEmptyTheContainerAndStartCleanForTheNextBatch()
    {
        //Arrange
        var uut = CreateContainer(pool: new PreparedBatchPool(8));
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3, key: "B"), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);
        uut.Clear();
        foreach (var group in groups)
        {
            group.Batch?.Return();
        }

        groups.Clear();

        //Act
        uut.Add(TestBatchItem.Create(10, key: "B"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(11, key: "C"), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        uut.IsEmpty.ShouldBeFalse();
        uut.NumMessagesInBatch.ShouldBe(2);
        groups.Select(g => (g.Start, g.Count, g.SequenceId)).ShouldBeSequence((0, 1, 10ul), (1, 1, 11ul));
        uut.Items.Select(item => item.Metadata.SequenceId).ShouldBeSequence(10ul, 11ul);
    }

    // Additional coverage
    [Fact]
    public void Clear_GivenEmptyContainer_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateContainer();

        //Act
        var exception = Record.Exception(uut.Clear);

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenEmptyContainer_ShouldNotAddAGroup()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenGroupsInDestination_ShouldAppendWithStartRelativeToTheItemsOfTheContainer()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup> { new(0, 1, null, 99, 99) };
        uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "B"), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(3);
        groups[0].SequenceId.ShouldBe(99ul);
        groups[1].Start.ShouldBe(0);
        groups[2].Start.ShouldBe(1);
    }

    // Upstream: BatchMessageKeyBasedContainer#createOpSendMsgs
    [Fact]
    public void CreateBatches_GivenKnownMessagesOfOneKey_ShouldProduceTheSameMetadataAsTheJavaClient()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        foreach (var item in JavaKnownAnswer.CreateItemsWithSameKey())
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(1);
        var metadata = groups[0].Batch!.Metadata;
        metadata.ProducerName = JavaKnownAnswer.ProducerName;
        metadata.PublishTime = JavaKnownAnswer.PublishTime;
        BatchPayloadReader.ToHex(metadata.ToByteArray()).ShouldBe(JavaKnownAnswer.KeyBasedMetadata);
    }

    // Additional coverage
    [Fact]
    public void CreateBatches_GivenSecondRoundWithMoreKeysThanTheFirst_ShouldWorkWithRentedAndNewContainers()
    {
        //Arrange
        var uut = CreateContainer(pool: new PreparedBatchPool(8));
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, key: "A"), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "A"), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);
        uut.Clear();
        groups[0].Batch!.Return();
        groups.Clear();

        for (ulong i = 10; i < 20; ++i)
        {
            uut.Add(TestBatchItem.Create(i, key: $"key-{i % 3}"), MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(3);
        groups.Sum(g => g.Count).ShouldBe(10);
        uut.Items.Count.ShouldBe(10);
    }

    // Upstream: BatchMessageContainerImplTest#testMultiBatchesPartialBuildFailureReleasesBuiltOps
    // The Java container builds the commands of the batches, and when the second batch fails, the command of the first one must be released. The two rows (None and Zlib) differ in who owns the payload after the
    // compression, and the DotPulsar container does not compress, so the rows are one. The batches are the buffers that are rented from the pool: the first is built and the second fails, so both must be
    // returned, the messages must stay in the container, and the build after that must give two batches.
    [Fact]
    public void CreateBatches_GivenSecondBatchThatFailsToBuild_ShouldReturnTheBatchesThatWereBuiltAndKeepAllMessages()
    {
        //Arrange
        var pool = new PreparedBatchPool(8);
        var firstRented = pool.Get();
        var secondRented = pool.Get();
        firstRented.Return();
        secondRented.Return(); // The container rents these two batches, in this order
        var uut = new KeyBasedBatchMessageContainer<FailingBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1), pool);
        var items = new[] { FailingBatchItem.Create(0, "a"), FailingBatchItem.Create(1, "a"), FailingBatchItem.Create(2, "b"), FailingBatchItem.Create(3, "b") };
        var groups = new List<PreparedBatchGroup>();

        foreach (var item in items)
        {
            uut.Add(item, MaxMessageSize);
        }

        items[3].Fail = true; // The batch of the key b fails, and it is built after the batch of the key a

        //Act
        var exception = Record.Exception(() => uut.CreateBatches(groups, MaxMessageSize));
        var groupsAfterFailure = groups.Count;
        var numberOfMessagesAfterFailure = uut.NumMessagesInBatch;
        var firstReturned = firstRented.IsReturned;
        var secondReturned = secondRented.IsReturned;
        items[3].Fail = false;
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        exception.ShouldBeOfType<OutOfMemoryException>();
        groupsAfterFailure.ShouldBe(0);
        numberOfMessagesAfterFailure.ShouldBe(4);
        firstReturned.ShouldBeTrue("the batch that was built is returned");
        secondReturned.ShouldBeTrue("the batch that failed is returned");
        groups.Count.ShouldBe(2);
        groups.Select(group => group.Batch!).ShouldBe([firstRented, secondRented], true);
        BatchPayloadReader.Read(groups[0].Batch!).Select(m => m.Metadata.SequenceId).ShouldBeSequence(0ul, 1ul);
        BatchPayloadReader.Read(groups[1].Batch!).Select(m => m.Metadata.SequenceId).ShouldBeSequence(2ul, 3ul);
        uut.Items.Count.ShouldBe(4);

        //Annihilate
        groups[0].Batch!.Return();
        groups[1].Batch!.Return();
    }

    private static KeyBasedBatchMessageContainer<TestBatchItem> CreateContainer(
        uint maxMessages = 1000,
        uint maxBytes = 128 * 1024,
        TimeSpan? maxPublishDelay = null,
        Func<TimeSpan>? clock = null,
        PreparedBatchPool? pool = null)
        => new(maxMessages, maxBytes, maxPublishDelay ?? TimeSpan.FromMilliseconds(1), pool, clock);
}
