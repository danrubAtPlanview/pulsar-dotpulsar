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

using DotPulsar.Internal;
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;
using System.Text;

[Trait("Category", "Unit")]
public sealed class BatchMessageContainerTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;

    // Upstream: BatchMessageContainerImplTest#testUpdateMaxBatchSize
    [Fact]
    public void UpdateMaxBatchSize_GivenGrowShrinkAndAlternatingSizes_ShouldFollowCoolingOffRules()
    {
        //Arrange
        const int shrinkCoolingOffPeriod = 10;
        var uut = CreateContainer();

        //Assert
        uut.MaxBatchSize.ShouldBe(1024);

        //Act and Assert: expand
        uut.UpdateMaxBatchSize(2048);
        uut.MaxBatchSize.ShouldBe(2048);

        //Act and Assert: cooling-off period
        uut.UpdateMaxBatchSize(2);
        uut.MaxBatchSize.ShouldBe(2048);

        //Act and Assert: shrink
        for (var i = 0; i < 15; ++i)
        {
            uut.UpdateMaxBatchSize(2);
            uut.MaxBatchSize.ShouldBe(i < shrinkCoolingOffPeriod ? 2048 : 1536);
        }

        //Act and Assert: big message sudden appearance
        uut.UpdateMaxBatchSize(2048);
        for (var i = 0; i < 15; ++i)
        {
            uut.UpdateMaxBatchSize(i == shrinkCoolingOffPeriod - 2 ? 2000 : 2);
            uut.MaxBatchSize.ShouldBe(2048);
        }

        //Act and Assert: big and small message alternating occurrence
        for (var i = 0; i < shrinkCoolingOffPeriod * 3; ++i)
        {
            uut.UpdateMaxBatchSize(i % 2 == 0 ? 2 : 2000);
            uut.MaxBatchSize.ShouldBe(2048);
        }

        //Act and Assert: consecutive big message
        for (var i = 0; i < 15; ++i)
        {
            uut.UpdateMaxBatchSize(2000);
            uut.MaxBatchSize.ShouldBe(2048);
        }

        //Act and Assert: expand after shrink
        uut.UpdateMaxBatchSize(4096);
        uut.MaxBatchSize.ShouldBe(4096);
    }

    // Upstream: BatchMessageContainerImplTest#testMessagesSize
    [Fact]
    public void MaxMessagesNum_GivenBatchesOf10Then200Then10_ShouldBe32Then200Then200()
    {
        //Arrange
        var uut = CreateContainer();

        //Assert
        uut.MaxMessagesNum.ShouldBe(32);

        //Act and Assert
        AddMessagesAndCreateBatches(uut, 10);
        uut.MaxMessagesNum.ShouldBe(32);

        AddMessagesAndCreateBatches(uut, 200);
        uut.MaxMessagesNum.ShouldBe(200);

        AddMessagesAndCreateBatches(uut, 10);
        uut.MaxMessagesNum.ShouldBe(200);
    }

    // Upstream: BatchMessageContainerImplTest#testPlainMessageIsNotBatchedWithTransactionalMessages
    [Fact]
    public void HasSameTxn_GivenTransactionalBatchAndPlainMessage_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, transaction: (7, 13)), MaxMessageSize);

        //Act
        var actual = uut.HasSameTxn(TestBatchItem.Create(2));

        //Assert
        actual.ShouldBeFalse();
    }

    // Upstream: BatchMessageContainerImplTest#testTransactionalMessageIsNotBatchedWithPlainMessages
    [Fact]
    public void HasSameTxn_GivenPlainBatchAndTransactionalMessage_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.HasSameTxn(TestBatchItem.Create(2, transaction: (7, 13)));

        //Assert
        actual.ShouldBeFalse();
    }

    // Upstream: BatchMessageContainerImplTest#testSameTransactionStillBatchesTogether
    [Fact]
    public void HasSameTxn_GivenSameTransaction_ShouldReturnTrueAndGivenOtherTransaction_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, transaction: (7, 13)), MaxMessageSize);

        //Act
        var sameTransaction = uut.HasSameTxn(TestBatchItem.Create(2, transaction: (7, 13)));
        var otherTransaction = uut.HasSameTxn(TestBatchItem.Create(3, transaction: (7, 14)));

        //Assert
        sameTransaction.ShouldBeTrue();
        otherTransaction.ShouldBeFalse();
    }

    // Upstream: BatchMessageContainerImplTest#testPlainMessagesStillBatchTogether
    [Fact]
    public void HasSameTxn_GivenTwoPlainMessages_ShouldReturnTrue()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.HasSameTxn(TestBatchItem.Create(2));

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void HasSameTxn_GivenEmptyContainer_ShouldReturnTrueForAnyMessage()
    {
        //Arrange
        var uut = CreateContainer();

        //Act
        var plain = uut.HasSameTxn(TestBatchItem.Create(1));
        var transactional = uut.HasSameTxn(TestBatchItem.Create(2, transaction: (7, 13)));

        //Assert
        plain.ShouldBeTrue();
        transactional.ShouldBeTrue();
    }

    [Fact]
    public void HasSameTxn_GivenTransactionWithMostBitsZero_ShouldNotTreatItAsPlain()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1, transaction: (0, 0)), MaxMessageSize);

        //Act
        var plain = uut.HasSameTxn(TestBatchItem.Create(2));
        var sameTransaction = uut.HasSameTxn(TestBatchItem.Create(3, transaction: (0, 0)));
        var otherTransaction = uut.HasSameTxn(TestBatchItem.Create(4, transaction: (0, 1)));

        //Assert
        plain.ShouldBeFalse();
        sameTransaction.ShouldBeTrue();
        otherTransaction.ShouldBeFalse();
    }

    [Fact]
    public void HasSameTxn_GivenOnlyOneOfTheTransactionBits_ShouldTreatTheMessageAsPlain()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        var item = TestBatchItem.Create(2);
        item.Metadata.TxnidMostBits = 7;

        //Act
        var actual = uut.HasSameTxn(item);

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void HasSameTxn_GivenCallsToTheQuery_ShouldNotChangeTheContainer()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        _ = uut.HasSameTxn(TestBatchItem.Create(2, transaction: (7, 13)));

        //Act
        var actual = uut.HasSameTxn(TestBatchItem.Create(3));

        //Assert
        actual.ShouldBeTrue();
        uut.NumMessagesInBatch.ShouldBe(1);
    }

    [Fact]
    public void CanAddToCurrentBatch_GivenMessageThatFits_ShouldReturnTrue()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.CanAddToCurrentBatch(TestBatchItem.Create(2), MaxMessageSize);

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void CanAddToCurrentBatch_GivenMessageThatDoesNotFit_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 1);
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.CanAddToCurrentBatch(TestBatchItem.Create(2), MaxMessageSize);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void CanAddToCurrentBatch_GivenMessageWithOtherTransaction_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.CanAddToCurrentBatch(TestBatchItem.Create(2, transaction: (7, 13)), MaxMessageSize);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void CanAddToCurrentBatch_GivenMessageWithOtherSchemaVersion_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        var item = TestBatchItem.Create(2);
        item.Metadata.SchemaVersion = ByteString.CopyFrom(1);

        //Act
        var actual = uut.CanAddToCurrentBatch(item, MaxMessageSize);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void HaveEnoughSpace_GivenMaxBytesZero_ShouldUseMaxMessageSize()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 0);
        uut.Add(TestBatchItem.CreateWithSize(1, 60), 100);

        //Act
        var fits = uut.HaveEnoughSpace(TestBatchItem.CreateWithSize(2, 40), 100);
        var doesNotFit = uut.HaveEnoughSpace(TestBatchItem.CreateWithSize(3, 41), 100);

        //Assert
        fits.ShouldBeTrue();
        doesNotFit.ShouldBeFalse();
    }

    [Fact]
    public void HaveEnoughSpace_GivenMaxBytes_ShouldUseMaxBytesAndNotMaxMessageSize()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 50);
        uut.Add(TestBatchItem.CreateWithSize(1, 30), 1000);

        //Act
        var fits = uut.HaveEnoughSpace(TestBatchItem.CreateWithSize(2, 20), 1000);
        var doesNotFit = uut.HaveEnoughSpace(TestBatchItem.CreateWithSize(3, 21), 1000);

        //Assert
        fits.ShouldBeTrue();
        doesNotFit.ShouldBeFalse();
    }

    [Fact]
    public void HaveEnoughSpace_GivenMaxMessagesReached_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 2);
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var beforeLimit = uut.HaveEnoughSpace(TestBatchItem.Create(2), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2), MaxMessageSize);
        var atLimit = uut.HaveEnoughSpace(TestBatchItem.Create(3), MaxMessageSize);

        //Assert
        beforeLimit.ShouldBeTrue();
        atLimit.ShouldBeFalse();
    }

    [Fact]
    public void HaveEnoughSpace_GivenMaxMessagesZero_ShouldNotLimitTheNumberOfMessages()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 0);
        for (ulong i = 0; i < 2000; ++i)
        {
            uut.Add(TestBatchItem.Create(i, "x"), MaxMessageSize);
        }

        //Act
        var actual = uut.HaveEnoughSpace(TestBatchItem.Create(2000, "x"), MaxMessageSize);

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void HaveEnoughSpace_GivenMessageLargerThanMaxBytesAndEmptyContainer_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 10);

        //Act
        var actual = uut.HaveEnoughSpace(TestBatchItem.CreateWithSize(1, 11), MaxMessageSize);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Add_GivenMessagesUpToMaxBytes_ShouldReportFull()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 100);

        //Act
        var first = uut.Add(TestBatchItem.CreateWithSize(1, 40), MaxMessageSize);
        var second = uut.Add(TestBatchItem.CreateWithSize(2, 40), MaxMessageSize);
        var third = uut.Add(TestBatchItem.CreateWithSize(3, 20), MaxMessageSize);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeFalse();
        third.ShouldBeTrue();
        uut.CurrentBatchSize.ShouldBe(100);
        uut.NumMessagesInBatch.ShouldBe(3);
    }

    [Fact]
    public void Add_GivenMaxMessagesReached_ShouldReportFull()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 3);

        //Act
        var first = uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        var second = uut.Add(TestBatchItem.Create(2), MaxMessageSize);
        var third = uut.Add(TestBatchItem.Create(3), MaxMessageSize);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeFalse();
        third.ShouldBeTrue();
    }

    [Fact]
    public void Add_GivenMaxBytesZeroAndMaxMessageSizeReached_ShouldReportFull()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 0);

        //Act
        var first = uut.Add(TestBatchItem.CreateWithSize(1, 50), 100);
        var second = uut.Add(TestBatchItem.CreateWithSize(2, 50), 100);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeTrue();
    }

    [Fact]
    public void Add_GivenMessageLargerThanMaxBytes_ShouldReportFullAndFormABatchOfOne()
    {
        //Arrange
        var uut = CreateContainer(maxBytes: 10);
        var groups = new List<PreparedBatchGroup>();

        //Act
        var actual = uut.Add(TestBatchItem.CreateWithSize(1, 11), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        actual.ShouldBeTrue();
        groups.Count.ShouldBe(1);
        groups[0].Count.ShouldBe(1);
        groups[0].Batch.ShouldBeNull();
    }

    [Fact]
    public void Add_GivenSequenceIdsThatAreNotIncreasing_ShouldSetLowestToTheFirstAndHighestToTheLastAdded()
    {
        //Arrange
        var uut = CreateContainer();

        //Act
        uut.Add(TestBatchItem.Create(5), MaxMessageSize);
        uut.Add(TestBatchItem.Create(9), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3), MaxMessageSize);

        //Assert
        uut.LowestSequenceId.ShouldBe(5ul);
        uut.HighestSequenceId.ShouldBe(3ul);
    }

    [Fact]
    public void IsEmpty_GivenNewContainer_ShouldBeTrueAndAfterAddFalseAndAfterClearTrue()
    {
        //Arrange
        var uut = CreateContainer();
        var isEmptyAtStart = uut.IsEmpty;

        //Act
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        var isEmptyAfterAdd = uut.IsEmpty;
        uut.Clear();

        //Assert
        isEmptyAtStart.ShouldBeTrue();
        isEmptyAfterAdd.ShouldBeFalse();
        uut.IsEmpty.ShouldBeTrue();
        uut.NumMessagesInBatch.ShouldBe(0);
        uut.CurrentBatchSize.ShouldBe(0);
        uut.Items.Count.ShouldBe(0);
    }

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

    [Fact]
    public void GetRemainingPublishDelay_GivenMessagesAdded_ShouldCountDownFromTheFirstMessage()
    {
        //Arrange
        var clock = new FakeClock();
        var uut = CreateContainer(maxPublishDelay: TimeSpan.FromMilliseconds(5), clock: clock.Now);
        clock.Advance(TimeSpan.FromMilliseconds(10));
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act and Assert
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.FromMilliseconds(5));

        clock.Advance(TimeSpan.FromMilliseconds(2));
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.FromMilliseconds(3));

        // Adding a message does not restart the delay
        uut.Add(TestBatchItem.Create(2), MaxMessageSize);
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.FromMilliseconds(3));

        clock.Advance(TimeSpan.FromMilliseconds(3));
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.Zero);

        clock.Advance(TimeSpan.FromMilliseconds(100));
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.Zero);
    }

    [Fact]
    public void ShouldFlush_GivenEmptyContainer_ShouldReturnFalse()
    {
        //Arrange
        var clock = new FakeClock();
        var uut = CreateContainer(maxPublishDelay: TimeSpan.FromMilliseconds(1), clock: clock.Now);
        clock.Advance(TimeSpan.FromHours(1));

        //Act
        var actual = uut.ShouldFlush(MaxMessageSize);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void ShouldFlush_GivenDelayElapsed_ShouldReturnTrue()
    {
        //Arrange
        var clock = new FakeClock();
        var uut = CreateContainer(maxPublishDelay: TimeSpan.FromMilliseconds(5), clock: clock.Now);
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var beforeDelay = uut.ShouldFlush(MaxMessageSize);
        clock.Advance(TimeSpan.FromMilliseconds(5));
        var afterDelay = uut.ShouldFlush(MaxMessageSize);

        //Assert
        beforeDelay.ShouldBeFalse();
        afterDelay.ShouldBeTrue();
    }

    [Fact]
    public void ShouldFlush_GivenFullContainerBeforeDelay_ShouldReturnTrue()
    {
        //Arrange
        var clock = new FakeClock();
        var uut = CreateContainer(maxMessages: 2, maxPublishDelay: TimeSpan.FromHours(1), clock: clock.Now);
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        var afterFirst = uut.ShouldFlush(MaxMessageSize);

        //Act
        uut.Add(TestBatchItem.Create(2), MaxMessageSize);
        var afterSecond = uut.ShouldFlush(MaxMessageSize);

        //Assert
        afterFirst.ShouldBeFalse();
        afterSecond.ShouldBeTrue();
    }

    [Fact]
    public void Clear_GivenBatchThatWasFlushed_ShouldRestartTheDelayWithTheNextFirstMessage()
    {
        //Arrange
        var clock = new FakeClock();
        var uut = CreateContainer(maxPublishDelay: TimeSpan.FromMilliseconds(5), clock: clock.Now);
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        clock.Advance(TimeSpan.FromMilliseconds(5));
        uut.Clear();
        clock.Advance(TimeSpan.FromMilliseconds(20));

        //Act
        uut.Add(TestBatchItem.Create(2), MaxMessageSize);

        //Assert
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.FromMilliseconds(5));
    }

    [Fact]
    public void HasSameSchema_GivenEmptyContainer_ShouldReturnTrue()
    {
        //Arrange
        var uut = CreateContainer();
        var item = TestBatchItem.Create(1);
        item.Metadata.SchemaVersion = ByteString.CopyFrom(1);

        //Act
        var actual = uut.HasSameSchema(item);

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void HasSameSchema_GivenExplicitAndImplicitSchemaVersion_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateContainer();
        var withVersion = TestBatchItem.Create(1);
        withVersion.Metadata.SchemaVersion = ByteString.CopyFrom(1);
        var withoutVersion = TestBatchItem.Create(2);

        //Act
        uut.Add(withVersion, MaxMessageSize);
        var implicitInExplicit = uut.HasSameSchema(withoutVersion);
        uut.Clear();
        uut.Add(withoutVersion, MaxMessageSize);
        var explicitInImplicit = uut.HasSameSchema(withVersion);

        //Assert
        implicitInExplicit.ShouldBeFalse();
        explicitInImplicit.ShouldBeFalse();
    }

    [Fact]
    public void HasSameSchema_GivenNoSchemaVersion_ShouldReturnTrue()
    {
        //Arrange
        var uut = CreateContainer();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);

        //Act
        var actual = uut.HasSameSchema(TestBatchItem.Create(2));

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void HasSameSchema_GivenSameAndOtherExplicitSchemaVersion_ShouldReturnTrueAndFalse()
    {
        //Arrange
        var uut = CreateContainer();
        var first = TestBatchItem.Create(1);
        first.Metadata.SchemaVersion = ByteString.CopyFrom(1, 2);
        uut.Add(first, MaxMessageSize);
        var same = TestBatchItem.Create(2);
        same.Metadata.SchemaVersion = ByteString.CopyFrom(1, 2);
        var other = TestBatchItem.Create(3);
        other.Metadata.SchemaVersion = ByteString.CopyFrom(1, 3);

        //Act
        var sameVersion = uut.HasSameSchema(same);
        var otherVersion = uut.HasSameSchema(other);

        //Assert
        sameVersion.ShouldBeTrue();
        otherVersion.ShouldBeFalse();
    }

    [Fact]
    public void HasSameSchema_GivenSchemaIdAndSchemaVersion_ShouldCompareBoth()
    {
        //Arrange
        var uut = CreateContainer();
        var first = TestBatchItem.Create(1);
        first.Metadata.SchemaId = ByteString.CopyFrom(5);
        first.Metadata.SchemaVersion = ByteString.CopyFrom(1);
        uut.Add(first, MaxMessageSize);
        var same = TestBatchItem.Create(2);
        same.Metadata.SchemaId = ByteString.CopyFrom(5);
        same.Metadata.SchemaVersion = ByteString.CopyFrom(1);
        var otherId = TestBatchItem.Create(3);
        otherId.Metadata.SchemaId = ByteString.CopyFrom(6);
        otherId.Metadata.SchemaVersion = ByteString.CopyFrom(1);
        var otherVersion = TestBatchItem.Create(4);
        otherVersion.Metadata.SchemaId = ByteString.CopyFrom(5);
        otherVersion.Metadata.SchemaVersion = ByteString.CopyFrom(2);

        //Act
        var sameResult = uut.HasSameSchema(same);
        var otherIdResult = uut.HasSameSchema(otherId);
        var otherVersionResult = uut.HasSameSchema(otherVersion);

        //Assert
        sameResult.ShouldBeTrue();
        otherIdResult.ShouldBeFalse();
        otherVersionResult.ShouldBeFalse();
    }

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

    [Fact]
    public void CreateBatches_GivenOneMessage_ShouldReturnSingleGroupWithoutPreparedBatch()
    {
        //Arrange
        var uut = CreateContainer();
        var item = TestBatchItem.Create(42, key: "key", transaction: (7, 13));
        var metadataBefore = item.Metadata.Clone();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(item, MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(1);
        groups[0].Start.ShouldBe(0);
        groups[0].Count.ShouldBe(1);
        groups[0].Batch.ShouldBeNull();
        groups[0].SequenceId.ShouldBe(42ul);
        groups[0].HighestSequenceId.ShouldBe(42ul);
        uut.Items.Count.ShouldBe(1);
        uut.Items[0].ShouldBeSameAs(item);
        item.Metadata.ShouldBe(metadataBefore);
        item.Metadata.HasNumMessagesInBatch.ShouldBeFalse();
    }

    [Fact]
    public void CreateBatches_GivenOneMessage_ShouldUpdateTheMaxBatchSizeWithThePayloadSize()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.CreateWithSize(1, 4000), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        uut.MaxBatchSize.ShouldBe(4000);
    }

    [Fact]
    public void CreateBatches_GivenThreeMessages_ShouldWriteSingleMessageMetadataFraming()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        var items = JavaKnownAnswer.CreateItems();
        foreach (var item in items)
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(1);
        var batch = groups[0].Batch;
        batch.ShouldNotBeNull();
        var messages = BatchPayloadReader.Read(batch);
        messages.Count.ShouldBe(3);

        messages[0].Payload.ShouldBe(Encoding.UTF8.GetBytes("payload-0"));
        messages[0].Metadata.PayloadSize.ShouldBe(9);
        messages[0].Metadata.SequenceId.ShouldBe(100ul);
        messages[0].Metadata.PartitionKey.ShouldBe("key-a");
        messages[0].Metadata.HasPartitionKeyB64Encoded.ShouldBeTrue();
        messages[0].Metadata.PartitionKeyB64Encoded.ShouldBeFalse();
        messages[0].Metadata.EventTime.ShouldBe(1700000000000ul);
        messages[0].Metadata.Properties.Select(p => (p.Key, p.Value)).ShouldBeSequence(("k1", "v1"), ("k2", "v2"));
        messages[0].Metadata.HasOrderingKey.ShouldBeFalse();
        messages[0].Metadata.HasNullValue.ShouldBeFalse();

        messages[1].Payload.ShouldBeEmpty();
        messages[1].Metadata.PayloadSize.ShouldBe(0);
        messages[1].Metadata.SequenceId.ShouldBe(101ul);
        messages[1].Metadata.OrderingKey.ToByteArray().ShouldBe([1, 2, 3]);
        messages[1].Metadata.HasPartitionKey.ShouldBeFalse();
        messages[1].Metadata.HasPartitionKeyB64Encoded.ShouldBeFalse();
        messages[1].Metadata.NullValue.ShouldBeTrue();
        messages[1].Metadata.HasEventTime.ShouldBeFalse();

        messages[2].Payload.ShouldBe([0x00, 0xff, 0x7f]);
        messages[2].Metadata.SequenceId.ShouldBe(102ul);
        messages[2].Metadata.PartitionKey.ShouldBe("a2V5LWI=");
        messages[2].Metadata.PartitionKeyB64Encoded.ShouldBeTrue();
        messages[2].Metadata.NullPartitionKey.ShouldBeTrue();
        messages[2].Metadata.HasNullValue.ShouldBeFalse();
    }

    [Fact]
    public void CreateBatches_GivenThreeMessages_ShouldBeReadableByTheBatchHandlerOfTheConsumer()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        var items = JavaKnownAnswer.CreateItems();
        foreach (var item in items)
        {
            uut.Add(item, MaxMessageSize);
        }

        uut.CreateBatches(groups, MaxMessageSize);
        var batch = groups[0].Batch!;
        var batchHandler = new BatchHandler<byte[]>(false, new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray), BatchTestData.Topic);

        //Act
        var first = batchHandler.Add(BatchTestData.CreateEntryId(1, 2), null, 0, batch.Metadata, batch.Payload, out _);
        var second = batchHandler.GetNext();
        var third = batchHandler.GetNext();

        //Assert
        batchHandler.GetNext().ShouldBeNull();
        first.ShouldNotBeNull();
        first!.MessageId.BatchIndex.ShouldBe(0);
        Encoding.UTF8.GetString(first.Data.ToArray()).ShouldBe("payload-0");
        first.Key.ShouldBe("key-a");
        first.HasBase64EncodedKey.ShouldBeFalse();
        first.SequenceId.ShouldBe(100ul);
        first.EventTime.ShouldBe(1700000000000ul);
        first.Properties["k1"].ShouldBe("v1");
        first.Properties["k2"].ShouldBe("v2");

        second.ShouldNotBeNull();
        second!.MessageId.BatchIndex.ShouldBe(1);
        second.Data.Length.ShouldBe(0);
        second.OrderingKey.ShouldBe([1, 2, 3]);
        second.SequenceId.ShouldBe(101ul);

        third.ShouldNotBeNull();
        third!.MessageId.BatchIndex.ShouldBe(2);
        third.Data.ToArray().ShouldBe([0x00, 0xff, 0x7f]);
        third.HasBase64EncodedKey.ShouldBeTrue();
        third.Key.ShouldBe("a2V5LWI=");
        third.SequenceId.ShouldBe(102ul);
    }

    [Fact]
    public void CreateBatches_GivenThreeMessages_ShouldSetBatchMetadata()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        var items = JavaKnownAnswer.CreateItems((7, 13));
        items[0].Metadata.OrderingKey = ByteString.CopyFrom(4, 5);
        foreach (var item in items)
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var batch = groups[0].Batch!;
        var metadata = batch.Metadata;
        groups[0].Count.ShouldBe(3);
        groups[0].SequenceId.ShouldBe(100ul);
        groups[0].HighestSequenceId.ShouldBe(102ul);
        batch.Count.ShouldBe(3);
        batch.SequenceId.ShouldBe(100ul);
        batch.HighestSequenceId.ShouldBe(102ul);
        metadata.NumMessagesInBatch.ShouldBe(3);
        metadata.SequenceId.ShouldBe(100ul);
        metadata.HighestSequenceId.ShouldBe(102ul);
        metadata.PartitionKey.ShouldBe("key-a");
        metadata.HasPartitionKeyB64Encoded.ShouldBeTrue();
        metadata.PartitionKeyB64Encoded.ShouldBeFalse();
        metadata.OrderingKey.ToByteArray().ShouldBe([4, 5]);
        metadata.ReplicatedFrom.ShouldBe("cluster");
        metadata.ReplicateTo.ShouldBeSequence("us-east", "us-west");
        metadata.SchemaVersion.ToByteArray().ShouldBe([9, 8]);
        metadata.TxnidMostBits.ShouldBe(7ul);
        metadata.TxnidLeastBits.ShouldBe(13ul);
        metadata.HasDeliverAtTime.ShouldBeFalse();
        metadata.HasCompression.ShouldBeFalse();
        metadata.HasUncompressedSize.ShouldBeFalse();
        metadata.Properties.Count.ShouldBe(0);
        metadata.HasEventTime.ShouldBeFalse();
    }

    [Fact]
    public void CreateBatches_GivenPlainMessages_ShouldNotSetTransactionOnBatchMetadata()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var metadata = groups[0].Batch!.Metadata;
        metadata.HasTxnidMostBits.ShouldBeFalse();
        metadata.HasTxnidLeastBits.ShouldBeFalse();
        metadata.HasPartitionKey.ShouldBeFalse();
        metadata.HasOrderingKey.ShouldBeFalse();
        metadata.HasSchemaVersion.ShouldBeFalse();
        metadata.HasReplicatedFrom.ShouldBeFalse();
    }

    [Fact]
    public void CreateBatches_GivenTransactionWithMostBitsZero_ShouldCopyBothBitsToBatchMetadata()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, transaction: (0, 5)), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, transaction: (0, 5)), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var metadata = groups[0].Batch!.Metadata;
        metadata.HasTxnidMostBits.ShouldBeTrue();
        metadata.TxnidMostBits.ShouldBe(0ul);
        metadata.HasTxnidLeastBits.ShouldBeTrue();
        metadata.TxnidLeastBits.ShouldBe(5ul);
    }

    [Fact]
    public void CreateBatches_GivenSequenceIdsThatAreNotIncreasing_ShouldUseFirstAsSequenceIdAndLastAddedAsHighestSequenceId()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(5), MaxMessageSize);
        uut.Add(TestBatchItem.Create(9), MaxMessageSize);
        uut.Add(TestBatchItem.Create(3), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var batch = groups[0].Batch!;
        batch.SequenceId.ShouldBe(5ul);
        batch.HighestSequenceId.ShouldBe(3ul);
        batch.Metadata.SequenceId.ShouldBe(5ul);
        batch.Metadata.HighestSequenceId.ShouldBe(3ul);
    }

    [Fact]
    public void CreateBatches_GivenTransactionalBatchThenPlainBatch_ShouldNotCarryTransactionToThePlainBatch()
    {
        //Arrange
        var pool = new PreparedBatchPool(4);
        var uut = CreateContainer(pool: pool);
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.Create(1, key: "key", transaction: (7, 13)), MaxMessageSize);
        uut.Add(TestBatchItem.Create(2, key: "key", transaction: (7, 13)), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);
        var transactionalBatch = groups[0].Batch!;
        uut.Clear();
        groups.Clear();
        transactionalBatch.Return();

        //Act
        uut.Add(TestBatchItem.Create(3), MaxMessageSize);
        uut.Add(TestBatchItem.Create(4), MaxMessageSize);
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var plainBatch = groups[0].Batch!;
        plainBatch.ShouldBeSameAs(transactionalBatch);
        plainBatch.Metadata.HasTxnidMostBits.ShouldBeFalse();
        plainBatch.Metadata.HasTxnidLeastBits.ShouldBeFalse();
        plainBatch.Metadata.HasPartitionKey.ShouldBeFalse();
        plainBatch.Metadata.SequenceId.ShouldBe(3ul);
        plainBatch.Metadata.HighestSequenceId.ShouldBe(4ul);
        BatchPayloadReader.Read(plainBatch).Select(m => m.Metadata.SequenceId).ShouldBeSequence(3ul, 4ul);
    }

    [Fact]
    public void CreateBatches_GivenMoreThan1000Messages_ShouldWriteAllMessagesInOrder()
    {
        //Arrange
        const int numberOfMessages = 5000;
        var uut = CreateContainer(maxMessages: 0, maxBytes: 0);
        var groups = new List<PreparedBatchGroup>();
        for (var i = 0; i < numberOfMessages; ++i)
        {
            uut.Add(TestBatchItem.Create((ulong) i, $"payload-{i}", key: $"key-{i % 7}"), MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        groups.Count.ShouldBe(1);
        groups[0].Count.ShouldBe(numberOfMessages);
        var batch = groups[0].Batch!;
        batch.Metadata.NumMessagesInBatch.ShouldBe(numberOfMessages);
        batch.Metadata.SequenceId.ShouldBe(0ul);
        batch.Metadata.HighestSequenceId.ShouldBe(numberOfMessages - 1ul);
        var messages = BatchPayloadReader.Read(batch);
        messages.Count.ShouldBe(numberOfMessages);
        for (var i = 0; i < numberOfMessages; ++i)
        {
            messages[i].Metadata.SequenceId.ShouldBe((ulong) i);
            messages[i].Metadata.PartitionKey.ShouldBe($"key-{i % 7}");
            Encoding.UTF8.GetString(messages[i].Payload).ShouldBe($"payload-{i}");
        }
    }

    [Fact]
    public void Add_GivenMaxMessagesOf1200_ShouldReportFullAtMessage1200()
    {
        //Arrange
        var uut = CreateContainer(maxMessages: 1200);
        var full = false;
        var added = 0;

        //Act
        while (!full)
        {
            full = uut.Add(TestBatchItem.Create((ulong) added, "x"), MaxMessageSize);
            ++added;
        }

        //Assert
        added.ShouldBe(1200);
        uut.HaveEnoughSpace(TestBatchItem.Create(1200, "x"), MaxMessageSize).ShouldBeFalse();
    }

    [Fact]
    public void CreateBatches_GivenPayloadsInSeveralSegments_ShouldCopyAllSegments()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.CreateSegmented(1, [1, 2, 3, 4, 5]), MaxMessageSize);
        uut.Add(TestBatchItem.CreateSegmented(2, [6, 7]), MaxMessageSize);
        uut.Add(TestBatchItem.CreateSegmented(3, [8]), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var messages = BatchPayloadReader.Read(groups[0].Batch!);
        messages[0].Payload.ShouldBe([1, 2, 3, 4, 5]);
        messages[1].Payload.ShouldBe([6, 7]);
        messages[2].Payload.ShouldBe([8]);
    }

    [Fact]
    public void CreateBatches_GivenMessagesWithProperties_ShouldNotChangeThePropertiesOfTheMessages()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        var items = JavaKnownAnswer.CreateItems();
        var propertiesBefore = items[0].Metadata.Properties.Clone();
        foreach (var item in items)
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        items[0].Metadata.Properties.ShouldBe(propertiesBefore);
        items[0].Metadata.Properties.Count.ShouldBe(2);
    }

    [Fact]
    public void CurrentBatchSize_GivenThreeMessages_ShouldBeTheSumOfThePayloadSizesWithoutFraming()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        foreach (var item in JavaKnownAnswer.CreateItems())
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        uut.CurrentBatchSize.ShouldBe(12);
        groups[0].Batch!.Payload.Length.ShouldBe(JavaKnownAnswer.PayloadSize);
    }

    [Fact]
    public void CreateBatches_GivenBatchLargerThanTheMaxBatchSize_ShouldGrowTheMaxBatchSize()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        uut.Add(TestBatchItem.CreateWithSize(1, 3000), MaxMessageSize);
        uut.Add(TestBatchItem.CreateWithSize(2, 3000), MaxMessageSize);

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        uut.MaxBatchSize.ShouldBe((int) groups[0].Batch!.Payload.Length);
        uut.MaxBatchSize.ShouldBeGreaterThan(6000);
    }

    [Fact]
    public void Clear_GivenBatchWasCreated_ShouldKeepTheBufferSizingAndResetTheRest()
    {
        //Arrange
        var uut = CreateContainer();
        var groups = new List<PreparedBatchGroup>();
        for (var i = 0; i < 100; ++i)
        {
            uut.Add(TestBatchItem.CreateWithSize((ulong) i + 10, 100), MaxMessageSize);
        }

        uut.CreateBatches(groups, MaxMessageSize);
        var maxBatchSize = uut.MaxBatchSize;

        //Act
        uut.Clear();

        //Assert
        uut.MaxBatchSize.ShouldBe(maxBatchSize);
        uut.MaxMessagesNum.ShouldBe(100);
        uut.IsEmpty.ShouldBeTrue();
        uut.LowestSequenceId.ShouldBe(0ul);
        uut.HighestSequenceId.ShouldBe(0ul);
        uut.Items.Count.ShouldBe(0);
        uut.HasSameTxn(TestBatchItem.Create(1, transaction: (1, 1))).ShouldBeTrue();
    }

    private static BatchMessageContainer<TestBatchItem> CreateContainer(
        uint maxMessages = 1000,
        uint maxBytes = 128 * 1024,
        TimeSpan? maxPublishDelay = null,
        Func<TimeSpan>? clock = null,
        PreparedBatchPool? pool = null)
        => new(maxMessages, maxBytes, maxPublishDelay ?? TimeSpan.FromMilliseconds(1), pool, clock);

    private static void AddMessagesAndCreateBatches(BatchMessageContainer<TestBatchItem> container, int numberOfMessages)
    {
        var groups = new List<PreparedBatchGroup>();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            container.Add(TestBatchItem.Create((ulong) i, "payload"), MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);
        container.Clear();

        foreach (var group in groups)
        {
            group.Batch?.Return();
        }
    }
}
