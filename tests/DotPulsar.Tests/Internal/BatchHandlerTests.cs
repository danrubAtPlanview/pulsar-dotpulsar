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
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf.Collections;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class BatchHandlerTests
{
    [Fact]
    public void Add_GivenBrokerAckSet_ShouldSkipAcknowledgedIndexes()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var messages = BatchTestData.AddBatch(uut, 1, 2, 5, out var chargedButSkipped, BatchTestData.CreateAckSet(0b10110));

        //Assert
        messages.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(1, 2, 4);
        messages.Select(message => message.Data.ToArray()[0]).ShouldBeSequence((byte) 1, (byte) 2, (byte) 4);
        chargedButSkipped.ShouldBe(0);
    }

    [Fact]
    public void Add_GivenAllIndexesAcked_ShouldReturnNull()
    {
        //Arrange
        var uut = CreateHandler();
        var data = new ReadOnlySequence<byte>(BatchTestData.CreateBatchPayload(3));

        //Act
        var actual = uut.Add(BatchTestData.CreateEntryId(1, 2), BatchTestData.CreateAckSet(0), 0, BatchTestData.CreateMetadata(3), data, out var chargedButSkipped);

        //Assert
        actual.ShouldBeNull();
        chargedButSkipped.ShouldBe(0);
        uut.GetNext().ShouldBeNull();
        uut.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
    }

    [Fact]
    public void Add_GivenCompactedOutMessage_ShouldSkipItAndReportChargedButSkipped()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var messages = BatchTestData.AddBatch(uut, 1, 2, 4, out var chargedButSkipped, null, 1, 2);

        //Assert
        messages.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(0, 3);
        chargedButSkipped.ShouldBe(2);
    }

    [Fact]
    public void Add_GivenCompactedOutMessageThatIsAlsoAcknowledgedByBroker_ShouldNotCountItAsChargedButSkipped()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var messages = BatchTestData.AddBatch(uut, 1, 2, 4, out var chargedButSkipped, BatchTestData.CreateAckSet(0b1101), 1);

        //Assert
        messages.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(0, 2, 3);
        chargedButSkipped.ShouldBe(0);
    }

    [Fact]
    public void Add_GivenCompactedOutMessage_ShouldAllowTheBatchToBeCompletedByTheDeliveredMessages()
    {
        //Arrange
        var uut = CreateHandler();
        var messages = BatchTestData.AddBatch(uut, 1, 2, 3, null, 1);

        //Act
        uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, messages[0].MessageId.BatchIndex), out var isCompleteAfterFirst);
        uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, messages[1].MessageId.BatchIndex), out var isCompleteAfterSecond);

        //Assert
        isCompleteAfterFirst.ShouldBeFalse();
        isCompleteAfterSecond.ShouldBeTrue();
    }

    [Fact]
    public void Add_GivenAllMessagesCompactedOut_ShouldReturnNullAndNotTrackTheBatch()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var messages = BatchTestData.AddBatch(uut, 1, 2, 2, out var chargedButSkipped, null, 0, 1);

        //Assert
        messages.ShouldBeEmpty();
        chargedButSkipped.ShouldBe(2);
        uut.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
    }

    [Fact]
    public void Add_ShouldSetTopicAndBatchSizeOnMessageIds()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var messages = BatchTestData.AddBatch(uut, 1, 2, 3);

        //Assert
        messages.ShouldAllBe(message => message.MessageId.Topic == BatchTestData.Topic);
        messages.ShouldAllBe(message => message.MessageId.GetBatchSize() == 3);
        messages.Select(message => message.MessageId.BatchIndex).ShouldBeSequence(0, 1, 2);
    }

    [Fact]
    public void Add_GivenBatchOfMoreThan64Messages_ShouldTrackAllIndexes()
    {
        //Arrange
        var uut = CreateHandler();
        var messages = BatchTestData.AddBatch(uut, 1, 2, 130);

        //Act
        var results = messages.Select(message => uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, message.MessageId.BatchIndex), out var isComplete) is not null && isComplete).ToList();

        //Assert
        messages.Count.ShouldBe(130);
        results.Take(129).ShouldAllBe(result => !result);
        results[129].ShouldBeTrue();
    }

    [Fact]
    public void Add_GivenTrackBatchesIsFalse_ShouldNotTrackTheBatch()
    {
        //Arrange
        var uut = new BatchHandler<byte[]>(false, new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray), BatchTestData.Topic);

        //Act
        var messages = BatchTestData.AddBatch(uut, 1, 2, 3);

        //Assert
        messages.Count.ShouldBe(3);
        uut.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
        uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeNull();
    }

    [Fact]
    public void Add_GivenRedeliveryOfTrackedEntry_ShouldMergeTrackers()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 2, 5);
        uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 0), out _);
        uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 1), out _);

        //Act
        _ = BatchTestData.AddBatch(uut, 1, 2, 5, BatchTestData.CreateAckSet(0b01101));
        var tracker = uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 3), out _);

        //Assert
        tracker.ShouldNotBeNull();
        var ackSet = new RepeatedField<long>();
        uut.CopyAckSetTo(tracker, ackSet);
        ackSet.ShouldBeAckSet(0b00100);
    }

    [Fact]
    public void Add_GivenRedeliveryOfTrackedEntry_ShouldNotCreateASecondTracker()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 2, 2);
        _ = BatchTestData.AddBatch(uut, 1, 2, 2);

        //Act
        var first = uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 0), out var isCompleteAfterFirst);
        var second = uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 1), out var isCompleteAfterSecond);
        var third = uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 0), out _);

        //Assert
        first.ShouldNotBeNull();
        second.ShouldBeSameAs(first);
        isCompleteAfterFirst.ShouldBeFalse();
        isCompleteAfterSecond.ShouldBeTrue();
        third.ShouldBeNull();
    }

    [Fact]
    public void Add_GivenRedeliveryOfTrackedEntryWithAllIndexesAcknowledged_ShouldRemoveTheTracker()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 2, 3);

        //Act
        _ = BatchTestData.AddBatch(uut, 1, 2, 3, BatchTestData.CreateAckSet(0));

        //Assert
        uut.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
    }

    [Fact]
    public void TryGetBatchSize_GivenTrackedEntry_ShouldReturnBatchSize()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 2, 7);

        //Act
        var actual = uut.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 3), out var batchSize);

        //Assert
        actual.ShouldBeTrue();
        batchSize.ShouldBe(7);
    }

    [Fact]
    public void AcknowledgeIndividual_GivenLastIndex_ShouldRemoveTracker()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 2, 2);
        uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 0), out _);

        //Act
        var tracker = uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 1), out var isComplete);

        //Assert
        tracker.ShouldNotBeNull();
        isComplete.ShouldBeTrue();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 2, 0)).ShouldBeFalse();
    }

    [Fact]
    public void AcknowledgeIndividual_GivenUntrackedEntry_ShouldReturnNull()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var actual = uut.AcknowledgeIndividual(BatchTestData.CreateBatchedId(1, 2, 0), out var isComplete);

        //Assert
        actual.ShouldBeNull();
        isComplete.ShouldBeFalse();
    }

    [Fact]
    public void AcknowledgeCumulative_ShouldPruneEarlierTrackersOfSamePartitionOnly()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 1, 3);
        _ = BatchTestData.AddBatch(uut, 1, 2, 3);
        _ = BatchTestData.AddBatch(uut, 1, 3, 3);
        _ = BatchTestData.AddBatch(uut, 2, 0, 3);
        var otherPartition = BatchTestData.CreateEntryId(1, 0, 5);
        var data = new ReadOnlySequence<byte>(BatchTestData.CreateBatchPayload(3));
        uut.Add(otherPartition, null, 0, BatchTestData.CreateMetadata(3), data, out _);

        //Act
        var tracker = uut.AcknowledgeCumulative(BatchTestData.CreateBatchedId(1, 2, 0), out var isComplete);

        //Assert
        tracker.ShouldNotBeNull();
        isComplete.ShouldBeFalse();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 1, 0)).ShouldBeFalse();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 2, 0)).ShouldBeTrue();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 3, 0)).ShouldBeTrue();
        uut.IsTracked(BatchTestData.CreateBatchedId(2, 0, 0)).ShouldBeTrue();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 0, 0, 5)).ShouldBeTrue();
    }

    [Fact]
    public void AcknowledgeCumulative_GivenLastIndex_ShouldRemoveTheTrackerOfTheBatch()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 2, 3);

        //Act
        var tracker = uut.AcknowledgeCumulative(BatchTestData.CreateBatchedId(1, 2, 2), out var isComplete);

        //Assert
        tracker.ShouldNotBeNull();
        isComplete.ShouldBeTrue();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 2, 0)).ShouldBeFalse();
    }

    [Fact]
    public void AcknowledgeCumulative_GivenUntrackedEntry_ShouldStillPruneEarlierTrackers()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 1, 3);

        //Act
        var tracker = uut.AcknowledgeCumulative(BatchTestData.CreateBatchedId(1, 5, 0), out _);

        //Assert
        tracker.ShouldBeNull();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 1, 0)).ShouldBeFalse();
    }

    [Fact]
    public void PruneUpTo_ShouldRemoveTrackersUpToAndIncludingTheEntry()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 1, 3);
        _ = BatchTestData.AddBatch(uut, 1, 2, 3);
        _ = BatchTestData.AddBatch(uut, 1, 3, 3);

        //Act
        uut.PruneUpTo(BatchTestData.CreateEntryId(1, 2));

        //Assert
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 1, 0)).ShouldBeFalse();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 2, 0)).ShouldBeFalse();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 3, 0)).ShouldBeTrue();
    }

    [Fact]
    public void Clear_ShouldRemoveAllTrackers()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 1, 3);
        _ = BatchTestData.AddBatch(uut, 1, 2, 3);

        //Act
        uut.Clear();

        //Assert
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 1, 0)).ShouldBeFalse();
        uut.IsTracked(BatchTestData.CreateBatchedId(1, 2, 0)).ShouldBeFalse();
    }

    [Fact]
    public void Clear_ShouldRemoveQueuedMessages()
    {
        //Arrange
        var uut = CreateHandler();
        var data = new ReadOnlySequence<byte>(BatchTestData.CreateBatchPayload(3));
        uut.Add(BatchTestData.CreateEntryId(1, 2), null, 0, BatchTestData.CreateMetadata(3), data, out _);

        //Act
        uut.Clear();

        //Assert
        uut.GetNext().ShouldBeNull();
    }

    [Fact]
    public void Add_GivenInvalidBatchData_ShouldThrowAndNotTrackTheBatch()
    {
        //Arrange
        var uut = CreateHandler();
        var data = new ReadOnlySequence<byte>(new byte[] { 0, 0, 0 });

        //Act
        var exception = Record.Exception(() => uut.Add(BatchTestData.CreateEntryId(1, 2), null, 0, BatchTestData.CreateMetadata(3), data, out _));

        //Assert
        exception.ShouldNotBeNull();
        uut.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 2, 0), out _).ShouldBeFalse();
        uut.GetNext().ShouldBeNull();
    }

    [Fact]
    public void AcknowledgeIndividual_AfterWarmup_ShouldNotAllocate()
    {
        //Arrange
        var uut = CreateHandler();
        _ = BatchTestData.AddBatch(uut, 1, 1, 1000);
        var ackSet = new RepeatedField<long>();
        var messageId = BatchTestData.CreateBatchedId(1, 1, 0);
        var olderEntryId = BatchTestData.CreateEntryId(0, 0);
        var tracker = uut.AcknowledgeIndividual(messageId, out _);
        uut.CopyAckSetTo(tracker!, ackSet);
        uut.TryGetBatchSize(messageId, out _);
        uut.PruneUpTo(olderEntryId);
        var before = GC.GetAllocatedBytesForCurrentThread();

        //Act
        for (var i = 1; i < 900; ++i)
        {
            messageId.BatchIndex = i;
            uut.AcknowledgeIndividual(messageId, out _);
            uut.TryGetBatchSize(messageId, out _);
            uut.CopyAckSetTo(tracker!, ackSet);
            uut.PruneUpTo(olderEntryId);
        }

        //Assert
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        allocated.ShouldBe(0);
    }

    private static BatchHandler<byte[]> CreateHandler()
        => new(true, new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray), BatchTestData.Topic);
}
