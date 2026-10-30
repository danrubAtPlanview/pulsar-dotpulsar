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

[Trait("Category", "Unit")]
public sealed class BatchAckTrackerTests
{
    private static readonly MessageIdData _entryId = BatchTestData.CreateEntryId(1, 2, 3);

    [Fact]
    public void Constructor_GivenNoBrokerAckSet_ShouldKeepAllIndexesUnacknowledged()
    {
        //Act
        var uut = new BatchAckTracker(_entryId, 70, null);

        //Assert
        uut.IsEmpty.ShouldBeFalse();
        uut.BatchSize.ShouldBe(70);
        uut.LedgerId.ShouldBe(1UL);
        uut.EntryId.ShouldBe(2UL);
        uut.Partition.ShouldBe(3);
        Enumerable.Range(0, 70).ShouldAllBe(index => !uut.IsAcked(index));
    }

    [Fact]
    public void Constructor_GivenBrokerAckSet_ShouldOnlyKeepUnackedIndexes()
    {
        //Act
        var uut = new BatchAckTracker(_entryId, 5, BatchTestData.CreateAckSet(0b10110));

        //Assert
        uut.IsAcked(0).ShouldBeTrue();
        uut.IsAcked(1).ShouldBeFalse();
        uut.IsAcked(2).ShouldBeFalse();
        uut.IsAcked(3).ShouldBeTrue();
        uut.IsAcked(4).ShouldBeFalse();
    }

    [Fact]
    public void Constructor_GivenBrokerAckSetWithFewerWordsThanTheBatch_ShouldTreatTheMissingWordsAsAcknowledged()
    {
        //Act
        var uut = new BatchAckTracker(_entryId, 70, BatchTestData.CreateAckSet(-1));

        //Assert
        uut.IsAcked(63).ShouldBeFalse();
        uut.IsAcked(64).ShouldBeTrue();
        uut.IsAcked(69).ShouldBeTrue();
    }

    [Fact]
    public void Constructor_GivenEmptyBrokerAckSet_ShouldKeepAllIndexesUnacknowledged()
    {
        //Act
        var uut = new BatchAckTracker(_entryId, 5, new RepeatedField<long>());

        //Assert
        Enumerable.Range(0, 5).ShouldAllBe(index => !uut.IsAcked(index));
    }

    [Fact]
    public void Constructor_GivenBrokerAckSetWithAllIndexesAcknowledged_ShouldBeEmpty()
    {
        //Act
        var uut = new BatchAckTracker(_entryId, 5, BatchTestData.CreateAckSet(0));

        //Assert
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void Constructor_GivenBrokerAckSetWithScatteredBits_ShouldOnlyBeEmptyAfterAcknowledgingAllSetBits()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 128, BatchTestData.CreateAckSet(unchecked((long) 0x8000000000000001UL), 0xF0));
        var setIndexes = new[] { 0, 63, 68, 69, 70, 71 };

        //Act
        var results = setIndexes.Select(uut.AckIndividual).ToArray();

        //Assert
        results.ShouldBeSequence(false, false, false, false, false, true);
    }

    [Fact]
    public void AckIndividual_GivenAllIndexes_ShouldOnlyBeEmptyAfterTheLast()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 3, null);

        //Act
        var results = new[] { uut.AckIndividual(2), uut.AckIndividual(0), uut.AckIndividual(1) };

        //Assert
        results.ShouldBeSequence(false, false, true);
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void AckIndividual_GivenTheSameIndexTwice_ShouldNotBeEmpty()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 3, null);

        //Act
        uut.AckIndividual(1);
        var actual = uut.AckIndividual(1);

        //Assert
        actual.ShouldBeFalse();
        uut.IsAcked(1).ShouldBeTrue();
        uut.IsAcked(0).ShouldBeFalse();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    [InlineData(1000)]
    public void AckIndividual_GivenIndexOutsideBatch_ShouldBeIgnored(int batchIndex)
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 3, null);

        //Act
        var actual = uut.AckIndividual(batchIndex);

        //Assert
        actual.ShouldBeFalse();
        Enumerable.Range(0, 3).ShouldAllBe(index => !uut.IsAcked(index));
    }

    [Fact]
    public void AckIndividual_GivenIndexesOfAllWordsOfALargeBatch_ShouldOnlyBeEmptyAfterTheLast()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 200, null);
        var results = new List<bool>();

        //Act
        for (var i = 199; i >= 0; --i)
        {
            results.Add(uut.AckIndividual(i));
        }

        //Assert
        results.Take(199).ShouldAllBe(result => !result);
        results[199].ShouldBeTrue();
    }

    [Fact]
    public void AckCumulative_GivenIndex4OfBatch10_ShouldLeaveIndexes5To9()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 10, null);

        //Act
        var actual = uut.AckCumulative(4);

        //Assert
        actual.ShouldBeFalse();
        Enumerable.Range(0, 5).ShouldAllBe(index => uut.IsAcked(index));
        Enumerable.Range(5, 5).ShouldAllBe(index => !uut.IsAcked(index));
    }

    [Fact]
    public void AckCumulative_GivenLastIndex_ShouldBeEmpty()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 10, null);

        //Act
        var actual = uut.AckCumulative(9);

        //Assert
        actual.ShouldBeTrue();
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void AckCumulative_GivenIndexBeyondBatch_ShouldAcknowledgeAll()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 70, null);

        //Act
        var actual = uut.AckCumulative(1000);

        //Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void AckCumulative_GivenIndexInSecondWord_ShouldCountRemainingIndexesCorrectly()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 130, null);
        uut.AckIndividual(10);
        uut.AckIndividual(100);

        //Act
        uut.AckCumulative(70);
        var results = new List<bool>();
        for (var i = 71; i < 130; ++i)
        {
            results.Add(uut.AckIndividual(i));
        }

        //Assert
        results.Take(results.Count - 1).ShouldAllBe(result => !result);
        results.Last().ShouldBeTrue();
    }

    [Fact]
    public void AckCumulative_GivenNegativeIndex_ShouldBeIgnored()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 10, null);

        //Act
        var actual = uut.AckCumulative(-1);

        //Assert
        actual.ShouldBeFalse();
        uut.IsAcked(0).ShouldBeFalse();
    }

    [Fact]
    public void CopyAckSetTo_GivenPartiallyAcknowledgedBatch_ShouldWriteTheUnacknowledgedIndexes()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 70, null);
        uut.AckCumulative(65);
        uut.AckIndividual(67);
        var target = new RepeatedField<long>();

        //Act
        uut.CopyAckSetTo(target);

        //Assert
        target.ShouldBeAckSet(0L, 0b110100L);
    }

    [Fact]
    public void CopyAckSetTo_GivenAllIndexesAcknowledged_ShouldWriteEmptyList()
    {
        //Arrange
        var uut = new BatchAckTracker(_entryId, 70, null);
        uut.AckCumulative(69);
        var target = new RepeatedField<long> { 1 };

        //Act
        uut.CopyAckSetTo(target);

        //Assert
        target.Count.ShouldBe(0);
    }

    // Upstream: MessageIdAdvUtilsTest#testAcknowledgeIndividualConcurrently
    [Fact]
    public async Task AckIndividual_GivenConcurrentAcksOfAll32Indexes_ShouldReportEmptyExactlyOnce()
    {
        //Arrange
        const int batchSize = 32;
        const int iterations = 100;
        var batchHandlers = Enumerable.Range(0, iterations).Select(iteration =>
        {
            var batchHandler = new BatchHandler<byte[]>(true, new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray), BatchTestData.Topic, null);
            _ = BatchTestData.AddBatch(batchHandler, 1, 0, batchSize);
            return batchHandler;
        }).ToArray();
        var completed = new int[iterations];
        using var barrier = new Barrier(batchSize);

        // The threads are started once and meet at the barrier for every iteration, because starting 32 threads for each iteration is slow when the machine is busy
        var tasks = Enumerable.Range(0, batchSize).Select(batchIndex => Task.Factory.StartNew(() =>
        {
            var messageId = BatchTestData.CreateBatchedId(1, 0, batchIndex);

            for (var iteration = 0; iteration < iterations; ++iteration)
            {
                barrier.SignalAndWait();
                batchHandlers[iteration].AcknowledgeIndividual(messageId, out var isComplete);

                if (isComplete)
                    _ = Interlocked.Increment(ref completed[iteration]);
            }
        }, TaskCreationOptions.LongRunning)).ToArray();

        //Act
        await Task.WhenAll(tasks);

        //Assert
        completed.ShouldAllBe(count => count == 1);

        foreach (var batchHandler in batchHandlers)
            batchHandler.TryGetBatchSize(BatchTestData.CreateBatchedId(1, 0, 0), out _).ShouldBeFalse();
    }
}
