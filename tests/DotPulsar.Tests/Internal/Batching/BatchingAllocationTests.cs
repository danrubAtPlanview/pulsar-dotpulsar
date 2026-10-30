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

// The batching of messages does not allocate in the steady state: the batch, the buffer of the batch and the lists are reused
[Trait("Category", "Unit")]
public sealed class BatchingAllocationTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;
    private const int WarmUpRounds = 200;
    private const int MeasuredRounds = 200;
    private const int MaxAttempts = 5;

    [Theory]
    [InlineData(BatcherType.Default, 1)]
    [InlineData(BatcherType.Default, 2)]
    [InlineData(BatcherType.Default, 100)]
    [InlineData(BatcherType.Default, 1000)]
    [InlineData(BatcherType.KeyBased, 1)]
    [InlineData(BatcherType.KeyBased, 2)]
    [InlineData(BatcherType.KeyBased, 100)]
    [InlineData(BatcherType.KeyBased, 1000)]
    public void BatchingRound_GivenSteadyState_ShouldNotAllocate(BatcherType batcherType, int numberOfMessages)
    {
        //Arrange
        var items = CreateItems(numberOfMessages, batcherType == BatcherType.KeyBased ? 5 : 1);
        var container = BatchMessageContainerFactory.Create<TestBatchItem>(batcherType, 0, 0, TimeSpan.FromMilliseconds(1), new PreparedBatchPool(8));
        var groups = new List<PreparedBatchGroup>(16);
        for (var i = 0; i < WarmUpRounds; ++i)
        {
            Round(container, items, groups);
        }

        //Act
        var allocated = MeasureAllocations(() => Round(container, items, groups));

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public void Add_GivenLimitChecksForEveryMessage_ShouldNotAllocate()
    {
        //Arrange
        var items = CreateItems(100, 1);
        var container = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1), new PreparedBatchPool(8));
        var groups = new List<PreparedBatchGroup>(4);
        for (var i = 0; i < WarmUpRounds; ++i)
        {
            LimitsRound(container, items, groups);
        }

        //Act
        var allocated = MeasureAllocations(() => LimitsRound(container, items, groups));

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public void BatchingRound_GivenMoreMessagesThanTheBatchLimit_ShouldNotAllocatePerMessage()
    {
        //Arrange
        var items = CreateItems(3000, 1);
        var container = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1), new PreparedBatchPool(8));
        var groups = new List<PreparedBatchGroup>(4);
        for (var i = 0; i < WarmUpRounds; ++i)
        {
            ChunkedRound(container, items, groups);
        }

        //Act
        var allocated = MeasureAllocations(() => ChunkedRound(container, items, groups));

        //Assert
        allocated.ShouldBe(0);
    }

    // Guards the tests above against measuring nothing
    [Fact]
    public void GetAllocatedBytesForCurrentThread_GivenCodeThatAllocates_ShouldReportTheAllocations()
    {
        //Arrange
        var items = new byte[MeasuredRounds][];
        var before = GC.GetAllocatedBytesForCurrentThread();

        //Act
        for (var i = 0; i < items.Length; ++i)
        {
            items[i] = new byte[100];
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        //Assert
        allocated.ShouldBeGreaterThanOrEqualTo(items.Length * 100L);
    }

    // ArrayPool<byte>.Shared trims its buffers when a gen 2 collection runs under memory pressure, and the next rent then allocates a new array.
    // A real allocation in the code under test shows up in every attempt, so the smallest measurement is the one that counts.
    private static long MeasureAllocations(Action round)
    {
        var smallest = long.MaxValue;

        for (var attempt = 0; attempt < MaxAttempts && smallest != 0; ++attempt)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < MeasuredRounds; ++i)
            {
                round();
            }

            smallest = Math.Min(smallest, GC.GetAllocatedBytesForCurrentThread() - before);
        }

        return smallest;
    }

    private static void Round(IBatchMessageContainer<TestBatchItem> container, TestBatchItem[] items, List<PreparedBatchGroup> groups)
    {
        for (var i = 0; i < items.Length; ++i)
        {
            container.Add(items[i], MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);

        for (var i = 0; i < groups.Count; ++i)
        {
            groups[i].Batch?.Return();
        }

        groups.Clear();
        container.Clear();
    }

    // The checks that a dispatcher makes for every message before it adds it
    private static void LimitsRound(BatchMessageContainer<TestBatchItem> container, TestBatchItem[] items, List<PreparedBatchGroup> groups)
    {
        for (var i = 0; i < items.Length; ++i)
        {
            if (!container.CanAddToCurrentBatch(items[i], MaxMessageSize))
                throw new InvalidOperationException("The message does not fit");

            container.Add(items[i], MaxMessageSize);
            _ = container.ShouldFlush(MaxMessageSize);
            _ = container.GetRemainingPublishDelay();
        }

        container.CreateBatches(groups, MaxMessageSize);
        groups[0].Batch?.Return();
        groups.Clear();
        container.Clear();
    }

    // The messages are added to batches of at most 1000 messages. A batch is created when the limit is reached.
    private static void ChunkedRound(BatchMessageContainer<TestBatchItem> container, TestBatchItem[] items, List<PreparedBatchGroup> groups)
    {
        for (var i = 0; i < items.Length; ++i)
        {
            if (container.Add(items[i], MaxMessageSize))
            {
                container.CreateBatches(groups, MaxMessageSize);
                groups[0].Batch?.Return();
                groups.Clear();
                container.Clear();
            }
        }
    }

    private static TestBatchItem[] CreateItems(int numberOfMessages, int numberOfKeys)
    {
        var items = new TestBatchItem[numberOfMessages];

        for (var i = 0; i < numberOfMessages; ++i)
        {
            var orderingKey = numberOfKeys > 1 ? new[] { (byte) (i % numberOfKeys) } : null;
            var item = TestBatchItem.Create((ulong) i, new byte[100], key: $"key-{i % numberOfKeys}", orderingKey: orderingKey, transaction: (7, 13));
            item.Metadata.Properties.Add(new KeyValue { Key = "key", Value = "value" });
            item.Metadata.EventTime = 1;
            items[i] = item;
        }

        return items;
    }
}
