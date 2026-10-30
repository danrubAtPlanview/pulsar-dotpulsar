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
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

[Trait("Category", "Unit")]
public sealed class ChunkedMessageAssemblerTests
{
    private const ulong Ledger = 1;
    private const int ExpireMs = 1000;

    private readonly List<MessageIdData> _acks;
    private readonly List<MessageIdData> _track;
    private long _now;

    public ChunkedMessageAssemblerTests()
    {
        _acks = [];
        _track = [];
        _now = 5_000_000;
    }

    [Fact]
    public void Process_GivenChunksInOrder_ShouldReturnNullUntilTheLastChunkAndThenTheMessage()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        var first = Feed(uut, "a", 0, 3, 1);
        var second = Feed(uut, "a", 1, 3, 2);
        var third = Feed(uut, "a", 2, 3, 3);

        //Assert
        first.ShouldBeNull();
        second.ShouldBeNull();
        third.ShouldNotBeNull();
        Text(third.Value).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
    }

    [Fact]
    public void Process_GivenChunksInOrder_ShouldReturnTheIdOfEveryChunkInChunkOrder()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 3, 10);
        Feed(uut, "a", 1, 3, 11);

        //Act
        var actual = Feed(uut, "a", 2, 3, 15);

        //Assert
        actual.ShouldNotBeNull();
        EntryIds(actual.Value.ChunkMessageIds).ShouldBe([10UL, 11UL, 15UL]);
        actual.Value.FirstChunkMessageId.EntryId.ShouldBe(10UL);
        actual.Value.LastChunkMessageId.EntryId.ShouldBe(15UL);
    }

    [Fact]
    public void Process_GivenChunksInOrder_ShouldNotCopyTheChunksButLinkThem()
    {
        //Arrange
        using var uut = CreateUut();
        var first = new byte[] { 1, 2, 3 };
        var second = new byte[] { 4, 5 };
        Process(uut, "a", 0, 2, 1, first);

        //Act
        var actual = Process(uut, "a", 1, 2, 2, second);

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.IsSingleSegment.ShouldBeFalse();
        actual.Value.Payload.ToArray().ShouldBe([1, 2, 3, 4, 5]);
        var segments = new List<ReadOnlyMemory<byte>>();
        foreach (var memory in actual.Value.Payload)
            segments.Add(memory);
        segments.Count.ShouldBe(2);
        segments[0].Span.Overlaps(first, out _).ShouldBeTrue();
        segments[1].Span.Overlaps(second, out _).ShouldBeTrue();
    }

    [Fact]
    public void Process_GivenChunkPayloadsInSeveralSegments_ShouldConcatenateAllSegmentsInOrder()
    {
        //Arrange
        using var uut = CreateUut();
        var payload = ChunkTestData.CreatePayload(40);
        Process(uut, "a", 0, 3, 1, ChunkTestData.CreateSegmentedSequence(payload.AsSpan(0, 15).ToArray(), 4, 1, 10));
        Process(uut, "a", 1, 3, 2, ChunkTestData.CreateSegmentedSequence(payload.AsSpan(15, 15).ToArray(), 7, 8));

        //Act
        var actual = Process(uut, "a", 2, 3, 3, new ReadOnlySequence<byte>(payload.AsSpan(30, 10).ToArray()));

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.ToArray().ShouldBe(payload);
    }

    [Fact]
    public void Process_GivenEmptyMiddleChunk_ShouldStillAssembleTheMessage()
    {
        //Arrange
        using var uut = CreateUut();
        Process(uut, "a", 0, 3, 1, new byte[] { 1, 2 });
        Process(uut, "a", 1, 3, 2, ReadOnlySequence<byte>.Empty);

        //Act
        var actual = Process(uut, "a", 2, 3, 3, new byte[] { 3 });

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.ToArray().ShouldBe([1, 2, 3]);
    }

    [Fact]
    public void Process_GivenEmptyPayloadInEveryChunk_ShouldAssembleAnEmptyMessage()
    {
        //Arrange
        using var uut = CreateUut();
        Process(uut, "a", 0, 2, 1, ReadOnlySequence<byte>.Empty);

        //Act
        var actual = Process(uut, "a", 1, 2, 2, ReadOnlySequence<byte>.Empty);

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.Length.ShouldBe(0);
        actual.Value.ChunkMessageIds.Length.ShouldBe(2);
    }

    [Fact]
    public void Process_GivenChunksThatFillExactlyTheTotalSize_ShouldAssembleTheMessage()
    {
        //Arrange
        using var uut = CreateUut();
        Process(uut, "a", 0, 2, 1, new byte[50], 100);

        //Act
        var actual = Process(uut, "a", 1, 2, 2, new byte[50], 100);

        //Assert
        actual.ShouldNotBeNull();
        actual.Value.Payload.Length.ShouldBe(100);
    }

    [Fact]
    public void Process_GivenCompletedMessage_ShouldLeaveNoPendingContext()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);

        //Act
        Feed(uut, "a", 1, 2, 2);

        //Assert
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenFirstChunkOfIncompleteMessage_ShouldHaveOnePendingContext()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        Feed(uut, "a", 0, 2, 1);

        //Assert
        uut.PendingCount.ShouldBe(1);
        uut.QueueCount.ShouldBe(1);
    }

    // Upstream: MessageChunkingTest#testPendingChunkedMessageUuidQueueDoesNotLeak
    [Fact]
    public void Process_GivenFiftyCompletedMessages_ShouldLeaveNoPendingContexts()
    {
        //Arrange
        using var uut = CreateUut(10, true);

        //Act
        for (var i = 0; i < 50; ++i)
        {
            var uuid = i.ToString();
            Feed(uut, uuid, 0, 2, (ulong) (i * 2) + 1);
            var message = Feed(uut, uuid, 1, 2, (ulong) (i * 2) + 2);

            message.ShouldNotBeNull();
            Text(message.Value).ShouldBe($"chunk-{uuid}-0|chunk-{uuid}-1|");
        }

        //Assert
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
    }

    // Upstream: MessageChunkingSharedTest#testInterleavedChunks
    [Fact]
    public void Process_GivenInterleavedMessagesFromTwoProducers_ShouldCompleteInCompletionOrder()
    {
        //Arrange
        using var uut = CreateUut();
        var completed = new List<string>();
        ulong entry = 0;

        //Act
        foreach (var (uuid, chunkId, numberOfChunks) in new[] { ("A-1", 0, 3), ("A-1", 1, 3), ("B-1", 0, 2), ("B-1", 1, 2), ("A-1", 2, 3) })
        {
            var message = Feed(uut, uuid, chunkId, numberOfChunks, ++entry);
            if (message is not null)
                completed.Add(uuid);
        }

        //Assert
        completed.ShouldBe(["B-1", "A-1"]);
        uut.PendingCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenManyMessagesWithRandomlyInterleavedChunks_ShouldAssembleEveryMessageCorrectly()
    {
        //Arrange
        using var uut = CreateUut(0);
        var random = new Random(12345);
        const int messages = 30;
        const int chunks = 6;
        var next = new int[messages];
        var completed = new Dictionary<string, string>();
        ulong entry = 0;

        //Act
        for (var remaining = messages * chunks; remaining > 0; --remaining)
        {
            int i;
            do
            {
                i = random.Next(messages);
            } while (next[i] == chunks);

            var uuid = $"m{i}";
            var message = Feed(uut, uuid, next[i]++, chunks, ++entry);
            if (message is not null)
                completed.Add(uuid, Text(message.Value));
        }

        //Assert
        completed.Count.ShouldBe(messages);
        foreach (var (uuid, text) in completed)
            text.ShouldBe(string.Concat(Enumerable.Range(0, chunks).Select(chunkId => $"chunk-{uuid}-{chunkId}|")));
        uut.PendingCount.ShouldBe(0);
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
    }

    // Upstream: MessageChunkingTest#testResendChunkMessagesWithoutAckHole
    [Fact]
    public void Process_GivenResentFirstChunkWithSameEntry_ShouldNotAck()
    {
        //Arrange
        using var uut = CreateUut(10, true);
        Feed(uut, "0", 0, 2, 1);
        Feed(uut, "0", 0, 2, 1);

        //Act
        var actual = Feed(uut, "0", 1, 2, 2);

        //Assert
        actual.ShouldNotBeNull();
        Text(actual.Value).ShouldBe("chunk-0-0|chunk-0-1|");
        EntryIds(actual.Value.ChunkMessageIds).ShouldBe([1UL, 2UL]);
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
    }

    // Upstream: MessageChunkingTest#testResendChunkMessagesWithoutAckHole, ConsumerImpl#doProcessMessageChunk (L1813-L1832)
    [Fact]
    public void Process_GivenFirstChunkAgainWithDifferentEntry_ShouldAckPreviousChunks()
    {
        //Arrange
        using var uut = CreateUut(10, true);
        Feed(uut, "0", 0, 3, 1);
        Feed(uut, "0", 1, 3, 2);

        //Act
        Feed(uut, "0", 0, 3, 3);

        //Assert
        EntryIds(_acks).ShouldBe([1UL, 2UL]);
        _track.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(1);
    }

    [Fact]
    public void Process_GivenFirstChunkAgainWithDifferentEntry_ShouldRestartAssemblyWithTheNewChunk()
    {
        //Arrange
        using var uut = CreateUut(10, true);
        Feed(uut, "0", 0, 2, 1);
        Feed(uut, "0", 0, 2, 2);

        //Act
        var actual = Feed(uut, "0", 1, 2, 3);

        //Assert
        actual.ShouldNotBeNull();
        EntryIds(actual.Value.ChunkMessageIds).ShouldBe([2UL, 3UL]);
        Text(actual.Value).ShouldBe("chunk-0-0|chunk-0-1|");
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testResendChunkMessages
    [Fact]
    public void Process_GivenResentAndInterleavedChunks_ShouldAssembleBothMessages()
    {
        //Arrange
        using var uut = CreateUut(10, true);
        var completed = new List<AssembledChunkedMessage>();

        //Act
        foreach (var (uuid, chunkId, numberOfChunks, entry) in new[]
                 {
                     ("0", 0, 2, 1UL), ("0", 0, 2, 2UL), ("1", 0, 3, 3UL), ("1", 1, 3, 4UL), ("1", 0, 3, 5UL), ("0", 1, 2, 6UL), ("1", 1, 3, 7UL), ("1", 2, 3, 8UL)
                 })
        {
            var message = Feed(uut, uuid, chunkId, numberOfChunks, entry);
            if (message is not null)
                completed.Add(message.Value);
        }

        //Assert
        completed.Count.ShouldBe(2);
        Text(completed[0]).ShouldBe("chunk-0-0|chunk-0-1|");
        EntryIds(completed[0].ChunkMessageIds).ShouldBe([2UL, 6UL]);
        Text(completed[1]).ShouldBe("chunk-1-0|chunk-1-1|chunk-1-2|");
        EntryIds(completed[1].ChunkMessageIds).ShouldBe([5UL, 7UL, 8UL]);
        EntryIds(_acks).ShouldBe([1UL, 3UL, 4UL]);
        _track.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk (L1864-L1891)
    [Fact]
    public void Process_GivenDuplicatedChunkWithDifferentEntry_ShouldAckDuplicate()
    {
        //Arrange
        using var uut = CreateUut();
        Process(uut, "a", 0, 4, 1, new byte[] { 0 });
        Process(uut, "a", 1, 4, 2, new byte[] { 1 });
        Process(uut, "a", 2, 4, 3, new byte[] { 2 });

        //Act
        var firstDuplicate = Process(uut, "a", 1, 4, 4, new byte[] { 9 });
        var secondDuplicate = Process(uut, "a", 2, 4, 5, new byte[] { 9 });
        var last = Process(uut, "a", 3, 4, 6, new byte[] { 3 });

        //Assert
        firstDuplicate.ShouldBeNull();
        secondDuplicate.ShouldBeNull();
        EntryIds(_acks).ShouldBe([4UL, 5UL]);
        _track.ShouldBeEmpty();
        last.ShouldNotBeNull();
        last.Value.Payload.ToArray().ShouldBe([0, 1, 2, 3]);
        EntryIds(last.Value.ChunkMessageIds).ShouldBe([1UL, 2UL, 3UL, 6UL]);
    }

    [Fact]
    public void Process_GivenRedeliveredChunkWithSameEntry_ShouldNotAckAndKeepAssembling()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 3, 1);
        Feed(uut, "a", 1, 3, 2);

        //Act
        var redelivered = Feed(uut, "a", 1, 3, 2);
        var last = Feed(uut, "a", 2, 3, 3);

        //Assert
        redelivered.ShouldBeNull();
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
        last.ShouldNotBeNull();
        Text(last.Value).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk (L1893-L1926)
    [Fact]
    public void Process_GivenChunkGapAfterExpiry_ShouldAckChunk()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        var actual = Feed(uut, "a", 1, 2, 7, (ulong) _now - ExpireMs - 1);

        //Assert
        actual.ShouldBeNull();
        EntryIds(_acks).ShouldBe([7UL]);
        _track.ShouldBeEmpty();
    }

    [Fact]
    public void Process_GivenChunkGapBeforeExpiry_ShouldTrackChunk()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        var actual = Feed(uut, "a", 1, 2, 7, (ulong) _now - ExpireMs);

        //Assert
        actual.ShouldBeNull();
        _acks.ShouldBeEmpty();
        EntryIds(_track).ShouldBe([7UL]);
    }

    [Fact]
    public void Process_GivenChunkGapAndExpiryDisabled_ShouldTrackChunk()
    {
        //Arrange
        using var uut = CreateUut(10, false, 0);

        //Act
        Feed(uut, "a", 1, 2, 7, 1);

        //Assert
        _acks.ShouldBeEmpty();
        EntryIds(_track).ShouldBe([7UL]);
    }

    [Fact]
    public void Process_GivenChunkInTheMiddleOfTheMessageWithoutTheFirstChunk_ShouldDropOnlyThatChunk()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        var actual = Feed(uut, "a", 2, 4, 7);

        //Assert
        actual.ShouldBeNull();
        EntryIds(_track).ShouldBe([7UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenChunkSkippingAChunk_ShouldDropTheContextAndTrackTheChunk()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 3, 1);

        //Act
        var actual = Feed(uut, "a", 2, 3, 3);
        var next = Feed(uut, "a", 1, 3, 2);

        //Assert
        actual.ShouldBeNull();
        next.ShouldBeNull();
        EntryIds(_track).ShouldBe([3UL, 2UL]);
        _acks.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenChunksInReverseOrder_ShouldNotAssembleTheMessage()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        var results = new[] { 3, 2, 1, 0 }.Select(chunkId => Feed(uut, "a", chunkId, 4, (ulong) chunkId + 1)).ToArray();

        //Assert
        results.ShouldAllBe(result => result == null);
        EntryIds(_track).ShouldBe([4UL, 3UL, 2UL]);
        uut.PendingCount.ShouldBe(1);
    }

    // Upstream: MessageChunkingTest#testOrphanedLastChunkDoesNotStallDispatch
    [Fact]
    public void Process_GivenOrphanedLastChunkAfterTheMessageWasExpired_ShouldDiscardTheChunk()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "orphan", 0, 2, 1);
        _now += ExpireMs + 1;
        uut.RemoveExpired(_acks);

        //Act
        var actual = Feed(uut, "orphan", 1, 2, 2, (ulong) _now - ExpireMs - 1);

        //Assert
        actual.ShouldBeNull();
        EntryIds(_acks).ShouldBe([1UL, 2UL]);
        uut.PendingCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenChunkOverflowingTotalSize_ShouldDropContext()
    {
        //Arrange
        using var uut = CreateUut();
        Process(uut, "a", 0, 3, 1, new byte[60], 100);

        //Act
        var actual = Process(uut, "a", 1, 3, 2, new byte[60], 100);

        //Assert
        actual.ShouldBeNull();
        EntryIds(_acks).ShouldBe([1UL, 2UL]);
        _track.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenChunkThatOverflowsTheTotalSize_ShouldNotThrow()
    {
        //Arrange
        using var uut = CreateUut();

        //Act
        var act = () => Process(uut, "a", 0, 2, 1, new byte[101], 100);

        //Assert
        Should.NotThrow(act);
    }

    [Theory]
    [InlineData(5, 4)]
    [InlineData(2, 2)]
    [InlineData(2, -1)]
    [InlineData(1, 0)]
    [InlineData(0, 0)]
    [InlineData(-3, 0)]
    public void Process_GivenChunkIdOrNumberOfChunksThatDoNotMatch_ShouldDiscardTheChunkAndDropTheContext(int chunkId, int numberOfChunks)
    {
        //Arrange
        using var uut = CreateUut();
        var firstChunkOfMessage = numberOfChunks < 2 ? 2 : numberOfChunks;
        Feed(uut, "a", 0, firstChunkOfMessage, 1);

        //Act
        var actual = Feed(uut, "a", chunkId, numberOfChunks, 2);

        //Assert
        actual.ShouldBeNull();
        EntryIds(_track).ShouldBe([2UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testMaxPendingChunkMessages
    [Fact]
    public void Process_GivenMaxPendingExceededWithAutoAck_ShouldAckOldestChunks()
    {
        //Arrange
        using var uut = CreateUut(1, true);
        Feed(uut, "0", 0, 2, 1);

        //Act
        var firstOfSecond = Feed(uut, "1", 0, 2, 2);
        var second = Feed(uut, "1", 1, 2, 3);
        var lateChunkOfFirst = Feed(uut, "0", 1, 2, 4);

        //Assert
        firstOfSecond.ShouldBeNull();
        EntryIds(_acks).ShouldBe([1UL]);
        second.ShouldNotBeNull();
        Text(second.Value).ShouldBe("chunk-1-0|chunk-1-1|");
        lateChunkOfFirst.ShouldBeNull();
        EntryIds(_track).ShouldBe([4UL]);
        uut.PendingCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenMaxPendingExceededWithoutAutoAck_ShouldTrackOldestChunks()
    {
        //Arrange
        using var uut = CreateUut(1, false);
        Feed(uut, "0", 0, 3, 1);
        Feed(uut, "0", 1, 3, 2);

        //Act
        Feed(uut, "1", 0, 2, 3);

        //Assert
        EntryIds(_track).ShouldBe([1UL, 2UL]);
        _acks.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(1);
    }

    [Fact]
    public void Process_GivenMaxPendingExceededRepeatedly_ShouldEvictTheOldestFirst()
    {
        //Arrange
        using var uut = CreateUut(2, true);
        Feed(uut, "a", 0, 2, 1);
        Feed(uut, "b", 0, 2, 2);

        //Act
        Feed(uut, "c", 0, 2, 3);
        var afterC = EntryIds(_acks);
        Feed(uut, "d", 0, 2, 4);

        //Assert
        afterC.ShouldBe([1UL]);
        EntryIds(_acks).ShouldBe([1UL, 2UL]);
        uut.PendingCount.ShouldBe(2);
        uut.QueueCount.ShouldBe(2);
        Feed(uut, "c", 1, 2, 5).ShouldNotBeNull();
        Feed(uut, "d", 1, 2, 6).ShouldNotBeNull();
    }

    [Fact]
    public void Process_GivenMaxPendingExceeded_ShouldNotEvictAMessageThatWasStartedAgain()
    {
        //Arrange
        using var uut = CreateUut(2, true);
        Feed(uut, "a", 0, 2, 1);
        Feed(uut, "b", 0, 2, 2);

        //Act
        Feed(uut, "a", 0, 2, 1);

        //Assert
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(2);
    }

    [Fact]
    public void Process_GivenMaxPendingOfOneAndFirstChunkRedelivered_ShouldNotEvictAnything()
    {
        //Arrange
        using var uut = CreateUut(1, true);
        Feed(uut, "a", 0, 2, 1);

        //Act
        Feed(uut, "a", 0, 2, 1);

        //Assert
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(1);
    }

    [Fact]
    public void Process_GivenMaxPendingExceededAfterSeveralChunksWereReceived_ShouldAckEveryReceivedChunkOfTheOldest()
    {
        //Arrange
        using var uut = CreateUut(1, true);
        Feed(uut, "a", 0, 4, 1);
        Feed(uut, "a", 1, 4, 2);
        Feed(uut, "a", 2, 4, 3);

        //Act
        Feed(uut, "b", 0, 2, 4);

        //Assert
        EntryIds(_acks).ShouldBe([1UL, 2UL, 3UL]);
    }

    [Fact]
    public void Process_GivenNoMaxPending_ShouldNeverEvict()
    {
        //Arrange
        using var uut = CreateUut(0, true);

        //Act
        for (var i = 0; i < 100; ++i)
            Feed(uut, $"m{i}", 0, 2, (ulong) i + 1);

        //Assert
        uut.PendingCount.ShouldBe(100);
        uut.QueueCount.ShouldBe(100);
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
    }

    [Fact]
    public void Process_GivenMaxPending_ShouldNeverHaveMorePendingMessagesThanTheMax()
    {
        //Arrange
        using var uut = CreateUut(3, true);

        //Act
        for (var i = 0; i < 20; ++i)
        {
            Feed(uut, $"m{i}", 0, 2, (ulong) i + 1);

            //Assert
            uut.PendingCount.ShouldBeLessThanOrEqualTo(3);
            uut.QueueCount.ShouldBe(uut.PendingCount);
        }

        EntryIds(_acks).ShouldBe(Enumerable.Range(1, 17).Select(entry => (ulong) entry).ToArray());
    }

    // Upstream: MessageChunkingTest#testExpireIncompleteChunkMessage
    [Fact]
    public void RemoveExpired_GivenIncompleteMessageOlderThanExpiry_ShouldRemoveContext()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 3, 1);
        Feed(uut, "a", 1, 3, 2);
        _now += ExpireMs + 1;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        EntryIds(acks).ShouldBe([1UL, 2UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public void RemoveExpired_GivenIncompleteMessageThatIsNotOlderThanExpiry_ShouldKeepContext()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        _now += ExpireMs;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        acks.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(1);
    }

    [Fact]
    public void RemoveExpired_GivenExpiredMessage_ShouldNotAssembleItWhenTheLastChunkArrives()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        _now += ExpireMs + 1;
        uut.RemoveExpired([]);

        //Act
        var actual = Feed(uut, "a", 1, 2, 2);

        //Assert
        actual.ShouldBeNull();
    }

    [Fact]
    public void RemoveExpired_GivenSeveralMessagesWhereOnlyTheOldestAreExpired_ShouldKeepTheNewest()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        _now += 10;
        Feed(uut, "b", 0, 2, 2);
        _now += 90;
        Feed(uut, "c", 0, 2, 3);
        _now += ExpireMs - 100 + 11;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        EntryIds(acks).ShouldBe([1UL, 2UL]);
        uut.PendingCount.ShouldBe(1);
        uut.QueueCount.ShouldBe(1);
        Feed(uut, "c", 1, 2, 4).ShouldNotBeNull();
    }

    [Fact]
    public void RemoveExpired_GivenExpiryDisabled_ShouldDoNothing()
    {
        //Arrange
        using var uut = CreateUut(10, false, 0);
        Feed(uut, "a", 0, 2, 1);
        _now += 1_000_000_000;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        acks.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(1);
    }

    [Fact]
    public void RemoveExpired_GivenNothingPending_ShouldDoNothing()
    {
        //Arrange
        using var uut = CreateUut();
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        acks.ShouldBeEmpty();
    }

    // Upstream: MessageChunkingTest#testExpiryWorksAfterAnEarlierChunkedMessageCompletes
    [Fact]
    public void RemoveExpired_GivenCompletedThenIncompleteMessage_ShouldExpireIncomplete()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "done", 0, 2, 1);
        Feed(uut, "done", 1, 2, 2).ShouldNotBeNull();
        Feed(uut, "stuck", 0, 2, 3);
        _now += ExpireMs + 1;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        EntryIds(acks).ShouldBe([3UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testExpirySkipsStaleQueueHeadFromDiscardPath
    [Fact]
    public void RemoveExpired_GivenStaleQueueHeadFromDiscardPath_ShouldStillExpireLiveEntries()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "ghost", 0, 3, 1);
        Feed(uut, "ghost", 2, 3, 3);
        Feed(uut, "stuck", 0, 2, 4);
        uut.PendingCount.ShouldBe(1);
        uut.QueueCount.ShouldBe(1);
        _now += ExpireMs + 1;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        EntryIds(acks).ShouldBe([4UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testExpiryStaleHeadSkipDoesNotDropConcurrentLiveEntry
    [Fact]
    public void RemoveExpired_GivenCompletionOfOtherMessage_ShouldKeepLiveEntryQueued()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "A", 0, 2, 1);
        Feed(uut, "B", 0, 2, 2);

        //Act
        Feed(uut, "A", 1, 2, 3).ShouldNotBeNull();

        //Assert
        uut.PendingCount.ShouldBe(1);
        uut.QueueCount.ShouldBe(1);
        _now += ExpireMs + 1;
        var acks = new List<MessageIdData>();
        uut.RemoveExpired(acks);
        EntryIds(acks).ShouldBe([2UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testNonHeadCompletionKeepsIncompleteHeadExpirable
    [Fact]
    public void Process_GivenNonHeadCompletion_ShouldKeepIncompleteHeadExpirable()
    {
        //Arrange
        using var uut = CreateUut(10, false, 5000);
        Feed(uut, "A", 0, 2, 1);
        Feed(uut, "B", 0, 2, 2);

        //Act
        var b = Feed(uut, "B", 1, 2, 3);

        //Assert
        b.ShouldNotBeNull();
        Text(b.Value).ShouldBe("chunk-B-0|chunk-B-1|");
        uut.PendingCount.ShouldBe(1);
        uut.QueueCount.ShouldBe(1);
        _now += 5001;
        var acks = new List<MessageIdData>();
        uut.RemoveExpired(acks);
        EntryIds(acks).ShouldBe([1UL]);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public void RemoveExpired_GivenMessageThatWasStartedAgain_ShouldExpireFromTheTimeItWasStartedAgain()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        _now += ExpireMs;
        Feed(uut, "a", 0, 2, 1);
        _now += 1;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        acks.ShouldBeEmpty();
        uut.PendingCount.ShouldBe(1);
    }

    [Fact]
    public void Clear_GivenPendingMessages_ShouldDropAllContextsWithoutAcknowledging()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        Feed(uut, "b", 0, 2, 2);

        //Act
        uut.Clear();

        //Assert
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
        Feed(uut, "a", 1, 2, 3).ShouldBeNull();
        EntryIds(_track).ShouldBe([3UL]);
    }

    [Fact]
    public void Clear_GivenAssemblerThatIsUsedAfterwards_ShouldAssembleNewMessages()
    {
        //Arrange
        using var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        uut.Clear();
        Feed(uut, "a", 0, 2, 1);

        //Act
        var actual = Feed(uut, "a", 1, 2, 2);

        //Assert
        actual.ShouldNotBeNull();
        _acks.ShouldBeEmpty();
    }

    [Fact]
    public void Dispose_GivenPendingMessages_ShouldDropAllContexts()
    {
        //Arrange
        var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        Feed(uut, "b", 0, 2, 2);

        //Act
        uut.Dispose();

        //Assert
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
        _acks.ShouldBeEmpty();
    }

    [Fact]
    public void Dispose_GivenCalledTwice_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateUut();
        uut.Dispose();

        //Act
        var act = () => uut.Dispose();

        //Assert
        Should.NotThrow(act);
    }

    [Fact]
    public void Process_GivenDisposed_ShouldReturnNullAndKeepNothing()
    {
        //Arrange
        var uut = CreateUut();
        uut.Dispose();

        //Act
        var first = Feed(uut, "a", 0, 2, 1);
        var second = Feed(uut, "a", 1, 2, 2);

        //Assert
        first.ShouldBeNull();
        second.ShouldBeNull();
        uut.PendingCount.ShouldBe(0);
        _acks.ShouldBeEmpty();
        _track.ShouldBeEmpty();
    }

    [Fact]
    public void RemoveExpired_GivenDisposed_ShouldDoNothing()
    {
        //Arrange
        var uut = CreateUut();
        Feed(uut, "a", 0, 2, 1);
        uut.Dispose();
        _now += ExpireMs + 1;
        var acks = new List<MessageIdData>();

        //Act
        uut.RemoveExpired(acks);

        //Assert
        acks.ShouldBeEmpty();
    }

    [Fact]
    public async Task Process_GivenIncompleteMessageAndAcknowledgeExpired_ShouldAcknowledgeTheChunksWhenTheMessageExpires()
    {
        //Arrange
        var acknowledged = new TaskCompletionSource<IReadOnlyList<MessageIdData>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromMilliseconds(100), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), ids =>
        {
            acknowledged.TrySetResult(ids);
            return default;
        });

        //Act
        Feed(uut, "a", 0, 3, 1);
        Feed(uut, "a", 1, 3, 2);
        var actual = await acknowledged.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        EntryIds(actual).ShouldBe([1UL, 2UL]);
        uut.PendingCount.ShouldBe(0);
        _acks.ShouldBeEmpty();
    }

    [Fact]
    public async Task Process_GivenAcknowledgeExpiredThatThrows_ShouldKeepAcknowledgingLaterMessages()
    {
        //Arrange
        var calls = 0;
        var acknowledged = new TaskCompletionSource<IReadOnlyList<MessageIdData>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromMilliseconds(100), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), ids =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                throw new InvalidOperationException("The ack failed");

            acknowledged.TrySetResult(ids);
            return default;
        });
        Feed(uut, "a", 0, 2, 1);
        while (Volatile.Read(ref calls) == 0)
            await Task.Delay(20, Current.CancellationToken);

        //Act
        Feed(uut, "b", 0, 2, 2);
        var actual = await acknowledged.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        EntryIds(actual).ShouldBe([2UL]);
    }

    [Fact]
    public async Task Process_GivenAcknowledgeExpiredThatFails_ShouldNotFaultTheTimer()
    {
        //Arrange
        var calls = 0;
        using var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromMilliseconds(50), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), _ =>
        {
            Interlocked.Increment(ref calls);
            return new ValueTask(Task.FromException(new InvalidOperationException("The ack failed")));
        });

        //Act
        Feed(uut, "a", 0, 2, 1);
        while (Volatile.Read(ref calls) == 0)
            await Task.Delay(20, Current.CancellationToken);
        await Task.Delay(200, Current.CancellationToken);

        //Assert
        calls.ShouldBe(1);
        uut.PendingCount.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenExpireTimeLongerThanATimerCanHandle_ShouldNotThrowAndKeepTheMessage()
    {
        //Arrange
        using var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromDays(100), () => _now, _ => default);
        using var uutWithMaxValue = new ChunkedMessageAssembler(10, false, TimeSpan.MaxValue, () => _now, _ => default);

        //Act
        var act = () =>
        {
            Feed(uut, "a", 0, 2, 1);
            Feed(uutWithMaxValue, "a", 0, 2, 1);
        };

        //Assert
        Should.NotThrow(act);
        uut.PendingCount.ShouldBe(1);
        uutWithMaxValue.PendingCount.ShouldBe(1);
        _now += 1_000_000_000;
        var acks = new List<MessageIdData>();
        uutWithMaxValue.RemoveExpired(acks);
        acks.ShouldBeEmpty();
    }

    [Fact]
    public async Task Process_GivenNoAcknowledgeExpired_ShouldNotExpireMessagesByItself()
    {
        //Arrange
        using var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromMilliseconds(50), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), null);
        Feed(uut, "a", 0, 2, 1);

        //Act
        await Task.Delay(400, Current.CancellationToken);

        //Assert
        uut.PendingCount.ShouldBe(1);
        var acks = new List<MessageIdData>();
        uut.RemoveExpired(acks);
        EntryIds(acks).ShouldBe([1UL]);
    }

    [Fact]
    public async Task Dispose_GivenIncompleteMessageAndTimer_ShouldNotAcknowledgeAfterwards()
    {
        //Arrange
        var calls = 0;
        var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromMilliseconds(50), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), _ =>
        {
            Interlocked.Increment(ref calls);
            return default;
        });
        Feed(uut, "a", 0, 2, 1);

        //Act
        uut.Dispose();
        await Task.Delay(400, Current.CancellationToken);

        //Assert
        calls.ShouldBe(0);
    }

    [Fact]
    public async Task Process_GivenCompletedMessagesOnly_ShouldNotAcknowledgeAnythingFromTheTimer()
    {
        //Arrange
        var calls = 0;
        using var uut = new ChunkedMessageAssembler(10, false, TimeSpan.FromMilliseconds(50), static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), _ =>
        {
            Interlocked.Increment(ref calls);
            return default;
        });
        Feed(uut, "a", 0, 2, 1);
        Feed(uut, "a", 1, 2, 2).ShouldNotBeNull();

        //Act
        await Task.Delay(400, Current.CancellationToken);

        //Assert
        calls.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenIncompleteMessage_ShouldRetainTheChunkMemory()
    {
        //Arrange
        using var uut = CreateUut();
        var reference = StartMessage(uut, "a", 1);

        //Act
        var actual = IsCollected(reference);

        //Assert
        actual.ShouldBeFalse();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Process_GivenCompletedMessage_ShouldReleaseTheChunkMemoryWhenTheMessageIsGone()
    {
        //Arrange
        using var uut = CreateUut();
        var reference = CompleteMessage(uut, "a", 1);

        //Act
        var actual = IsCollected(reference);

        //Assert
        actual.ShouldBeTrue();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void RemoveExpired_GivenExpiredMessage_ShouldReleaseTheChunkMemory()
    {
        //Arrange
        using var uut = CreateUut();
        var reference = StartMessage(uut, "a", 1);
        _now += ExpireMs + 1;

        //Act
        uut.RemoveExpired([]);

        //Assert
        IsCollected(reference).ShouldBeTrue();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Process_GivenEvictedMessage_ShouldReleaseTheChunkMemory()
    {
        //Arrange
        using var uut = CreateUut(1, true);
        var evicted = StartMessage(uut, "a", 1);
        var kept = StartMessage(uut, "b", 2);

        //Act
        var actualEvicted = IsCollected(evicted);
        var actualKept = IsCollected(kept);

        //Assert
        actualEvicted.ShouldBeTrue();
        actualKept.ShouldBeFalse();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Process_GivenFirstChunkAgainWithDifferentEntry_ShouldReleaseTheMemoryOfThePreviousChunks()
    {
        //Arrange
        using var uut = CreateUut();
        var previous = StartMessage(uut, "a", 1);
        var restarted = StartMessage(uut, "a", 2);

        //Act
        var actualPrevious = IsCollected(previous);
        var actualRestarted = IsCollected(restarted);

        //Assert
        actualPrevious.ShouldBeTrue();
        actualRestarted.ShouldBeFalse();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Process_GivenChunkGap_ShouldReleaseTheMemoryOfTheDroppedContext()
    {
        //Arrange
        using var uut = CreateUut();
        var reference = StartMessage(uut, "a", 1);
        Feed(uut, "a", 2, 3, 2);

        //Act
        var actual = IsCollected(reference);

        //Assert
        actual.ShouldBeTrue();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Clear_GivenIncompleteMessage_ShouldReleaseTheChunkMemory()
    {
        //Arrange
        using var uut = CreateUut();
        var reference = StartMessage(uut, "a", 1);

        //Act
        uut.Clear();

        //Assert
        IsCollected(reference).ShouldBeTrue();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Dispose_GivenIncompleteMessage_ShouldReleaseTheChunkMemory()
    {
        //Arrange
        var uut = CreateUut();
        var reference = StartMessage(uut, "a", 1);

        //Act
        uut.Dispose();

        //Assert
        IsCollected(reference).ShouldBeTrue();
        GC.KeepAlive(uut);
    }

    [Fact]
    public void Process_GivenMiddleChunks_ShouldOnlyAllocateTheSegmentOfEveryChunk()
    {
        //Arrange
        using var uut = CreateUut(0, false, 0);
        const int numberOfChunks = 102;
        var chunks = Enumerable.Range(0, numberOfChunks).Select(_ => new ReadOnlySequence<byte>(new byte[10240])).ToArray();
        var metadatas = Enumerable.Range(0, numberOfChunks).Select(chunkId => ChunkTestData.CreateChunkMetadata("a", chunkId, numberOfChunks, numberOfChunks * 10240)).ToArray();
        var ids = Enumerable.Range(0, numberOfChunks).Select(chunkId => ChunkTestData.CreateEntryId(Ledger, (ulong) chunkId)).ToArray();
        WarmUp(uut);
        uut.Process(ids[0], metadatas[0], chunks[0], _acks, _track);

        //Act
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var chunkId = 1; chunkId < numberOfChunks - 1; ++chunkId)
            uut.Process(ids[chunkId], metadatas[chunkId], chunks[chunkId], _acks, _track);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        //Assert
        allocated.ShouldBeLessThanOrEqualTo(64 * (numberOfChunks - 2));
    }

    [Fact]
    public void Process_GivenChunksThatAreDiscardedOrDuplicated_ShouldNotAllocate()
    {
        //Arrange
        using var uut = CreateUut();
        var id = ChunkTestData.CreateEntryId(Ledger, 1);
        var orphan = ChunkTestData.CreateChunkMetadata("orphan", 1, 2, 100, (ulong) _now);
        var duplicated = ChunkTestData.CreateChunkMetadata("duplicated", 1, 3, 100, (ulong) _now);
        var payload = new ReadOnlySequence<byte>(new byte[10]);
        uut.Process(id, ChunkTestData.CreateChunkMetadata("duplicated", 0, 3, 100, (ulong) _now), payload, _acks, _track);
        uut.Process(ChunkTestData.CreateEntryId(Ledger, 2), duplicated, payload, _acks, _track);
        for (var i = 0; i < 10; ++i)
        {
            uut.Process(id, orphan, payload, _acks, _track);
            uut.Process(id, duplicated, payload, _acks, _track);
            _acks.Clear();
            _track.Clear();
        }

        //Act
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; ++i)
        {
            uut.Process(id, orphan, payload, _acks, _track);
            uut.Process(id, duplicated, payload, _acks, _track);
            _acks.Clear();
            _track.Clear();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public void Process_GivenOneMegabyteInHundredChunks_ShouldNotCopyThePayload()
    {
        //Arrange
        using var uut = CreateUut(0, false, 0);
        const int numberOfChunks = 100;
        var chunks = Enumerable.Range(0, numberOfChunks).Select(_ => new ReadOnlySequence<byte>(new byte[10240])).ToArray();
        var metadatas = Enumerable.Range(0, numberOfChunks).Select(chunkId => ChunkTestData.CreateChunkMetadata("a", chunkId, numberOfChunks, numberOfChunks * 10240)).ToArray();
        var ids = Enumerable.Range(0, numberOfChunks).Select(chunkId => ChunkTestData.CreateEntryId(Ledger, (ulong) chunkId)).ToArray();
        WarmUp(uut);
        AssembledChunkedMessage? message = null;

        //Act
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var chunkId = 0; chunkId < numberOfChunks; ++chunkId)
            message = uut.Process(ids[chunkId], metadatas[chunkId], chunks[chunkId], _acks, _track);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        //Assert
        message.ShouldNotBeNull();
        message.Value.Payload.Length.ShouldBe(numberOfChunks * 10240);
        allocated.ShouldBeLessThan(16 * 1024);
    }

    [Fact]
    public async Task Process_GivenConcurrentProducersOfDistinctMessages_ShouldAssembleEveryMessageCorrectly()
    {
        //Arrange
        using var uut = CreateUut(0, false, 0);
        const int workers = 8;
        const int messages = 200;

        //Act
        var results = await Task.WhenAll(Enumerable.Range(0, workers).Select(worker => Task.Run(() => RunWorker(uut, worker, messages, 5, 4, worker), Current.CancellationToken)));

        //Assert
        results.Sum(result => result.Completed.Count).ShouldBe(workers * messages);
        results.Sum(result => result.Corrupted).ShouldBe(0);
        results.Sum(result => result.Acks.Count + result.Track.Count).ShouldBe(0);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public async Task Process_GivenConcurrentProducersAndExpirySweeps_ShouldAssembleEveryMessageCorrectlyAndKeepAllStateConsistent()
    {
        //Arrange
        using var uut = CreateUut(0, false, 60_000);
        const int workers = 8;
        const int messages = 200;
        using var stop = new CancellationTokenSource();
        var sweeper = Task.Run(() =>
        {
            var acks = new List<MessageIdData>();
            while (!stop.IsCancellationRequested)
            {
                uut.RemoveExpired(acks);
                _ = uut.PendingCount;
                _ = uut.QueueCount;
            }

            return acks.Count;
        }, Current.CancellationToken);

        //Act
        var results = await Task.WhenAll(Enumerable.Range(0, workers).Select(worker => Task.Run(() => RunWorker(uut, worker, messages, 5, 4, worker), Current.CancellationToken)));
        await stop.CancelAsync();
        var swept = await sweeper;

        //Assert
        swept.ShouldBe(0);
        results.Sum(result => result.Completed.Count).ShouldBe(workers * messages);
        results.Sum(result => result.Corrupted).ShouldBe(0);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
    }

    [Fact]
    public async Task Process_GivenConcurrentProducersEvictionsAndExpiry_ShouldAccountForEveryChunkExactlyOnce()
    {
        //Arrange
        const int maxPending = 3;
        using var uut = new ChunkedMessageAssembler(maxPending, true, TimeSpan.FromMilliseconds(2), static () => Stopwatch.GetTimestamp() * 1000 / Stopwatch.Frequency, null);
        const int workers = 8;
        const int messages = 150;
        const int chunks = 5;
        using var stop = new CancellationTokenSource();
        var sweeper = Task.Run(() =>
        {
            var acks = new List<MessageIdData>();
            while (!stop.IsCancellationRequested)
            {
                uut.RemoveExpired(acks);
                Thread.Yield();
            }

            return acks;
        }, Current.CancellationToken);

        //Act
        var results = await Task.WhenAll(Enumerable.Range(0, workers).Select(worker => Task.Run(() => RunWorker(uut, worker, messages, chunks, 8, worker), Current.CancellationToken)));
        await stop.CancelAsync();
        var swept = await sweeper;
        await Task.Delay(20, Current.CancellationToken);
        var flushed = new List<MessageIdData>();
        uut.RemoveExpired(flushed);

        //Assert
        results.Sum(result => result.Corrupted).ShouldBe(0);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
        var accounted = results.SelectMany(result => result.CompletedIds.Concat(result.Acks).Concat(result.Track)).Concat(swept).Concat(flushed).Select(id => (id.LedgerId, id.EntryId)).ToList();
        accounted.Count.ShouldBe(workers * messages * chunks);
        accounted.Distinct().Count().ShouldBe(workers * messages * chunks);
    }

    [Fact]
    public async Task Dispose_GivenConcurrentProducers_ShouldNotThrowAndDropEverything()
    {
        //Arrange
        var uut = CreateUut(0, false, 0);
        var workers = Enumerable.Range(0, 8).Select(worker => Task.Run(() => RunWorker(uut, worker, 500, 5, 4, worker), Current.CancellationToken)).ToArray();
        await Task.Delay(5, Current.CancellationToken);

        //Act
        uut.Dispose();
        var results = await Task.WhenAll(workers);

        //Assert
        results.Sum(result => result.Corrupted).ShouldBe(0);
        uut.PendingCount.ShouldBe(0);
        uut.QueueCount.ShouldBe(0);
        Feed(uut, "late", 0, 2, 1).ShouldBeNull();
    }

    private ChunkedMessageAssembler CreateUut(uint maxPendingChunkedMessage = 10, bool autoAckOldestChunkedMessageOnQueueFull = false, int expireMs = ExpireMs)
        => new(maxPendingChunkedMessage, autoAckOldestChunkedMessageOnQueueFull, TimeSpan.FromMilliseconds(expireMs), () => _now, null);

    // The payload of a chunk is "chunk-{uuid}-{chunkId}|", like the one of the Java tests
    private AssembledChunkedMessage? Feed(ChunkedMessageAssembler uut, string uuid, int chunkId, int numberOfChunks, ulong entry, ulong? publishTime = null)
    {
        var metadata = ChunkTestData.CreateChunkMetadata(uuid, chunkId, numberOfChunks, 100, publishTime ?? (ulong) _now);
        var payload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"chunk-{uuid}-{chunkId}|"));
        return uut.Process(ChunkTestData.CreateEntryId(Ledger, entry), metadata, payload, _acks, _track);
    }

    private AssembledChunkedMessage? Process(ChunkedMessageAssembler uut, string uuid, int chunkId, int numberOfChunks, ulong entry, ReadOnlySequence<byte> payload, int totalChunkMsgSize = 1_000_000)
    {
        var metadata = ChunkTestData.CreateChunkMetadata(uuid, chunkId, numberOfChunks, totalChunkMsgSize, (ulong) _now);
        return uut.Process(ChunkTestData.CreateEntryId(Ledger, entry), metadata, payload, _acks, _track);
    }

    private AssembledChunkedMessage? Process(ChunkedMessageAssembler uut, string uuid, int chunkId, int numberOfChunks, ulong entry, byte[] payload, int totalChunkMsgSize = 1_000_000)
        => Process(uut, uuid, chunkId, numberOfChunks, entry, new ReadOnlySequence<byte>(payload), totalChunkMsgSize);

    private void WarmUp(ChunkedMessageAssembler uut)
    {
        for (var i = 0; i < 3; ++i)
        {
            Process(uut, "warm-up", 0, 2, 1, new byte[1]);
            Process(uut, "warm-up", 1, 2, 2, new byte[1]);
        }

        _acks.Clear();
        _track.Clear();
    }

    // The memory of the chunk is only reachable from the assembler, and not from this method, so it can be collected when the assembler lets go of it
    [MethodImpl(MethodImplOptions.NoInlining)]
    private WeakReference StartMessage(ChunkedMessageAssembler uut, string uuid, ulong entry)
    {
        var chunk = new byte[1024];
        Process(uut, uuid, 0, 3, entry, chunk);
        return new WeakReference(chunk);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private WeakReference CompleteMessage(ChunkedMessageAssembler uut, string uuid, ulong entry)
    {
        var chunk = new byte[1024];
        Process(uut, uuid, 0, 2, entry, chunk);
        var message = Process(uut, uuid, 1, 2, entry + 1, new byte[1024]);
        message.ShouldNotBeNull();
        return new WeakReference(chunk);
    }

    private static bool IsCollected(WeakReference reference)
    {
        for (var i = 0; i < 5 && reference.IsAlive; ++i)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        return !reference.IsAlive;
    }

    private static string Text(AssembledChunkedMessage message)
        => Encoding.UTF8.GetString(message.Payload.ToArray());

    private static ulong[] EntryIds(IEnumerable<MessageIdData> ids)
        => ids.Select(id => id.EntryId).ToArray();

    // Sends the chunks of the messages of one worker. The chunks of the messages in a window of the messages are interleaved, and the chunks of a message are in order.
    private static WorkerResult RunWorker(ChunkedMessageAssembler uut, int worker, int messages, int chunks, int window, int seed)
    {
        var result = new WorkerResult();
        var random = new Random(seed);
        ulong entry = 0;

        for (var start = 0; start < messages; start += window)
        {
            var count = Math.Min(window, messages - start);
            var next = new int[count];

            for (var remaining = count * chunks; remaining > 0; --remaining)
            {
                int i;
                do
                {
                    i = random.Next(count);
                } while (next[i] == chunks);

                var uuid = $"w{worker}-m{start + i}";
                var chunkId = next[i]++;
                var id = new MessageIdData { LedgerId = (ulong) worker, EntryId = entry++ };
                var payload = Encoding.UTF8.GetBytes($"{uuid}:{chunkId};");
                var metadata = ChunkTestData.CreateChunkMetadata(uuid, chunkId, chunks, 100_000);

                var message = uut.Process(id, metadata, new ReadOnlySequence<byte>(payload), result.Acks, result.Track);
                if (message is null)
                    continue;

                var expected = string.Concat(Enumerable.Range(0, chunks).Select(c => $"{uuid}:{c};"));
                if (Encoding.UTF8.GetString(message.Value.Payload.ToArray()) != expected)
                    ++result.Corrupted;

                result.Completed.Add(uuid);
                result.CompletedIds.AddRange(message.Value.ChunkMessageIds);
            }
        }

        return result;
    }

    private sealed class WorkerResult
    {
        public List<string> Completed { get; } = [];
        public List<MessageIdData> CompletedIds { get; } = [];
        public List<MessageIdData> Acks { get; } = [];
        public List<MessageIdData> Track { get; } = [];
        public int Corrupted { get; set; }
    }
}
