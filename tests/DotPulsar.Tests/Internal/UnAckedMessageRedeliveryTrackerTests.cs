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

using DotPulsar.Abstractions;
using DotPulsar.Internal;

// The time of these tests is a fake monotonic clock that moves with the ticks, and a fake timer that fires when the test says so. Nothing in them waits.
[Trait("Category", "Unit")]
public sealed class UnAckedMessageRedeliveryTrackerTests
{
    private static readonly TimeSpan _second = TimeSpan.FromSeconds(1);

    // A backoff that returns the delay for the count of the redeliveries that it is asked for, and remembers the counts
    private sealed class FakeBackoff : IRedeliveryBackoff
    {
        private readonly Func<uint, TimeSpan> _delay;
        private readonly List<uint> _counts;

        public FakeBackoff(Func<uint, TimeSpan> delay)
        {
            _delay = delay;
            _counts = [];
        }

        public uint[] Counts
        {
            get
            {
                lock (_counts)
                    return [.. _counts];
            }
        }

        public TimeSpan Next(uint redeliveryCount)
        {
            lock (_counts)
                _counts.Add(redeliveryCount);

            return _delay(redeliveryCount);
        }
    }

    private static FakeBackoff Constant(TimeSpan delay)
        => new(_ => delay);

    private static ValueTask NoRedelivery(MessageId[] messageIds, CancellationToken cancellationToken)
        => default;

    private static UnAckedMessageRedeliveryTracker CreateTracker(TimeSpan ackTimeout, TimeSpan tickTime, IRedeliveryBackoff backoff, FakeMonotonicClock clock, bool countRedeliveriesLocally = false, FakeOneShotTimerFactory? timers = null, Func<MessageId[], CancellationToken, ValueTask>? redeliver = null)
        => new(ackTimeout, tickTime, backoff, countRedeliveriesLocally, redeliver ?? NoRedelivery, () => clock.Now, timers is null ? null : timers.Create);

    // A tick of the timer: the time passes, and the tracker is ticked
    private static MessageId[]? Tick(UnAckedMessageRedeliveryTracker uut, FakeMonotonicClock clock, TimeSpan tickTime)
    {
        clock.Advance(tickTime);
        return uut.Tick();
    }

    // Upstream: UnAckedTopicMessageRedeliveryTrackerTest#testLocalRedeliveryCountForExclusiveSubscription (adapted: the waits for the timer are ticks of a fake clock, the three message ids are entries,
    // and the removal of a topic is a removal of an entry, because there is a tracker for every topic)
    [Fact]
    public void GetLocalRedeliveryCount_GivenSubscriptionThatIsNotShared_ShouldCountExpiriesAndKeepCountsUntilTheEntryIsRemoved()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var tick = TimeSpan.FromMilliseconds(10);
        // A long delay keeps the expired entries from being redelivered
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(100), tick, Constant(TimeSpan.FromSeconds(60)), clock, countRedeliveriesLocally: true);
        var msg1 = new MessageId(1, 0, -1, -1);
        var msg2 = new MessageId(1, 1, -1, -1);
        var msg3 = new MessageId(1, 2, -1, -1);
        var ticksToExpire = 11;
        _ = uut.Add(msg1);
        _ = uut.Add(msg2);
        _ = uut.Add(msg3);

        //Act
        for (var i = 0; i < ticksToExpire; ++i)
            _ = Tick(uut, clock, tick);

        var countsAfterFirstExpiry = new[] { msg1, msg2, msg3 }.Select(id => uut.GetLocalRedeliveryCount(id.LedgerId, id.EntryId)).ToArray();

        // The redelivery of a consumer that is not Shared clears the tracker, but the counts must survive it
        uut.Clear();
        _ = uut.Add(msg1);
        _ = uut.Add(msg2);
        _ = uut.Add(msg3);

        for (var i = 0; i < ticksToExpire; ++i)
            _ = Tick(uut, clock, tick);

        var countAfterSecondExpiry = uut.GetLocalRedeliveryCount(msg1.LedgerId, msg1.EntryId);
        _ = uut.RemoveMessagesTill(msg1);
        var countAfterRemoveMessagesTill = uut.GetLocalRedeliveryCount(msg1.LedgerId, msg1.EntryId);
        _ = uut.Remove(msg2);
        var countAfterRemove = uut.GetLocalRedeliveryCount(msg2.LedgerId, msg2.EntryId);

        //Assert
        countsAfterFirstExpiry.ShouldBe([1u, 1u, 1u]);
        countAfterSecondExpiry.ShouldBe(2u);
        countAfterRemoveMessagesTill.ShouldBe(0u);
        countAfterRemove.ShouldBe(0u);
        uut.GetLocalRedeliveryCount(msg3.LedgerId, msg3.EntryId).ShouldBe(2u);
    }

    // Upstream: UnAckedTopicMessageRedeliveryTrackerTest#testNoLocalRedeliveryCountForSharedSubscription (adapted: the wait for the entry to be in the entries that wait for the delay is the ticks of a fake clock)
    [Fact]
    public void GetLocalRedeliveryCount_GivenSharedSubscription_ShouldNotCountExpiries()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var tick = TimeSpan.FromMilliseconds(10);
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(100), tick, Constant(TimeSpan.FromSeconds(60)), clock, countRedeliveriesLocally: false);
        var messageId = new MessageId(1, 0, -1, -1);
        _ = uut.Add(messageId);

        //Act
        for (var i = 0; i < 11; ++i)
            _ = Tick(uut, clock, tick);

        //Assert
        uut.Count.ShouldBe(1);
        uut.GetLocalRedeliveryCount(messageId.LedgerId, messageId.EntryId).ShouldBe(0u);
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testCheckUnAcknowledgedMessageRedeliveryTimer (the delay: the ack timeout and then the delay of the backoff)
    [Fact]
    public void Tick_GivenExpiredEntryAndDelay_ShouldReturnItWhenTheDelayHasPassed()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), _second, Constant(TimeSpan.FromSeconds(2)), clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 5).Select(_ => (Result: Tick(uut, clock, _second), Count: uut.Count)).ToArray();

        //Assert
        // The entry expires on the third tick, which is after the ack timeout of 2 seconds, and is due 2 seconds later
        ticks.Select(tick => tick.Result is not null).ShouldBe([false, false, false, false, true]);
        ticks[4].Result.ShouldBe([messageId]);
        ticks.Select(tick => tick.Count).ShouldBe([1, 1, 1, 1, 0]);
        uut.IsEmpty.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenExpiredEntryAndNoDelay_ShouldReturnItOnTheTickItExpires()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), _second, Constant(TimeSpan.Zero), clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 3).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        ticks[0].ShouldBeNull();
        ticks[1].ShouldBeNull();
        ticks[2].ShouldBe([messageId]);
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenDelayThatIsNotAMultipleOfTheTickTime_ShouldReturnTheEntryOnTheFirstTickAfterTheDelay()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromMilliseconds(1500)), clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 5).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        // The entry expires on the second tick (2 s), is due at 3.5 s, and is returned by the tick at 4 s
        ticks.Select(tick => tick is not null).ShouldBe([false, false, false, true, false]);
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenNothingTracked_ShouldReturnNullAndNotReadTheClock()
    {
        //Arrange
        var reads = 0;
        using var uut = new UnAckedMessageRedeliveryTracker(TimeSpan.FromSeconds(2), _second, Constant(_second), false, NoRedelivery, () =>
        {
            reads++;
            return TimeSpan.Zero;
        });

        //Act
        var actual = uut.Tick();

        //Assert
        actual.ShouldBeNull();
        reads.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenEntriesWithDifferentRedeliveryCounts_ShouldAskTheBackoffForEachCountAndReturnEachWhenItIsDue()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var backoff = new FakeBackoff(count => TimeSpan.FromSeconds(count + 1));
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, backoff, clock);
        var first = new MessageId(1, 1, -1, -1);
        var second = new MessageId(1, 2, -1, -1);
        var third = new MessageId(1, 3, -1, -1);
        _ = uut.Add(first, 0);
        _ = uut.Add(second, 1);
        _ = uut.Add(third, 3);

        //Act
        var ticks = Enumerable.Range(0, 6).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        // They expire on the second tick (2 s). The delays are 1, 2 and 4 seconds, so they are due at 3, 4 and 6 seconds.
        backoff.Counts.Order().ShouldBe([0u, 1u, 3u]);
        ticks.Select(tick => tick?.Length ?? 0).ShouldBe([0, 0, 1, 1, 0, 1]);
        ticks[2].ShouldBe([first]);
        ticks[3].ShouldBe([second]);
        ticks[5].ShouldBe([third]);
        uut.Count.ShouldBe(0);
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testCheckUnAcknowledgedMessageRedeliveryTimer (the delay of a Shared subscription is for the count that the broker sends with the message)
    [Fact]
    public void Tick_GivenSharedSubscription_ShouldAskTheBackoffForTheRedeliveryCountOfTheMessage()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var backoff = Constant(TimeSpan.Zero);
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, backoff, clock, countRedeliveriesLocally: false);
        var messageId = new MessageId(1, 1, -1, -1);

        //Act
        for (uint redeliveryCount = 1; redeliveryCount <= 3; ++redeliveryCount)
        {
            _ = uut.Add(messageId, redeliveryCount);
            _ = Tick(uut, clock, _second);
            _ = Tick(uut, clock, _second);
        }

        //Assert
        backoff.Counts.ShouldBe([1u, 2u, 3u]);
        uut.GetLocalRedeliveryCount(messageId.LedgerId, messageId.EntryId).ShouldBe(0u);
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testCheckUnAcknowledgedMessageRedeliveryTimer (the delay of a subscription that is not Shared is for the count of the redeliveries that the tracker has done)
    [Fact]
    public void Tick_GivenSubscriptionThatIsNotShared_ShouldAskTheBackoffForTheNumberOfTimesTheEntryHasExpired()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var backoff = Constant(TimeSpan.Zero);
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, backoff, clock, countRedeliveriesLocally: true);
        var messageId = new MessageId(1, 1, -1, -1);

        //Act
        for (var i = 0; i < 3; ++i)
        {
            // The broker does not count the redeliveries of this subscription, so the count of the message is 0
            _ = uut.Add(messageId, 0);
            _ = Tick(uut, clock, _second);
            _ = Tick(uut, clock, _second);
        }

        //Assert
        backoff.Counts.ShouldBe([0u, 1u, 2u]);
        uut.GetLocalRedeliveryCount(messageId.LedgerId, messageId.EntryId).ShouldBe(3u);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenEntryThatIsAlreadyTracked_ShouldKeepTheFirstRedeliveryCountAndTheFirstBucket()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var backoff = Constant(TimeSpan.Zero);
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, backoff, clock);
        var messageId = new MessageId(1, 1, -1, -1);

        //Act
        var first = uut.Add(messageId, 1);
        _ = Tick(uut, clock, _second);
        var second = uut.Add(messageId, 5);
        var expired = Tick(uut, clock, _second);

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeFalse();
        expired.ShouldBe([messageId]);
        backoff.Counts.ShouldBe([1u]);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenBatchMessageIdsOfSameEntry_ShouldTrackEntryOnce()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(_second), clock);

        //Act
        var first = uut.Add(new MessageId(1, 1, -1, 0));
        var second = uut.Add(new MessageId(1, 1, -1, 1));
        var removed = uut.Remove(new MessageId(1, 1, -1, -1));

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeFalse();
        removed.ShouldBeTrue();
        uut.IsEmpty.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenEntryThatWaitsForTheDelay_ShouldNotReturnItAnymore()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromSeconds(2)), clock, countRedeliveriesLocally: true);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        _ = Tick(uut, clock, _second);
        _ = Tick(uut, clock, _second);
        var countWhileWaiting = uut.Count;

        //Act
        var removed = uut.Remove(messageId);
        var ticks = Enumerable.Range(0, 5).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        countWhileWaiting.ShouldBe(1);
        removed.ShouldBeTrue();
        ticks.ShouldAllBe(tick => tick == null);
        uut.IsEmpty.ShouldBeTrue();
        uut.GetLocalRedeliveryCount(messageId.LedgerId, messageId.EntryId).ShouldBe(0u);
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenEntryThatWaitsForTheDelayAndIsTrackedAgain_ShouldRemoveBoth()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromSeconds(10)), clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        _ = Tick(uut, clock, _second);
        _ = Tick(uut, clock, _second);

        // Upstream tracks it again, and an acknowledgment only removed the one in the bucket
        var addedAgain = uut.Add(messageId);
        var countBeforeRemove = uut.Count;

        //Act
        var removed = uut.Remove(messageId);

        //Assert
        addedAgain.ShouldBeTrue();
        countBeforeRemove.ShouldBe(2);
        removed.ShouldBeTrue();
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenEntryThatIsNotTracked_ShouldReturnFalse()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(_second), clock);

        //Act
        var actual = uut.Remove(new MessageId(1, 1, -1, -1));

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void RemoveMessagesTill_GivenEntriesInBucketsAndEntriesThatWaitForTheDelay_ShouldRemoveThoseUpToAndIncludingTheEntry()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromSeconds(10)), clock);
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        _ = uut.Add(new MessageId(1, 5, -1, -1));
        _ = Tick(uut, clock, _second);
        _ = Tick(uut, clock, _second);
        // The first two wait for the delay, and these three are in a bucket
        _ = uut.Add(new MessageId(1, 3, -1, -1));
        _ = uut.Add(new MessageId(1, 6, -1, -1));
        _ = uut.Add(new MessageId(2, 0, -1, -1));

        //Act
        var removed = uut.RemoveMessagesTill(new MessageId(1, 5, -1, 2));

        //Assert
        removed.ShouldBe(3);
        uut.Count.ShouldBe(2);
        uut.Remove(1, 6).ShouldBeTrue();
        uut.Remove(2, 0).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Clear_GivenEntriesInBucketsAndEntriesThatWaitForTheDelay_ShouldEmptyThemAndKeepTheLocalRedeliveryCounts()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromSeconds(10)), clock, countRedeliveriesLocally: true);
        var waiting = new MessageId(1, 1, -1, -1);
        _ = uut.Add(waiting);
        _ = Tick(uut, clock, _second);
        _ = Tick(uut, clock, _second);
        _ = uut.Add(new MessageId(1, 2, -1, -1));

        //Act
        uut.Clear();

        //Assert
        uut.Count.ShouldBe(0);
        uut.IsEmpty.ShouldBeTrue();
        uut.GetLocalRedeliveryCount(waiting.LedgerId, waiting.EntryId).ShouldBe(1u);
        Enumerable.Range(0, 20).Select(_ => Tick(uut, clock, _second)).ShouldAllBe(tick => tick == null);
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenBackoffThatThrows_ShouldReturnTheEntryWithoutDelay()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var backoff = new FakeBackoff(_ => throw new InvalidOperationException("The policy is broken"));
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, backoff, clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 2).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        ticks[0].ShouldBeNull();
        ticks[1].ShouldBe([messageId]);
        backoff.Counts.ShouldBe([0u]);
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenBackoffWithNegativeDelay_ShouldReturnTheEntryWithoutDelay()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromSeconds(-5)), clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 2).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        ticks[1].ShouldBe([messageId]);
    }

    // Additional coverage
    [Fact]
    public void Tick_GivenBackoffWithTheLargestDelay_ShouldKeepTheEntryAndNotOverflow()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.MaxValue), clock);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 3).Select(_ => Tick(uut, clock, _second)).ToArray();
        clock.Advance(TimeSpan.FromDays(365 * 1000));
        var late = uut.Tick();

        //Assert
        ticks.ShouldAllBe(tick => tick == null);
        late.ShouldBeNull();
        uut.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenTickTimeGreaterThanAckTimeout_ShouldUseTheAckTimeoutAsTick()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var timers = new FakeOneShotTimerFactory();
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), Constant(TimeSpan.Zero), clock, timers: timers);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        uut.Start();
        var ticks = Enumerable.Range(0, 2).Select(_ => Tick(uut, clock, _second)).ToArray();

        //Assert
        timers.Timer.Scheduled.ShouldBe([_second]);
        ticks[0].ShouldBeNull();
        ticks[1].ShouldBe([messageId]);
    }

    // Additional coverage
    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1000, 0)]
    public void Constructor_GivenAckTimeoutOrTickTimeBelowOneMillisecond_ShouldThrowArgumentOutOfRangeException(int ackTimeoutMs, int tickTimeMs)
    {
        //Arrange & Act
        var exception = Record.Exception(() => new UnAckedMessageRedeliveryTracker(TimeSpan.FromMilliseconds(ackTimeoutMs), TimeSpan.FromMilliseconds(tickTimeMs), Constant(_second), false, NoRedelivery));

        //Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    // Additional coverage
    [Fact]
    public async Task Start_GivenExpiredEntry_ShouldRedeliverItOnTheTickThatFollowsTheDelay()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var timers = new FakeOneShotTimerFactory();
        var redelivered = new TaskCompletionSource<MessageId[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var invocations = 0;
        CancellationToken token = default;
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(_second), clock, timers: timers, redeliver: (ids, cancellationToken) =>
        {
            Interlocked.Increment(ref invocations);
            token = cancellationToken;
            redelivered.TrySetResult(ids);
            return default;
        });
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        uut.Start();
        uut.Start();
        var createdTimers = timers.Created;
        var firstSchedule = timers.Timer.Scheduled.ToArray();

        //Act
        // The entry expires on the second tick (2 s), and is due on the third tick (3 s)
        clock.Advance(_second);
        timers.Timer.Fire();
        clock.Advance(_second);
        timers.Timer.Fire();
        var invocationsBeforeDue = Volatile.Read(ref invocations);
        clock.Advance(_second);
        timers.Timer.Fire();
        var actual = await redelivered.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        createdTimers.ShouldBe(1);
        firstSchedule.ShouldBe([_second]);
        invocationsBeforeDue.ShouldBe(0);
        actual.ShouldBe([messageId]);
        token.CanBeCanceled.ShouldBeTrue();
        timers.Timer.Scheduled.ShouldBe([_second, _second, _second, _second]);
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenStartedTracker_ShouldNotRedeliver()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var timers = new FakeOneShotTimerFactory();
        var invocations = 0;
        var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.Zero), clock, timers: timers, redeliver: (_, _) =>
        {
            Interlocked.Increment(ref invocations);
            return default;
        });
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        uut.Start();
        clock.Advance(_second);
        timers.Timer.Fire();
        clock.Advance(_second);

        //Act
        uut.Dispose();
        timers.Timer.FireWithoutSchedule();
        uut.Start();

        //Assert
        invocations.ShouldBe(0);
        timers.Timer.IsDisposed.ShouldBeTrue();
        timers.Created.ShouldBe(1);
        uut.IsEmpty.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenLocalRedeliveryCounts_ShouldForgetThem()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        var uut = CreateTracker(TimeSpan.FromSeconds(1), _second, Constant(TimeSpan.FromSeconds(10)), clock, countRedeliveriesLocally: true);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        _ = Tick(uut, clock, _second);
        _ = Tick(uut, clock, _second);
        var before = uut.GetLocalRedeliveryCount(messageId.LedgerId, messageId.EntryId);

        //Act
        uut.Dispose();

        //Assert
        before.ShouldBe(1u);
        uut.GetLocalRedeliveryCount(messageId.LedgerId, messageId.EntryId).ShouldBe(0u);
    }
}
