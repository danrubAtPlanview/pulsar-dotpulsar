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
using System.Collections.Concurrent;

[Trait("Category", "Unit")]
public sealed class UnAckedMessageTrackerTests
{
    private static ValueTask NoRedelivery(MessageId[] messageIds, CancellationToken cancellationToken)
        => default;

    private static UnAckedMessageTracker CreateTracker(TimeSpan ackTimeout, TimeSpan tickTime)
        => new(ackTimeout, tickTime, NoRedelivery);

    // Upstream: UnAckedMessageTrackerTest#testAddAndRemove
    [Fact]
    public void Add_GivenSameMessageIdTwice_ShouldTrackItOnceAndRemoveIt()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromMilliseconds(1000000), TimeSpan.FromMilliseconds(100000));
        uut.Dispose();
        uut.IsEmpty.ShouldBeTrue();
        uut.Count.ShouldBe(0);
        var messageId = new MessageId(1, 1, -1, -1);

        //Act
        var firstAdd = uut.Add(messageId);
        var secondAdd = uut.Add(messageId);
        var countAfterAdd = uut.Count;
        var expired = uut.Tick();
        var thirdAdd = uut.Add(messageId);
        var countAfterTick = uut.Count;
        var removed = uut.Remove(messageId);

        //Assert
        firstAdd.ShouldBeTrue();
        secondAdd.ShouldBeFalse();
        countAfterAdd.ShouldBe(1);
        expired.ShouldBeNull();
        thirdAdd.ShouldBeFalse();
        countAfterTick.ShouldBe(1);
        removed.ShouldBeTrue();
        uut.IsEmpty.ShouldBeTrue();
        uut.Count.ShouldBe(0);
    }

    // Upstream: UnAckedMessageTrackerTest#testTrackChunkedMessageId (adapted: the chunk ids are not part of the message id yet, so only the tracked id of the last chunk is verified)
    [Fact]
    public void Tick_GivenTrackedMessageIdExpired_ShouldReturnTheTrackedInstanceAndUntrackIt()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(1000));
        uut.IsEmpty.ShouldBeTrue();
        var messageId = new MessageId(1, 4, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var first = uut.Tick();
        var second = uut.Tick();

        //Assert
        first.ShouldBeNull();
        second.ShouldNotBeNull();
        second.Length.ShouldBe(1);
        second[0].ShouldBeSameAs(messageId);
        uut.Count.ShouldBe(0);
    }

    [Fact]
    public void Tick_GivenAckTimeoutTwiceTickTime_ShouldExpireOnThirdTick()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var first = uut.Tick();
        var second = uut.Tick();
        var third = uut.Tick();

        //Assert
        first.ShouldBeNull();
        second.ShouldBeNull();
        third.ShouldBe([messageId]);
        uut.Count.ShouldBe(0);
    }

    [Fact]
    public void Tick_GivenAckTimeoutNotAMultipleOfTickTime_ShouldRoundTheNumberOfTicksUp()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(2500), TimeSpan.FromSeconds(1));
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        var ticks = Enumerable.Range(0, 4).Select(_ => uut.Tick()).ToArray();

        //Assert
        ticks[0].ShouldBeNull();
        ticks[1].ShouldBeNull();
        ticks[2].ShouldBeNull();
        ticks[3].ShouldBe([messageId]);
    }

    [Fact]
    public void Tick_GivenMessagesInSeveralBuckets_ShouldExpireEachBucketInItsOwnTurn()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var first = new MessageId(1, 1, -1, -1);
        var second = new MessageId(1, 2, -1, -1);
        var third = new MessageId(1, 3, -1, -1);
        var fourth = new MessageId(1, 4, -1, -1);
        _ = uut.Add(first);
        _ = uut.Add(second);

        //Act
        var firstTick = uut.Tick();
        _ = uut.Add(third);
        var secondTick = uut.Tick();
        _ = uut.Add(fourth);
        var thirdTick = uut.Tick();
        var fourthTick = uut.Tick();
        var fifthTick = uut.Tick();

        //Assert
        firstTick.ShouldBeNull();
        secondTick.ShouldBeNull();
        thirdTick.ShouldNotBeNull();
        thirdTick.ShouldBe([first, second], ignoreOrder: true);
        fourthTick.ShouldBe([third]);
        fifthTick.ShouldBe([fourth]);
    }

    [Fact]
    public void Tick_GivenNothingTracked_ShouldReturnNull()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));

        //Act
        var actual = uut.Tick();

        //Assert
        actual.ShouldBeNull();
    }

    [Fact]
    public void Constructor_GivenTickTimeGreaterThanAckTimeout_ShouldUseAckTimeoutAsTick()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        using var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), NoRedelivery, timers.Create);
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);

        //Act
        uut.Start();
        var first = uut.Tick();
        var second = uut.Tick();

        //Assert
        timers.Timer.Scheduled.ShouldBe([TimeSpan.FromSeconds(1)]);
        first.ShouldBeNull();
        second.ShouldBe([messageId]);
    }

    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1000, 0)]
    [InlineData(-1000, 1000)]
    [InlineData(1000, -100)]
    [InlineData(0, 0)]
    public void Constructor_GivenAckTimeoutOrTickTimeBelowOneMillisecond_ShouldThrow(int ackTimeoutMs, int tickTimeMs)
    {
        //Arrange
        var ackTimeout = TimeSpan.FromMilliseconds(ackTimeoutMs);
        var tickTime = TimeSpan.FromMilliseconds(tickTimeMs);

        //Act
        var exception = Record.Exception(() => CreateTracker(ackTimeout, tickTime));

        //Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Add_GivenBatchMessageIdsOfSameEntry_ShouldTrackEntryOnce()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));

        //Act
        var first = uut.Add(new MessageId(1, 1, -1, 0));
        var second = uut.Add(new MessageId(1, 1, -1, 1));
        var removed = uut.Remove(new MessageId(1, 1, -1, -1));

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeFalse();
        removed.ShouldBeTrue();
        uut.Count.ShouldBe(0);
    }

    [Fact]
    public void Add_GivenAlreadyTrackedMessageId_ShouldNotMoveItToNewestBucket()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        _ = uut.Tick();

        //Act
        var added = uut.Add(messageId);
        var second = uut.Tick();
        var third = uut.Tick();

        //Assert
        added.ShouldBeFalse();
        second.ShouldBeNull();
        third.ShouldBe([messageId]);
    }

    [Fact]
    public void Add_GivenExpiredMessageReAdded_ShouldTrackInNewestBucket()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        _ = uut.Tick();
        _ = uut.Tick();
        _ = uut.Tick();

        //Act
        var added = uut.Add(messageId);
        var first = uut.Tick();
        var second = uut.Tick();
        var third = uut.Tick();

        //Assert
        added.ShouldBeTrue();
        first.ShouldBeNull();
        second.ShouldBeNull();
        third.ShouldBe([messageId]);
    }

    [Fact]
    public void Remove_GivenMessageIdInOlderBucket_ShouldNotExpireIt()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var messageId = new MessageId(1, 1, -1, -1);
        _ = uut.Add(messageId);
        _ = uut.Tick();

        //Act
        var removed = uut.Remove(messageId);
        var second = uut.Tick();
        var third = uut.Tick();

        //Assert
        removed.ShouldBeTrue();
        second.ShouldBeNull();
        third.ShouldBeNull();
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void Remove_GivenLedgerIdAndEntryId_ShouldRemoveEntry()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        _ = uut.Add(new MessageId(1, 1, -1, 3));

        //Act
        var removed = uut.Remove(1, 1);
        var removedAgain = uut.Remove(1, 1);

        //Assert
        removed.ShouldBeTrue();
        removedAgain.ShouldBeFalse();
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void Remove_GivenUntrackedMessageId_ShouldReturnFalse()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        _ = uut.Add(new MessageId(1, 1, -1, -1));

        //Act
        var removed = uut.Remove(new MessageId(1, 2, -1, -1));

        //Assert
        removed.ShouldBeFalse();
        uut.Count.ShouldBe(1);
    }

    [Fact]
    public void RemoveMessagesTill_GivenBatchMessageId_ShouldRemoveEntriesUpToAndIncludingItsEntry()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
        for (ulong entryId = 1; entryId <= 6; ++entryId)
            _ = uut.Add(new MessageId(1, entryId, -1, -1));
        _ = uut.Add(new MessageId(2, 0, -1, -1));

        //Act
        var removed = uut.RemoveMessagesTill(new MessageId(1, 5, -1, 2));

        //Assert
        removed.ShouldBe(5);
        uut.Count.ShouldBe(2);
        uut.Remove(1, 6).ShouldBeTrue();
        uut.Remove(2, 0).ShouldBeTrue();
    }

    [Fact]
    public void RemoveMessagesTill_GivenEntriesInSeveralBuckets_ShouldNotExpireTheRemovedEntries()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var kept = new MessageId(1, 3, -1, -1);
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        _ = uut.Tick();
        _ = uut.Add(new MessageId(1, 2, -1, -1));
        _ = uut.Add(kept);

        //Act
        var removed = uut.RemoveMessagesTill(1, 2);
        var ticks = Enumerable.Range(0, 3).Select(_ => uut.Tick()).ToArray();

        //Assert
        removed.ShouldBe(2);
        ticks[0].ShouldBeNull();
        ticks[1].ShouldBeNull();
        ticks[2].ShouldBe([kept]);
    }

    [Fact]
    public void RemoveMessagesTill_GivenNothingTracked_ShouldReturnZero()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));

        //Act
        var removed = uut.RemoveMessagesTill(new MessageId(1, 1, -1, -1));

        //Assert
        removed.ShouldBe(0);
    }

    [Fact]
    public void Clear_GivenTrackedMessages_ShouldBeEmpty()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        _ = uut.Tick();
        _ = uut.Add(new MessageId(1, 2, -1, -1));

        //Act
        uut.Clear();

        //Assert
        uut.IsEmpty.ShouldBeTrue();
        uut.Count.ShouldBe(0);
        uut.Tick().ShouldBeNull();
        uut.Tick().ShouldBeNull();
        uut.Tick().ShouldBeNull();
    }

    [Fact]
    public void Start_GivenNewTracker_ShouldNotCreateTimerBeforeStart()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();

        //Act
        using var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), NoRedelivery, timers.Create);

        //Assert
        timers.Created.ShouldBe(0);
    }

    [Fact]
    public void Start_GivenNewTracker_ShouldScheduleTheFirstTickOneTickAhead()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        using var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(500), NoRedelivery, timers.Create);

        //Act
        uut.Start();

        //Assert
        timers.Timer.Scheduled.ShouldBe([TimeSpan.FromMilliseconds(500)]);
    }

    [Fact]
    public void Start_GivenCalledTwice_ShouldCreateOneTimer()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        using var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), NoRedelivery, timers.Create);

        //Act
        uut.Start();
        uut.Start();

        //Assert
        timers.Created.ShouldBe(1);
        timers.Timer.Scheduled.Length.ShouldBe(1);
    }

    // Upstream: UnAckedMessageTracker#TimerTask.run
    [Fact]
    public void Start_GivenUnacknowledgedMessage_ShouldInvokeRedeliverAfterTimeout()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var redelivered = new List<MessageId[]>();
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(100),
            (messageIds, _) =>
            {
                redelivered.Add(messageIds);
                return default;
            },
            timers.Create);
        var messageId = new MessageId(1, 1, -1, -1);
        uut.Start();
        _ = uut.Add(messageId);

        //Act
        for (var i = 0; i < 10; ++i)
            timers.Timer.Fire();
        var redeliveredBeforeTimeout = redelivered.Count;
        timers.Timer.Fire();

        //Assert
        redeliveredBeforeTimeout.ShouldBe(0);
        redelivered.Count.ShouldBe(1);
        redelivered[0].ShouldBe([messageId]);
        uut.Count.ShouldBe(0);
        timers.Timer.Scheduled.ShouldAllBe(dueTime => dueTime == TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void Start_GivenAcknowledgedMessage_ShouldNotInvokeRedeliver()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var redeliveries = 0;
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(100),
            (_, _) =>
            {
                ++redeliveries;
                return default;
            },
            timers.Create);
        var messageId = new MessageId(1, 1, -1, -1);
        uut.Start();
        _ = uut.Add(messageId);

        //Act
        _ = uut.Remove(messageId);
        for (var i = 0; i < 30; ++i)
            timers.Timer.Fire();

        //Assert
        redeliveries.ShouldBe(0);
    }

    [Fact]
    public void Start_GivenExpiredMessages_ShouldRescheduleTimerBeforeRedelivering()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        TimeSpan? pendingDuringRedelivery = null;
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            (_, _) =>
            {
                pendingDuringRedelivery = timers.Timer.Pending;
                return default;
            },
            timers.Create);
        uut.Start();
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        timers.Timer.Fire();

        //Act
        timers.Timer.Fire();

        //Assert
        pendingDuringRedelivery.ShouldBe(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Start_GivenRedeliverThrows_ShouldKeepTicking()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var redeliveries = 0;
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            (_, _) =>
            {
                ++redeliveries;
                throw new InvalidOperationException("Redelivery failed");
            },
            timers.Create);
        uut.Start();
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        timers.Timer.Fire();
        timers.Timer.Fire();
        _ = uut.Add(new MessageId(1, 2, -1, -1));

        //Act
        timers.Timer.Fire();
        timers.Timer.Fire();

        //Assert
        redeliveries.ShouldBe(2);
        timers.Timer.Pending.ShouldBe(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Start_GivenRedeliverFaults_ShouldKeepTicking()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var redeliveries = 0;
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            (_, _) =>
            {
                ++redeliveries;
                return new ValueTask(Task.FromException(new InvalidOperationException("Redelivery failed")));
            },
            timers.Create);
        uut.Start();
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        timers.Timer.Fire();
        timers.Timer.Fire();
        _ = uut.Add(new MessageId(1, 2, -1, -1));

        //Act
        timers.Timer.Fire();
        timers.Timer.Fire();

        //Assert
        redeliveries.ShouldBe(2);
        timers.Timer.Pending.ShouldBe(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Start_GivenSlowRedeliver_ShouldNotDelayTheNextTick()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var redeliveries = 0;
        var neverCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            (_, _) =>
            {
                ++redeliveries;
                return new ValueTask(neverCompleted.Task);
            },
            timers.Create);
        uut.Start();
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        timers.Timer.Fire();

        //Act
        timers.Timer.Fire();
        _ = uut.Add(new MessageId(1, 2, -1, -1));
        timers.Timer.Fire();
        timers.Timer.Fire();

        //Assert
        redeliveries.ShouldBe(2);
        timers.Timer.Pending.ShouldBe(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Dispose_GivenStartedTracker_ShouldNotRedeliver()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var redeliveries = 0;
        var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            (_, _) =>
            {
                ++redeliveries;
                return default;
            },
            timers.Create);
        uut.Start();
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        timers.Timer.Fire();

        //Act
        uut.Dispose();
        timers.Timer.FireWithoutSchedule();
        timers.Timer.FireWithoutSchedule();

        //Assert
        redeliveries.ShouldBe(0);
        timers.Timer.IsDisposed.ShouldBeTrue();
        timers.Timer.Pending.ShouldBeNull();
        timers.Timer.Scheduled.Length.ShouldBe(2);
    }

    [Fact]
    public void Dispose_GivenTrackedMessages_ShouldClearTheTracker()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        _ = uut.Add(new MessageId(1, 1, -1, -1));

        //Act
        uut.Dispose();

        //Assert
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void Dispose_GivenCalledTwice_ShouldNotThrow()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), NoRedelivery, timers.Create);
        uut.Start();

        //Act
        var exception = Record.Exception(() =>
        {
            uut.Dispose();
            uut.Dispose();
        });

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public void Dispose_GivenRedeliverInFlight_ShouldCancelTheToken()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var token = CancellationToken.None;
        var neverCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var uut = new UnAckedMessageTracker(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            (_, cancellationToken) =>
            {
                token = cancellationToken;
                return new ValueTask(neverCompleted.Task);
            },
            timers.Create);
        uut.Start();
        _ = uut.Add(new MessageId(1, 1, -1, -1));
        timers.Timer.Fire();
        timers.Timer.Fire();
        token.CanBeCanceled.ShouldBeTrue();
        token.IsCancellationRequested.ShouldBeFalse();

        //Act
        uut.Dispose();

        //Assert
        token.IsCancellationRequested.ShouldBeTrue();
    }

    [Fact]
    public void Start_GivenDisposedTracker_ShouldNotCreateTimer()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), NoRedelivery, timers.Create);
        uut.Dispose();

        //Act
        uut.Start();

        //Assert
        timers.Created.ShouldBe(0);
    }

    [Fact]
    public async Task Start_GivenRealTimer_ShouldInvokeRedeliverAfterTimeout()
    {
        //Arrange
        var redelivered = new TaskCompletionSource<MessageId[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var uut = new UnAckedMessageTracker(
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromMilliseconds(50),
            (messageIds, _) =>
            {
                redelivered.TrySetResult(messageIds);
                return default;
            });
        var messageId = new MessageId(1, 1, -1, -1);
        uut.Start();
        _ = uut.Add(messageId);

        //Act
        var actual = await redelivered.Task.WaitAsync(TimeSpan.FromSeconds(30), Current.CancellationToken);

        //Assert
        actual.ShouldBe([messageId]);
        uut.Count.ShouldBe(0);
    }

    [Fact]
    public async Task AddRemoveAndTick_GivenConcurrentUse_ShouldAccountForEveryMessageExactlyOnce()
    {
        //Arrange
        const int adders = 4;
        const int entriesPerAdder = 5000;
        using var uut = CreateTracker(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1));
        using var stop = new CancellationTokenSource();
        var added = 0;
        var removed = 0;
        var removedTill = 0;
        var expired = new ConcurrentBag<MessageId>();

        //Act
        var addTasks = Enumerable.Range(1, adders).Select(ledgerId => Run(() =>
        {
            for (var entryId = 0; entryId < entriesPerAdder; ++entryId)
            {
                if (uut.Add(new MessageId((ulong) ledgerId, (ulong) entryId, -1, -1)))
                    Interlocked.Increment(ref added);
            }
        })).ToArray();

        var removeTask = Run(() =>
        {
            for (var entryId = 0; entryId < entriesPerAdder; entryId += 2)
            {
                for (var ledgerId = 1; ledgerId <= adders; ++ledgerId)
                {
                    if (uut.Remove((ulong) ledgerId, (ulong) entryId))
                        Interlocked.Increment(ref removed);
                }
            }
        });

        var tillTask = Run(() =>
        {
            while (!stop.IsCancellationRequested)
                _ = Interlocked.Add(ref removedTill, uut.RemoveMessagesTill(2, entriesPerAdder / 2));
        });

        var tickTask = Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                var messageIds = uut.Tick();

                if (messageIds is null)
                    continue;

                foreach (var messageId in messageIds)
                    expired.Add(messageId);
            }
        });

        await Task.WhenAll(addTasks.Append(removeTask));
        stop.Cancel();
        await Task.WhenAll(tillTask, tickTask);

        //Assert
        added.ShouldBe(adders * entriesPerAdder);
        expired.Select(messageId => (messageId.LedgerId, messageId.EntryId)).Distinct().Count().ShouldBe(expired.Count);
        (removed + removedTill + expired.Count + uut.Count).ShouldBe(added);
    }

    [Fact]
    public async Task AddAndClear_GivenConcurrentUse_ShouldKeepTheTrackerConsistent()
    {
        //Arrange
        const int entries = 20000;
        using var uut = CreateTracker(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1));
        using var stop = new CancellationTokenSource();
        var clearTask = Run(() =>
        {
            while (!stop.IsCancellationRequested)
                uut.Clear();
        });

        //Act
        var addTask = Run(() =>
        {
            for (var entryId = 0; entryId < entries; ++entryId)
                _ = uut.Add(new MessageId(1, (ulong) entryId, -1, -1));
        });

        await addTask;
        stop.Cancel();
        await clearTask;
        uut.Clear();

        //Assert
        uut.IsEmpty.ShouldBeTrue();
        uut.Tick().ShouldBeNull();
        uut.Tick().ShouldBeNull();
        uut.Tick().ShouldBeNull();
        uut.Tick().ShouldBeNull();
    }

    [Fact]
    public async Task AddRemoveAndDispose_GivenConcurrentUse_ShouldNotThrow()
    {
        //Arrange
        var timers = new FakeOneShotTimerFactory();
        var uut = new UnAckedMessageTracker(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100), NoRedelivery, timers.Create);
        uut.Start();

        //Act
        var tasks = Enumerable.Range(1, 4).Select(ledgerId => Run(() =>
        {
            for (var entryId = 0; entryId < 5000; ++entryId)
            {
                _ = uut.Add(new MessageId((ulong) ledgerId, (ulong) entryId, -1, -1));
                _ = uut.Remove((ulong) ledgerId, (ulong) entryId);
                _ = timers.Timer.TryFire();
            }
        })).ToArray();
        var disposeTask = Run(() => uut.Dispose());
        var exception = await Record.ExceptionAsync(() => Task.WhenAll(tasks.Append(disposeTask)));

        //Assert
        exception.ShouldBeNull();
        uut.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public void AddAndRemove_GivenSteadyState_ShouldNotAllocate()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var messageIds = CreateMessageIds(100);
        for (var round = 0; round < 10; ++round)
            AddAndRemove(uut, messageIds);

        //Act
        var allocated = MeasureAllocatedBytes(() =>
        {
            for (var round = 0; round < 100; ++round)
                AddAndRemove(uut, messageIds);
        });

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public void RemoveMessagesTill_GivenSteadyState_ShouldNotAllocate()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        var messageIds = CreateMessageIds(100);
        for (var round = 0; round < 10; ++round)
            AddAndRemoveTill(uut, messageIds);

        //Act
        var allocated = MeasureAllocatedBytes(() =>
        {
            for (var round = 0; round < 100; ++round)
                AddAndRemoveTill(uut, messageIds);
        });

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public void Tick_GivenNothingExpired_ShouldNotAllocate()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));
        for (var round = 0; round < 10; ++round)
            _ = uut.Tick();

        //Act
        var allocated = MeasureAllocatedBytes(() =>
        {
            for (var round = 0; round < 100; ++round)
                _ = uut.Tick();
        });

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public void Tick_GivenExpiredMessages_ShouldOnlyAllocateTheResult()
    {
        //Arrange
        const int count = 100;
        using var uut = CreateTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        var messageIds = CreateMessageIds(count);
        MessageId[]? expired = null;
        for (var round = 0; round < 10; ++round)
        {
            AddAll(uut, messageIds);
            _ = uut.Tick();
            _ = uut.Tick();
        }

        //Act
        var allocated = MeasureAllocatedBytes(() =>
        {
            AddAll(uut, messageIds);
            _ = uut.Tick();
            expired = uut.Tick();
        });

        //Assert
        expired.ShouldNotBeNull();
        expired.Length.ShouldBe(count);
        allocated.ShouldBeGreaterThan(count * IntPtr.Size);
        allocated.ShouldBeLessThanOrEqualTo((count * IntPtr.Size) + 64);
    }

    private static Task Run(Action action)
        => Task.Run(action, Current.CancellationToken);

    // The smallest of a few attempts, so that a one-time allocation of the runtime (tiered compilation, a test listener) is not mistaken for an allocation of the code under test
    private static long MeasureAllocatedBytes(Action action)
    {
        var minimum = long.MaxValue;

        for (var attempt = 0; attempt < 5; ++attempt)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            action();
            minimum = Math.Min(minimum, GC.GetAllocatedBytesForCurrentThread() - before);
        }

        return minimum;
    }

    private static MessageId[] CreateMessageIds(int count)
        => Enumerable.Range(0, count).Select(entryId => new MessageId(1, (ulong) entryId, -1, -1)).ToArray();

    private static void AddAll(UnAckedMessageTracker uut, MessageId[] messageIds)
    {
        for (var i = 0; i < messageIds.Length; ++i)
            _ = uut.Add(messageIds[i]);
    }

    private static void AddAndRemove(UnAckedMessageTracker uut, MessageId[] messageIds)
    {
        AddAll(uut, messageIds);

        if (uut.Count != messageIds.Length || uut.IsEmpty)
            throw new InvalidOperationException("Unexpected count");

        for (var i = 0; i < messageIds.Length; i += 2)
            _ = uut.Remove(messageIds[i]);

        for (var i = 1; i < messageIds.Length; i += 2)
            _ = uut.Remove(1, (ulong) i);
    }

    private static void AddAndRemoveTill(UnAckedMessageTracker uut, MessageId[] messageIds)
    {
        AddAll(uut, messageIds);

        if (uut.RemoveMessagesTill(messageIds[messageIds.Length / 2]) != (messageIds.Length / 2) + 1)
            throw new InvalidOperationException("Unexpected number of removed messages");

        _ = uut.RemoveMessagesTill(1, (ulong) messageIds.Length);
    }
}
