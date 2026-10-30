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
public sealed class NegativeAcknowledgeTrackerTests
{
    private readonly FakeMonotonicClock _clock;
    private readonly FakeOneShotTimerFactory _timers;
    private readonly ConcurrentQueue<MessageId[]> _redelivered;

    public NegativeAcknowledgeTrackerTests()
    {
        _clock = new FakeMonotonicClock();
        _timers = new FakeOneShotTimerFactory();
        _redelivered = new ConcurrentQueue<MessageId[]>();
    }

    // Additional coverage
    [Theory]
    [InlineData(0, 100)]
    [InlineData(1, 100)]
    [InlineData(99, 100)]
    [InlineData(100, 100)]
    [InlineData(101, 101)]
    [InlineData(1000, 1000)]
    [InlineData(60000, 60000)]
    public void Add_GivenDelay_ShouldScheduleTimerForTheDelayButAtLeast100Milliseconds(int delayMs, int expectedMs)
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(delayMs));

        //Act
        _ = uut.Add(new MessageId(1, 1, -1, -1));

        //Assert
        _timers.Timer.Scheduled.ShouldBe([TimeSpan.FromMilliseconds(expectedMs)]);
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenTracker_ShouldNotCreateTimerBeforeFirstAdd()
    {
        //Arrange & Act
        using var uut = CreateTracker(TimeSpan.FromSeconds(1));

        //Assert
        _timers.Created.ShouldBe(0);
    }

    // Upstream: NegativeAcksTest#testNegativeAcksDeleteFromUnackedTracker (adapted: the sequence of the negative acknowledge of the sub-consumer, on the two trackers)
    [Fact]
    public void Add_GivenBatchMessageIdsOfSameEntry_ShouldTrackOneEntryAndRemoveFromUnackedTracker()
    {
        //Arrange
        using var unAckedMessageTracker = new UnAckedMessageTracker(TimeSpan.FromSeconds(100), TimeSpan.FromSeconds(1), (_, _) => default);
        using var uut = CreateTracker(TimeSpan.FromSeconds(100));
        var topicMessageId = new MessageId(3, 1, 0, -1, "topic-1");
        var batchMessageIds = new[] { new MessageId(3, 1, 0, 0), new MessageId(3, 1, 0, 1), new MessageId(3, 1, 0, 2) };
        _ = unAckedMessageTracker.Add(topicMessageId);

        //Act
        _ = uut.Add(topicMessageId);
        _ = unAckedMessageTracker.Remove(topicMessageId);
        var afterTopicMessageId = (uut.Count, unAckedMessageTracker.Count);
        _ = unAckedMessageTracker.Add(new MessageId(3, 1, 0, -1));
        foreach (var batchMessageId in batchMessageIds)
        {
            _ = uut.Add(batchMessageId);
            _ = unAckedMessageTracker.Remove(batchMessageId);
        }

        //Assert
        afterTopicMessageId.ShouldBe((1, 0));
        uut.Count.ShouldBe(1);
        unAckedMessageTracker.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenThreeBatchMessageIdsOfSameEntry_ShouldOnlyTrackTheFirst()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(100));

        //Act
        var results = new[]
        {
            uut.Add(new MessageId(3, 1, 0, 0)),
            uut.Add(new MessageId(3, 1, 0, 1)),
            uut.Add(new MessageId(3, 1, 0, 2))
        };

        //Assert
        results.ShouldBe([true, false, false]);
        uut.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public void Add_GivenMessageIdsOfDifferentEntries_ShouldTrackEachEntry()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(100));

        //Act
        _ = uut.Add(new MessageId(3, 1, 0, -1));
        _ = uut.Add(new MessageId(3, 2, 0, -1));
        _ = uut.Add(new MessageId(4, 1, 0, -1));

        //Assert
        uut.Count.ShouldBe(3);
    }

    // Additional coverage
    [Theory]
    [InlineData(0, 8, 0)]
    [InlineData(255, 8, 0)]
    [InlineData(256, 8, 256)]
    [InlineData(511, 8, 256)]
    [InlineData(1000, 8, 768)]
    [InlineData(1023, 8, 768)]
    [InlineData(1024, 8, 1024)]
    [InlineData(1000, 0, 1000)]
    [InlineData(1000, 4, 992)]
    public void TrimLowerBits_GivenTimestamp_ShouldClearTheLowerBits(long timestamp, int bits, long expected)
    {
        //Arrange & Act
        var actual = NegativeAcknowledgeTracker.TrimLowerBits(timestamp, bits);

        //Assert
        actual.ShouldBe(expected);
    }

    // Additional coverage
    [Fact]
    public void TrimLowerBits_GivenEightBits_ShouldBucketBy256MillisecondsAndNeverBeLaterThanRequested()
    {
        //Arrange & Act
        var results = Enumerable.Range(0, 2048).Select(timestamp => (timestamp, trimmed: NegativeAcknowledgeTracker.TrimLowerBits(timestamp, NegativeAcknowledgeTracker.PrecisionBitCount))).ToArray();

        //Assert
        results.ShouldAllBe(result => result.trimmed <= result.timestamp && (result.timestamp - result.trimmed) < 256);
        results.Select(result => result.trimmed).Distinct().Count().ShouldBe(8);
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenNacksWithinTheSameBucket_ShouldRedeliverThemTogether()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        var first = new MessageId(1, 1, 0, -1);
        var second = new MessageId(1, 2, 0, -1);
        _ = uut.Add(first);
        _clock.Advance(TimeSpan.FromMilliseconds(10));
        _ = uut.Add(second);
        _clock.Advance(TimeSpan.FromMilliseconds(990));

        //Act
        _timers.Timer.Fire();

        //Assert
        _redelivered.Count.ShouldBe(1);
        _redelivered.Single().ShouldBe([first, second], ignoreOrder: true);
        uut.Count.ShouldBe(0);
    }

    // Upstream: NegativeAcksTracker#triggerRedelivery
    [Fact]
    public void Fire_GivenDueEntries_ShouldRedeliverOnlyDueEntriesAndReschedule()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        var first = new MessageId(1, 1, 0, -1);
        var second = new MessageId(1, 2, 0, -1);
        _ = uut.Add(first);
        _clock.Advance(TimeSpan.FromMilliseconds(300));
        _ = uut.Add(second);
        _clock.Advance(TimeSpan.FromMilliseconds(700));

        //Act
        _timers.Timer.Fire();
        var afterFirstFire = _redelivered.ToArray();
        var pendingAfterFirstFire = _timers.Timer.Pending;
        _clock.Advance(TimeSpan.FromMilliseconds(280));
        _timers.Timer.Fire();

        //Assert
        afterFirstFire.Length.ShouldBe(1);
        afterFirstFire[0].ShouldBe([first]);
        pendingAfterFirstFire.ShouldBe(TimeSpan.FromMilliseconds(280));
        _redelivered.Count.ShouldBe(2);
        _redelivered.Last().ShouldBe([second]);
        uut.Count.ShouldBe(0);
        _timers.Timer.Pending.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenTimerFiresBeforeEntryIsDue_ShouldRescheduleForTheRemainingTime()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(767));

        //Act
        _timers.Timer.Fire();

        //Assert
        _redelivered.ShouldBeEmpty();
        uut.Count.ShouldBe(1);
        _timers.Timer.Pending.ShouldBe(TimeSpan.FromMilliseconds(1));
    }

    // Additional coverage
    [Fact]
    public void Add_GivenAllEntriesRedelivered_ShouldScheduleTimerAgainOnNextAdd()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));
        _timers.Timer.Fire();
        _timers.Timer.Pending.ShouldBeNull();

        //Act
        _ = uut.Add(new MessageId(1, 2, 0, -1));

        //Assert
        _timers.Created.ShouldBe(1);
        _timers.Timer.Pending.ShouldBe(TimeSpan.FromMilliseconds(1000));
    }

    // Additional coverage
    [Fact]
    public void Add_GivenTimerAlreadyScheduled_ShouldNotScheduleItAgain()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(300));

        //Act
        _ = uut.Add(new MessageId(1, 2, 0, -1));

        //Assert
        _timers.Timer.Scheduled.ShouldBe([TimeSpan.FromMilliseconds(1000)]);
    }

    // Upstream: NegativeAcksTest#testNegativeAcksWithBatch
    [Fact]
    public void Add_GivenEntryAlreadyPending_ShouldKeepTheFirstDeadline()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromSeconds(3));
        var first = new MessageId(1, 1, 0, 0);
        var second = new MessageId(1, 1, 0, 1);
        _ = uut.Add(first);
        _clock.Advance(TimeSpan.FromSeconds(2));

        //Act
        var added = uut.Add(second);
        _clock.Advance(TimeSpan.FromSeconds(1));
        _timers.Timer.Fire();

        //Assert
        added.ShouldBeFalse();
        _redelivered.Count.ShouldBe(1);
        _redelivered.Single().ShouldBe([new MessageId(1, 1, 0, -1)]);
        _timers.Timer.Pending.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Add_GivenEntryRedeliveredAndNackedAgain_ShouldTrackItAgain()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        var messageId = new MessageId(1, 1, 0, -1);
        _ = uut.Add(messageId);
        _clock.Advance(TimeSpan.FromMilliseconds(1000));
        _timers.Timer.Fire();

        //Act
        var added = uut.Add(messageId);

        //Assert
        added.ShouldBeTrue();
        uut.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenBatchMessageId_ShouldRedeliverTheEntryLevelMessageIdWithPartitionAndTopic()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(3, 1, 2, 5, "persistent://public/default/topic-partition-2"));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));

        //Act
        _timers.Timer.Fire();

        //Assert
        var actual = _redelivered.Single().Single();
        actual.ShouldBe(new MessageId(3, 1, 2, -1, "persistent://public/default/topic-partition-2"));
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenEntryLevelMessageId_ShouldRedeliverTheSameInstance()
    {
        //Arrange
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        var messageId = new MessageId(3, 1, 2, -1, "topic");
        _ = uut.Add(messageId);
        _clock.Advance(TimeSpan.FromMilliseconds(1000));

        //Act
        _timers.Timer.Fire();

        //Assert
        _redelivered.Single().Single().ShouldBeSameAs(messageId);
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenRedeliverThrows_ShouldKeepRedeliveringLaterEntries()
    {
        //Arrange
        var redeliveries = 0;
        using var uut = new NegativeAcknowledgeTracker(
            TimeSpan.FromMilliseconds(1000),
            (_, _) =>
            {
                ++redeliveries;
                throw new InvalidOperationException("Redelivery failed");
            },
            () => _clock.Now,
            _timers.Create);
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));
        _timers.Timer.Fire();
        _ = uut.Add(new MessageId(1, 2, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));

        //Act
        _timers.Timer.Fire();

        //Assert
        redeliveries.ShouldBe(2);
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenRedeliverFaults_ShouldKeepRedeliveringLaterEntries()
    {
        //Arrange
        var redeliveries = 0;
        using var uut = new NegativeAcknowledgeTracker(
            TimeSpan.FromMilliseconds(1000),
            (_, _) =>
            {
                ++redeliveries;
                return new ValueTask(Task.FromException(new InvalidOperationException("Redelivery failed")));
            },
            () => _clock.Now,
            _timers.Create);
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));
        _timers.Timer.Fire();
        _ = uut.Add(new MessageId(1, 2, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));

        //Act
        _timers.Timer.Fire();

        //Assert
        redeliveries.ShouldBe(2);
    }

    // Additional coverage
    [Fact]
    public void Fire_GivenRedelivery_ShouldCallRedeliverOutsideOfTheLock()
    {
        //Arrange
        var canUseTrackerFromAnotherThread = false;
        NegativeAcknowledgeTracker? tracker = null;
        using var uut = new NegativeAcknowledgeTracker(
            TimeSpan.FromMilliseconds(1000),
            (_, _) =>
            {
                using var used = new ManualResetEventSlim();
                _ = Run(() =>
                {
                    _ = tracker!.Count;
                    used.Set();
                });
                canUseTrackerFromAnotherThread = used.Wait(TimeSpan.FromSeconds(30));
                return default;
            },
            () => _clock.Now,
            _timers.Create);
        tracker = uut;
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));

        //Act
        _timers.Timer.Fire();

        //Assert
        canUseTrackerFromAnotherThread.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenPendingEntries_ShouldDisposeTimerAndClear()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _ = uut.Add(new MessageId(1, 2, 0, -1));

        //Act
        uut.Dispose();

        //Assert
        uut.Count.ShouldBe(0);
        _timers.Timer.IsDisposed.ShouldBeTrue();
        _timers.Timer.Pending.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenTimerAlreadyQueued_ShouldNotRedeliver()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));
        uut.Dispose();

        //Act
        _timers.Timer.FireWithoutSchedule();

        //Assert
        _redelivered.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public void Add_GivenDisposedTracker_ShouldNotTrackOrSchedule()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        uut.Dispose();

        //Act
        var added = uut.Add(new MessageId(1, 1, 0, -1));

        //Assert
        added.ShouldBeFalse();
        uut.Count.ShouldBe(0);
        _timers.Created.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenCalledTwice_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 1, 0, -1));

        //Act
        var exception = Record.Exception(() =>
        {
            uut.Dispose();
            uut.Dispose();
        });

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenRedeliverInFlight_ShouldCancelTheToken()
    {
        //Arrange
        var token = CancellationToken.None;
        var neverCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var uut = new NegativeAcknowledgeTracker(
            TimeSpan.FromMilliseconds(1000),
            (_, cancellationToken) =>
            {
                token = cancellationToken;
                return new ValueTask(neverCompleted.Task);
            },
            () => _clock.Now,
            _timers.Create);
        _ = uut.Add(new MessageId(1, 1, 0, -1));
        _clock.Advance(TimeSpan.FromMilliseconds(1000));
        _timers.Timer.Fire();
        token.CanBeCanceled.ShouldBeTrue();
        token.IsCancellationRequested.ShouldBeFalse();

        //Act
        uut.Dispose();

        //Assert
        token.IsCancellationRequested.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task AddAndFire_GivenConcurrentUse_ShouldRedeliverEveryEntryExactlyOnce()
    {
        //Arrange
        const int adders = 4;
        const int entriesPerAdder = 5000;
        using var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        using var stop = new CancellationTokenSource();
        var added = 0;
        var timer = (FakeOneShotTimer?) null;

        //Act
        var addTasks = Enumerable.Range(1, adders).Select(ledgerId => Run(() =>
        {
            for (var entryId = 0; entryId < entriesPerAdder; ++entryId)
            {
                if (uut.Add(new MessageId((ulong) ledgerId, (ulong) entryId, 0, -1)))
                    Interlocked.Increment(ref added);
            }
        })).ToArray();

        var fireTask = Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                _clock.Advance(TimeSpan.FromMilliseconds(300));

                if (_timers.Created == 0)
                    continue;

                timer ??= _timers.Timer;
                _ = timer.TryFire();
            }
        });

        await Task.WhenAll(addTasks);
        stop.Cancel();
        await fireTask;

        while (uut.Count > 0)
        {
            _clock.Advance(TimeSpan.FromMinutes(1));
            _timers.Timer.Fire();
        }

        //Assert
        var redelivered = _redelivered.SelectMany(messageIds => messageIds).ToArray();
        added.ShouldBe(adders * entriesPerAdder);
        redelivered.Length.ShouldBe(added);
        redelivered.Select(messageId => (messageId.LedgerId, messageId.EntryId)).Distinct().Count().ShouldBe(added);
    }

    // Additional coverage
    [Fact]
    public async Task AddAndDispose_GivenConcurrentUse_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateTracker(TimeSpan.FromMilliseconds(1000));
        _ = uut.Add(new MessageId(1, 0, 0, -1));

        //Act
        var tasks = Enumerable.Range(1, 4).Select(ledgerId => Run(() =>
        {
            for (var entryId = 0; entryId < 5000; ++entryId)
            {
                _ = uut.Add(new MessageId((ulong) ledgerId, (ulong) entryId, 0, -1));
                _clock.Advance(TimeSpan.FromMilliseconds(5));
                _ = _timers.Timer.TryFire();
            }
        })).ToArray();
        var disposeTask = Run(() => uut.Dispose());
        var exception = await Record.ExceptionAsync(() => Task.WhenAll(tasks.Append(disposeTask)));

        //Assert
        exception.ShouldBeNull();
        uut.Count.ShouldBe(0);
    }

    private static Task Run(Action action)
        => Task.Run(action, Current.CancellationToken);

    private NegativeAcknowledgeTracker CreateTracker(TimeSpan delay)
        => new(
            delay,
            (messageIds, _) =>
            {
                _redelivered.Enqueue(messageIds);
                return default;
            },
            () => _clock.Now,
            _timers.Create);
}
