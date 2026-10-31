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
using DotPulsar.Internal.Exceptions;

[Trait("Category", "Unit")]
public class AsyncQueueWithCursorTests
{
    [Fact]
    public async Task TryPeek_GivenQueueIsEmpty_ShouldReturnFalse()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(10);

        //Act
        var actual = sut.TryPeek(out _);

        //Assert
        actual.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task TryPeek_GivenQueueHasItem_ShouldReturnTrueAndItem()
    {
        //Arrange
        var expected = new QueueItem(1);
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(expected, CancellationToken.None);

        //Act
        var actual = sut.TryPeek(out var result);

        //Assert
        actual.ShouldBeTrue();
        result.ShouldNotBeNull();
        result!.ShouldBe(expected);

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Dequeue_GivenQueueHasItem_ShouldRemoveItemFromQueue()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(10);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);

        //Act
        sut.Dequeue();
        var actual = sut.TryPeek(out _);

        //Assert
        actual.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Dequeue_GivenQueueIsEmpty_ShouldThrowException()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(10);

        //Act
        var act = () => sut.Dequeue();

        //Assert
        act.ShouldThrow<InvalidOperationException>();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Enqueue_GivenQueueIsEmpty_ShouldAddItem()
    {
        //Arrange
        var expected = new QueueItem(1);
        var sut = new AsyncQueueWithCursor<QueueItem>(10);

        //Act
        await sut.Enqueue(expected, CancellationToken.None);
        var actual = sut.TryPeek(out var result);

        //Assert
        actual.ShouldBeTrue();
        result.ShouldNotBeNull();
        result!.ShouldBe(expected);

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Enqueue_GivenQueueIsFull_ShouldBlockInsertion()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);

        //Act
        var actual = sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);

        //Assert
        actual.IsCompleted.ShouldBeFalse();
        actual.IsFaulted.ShouldBeFalse();
        actual.IsCanceled.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Dequeue_GivenQueueIsFullAndEnqueuePending_ShouldDequeueAndEnqueue()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(10)); // Only there so that the test does not wait for ever; the cancellation of an enqueue is tested below
        var pendingEnqueue = sut.Enqueue(Substitute.For<IDisposable>(), cts.Token);

        //Act
        sut.Dequeue();
        await pendingEnqueue;

        //Assert
        pendingEnqueue.IsCompleted.ShouldBeTrue();
        pendingEnqueue.IsFaulted.ShouldBeFalse();
        pendingEnqueue.IsCanceled.ShouldBeFalse();

        //Annihilate
        cts.Dispose();
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Enqueue_GivenQueueIsFullAndTokenCanceled_ShouldCancelTask()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        var cts = new CancellationTokenSource();

        //Act
        var pendingEnqueue = sut.Enqueue(Substitute.For<IDisposable>(), cts.Token).AsTask();
        cts.Cancel();
        var exception = await Record.ExceptionAsync(() => pendingEnqueue);

        //Assert
        exception.ShouldBeOfType<TaskCanceledException>();
        pendingEnqueue.IsCompleted.ShouldBeTrue();
        pendingEnqueue.IsFaulted.ShouldBeFalse();
        pendingEnqueue.IsCanceled.ShouldBeTrue();

        //Annihilate
        cts.Dispose();
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task NextItem_GivenQueueHasItems_ShouldReturnItems()
    {
        //Arrange
        var expected1 = new QueueItem(1);
        var expected2 = new QueueItem(2);
        var sut = new AsyncQueueWithCursor<QueueItem>(2);
        await sut.Enqueue(expected1, CancellationToken.None);
        await sut.Enqueue(expected2, CancellationToken.None);

        //Act
        var actual1 = await sut.NextItem(CancellationToken.None);
        var actual2 = await sut.NextItem(CancellationToken.None);

        //Assert
        actual1.ShouldBe(expected1);
        actual2.ShouldBe(expected2);

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task NextItem_GivenQueueIfFull_ShouldBlock()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var actual = sut.NextItem(CancellationToken.None);

        //Assert
        actual.IsCompleted.ShouldBeFalse();
        actual.IsFaulted.ShouldBeFalse();
        actual.IsCanceled.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task NextItem_GivenItemInserted_ShouldReturnInsertedItem()
    {
        //Arrange
        var expected = new QueueItem(1);
        var sut = new AsyncQueueWithCursor<QueueItem>(1);

        //Act
        var task = sut.NextItem(CancellationToken.None);
        await sut.Enqueue(expected, CancellationToken.None);
        var actual = await task;

        //Assert
        actual.ShouldBe(expected);
        task.IsCompleted.ShouldBeTrue();
        task.IsFaulted.ShouldBeFalse();
        task.IsCanceled.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task NextItem_WhenCanceled_ShouldThrowException()
    {
        //Arrange
        var uut = new AsyncQueueWithCursor<QueueItem>(1);
        var cts = new CancellationTokenSource();

        //Act
        var task = uut.NextItem(cts.Token);
        cts.Cancel();
        var exception = await Record.ExceptionAsync(() => task.AsTask());

        //Assert
        exception.ShouldBeOfType<TaskCanceledException>();
    }

    [Fact]
    public async Task ResetCursor_GivenCursorNotAtFirstElement_ShouldMoveToFirst()
    {
        //Arrange
        var expected1 = new QueueItem(1);
        var expected2 = new QueueItem(2);
        var sut = new AsyncQueueWithCursor<QueueItem>(2);
        await sut.Enqueue(expected1, CancellationToken.None);
        await sut.Enqueue(expected2, CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var before = await sut.NextItem(CancellationToken.None);
        sut.ResetCursor();
        var after = await sut.NextItem(CancellationToken.None);

        //Assert
        before.ShouldBe(expected2);
        after.ShouldBe(expected1);

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task RemoveCurrentItem_GivenQueueEmpty_ShouldThrow()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(2);

        //Act
        var exception = Record.Exception(() => sut.RemoveCurrentItem());

        //Assert
        exception.ShouldBeOfType<AsyncQueueWithCursorNoItemException>();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task RemoveCurrentItem_GivenQueueWithItemButCursorNotMoved_ShouldThrow()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(2);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);

        //Act
        var exception = Record.Exception(() => sut.RemoveCurrentItem());

        //Assert
        exception.ShouldBeOfType<AsyncQueueWithCursorNoItemException>();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task RemoveCurrentItem_GivenQueueWithCursorOnItem_ShouldRemoveItem()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(2);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        sut.RemoveCurrentItem();

        //Assert
        sut.TryPeek(out _).ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task RemoveCurrentItem_GivenQueueWithMultipleItems_ShouldMoveCursor()
    {
        //Arrange
        var expected = new QueueItem(3);
        var sut = new AsyncQueueWithCursor<QueueItem>(3);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.Enqueue(expected, CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        sut.RemoveCurrentItem();
        var result = await sut.NextItem(CancellationToken.None);

        //Assert
        result.ShouldBe(expected);

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveCurrentItem_GivenQueueIsFullAndEnqueuePending_ShouldEnqueue()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var pendingEnqueue = sut.Enqueue(Substitute.For<IDisposable>(), cts.Token);

        //Act
        sut.RemoveCurrentItem();
        await pendingEnqueue;

        //Assert
        sut.TryPeek(out _).ShouldBeTrue();

        //Annihilate
        await sut.DisposeAsync();
    }

    [Fact]
    public async Task Dispose_GivenEnqueueWaiting_ShouldCancelTask()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        var enqueueTask = sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);

        //Act
        await sut.DisposeAsync();
        var act = async () => await enqueueTask;

        //Assert
        await act.ShouldThrowAsync<TaskCanceledException>();
        enqueueTask.IsCompleted.ShouldBeTrue();
        enqueueTask.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task Enqueue_GivenDisposed_ShouldThrow()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.DisposeAsync();

        //Act
        var act = async () => await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);

        //Assert
        await act.ShouldThrowAsync<AsyncLockDisposedException>();
    }

    [Fact]
    public async Task Dispose_GivenNextItemAwaiting_ShouldCancelTask()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(2);
        var nextItemTask = sut.NextItem(CancellationToken.None);

        //Act
        await sut.DisposeAsync();
        var act = async () => await nextItemTask;

        //Assert
        await act.ShouldThrowAsync<TaskCanceledException>();
        nextItemTask.IsCompleted.ShouldBeTrue();
        nextItemTask.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task Dispose_GivenQueueEmptyAwaiting_ShouldCancelTask()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(Substitute.For<IDisposable>(), CancellationToken.None);
        var waitForEmptyTask = sut.WaitForEmpty(CancellationToken.None);

        //Act
        await sut.DisposeAsync();
        var act = async () => await waitForEmptyTask;

        //Assert
        await act.ShouldThrowAsync<TaskCanceledException>();
        waitForEmptyTask.IsCompleted.ShouldBeTrue();
        waitForEmptyTask.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task Dispose_GivenQueuedItem_ShouldDisposeItem()
    {
        //Arrange
        var item = Substitute.For<IDisposable>();
        var sut = new AsyncQueueWithCursor<IDisposable>(1);
        await sut.Enqueue(item, CancellationToken.None);

        //Act
        await sut.DisposeAsync();

        //Assert
        item.Received().Dispose();
    }

    // Additional coverage
    [Fact]
    public async Task TryPeekNextItem_GivenItemAfterCursor_ShouldNotMoveCursor()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var actual = sut.TryPeekNextItem(out var peeked);
        var next = await sut.NextItem(CancellationToken.None);

        //Assert
        actual.ShouldBeTrue();
        peeked.ShouldBe(new QueueItem(2));
        next.ShouldBe(new QueueItem(2));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task TryPeekNextItem_GivenCursorAtLastItem_ShouldReturnFalse()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var actual = sut.TryPeekNextItem(out _);

        //Assert
        actual.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task MoveNext_GivenItemAfterCursor_ShouldMoveCursorAndReturnItem()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var moved = sut.MoveNext();
        var hasNext = sut.TryPeekNextItem(out _);

        //Assert
        moved.ShouldBe(new QueueItem(2));
        hasNext.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task MoveNext_GivenNoItem_ShouldThrowAsyncQueueWithCursorNoItemException()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var exception = Record.Exception(() => sut.MoveNext());

        //Assert
        exception.ShouldBeOfType<AsyncQueueWithCursorNoItemException>();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task MoveCursorBack_GivenItemsPassed_ShouldReturnTheSameItemsAgain()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.Enqueue(new QueueItem(3), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        sut.MoveNext();
        sut.MoveNext();

        //Act
        sut.MoveCursorBack(2);
        var first = sut.MoveNext();
        var second = sut.MoveNext();

        //Assert
        first.ShouldBe(new QueueItem(2));
        second.ShouldBe(new QueueItem(3));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task MoveCursorBack_GivenMoreThanTheItemsPassed_ShouldPutCursorBeforeFirstItem()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        sut.MoveNext();

        //Act
        sut.MoveCursorBack(5);
        var next = await sut.NextItem(CancellationToken.None);

        //Assert
        next.ShouldBe(new QueueItem(1));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task MoveCursorBack_GivenHeadDequeuedWhileCursorIsAtLaterItem_ShouldNotLoseTheItemsAfterTheHead()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.Enqueue(new QueueItem(3), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        sut.MoveNext();
        sut.MoveNext();
        sut.Dequeue();

        //Act
        sut.MoveCursorBack(2);
        var first = await sut.NextItem(CancellationToken.None);
        var second = sut.MoveNext();

        //Assert
        first.ShouldBe(new QueueItem(2));
        second.ShouldBe(new QueueItem(3));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RewriteItemsUpToCursor_GivenItems_ShouldReorderValuesInPlace()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(0), CancellationToken.None);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.Enqueue(new QueueItem(3), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        sut.MoveNext();
        sut.MoveNext();

        //Act
        sut.RewriteItemsUpToCursor([new QueueItem(2), new QueueItem(1)]);
        sut.MoveCursorBack(2);
        var values = new[] { sut.MoveNext(), sut.MoveNext(), sut.MoveNext() };

        //Assert
        values.ShouldBe([new QueueItem(2), new QueueItem(1), new QueueItem(3)]);

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RewriteItemsUpToCursor_GivenMoreItemsThanNodes_ShouldThrow()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);

        //Act
        var exception = Record.Exception(() => sut.RewriteItemsUpToCursor([new QueueItem(1), new QueueItem(2)]));

        //Assert
        exception.ShouldBeOfType<AsyncQueueWithCursorNoItemException>();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task TryPeekLast_GivenItems_ShouldReturnTheLastItemWithoutRemovingIt()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);

        //Act
        var actual = sut.TryPeekLast(out var last);
        var stillThere = sut.TryPeek(out var first);

        //Assert
        actual.ShouldBeTrue();
        last.ShouldBe(new QueueItem(2));
        stillThere.ShouldBeTrue();
        first.ShouldBe(new QueueItem(1));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task TryPeekLast_GivenQueueIsEmpty_ShouldReturnFalse()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);

        //Act
        var actual = sut.TryPeekLast(out _);

        //Assert
        actual.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task WaitForNextItem_GivenItemAfterCursor_ShouldReturnTrueWithoutWaiting()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);

        //Act
        var wait = sut.WaitForNextItem(TimeSpan.FromHours(1), CancellationToken.None);

        //Assert
        wait.IsCompletedSuccessfully.ShouldBeTrue();
        (await wait).ShouldBeTrue();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task WaitForNextItem_GivenEnqueueDuringWait_ShouldReturnTrueAndNotMoveCursor()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        var wait = sut.WaitForNextItem(TimeSpan.FromHours(1), CancellationToken.None).AsTask();

        //Act
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        var actual = await wait.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);
        var next = await sut.NextItem(CancellationToken.None);

        //Assert
        actual.ShouldBeTrue();
        next.ShouldBe(new QueueItem(1));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task WaitForNextItem_GivenTimeout_ShouldReturnFalse()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);

        //Act
        var actual = await sut.WaitForNextItem(TimeSpan.FromMilliseconds(50), CancellationToken.None);

        //Assert
        actual.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task WaitForNextItem_GivenSignalWaiters_ShouldReturnFalse()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        var wait = sut.WaitForNextItem(TimeSpan.FromHours(1), CancellationToken.None).AsTask();

        //Act
        sut.SignalWaiters();
        var actual = await wait.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task WaitForNextItem_GivenSignalWaitersBeforeTheWait_ShouldReturnFalseWithoutWaiting()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        sut.SignalWaiters();

        //Act
        var wait = sut.WaitForNextItem(TimeSpan.FromHours(1), CancellationToken.None);

        //Assert
        wait.IsCompletedSuccessfully.ShouldBeTrue();
        (await wait).ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenItems_ShouldReturnThemInOrderAndEmptyTheQueue()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.Enqueue(new QueueItem(3), CancellationToken.None);

        //Act
        var removed = sut.RemoveAll();

        //Assert
        removed.ShouldBe([new QueueItem(1), new QueueItem(2), new QueueItem(3)]);
        sut.Count.ShouldBe(0);
        sut.TryPeek(out _).ShouldBeFalse();

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenCursorInTheQueue_ShouldPutCursorBeforeTheFirstNewItem()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        _ = sut.RemoveAll();
        await sut.Enqueue(new QueueItem(3), CancellationToken.None);

        //Act
        var next = await sut.NextItem(CancellationToken.None);

        //Assert
        next.ShouldBe(new QueueItem(3));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenPendingNextItem_ShouldNotCancelIt()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        await sut.NextItem(CancellationToken.None);
        var pending = sut.NextItem(CancellationToken.None).AsTask();

        //Act
        _ = sut.RemoveAll();
        await sut.Enqueue(new QueueItem(2), CancellationToken.None);

        //Assert
        (await pending.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken)).ShouldBe(new QueueItem(2));

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenWaitForEmptyPending_ShouldCompleteIt()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        var waitForEmpty = sut.WaitForEmpty(CancellationToken.None);

        //Act
        _ = sut.RemoveAll();

        //Assert
        await waitForEmpty.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenQueueIsFullAndEnqueuePending_ShouldReleaseTheEnqueue()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(1);
        await sut.Enqueue(new QueueItem(1), CancellationToken.None);
        var blocked = sut.Enqueue(new QueueItem(2), CancellationToken.None).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);

        //Act
        _ = sut.RemoveAll();

        //Assert
        await blocked.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);
        sut.Count.ShouldBe(1);

        //Annihilate
        await sut.DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task WaitForNextItem_GivenCancellation_ShouldThrowOperationCanceledException()
    {
        //Arrange
        var sut = new AsyncQueueWithCursor<QueueItem>(10);
        using var cts = new CancellationTokenSource();
        var wait = sut.WaitForNextItem(TimeSpan.FromHours(1), cts.Token).AsTask();

        //Act
        await cts.CancelAsync();

        //Assert
        var exception = await Record.ExceptionAsync(() => wait);
        exception.ShouldBeAssignableTo<OperationCanceledException>();

        //Annihilate
        await sut.DisposeAsync();
    }

    private class QueueItem : IDisposable, IEquatable<QueueItem>
    {
        private int Number { get; }

        public QueueItem(int number) => Number = number;

        public void Dispose() { }

        public bool Equals(QueueItem? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Number == other.Number;
        }

        public override bool Equals(object? obj) => obj is QueueItem qi && Equals(qi);

        public override int GetHashCode() => Number;
    }
}
