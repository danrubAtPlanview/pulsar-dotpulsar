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
public class AsyncQueueTests
{
    [Fact]
    public async Task Enqueue_GivenDequeueTaskWasWaiting_ShouldCompleteDequeueTask()
    {
        //Arrange
        const int expected = 1;
        var queue = new AsyncQueue<int>();
        var dequeueTask = queue.Dequeue(Current.CancellationToken);
        queue.Enqueue(expected);

        //Act
        var actual = await dequeueTask;

        //Assert
        actual.ShouldBe(expected);

        //Annihilate
        queue.Dispose();
    }

    [Fact]
    public async Task DequeueAsync_GivenQueueWasNotEmpty_ShouldCompleteDequeueTask()
    {
        //Arrange
        const int expected = 1;
        var queue = new AsyncQueue<int>();
        queue.Enqueue(expected);

        //Act
        var actual = await queue.Dequeue(Current.CancellationToken);

        //Assert
        actual.ShouldBe(expected);

        //Annihilate
        queue.Dispose();
    }

    [Fact]
    public async Task DequeueAsync_GivenMultipleDequeues_ShouldCompleteInOrderedSequence()
    {
        //Arrange
        const int expected1 = 1, expected2 = 2;
        var queue = new AsyncQueue<int>();
        var dequeue1 = queue.Dequeue(Current.CancellationToken);
        var dequeue2 = queue.Dequeue(Current.CancellationToken);
        queue.Enqueue(expected1);
        queue.Enqueue(expected2);

        //Act
        var actual1 = await dequeue1;
        var actual2 = await dequeue2;

        //Assert
        actual1.ShouldBe(expected1);
        actual2.ShouldBe(expected2);

        //Annihilate
        queue.Dispose();
    }

    [Fact]
    public async Task DequeueAsync_GivenSequenceOfInput_ShouldReturnSameSequenceOfOutput()
    {
        //Arrange
        const int expected1 = 1, expected2 = 2;
        var queue = new AsyncQueue<int>();
        queue.Enqueue(expected1);
        queue.Enqueue(expected2);

        //Act
        var actual1 = await queue.Dequeue(Current.CancellationToken);
        var actual2 = await queue.Dequeue(Current.CancellationToken);

        //Assert
        actual1.ShouldBe(expected1);
        actual2.ShouldBe(expected2);

        //Annihilate
        queue.Dispose();
    }

    [Fact]
    public async Task DequeueAsync_GivenTokenIsCanceled_ShouldCancelTask()
    {
        //Arrange
        CancellationTokenSource source1 = new(), source2 = new();
        const int excepted = 1;
        var queue = new AsyncQueue<int>();
        var task1 = queue.Dequeue(source1.Token).AsTask();
        var task2 = queue.Dequeue(source2.Token).AsTask();

        //Act
        source1.Cancel();
        queue.Enqueue(excepted);
        var exception = await Record.ExceptionAsync(() => task1);
        var actual = await task2;

        //Assert
        exception.ShouldBeOfType<TaskCanceledException>();
        actual.ShouldBe(excepted);

        //Annihilate
        source1.Dispose();
        source2.Dispose();
        queue.Dispose();
    }

    // Additional coverage
    [Fact]
    public void RemoveAll_GivenQueuedItems_ShouldReturnThemInOrderAndEmptyTheQueue()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);

        //Act
        var removed = queue.RemoveAll();

        //Assert
        removed.ShouldBeSequence(1, 2, 3);
        queue.RemoveAll().ShouldBeEmpty();

        //Annihilate
        queue.Dispose();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenQueuedItemsAndLaterEnqueue_ShouldOnlyDeliverTheNewItem()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);
        _ = queue.RemoveAll();

        //Act
        queue.Enqueue(3);
        var actual = await queue.Dequeue(Current.CancellationToken);

        //Assert
        actual.ShouldBe(3);

        //Annihilate
        queue.Dispose();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveAll_GivenPendingDequeue_ShouldNotCompleteIt()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        var dequeueTask = queue.Dequeue(Current.CancellationToken).AsTask();

        //Act
        var removed = queue.RemoveAll();
        queue.Enqueue(1);

        //Assert
        removed.ShouldBeEmpty();
        (await dequeueTask).ShouldBe(1);

        //Annihilate
        queue.Dispose();
    }

    // Additional coverage
    [Fact]
    public async Task RemoveWhere_GivenPredicate_ShouldReturnMatchingItemsAndKeepTheOthersInOrder()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        for (var i = 1; i <= 6; ++i)
        {
            queue.Enqueue(i);
        }

        //Act
        var removed = queue.RemoveWhere(item => item % 2 == 0);

        //Assert
        removed.ShouldBeSequence(2, 4, 6);
        var remaining = new List<int>();
        for (var i = 0; i < 3; ++i)
        {
            remaining.Add(await queue.Dequeue(Current.CancellationToken));
        }
        remaining.ShouldBeSequence(1, 3, 5);

        //Annihilate
        queue.Dispose();
    }

    // Additional coverage
    [Fact]
    public void RemoveWhere_GivenNoMatch_ShouldKeepAllItems()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);

        //Act
        var removed = queue.RemoveWhere(_ => false);

        //Assert
        removed.ShouldBeEmpty();
        queue.RemoveAll().ShouldBeSequence(1, 2);

        //Annihilate
        queue.Dispose();
    }

    // Additional coverage
    [Fact]
    public void RemoveAll_GivenDisposedQueue_ShouldThrowAsyncQueueDisposedException()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        queue.Dispose();

        //Act
        var exception = Record.Exception(() => queue.RemoveAll());

        //Assert
        exception.ShouldBeOfType<AsyncQueueDisposedException>();
    }

    // Additional coverage
    [Fact]
    public void RemoveWhere_GivenDisposedQueue_ShouldThrowAsyncQueueDisposedException()
    {
        //Arrange
        var queue = new AsyncQueue<int>();
        queue.Dispose();

        //Act
        var exception = Record.Exception(() => queue.RemoveWhere(_ => true));

        //Assert
        exception.ShouldBeOfType<AsyncQueueDisposedException>();
    }
}
