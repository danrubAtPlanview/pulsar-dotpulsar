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

namespace DotPulsar.Internal;

using DotPulsar.Internal.Exceptions;

public sealed class AsyncQueueWithCursor<T> : IAsyncDisposable where T : IDisposable
{
    private static readonly TimeSpan _maxDelay = TimeSpan.FromMilliseconds(int.MaxValue);

    private readonly AsyncLock _pendingLock;
    private readonly SemaphoreSlim _cursorSemaphore;
    private readonly LinkedList<T> _queue;
    private readonly List<TaskCompletionSource<object>> _queueEmptyTcs;
    private readonly uint _maxItems;
    private IDisposable? _pendingLockGrant;
    private LinkedListNode<T>? _currentNode;
    private TaskCompletionSource<LinkedListNode<T>>? _cursorNextItemTcs;
    private TaskCompletionSource<bool>? _cursorWaitTcs;
    private bool _waitSignaled;
    private int _isDisposed;

    public AsyncQueueWithCursor(uint maxItems)
    {
        _pendingLock = new AsyncLock();
        _cursorSemaphore = new SemaphoreSlim(1, 1);
        _queueEmptyTcs = [];
        _queue = new LinkedList<T>();
        _maxItems = maxItems;
    }

    /// <summary>
    /// Enqueue item
    /// </summary>
    public async ValueTask Enqueue(T item, CancellationToken cancellationToken)
    {
        try
        {
            var grant = await _pendingLock.Lock(cancellationToken).ConfigureAwait(false);
            lock (_pendingLock)
            {
                _pendingLockGrant = grant;
            }
        }
        catch (Exception)
        {
            ReleasePendingLockGrant();
            throw;
        }

        lock (_queue)
        {
            ThrowIfDisposed();
            var node = _queue.AddLast(item);
            _cursorNextItemTcs?.TrySetResult(node);
            _cursorWaitTcs?.TrySetResult(true);

            if (_queue.Count < _maxItems)
                ReleasePendingLockGrant();
        }
    }

    /// <summary>
    /// Attempt to retrieve the last item in the queue without removing it
    /// </summary>
    public bool TryPeek(out T? item)
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            var node = _queue.First;
            if (node is not null)
            {
                item = node.Value;
                return true;
            }

            item = default;
            return false;
        }
    }

    /// <summary>
    /// Attempt to retrieve the item that was enqueued last, without removing it
    /// </summary>
    public bool TryPeekLast(out T? item)
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            var node = _queue.Last;
            if (node is not null)
            {
                item = node.Value;
                return true;
            }

            item = default;
            return false;
        }
    }

    /// <summary>
    /// The number of items in the queue
    /// </summary>
    public int Count
    {
        get
        {
            lock (_queue)
            {
                return _queue.Count;
            }
        }
    }

    /// <summary>
    /// Remove every item from the queue and return them, in the order they had. The cursor is put before the first item, but a waiting NextItem is not cancelled,
    /// so it returns the next item that is enqueued. Anyone that waits for the queue to be empty, or for room in the queue, is released.
    /// </summary>
    public List<T> RemoveAll()
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            var items = new List<T>(_queue.Count);
            foreach (var item in _queue)
            {
                items.Add(item);
            }

            _queue.Clear();
            _currentNode = null;
            NotifyQueueEmptyAwaiters();
            ReleasePendingLockGrant();

            return items;
        }
    }

    /// <summary>
    /// Remove the last item from the queue
    /// </summary>
    public void Dequeue()
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            if (_currentNode == _queue.First)
                _currentNode = null;

            try
            {
                _queue.RemoveFirst();
            }
            catch (InvalidOperationException e)
            {
                throw new AsyncQueueWithCursorNoItemException(e);
            }

            if (_queue.Count == 0)
                NotifyQueueEmptyAwaiters();

            ReleasePendingLockGrant();
        }
    }

    /// <summary>
    /// Remove the current item the cursor is pointing to if applicable, and return it.
    /// </summary>
    public T RemoveCurrentItem()
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            if (_currentNode is null)
                throw new AsyncQueueWithCursorNoItemException();

            var item = _currentNode.Value;
            var newCurrent = _currentNode.Previous;
            _queue.Remove(_currentNode);
            _currentNode = newCurrent;

            if (_queue.Count == 0)
                NotifyQueueEmptyAwaiters();

            ReleasePendingLockGrant();

            return item;
        }
    }

    /// <summary>
    /// Return the item the cursor is pointing to and move the cursor, to the following item.
    /// Ordering is not supported for multiple concurrent awaiters.
    /// </summary>
    /// <exception cref="OperationCanceledException">If cancellation requested</exception>
    public async ValueTask<T> NextItem(CancellationToken cancellationToken)
    {
        await _cursorSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        CancellationTokenRegistration? registration = null;
        try
        {
            lock (_queue)
            {
                ThrowIfDisposed();

                _currentNode = GetNodeAfterCursor();

                if (_currentNode is not null)
                    return _currentNode.Value;

                var tcs = new TaskCompletionSource<LinkedListNode<T>>(TaskCreationOptions.RunContinuationsAsynchronously);
                _cursorNextItemTcs = tcs;
                registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
            }

            var result = await _cursorNextItemTcs.Task.ConfigureAwait(false);

            lock (_queue)
            {
                ThrowIfDisposed();
                _currentNode = result;
            }

            return _currentNode.Value;
        }
        finally
        {
            var shouldThrow = _cursorNextItemTcs is not null && _cursorNextItemTcs.Task.IsCanceled;

            lock (_queue)
            {
                registration?.Dispose();
                _cursorNextItemTcs = null;
            }

            try
            {
                _cursorSemaphore.Release();
            }
            catch
            {
                // Ignore
            }

            if (shouldThrow)
            {
                throw new TaskCanceledException("The task was cancelled");
            }
        }
    }

    /// <summary>
    /// Attempt to retrieve the item after the one the cursor is pointing to, without moving the cursor.
    /// </summary>
    public bool TryPeekNextItem(out T? item)
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            var node = GetNodeAfterCursor();
            if (node is not null)
            {
                item = node.Value;
                return true;
            }

            item = default;
            return false;
        }
    }

    /// <summary>
    /// Move the cursor to the item after the one it is pointing to, and return that item. NextItem is for waiting for an item that is not there yet.
    /// </summary>
    /// <exception cref="AsyncQueueWithCursorNoItemException">If there is no item after the cursor</exception>
    public T MoveNext()
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            var node = GetNodeAfterCursor() ?? throw new AsyncQueueWithCursorNoItemException();
            _currentNode = node;
            return node.Value;
        }
    }

    /// <summary>
    /// Move the cursor back by the specified number of items. Moving it back beyond the first item puts it before the first item, so that NextItem returns the first item.
    /// </summary>
    public void MoveCursorBack(int count)
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            for (var i = 0; i < count && _currentNode is not null; ++i)
                _currentNode = _currentNode.Previous;
        }
    }

    /// <summary>
    /// Overwrite the items that end with the item the cursor is pointing to, in order, with the specified items.
    /// </summary>
    /// <exception cref="AsyncQueueWithCursorNoItemException">If there are fewer items up to the cursor than specified</exception>
    public void RewriteItemsUpToCursor(IReadOnlyList<T> items)
    {
        lock (_queue)
        {
            ThrowIfDisposed();

            var node = _currentNode;
            for (var i = items.Count - 1; i >= 0; --i)
            {
                if (node is null)
                    throw new AsyncQueueWithCursorNoItemException();

                node.Value = items[i];
                node = node.Previous;
            }
        }
    }

    /// <summary>
    /// Wait until there is an item after the one the cursor is pointing to, the timeout elapses, or SignalWaiters is called. The cursor is not moved.
    /// Returns true if there is an item. Only one waiter is supported, and it must not be combined with a waiting NextItem.
    /// </summary>
    /// <exception cref="OperationCanceledException">If cancellation requested</exception>
    public ValueTask<bool> WaitForNextItem(TimeSpan timeout, CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> tcs;

        lock (_queue)
        {
            ThrowIfDisposed();

            if (GetNodeAfterCursor() is not null)
                return new ValueTask<bool>(true);

            // A signal that came while nobody was waiting is not lost, because the caller may have decided to wait just before it was given
            if (_waitSignaled)
            {
                _waitSignaled = false;
                return new ValueTask<bool>(false);
            }

            tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _cursorWaitTcs = tcs;
        }

        return new ValueTask<bool>(WaitForNextItem(tcs, timeout, cancellationToken));
    }

    private async Task<bool> WaitForNextItem(TaskCompletionSource<bool> tcs, TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var delay = Task.Delay(timeout > _maxDelay ? _maxDelay : timeout, cts.Token);
            var completed = await Task.WhenAny(tcs.Task, delay).ConfigureAwait(false);
            cts.Cancel();

            cancellationToken.ThrowIfCancellationRequested();

            return completed == tcs.Task && tcs.Task.Result;
        }
        finally
        {
            lock (_queue)
            {
                if (ReferenceEquals(_cursorWaitTcs, tcs))
                    _cursorWaitTcs = null;
            }
        }
    }

    /// <summary>
    /// End a WaitForNextItem without an item being there. If nothing is waiting, the next WaitForNextItem ends at once.
    /// </summary>
    public void SignalWaiters()
    {
        lock (_queue)
        {
            if (_cursorWaitTcs is null)
                _waitSignaled = true;
            else
                _ = _cursorWaitTcs.TrySetResult(false);
        }
    }

    /// <summary>
    /// Reset the cursor back to the last item, and cancel any waiting NextItem tasks
    /// </summary>
    public void ResetCursor()
    {
        lock (_queue)
        {
            ThrowIfDisposed();
            _currentNode = null;
            _cursorNextItemTcs?.TrySetCanceled();
        }
    }

    /// <summary>
    /// Wait for the queue to become empty and return. Note that this will not block for enqueue operations.
    /// </summary>
    public async Task WaitForEmpty(CancellationToken cancellationToken)
    {
        TaskCompletionSource<object> tcs;
        CancellationTokenRegistration registration;
        lock (_queue)
        {
            ThrowIfDisposed();

            if (_queue.Count == 0)
                return;

            tcs = new TaskCompletionSource<object>();
            registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
            _queueEmptyTcs.Add(tcs);
        }

        try
        {
            await tcs.Task.ConfigureAwait(false);
        }
        finally
        {
            registration.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_queue)
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;
        }

        _cursorSemaphore.Dispose();
        _cursorNextItemTcs?.TrySetCanceled(CancellationToken.None);
        _cursorWaitTcs?.TrySetResult(false);
        var disposeLock = _pendingLock.DisposeAsync();
        ReleasePendingLockGrant();
        await disposeLock.ConfigureAwait(false);

        foreach (var tcs in _queueEmptyTcs)
        {
            tcs.TrySetCanceled(CancellationToken.None);
        }

        lock (_queue)
        {
            foreach (var item in _queue)
            {
                item.Dispose();
            }
        }
    }

    private LinkedListNode<T>? GetNodeAfterCursor()
        => _currentNode is null || _currentNode.List is null ? _queue.First : _currentNode.Next;

    private void ThrowIfDisposed()
    {
        if (_isDisposed != 0)
            throw new AsyncQueueWithCursorDisposedException();
    }

    private void NotifyQueueEmptyAwaiters()
    {
        foreach (var tcs in _queueEmptyTcs)
        {
            tcs.TrySetResult(0);
        }

        _queueEmptyTcs.Clear();
    }

    private void ReleasePendingLockGrant()
    {
        lock (_pendingLock)
        {
            if (_pendingLockGrant is null)
                return;

            _pendingLockGrant.Dispose();
            _pendingLockGrant = null;
        }
    }
}
