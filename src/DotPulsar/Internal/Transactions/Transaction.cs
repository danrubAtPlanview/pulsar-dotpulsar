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

namespace DotPulsar.Internal.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using System.Collections.Concurrent;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionImpl.java
public sealed class Transaction : ITransaction
{
    // The longest due time of a timer is 4294967294 milliseconds
    private static readonly TimeSpan _maxTimerDueTime = TimeSpan.FromMilliseconds(uint.MaxValue - 1);
    private static readonly TaskCompletionSource<object?> _noOps = CreateCompletedSource();

    private readonly ITransactionCoordinatorClient _tcClient;
    private readonly ConcurrentDictionary<string, Task> _registeredPartitions;
    private readonly ConcurrentDictionary<(string Topic, string Subscription), Task> _registeredSubscriptions;
    private readonly object _registrationLock;
    private readonly object _stateLock;
    private readonly StateManager<TransactionState> _stateManager;
    private readonly Timer _timer;
    private volatile TaskCompletionSource<object?> _opsCompleted;
    private volatile bool _hasOpsFailed;
    private volatile int _state;
    private long _opCount;

    public Transaction(ITransactionCoordinatorClient tcClient, TransactionId id, TimeSpan timeout)
    {
        _tcClient = tcClient;
        _registeredPartitions = new ConcurrentDictionary<string, Task>(StringComparer.Ordinal);
        _registeredSubscriptions = new ConcurrentDictionary<(string Topic, string Subscription), Task>();
        _registrationLock = new object();
        _stateLock = new object();
        _stateManager = new StateManager<TransactionState>(TransactionState.Open, TransactionState.Committed, TransactionState.Aborted, TransactionState.Error, TransactionState.TimedOut);
        _opsCompleted = _noOps;
        _hasOpsFailed = false;
        _state = (int) TransactionState.Open;
        _opCount = 0;
        Id = id;
        _timer = new Timer(static state => ((Transaction) state!).OnTimeout(), this, timeout < _maxTimerDueTime ? timeout : _maxTimerDueTime, Timeout.InfiniteTimeSpan);
    }

    public TransactionId Id { get; }

    public TransactionState CurrentState => (TransactionState) _state;

    public IState<TransactionState> State => _stateManager;

    public bool IsOpen => _state == (int) TransactionState.Open;

    // Upstream: ProducerBase#newMessage, ConsumerBase#acknowledgeAsync (checkArgument)
    public static Transaction From(ITransaction transaction)
        => transaction as Transaction ?? throw new ArgumentException("The transaction must be created by DotPulsar", nameof(transaction));

    // Upstream: TransactionImpl#checkIfOpen
    public void EnsureOpen()
    {
        var state = CurrentState;
        if (state != TransactionState.Open)
            throw new InvalidTransactionStatusException($"[{Id}] with unexpected state : {state}, expect Open state!");
    }

    // Upstream: TransactionImpl#registerProducedTopic
    public ValueTask RegisterProducedTopic(string topic, CancellationToken cancellationToken)
    {
        EnsureOpen();

        if (!_registeredPartitions.TryGetValue(topic, out var task))
            task = RegisterPartition(topic);

        return ToValueTask(task, cancellationToken);
    }

    // Upstream: TransactionImpl#registerAckedTopic
    public ValueTask RegisterAckedTopic(string topic, string subscription, CancellationToken cancellationToken)
    {
        EnsureOpen();

        if (!_registeredSubscriptions.TryGetValue((topic, subscription), out var task))
            task = RegisterSubscription(topic, subscription);

        return ToValueTask(task, cancellationToken);
    }

    // Upstream: TransactionImpl#registerSendOp
    public void RegisterSendOp(Task sendTask)
        => RegisterOp(sendTask);

    // Upstream: TransactionImpl#registerAckOp
    public void RegisterAckOp(Task ackTask)
        => RegisterOp(ackTask);

    // Upstream: TransactionImpl#commit
    public async ValueTask Commit(CancellationToken cancellationToken)
    {
        _timer.Dispose();

        if (!TrySetState(TransactionState.Committing, TransactionState.Open, TransactionState.Committing))
            throw new InvalidTransactionStatusException(CreateInvalidStateMessage(TransactionState.Open, TransactionState.Committing));

        // Cancelling stops the wait of the caller. The transaction stays in the 'Committing' state, so that the commit can be tried again
        await _opsCompleted.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        if (_hasOpsFailed)
        {
            if (TrySetState(TransactionState.Aborting, TransactionState.Committing))
            {
                try
                {
                    await AbortCore().ConfigureAwait(false);
                }
                catch
                {
                    // The commit fails with the same exception whether the abort succeeds or not
                }
            }

            throw new TransactionHasOperationFailedException();
        }

        try
        {
            await _tcClient.Commit(Id, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (exception is TransactionNotFoundException or InvalidTransactionStatusException)
                SetState(TransactionState.Error);

            throw;
        }

        SetState(TransactionState.Committed);
    }

    // Upstream: TransactionImpl#abort
    public async ValueTask Abort(CancellationToken cancellationToken)
    {
        _timer.Dispose();

        if (!TrySetState(TransactionState.Aborting, TransactionState.Open, TransactionState.Aborting))
            throw new InvalidTransactionStatusException(CreateInvalidStateMessage(TransactionState.Open, TransactionState.Aborting));

        await _opsCompleted.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        await AbortCore().ConfigureAwait(false);
    }

    // Upstream: TransactionImpl#internalAbort
    private async Task AbortCore()
    {
        try
        {
            await _tcClient.Abort(Id, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (exception is TransactionNotFoundException or InvalidTransactionStatusException)
                SetState(TransactionState.Error);

            throw;
        }

        SetState(TransactionState.Aborted);
    }

    // Upstream: TransactionImpl#run
    private void OnTimeout()
        => _ = TrySetState(TransactionState.TimedOut, TransactionState.Open);

    private Task RegisterPartition(string topic)
    {
        lock (_registrationLock)
        {
            if (_registeredPartitions.TryGetValue(topic, out var task))
                return task;

            // The request is shared by every send to the partition, so the cancellation token of the first caller must not cancel it
            task = _tcClient.AddPublishPartitionToTxn(Id, topic, CancellationToken.None).AsTask();
            _registeredPartitions[topic] = task;
            return task;
        }
    }

    private Task RegisterSubscription(string topic, string subscription)
    {
        lock (_registrationLock)
        {
            if (_registeredSubscriptions.TryGetValue((topic, subscription), out var task))
                return task;

            task = _tcClient.AddSubscriptionToTxn(Id, topic, subscription, CancellationToken.None).AsTask();
            _registeredSubscriptions[(topic, subscription)] = task;
            return task;
        }
    }

    private static ValueTask ToValueTask(Task task, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_0
        if (task.Status == TaskStatus.RanToCompletion)
#else
        if (task.IsCompletedSuccessfully)
#endif
            return default;

        return cancellationToken.CanBeCanceled ? new ValueTask(task.WaitAsync(cancellationToken)) : new ValueTask(task);
    }

    private void RegisterOp(Task opTask)
    {
        if (Interlocked.Increment(ref _opCount) == 1)
            _opsCompleted = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        if (opTask.IsCompleted)
            OnOpCompleted(opTask);
        else
            _ = opTask.ContinueWith(static (task, state) => ((Transaction) state!).OnOpCompleted(task), this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private void OnOpCompleted(Task opTask)
    {
        if (opTask.IsFaulted || opTask.IsCanceled)
        {
            _ = opTask.Exception; // The caller of the operation gets the exception, so it is observed here
            _hasOpsFailed = true;
        }

        var opsCompleted = _opsCompleted;
        if (Interlocked.Decrement(ref _opCount) == 0)
            _ = opsCompleted.TrySetResult(null);
    }

    // The state of the transaction and the state of the state manager are changed together, and a final state never changes
    private bool TrySetState(TransactionState next, params TransactionState[] expected)
    {
        lock (_stateLock)
        {
            if (!expected.Contains(CurrentState))
                return false;

            SetStateCore(next);
            return true;
        }
    }

    private void SetState(TransactionState next)
    {
        lock (_stateLock)
        {
            SetStateCore(next);
        }
    }

    private void SetStateCore(TransactionState next)
    {
        if (_stateManager.IsFinalState((TransactionState) _state))
            return;

        _state = (int) next;
        _ = _stateManager.SetState(next);
    }

    private string CreateInvalidStateMessage(params TransactionState[] expected)
        => $"[{Id.MostSigBits}:{Id.LeastSigBits}] with unexpected state: {CurrentState}, expect: [{string.Join(", ", expected)}]";

    private static TaskCompletionSource<object?> CreateCompletedSource()
    {
        var source = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetResult(null);
        return source;
    }
}
