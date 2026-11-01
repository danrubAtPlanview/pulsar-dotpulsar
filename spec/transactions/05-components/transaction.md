# Component: `Transaction`

| | |
|---|---|
| C# | `src/DotPulsar/Internal/Transactions/Transaction.cs`: `public sealed class Transaction : ITransaction` |
| Upstream | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionImpl.java` (280 lines) |
| WP | WP3 (class), WP4/WP5 (callers) |

## 1. Responsibilities

Java `TransactionImpl` has five jobs, and this class keeps all of them:

1. Hold the `TransactionId` and the client-side state machine.
2. Arm the client-side timeout, which only flips `Open` to `TimedOut`.
3. Register produced partitions and acked subscriptions with the TC, once per key per transaction.
4. Track in-flight sends and acks (ops) so that commit and abort wait for them, and remember whether any op failed.
5. Commit or abort through the TC client.

## 2. Fields

| C# field | Type | Java | Notes |
|---|---|---|---|
| `_tcClient` | `ITransactionCoordinatorClient` | `tcClient` | |
| `_timeout` | `TimeSpan` | `transactionTimeoutMs` | |
| `_registeredPartitions` | `ConcurrentDictionary<string, Task>` (ordinal comparer) | `registerPartitionMap` | Key: partition topic name |
| `_registeredSubscriptions` | `ConcurrentDictionary<(string Topic, string Subscription), Task>` | `registerSubscriptionMap` (`Pair<topic, sub>`) | A value-tuple key does not allocate on lookup |
| `_registrationLock` | `object` | (atomic `compute`) | Taken only on the registration **miss** path |
| `_opCount` | `long` | `opCount` + `OP_COUNT_UPDATE` | Interlocked |
| `_opsCompleted` | `TaskCompletionSource<object?>` (volatile reference) | `opFuture` | Starts completed. A new one is created on the 0→1 transition. |
| `_hasOpsFailed` | `volatile bool` | `hasOpsFailed` | Set once, never reset |
| `_state` | `volatile int` (cast of `TransactionState`) | `state` + `STATE_UPDATE` | Hot-path read |
| `_stateLock` | `object` | (CAS) | Guards transitions **and** `_stateManager` updates, so the two never disagree |
| `_stateManager` | `StateManager<TransactionState>` | — | Backs `IStateHolder.State`. Final states: `Committed`, `Aborted`, `Error`, `TimedOut`. |
| `_timer` | `System.Threading.Timer` | `timeout` (`HashedWheelTimer` task) | Created in the ctor with `Timeout.InfiniteTimeSpan` period |

Public members: `TransactionId Id`, `TransactionState CurrentState => (TransactionState) _state`,
`IState<TransactionState> State => _stateManager`.

## 3. Constructor

```csharp
public Transaction(ITransactionCoordinatorClient tcClient, TransactionId id, TimeSpan timeout)
```

Upstream: `TransactionImpl(client, transactionTimeoutMs, least, most)` L94-104.

1. `_state = Open`; `_stateManager = new StateManager<TransactionState>(TransactionState.Open, TransactionState.Committed, TransactionState.Aborted, TransactionState.Error, TransactionState.TimedOut)`.
2. Assign all fields. `_opsCompleted` is a TCS completed at construction; you can keep one shared static completed
   instance to avoid an allocation, as long as it is never completed again.
3. `_timer = new Timer(static state => ((Transaction) state!).OnTimeout(), this, timeout, Timeout.InfiniteTimeSpan);`
   (upstream L103 `client.getTimer().newTimeout(this, transactionTimeoutMs)`).

## 4. Methods

### 4.1 `OnTimeout()` (private), upstream `run(Timeout)` L83-85

`TryTransition(TransactionState.Open, TransactionState.TimedOut)`. Nothing else happens: the broker times the
transaction out on its own.

### 4.2 `ValueTask RegisterProducedTopic(string topic, CancellationToken cancellationToken)`, upstream `registerProducedTopic` L107-122

1. If `CurrentState != Open`, throw `InvalidTransactionStatusException(InvalidStatusMessage(Open))`
   (`checkIfOpen` L258-267, message format in [05/exceptions](exceptions.md) §3).
2. **Fast path, no allocation:** `if (_registeredPartitions.TryGetValue(topic, out var task))`, return
   `task.IsCompletedSuccessfully ? default : new ValueTask(task)`. On netstandard2.0 use
   `task.Status == TaskStatus.RanToCompletion` instead of `IsCompletedSuccessfully`.
3. **Miss:** inside `lock (_registrationLock)`, re-check `TryGetValue`. If still missing, run
   `task = _tcClient.AddPublishPartitionToTxn(Id, topic, CancellationToken.None).AsTask()` and `_registeredPartitions[topic] = task`.
   Do not pass the caller's token to the TC request: the cached task is shared, and Java futures are not cancellable.
4. Return `new ValueTask(task)`. When `cancellationToken.CanBeCanceled`, return `new ValueTask(task.WaitAsync(cancellationToken))`
   on net6+, and a `Task.WhenAny`-based helper on netstandard. The helper goes in
   `Internal/Extensions/TaskExtensions.cs` (new) if no such helper exists yet.
5. A **failed** registration task stays cached (Java does the same: `compute` returns the existing future, which
   is completed exceptionally). Every later send to that topic in this transaction fails with the same exception.
   Port this behaviour exactly.

### 4.3 `ValueTask RegisterAckedTopic(string topic, string subscription, CancellationToken cancellationToken)`, upstream `registerAckedTopic` L148-163

The same as §4.2, keyed by `(topic, subscription)`, calling `_tcClient.AddSubscriptionToTxn(Id, topic, subscription, CancellationToken.None)`.

### 4.4 `void RegisterSendOp(Task sendTask)` / `void RegisterAckOp(Task ackTask)`, upstream L124-145 / L165-186

The two methods share one private implementation, `RegisterOp(Task opTask)`:

1. `if (Interlocked.Increment(ref _opCount) == 1) _opsCompleted = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);`
   This mirrors Java's `getAndIncrement() == 0`.
2. If `opTask` is already complete, call `OnOpCompleted(opTask)` synchronously. This avoids allocating a
   continuation on the hot path when the receipt arrived before registration.
3. Otherwise:
   `opTask.ContinueWith(static (t, s) => ((Transaction) s!).OnOpCompleted(t), this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);`
4. `OnOpCompleted(Task t)`:
   - If `t.IsFaulted || t.IsCanceled`, set `_hasOpsFailed = true`. Java treats any exceptional completion as a
     failure, and cancellation is exceptional in Java.
   - `var tcs = _opsCompleted;` (read **before** the decrement, as in Java L136/L177)
   - `if (Interlocked.Decrement(ref _opCount) == 0) tcs.TrySetResult(null);`

**Producer side.** The send `Task` passed in must be the task that completes when the **receipt** arrives, or fails.
The C# producer returns `ValueTask<MessageId>`; see [producer-integration](producer-integration.md) for the
allocation-free way to get a `Task` (`SendOp.ReceiptTcs.Task`).

### 4.5 `ValueTask Commit(CancellationToken cancellationToken)`, upstream `commit` L189-217

1. `_timer.Dispose()` (Java `timeout.cancel()`, which runs first even if the state check fails).
2. `CheckState(Open, Committing)`. If neither matches, throw `InvalidTransactionStatusException(CheckStateMessage(Open, Committing))`.
3. `SetState(Committing)`. This is an unconditional write under `_stateLock`, as in Java L195 `this.state = COMMITTING`.
4. `await _opsCompleted.Task` (honouring `cancellationToken` for the caller's wait only).
5. If `_hasOpsFailed`:
   1. `CheckState(Committing)`. If that fails, go straight to step 5.3 without aborting
      (Java `checkState(COMMITTING).thenCompose(internalAbort)`: a failed check skips the abort).
   2. `await InternalAbort()`, ignoring its outcome.
   3. Throw `new TransactionHasOperationFailedException()`, using the default message from [exceptions](exceptions.md).
6. Otherwise `await _tcClient.Commit(Id, CancellationToken.None)`:
   - On `TransactionNotFoundException` or `InvalidTransactionStatusException`, `SetState(Error)` and rethrow.
   - On any other exception, rethrow with the state unchanged (still `Committing`).
   - On success, `SetState(Committed)`.

### 4.6 `ValueTask Abort(CancellationToken cancellationToken)`, upstream `abort` L220-224

1. `_timer.Dispose()`.
2. `CheckState(Open, Aborting)`, else throw.
3. `await InternalAbort()`.

### 4.7 `InternalAbort()` (private), upstream L226-246

1. `SetState(Aborting)`.
2. `await _opsCompleted.Task`. Java uses `whenComplete`, so failed ops are ignored here.
3. `await _tcClient.Abort(Id, CancellationToken.None)`. Error mapping is the same as commit: `TransactionNotFound` or
   `InvalidTxnStatus` → `Error`, then rethrow. On success, `SetState(Aborted)`.

### 4.8 State helpers

```csharp
private bool TryTransition(TransactionState expected, TransactionState next)   // CAS semantics under _stateLock
private void SetState(TransactionState next)                                   // unconditional, under _stateLock, also _stateManager.SetState
private bool CheckState(params TransactionState[] expected)                    // reads _state once
```

Allocation note: `CheckState(params ...)` allocates an array per call. That is acceptable on commit/abort, which are
per-transaction operations. Alternatively, write the two-argument overload explicitly.

**`StateManager` interplay.** `StateManager.SetState` ignores updates once a final state is reached. Because every
transition goes through `_stateLock` and Java never leaves a final state, the two state stores stay consistent.

### 4.9 `IsOpen` (hot path)

`public bool IsOpen => _state == (int) TransactionState.Open;` is used by the producer and consumer for the fast
`checkIfOpen`.

## 5. Concurrency and edge cases (port verbatim)

| Case | Behaviour |
|---|---|
| `Commit()` called twice concurrently | Both pass `CheckState(Open, Committing)`, and both send `EndTxn(COMMIT)`. The second normally gets `InvalidTxnStatus` or `TransactionNotFound` from the TC, so the state becomes `Error` **or** `Committed`, depending on which response lands last. This is upstream behaviour (`TransactionTest#testEndTxnWhenCommittingOrAborting`); do not "fix" it. |
| Timer fires during `Commit` | Cannot happen: the timer is disposed first, and the CAS only moves from `Open`. |
| Timer fires after `Open` and before the first op | The state becomes `TimedOut`. Every later send, ack, commit or abort throws `InvalidTransactionStatusException` (`TransactionEndToEndTest#testTxnTimeOutInClient`). |
| Op registered after commit has started | Java allows it (no check in `registerSendOp`), but the producer and consumer call `RegisterProducedTopic`/`RegisterAckedTopic` first, and those check `Open`. Same in C#. |
| `_opsCompleted` swap race | Same as Java: commit may wait on an older TCS that already completed while a new op starts. Upstream accepts this, so keep it. |

## 6. Performance

- The steady-state send or ack hot path (partition already registered) allocates **nothing** in `Transaction`:
  one `TryGetValue` on a `ConcurrentDictionary` with a string key, an `IsCompletedSuccessfully` check, one
  `Interlocked.Increment`, and either a synchronous completion or one continuation registration. The
  `ContinueWith` delegate is static and takes its state as an argument; the continuation object itself is a
  framework allocation, accepted and measured in [07-performance](../07-performance.md).
- Per-transaction allocations (accepted): the `Transaction` object, two dictionaries, a `Timer`, a `StateManager`,
  and one TCS per busy period.

## 7. Unit tests (`tests/DotPulsar.Tests/Internal/Transactions/TransactionTests.cs`)

Each test mocks `ITransactionCoordinatorClient` with NSubstitute.

| Test | Upstream intent |
|---|---|
| `RegisterProducedTopic_GivenSameTopicTwice_ShouldSendOneRequest` | `registerPartitionMap.compute` caching |
| `RegisterProducedTopic_GivenFailedRegistration_ShouldFailLaterCallsWithSameException` | cached failed future |
| `RegisterProducedTopic_GivenStateNotOpen_ShouldThrowInvalidTransactionStatusException` | `checkIfOpen` |
| `RegisterAckedTopic_GivenSameTopicAndSubscriptionTwice_ShouldSendOneRequest` | ack caching |
| `RegisterAckedTopic_GivenDifferentSubscription_ShouldSendAnotherRequest` | key includes subscription |
| `Commit_GivenPendingOps_ShouldWaitForOpsBeforeEndTxn` | `opFuture` |
| `Commit_GivenFailedOp_ShouldAbortAndThrowTransactionHasOperationFailedException` | L197-200; final state `Aborted` |
| `Commit_GivenTransactionNotFound_ShouldSetErrorState` / `..._GivenInvalidTxnStatus_...` | L205-208 |
| `Commit_GivenOtherError_ShouldKeepCommittingState` | |
| `Commit_GivenCommittingState_ShouldSendEndTxnAgain` | `checkState(OPEN, COMMITTING)` |
| `Commit_GivenCommittedState_ShouldThrowInvalidTransactionStatusException` | |
| `Abort_GivenOpenTransaction_ShouldSetAborted` | |
| `Timeout_GivenElapsed_ShouldSetTimedOutAndRejectCommitAndAbort` | `run` + `checkState` |
| `Commit_ShouldCancelTimer` | `TransactionTest#testCancelTxnTimeout`. Short timeout; commit first; wait for timeout + margin; state stays `Committed`. |
| `State_GivenCommit_ShouldNotifyOnStateChangeTo` | `IStateHolder` integration |
| `RegisterSendOp_GivenConcurrentOps_ShouldCompleteWhenAllDone` | 30 tasks completing concurrently (`TransactionTest#testAsyncSendOrAckForSingleFuture`, unit form) |

## 8. Helpers used by the producer and consumer integration

| Member | Behaviour | Upstream |
|---|---|---|
| `public static Transaction From(ITransaction transaction)` | `transaction as Transaction ?? throw new ArgumentException("The transaction must be created by DotPulsar", nameof(transaction))` | `ProducerBase#newMessage` L91-104 `checkArgument(txn instanceof TransactionImpl)`; `ConsumerBase#acknowledgeAsync` L687-707 |
| `public void EnsureOpen()` | `if (!IsOpen) throw new InvalidTransactionStatusException(...)`, with the `checkIfOpen` message format from [exceptions §3](exceptions.md#3-message-formats-port-exactly-tests-may-compare) | `TransactionImpl#checkIfOpen` L258-267 |
| `public void RegisterSendOp(Task sendTask)` | §4.4. Takes `Task`, not `Task<MessageId>`, so that the same method also serves acks. | L124-145 |
