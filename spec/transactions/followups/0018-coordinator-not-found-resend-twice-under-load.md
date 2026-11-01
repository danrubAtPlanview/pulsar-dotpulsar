# 0018 - `TransactionMetaStoreHandlerTests.Request_GivenTransactionCoordinatorNotFound_ShouldRetryEveryRequestType` sent a request 3 times in 1 of 24 stressed Release runs

Status: **fixed in WP10** (guard in `TransactionMetaStoreOp.TryBeginSend`, atomic recording in the test fake). The one related case that is left is **accepted as Java parity** (last section). Found in the Release stress rounds of the WP10 final verification.

## Fix (WP10)

1. `TransactionMetaStoreOp.TryBeginSend` checks the completion after it has taken the connection (hypothesis 1 below). The response claims the completion while its own send still holds the connection, and `EndSend` gives the connection back afterwards, so a resend that takes the connection after the end of a send always sees the completion. Unit tests: `TryBeginSend_GivenCompletedOperation_ShouldReturnFalse` and `TryBeginSend_GivenOperationCompletedByTheResponseOfTheSendThatHasEnded_ShouldReturnFalse`; both fail without the guard.
2. A second flake of the same class was found while looking for the first one: `Request_Given1000PendingOps_ShouldWaitForPermit` failed once in 60 stressed runs with "The request is not pending". `FakeCoordinator.Handle` recorded a request in `Requests` and made it pending in a second, separate critical section, so a thread that was descheduled in between left a request that `Requests` listed and `Answer` could not find. The fake records and registers a request in one step now (the test fake only; the handler was correct).
3. A third one, `SubProducerChunkingTests.SendTimeout_GivenTimeoutWhileAChunkIsBeingWritten_...`, failed once in 80 runs: it slept 250 ms after opening the write gate and then expected the chunk that was being written to be recorded. It waits for the recorded chunk first now, and then does its negative wait.

Stress results (Release unit suite, 4 parallel `dotnet test` processes and 12 busy loops per round): before the fixes 2 failures of the two coordinator tests in 84 runs; after the fixes no failure of them in 80 runs (one failure of the chunking test of item 3 in the same 80), and 40 of 40 runs passed on the final tree. The failure of the first test (3 requests instead of 2) was never captured again, so it is not proven that item 1 was its cause. Item 1 closes a real gap in any case: an operation that has completed must not be sent again.

## What was seen

Release build, the whole unit suite (`Category=Unit`, without `MessageProcessorTests`), 6 rounds of 4 parallel `dotnet test` processes with 12 busy loops as extra CPU load (`stress.sh`): 23 of 24 runs passed, 1 failed (round 1):

```
TransactionMetaStoreHandlerTests.Request_GivenTransactionCoordinatorNotFound_ShouldRetryEveryRequestType(request: NewTransaction)
  Shouldly.ShouldAssertException : coordinator.Requests.Count  should be 2  but was 3
```

The same test (4 rows) passed in 72 runs when it was stressed alone (12 rounds, 6 parallel processes, 14 busy loops), and in every normal run of the unit suite (Debug: 2137 passed; Release: 3 runs of 2144 passed). Debug was not stressed. The failure was not reproduced again, so the explanation below is a hypothesis from reading the code.

## Hypothesis (not confirmed)

`TransactionMetaStoreHandler.HandleResponse` handles `TransactionCoordinatorNotFound` like Java (`checkIfNeedRetryByError`) with two actions that both resend the operation: `RequestReconnect` (the connect loop reconnects, and `ResendPendingOps` sends every pending operation again) and `ScheduleResend` (a timer sends the operation after `op.NextRetryDelay()`, 100 ms for the first retry). Under load the order of the two can change, and a third request is possible in two ways that I have not told apart:

1. `ResendPendingOps` enumerates `_pendingOps` (a `ConcurrentDictionary`, so the enumeration is live) and reaches the operation just before the response of request 2 completes it. `CheckStateAndSend` then runs after `HandleResponse` has called `EndSend`, so `TryBeginSend` succeeds and the operation is sent once more, although it has completed. Nothing in `CheckStateAndSend` or `SendOnce` looks at `TryClaimCompletion` or at the cancelled `op.CancellationToken` before the send. The fake of the unit test records a request when it is sent, so a request that a real connection would reject for the cancelled token counts there.
2. In production the reconnect can produce another `IConnection` (the coordinator moved to another broker). `TryBeginSend` only prevents a second send on the same connection, so the timer resend and the reconnect resend can both be in flight on different connections. The fake pool of the unit test (`FakeCoordinator.CreatePool(1, coordinator)`) always returns the same connection, so this does not explain the test failure.

The effect of a duplicate request matters only for `NewTransaction`: if both requests are answered, the coordinator has created a second transaction that nobody uses until its timeout. `AddPublishPartitionToTxn` and `AddSubscriptionToTxn` are repeated registrations of the same transaction, and a repeated `EndTxn` is answered after the operation has completed, so its response is ignored (`TryClaimCompletion`). Java has the same two resend paths and no guard at all.

## The case that is accepted

Hypothesis 2 above (in production a reconnect can produce another `IConnection`, so the timer resend and the reconnect resend can both be in flight on different connections, and a `NewTransaction` request would then create a second transaction that nobody uses until its timeout) is not changed: Java has the same two resend paths, the unit test fake always returns the same connection, and a guard needs an attempt counter per operation (`ScheduleResend` remembers the attempt number of the failure, `ResendAfter` does nothing when the operation has been sent since). Severity low: it needs the coordinator to answer `TransactionCoordinatorNotFound` and a reconnect to another connection within the first retry delay of 100 ms.

Decision: accepted, no change. The same two resend paths exist in the Java client, which has no guard at all, so the behaviour is parity; it needs the coordinator to answer `TransactionCoordinatorNotFound` and the reconnect to reach another connection within the first retry delay of 100 ms; and the effect (an unused transaction that the coordinator ends by its timeout) does not change a result that the application sees. The attempt counter above is the change to make if the maintainers want more than parity.
