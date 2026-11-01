# 0009 - Flaky unit tests under CPU load

Status: **fixed**. The flake-hunting agent fixed three production races (16 commits, merged), and the smaller hazard that it left open (receipt vs write) is fixed in WP10 (section "Receipt vs write"), together with three more test flakes that the WP10 sign-off stress found. The stress of the sign-off of WP11 found one more, a hang in a test helper that WP11 added (section "WP11 sign-off stress"), fixed as well.
Found on the merged tip (after WP6, WP7, WP8 consumer and the ack allocation fixes) by running the Debug unit suite alone (1 failure in about 5 runs) and in 4 parallel `dotnet test` processes (about 1 failure per process).

## Stress procedure (repeat it for WP10 sign-off)

Build once and copy the test output folder to a snapshot, so a rebuild never disturbs a run. Per round, start 4 `dotnet test <snapshot>/DotPulsar.Tests.dll --no-build --blame-hang-timeout 90s --filter ... --logger trx` processes in parallel and wait for all of them. For heavy load add 22 busy-loop processes (`while :; do :; done`).
Filters: Debug `Category=Unit`; Release `Category=Unit&FullyQualifiedName!~MessageProcessorTests`. The script is `stress.sh` in the scratchpad folder of the implementation session (`...\scratchpad\flaky\stress.sh`).

| Configuration | Load | Rounds x processes | Result |
|---|---|---|---|
| Baseline `73df5e6`, Debug | none | 3 x 4 | 14 failures in 12 processes (`DisposeAsync_GivenCallerWaitingForPermit` 11, `SendTimeout_GivenFrozenBatch` 2, `Request_GivenTransactionCoordinatorNotFoundTwice...` 1) |
| Final tip, Debug | none | 10 x 4 | 40/40 pass |
| Final tip, Release | none | 10 x 4 | 40/40 pass |
| Final tip, Debug | 22 busy loops | 6 x 4 | 24/24 pass |
| Final tip, Release | 22 busy loops | 6 x 4 | 24/24 pass |

## Root causes and fixes

| Test | Root cause | Kind | Commit (merged subject) |
|---|---|---|---|
| `SendTimeout_GivenTimeoutWhileBatchIsBeingWritten`, `SendTimeout_GivenFrozenBatch` | The tests read the metadata of a returned batch, but all producers of the process rent batches from `PreparedBatchPool.Shared`; a returned batch is the next one handed out, so a parallel test re-rented it | test | `Isolate the tests that read a pooled batch` (collection `SharedBatchPool`) |
| `DisposeAsync_GivenCallerWaitingForPermit` | **Production race** in `TransactionMetaStoreHandler.AcquirePermit`: `SemaphoreSlim` notices a cancelled wait asynchronously, so a permit released by `FailAllPending` could reach the cancelled waiter, which then registered with the closing handler and got `MetaStoreHandlerNotReadyException`. Also the semaphore was disposed at the end of dispose, so a late release threw and was swallowed (999 permits left) | production | `Fail a caller that waits for a permit on dispose` (the waiter gives its permit back and throws `PulsarClientClosedException`; the semaphore is no longer disposed) |
| `EnsureStarted_GivenCancelledCallerAndFailedStart` | **Production race** in `TransactionCoordinatorClient.EnsureStarted`: a failed start resets the state to None and faults its task afterwards; a caller in between joined the failing start instead of retrying | production | `Start again when the failed start has reset the state` |
| SubProducer write path (found while analysing the send-timeout flakes) | **Production race**: the dispatcher sets `_sendingHead` under the lock and writes the group afterwards, but `SendGroup`/`SendBatch` looked at the message again; a send timeout in between made them see a message that was no longer in a group, so the first message was written alone with its own metadata, or `Batch!` was null (NullReferenceException that would fault the producer) | production | `Write the batch the dispatcher took from the queue` (the batch captured under the lock is passed to the write); regression test `SubProducerSendTimeoutBatchTests` |
| `Request_GivenTransactionCoordinatorNotFoundTwiceThenSuccess` | reconnect and retry timer both fire at about 100 ms; the 200 ms lower bound was never guaranteed | test | `Expect only the first backoff in the retry test` |
| `SendTimeout_GivenBatchThatWaitedForItsDelay`, `SendTimeout_GivenLateReceiptAfterTimeout` and 4 more | too small margins on sleeps | test | `Measure the send timeout tests by the time`, `Give the send timeout tests room to be answered` |
| `ConnectionTests...ShouldNotCloseConnectionOnInactivity`, `Connect_GivenRetriableErrorsUntilDeadline_ShouldFail` | handler created after `Connect`; deadline started at construction; state read too early | test | `Give two timing tests of the transactions room to start`, `Wait for the Failed state in the lookup deadline test` |
| `EnsureStarted_GivenStartFails_ShouldThrowToEveryWaiter...` | `Task.Run` callers had not joined the start before the test failed it | test | `Let all callers join the start that the test fails` |
| `BatchAckTrackerTests...ConcurrentAcksOfAll32Indexes` | starting 3200 threads took 2m45s under saturation and hit the hang kill | test | `Start the threads of the concurrent ack test once` |
| `AckSetWriterTests.WriteIndividual_AfterWarmup_ShouldNotAllocate` (6544 B once) | a one-time runtime allocation on the thread (cause not isolated) | test | `Measure the allocations of a thread more than once` (smallest of up to 5 attempts, `AllocationMeasurement.SmallestBytesOfCurrentThread`) |
| `EciesKeyWrapTests.WrapUnwrap_GivenManyKeys...` | probabilistic: about 4 in 10000 runs fail by chance (a leading zero in 1 of 128 keys) | test | `Do not leave the leading zero test to luck` |
| `SubConsumerTransactionalAckTests...ReceiptOnThreadPool` (101.9 B against 64 B) | continuation timing shifts the allocation count on a busy machine | test | `Measure more pairs in the thread pool ack allocation tests` |
| `BatchingAllocationTests.BatchingRound_GivenSteadyState(KeyBased, 100)` (4120 B once) | probably `ArrayPool<byte>.Shared` interference from parallel tests; not reproduced alone (mitigation, not a proven cause) | test | `Do not run the batching allocation tests in parallel` (exclusive collection) |

Not seen on the merged tip: `SubConsumerTests.Acknowledge_GivenTransaction_ShouldRegisterAckOpSoThatCommitWaitsForTheReceipt`; the unknown Release failure of followup 0008 item 2.
Seen once, under load, on the producer agent's run: `SubProducerSendTimeoutTests.SendTimeout_GivenQueueFullAndEnqueuerBlocked_ShouldUnblockEnqueuerAfterTimeout` (class passed 15/15 alone). The hunter widened the margins of the other send-timeout tests; this one was not in its list. Include it in the WP10 stress runs.

## Receipt vs write (production hazard, fixed in WP10)

`SubProducer.ProcessReceipt` returned the batch to the pool even while the dispatcher was still inside `ProducerChannel.Send` for it. A receipt can only follow the full frame write, so the payload is never read after the return, but the `finally` of `ProducerChannel.Send` resets the compression and the uncompressed size on the batch metadata after the write: it would have corrupted another producer's batch if the dispatcher thread had been descheduled for longer than a broker round trip while another producer rented that batch.

Fix: `PreparedBatch` knows when it is written. The dispatcher calls `BeginWrite` when it takes the batch and `EndWrite` in its `finally`, after the channel is done with the metadata. A `Return` that comes while the batch is written (from the receipt, the failure or the send timeout) is done by `EndWrite`. Both sides write their own flag with a full fence and then read the flag of the other one, so at least one of them sees the other, and `Return` does nothing for a batch that has been given back already. Tests: four `PreparedBatchTests` (a return during the write waits for the end of the write, an `EndWrite` without a return gives nothing back, a double return gives the batch out once, the state does not survive a new rent) and `SubProducerBatchOwnershipTests.Dispatcher_GivenReceiptBeforeTheWriteOfTheBatchHasReturned_ShouldReturnTheBatchWhenTheWriteIsDone` (a new `RespondBeforeWriteCompletes` option of `FakeProducerChannel` answers the entry before the write has returned); the dispatcher test fails without the `BeginWrite` and `EndWrite` calls and passed 10 of 10 runs with them.

Side effect: when a write fails, the batch is given back when the dispatcher leaves the write, a few microseconds after the messages are failed (before, in the same call). Two tests of `SubProducerBatchOwnershipTests` read `IsReturned` at once and failed in Debug; they wait for the return now.

## WP10 sign-off stress (Release)

Procedure as above, 4 parallel processes per round and 12 busy loops. Findings and fixes (all tests, none in production code except item 1):

| Run | Result | Cause and fix |
|---|---|---|
| 6 rounds (24 runs) | 1 failure: `Request_GivenTransactionCoordinatorNotFound_ShouldRetryEveryRequestType` saw 3 requests | followup 0018 (guard in `TransactionMetaStoreOp.TryBeginSend`) |
| 15 rounds (60 runs) | 1 failure: `Request_Given1000PendingOps_ShouldWaitForPermit` | the test fake recorded and registered a request in two critical sections (followup 0018, item 2) |
| 20 rounds (80 runs), after both fixes | 1 failure: `SubProducerChunkingTests.SendTimeout_GivenTimeoutWhileAChunkIsBeingWritten_...` | a fixed 250 ms sleep before a positive assertion; waits for the condition now |
| 10 rounds (40 runs), final tree | 0 failures | |
| 10 rounds (40 runs), tree with the state fixes of followups 0016 and 0017 | 1 failure: `SubProducerSendTimeoutTests.SendTimeout_GivenQueueFullAndEnqueuerBlocked_ShouldUnblockEnqueuerAfterTimeout` (the message that was blocked was not written within 10 s) | not reproduced (150 runs of the class alone under the same load passed; about 1 in 300 stressed runs over all the rounds). Likely cause, from the code and the message: the send timeout of that test is 200 ms, so a machine that does not schedule the dispatcher within that time lets the timeout remove the first message unsent, and then only one message is ever written. The test gives the first message a send timeout of 1 s and waits until it is written before it sends the second one (commit `Wait for the first message in a send timeout test`). Not proven, because it did not reproduce |
| 15 rounds (60 runs), with that fix | 1 failure: `AsyncQueueWithCursorTests.Dequeue_GivenQueueIsFullAndEnqueuePending_ShouldDequeueAndEnqueue`, a test of master (`TaskCanceledException` after 179 ms) | its pending enqueue has a token that is cancelled after 100 ms, and under this load the test took longer to reach `Dequeue`. Explained from the test; fixed with a safety-net token of 10 s (commit `Give the pending enqueue test time to finish`) |
| 15 rounds (60 runs), with those fixes | 1 failure: `ProducerSendTimeoutTests.Send_GivenBatchingAndNoReceiptWithinSendTimeout_ShouldFailEveryMessageOfTheBatch` (the batch had not been written when the send timeout of 300 ms removed it) | the same cause as the one of the sub producer test above, which confirms it; the two tests of that class that assert what was written before the timeout use a send timeout of 2 s (commit `Give two send timeout tests a longer timeout`) |

The remaining rate under this load is about one failed run in 60, and each failure so far was a different test that depends on a short real-time window (a fixed sleep, a timeout of 100 to 300 ms, or a non-atomic record in a test double), never a library defect except the guard of item 1. Every pass of the stress finds at most one more test of this kind, so the list is not finished: the unit suite has many tests with short windows, and the load (4 full suites and 12 busy loops on 22 cores, so more busy threads than cores) is far beyond what a CI agent shows. The load is far above what a CI agent shows (4 full suites and 12 busy loops on 22 cores). If new failures appear, look first for `Task.Delay` followed by an immediate assertion and for test doubles that update two collections in separate locks.

## WP11 sign-off stress (Release)

Same procedure (4 parallel processes per round, 12 busy loops, `Category=Unit`, hang timeout 120 s). One more thing to check: when the test host is killed by the hang detector, `dotnet test` still prints `Passed!` with the number of the tests that had finished, and also `Test Run Aborted`. A run is only good if it has the full total (2290) and no `Test Run Aborted`. The script `stress2.sh` (same folder as `stress.sh`) counts the aborted runs.

| Run | Result | Cause and fix |
|---|---|---|
| 13 rounds and a part of the 14th (56 runs started; I stopped it) | 21 of the 56 runs aborted, all in the same test, `LossyRelayTests.DropClientConnections_GivenConnection_ShouldCloseTheSideOfTheClientAndKeepTheSideOfTheBrokerOpen` (2244 tests had finished, 46 had not started). No test failed. | A race in the test helper `LossyRelay` (added in WP11-A): `Accept` connected to the broker and only then added the client socket to the lists that `DropClientConnections` closes. The test waits until the broker has accepted, and then drops: under load that can be before the registration, the drop closes nothing and the client read waits for ever. Not a library defect. |
| `LossyRelayTests` alone, 8 rounds (32 runs), before the fix | 10 of 32 aborted | reproduces the cause |
| `LossyRelayTests` alone, 16 rounds (64 runs), with the fix (commit `Register the relay client before the broker`) | 0 of 64 aborted | the client is added first; if `DropConnections` runs before the broker is connected, the broker socket is closed |
| 15 rounds (60 runs) of the whole unit suite, with the fix | 60 of 60 runs: 2290 passed, none aborted | |
| 12 rounds (48 runs) of the whole unit suite after the negative-ack redelivery backoff (WP11-D, tip `f96b0a03`) | 48 of 48 runs: 2306 passed, none aborted | |
| 8 rounds (32 runs) of the whole unit suite after the follow-up items (tip `fc04185d`: ids outside the lookups, configurable broker image, stale chunk requests) | 32 of 32 runs: 2340 passed, none aborted | |

The two integration classes that use the relay (`ProducerReconnectTests`, `BatchMessageTests`) ran again with the fix: 45 passed. As in the earlier passes this does not prove that no other test of this kind is left; it is the same list that is still not finished.

## Side effects of the fixes

The exclusive collections `SharedBatchPool` and `Allocations` add a few seconds at the end of the unit run; the send-timeout tests that need room to be answered take 200 ms to 1 s each.
