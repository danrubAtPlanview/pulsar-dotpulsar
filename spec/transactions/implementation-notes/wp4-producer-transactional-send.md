# WP4 as built: transactional produce (merged)

Commits on `feature/transactions`: `Add transactional produce to the producer`, `Do not allocate per operation for a send`, `Add send benchmarks for transactions`, `Stop waiting for a completed send to register`,
`Add integration tests for transactional produce`.
After the merge: 1538 unit tests green in Debug, 1534 in Release (`MessageProcessorTests` excluded). 30 new integration tests pass; the transaction integration set was green in 4 of 8 attempts on the final code
(runs 7 and 8 consecutive); every failed attempt died in the **second container's startup** ("Failed to create token", or a hang over 5 minutes), before a test ran. See `followups/0006`.

## What was built

- `MessageMetadata.Transaction`, `IMessageBuilder.Transaction`, `MessageBuilder`, `MessageMetadataObjectPool.Return` reset.
- `Producer.InternalSend`, `SendOp`, the `SubProducer` dispatcher, per-message `NotAllowedError` handling (a send error no longer faults the producer), `ProducerChannel` pooled-command txn-id set and clear,
  `ProducerChannelFactory` `TxnEnabled` (passed from `PulsarClient`).
- `AsyncQueueWithCursor.RemoveCurrentItem` now releases a blocked enqueuer (before, a full queue never accepted another message after items were removed). Pre-existing bug, fixed with a test.
- `Transaction.BeginOp()` / `EndOp(bool failed)` (WP3 code changed): `RegisterSendOp`/`RegisterAckOp(Task)` are built on them; the commit/abort waiter is created lazily.
- `benchmarks/Transactions` (in `DotPulsar.sln`): `FakeConnection`, `FakeConnectionPool`, `FakeTransactionCoordinatorClient`, `SendBenchmarks`. `AckBenchmarks`/`AckSetBenchmarks` are still to do.
  BenchmarkDotNet's generated project hangs on restore (private feed): run with `--inProcess`.
- Test helpers: `FakeProducerConnection`, `MessageMetadataTestExtensions` (reflection over the internal protocol metadata; the library has no `InternalsVisibleTo`).

## Benchmark vs thresholds (Medium job, in-process, Release, .NET 10)

| | PlainSend | TxnSend_SteadyState | TxnSend_FirstPerPartition |
|---|---|---|---|
| Direct completion, us / B per op | 3.30 / 2933 | 3.26 / 2928 | 4.71 / 7202 |
| Thread-pool completion, us / B per op | 3.97 / 2988 | 3.41 / 2981 | 4.99 / 7250 |

P1: `PlainSend` allocates the same as master `e8f2ba2` (2932 / 2984 B); timing +2.7% direct (noise about 5%). P2: txn send adds +5 B direct and 0 B on the thread pool over a plain send (limit 64-112 B).
The first version cost 215 B (a continuation per send); the transaction op now ends in the continuation `InternalSend` already attaches.

## Deviations from the spec

1. `SendOp.RegistrationGate` is a `Task?` (null = nothing to wait for), not a `ValueTask`; `SendOp.Transaction` exists.
2. The producer uses `BeginOp`/`EndOp` instead of `RegisterSendOp(Task)`.
3. The send op is registered **before** `ChoosePartitions` (earlier than the spec), so R-OP-SYNC holds before the producer is connected; a commit started in that window fails
   (test `SendChannel_GivenCommitStartedBeforeProducerIsConnected_...`). `Encode` moved before `ChoosePartitions`; the open check runs before the sequence id is assigned.
4. The gate wait stops when the dispatcher stops or the message's receipt completes (caller cancel, or a future send timeout), not on the message's own cancellation token.
5. Rows that rely on Java's default batching (16, 17) are theories with `enableBatch` false (runs) and true (skipped, WP6).
6. `testDelayedDeliveryExceedsMaxDelay` is ported without encryption and send timeout (`TODO(WP7)`, `TODO(WP9)`). Row 23 covers the send half only (`TODO(WP5)`; the ack half is in `TransactionConsumerTests`). Row 29 awaits the receipts instead of a flush; C5 awaits the commits.

## Skipped tests

- 6 rows `[InlineData(true, Skip = "Requires producer batching (WP6)")]` + `TODO(WP6)` in `TransactionEndToEndTestsBase` (rows 4, 16, 17, in both concrete classes). WP6 removes the skips and adds `.EnableBatching(enableBatch)`.
- `TopicTransactionBufferPublishOrderTests` with `recovering: false`: on broker 4.0.9 the plain message is stored before the transactional one, so receipts arrive out of order and DotPulsar reconnects and resends (duplicates with deduplication off).
  Broker fix is apache/pulsar#27082 (master only); stays skipped until the image is bumped. `recovering: true` passes.
- Not covered: P1 `testAbortedTransactionDoesNotDiscardPlainMessageBatchedWithIt` (needs `Flush` and batching), `testSendTxnMessageTimeout` (WP7), encryption, chunking and DLQ rows.

## Notes for WP6 (batching)

- `SubProducer.TryAwaitRegistrationGate(sendOp, ct)` runs right after `NextItem` and before anything is written. Await the gate **before adding an op to a batch**. `false` means the op was already removed by `RemoveCurrentItem`: skip it.
- Group batches by `sendOp.Transaction` reference, or by the txn ids on `sendOp.Metadata`. The batch metadata must carry the txn ids: `ProducerChannel.Send` copies them to the command, or clears them when absent.
- The transaction op ends in `Producer.InternalSend`'s `ContinueWith` on each op's own `ReceiptTcs.Task`. Every `SendOp` in a batch must still get its own `ReceiptTcs` completed, or commit waits forever.
- `ProcessNotAllowedSendError` matches the head op only. WP6 must match the group and register the alias key (spec 06/producer-batching section 9.6).

## Notes for WP7 (send timeout)

- Complete the op's `ReceiptTcs` with `OperationTimeoutException`; commit then aborts through the existing `EndOp(true)`, and a dispatcher blocked on that op's gate stops waiting. Remove the op from the queue yourself when it was not at the cursor.

## Findings outside WP4's scope

- **R-MAX-SIZE does not hold today.** The spec says a too-large message fails only that send with `TooLargeMessageException`. In reality `Connection.Send` throws inside the dispatcher, the producer faults, and the send ends as `TaskCanceledException`.
  P7 passes anyway (its commit/abort assertions hold). Chunking (WP8) changes the `Connection.Send` size check; revisit there. See `followups/0006`.
- A `NotAllowedError` for an op behind the head of the queue triggers reconnect-and-resend (as the spec says). Java ignores it.
