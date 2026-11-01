# WP6 as built: producer batching and send timeout (merged)

Commits on `feature/transactions`: `Add batching to the producer`, `Add batching benchmarks`, `Enable the integration tests that need batching`, `Add a send timeout to the producer`, `Add the integration tests of the send timeout`.
The WP6 agent worked from the WP4 base; the merge conflicted in `CHANGELOG.md`, `ProducerOptions.cs` (kept `InitialSubscriptionName` and the batching options) and `TransactionConsumerEndToEndTestsBase.cs` (kept the tip, which already ported the WP7 tests).
After the merge together with WP8 consumer and the ack allocation fixes: 1885 unit tests pass in Debug (11 allocation tests skip in Debug), 1893 in Release (`MessageProcessorTests` excluded), see "Flaky unit tests" in followup 0009.
The merged state has **not** been run against a broker yet. On the agent's own base: Integration collection 104-111 passed; Transaction collection (default fixture) 99 passed; NOBIA fixture 21 passed.

## Public API added

- `ProducerOptions`: `BatcherType`, `BatchingMaxBytes`, `BatchingMaxMessages`, `BatchingMaxPublishDelay`, `EnableBatching` (off by default), `SendTimeout` (default `TimeSpan.Zero` = no timeout), `Default*` statics.
- `IProducerBuilder`: the same, with validation (a batching delay under 1 ms or a negative send timeout throws `ConfigurationException`, also in `PulsarClient.CreateProducer`).
- `IProducer.Flush(CancellationToken)` (`ValueTask`).

## Wiring

- `SendOp : IBatchItem`. `AsyncQueueWithCursor` gained `TryPeekNextItem`, `MoveNext`, `MoveCursorBack`, `RewriteItemsUpToCursor`, `WaitForNextItem`, `SignalWaiters`, `TryPeekLast`, `RemoveAll`, `Count`; `RemoveCurrentItem` returns the removed item.
- The dispatcher awaits an op's registration gate before the op joins a batch, but a batch waits for a pending gate no longer than its publish delay. Groups are frozen on the ops and the cursor is moved back, so first send and resend take the same path.
  A batch that is too large fails its messages with `TooLargeMessageException` instead of faulting the producer.
- Receipts complete each op with `BatchMessageIds.Create`. `NotAllowedError` is matched against the batch's sequence id and its highest sequence id and fails every op of the batch with one exception.
  `RequestResponseHandler` registers the highest-sequence-id alias key and removes both keys when the request completes.
- `ProducerChannel` sets `NumMessages` and `HighestSequenceId` on the send command each time (`NumMessages` is 1 for a non-batch, so the wire bytes are unchanged). A queue synchronization lock is only taken when a send timeout is set.
- `SubProducer.Flush` and `WaitForSendQueueEmpty` are public because the test project has no `InternalsVisibleTo`.

## Deviations

1. The send timeout throws `OperationTimeoutException` (sequence id in the message), not the spec's `SendTimeoutException` (`03-public-api` and the `06-prerequisites` README override `send-timeout.md`).
2. `SendOp` lost its `CancellationToken` field (the caller's cancellation already completes the receipt) to keep a message's allocation unchanged. `CreatedTimestamp` (stamped only when a timeout is set) replaced it; the registration gate and the group share one field. `Producer.InternalSend` lost its `sendOpCancelable` parameter.
3. A receipt or `NotAllowedError` for an earlier message, or on an empty queue, is ignored; a later one still triggers reconnect-and-resend.
4. A frozen group counts its timeout from when it was made; a message waiting in a batch counts from created + publish delay.
5. `testSendTimeoutAndRecover` is ported without the "flush throws" assertion (failed messages no longer wait in the queue). `IProducerChannel.Name` was not added (the timeout message uses the topic).

## Benchmark result: the 3x batching throughput target is NOT met

`BatchingBenchmarks` (in-process, Release, fake connection that serializes frames): 1.64 us/op and 1.21 KB/op batched against 2.51 us/op and 4.08 KB/op unbatched (about 1.5-1.6x). Only the frame cost is saved, the per-message producer cost is the same.
Plain send allocation is unchanged (2940-2943 B/op against 2934-2942 B on the unmodified tip; time within noise, about +2%).

## Tests

- 82 phase 1 unit tests (`SubProducerBatchingTests` 31, `ProducerBatchingTests` 14, queue/request-handler/channel/builder additions) and phase 2 (`SubProducerSendTimeoutTests` 15, `ProducerSendTimeoutTests` 5, queue/builder additions). `FakeProducerChannel` is the dispatcher test double.
- Integration: `BatchMessageTests` (21), `SimpleProducerConsumerTests` (5), the P1 test, `testSendTxnMessageTimeout` (new `BlackHoleRelay`). All WP6 skips were removed (`grep -rn WP6 tests` is empty). `ProduceBatchedMessages` was re-implemented with a real batching producer in both fixtures (about 20 tests use it).
- About 39 mutations of the wiring were killed (one equivalent mutant).
- Defects found and fixed along the way: a lost wakeup in `SignalWaiters`, a lost flush request (replaced by a request counter), a 16 B per message growth of `SendOp`.

## Still skipped or not ported

- Skipped, were waiting on WP7 which is now merged: `txnAckTestBatchAndCumulativeSub` and `testSendTxnAckBatchMessageToDLQ` (both in `TransactionConsumerEndToEndTestsBase`), `BatchMessageIndexAckDisableTests` (ack timeout) and `BatchMessageIndexAckTests` (negative ack). **Owner: WP10 (un-skip and port now).**
- `testSendOverSizeMessage`: an oversized single message still faults the producer (followup 0006, WP8 producer).
- Not ported: `testOutOfOrderAcksForBatchMessage`, `testNonBatchCumulativeAckAfterBatchPublish`, `testBatchAndNonBatchCumulativeAcks`, `testConcurrentBatchMessageAck`, `testBatchMessageDispatchingAccordingToPermits`, the deduplication exactly-once-after-connection-drop test,
  the `SimpleProducerConsumerTest` max-bytes-within-send-timeout test, and the unit allocation test (acceptance 9; the benchmark covers it).

## Notes for WP8 producer and WP9 producer

- Chunking is mutually exclusive with batching: read `06-prerequisites/chunking.md` §7 for what to do when both are enabled, and where chunks go in the `SubProducer` dispatcher (a frozen group of 1 `SendOp` with several chunks to send, receipts per chunk).
- A new `SendOp` field costs 8-16 B per message; the agent had to claw an increase back. Keep the plain send at 2940-2943 B/op.
- The warnings of the new tests (xUnit1031/xUnit1051 in `SubProducerBatchingTests`, `RequestResponseHandlerTests`, `SubProducerTests`) should be fixed.
