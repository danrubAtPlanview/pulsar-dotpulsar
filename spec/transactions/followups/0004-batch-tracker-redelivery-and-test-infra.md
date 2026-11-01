# Follow-up 0004 — Batch tracker on redelivery (spec change) and test-infrastructure notes from WP5a

| | |
|---|---|
| **Status** | Done in WP10: the redelivery decision is implemented (WP7a) and the test-infrastructure notes are resolved (section at the end). |
| **Found by** | WP5a (batch-index acknowledgment) |
| **Spec impact** | Overrides `06-prerequisites/batch-index-ack.md` open decision 4 ("merge the redelivered tracker by AND"). |

## 1. Hazard: AND-merging a redelivered batch tracker loses messages after an aborted transactional cumulative ack

`BatchHandler` keeps one `BatchAckTracker` per entry (ledger, entry, partition). The spec said that when the broker redelivers the entry
the new tracker is merged into the existing one with AND.

1. A transactional **cumulative** ack of index *k* clears the tracker's bits `0..k` (this mirrors Java's `BatchMessageIdImpl.ackCumulative`).
2. The transaction is **aborted**. The broker's pending-ack state is rolled back and the whole entry is redelivered.
3. With AND, the redelivered tracker still has bits `0..k` cleared. The batch completes after fewer acks than it has messages, and the whole
   entry is acknowledged while some messages were never acknowledged again. That is silent message loss.

Java does not have this problem because each delivery of a batch gets a fresh `BatchMessageAcker`.

**Decision (applied in WP5):** when an entry that already has a tracker is delivered again, **replace** the tracker with a fresh one built from the
broker's `ack_set` and batch size. Never AND-merge. It matches the broker's view, which redelivers the whole entry except the indexes it knows are
acknowledged, and it matches Java. An old-delivery message acknowledged after the replacement simply clears its own bit in the new tracker.

Required tests (WP5): transactional cumulative ack of index *k*, then abort, then redelivery, expecting the batch to be acknowledged only after every
index is acknowledged again; plus plain redelivery with `BatchIndexAckEnabled` false and true.

**Review after parity:** confirm no behaviour change for existing users with `BatchIndexAckEnabled = false` (the default). Before WP5a, a redelivered batch
created a duplicate tracker (a leak), so the previous behaviour was already unreliable.

## 2. Deviations the WP5a author recorded (keep in `upstream-map.yaml`)

- Cumulative ack with the option enabled sends the tracker's bits after clearing `0..i`, not a fresh set, matching Java's immediate-ack path
  (`doImmediateBatchIndexAck`). A fresh set could un-ack indexes that were already acked individually.
- Compacted-out messages clear their own bit in the tracker so that the batch can still complete. The spec left them set, which left the entry
  unacknowledged forever when the option is off and `ReadCompacted` is on.
- `BatchHandler` prunes trackers by scanning the dictionary with a reused list; the first `Dictionary.Keys` access allocates. This path is only exercised for
  cumulative acks. Revisit during the performance review (WP10) if `AckBenchmarks` shows it.
- Tests reach the internal `MessageId.BatchSize` through a reflection helper (`MessageIdInternals`), because the repo has no `InternalsVisibleTo`.
  Consider asking the maintainers whether they would accept `InternalsVisibleTo` for the test project.

## 3. Test infrastructure notes (open)

- **Container resource pressure.** Running the `Integration` and `Transaction` collections in the same `dotnet test` invocation, or three Pulsar containers
  starting at once, intermittently fails with `Could not create the Java Virtual Machine` / killed containers (seen by WP3 and WP5a on a developer laptop).
  Locally, run collections separately. Before WP10's 20x loop, check whether the CI runner has enough memory for the Integration, Transaction and
  TransactionWithoutBatchIndexAck containers, or serialise those collections (xUnit collections already run sequentially unless `maxParallelThreads` is raised; the
  problem appears when separate `dotnet test` invocations overlap).
- **Batch production without producer batching.** Until WP6, integration tests produce batches with `pulsar-perf produce` inside the container. It overshoots its
  message count when it produces more than one batch per run, so the helper `ProduceBatchedMessages` (on `IntegrationFixture` and `TransactionFixtureBase`) runs it
  once per batch with a long time window and a rate limit. After WP6, replace the helper with the DotPulsar producer and delete it.
- **Skipped ports waiting on other work packages:** `BatchMessageIndexAckTest#testBatchMessageIndexAckForSharedSubscription` (negative ack, WP7),
  `BatchMessageIndexAckDisableTest#testBatchMessageIndexAckForSharedSubscription` (ack timeout, WP7). Not yet ported because they need producer batching (WP6):
  `BatchMessageWithBatchIndexLevelTest#*`, four `BatchMessageTest#*` ack tests, `ConsumerSeekBatchIndexAckTest`.

## Status at the end of WP10

Item 1 (the redelivered batch tracker is replaced, not AND-merged) is implemented in WP7a (decision record `docs/decisions/0006-replace-the-batch-tracker-on-redelivery.md`). Item 2 is recorded. Item 3 (test infrastructure) is done: the container resource pressure is solved by `BrokerResources` (at most 2 brokers at a time with a 1 GB JVM, see `implementation-notes/wp10-parity-completion.md`); `ProduceBatchedMessages` produces with a real batching producer since WP6 and is kept as the helper of the tests that need whole batches; the ports that waited for other work packages are ported (the only one left out is `ConsumerSeekBatchIndexAckTest#testSeekReplaysBatchAfterSuccessWhileDisconnected`, excluded in `upstream-map.yaml` with its reason: it needs a seek with an ack set, open decision 3).
