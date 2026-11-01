# WP10 test ports as built: tests that were blocked by batching, ack timeout and DLQ work (merged)

Commits on `feature/transactions` (cherry-picked from the agent branch; the only conflict was the header comment and the signature of the first DLQ test in `DeadLetterTopicTests.cs`, because the producer chunking work had already ported the large-message rows):
`Port the ack timeout tests that need batching`, `Port the dead letter tests that need batching`, `Port the negative ack tests that need batching`, `Port the shared subscription batch index ack tests`, `Port the transaction tests that need batching`, `Port the remaining BatchMessageTest tests`.
No change to `src/` or `CHANGELOG.md`. After the merge a nullable warning in `MessageChunkingSmallMessageSizeTests` (from the chunking merge, not from these commits) was folded into `Fix the build warnings of the new chunking code`. Build: 0 errors, the 4 pre-existing warnings. Debug unit: 2025 passed, 11 skipped.

## What was ported

| Upstream | C# | Notes |
|---|---|---|
| `AckTimeoutTest#testSingleMessageBatch` (first half) | `AckTimeoutTests` | The back-off half is not supported (no `AckTimeoutRedeliveryBackoff`). |
| `AckTimeoutTest#testUnAckMessageRedeliveryWithReceiveAsync` (both rows) | `AckTimeoutTests` | `receiveAsync` is 10 concurrent `Receive` tasks. |
| `DeadLetterTopicTest#testDeadLetterTopic` (non-large row), `#testDeadLetterTopicHasOriginalInfo`, `#testDeadLetterTopicWithPoolMessages` | `DeadLetterTopicTests`, each with a batching row | The Java producer batches but sends synchronously, so a batch has one message; the batching row sends without waiting, in batches of 10, so the dead letter topic receives whole batches. |
| `DeadLetterTopicTest#testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId` | `DeadLetterTopicTests` | |
| `DeadLetterTopicTest#testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound` | new `DeadLetterTopicBatchIndexAckTests` (Transaction fixture) | Needs a broker with batch index ack on; the Integration broker has it off. |
| `NegativeAcknowledgeTest` batching rows, `#testNegativeAcksWithBatch`, `#testNegativeAcksWithBatchAckEnabled` | `NegativeAcknowledgeTests` | All 32 rows follow the Java shape (20 messages, 10 nacked by message and 10 by id, tracker size 0, 2 partitions). |
| `BatchMessageIndexAckDisableTest`, `BatchMessageIndexAckTest` (exclusive and shared) | `BatchMessageIndexAckDisableTests`, `BatchMessageIndexAckTests` | The odd `negativeAcknowledge(consumer.receive())` loop of the upstream test is kept. |
| `TransactionEndToEndTest#testCumulativeAckRedeliverMessages` | `TransactionConsumerEndToEndTestsBase` | Now a theory: no batching (Java's sync sends are one-message batches) and one batch of 5. |
| the timed-out-in-client test (`TransactionEndToEndTestsBase`) | | Asserts `InvalidTransactionStatusException` on `Acknowledge`. |
| `BatchMessageTest` ack tests (`testOutOfOrderAcksForBatchMessage`, `testNonBatchCumulativeAckAfterBatchPublish`, `testBatchAndNonBatchCumulativeAcks`, `testConcurrentBatchMessageAck`; the last three for both batchers) | `BatchMessageTests` | |
| `BatchMessageTest#testBatchMessageDispatchingAccordingToPermits` | `BatchMessageTests` | Reads the per-consumer `msgOutCounter` from the topic stats, because the consumer queue is not accessible. DotPulsar sends flow permits lazily on the first `Receive` and halves the first flow, so the test starts a pending `Receive` and uses prefetch 20 for Java's 10 permits. The wait is 10 s (Java 3 s) after one "Failed to create ledger" flake. |
| dedup resend test (`Send_GivenBatchingAndConnectionDrop...`, spec 8.2) | `BatchMessageTests` with the new `Prerequisites/LossyRelay.cs` | The relay swallows the broker's receipts and drops the connections afterwards, so the broker stores the batches and the producer resends. With broker dedup turned off the test fails (every resent message stored twice, 2 of 2 runs), so it is not vacuous. |
| max-bytes-within-send-timeout test | `SimpleProducerConsumerTests` | |

`testSimpleBatchSyncProducerWithFixedBatchSize` (Java L366) was in the spec table but in neither the agent's list nor the C# tests; the WP10 tail ported it (`Send_GivenFixedBatchSizeAndSynchronousSends_ShouldCreateOneEntryPerMessage`, both batchers: 10 sync sends with `BatchingMaxMessages` 5, backlog 10, values in order; mutation checks killed it with a hang caught by the blame timeout and an assertion failure; the `BatchingMaxMessages` mutant is equivalent for this test).
Skips left in these classes: none of the ported tests is skipped. The 2 Integration skips of the agent's run are the two `ConsumerTests` rows that need the broker fix apache/pulsar#27128 (followup 0002, `Skip` on the rows at `ConsumerTests.cs:595-596`; they were reported as the large-message DLQ row and `testSendOverSizeMessage`, but both of those were ported by the producer chunking work), and the 11 skips of the NOBIA collection already existed.

## Runs (agent's base, before the merge)

| Collection | Passed | Skipped | Failed |
|---|---|---|---|
| Integration (the six classes above) | 102 | 2 | 0 |
| Transaction (`DeadLetterTopicBatchIndexAckTests`, `BatchMessageIndexAckTests`, both `TransactionConsumerEndToEnd...` and `TransactionEndToEnd...` classes) | 48 | 0 | 1 |
| No-batch-index-ack | 26 | 11 | 0 |

The one failure is `Receive_GivenMessagesInPendingAckStateAndSmallPrefetch_ShouldNotGetConsumerStuck` (3 s receive timeout under full-class load); it passed 3 of 3 alone. Each ported test was run 5 times in a row: all stable except the permits test (fixed, see above).

## Mutation checks (applied in `src/`, built, run, reverted)

Killed: untrack on ack, tracking removed, nack redelivery removed, the `EntryId--` for a partial batch (cumulative ack), non-batch cumulative acks dropped, `AckIndividual` never completing, initial permits x10, DLQ origin id without batch index, final-round off-by-one, ack set filter off, no-arg redeliver skipped, oversize message dropped.
Only killed in combination: removing the DLQ candidate on whole-entry ack and in `TryDeadLetter` (each alone is an equivalent mutant).
Not killed: the consumer-side `EnsureOpen` in the txn-ack path (the broker answers `InvalidTxnStatus` with the same exception type; the unit tests cover the guard); ack set filter off survives `DeadLetterTopicBatchIndexAckTests` (the tracker counts only the messages the app received; the off-by-one mutation catches that test).
Not run: removing the lock in `AcknowledgeIndividual` (only a probabilistic detection).

## Findings

See [followup 0012](../followups/0012-wp10-test-port-findings.md): generated producer names are not reused after a reconnect (no broker dedup for unnamed producers), lazy flow permits, ack timeout tracks on `Receive`.

## WP10 tail: fold of the two transaction end-to-end bases (merged)

`TransactionConsumerEndToEndTestsBase` was folded into `TransactionEndToEndTestsBase<TFixture>`; `TransactionConsumerEndToEndTests` became `TransactionEndToEndTests` and `TransactionConsumerEndToEndWithoutBatchIndexAckTests` became `TransactionEndToEndWithoutBatchIndexAckTests`. 74 test cases before and after (37 per fixture), identical names apart from the two class names; only duplicated plumbing was removed (header, constructor, fields, `Dispose`, `GetTxn`, `CreateClient`, the simple `SendAsync`, wait constants). Runs: `TransactionEndToEndTests` 37 passed (before: 10 + 27), `TransactionEndToEndWithoutBatchIndexAckTests` 26 passed, 11 skipped (before: 10 + 16, 11 skipped).
Found on the way: `TransactionEndToEndTests.Send_GivenTransactionalMessageWithoutReceipt_ShouldThrowOperationTimeoutException` takes about 91 s in both fixtures, so `--blame-hang-timeout 90s` kills a run of the class (use 300 s); the cause was not found (assigned to `wp10-g1a-e2e`).
