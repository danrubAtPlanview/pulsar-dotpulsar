# WP7a + WP7 (consumer side) as built: redelivery primitive, ack timeout, negative ack, dead letter (merged)

Commits on `feature/transactions`: `Add redelivery primitive to consumer`, `Add ack timeout, negative ack and dead letter`. Merged after WP4, so `ProducerChannelFactory`/`Producer` take both
`transactionsEnabled` and `initialSubscriptionName` (the dead-letter producer passes `transactionsEnabled: false`).
After the merge: 1658 unit tests green in Debug, 1654 in Release (`MessageProcessorTests` excluded), 3 runs. On the agent's own base (before WP4): full "Integration" collection 131 passed / 32 skipped / 0 failed; full "Transaction" collection 80 passed / 26 skipped / 0 failed.
The merged state has **not** been run against a broker yet.

## Phase 1: redelivery primitive (WP7a)

- `SubConsumer.RedeliverUnacknowledgedMessages(IEnumerable<MessageId>, ct)`: an empty list does nothing; Exclusive/Failover redeliver all; ids are converted to entry level (`SubConsumer.ToEntryLevel` is the single place where WP8 adds chunk-id expansion),
  de-duplicated, sent without a batch index, in commands of at most 1000.
- Redeliver-all increments `ConsumerEpoch` on Exclusive and Failover; the epoch is sent when subscribing, and a redeliver is sent if it changed while subscribing. The channel and the factory take a nullable `ConsumerEpoch?` (readers pass `null` and send no epoch).
- A redeliver purges the prefetch queue and gives the permits back with a flow command sent after the redeliver command. Messages with an older epoch are skipped (a skipped batch counts as one permit per message).
- New helpers: `AsyncQueue.RemoveAll`/`RemoveWhere`, `BatchHandler.ClearQueuedMessages`/`RemoveQueuedMessages`, `MessagePackage.ConsumerEpoch`.
- The purge calls `BatchHandler.ClearQueuedMessages()`, which drops unconsumed messages but **keeps the trackers** (ack-timeout.md 4.4(c); the WP5 note said `Clear()`). Dropping the trackers would make an ack of an old id before redelivery acknowledge the whole batch.
  Trackers are recreated by replace-on-redelivery (D8); a test covers an aborted transactional cumulative ack followed by redeliver-all.

## Phase 2: wiring

- Ack timeout: `ConsumerOptions.AckTimeout`/`AckTimeoutTickTime` + builder, validated in `PulsarClient.CreateConsumer`. Tracker per sub-consumer, only when the timeout is set and the topic is persistent. `Track` at the 3 `Consumer.Receive` return points;
  removal on individual ack of a whole entry and on cumulative ack (before the batch checks); transactional ack removes just before sending, before the receipt (in `SubConsumer.TransactionalAcknowledge`); clear on redeliver-all; dispose on dispose and channel fault.
- Negative ack: `IConsumer.NegativeAcknowledge(MessageId, ct)`, `ConsumerExtensions.NegativeAcknowledge(IMessage, ct)`, `NegativeAcknowledgeRedeliveryDelay` + builder. A `NegativeAcknowledgeTracker` per sub-consumer (created always, timer lazy).
- Dead letter: `DeadLetterPolicy` validated on the builder and the client. Candidates are registered on receive (batched and non-batched), removed on an individual ack of a whole entry (not on transactional or cumulative acks),
  cleared on reconnect and seek, handler disposed with the sub-consumer. The Shared/KeyShared redelivery step tries the dead letter first and redelivers only the entries that were not dead-lettered. `ProducerOptions.InitialSubscriptionName` is internal.
- Public members on internal types (spec OD-7): `Consumer<T>.UnAckedMessageCount`, `SubConsumer<T>.UnAckedMessageCount`.
- CHANGELOG: Changed (4 redelivery behaviour entries), Added (ack timeout, negative ack, dead letter topic), Fixed (messages received twice after `RedeliverUnacknowledgedMessages`).

## Deviations from the specs and notes

1. Trackers kept on purge (see above).
2. Epoch is a nullable `ConsumerEpoch?` parameter, not a bool filter flag.
3. **Chunk ids are not handled**: `MessageId` has no `ChunkMessageIds` yet. `SubConsumer.ToEntryLevel` is the place for WP8.
4. A transactional individual ack of a batch index does not untrack the entry (Java parity). Spec OD-2 is not applied: the BatchHandler is not touched by transactional individual acks, so completeness is unknown.
5. The nack tracker object is created per sub-consumer even when unused (timer starts lazily).
6. The dead-letter send timeout is the handler's own 30 s; the producer `SendTimeout` is not involved.
7. New integration classes `AckTimeoutTests`, `DeadLetterTopicTests`, `NegativeAcknowledgeTests` use `IntegrationFixture`; they call the static `TransactionFixture.ReceiveOrNull` and inline admin REST calls.
8. The upstream Java sources are not in the repo, so the ported tests follow the spec table rows, not the Java code. Several are adapted.
9. A Release-only race was found and fixed in `SubConsumerTests.Acknowledge_GivenTransaction_ShouldRegisterAckOpSoThatCommitWaitsForTheReceipt` (it asserted the outer ack task was complete when the commit completed; the commit only waits for the inner task).

## Upstream rows

- Ported: `testCumulativeAckRedeliverMessages`, `txnAckTestNoBatchAndCumulativeSub`, `testSendTxnAckMessageToDLQ`; ack timeout (`UnAcknowledgedMessagesTimeoutTest` 4 rows, `PerMessageUnAcknowledgedRedeliveryTest` 5 rows,
  `ConsumerRedeliveryTest#testMessageRedeliveryWhenTimeoutInListener`); dead letter (`DeadLetterTopicTest` 11 rows incl. message key, binary key, ordering key, event time, producer name, original info, custom topic name, initial subscription, pool messages false row);
  `DeadLetterPolicyTest`; `NegativeAcksTest#testNegativeAcks` (16 non-batch rows) plus a delay test.
- Skipped with a TODO naming the owner (WP6 unless noted): `testNegativeAcks` batch rows (16), `testNegativeAcksWithBatch`, `testNegativeAcksWithBatchAckEnabled`, `testSingleMessageBatch`, `testUnAckMessageRedeliveryWithReceiveAsync`,
  `testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound`, `testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId`, `testDeadLetterTopicWithPoolMessages` (true row), `testDeadLetterTopic` (large messages row, WP8),
  `testSendTxnAckBatchMessageToDLQ`, `txnAckTestBatchAndCumulativeSub`.
- Not ported: backoff rows (OD-1); shared two-consumer partitioned ack timeout tests (`testSharedSingleAckedPartitionedTopic`, `testFailoverSingleAckedPartitionedTopic`); DLQ `testMultipleSameNameConsumersToDeadLetterTopic`,
  `testDuplicatedMessageSendToDeadLetterTopic`, `...UnderPartitionedTopicWithKeyShareType`, `...WithInitialSubscriptionAndMultiConsumers`, `...WithMaxUnackedMessagesBlocking`.

## Not done

- The two `BatchMessageIndexAck*Tests` skips still say WP7: the negative ack and ack timeout they need now exist, but they also need producer batching (WP6).
- The encryption `Fail` action tracker hook (WP9). No README/docs changes. No allocation check of the disabled paths beyond null checks.
