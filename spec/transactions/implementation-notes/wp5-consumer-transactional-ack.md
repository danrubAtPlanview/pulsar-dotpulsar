# WP5 as built: transactional acknowledgments on IConsumer, replace-on-redelivery (merged)

Commits on `feature/transactions`: `Replace redelivered batch trackers, never merge`, `Add transactional acknowledgments to IConsumer`, `Add consumer transaction integration tests`.
Unit: about 35 test methods added and 4 removed or replaced; after the merge the suite is green in Debug (1497) and Release (1493, `MessageProcessorTests` excluded). Integration: the "Transaction" collection passed three full runs; other runs lost the broker container to memory load
(`container ... is not running`); no logic failure was seen while a broker was alive.

## What was built

- `BatchHandler.Track` replaces the tracker of an entry that is delivered again (decision D8). The public `IntersectWith` methods are gone. `BatchHandlerTests`/`ConsumerChannelTests` were adjusted; the 3 tests that pinned `IntersectWith` were removed.
- `IConsumer`: 3 overloads next to their non-transactional twins (`Acknowledge(MessageId, ITransaction)`, `Acknowledge(IEnumerable<MessageId>, ITransaction)`, `AcknowledgeCumulative(MessageId, ITransaction)`).
  `ConsumerExtensions`: 4 new (`IMessage` and `IEnumerable<IMessage>` transactional forms, `AcknowledgeCumulative`, plain `IEnumerable<IMessage>` for symmetry).
- `SubConsumer`: builds a fresh `CommandAck`, calls `transaction.RegisterAckedTopic(Topic, SubscriptionName)`, registers the ack op **before** awaiting it, sends via `SendTransactional` under the executor, evaluates the response outside the
  executor (a conflict does not fault the consumer), and bounds the wait with `OperationTimeout`. Cumulative acks are rejected for Shared/KeyShared (`ConfigurationException`).
  `Acknowledge(MessageId, txn)` calls `EnsureOpen()` first and registers nothing when the transaction is not open (Java parity); the list and cumulative overloads register a failing op instead.
- `IConsumerChannel`, `ConsumerChannel`, `NotReadyChannel` needed no change (`SendTransactional` and the pooled-`CommandAck` reset already existed).

## Deviations from the spec

- `NotSupportedException` from `SendTransactional` (broker protocol < 17) is caught inside the executor and rethrown outside it, so the consumer does not fault.
- `Acknowledge(IEnumerable<MessageId>, txn)` with an empty list is a no-op (Java sends an empty ack).
- Multi-topic cumulative: Java's `MultiTopicsConsumerImpl` drops the transaction on a cumulative ack; `Consumer` routes it with the transaction.
- A `Receive` right after `Create()` hits `ChannelNotReadyException` and waits the 3 s retry interval, so the tests wait for `ConsumerState.Active` first.
- P5 (`ackCommitTest`) uses a 30 s transaction timeout instead of 5 s (it timed out in the client). T24/T25 drop `SendTimeout(5 s)` (`TODO(WP7)`).
- Integration test layout: `TransactionConsumerEndToEndTestsBase` (generic over the fixture) with two thin subclasses, plus `TransactionConsumerTests`; they are separate from WP4's `TransactionEndToEndTestsBase` (fold together in WP10).
  Batches come from the perf-tool helper and are checked by batch index.

## Skipped tests and who un-skips them (all carry a TODO marker)

- **WP6 (batching):** `testUnackMessageAfterAckAllMessages` (4 batchSend rows), `testAckWithTransactionReduceUnAckMessageCount` batch row, `txnIndividualAckTestBatchAndSharedSub`, `...BatchAndFailoverSub`,
  `testSelectiveRedeliveryAfterConflictingTransactionalBatchAck` (4 rows; also needs the key-based batcher).
- **WP7a (redelivery primitive):** `txnAckTestBatchAndCumulativeSub`, `txnAckTestNoBatchAndCumulativeSub`, `testCumulativeAckRedeliverMessages`.
- **WP7 (dead letter):** `testSendTxnAckMessageToDLQ`, `testSendTxnAckBatchMessageToDLQ`.
- 5 NOBIA rows are disabled upstream and stay skipped.

## Parity rows covered

E2E rows 1, 2 (both variants), 3, 6 (non-batch), 7 (non-batch), 8, 10, 20, 22, 26; T24, T25, T27, A9, P5, P6; row 23 consumer half. T2 only for the ack of a never-delivered id.
Not done because they need a transactional producer, now WP4: 4, 5, 13, 16, 17, 18, 19, 25, 29. Row 12 needs `CreateSubscription`/`DeleteTopic` fixture helpers. Row 21 is excluded in the spec.

## Notes for WP7a and WP7

- A transactional ack uses a fresh `CommandAck`; the executor retries the same instance and the request id is reassigned per attempt.
- The ack-timeout tracker hook belongs in `SubConsumer.TransactionalAcknowledge` before `SendAndAwaitReceipt` (or in `ConsumerChannel.SendTransactional`). Ids still carry their `BatchIndex` before the channel mutates them.
- The redelivery primitive must call `BatchHandler.Clear()` when it purges the prefetch queue, so unconsumed batch messages are dropped.
- A transactional cumulative ack only clears tracker bits and never prunes earlier trackers. If the transaction aborts and the broker does not redeliver (Exclusive/Failover), the tracker stays cleared. This is Java parity, but the redelivery path must recreate it.
- The real-broker redelivery tests call `RedeliverUnacknowledgedMessages()` only after the prefetch queue has been fully drained; that is why they pass without epoch handling.
- Dead-letter candidates must not be removed on transactional acks (Java parity).
