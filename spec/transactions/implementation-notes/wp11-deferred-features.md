# WP11: the three items that the specification deferred, implemented on request

Decided by the requester on 2026-11-01, after the end of WP10: (A) deduplication of producers without a name after a reconnect (acceptance criterion 7 of `06-prerequisites/producer-batching.md`, followup 0012 item 1), (B) `ackTimeoutRedeliveryBackoff` (OD-1 of `06-prerequisites/ack-timeout.md`), (C) seek with `ack_set` (OD 3 of `06-prerequisites/batch-index-ack.md`). Each was done by an agent in its own worktree, from `4f0c629`, and cherry-picked onto `feature/transactions`; I reviewed the production diff of both. The verification of the whole branch after the merge is at the end of this note.

Commits (as on `feature/transactions`):

| WP | Commits |
|---|---|
| A | `Let a producer choose its id on a connection`, `Send the generated name of a producer again`, `Test the identity of a producer across reconnects`, `Test the reconnect of a producer on a real broker` |
| B | `Add the redelivery backoff policy types`, `Add the ack timeout redelivery tracker`, `Add AckTimeoutRedeliveryBackoff to the consumer`, `Port the ack timeout rows that use the backoff`, `Document the ack timeout redelivery backoff` |
| C | `Send an ack set when seeking to a batch message`, `Port ConsumerSeekBatchIndexAckTest` |

## A. Deduplication of producers without a name

**What the broker really does** (read at the pinned commit `f3846f4`, at `v4.0.9` and at older tags; this supersedes the description in followup 0012, which assumed more):

- `Producer.isSuccessorTo` needs the same name, topic and producer id **and the same connection**, with a lower epoch. A registration from a *different* connection is not a successor: the broker pings the old connection and replaces the producer after `connectionLivenessCheckTimeoutMillis` (5 s by default).
- Brokers 3.0.0 and 3.1.0 only replace a producer if neither name is user-provided. 3.0.5, 3.2 and later, and 4.0.x, dropped that check and have the liveness check, so on a current broker a reconnect is answered late instead of with `ProducerBusy`. On a broker without the liveness check (for example 3.1.0) the wait is the broker keep-alive, 30 to 60 s (Java behaves the same).

**What was built:**

- Each `ProducerChannelFactory` takes one id from a process-wide allocator (`ProducerIds`, like `PulsarClientImpl#newProducerId`); the producer has that id on every connection. A static allocator avoids passing it through the dead letter producer; the broker keeps producer and consumer ids in separate maps, so ids only have to be unique among producers.
- `ChannelManager` honours `CommandProducer.ProducerId` when it is set. Producers are kept in a new dictionary-based `SparseIdLookup` (`IdLookup`, and with it the ids of consumers and of the transaction meta store handlers, is unchanged). A new registration replaces the stale channel of an earlier attempt on the same connection; the remove, close-success and exclusive-queue-error callbacks check that the channel that they act on is still the one that holds the id (otherwise a late callback of the old channel removes or disconnects the new one).
- A producer without a name sends `UserProvidedProducerName = false` and `Epoch` (0, 1, ...) on every connect, including the first, and from the second connect the name that the broker generated. A producer with a name sends no epoch and no flag, as before (only its id comes from the allocator).
- A side effect: the chunk UUID contains the producer name, so a resent chunked message keeps its UUID.

**Result:**

- Reconnect while the broker still holds the old producer: the producer is registered again **8.0 s** after the cut (3 s of client retry plus the 5 s liveness check of the broker), named and unnamed alike. The broker keep-alive is 30 s.
- With the old code 5 of the 8 integration tests fail (all the unnamed ones, the unload test and the partitioned test; the 3 named ones pass); the old unnamed reconnect took 3.0 s and stored duplicates. Removing the identity checks in `ChannelManager` makes 3 unit tests fail.
- Benchmarks (`PlainSend`, the ack benchmarks, short job, before and after): identical allocations (PlainSend 2.88 KB, PlainAck 104 B), times within noise. They use the fake connection and bypass `ChannelManager`; a real send now does one dictionary lookup under a lock instead of an array index, which was not measured.

**Not verified:** the same-connection successor path (the epoch) on a real broker (DotPulsar closes the producer before it registers again, so that path is covered by unit tests only); a broker without the liveness check.

**Possible followup:** send the epoch for producers with a name as well. Java does, and 4.0.9 would then replace them at once on the same connection.

## B. `ackTimeoutRedeliveryBackoff`

**Upstream semantics** (read, not guessed): an entry first expires after the ack timeout, as without the backoff; its due time is then `now + Next(redeliveryCount)`, and the first tick at or after that redelivers it. The delay is added to the ack timeout. For Shared and KeyShared subscriptions the redelivery count is the one the broker sends (`RedeliveryCount`); for the other subscription types the tracker counts the redeliveries that it caused (the local count survives `Clear()` and is lost when the message is acknowledged or negatively acknowledged).

**What was built:**

- Public API: `IRedeliveryBackoff` (`TimeSpan Next(uint redeliveryCount)`), `MultiplierRedeliveryBackoff` (two constructors, the Java defaults and validation), `ConsumerOptions.AckTimeoutRedeliveryBackoff`, `IConsumerBuilder.AckTimeoutRedeliveryBackoff`. README, `docs/api/index.md` and CHANGELOG are updated.
- `UnAckedMessageRedeliveryTracker` (internal) behind a new internal interface `IUnAckedMessageTracker`; the sealed `UnAckedMessageTracker` is unchanged apart from implementing it, so without the option nothing changes. The clock is monotonic and injectable (no sleeps in the unit tests).
- One tracker per sub consumer, as for the plain tracker (the multi-topic tracker of Java needs `removeTopicMessages` only when a topic goes away, and here the tracker of a partition is disposed with it).

**Differences from Java:** `Remove` also removes an entry that waits for its delay if it was added again; a backoff that throws means no delay; a huge delay saturates; the redelivery count is unsigned (no `next(-1)`); Java names the result `backoffNs` but the value is milliseconds.

**Not done:** `NegativeAcksTest#testNegativeAcksWithBackoff` stays Excluded. It needs a negative-ack option (`negativeAckRedeliveryBackoff`) and the use of the policy in the negative-ack tracker; `IRedeliveryBackoff` can be reused for it. See followup 0019.

## C. Seek with `ack_set`

- Java sends the ack set only when the id has `batchIndex >= 0 && batchSize > 0`, with the bits `[0, batchIndex)` cleared (acknowledged), whatever the batch index acknowledgment setting of the consumer is. A first-chunk id takes precedence and gets no ack set. The broker uses the ack set only with `acknowledgmentAtBatchIndexLevelEnabled` (confirmed against the real broker: with the setting off the whole batch is replayed).
- Built in `MessageIdDataExtensions.ToSeekMessageIdData`, used by `SubConsumer.Seek` and by `SubReader.Seek` (Java's `Reader.seek` delegates to the consumer). The `MessageId` of a send, or one that the application created, has no batch size, so the whole batch is replayed. CHANGELOG ("Changed") says so.
- `ConsumerSeekBatchIndexAckTest` has exactly one method with two rows (at the pin and on master); it is ported as `ConsumerSeekBatchIndexAckTests` (against a broker with the setting on, plus `ConsumerSeekWithoutBatchIndexAckTests` for the broker without it) and `SeekBatchIndexTests` (unit). The upstream test pauses the consumer and delays its reconnect, which the port cannot do, so it checks the replay only.
- Differences from Java: the Java client also drops the messages before the batch index on the client (`isPriorBatchIndex`), so it works on a broker without the setting; DotPulsar does not. Java's default seek is exclusive, DotPulsar's is inclusive. `SubscriptionSeekTest#testSeekForBatchMessageAndSpecifiedBatchIndex` is not in the map and relies on that client-side filter; not ported.

## Tests added by the three packages (as reported by the agents)

- Unit: A 46 (`SparseIdLookupTests`, `IdLookupTests`, `ChannelManagerProducerIdTests`, `ProducerChannelFactoryTests`, `ProducerIdentityTests`, `LossyRelayTests`), B 40 (`MultiplierRedeliveryBackoffTests` 15, `UnAckedMessageRedeliveryTrackerTests` 25, plus rows in `AckTimeoutTests`, `ConsumerBuilderTests`, `PulsarClientConsumerOptionsTests`, `SubConsumerTests`), C 29 (`SeekBatchIndexTests`) and a few rows in existing classes.
- Integration: `ProducerReconnectTests` (6), `BatchMessageTests` dedup test (a theory over named and unnamed), `AckTimeoutTests` (22 rows, 4 turned into theories over `useRedeliveryBackoff`), `ConsumerSeekBatchIndexAckTests` and `ConsumerSeekWithoutBatchIndexAckTests` (17).
- Map rows moved by this work: see `upstream-map.yaml` (`UnAckedTopicMessageRedeliveryTrackerTest` split into 4 rows, `testCheckUnAcknowledgedMessageRedeliveryTimer`, `testSingleMessageBatch`, `MultiplierRedeliveryBackoffTest#testNext`, `ConsumerSeekBatchIndexAckTest`).

## Verification of the whole branch after the merge

The merge (tip `3e1c6055`, 153 commits over `master`) was verified as a whole; one test-helper fix followed (`a114997c`, 154 commits, see the stress below).

- Release and Debug build: OK, with the four warnings that `master` has (NU1608 and three xUnit1031 in `AwaiterTests`).
- Unit tests: Release 2290 passed, 3 of 3 runs, nothing skipped; Debug 2279 passed and 11 skipped (the allocation tests, which are only measured in an optimized build).
- Integration tests (Release, `Category=Integration`, at most 2 brokers at a time, `3e1c6055`): 553 tests, 539 passed, 14 skipped, 0 failed, 38 minutes. The 14 skips are the same as before WP11: 11 rows of `TransactionEndToEndWithoutBatchIndexAckTests` (by design), 2 rows of `ConsumerTests` that need apache/pulsar#27128, 1 row of `TopicTransactionBufferPublishOrderTests` that needs apache/pulsar#27082. The Debug integration run was **not** repeated after WP11 (521 tests, 507 passed, 14 skipped, 0 failed before it).
- Transactions collection (`FullyQualifiedName~DotPulsar.Tests.Transactions`, Release): 5 iterations on the final tree, 189 passed and 12 skipped every time (13 to 17 minutes each); 30 iterations before WP11.
- Stress of the unit suite under CPU load (4 parallel suites and 12 busy loops): the first pass found a hang of a test of WP11-A (`LossyRelayTests`, 21 of 56 runs aborted). The cause was a race in the test helper, and it is fixed in `a114997c`; after it, 60 of 60 runs passed with 2290 tests and none aborted. Details and the other numbers: `../followups/0009-flaky-unit-tests-under-cpu-load.md`, section "WP11 sign-off stress". After the fix the integration classes that use the relay (`ProducerReconnectTests`, `BatchMessageTests`) ran again: 45 passed. The rest of the integration suite was not run again after the fix, which changes a test helper only.
- Not verified (also in followup 0019): the same-connection epoch path on a real broker, a broker without the liveness check, the cost of the dictionary lookup of `SparseIdLookup` in a real send, Debug integration after WP11.
