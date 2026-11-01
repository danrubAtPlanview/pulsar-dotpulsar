# 0012 - Findings from the WP10 test ports

Status: item 4 done in WP10; **item 1 fixed in WP11-A** (the requester asked for it on 2026-11-01, after it had been accepted as a deviation); items 2 and 3 are documented differences.

## 1. A generated producer name is not reused after a reconnect (no deduplication for unnamed producers)

`ProducerChannelFactory` sends `ProducerName` only when the user set one. After a reconnect the broker therefore sees a new producer and does not deduplicate the resent batches or messages: without an explicit name the dedup test (`BatchMessageTests`, `LossyRelay`) stored messages 50-499 twice.
The Java client sends the generated name again after a reconnect, with `user_provided_producer_name = false`, so Java deduplicates. The behaviour predates batching, but acceptance criterion 7 of `06-prerequisites/producer-batching.md` ("after a forced reconnect ... with broker deduplication enabled, the consumer receives each message exactly once") does not hold for unnamed producers.
Not fixed. A fix has to keep the producer id and the epoch consistent, otherwise a reconnect while the broker still holds the old producer can loop on `ProducerBusy`. The test names its producer (`ProducerName("deduplication-producer")`) and says why in a comment. Medium severity (silent duplicates for users that rely on broker deduplication and do not name their producers; the same applies to plain, chunked and transactional sends).

## 2. DotPulsar sends the first flow permits lazily

The first flow command is sent when `Receive` is first called, and it is half of the prefetch count. Only `testBatchMessageDispatchingAccordingToPermits` is affected (it starts a pending `Receive` and uses prefetch 20 to get the 10 permits of the Java test). Not a bug, a difference to document in the port.

## 3. The ack timeout tracks messages when they are received, not when they arrive

The consumer starts the ack timeout of a message in `Receive` (WP7 design). The dead letter and ack timeout tests therefore rely on the test continuing to receive. Not a bug, listed so reviewers do not mistake it for one.

## 4. `testSimpleBatchSyncProducerWithFixedBatchSize` is not ported yet

See the WP10 tail in `10-implementation-plan.md`.

## Status at the end of WP10

Item 4 is done: `testSimpleBatchSyncProducerWithFixedBatchSize` was ported by the WP10 tail (see `upstream-map.yaml`). Item 2 matters again for tests that wait for the permits of a consumer (followup 0015: the test of two Shared consumers waits for the permits of both at the broker before it sends). Item 1 (a generated producer name is not reused after a reconnect, so unnamed producers are not deduplicated) is open with medium severity; items 2 and 3 are documented differences.

Decision for item 1 (WP10): accepted as a deviation. **Reversed on 2026-11-01 by the requester: fixed in WP11-A**, see `../implementation-notes/wp11-deferred-features.md`. A producer has an id that is the same on every connection (a process-wide allocator, honoured by `ChannelManager`), a growing epoch, and a producer without a name sends `UserProvidedProducerName = false` and, from the second connect, the name that the broker generated; deduplication then holds for plain, batched and chunked sends. The broker rules are not what this followup assumed: a registration from a different connection is not a successor of the old producer, the broker pings the old connection and replaces the producer after `connectionLivenessCheckTimeoutMillis` (5 s), so a reconnect while the broker still holds the old producer is registered 8 s after the cut on 4.0.9 (3 s client retry + 5 s), named or not; a broker without the liveness check (for example 3.1.0) waits for its keep-alive, as with Java.
