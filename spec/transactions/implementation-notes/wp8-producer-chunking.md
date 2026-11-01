# WP8 producer as built: chunking in the producer (merged)

Commits on `feature/transactions`: `Add the chunking options to the producer`, `Fail only the send of a message that is too large`, `Send the chunks of a message from the producer`, `Chunk the messages of the dead letter producer`, `Add the integration tests of the producer chunking`, `Add changelog entries for the producer chunking`, `Add a benchmark of sending chunked messages`, `Fix the integration tests of the producer chunking`, then `Fix the build warnings of the new chunking code` (eleven new warnings after the merges).
The merge conflicted in `CHANGELOG.md` (kept both) and, when the flake-hunter commits followed, in `SubProducer.cs`: the dispatcher of this work package takes a response queue and sends chunks, the flake fix passes the batch that was taken under the lock to the write. `SendGroup(channel, head, batch, responseQueue, ct)` now has both; `batch is not null` replaces `head.BatchCount > 1` (a batch exists only for a group of more than one op).
After the merge (with the WP9 consumer and the flake fixes): Debug unit 2025 passed, 11 skipped; Release unit 2032 passed, 3 runs. Warnings: the 4 pre-existing ones (NU1608, 3 xUnit1031 in `AwaiterTests`).
On the agent's own base: Integration collection 177 passed, 26 skipped, 0 failed; chunking collection (a broker with `maxMessageSize` 100) 10 passed; Transaction collection 103 passed, 4 skipped, 2 failed (`Acknowledge_GivenNoBatchAndSharedSubscription_...` and the `GivenBatch...` variant hit the 3 minute test timeout in the full run while another broker was up; the whole class passes alone, 24 passed, 2 skipped; **re-check on the tip**); NOBIA collection 24 passed, 13 skipped.

## Public API added

- `ProducerOptions.EnableChunking`, `ChunkMaxMessageSize` (0 or less: use the maximum message size of the broker), `Default*` statics; `IProducerBuilder.EnableChunking(bool)`, `ChunkMaxMessageSize(int)`.
- Validation in `ProducerBuilder.Create()` and `PulsarClient.CreateProducer`: chunking together with batching throws `ConfigurationException("Batching and chunking of messages can't be enabled together")` (the exact Java text, without the usual `ProducerOptions.` prefix).
- `Producer.CreateSubProducer` switches chunking off for `non-persistent://` topics, as Java does.
- The dead letter producer enables chunking and leaves batching off.

## Wiring and deviations from the spec

1. **Responses of chunks.** Every chunk shares `(producerId, sequenceId)`; `Awaiter` replaces and cancels a request with the same key, so the response of chunk 0 would have been lost. `RequestResponseHandler` keeps chunk sends (`CommandSend.IsChunk`) in a FIFO per key; receipts and send errors complete the oldest pending chunk; close and dispose cancel them. (The spec missed this.)
2. **Size check.** The spec's `NumChunksFromMsg <= 1` condition would reject single-frame messages of a chunking producer on small-limit brokers (the frame is larger than the limit by its command overhead; Java never size-checks chunking producers). `SendPackage.SkipSizeCheck` is set by `SendChunk` and by `Send` for caller-supplied chunk fields (`ChunkTestHelper`).
3. `PlanChunking(metadata, payload, chunkMaxMessageSize)` is synchronous; a message that fits in one chunk also goes through `SendChunk`. `SendChunk` has no `sequenceId` parameter.
4. No new `SendOp` field: the `ChunkedSendState` lives in `_gateOrGroup` as a group of one (`MakeChunked`, `BatchCount` is 1).
5. Any exception from planning fails only that send, not only `TooLargeMessageException`.
6. `SubProducer` got optional trailing parameters `enableChunking` and `chunkMaxMessageSize`.
7. Small-limit fixture: one broker with `maxMessageSize` 100 instead of the spec's 35, 50, 100, 1024 containers; 35 and 50 are covered with `ChunkMaxMessageSize(40)`.

## Dispatcher behaviour

- Resend after reconnect: planned once and kept; the chunks resume at `ReceiptedChunks`.
- Send timeout: the dispatcher checks `IsRemoved` between chunks and stops writing; late receipts for the removed message are ignored.
- Transaction gate: awaited before planning; every chunk carries the transaction ids on the command and in the metadata.
- A `NotAllowedError` on any chunk fails the whole op, the producer is not faulted. A planning failure fails only that send.
- The caller cancels after planning: all chunks are still written (group semantics).
- `MaxPendingMessages`: a chunked message takes one slot.
- **R-MAX-SIZE fixed** (followup 0006 item 1): an oversized plain send fails with `TooLargeMessageException` and the producer keeps sending; `testSendOverSizeMessage` (6 variants) is ported.

## Encryption markers (for WP9 producer, now wired)

`ProducerChannel` had three `TODO(WP9-producer)` markers: in `Send` after compression, in `SendChunk` per chunk, and the encryption-overhead argument (0) of `PlanChunking`.

## Tests

Every `TODO(WP8*)` marker and skip is gone: `testChunkingWithOrderingKey`, `testChunkSize`, `testBlockIfQueueFullWhenChunking`, the repeated-seek test, the non-persistent tests, shared single/multi consumer, interleaved chunks, the DLQ rows, `testAckChunkMessage` with the .NET producer. Not portable (spec): `testPublishWithFailure`, `testNonPersistentMessageOverLimitOnlyWithMetadata...`, `testChunksEnqueueFailed`. 32 mutations of the wiring were killed.
`IntegrationFixture` gained a protected constructor taking broker settings; `FakeProducerChannel`, `FakeProducerConnection` and the `FakeChannel` of `SubProducerTests` got the new interface members.

## Allocations (Release, in-process, bytes per op, direct / thread-pool completion)

| Benchmark | Before (`73df5e6`) | After |
|---|---|---|
| PlainSend | 2949 / 2999 | 2938 / 2991 |
| PlainSend_Metadata | 2939 / 2992 | 2930 / 2986 |

`benchmarks/Transactions/ChunkingBenchmarks.cs`: 2942 B for a plain 100-byte send, 3497 B with chunking enabled on a message that is not split, 13303 B for a message split in 10 chunks. Timing was noisy (other work ran), so no timing claim.

## Follow-ups

See [0011](../followups/0011-wp8-producer-findings.md).
