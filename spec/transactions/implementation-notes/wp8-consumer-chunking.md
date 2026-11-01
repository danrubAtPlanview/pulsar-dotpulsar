# WP8 (consumer side) as built: chunked messages in the consumer and reader (merged)

Commits on `feature/transactions`: `Add chunk options and chunk ids to MessageId`, `Reassemble chunked messages in consumer and reader`, `Add tests for chunked messages in consumer and reader`,
`Add changelog entries for chunked messages`, `Mark where the dead letter producer needs chunking`.
After the merge: 1737 unit tests green in Debug, 1733 in Release (`MessageProcessorTests` excluded), 2 consecutive Release runs. On the agent's own base (before WP4):
"Integration" collection 141 passed / 32 skipped / 0 failed; "Transaction" collection 88 passed / 15 skipped / 0 failed (the separate `TransactionWithoutBatchIndexAck` collection was not run).
The merged state has **not** been run against a broker yet. The agent saw one unexplained Release unit failure in its first Release run after the wiring (not reproduced in 8 more runs, test not captured): watch for it.

## What was built

- `MessageId`: `FirstChunkMessageId` (public), internal `ChunkMessageIds` and an internal 7-argument constructor. Chunk info lives in **one nullable private object** (not two fields) to cost +8 B per id instead of +16 B.
  Equality, hash, compare, `ToString` and `TryParse` are unchanged (entry level).
- Options: `MaxPendingChunkedMessage` (default 10), `AutoAckOldestChunkedMessageOnQueueFull` (false), `ExpireTimeOfIncompleteChunkedMessage` (1 min) on `ConsumerOptions`, `ReaderOptions`, `IConsumerBuilder`, `IReaderBuilder`.
  Validation (a negative expire time) is in `PulsarClient.CreateConsumer`/`CreateReader`, message text prefixed with the options type (like `NegativeAcknowledgeRedeliveryDelay`).
- Receive path: one `ChunkedMessageAssembler` per sub-consumer/sub-reader, shared by all its channels. `ConsumerChannel.Receive` runs the chunk step **after the decrypt position (`TODO(WP9)` marker) and before decompression**.
  Acks that the assembler asks for are sent in the background (fire-and-forget keeps the `Receive` state machine small). Ids handed back by the assembler go to the ack-timeout tracker.
  The assembler is cleared on seek and disposed with the sub-consumer, sub-reader and on channel fault. On a decompression failure the last chunk is rejected and the earlier chunks are individually acked.
- Expiry timer acks go through `SubConsumer.AcknowledgeChunks` (a no-op delegate for readers, as the spec says). Readers still send the channel's chunk acks (duplicate, corrupted, eviction with auto-ack, decompression failure).
- Ack/nack/redelivery/seek:
  - an individual ack of a chunked id expands to all chunk ids, in the plain and transactional paths, single and list forms (cumulative ack uses the id itself);
  - redelivery and nack expand through a new `ExpandChunks` step that runs **after** the dead-letter step (so a dead-lettered chunked message is acked as a whole; `DeadLetterHandler.TryDeadLetter` acks with the chunk ids);
  - seek and the reader start id use the first chunk (a start at the last chunk delivers nothing).
- Internal: new constructor parameters (chunk assembler), `SubConsumer.AcknowledgeChunks`.

## Deviations from the specs and notes

1. One private chunk-info object in `MessageId` instead of two fields.
2. `ToEntryLevel` keeps the chunk ids; `ExpandChunks` is a separate step after the dead-letter step. (The wp8a note said `ToEntryLevel` expands, but then only the last chunk would be dead-lettered.)
3. The reader start id uses the first chunk.
4. The `InternalAcknowledge` finally-clear of the pooled command keys off `Count > 1`; the ids from the assembler are shared by reference with the command's id list, so the single-id ack clears them afterwards (a test covers it).
5. New wiring tests use `[Fact(Timeout = 30_000)]` (no precedent in the repo) so a message that never arrives fails instead of hanging.
6. `pulsar-perf produce -ch` produces chunked messages, so the consumer integration tests run without a chunking producer. The perf tool sent **two** chunked messages for `-m 1`; those tests tolerate extras.

## Allocation numbers (Release, 2,000 messages)

Receive of a non-chunked message: already queued 720 → 728 B/msg, waiting 2032 → 2040 B/msg. The +8 B is the new `MessageId` field. `Receive` itself adds nothing. `AckBenchmarks.PlainAck` stayed at 136 B (see followup 0007).

## Tests

- 79 new unit tests (63 chunk-specific): `Internal/Chunking/*` (channel, sub-consumer, sub-reader, wiring, helper).
- Integration: `MessageChunkingTests` (10: assembly with chunk ids, individual and cumulative ack, ack-timeout redelivery, nack, reader start at a chunked id, seek, max-pending with auto-ack, resent first chunk, orphaned last chunks);
  `TransactionConsumeTests.Acknowledge_GivenChunkedMessageAndTransaction_ShouldLeaveNoUnackedMessagesAfterCommit` (= `testAckChunkMessage`).
- 14 wiring points were mutation-checked, and breaking the transactional expansion made `testAckChunkMessage` fail against the real broker.

## Still to do for the producer agent (WP8 producer)

Marked `TODO(WP8-producer)`: `testChunkingWithOrderingKey`, `testChunkSize`, `testBlockIfQueueFullWhenChunking`, `testRepeatedSeekDoesNotRetainFilteredChunkMessageIds`, the non-persistent-topic tests, `MessageChunkingSharedTest` single/multi-consumer,
and the dead-letter producer (`Consumer.CreateDeadLetterProducer`, marker comment) must chunk a large message.

## Finding

`ChunkedMessageAssembler` allocates `new MessageIdData[numberOfChunks]` from the broker-supplied `num_chunks_from_msg` without an upper bound (a malicious or buggy broker could ask for a huge array). Listed in followup 0008.
