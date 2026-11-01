# 0011 - Findings from WP8 (producer chunking)

Status: items 2 and 4 closed in WP10; items 1, 3, 5 and 6 are open review notes.

## 1. A chunking producer clones the metadata of every message

A producer with `EnableChunking(true)` plans every message (`PlanChunking`), which clones the metadata: a small message that is not split costs +555 B (3497 B against 2942 B for a plain send). Add a fast path that skips planning when the payload obviously fits (payload length plus a metadata allowance below the maximum message size). Low severity, only chunking producers pay it.

## 2. The schema-version check in `PlanChunking` is a copy of the one in `Send`

Followup 0005 item 1 (the schema-version check that can never be true) is copied into `PlanChunking` to stay identical to `Send`. Fixing it in one place should fix both. **FIXED in WP10**: both use `HasSchemaVersion` now.

## 3. `Connection.Send` computes the CRC32C before the size check

An oversized message costs a CRC before it is rejected. Move the check before the checksum. Low severity.

## 4. Two Transaction collection failures to re-check on the tip

`TransactionConsumerEndToEndTests.Acknowledge_GivenNoBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` and the `GivenBatchAndSharedSubscription` variant hit the 3 minute test timeout in a full Transaction collection run while another agent's broker was up. They pass alone and the whole class passes (24 passed, 2 skipped). Not run on the baseline. Repeat the full collection on a quiet machine (WP10), and if they fail again find out what the redelivery after abort waits for.

## 5. Chunk responses and `RequestResponseHandler`

The FIFO per key for chunk sends is new behaviour of a shared component (spec deviation 1 of the WP8 producer note). Reviewers should check close and dispose paths (pending chunk requests are cancelled) and the interplay with the resend after reconnect.

## 6. The perf-tool chunk helper is kept on purpose

`ProduceChunkedMessages` (the perf tool of the broker image, a Java producer) is still used by the 7 consumer tests of `MessageChunkingTests` and by `TransactionFixtureBase`: with it they test that the .NET consumer reads chunks written by the Java client. The tests that only need chunks use the .NET producer. Keep both (interop), and note that the perf tool sends one or two more messages than asked (followup 0008 item 3).

## Status at the end of WP10

Item 2 is fixed (WP10). Item 4 is done: the two redelivery tests passed in each of the 20 iterations of the WP10 loop, which ran the whole Transaction collection on a quiet machine. Items 1, 3, 5 and 6 are open review notes (the fast path for a chunking producer, the CRC before the size check, the `RequestResponseHandler` review, the perf-tool helper that is kept on purpose). Item 5 was reviewed after WP11-D, see the last section.

Decisions for the items that are left: items 1 and 3 are accepted performance notes (only a chunking producer pays the clone of the metadata, and only an oversized message pays the CRC before the size check); item 5 is a review note for the maintainers (the FIFO per key of `RequestResponseHandler` for chunk sends); item 6 is intentional.

## Review of item 5 (2026-11-01, after WP11-D)

**Read:** `RequestResponseHandler` (the queue per key, `Incoming`, `Dispose`, `CancelChunkRequests`), `ChannelManager` (`Outgoing(CommandSend)`, `Incoming(CommandCloseProducer)`, the close that the client starts, `Dispose`), `Connection.Send` (registration and then the write), `SubProducer` (`SendChunks`, `CreateResponse`, `ResponseProcessor`, the send timeout) and `ProducerChannel.SendChunk`. On the broker side `Producer.java` of apache/pulsar v4.0.9 (`close`, `closeNow`, `publishOperationCompleted`, `disconnect`), to find out when a broker answers the sends of a producer.

**The FIFO holds on every path that can be reached:**

- A chunk is registered under the sender lock of the producer, right before the write, and the frame is serialized before. After the registration only `_stream.Send` can fail, which means a dead connection, and the handler is disposed with it.
- One lock, always the innermost; the task completion sources are completed outside of it. No lock is taken in the other order.
- The broker closes the producer (`CommandCloseProducer`): the requests of that producer are cancelled.
- The client closes the producer: `Producer.close` of the broker waits until every pending publish is answered before it closes (`closeNow` only runs when `pendingPublishAcks == 0`, or from `publishOperationCompleted` when it gets there), so every chunk that was written is answered before the `Success` of the close.
- The connection is disposed: the requests are cancelled, and `Connection.Send` refuses new sends (`ThrowIfDisposed`) as it does for every other request.
- A send timeout does not touch the entries. The broker still answers the chunks that were written, in order, and the answers are consumed and ignored; `SendChunks` stops writing when the message was removed.
- A resend after a reconnect to another connection uses another handler. On the same connection see the gap below.
- The seven existing tests (order of the answers, errors, other keys and producers, more answers than chunks, close, dispose) cover the paths above.

**One gap, found by reading and not seen on a real broker:** the requests of a producer are only cancelled by a `CommandCloseProducer` that finds the channel registered, and by `Dispose`. When a producer registers again on a connection that still holds requests of an earlier channel of it (for example when the broker replaces the producer of the same connection, the same-connection path of WP11-A, and does not answer its sends), the entries stay in the queue and take the answers of the next channel. A producer has the same id on every connection and a sequence id restarts, so the last chunk of the next chunked message with the same producer id and sequence id never gets its answer; with the default `SendTimeout` (off) the message never completes.

**Fix:** the commit `Cancel stale chunk requests at registration`: `RequestResponseHandler.Outgoing(CommandProducer)` cancels the chunk requests of the producer id (and does nothing when no chunk request is pending). The new test `Outgoing_GivenCommandProducerForProducerWithPendingChunks_...` fails without it; a second new test checks that the chunks of other producers are kept. The chunk integration tests passed 3 of 3 runs (425 tests of the filters `Chunk`, `ChannelManager` and `RequestResponseHandlerTests`) after it (Debug).

**Left as it is:** the assumption that the broker answers every chunk that it was sent exactly once and in order (checked for the close of the producer in 4.0.9, not for other versions or for the replaced producer); an answer that arrives after the `CommandCloseProducer` of the same producer is dropped, which is only wrong if the producer has registered again in the meantime (it needs a round trip, the late answer does not).
