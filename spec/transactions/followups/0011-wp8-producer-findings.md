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

Item 2 is fixed (WP10). Item 4 is done: the two redelivery tests passed in each of the 20 iterations of the WP10 loop, which ran the whole Transaction collection on a quiet machine. Items 1, 3, 5 and 6 are open review notes (the fast path for a chunking producer, the CRC before the size check, the `RequestResponseHandler` review, the perf-tool helper that is kept on purpose).

Decisions for the items that are left: items 1 and 3 are accepted performance notes (only a chunking producer pays the clone of the metadata, and only an oversized message pays the CRC before the size check); item 5 is a review note for the maintainers (the FIFO per key of `RequestResponseHandler` for chunk sends); item 6 is intentional.
