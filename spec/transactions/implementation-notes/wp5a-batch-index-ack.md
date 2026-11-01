# WP5a as built: batch-index acknowledgment

## What WP5 must call

```csharp
Task<BaseCommand> IConsumerChannel<TMessage>.SendTransactional(CommandAck command, CancellationToken ct)
```

- **Caller builds** a fresh, non-pooled `CommandAck` with `AckType`, `TxnidMostBits`, `TxnidLeastBits`, and one `MessageIdData` per id filled with `MapFrom(id)`.
  If `id.BatchSize > 0` (internal property; tests use the reflection helper `MessageIdInternals`) also set `data.BatchSize`. The caller does **not** set `ConsumerId` or `RequestId`.
- It throws `NotSupportedException` first if `connection.ProtocolVersion < 17`. Inside the executor that would fault the consumer, so check outside it.
- **Batched ids** (`BatchIndex >= 0`): the batch size comes from the tracker, else from `data.BatchSize`. If neither is known the id is sent as a whole-entry ack (Java parity).
  - Individual: `ack_set` has everything set except that index; the tracker is untouched.
  - Cumulative: `BatchHandler.AcknowledgeCumulative` clears the tracker's bits and removes its tracker and any earlier ones, then writes the ack set with `0..i` cleared.
  - Either way it sets `batch_size`, clears `batch_index`, and always sends the ack set regardless of `BatchIndexAckEnabled`.
- **Non-batched ids** are left untouched. Re-sending an already prepared command leaves it unchanged, so executor retries are safe.
- **Response:** the raw `AckResponse` command. Evaluate errors outside the executor with `ThrowIfAckError()`.
- `SubConsumer.InternalTransactionalAcknowledge` is assigned to WP5.

## Bitset helpers (`AckSetWriter`, all `public static`)

`WordCount(int batchSize)`; `WriteIndividual(RepeatedField<long> target, int batchSize, int batchIndex)`; `WriteCumulative(target, batchSize, batchIndex)`;
`WriteWords(target, ReadOnlySpan<ulong> words)` (trims trailing zero words); `SetBits`; `ClearBitsUpTo`. Layout is `BitSet.toLongArray()`, a set bit means "still unacked".
The writers are zero-allocation after warm-up (stackalloc up to 2048 messages, otherwise pooled); a test asserts 0 bytes.

## Internal signatures later WPs touch

- `ConsumerChannel` and `ConsumerChannelFactory` each have a trailing `batchIndexAckEnabled` parameter (after `ackReceiptEnabled`).
- `BatchHandler(bool, IMessageFactory, string topic)`; `BatchHandler.Add(..., out int chargedButSkipped)`; `BatchHandler.CopyAckSetTo`, `IsTracked`; `BatchAckTracker.IntersectWith`.
- `MessagePackage(MessageIdData, uint, RepeatedField<long> ackSet, ReadOnlySequence<byte>)`.
- `MessageId` has an internal `BatchSize`.

## Decision changed in WP5

The spec's AND-merge of a redelivered tracker is replaced by **replace-on-redelivery** (see `followups/0004` and `12-risks-open-questions.md` D8).

## Not done / left to other WPs

- `CommandSend.NumMessages` is never reset (producer, WP6).
- Skipped: `BatchMessageIndexAckTest#testBatchMessageIndexAckForSharedSubscription` (needs negative ack, WP7) and the Disable variant (needs ack timeout, WP7).
- Not ported (need producer batching, WP6): `BatchMessageWithBatchIndexLevelTest#*`, four `BatchMessageTest#*` ack tests, `ConsumerSeekBatchIndexAckTest`.
