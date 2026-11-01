# WP6a as built: batching container core (not wired)

New files: public `BatcherType` (`byte`-backed `{ Default, KeyBased }`), and `src/DotPulsar/Internal/Batching/*` (namespace `DotPulsar.Internal.Batching`).
No existing file was edited. Known-answer bytes were produced with the Java client of Pulsar 4.0.9 (`Commands.serializeSingleMessageInBatchWithPayload`, `initBatchMessageMetadata`);
the C# payload and batch metadata (default, transactional + compression, key-based) match them byte for byte. The batching round allocates 0 bytes in steady state.
It was checked on net8.0, .NET Core 3.1 and .NET Framework 4.8 (netstandard2.0 build) as well.

## Differences from the spec

- The containers are generic over `IBatchItem` (`Metadata`, `Data`) instead of taking `SendOp`. `SendOp` already has matching members, so wiring adds only `: IBatchItem` to it.
  The interface lives in `Internal/Batching/`, not `Internal/Abstractions/`.
- `CreateBatches(destination, maxMessageSize)` has an extra `maxMessageSize` parameter (buffer size hint). `Items` replaces `Ops`. `PreparedBatchGroup` also carries `SequenceId` and `HighestSequenceId`.
- Added: `CanAddToCurrentBatch`, `GetRemainingPublishDelay`, `ShouldFlush`, `HighestSequenceId`, `PreparedBatchPool`, `BatchMessageContainerFactory.Create`.
- Java parity: the single-message sequence id is copied only if the source has it; `HasSameSchema` includes the `schema_id` branch; `PreparedBatch.Reset` clears every proto field (a reflection test guards this).
- Key-based key is a struct `BatchKey` (no per-message allocation). An ordering key never equals a partition key; a partition key of `""` is a different group from "no key".
- Compression stays in `ProducerChannel.Send`, as the spec says. The container writes the uncompressed batch payload.
- Not ported: the Java oversized-batch, OOM and encryption tests (SubProducer-level, or no managed equivalent).

## Call surface for the wiring step

`PreparedBatchPool.Shared` is the default pool. Containers are **not thread-safe**: one per sub-producer, used from the dispatcher only.

```csharp
IBatchMessageContainer<SendOp> container = BatchMessageContainerFactory.Create<SendOp>(batcherType, batchingMaxMessages, batchingMaxBytes, batchingMaxPublishDelay);
var groups = new List<PreparedBatchGroup>();
```

`IBatchMessageContainer<TItem>`:
- State: `NumMessagesInBatch`, `CurrentBatchSize` (uncompressed payload sum), `IsEmpty`, `HighestSequenceId` (of the last message added), `Items`.
- Limit checks: `HaveEnoughSpace(item, max)`, `HasSameTxn(item)`, `HasSameSchema(item)`, `CanAddToCurrentBatch(item, max)`, `IsBatchFull(max)`.
- `Add(item, max)` returns true when the container is full afterwards.
- `GetRemainingPublishDelay()` is `Timeout.InfiniteTimeSpan` when empty and `TimeSpan.Zero` when elapsed; it counts from the first message added and uses an injectable clock.
  `ShouldFlush(max)` is true when not empty and either full or the delay has elapsed.
- `CreateBatches(groups, max)` appends groups in send order and does not clear. `Clear()` resets the container.

Other types: `PreparedBatchGroup { Start, Count, Batch, SequenceId, HighestSequenceId }` (`Start` indexes into `Items`; `Batch` is null when `Count == 1`);
`PreparedBatch { Metadata (proto MessageMetadata), Payload (uncompressed), Count, SequenceId, HighestSequenceId, Return() (idempotent) }`;
`BatchMessageIds.Create(receiptMessageIdData, partition, topic, batchSize, batchIndex)` (a batch of more than 1 message gets the index within the batch; a single message keeps the receipt's batch index).

## Dispatcher sequence

1. First op: if `!CanAddToBatch(op)`, send it alone; otherwise `container.Add(first, channel.MaxMessageSize)` unconditionally (an oversized first message still forms its own batch).
2. Next ops: peek the next queued op. Stop if it is a potential duplicate (sequence id `<= _lastSequenceIdPushed` or `<= container.HighestSequenceId`) or `!container.CanAddToCurrentBatch(next, max)`.
   Otherwise `MoveNext()` and `Add`; stop when `Add` returns true.
3. If nothing is queued, stop when a flush was requested or `GetRemainingPublishDelay() == 0`; otherwise `WaitForNextItem(remaining)`.
4. Closing: `groups.Clear(); container.CreateBatches(groups, max);`. For key-based, call `RewriteItemsUpToCursor(container.Items)` next. For each group set `items[g.Start].BatchCount = g.Count` and `.Batch = g.Batch`,
   and `BatchCount = -1` on the other members. Update `_lastSequenceIdPushed` with `g.HighestSequenceId`. Then `container.Clear()`.
5. Send: `channel.Send(head.Batch.Metadata, head.Batch.Payload, tcs, ct)`; a single message goes out as-is.
6. Receipt: the expected sequence id is `g.SequenceId` (`Batch.SequenceId`, or `Metadata.SequenceId` for a single message). Complete each op with `BatchMessageIds.Create(receipt.MessageId, _partition, Topic, count, i)`,
   then `head.Batch?.Return()`. `SendOp.Dispose` should also return it.
7. Transaction rule: `HasSameTxn` inside `CanAddToCurrentBatch`. The container adopts the first message's identity from the proto `TxnidMostBits/LeastBits` **has-bits**, so a transaction `(0, x)` is not mistaken for a plain
   message, and copies it to the batch metadata. Mixed plain and transactional messages, and different transactions, return false.

## Pitfalls for the wiring step

- `ProducerChannel.Send` leaves `CommandSend.NumMessages = 1` and `highest_sequence_id` unset. A batch sent through it carries wrong command fields, so §4.11 (set `NumMessages` and `HighestSequenceId`, reset them after) must land with the wiring.
  (`CommandSend.NumMessages` is never reset today: pre-existing bug, spec README §6 #7.)
- `metadata.SchemaVersion is null` in `ProducerChannel` can never be true (the proto getter returns a default value), so the schema injection there looks dead. Pre-existing; see `followups/0005`.
- Batches that contain a transactional message: the `RegistrationGate` of WP4 is per op. The dispatcher awaits the gate of **each** op before adding it to the container; only then is `HasSameTxn` meaningful.

## Test note (found while merging WP5)

`BatchingAllocationTests` failed once in a Release run under memory pressure (8240 bytes): `PooledBufferWriter` rents from `ArrayPool<byte>.Shared`, which trims on a gen 2 GC under pressure, and the next rent allocates.
The tests now take the best of up to 5 measurements (commit `Make batching allocation tests take best of five`). Any new zero-allocation test should do the same.
