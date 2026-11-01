# Work package: Message chunking (producer split + consumer reassembly)

Status: DRAFT spec (prerequisite for transactions). Implement from this file alone; every behavioural rule cites its upstream source.

Pinned upstream:
- Java (primary): apache/pulsar @ f3846f4811b6a7d98de40a3be2caff1fe288e026 (`java:` below, path relative to repo root).
- C++ (cross-reference): apache/pulsar-client-cpp @ f71d7e99019d58b1d11858f05ff0dc83b718627b (`cpp:` below).
- DotPulsar: HEAD e8f2ba22c7136a35d279451e7278589fb0084d2e (`dp:` below, path relative to `src/DotPulsar/`).

Driving portable test: `java:pulsar-broker/src/test/java/org/apache/pulsar/broker/transaction/TransactionConsumeTest.java#testAckChunkMessage (L394-L434)` — 6,000,000-byte `Schema.STRING` value, `enableChunking(true)`, `enableBatching(false)`, Shared subscription, ack with transaction, commit, assert `unackedMessages == 0`. It also asserts that the returned id is a chunk id whose first chunk id != last chunk id.

Sibling specs (reference only, do not duplicate):
- `batching.md` (agent prereq-batching): producer batching, batch-index ack / ack_set, ack receipt, send timeout, `MessageId` batch extensions. Chunking applies only to non-batched sends.
- `encryption.md`: end-to-end encryption. Chunking defines where the per-chunk encrypt/decrypt hooks go (see "Ordering").
- `ack-timeout.md`, `dead-letter-policy.md` (negative ack): must expand chunk ids (see "Interactions").

---

## 1. Current DotPulsar state (verified)

- No message-level chunking. `dp:Internal/ChunkingPipeline.cs` + `dp:Internal/PulsarStream.cs` (`ChunkSize = 75000`, L30) is transport write-chunking only and unrelated.
- `dp:Internal/Connection.cs#Send(SendPackage, …) (L244-L262)` serializes the whole frame synchronously (`Serializer.Serialize(...)`, L250) and throws `TooLargeMessageException` when `sequence.Length > MaxMessageSize` (L252-L253). `MaxMessageSize` comes from `CommandConnected.max_message_size` (`dp:Internal/ConnectionPool.cs` L181). Frame length includes command + headers, so it is stricter than Java (Java compares metadata+payload only).
- Producer pipeline: `Producer.InternalSend` (`dp:Internal/Producer.cs` L233-L315) assigns sequence id, encodes, builds `SendOp(metadata.Metadata, data, tcs, ct)` → `SubProducer.Send` enqueues into `AsyncQueueWithCursor<SendOp>` bounded by `MaxPendingMessages` → `SubProducer.MessageDispatcher` (L107-L149) calls `IProducerChannel.Send(metadata, payload, responseTcs, ct)` → `ProducerChannel.Send` (`dp:Internal/ProducerChannel.cs` L63-L111) sets `PublishTime`, `ProducerName`, `SchemaVersion`, compresses (only if `metadata.Compression == None`), sends; restores the user's (pooled) metadata in `finally`. `SubProducer.ProcessReceipt` (L174-L191) peeks the queue head, requires `receipt.SequenceId == head.Metadata.SequenceId`, dequeues, completes `ReceiptTcs`.
- `SendPackage.Command` (`CommandSend`) is pooled per `ProducerChannel` (`_sendPackagePool`, L25/L80): any field set for one send (e.g. `IsChunk`) leaks to the next unless cleared.
- Consumer pipeline: `ConsumerChannel.Receive` (`dp:Internal/ConsumerChannel.cs` L73-L145): checksum → metadata → data → **decompress** (L102-L117) → batch (L122-L133) or single message (L135). `BatchHandler<T>` is one instance per sub-consumer, shared across reconnects through `ConsumerChannelFactory` (`dp:Internal/Consumer.cs` L463-L465; `dp:Internal/Reader.cs` L289-L291).
- Flow control: `ConsumerChannel` counts each dequeued `MessagePackage` against `_sendWhenZero` and re-sends `CommandFlow` when it reaches 0 (L79-L82, L240-L251). Every received entry (including a chunk) therefore replenishes one permit automatically; there is no "available permits" counter to leak.
- `MessagePackage.Data` is a private copy of each frame (`dp:Internal/Connection.cs` L366 `frame.Slice(...).ToArray()`), so a chunk's bytes stay valid after the frame is processed.
- `MessageId` (`dp:MessageId.cs`) is a sealed class with `LedgerId, EntryId, Partition, BatchIndex, Topic`; `Equals`/`GetHashCode`/`CompareTo` use exactly those fields; `ToString` = `ledger:entry:partition:batch[:topic]` with `TryParse` round-trip.
- `DotPulsar.MessageMetadata.Metadata` (the proto) is `internal` (`dp:MessageMetadata.cs` L32); there is no `InternalsVisibleTo`.
- Proto already has all fields: `MessageMetadata.uuid=26, num_chunks_from_msg=27, total_chunk_msg_size=28, chunk_id=29` (`dp:Internal/PulsarApi.proto` L161-L164), `MessageIdData.first_chunk_message_id=7` (L69), `CommandSend.is_chunk=7` (L541).

## 2. Upstream mapping

| Concern | Java | C++ |
|---|---|---|
| Producer config defaults | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/conf/ProducerConfigurationData.java` `chunkingEnabled=false`, `chunkMaxMessageSize=-1` (L206-L211) | `cpp:lib/ProducerImpl.cc` ctor `chunkingEnabled_ = conf.isChunkingEnabled() && topicName.isPersistent() && !conf.getBatchingEnabled()` (L73) |
| Builder validation | `java:…/impl/ProducerBuilderImpl.java#createAsync` "Batching and chunking of messages can't be enabled together" (L96-L98); `enableChunking`/`chunkMaxMessageSize` (L225-L233) | `cpp:tests/MessageChunkingTest.cc#testInvalidConfig` (L115-L123) |
| Effective chunk size | `java:…/impl/ProducerImpl.java#setChunkMaxMessageSize` (L354-L358) | — |
| Chunking only on persistent topics | `java:…/ProducerImpl.java#isChunkingEnabled` (L3163-L3165), field comment L121 | `cpp:lib/ProducerImpl.cc` L73 |
| Size check w/o chunking | `java:…/ProducerImpl.java#sendAsync` (L604-L624), `#getMessageSizeError` (L3139-L3151) | `cpp:lib/ProducerImpl.cc` L640-L651 |
| Metadata update before split | `java:…/ProducerImpl.java#updateMessageMetadata` (L783-L801) | `cpp:lib/ProducerImpl.cc#setMessageMetadata` call L547 |
| Chunk count | `java:…/ProducerImpl.java#sendAsync` (L646-L670): `payloadChunkSize = maxMessageSize - metadata.getSerializedSize()`; `<=0` → InvalidMessageException; `min(chunkMaxMessageSize, …)`; `totalChunks = ceilDiv(max(1, compressedLen), payloadChunkSize)` | `cpp:lib/ProducerImpl.cc` L549-L563, `#getNumOfChunks` (L659-L664) |
| Per-chunk send loop, uuid | `java:…/ProducerImpl.java#sendAsync` (L683-L773): shared `sequenceId`, `uuid = "%s-%d".format(producerName, sequenceId)` (L720) | `cpp:lib/ProducerImpl.cc` L611-L655 (`uuid = producerName_ + "-" + sequenceId`, L615) |
| Chunk metadata + per-chunk compress/encrypt | `java:…/ProducerImpl.java#serializeAndSendMessage` (L819-L928): slice `[readStart, min(chunkSize, len-readStart))`, `setUuid`, `setChunkId`, `setNumChunksFromMsg`, `setTotalChunkMsgSize(compressedPayloadSize)` (L833-L849); encrypt **each chunk** (L892) | `cpp:lib/ProducerImpl.cc` L622-L637 (`encryptMessage(msgMetadata, chunkedPayload, …)` per chunk) |
| `is_chunk` on CommandSend | `java:pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java#newSend` `if (hasTotalChunkMsgSize && totalChunkMsgSize > 1) setIsChunk(true)` (L639-L641) | — |
| Receipt handling | `java:…/ProducerImpl.java#ackReceived` (L1534-L1617): chunk 0 → `firstChunkMessageId`, last chunk → `ChunkMessageIdImpl(first,last)`; callback only on last chunk (L1592-L1606) | `cpp:lib/ProducerImpl.cc` L968-L974 (collects **all** chunk ids) |
| Producer chunk ctx | `java:…/ProducerImpl.java#ChunkedMessageCtx` (L1763-L1808) | `cpp:lib/OpSendMsg.h` (`chunkMessageIdList`) |
| Chunk message id | `java:…/impl/ChunkMessageIdImpl.java` (whole file, L27-L82): ledger/entry/partition = last chunk; `getFirstChunkMessageId`; `equals` = super (last only); `hashCode` includes first; `toString` = `first;last`; `toByteArray` sets `first_chunk_message_id` | `cpp:lib/ChunkMessageIdImpl.h` (L27-L45): holds full `vector<MessageId>`; last chunk's ledger/entry/partition |
| Consumer config defaults | `java:…/conf/ConsumerConfigurationData.java` `maxPendingChunkedMessage=10` (L248-L253), `autoAckOldestChunkedMessageOnQueueFull=false` (L256-L261), `expireTimeOfIncompleteChunkedMessageMillis=60000` (L264-L268) | `cpp:include/pulsar/ConsumerConfiguration.h` |
| Consumer state | `java:…/impl/ConsumerImpl.java` fields L232-L255; ctor L372-L375; `ConsumerBase.unAckedChunkedMessageIdSequenceMap` (`java:…/ConsumerBase.java` L98) | `cpp:lib/ConsumerImpl.h` L289-L357 (`ChunkedMessageCtx`, `chunkedMessageCache_`, `chunkProcessMutex_`) |
| Receive path | `java:…/ConsumerImpl.java#messageReceived` (L1531-L1743): decrypt (L1577) → chunk: skip decompress (L1591) → assemble (L1610-L1656) → `ChunkMessageIdImpl(ids[0], ids[last])` (L1660-L1663) → decompress assembled (L1664-L1671) → decompress failure acks earlier chunks (L1672-L1686) → register `unAckedChunkedMessageIdSequenceMap` (L1707-L1710) | `cpp:lib/ConsumerImpl.cc#messageReceived` (L706-…; chunk branch L730-L745) |
| Chunk assembly | `java:…/ConsumerImpl.java#doProcessMessageChunk` (L1782-L1952); `#processMessageChunk` (L1751-L1762); `#acknowledgeChunkedMessageIds` (L1772-L1776) | `cpp:lib/ConsumerImpl.cc#processMessageChunk` (L486-L704) |
| Eviction (max pending) | `java:…/ConsumerImpl.java#removeOldestPendingChunkedMessage` (L3433-L3444) | `cpp:lib/ConsumerImpl.cc` L560-L568 |
| Expiry | `java:…/ConsumerImpl.java#removeExpireIncompleteChunkedMessages` (L3446-L3460), `#doRemoveExpireIncompleteChunkedMessages` (L3465-L3487); lazy scheduling (L1787-L1796) | `cpp:lib/ConsumerImpl.cc#triggerCheckExpiredChunkedTimer` (L453-L483), `#discardChunkMessages` (L440-L451) |
| Remove ctx | `java:…/ConsumerImpl.java#removeChunkMessage` (L3493-L3518) | — |
| Consumer ctx | `java:…/ConsumerImpl.java#ChunkedMessageCtx` (L3393-L3428) | `cpp:lib/ConsumerImpl.h#ChunkedMessageCtx` (L289-L339) |
| Non-txn ack expansion | `java:…/PersistentAcknowledgmentsGroupingTracker.java#flushAsync` (L493-L545: individual ack of a chunk id acks all chunk entries; cumulative removes map entry and acks last only), `#newImmediateAckAndFlush` (L616-L640) | `cpp:lib/ConsumerImpl.cc` L2223-L2226, L2270-L2277 |
| Txn ack expansion | `java:…/ConsumerImpl.java#doTransactionAcknowledgeForResponse(MessageId,…)` (L3520-L3585): non-batch → `chunkMsgIds = map.remove(id)`; cumulative or unknown → ack the id; else multi-message ack of all chunk ids with txnid (L3541-L3567). List overload (L3631-L3666) does **not** expand chunks. | — (no txn in C++) |
| Seek | `java:…/ConsumerImpl.java#seekAsync(MessageId)` (L2983-L2992): seek to `firstChunkMessageId` if present | `cpp:lib/ConsumerImpl.cc` L69-L75 (start id inclusive → first chunk), `#seekAsync` L1784 |
| Ack timeout / redelivery expansion | `java:…/UnAckedMessageTracker.java` L158-L160, `#addChunkedMessageIdsAndRemoveFromSequenceMap` (L190-L199) | `cpp:lib/ConsumerImpl.cc` L1709-L1716 |
| Negative ack expansion | `java:…/NegativeAcksTracker.java#add` (L159-L166) | `cpp:lib/ConsumerImpl.cc` L1539-L1544 |
| Frame padding | `java:…/protocol/Commands.java` `DEFAULT_MAX_MESSAGE_SIZE = 5*1024*1024` (L132), `MESSAGE_SIZE_FRAME_PADDING = 10*1024` (L133) | — |

## 3. Public API additions

### 3.1 `ProducerOptions<TMessage>` (`dp:ProducerOptions.cs`)

```csharp
/// <summary>
/// The default of whether to enable chunking.
/// </summary>
public static readonly bool DefaultEnableChunking = false;

/// <summary>
/// The default chunk max message size.
/// </summary>
public static readonly int DefaultChunkMaxMessageSize = 0;

/// <summary>
/// Whether to split messages larger than the broker's max message size into chunks. Only applies to persistent topics and can't be combined with batching. The default is 'false'.
/// </summary>
public bool EnableChunking { get; set; }

/// <summary>
/// Set the max size in bytes of the payload of a single chunk. A value of 0 or less means the broker's max message size is used. The default is 0.
/// </summary>
public int ChunkMaxMessageSize { get; set; }
```
Constructor assigns both defaults.

### 3.2 `IProducerBuilder<TMessage>` (`dp:Abstractions/IProducerBuilder.cs`) + `ProducerBuilder<TMessage>` (`dp:Internal/ProducerBuilder.cs`)

```csharp
/// <summary>
/// Whether to split messages larger than the broker's max message size into chunks. The default is 'false'.
/// </summary>
IProducerBuilder<TMessage> EnableChunking(bool enableChunking);

/// <summary>
/// Set the max size in bytes of the payload of a single chunk. The default is 0 (the broker's max message size).
/// </summary>
IProducerBuilder<TMessage> ChunkMaxMessageSize(int chunkMaxMessageSize);
```
`ProducerBuilder`: fields `_enableChunking`, `_chunkMaxMessageSize` initialized from the `ProducerOptions` defaults; copied into options in `Create()`.

Validation (both `ProducerBuilder.Create()` and `PulsarClient.CreateProducer(options)` next to the existing `MaxPendingMessages` check, `dp:PulsarClient.cs` L63-L64):
- `EnableChunking && <batching enabled per batching.md>` → `throw new ConfigurationException("Batching and chunking of messages can't be enabled together")` (Java L96-L98). If batching.md's default is "enabled", the Java tests' `enableBatching(false)` maps to the batching.md option and must be set in ported tests.

### 3.3 `ConsumerOptions<TMessage>` (`dp:ConsumerOptions.cs`) and `ReaderOptions<TMessage>` (`dp:ReaderOptions.cs`)

Add to both (Java readers expose the same three settings; `MessageChunkingTest#testReaderChunkingConfiguration` L1201-L1213):

```csharp
/// <summary>
/// The default max pending chunked messages.
/// </summary>
public static readonly uint DefaultMaxPendingChunkedMessage = 10;

/// <summary>
/// The default of whether to acknowledge the oldest pending chunked message when the queue is full.
/// </summary>
public static readonly bool DefaultAutoAckOldestChunkedMessageOnQueueFull = false;

/// <summary>
/// The default expire time of incomplete chunked messages.
/// </summary>
public static readonly TimeSpan DefaultExpireTimeOfIncompleteChunkedMessage = TimeSpan.FromMinutes(1);

/// <summary>
/// The max number of chunked messages being assembled at the same time. When exceeded, the oldest is dropped. 0 means no limit. The default is 10.
/// </summary>
public uint MaxPendingChunkedMessage { get; set; }

/// <summary>
/// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
/// </summary>
public bool AutoAckOldestChunkedMessageOnQueueFull { get; set; }

/// <summary>
/// The time after which an incomplete chunked message is dropped and its chunks acknowledged. TimeSpan.Zero disables expiry. The default is 1 minute.
/// </summary>
public TimeSpan ExpireTimeOfIncompleteChunkedMessage { get; set; }
```

`IConsumerBuilder<TMessage>` / `IReaderBuilder<TMessage>` + `ConsumerBuilder` / `ReaderBuilder`: `MaxPendingChunkedMessage(uint)`, `AutoAckOldestChunkedMessageOnQueueFull(bool)`, `ExpireTimeOfIncompleteChunkedMessage(TimeSpan)`, same XML summaries. Validation: `ExpireTimeOfIncompleteChunkedMessage < TimeSpan.Zero` → `ConfigurationException("ExpireTimeOfIncompleteChunkedMessage may not be negative")`.

### 3.4 `MessageId` (`dp:MessageId.cs`)

```csharp
/// <summary>
/// The message id of the first chunk if this is the id of a chunked message, otherwise null.
/// </summary>
public MessageId? FirstChunkMessageId { get; }
```
- Internal (C# `internal`) state: `internal MessageIdData[]? ChunkMessageIds { get; }` — every chunk's id in chunk order (C++ model, `cpp:lib/ChunkMessageIdImpl.h` L29-L36). Producer-returned ids carry all chunk ids too (C++ L968-L974).
- Constructor: `internal MessageId(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic, MessageId? firstChunkMessageId, MessageIdData[]? chunkMessageIds)`; the existing public ctor chains to it with nulls. If batching.md adds its own internal fields/ctor, merge into one internal "full" constructor.
- `LedgerId/EntryId/Partition` = last chunk; `BatchIndex = -1`.
- `Equals`, `GetHashCode`, `CompareTo`, `==`, `ToString`, `TryParse`: **unchanged** (they ignore `FirstChunkMessageId`). This matches Java `equals` (`ChunkMessageIdImpl.java` L72-L75: chunk id equals the plain id of its last chunk) and `compareTo`. Deviation: Java `hashCode` mixes in the first chunk (L77-L80), which breaks the equals/hashCode contract; DotPulsar keeps the contract. Java `toString` `first;last` is not adopted, so `TryParse` round-trips; parsing a chunk id string yields a plain id (no chunk list), the same as an id received from outside the consumer in Java (no map entry → ack last only).
- `ToMessageIdData()` unchanged (does not write `first_chunk_message_id`).

## 4. Ordering of transforms (normative)

Upstream order differs from "compress → encrypt → chunk":

- Producer: **compress whole payload → split → encrypt each chunk** (Java `serializeAndSendMessage` L888-L892: compression only applies to the full payload, since the caller compressed it; `encryptMessageOrReleaseSource(msgMetadata, chunkPayload)` per chunk; C++ L622-L637 identical). Each chunk carries its own `encryption_param` (IV) written by the encryptor into that chunk's metadata.
- Consumer: **decrypt each chunk → reassemble → decompress assembled payload** (Java L1577-L1592: `decryptPayloadIfNeeded` runs per received entry before the chunk branch; L1591: chunked payloads skip decompression; L1664-L1671 decompress assembled). Undecryptable chunks (`ConsumerCryptoFailureAction.Consume`) are assembled still encrypted (Java L1591 `isMessageUndecryptable || isChunkedMessage` → retain, no decompress); encryption.md owns that behaviour.

encryption.md must define the hooks `IEncryptChunk`/`IDecryptPayload` at the points marked **[ENC]** below.

## 5. Producer design

### 5.1 New types

`dp:Internal/ChunkedSendState.cs` (namespace `DotPulsar.Internal`, `public sealed class`):
```csharp
public sealed class ChunkedSendState
{
    public ChunkedSendState(Internal.PulsarApi.MessageMetadata metadata, ReadOnlySequence<byte> payload, int totalChunks, int chunkSize);
    public Internal.PulsarApi.MessageMetadata Metadata { get; }   // private clone; chunk fields set; ChunkId mutated per chunk
    public ReadOnlySequence<byte> Payload { get; }                 // compressed (if compression) full payload
    public int TotalChunks { get; }
    public int ChunkSize { get; }                                  // payload bytes per chunk (last may be smaller)
    public int NextChunkToSend { get; set; }                       // reset to ReceiptedChunks on re-dispatch
    public int ReceiptedChunks { get; set; }
    public MessageIdData[] ChunkMessageIds { get; }                // filled by receipts, length TotalChunks
    public ReadOnlySequence<byte> GetChunk(int chunkId)            // Payload.Slice(chunkId * ChunkSize, Math.Min(ChunkSize, Payload.Length - chunkId * ChunkSize))
}
```
`SendOp` (`dp:Internal/SendOp.cs`) gains `public ChunkedSendState? Chunking { get; set; }` (null until planned; stays null for non-chunked). The plan is computed **once** (first dispatch) and kept across reconnects so chunk boundaries/uuid never change for a message (Java computes once in `sendAsync` and resends the same ops).

`dp:Internal/Constants.cs`: `public const int DefaultMaxMessageSize = 5 * 1024 * 1024;` and `public const int MessageSizeFramePadding = 10 * 1024;` (Java `Commands.java` L132-L133).

### 5.2 `IProducerChannel` / `ProducerChannel`

Add to `dp:Internal/Abstractions/IProducerChannel.cs`:
```csharp
int MaxMessageSize { get; }        // connection.MaxMessageSize (NotReadyChannel: Constants.DefaultMaxMessageSize)
string ProducerName { get; }       // _name
ValueTask<ChunkedSendState?> PlanChunking(SendOp sendOp, int chunkMaxMessageSize, CancellationToken cancellationToken);
Task SendChunk(ChunkedSendState state, int chunkId, ulong sequenceId, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken);
```
(`NotReadyChannel<T>` implements them by throwing like its other members.)

`ProducerChannel.PlanChunking` (exact algorithm, Java `sendAsync` L583-L670):
1. If `sendOp.Metadata.HasNumChunksFromMsg` (caller supplied chunk fields, as Java tests do via `TypedMessageBuilderImpl.getMetadataBuilder()`) → return `null` (send as-is; never re-chunk, never clear those fields).
2. `var metadata = sendOp.Metadata.Clone();` set exactly what `Send` would set: `PublishTime = now ms`, `ProducerName = _name`, `SchemaVersion` (if `metadata.SchemaVersion` empty and `_schemaVersion` not null).
3. Payload: if `_compressorFactory is not null && metadata.Compression == None`: `metadata.Compression = _compressorFactory.CompressionType; metadata.UncompressedSize = (uint) payload.Length; payload = compressor.Compress(payload)` (DotPulsar always compresses when configured; Java's `compressMinMsgBodySize` is not ported).
4. `var payloadChunkSize = MaxMessageSize - metadata.CalculateSize();` if `<= 0` → `throw new TooLargeMessageException(metadata.CalculateSize(), MaxMessageSize)` (Java L657-L665 InvalidMessageException "metadata … exceeds").
5. `if (chunkMaxMessageSize > 0) payloadChunkSize = Math.Min(Math.Min(chunkMaxMessageSize, MaxMessageSize), payloadChunkSize);` (Java L354-L358 + L668).
6. `var totalChunks = (int) ((Math.Max(1, payload.Length) + payloadChunkSize - 1) / payloadChunkSize);` (Java L669).
7. If `totalChunks == 1` → return a state with `TotalChunks = 1` and the compressed payload (so `Send` doesn't recompress; the dispatcher sends it through the normal single-message path using `state.Metadata`/`state.Payload`, no chunk fields set).
8. Else set on the clone: `Uuid = $"{_name}-{metadata.SequenceId}"` (Java L720; C++ L615), `NumChunksFromMsg = totalChunks`, `TotalChunkMsgSize = (int) payload.Length` (compressed size, Java L846-L848), `ChunkId = 0`.

`ProducerChannel.SendChunk(state, chunkId, …)`:
- `state.Metadata.ChunkId = chunkId;` take a pooled `SendPackage`; `Command.SequenceId = sequenceId`; `Command.IsChunk = state.Metadata.TotalChunkMsgSize > 1` (Java `Commands.newSend` L639-L641); `Metadata = state.Metadata`; `Payload = state.GetChunk(chunkId)` **[ENC: encrypt chunk payload with `state.Metadata`; encryption.md]**; `await _connection.Send(...)`. In `finally` call `Command.ClearIsChunk()` before returning to the pool.
- Existing `Send(...)` must also set `Command.IsChunk` from the metadata (`TotalChunkMsgSize > 1`, for caller-supplied chunks) and clear it in `finally`.
- Safe to mutate `state.Metadata.ChunkId` per chunk: `Connection.Send(SendPackage, …)` serializes synchronously at L250 before its first `await`.

`Connection.Send(SendPackage, …)` size check change (L252-L253), exact new condition:
```csharp
if (command.Metadata!.NumChunksFromMsg <= 1 && sequence.Length > MaxMessageSize)
    throw new TooLargeMessageException((int) sequence.Length, MaxMessageSize);
```
Rationale: Java never size-checks chunks (`getMessageSizeError` L3139-L3151 returns null when chunking is enabled) and relies on the broker's frame limit `maxMessageSize + MESSAGE_SIZE_FRAME_PADDING` (10 KiB). A chunk frame is (max − metadataSizeBeforeChunkFields) + metadataWithChunkFields + command ≈ max + ~100 bytes, which the current check would reject. Non-chunk frames keep today's check (no behaviour change).

### 5.3 `SubProducer` (`dp:Internal/SubProducer.cs`)

Constructor gets `bool enableChunking, int chunkMaxMessageSize` (from `Producer.CreateSubProducer`, `dp:Internal/Producer.cs` L156-L172). `enableChunking` is `options.EnableChunking && !topic.StartsWith("non-persistent://", StringComparison.Ordinal)` (Java `isChunkingEnabled` L3163-L3165; C++ L73).

`MessageDispatcher` (L107-L149) loop body, after the cancellation check:
```
if (_enableChunking && sendOp.Chunking is null)
    sendOp.Chunking = await channel.PlanChunking(sendOp, _chunkMaxMessageSize, cancellationToken);   // TooLargeMessageException → fail sendOp.ReceiptTcs, RemoveCurrentItem, continue
if (sendOp.Chunking is { TotalChunks: > 1 } state)
{
    for (var chunkId = state.ReceiptedChunks; chunkId < state.TotalChunks; ++chunkId)   // resume after reconnect
    {
        var tcs = <same response-queue wiring as today>;
        success = await _executor.TryExecuteOnce(() => channel.SendChunk(state, chunkId, sendOp.Metadata.SequenceId, tcs, cancellationToken), CancellationToken.None);
        if (!success) break out of both loops exactly like today (ChannelDisconnected)
    }
    continue;
}
// TotalChunks == 1: channel.Send(state.Metadata, state.Payload, tcs, ct) (already compressed → no recompress); else unchanged path
```
`ProcessReceipt` (L174-L191): after the sequence-id check, if `sendOp.Chunking is { TotalChunks: > 1 } state`:
- `state.ChunkMessageIds[state.ReceiptedChunks] = receipt.MessageId (with Partition = _partition)`; `state.ReceiptedChunks++`.
- If `ReceiptedChunks < TotalChunks` → return **without** dequeuing (the next receipt carries the same sequence id and matches the same head).
- Else dequeue and complete `ReceiptTcs` with `new MessageId(last.LedgerId, last.EntryId, _partition, -1, Topic, firstChunkMessageId: new MessageId(first.LedgerId, first.EntryId, _partition, -1, Topic), chunkMessageIds: state.ChunkMessageIds)` (Java L1592-L1606: callback only on the last chunk).

Failure semantics:
- A chunked message occupies **one** slot of `MaxPendingMessages` (Java/C++ take one permit per chunk; deviation documented: DotPulsar's queue blocks instead of failing, so per-chunk permits add nothing; `testBlockIfQueueFullWhenChunking` only asserts "no deadlock", which holds).
- Cancellation of a partially sent chunked message (`sendOp.CancellationToken`) after chunk ≥1 was written: stop sending further chunks, remove the op, fail its tcs (consumer drops the incomplete message via expiry/eviction; same outcome as Java).
- Reconnect: cursor reset → op re-dispatched → resumes at `ReceiptedChunks` with the original plan (same uuid, same `ChunkSize`). Matches Java (only un-receipted chunk ops stay in `pendingMessages`).

Transactions: every chunk's `CommandSend` must carry `txnid_most_bits/txnid_least_bits` (transaction spec sets them on `SendPackage.Command`; `SendChunk` must apply the same code path).

### 5.4 Producer-side zero-alloc / perf notes
- Non-chunking producers: one `bool` check per dispatch, no allocation.
- Chunked message: one `MessageMetadata.Clone()`, one `ChunkedSendState`, one `MessageIdData[TotalChunks]`; chunks are `ReadOnlySequence<byte>.Slice` views over the (compressed) payload (no copy). Negligible versus a multi-MB payload.
- Compression happens once per message (not per chunk, not per resend).

## 6. Consumer design

### 6.1 New type `dp:Internal/ChunkedMessageAssembler.cs`

```csharp
public sealed class ChunkedMessageAssembler : IDisposable
{
    public ChunkedMessageAssembler(uint maxPendingChunkedMessage, bool autoAckOldestChunkedMessageOnQueueFull, TimeSpan expireTimeOfIncompleteChunkedMessage, Func<long> utcNowMs);
    // Returns null while incomplete / discarded. acksToSend receives ids that must be individually acked (issued by caller after releasing the lock).
    // idsToRedeliver receives ids that must be "tracked" (Java trackMessage → handed to ack-timeout tracker if enabled, else nothing).
    public AssembledChunkedMessage? Process(MessageIdData messageId, MessageMetadata metadata, ReadOnlySequence<byte> chunkPayload, List<MessageIdData> acksToSend, List<MessageIdData> idsToTrack);
    public void RemoveExpired(List<MessageIdData> acksToSend);   // Java doRemoveExpireIncompleteChunkedMessages
    public void Clear();                                          // on Seek: drop all contexts, no acks
    public int PendingCount { get; }                              // test/diagnostics
    public int QueueCount { get; }                                // == Java getPendingChunkedMessageUuidQueueSizeForTest
    public void Dispose();
}
public readonly struct AssembledChunkedMessage { public ReadOnlySequence<byte> Payload { get; } public MessageIdData[] ChunkMessageIds { get; } }
```
State (all guarded by one `object _lock`; Java `chunkedMessageLock`, L235-L241):
- `Dictionary<string, Context> _contexts` (Java `chunkedMessagesMap`), `LinkedList<string> _uuidQueue` + `Dictionary<string, LinkedListNode<string>>` index for O(1) `remove(uuid)` (Java `pendingChunkedMessageUuidQueue`, ordered oldest-first), `int _pendingCount` (Java `pendingChunkedMessageCount`).
- `Context { int TotalChunks; MessageIdData?[] ChunkIds; SequenceBuilder<byte> Payload; long WrittenBytes; int TotalChunkMsgSize; int LastChunkId = -1; long ReceivedTimeMs; }` (Java L3393-L3428).

`Process` = Java `doProcessMessageChunk` (L1782-L1952) translated exactly, with `msgId`/`messageId` both being the chunk entry's `MessageIdData` (ledger, entry):
1. (Java L1784-L1786 permit refund) → **no-op** in DotPulsar (flow is counted per dequeued entry; see §1).
2. Lazy expiry scheduling (L1787-L1796): if `expire > 0` and not yet started → start `System.Threading.Timer` with due time = period = expire, callback raises `ExpiryDue` (see 6.3).
3. `ctx = _contexts.GetValueOrDefault(uuid)`.
4. `chunkId == 0`:
   - If `ctx != null`: `corrupted = ctx.ChunkIds.None(id => id != null && id.LedgerId == messageId.LedgerId && id.EntryId == messageId.EntryId)`; if corrupted → add all non-null `ctx.ChunkIds` to `acksToSend` (L1823-L1832). Then remove ctx: `_contexts.Remove`, `_uuidQueue.Remove(uuid)`, `_pendingCount--` (L1836-L1845).
   - `_pendingCount++`; if `maxPending > 0 && _pendingCount > maxPending` → `RemoveOldestPending(acksToSend, idsToTrack)` (L1847-L1850).
   - Create ctx (`TotalChunks = numChunksFromMsg`, `TotalChunkMsgSize = total_chunk_msg_size`, `ReceivedTimeMs = now`), add to map, append uuid at queue tail (L1851-L1858).
5. Out-of-order check `ctx == null || chunkId != ctx.LastChunkId + 1` (L1862):
   - If `ctx != null && chunkId <= ctx.LastChunkId` (duplicate): if `ctx.ChunkIds` has no entry with same ledger/entry → add `messageId` to `acksToSend` (L1873-L1891). Return null.
   - Else (lost first chunk / gap): if `ctx != null` → remove ctx (map, queue, `_pendingCount--`) (L1899-L1909); `_contexts.Remove(uuid)`. Then: if `expire > 0 && now > metadata.PublishTime + expireMs` → `acksToSend.Add(messageId)` else `idsToTrack.Add(messageId)` (L1920-L1925). Return null. (Java L1917-L1919 permit refund → no-op.)
6. Bounds guard: if `ctx.WrittenBytes + chunkPayload.Length > ctx.TotalChunkMsgSize` → malformed: remove ctx (map, queue, `_pendingCount--`), return null (Java L1932-L1938 throws after releasing; DotPulsar drops it without throwing so the receive loop survives).
7. `ctx.ChunkIds[chunkId] = messageId; ctx.Payload.Append(chunkPayload); ctx.WrittenBytes += len; ctx.LastChunkId = chunkId` (L1930-L1939).
8. If `chunkId != numChunksFromMsg - 1` → return null (L1942-L1945).
9. Last chunk: remove ctx from map, `_uuidQueue.Remove(uuid)`, `_pendingCount--` (Java L1642-L1648) and return `new AssembledChunkedMessage(ctx.Payload.Build(), ctx.ChunkIds!)`.

`RemoveOldestPending` = Java L3433-L3444 + `removeChunkMessage` (L3493-L3518): pop queue head until a uuid with a live ctx is found; for each non-null chunk id → `autoAck ? acksToSend : idsToTrack`; remove ctx; `_pendingCount--`.

`RemoveExpired` = Java L3465-L3487: peek head; ghost (no ctx) → remove and continue; `now > ctx.ReceivedTimeMs + expireMs` → remove uuid from queue + `removeChunkMessage(autoAck: true)`; else stop.

### 6.2 `ConsumerChannel<TMessage>.Receive` edit (`dp:Internal/ConsumerChannel.cs` L98-L135)

New ctor parameter `ChunkedMessageAssembler chunkAssembler` (passed through `ConsumerChannelFactory` like `BatchHandler`). After `var data = messagePackage.ExtractData(metadataSize);`:

```
// [ENC] decrypt here (per entry, before the chunk branch) — encryption.md
var messageIdData = messagePackage.MessageId;
MessageId? chunkedMessageId = null;
if (metadata.NumChunksFromMsg > 1 && !metadata.HasNumMessagesInBatch)
{
    var acks = <reused List<MessageIdData> field, cleared>; var track = <reused list>;
    var assembled = _chunkAssembler.Process(messageIdData, metadata, data, acks, track);
    if (acks.Count > 0) await SendIndividualAcks(acks, cancellationToken);   // new CommandAck{AckType=Individual}, MessageId.AddRange, Send(...)
    if (track.Count > 0) <hand to ack-timeout tracker if enabled (ack-timeout.md); otherwise no-op>;
    if (assembled is null) continue;
    data = assembled.Value.Payload;
    var ids = assembled.Value.ChunkMessageIds;
    chunkedMessageId = new MessageId(ids[^1].LedgerId, ids[^1].EntryId, ids[^1].Partition /* see note */, -1, _topic,
        firstChunkMessageId: ids[0].ToMessageId(_topic) /* batch -1 */, chunkMessageIds: ids);
}
// existing decompression block (uses last chunk's metadata: Compression / UncompressedSize are identical on every chunk)
// on decompression failure of an assembled message: RejectPackage(last entry, DecompressionError) AND individually ack ids[0..^1] (Java L1672-L1686)
...
if (metadata.HasNumMessagesInBatch) { …unchanged… }
return _messageFactory.Create(chunkedMessageId ?? messageIdData.ToMessageId(_topic), redeliveryCount, data, metadata);
```
- `_sendWhenZero` bookkeeping unchanged: a swallowed chunk falls into `continue`, consuming one counted permit exactly like a rejected package; the counter refills via `SendFlow` (no stall; covers Java `testOrphanedLastChunkDoesNotStallDispatch` intent).
- Partition note: `MessageIdData.Partition` from the broker is `-1`/unset for `CommandMessage`; DotPulsar today builds the message id from `MessageIdData` as-is (`ToMessageId(_topic)`). Do the same for chunk ids (consistency with existing `Equals` behaviour).
- `ConsumerChannel.Send(CommandSeek)` (L206-L212): add `_chunkAssembler.Clear()` next to `_batchHandler.Clear()`.
- `ConsumerChannel.DisposeAsync` does not dispose the assembler (it is per sub-consumer, outlives channels).

### 6.3 Expiry driver
`ChunkedMessageAssembler` owns a `System.Threading.Timer` (created lazily on first chunk when expiry > 0; disposed in `Dispose`). Callback: `Interlocked` re-entrancy guard → `RemoveExpired(acks)` under the lock → if any, invoke `Func<IReadOnlyList<MessageIdData>, ValueTask> _sendAcks` supplied by the owner. `SubConsumer`/`SubReader` supply `_sendAcks = ids => _executor.TryExecuteOnce(() => _channel.Send(BuildAck(ids), CancellationToken.None), CancellationToken.None)` (fire-and-forget, failures ignored like Java's logged ack). Readers never ack (DotPulsar `SubReader` has no Acknowledge); for readers the ack delegate is a no-op (Java reader acks are harmless on its non-durable subscription; skipping them is equivalent).
Owner disposal (`SubConsumer.DisposeAsync`, `SubReader.DisposeAsync`) disposes the assembler.

### 6.4 Wiring
- `Consumer.CreateSubConsumer` (`dp:Internal/Consumer.cs` L427-L474): `var chunkAssembler = new ChunkedMessageAssembler(_consumerOptions.MaxPendingChunkedMessage, _consumerOptions.AutoAckOldestChunkedMessageOnQueueFull, _consumerOptions.ExpireTimeOfIncompleteChunkedMessage, () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());` pass to `ConsumerChannelFactory` and `SubConsumer`.
- `Reader.CreateSubReader` (`dp:Internal/Reader.cs` L285-L291): same with `ReaderOptions` values.
- `ConsumerChannelFactory` (`dp:Internal/ConsumerChannelFactory.cs`): new ctor param stored and passed to `new ConsumerChannel<TMessage>(…)` (L60).
- `ConsumerBuilder`/`ReaderBuilder` copy the three options.

### 6.5 Ack / redelivery / seek expansion (`dp:Internal/SubConsumer.cs`)
- `InternalAcknowledge(MessageId, AckType, …)` (L197-L214): if `ackType == Individual && messageId.ChunkMessageIds is { Length: > 1 } ids` → fill the pooled `CommandAck` with one `MessageIdData` per chunk id (clear the list in `finally` like the `IEnumerable` overload does); cumulative → unchanged (ack last chunk id = the id itself; Java `flushAsync` L513-L515, `newImmediateAckAndFlush` L620-L621, C++ L2270-L2272).
- `InternalAcknowledge(IEnumerable<MessageId>, …)` (L216-L236): expand each chunked id the same way.
- Transactional acks (transaction spec, `ConsumerChannel.SendTransactional(CommandAck, ct)`; see batch-index-ack.md §5.5): expand chunked ids for Individual acks in **both** single and list forms, sending one `CommandAck` with all chunk `MessageIdData` + txnid + request id (Java single form L3541-L3567; Java list form L3631-L3666 doesn't expand — DotPulsar expands for consistency; documented improvement, no test depends on the difference).
- `RedeliverUnacknowledgedMessages(IEnumerable<MessageId>)` (L98-L103): expand chunked ids to all chunk ids (Java `UnAckedMessageTracker` L190-L199; C++ L1709-L1716). ack-timeout.md and dead-letter-policy.md (negative ack) must use the same `MessageId.ChunkMessageIds` expansion (C++ L1539-L1544).
- `Seek(MessageId)` (L114-L118) and `SubReader.Seek(MessageId)` (`dp:Internal/SubReader.cs` L82-L86): `var target = messageId.FirstChunkMessageId ?? messageId;` (Java L2987-L2991). DotPulsar's existing seek is broker-inclusive (no client-side start filtering), so seeking to a chunked id redelivers that message; this equals Java's `startMessageIdInclusive()` behaviour.
- Java's `unAckedChunkedMessageIdSequenceMap` is **not** ported: the chunk ids travel on the `MessageId` instance (C++ model). Consequence: no leak/cleanup logic is needed (covers intent of `testRepeatedSeekDoesNotRetainFilteredChunkMessageIds`).

### 6.6 Consumer-side zero-alloc / perf notes
- Non-chunked path: one `int` compare (`NumChunksFromMsg > 1`) per entry; no allocation.
- Assembly is zero-copy: each chunk's private array (already copied by `Connection` L366) is linked via `SequenceBuilder<byte>` (`dp:Internal/SequenceBuilder.cs`), so a 6 MB message is not re-copied; decompressors accept `ReadOnlySequence<byte>`. Java/C++ allocate and copy into one `total_chunk_msg_size` buffer.
- `acks`/`track` lists are reused per channel (cleared before use), allocated once.
- Timer exists only after the first chunk is seen.

## 7. Interactions
- Batching (batching.md): mutual exclusion validated at build time; `SubProducer` only plans chunking for non-batched `SendOp`s. Consumer: batch entries never have `num_chunks_from_msg > 1` (C++ L741-L743 guard `!has_num_messages_in_batch`).
- Encryption (encryption.md): per-chunk encrypt/decrypt points marked **[ENC]**; `TotalChunkMsgSize` is the size of the compressed, **unencrypted** payload (Java L848: `compressedPayloadSize`), while AES-GCM adds a 16-byte tag per chunk. The assembler bounds check (§6.1 step 6) must therefore run on **decrypted** chunk bytes (it does, since decryption precedes `Process`).
- Ack timeout (ack-timeout.md): tracked id for a chunked message = the chunk `MessageId`; on timeout, redeliver all `ChunkMessageIds`. `idsToTrack` from the assembler map to Java `trackMessage(msgId)` (only meaningful when ack timeout is enabled).
- DLQ / negative ack (dead-letter-policy.md): nack of a chunked id nacks every chunk id (C++ L1539-L1544); C++ `testChunkedMessageDLQ` (L543-…) shows DLQ republishes the assembled payload via a chunking-enabled DLQ producer (C++ L2107 sets `setChunkingEnabled(true)` on the DLQ producer) — dead-letter-policy.md should enable chunking (and disable batching) on its DLQ producer.
- Transactions: chunk sends carry txn ids; txn ack expands chunks (§6.5). `TransactionConsumeTest#testAckChunkMessage` relies on: producer returns a chunk id with distinct first/last; consumer txn-ack of the assembled id acks both entries so `unackedMessages == 0` after commit.

## 8. Tests to add

C# names follow `Method_GivenCondition_ShouldOutcome`; each ported method has `// Upstream: <JavaClass>#<method>` above its attribute. Integration classes: `[Collection("Integration"), Trait("Category", "Integration")]` unless noted. Unit: `[Trait("Category", "Unit")]`.

Test helper (tests project, `tests/DotPulsar.Tests/Internal/ChunkTestHelper.cs`): `SendSingleChunk(IProducer<string> producer, string uuid, int chunkId, int totalChunks)` = Java `MessageChunkingTest#sendSingleChunk` (L313-L323): value `$"chunk-{uuid}-{chunkId}|"`, sets `Uuid`, `ChunkId`, `NumChunksFromMsg`, `TotalChunkMsgSize = 100` on the internal proto via reflection on `DotPulsar.MessageMetadata.Metadata` (`BindingFlags.NonPublic | BindingFlags.Instance`), then `producer.Send(metadata, value)`. Requires §5.2 step 1 (caller-supplied chunk fields pass through untouched).

Broker max message size: Java tests mutate `conf.setMaxMessageSize(N)` at runtime. Recommended fixture: `ChunkingFixture` collections, one container per needed value, started with `.WithEnvironment("maxMessageSize", "<N>")` (the image's `apply-config-from-env.py` applies keys present in `standalone.conf`; verify during implementation, see Open decisions). Values needed: 35, 50, 100, 1024. Tests without `setMaxMessageSize` use `IntegrationFixture`.

### 8.1 Unit tests (port of client-side tests + assembler logic)

`tests/DotPulsar.Tests/MessageIdTests.cs` (existing class, add):
| Java | C# | Class. |
|---|---|---|
| `ChunkMessageIdImplTest#compareToTest` (L28-L42) | `CompareTo_GivenChunkMessageIds_ShouldCompareByLastChunk` | portable |
| `ChunkMessageIdImplTest#hashCodeTest` (L44-L57) | `GetHashCode_GivenDifferentChunkMessageIds_ShouldDiffer` | portable (asserts same id → same hash; different → different) |
| `ChunkMessageIdImplTest#equalsTest` (L59-L75) | `Equals_GivenChunkMessageIdAndItsLastChunkId_ShouldBeEqual` | portable |
| `ChunkMessageIdImplTest#serializeAndDeserializeTest` (L77-L87) | — | not portable (DotPulsar has no `ToByteArray`/`FromByteArray`) |
| `cpp:tests/MessageChunkingTest.cc#ChunkMessageIdTest.testSetChunkMessageId` (L312-L338) | — | not portable (C++ serialization API) |

`tests/DotPulsar.Tests/Internal/ChunkedMessageAssemblerTests.cs` (new; fake clock `Func<long>`): these port the Java tests whose assertions inspect `chunkedMessagesMap`/uuid queue (internal state not observable through DotPulsar's public API):
| Upstream intent | C# |
|---|---|
| `MessageChunkingTest#testExpiryWorksAfterAnEarlierChunkedMessageCompletes` (L341-L373) | `RemoveExpired_GivenCompletedThenIncompleteMessage_ShouldExpireIncomplete` |
| `#testExpirySkipsStaleQueueHeadFromDiscardPath` (L389-L418) | `RemoveExpired_GivenStaleQueueHeadFromDiscardPath_ShouldStillExpireLiveEntries` |
| `#testExpiryStaleHeadSkipDoesNotDropConcurrentLiveEntry` (L435-L557) | `RemoveExpired_GivenCompletionOfOtherMessage_ShouldKeepLiveEntryQueued` (race part not portable — single lock makes the interleaving impossible; assert final state only) |
| `#testNonHeadCompletionKeepsIncompleteHeadExpirable` (L559-L599) | `Process_GivenNonHeadCompletion_ShouldKeepIncompleteHeadExpirable` |
| `#testPendingChunkedMessageUuidQueueDoesNotLeak` (L835-L875) | `Process_GivenFiftyCompletedMessages_ShouldLeaveNoPendingContexts` (numMessages = 50, maxPending = 10) |
| `#testMaxPendingChunkMessages` assembler part (L612-L667) | `Process_GivenMaxPendingExceededWithAutoAck_ShouldAckOldestChunks` |
| `#testResendChunkMessagesWithoutAckHole` assembler part (L877-L907) | `Process_GivenResentFirstChunkWithSameEntry_ShouldNotAck` |
| corrupted first chunk (Java L1813-L1832) | `Process_GivenFirstChunkAgainWithDifferentEntry_ShouldAckPreviousChunks` |
| duplicated middle chunk (Java L1864-L1891) | `Process_GivenDuplicatedChunkWithDifferentEntry_ShouldAckDuplicate` |
| gap / lost first chunk (Java L1893-L1926) | `Process_GivenChunkGapAfterExpiry_ShouldAckChunk` and `…BeforeExpiry_ShouldTrackChunk` |
| overflow guard (§6.1 step 6) | `Process_GivenChunkOverflowingTotalSize_ShouldDropContext` |
| `#testExpireIncompleteChunkMessage` (L951-L1001) | `RemoveExpired_GivenIncompleteMessageOlderThanExpiry_ShouldRemoveContext` (Java body uses a spy producer + reflection; port the assertion against the assembler) |

`tests/DotPulsar.Tests/Internal/ProducerChannelTests.cs` (new or existing) — chunk planning with a substituted `IConnection` (`MaxMessageSize` set):
| C# | Asserts |
|---|---|
| `PlanChunking_GivenPayloadAboveMaxSize_ShouldSplitIntoCeilDivChunks` | `TotalChunks == ceil(len / (max - metadataSize))`, uuid `"{name}-{seq}"`, `TotalChunkMsgSize == len` |
| `PlanChunking_GivenChunkMaxMessageSize_ShouldUseSmallerChunkSize` | min rule |
| `PlanChunking_GivenMetadataLargerThanMaxSize_ShouldThrowTooLargeMessageException` | Java L657-L665 |
| `PlanChunking_GivenCallerSuppliedChunkFields_ShouldReturnNull` | §5.2 step 1 |
| `SendChunk_GivenChunk_ShouldSetIsChunkAndClearAfterwards` | Java `Commands` L639-L641 + pooled command hygiene |

`tests/DotPulsar.Tests/Internal/SubProducerTests.cs` (existing):
| C# | Asserts |
|---|---|
| `ProcessReceipt_GivenChunkedSendOp_ShouldCompleteOnlyAfterLastChunkWithFirstChunkId` | Java `ackReceived` L1592-L1606 |
| `MessageDispatcher_GivenReconnectAfterPartialReceipts_ShouldResumeAtNextChunk` | §5.3 |

`tests/DotPulsar.Tests/Internal/ProducerBuilderTests.cs` (existing):
| Java/C++ | C# | Class. |
|---|---|---|
| `cpp:tests/MessageChunkingTest.cc#testInvalidConfig` (L115-L123), Java `ProducerBuilderImpl` L96-L98 | `Create_GivenChunkingAndBatchingEnabled_ShouldThrowConfigurationException` | portable |
| `MessageChunkingTest#testReaderChunkingConfiguration` (L1201-L1213) | `ReaderBuilderTests.Create_GivenChunkingSettings_ShouldCopyToOptions` (12, true, 12 ms) | adapted (assert on built options instead of `consumer.conf`) |

### 8.2 Integration tests — `tests/DotPulsar.Tests/MessageChunkingTests.cs` (Java `MessageChunkingTest`)

| Java (lines) | C# | Fixture | Class. / adaptation |
|---|---|---|---|
| `testLargeMessage` (L111-L186) ×4 (`ackReceiptEnabled`,`clientSizeMaxMessageSize`) `{true,true},{true,false},{false,true},{false,false}` | `Receive_GivenLargeMessagesSentWithChunking_ShouldReceiveInOrderAndAckAll` `[Theory]` same 4 `InlineData` | ChunkingFixture 35 / 50 | adapted: ack receipt per batching.md option; LZ4 compression; 5 messages of `i*100` random digits; drop `chunkedMessageRate`/managed-cursor/availablePermits asserts (broker internals) → assert via admin REST `GET …/stats` subscription `unackedMessages == 0` and `msgBacklog == 0` after acks |
| `testChunkingWithOrderingKey` (L188-L209) | `Receive_GivenChunkedMessageWithOrderingKey_ShouldPreserveDataAndOrderingKey` | ChunkingFixture 100 | portable (200 random bytes data, 50 random bytes ordering key) |
| `testLargeMessageAckTimeOut` (L211-L289) ×2 | `Receive_GivenChunkedMessagesNotAcked_ShouldRedeliverAfterAckTimeout` | ChunkingFixture 50 | depends on ack-timeout.md (5 s) and DotPulsar `IReader`; drop tracker-map/cursor internals, assert redelivery of all 5 in order and backlog 0 after `AcknowledgeCumulative(last)` |
| `testPublishWithFailure` (L291-L312) | — | — | not portable (stops the broker; requires send timeout from batching.md; DotPulsar shared fixture). Optional Toxiproxy variant if send timeout exists |
| `testExpiryWorksAfter…`, `testExpirySkipsStale…`, `testExpiryStaleHeadSkip…`, `testNonHeadCompletion…`, `testPendingChunkedMessageUuidQueueDoesNotLeak`, `testExpireIncompleteChunkMessage` | see 8.1 | — | moved to unit (internal-state assertions) |
| `testMaxPendingChunkMessages` (L612-L667) | `Receive_GivenMaxPendingChunkedMessageExceeded_ShouldAutoAckOldestAndNotDeliverIt` | IntegrationFixture | adapted: `ChunkTestHelper.SendSingleChunk`; `maxPendingChunkedMessage(1)`, `autoAckOldestChunkedMessageOnQueueFull(true)`; assert value `"chunk-1-0|chunk-1-1|"`, admin stats `nonContiguousDeletedMessagesRanges == 0`, after `RedeliverUnacknowledgedMessages()` + chunk `"0",1,2` → `ReceiveOrNull(5 s)` is null |
| `testOrphanedLastChunkDoesNotStallDispatch` (L669-L721) | `Receive_GivenTwelveOrphanedLastChunks_ShouldStillDeliverNextMessage` | IntegrationFixture | adapted: `MessagePrefetchCount = 4`, expiry 1 s; replace map-size wait with `Task.Delay(1.5 s)` per iteration (expiry observable only via timing); 12 torn messages; final value `"chunk-live-0|chunk-live-1|"` within 15 s |
| `testLargeMessageOnNonPersistentTopicIsSentOnceWithoutChunking` (L723-L758) | `Send_GivenLargeMessageOnNonPersistentTopic_ShouldSendOnceWithoutChunking` | IntegrationFixture | portable (`chunkMaxMessageSize 100`, `"a" × 500`, second receive within 3 s is null) |
| `testLargeMessageOnNonPersistentTopicAboveBrokerLimitIsRejectedLocally` (L760-L789) | `Send_GivenNonPersistentMessageAboveBrokerLimit_ShouldThrowTooLargeMessageException` | ChunkingFixture 1024 | adapted: DotPulsar throws `TooLargeMessageException` (from `Connection.Send`) instead of `InvalidMessageException`; payload 1024 + 10240 + 1024 bytes. Depends on how `SubProducer` surfaces send-side exceptions (hazard "SendError faults whole SubProducer"); if it faults the producer, mark not portable |
| `testNonPersistentMessageOverLimitOnlyWithMetadataIsRejectedLocally` (L791-L833) | — | — | not portable (client-side `setMaxMessageSize` on a live producer + continuing after failure) |
| `testResendChunkMessagesWithoutAckHole` (L877-L907) | `Acknowledge_GivenResentFirstChunk_ShouldLeaveNoAckHole` | IntegrationFixture | adapted (helper); assert admin `nonContiguousDeletedMessagesRanges == 0` |
| `testResendChunkMessages` (L909-L949) | `Receive_GivenResentAndInterleavedChunks_ShouldAssembleBothMessages` | IntegrationFixture | adapted (helper); drop `availablePermits == 8` (no permit counter in DotPulsar) |
| `testChunksEnqueueFailed` (L1003-L1037) | — | — | not portable (memory limit controller, non-blocking `ProducerQueueIsFullError`) |
| `testSeekChunkMessages` (L1045-L1113) | `Seek_GivenChunkedMessageId_ShouldRedeliverFromThatMessage` | ChunkingFixture 50 | adapted: only the inclusive expectations (consumer1 + inclusive reader) apply because DotPulsar seek/`StartMessageId` has no exclusive client filtering; consumer2/exclusive-reader parts not portable |
| `testRepeatedSeekDoesNotRetainFilteredChunkMessageIds` (L1115-L1199) `{false},{true}` | `Seek_GivenEachChunkedBoundary_ShouldRedeliverFromBoundary` | IntegrationFixture | adapted: only `inclusive=true` expectations; map assertions dropped (no map in DotPulsar, §6.5). `chunkMaxMessageSize 100`, 4 messages `("message-" + i) × 30`, marker `"marker"` not chunked (`FirstChunkMessageId is null`) |
| `testChunkSize` (L1215-L1235) | `Send_GivenSizesUpToMaxMessageSize_ShouldChunkOnlyAbovePayloadChunkSize` | ChunkingFixture 50 | adapted: Java hardcodes metadata size 32; compute expected `payloadChunkSize = 50 - <metadata size>` from the first non-chunked send (or assert monotonic switch: first chunked size s ⇒ every size ≥ s chunked, every size < s not), using `FirstChunkMessageId is not null` instead of class checks |
| `testBlockIfQueueFullWhenChunking` (L1237-L1275) | `Send_GivenMaxPendingMessagesSmallerThanChunkCount_ShouldNotDeadlock` | ChunkingFixture 50 | portable (`MaxPendingMessages = 3`; 200-byte then 3 × 100-byte concurrent sends complete within 5 s) |

### 8.3 Integration tests — `tests/DotPulsar.Tests/MessageChunkingSharedTests.cs` (Java `MessageChunkingSharedTest`, `MAX_MESSAGE_SIZE = 100` as `ChunkMaxMessageSize`)

| Java | C# | Class. |
|---|---|---|
| `testSingleConsumer` (L61-L95) | `Receive_GivenChunkCountsAboveAndBelowPrefetch_ShouldReceiveAllInOrder` | portable (`MessagePrefetchCount = 5`, Shared, values with 1/10/4 chunks, 3 s receive) |
| `testMultiConsumers` (L97-L135) | `Receive_GivenTwoSharedConsumers_ShouldSplitChunkedMessagesBetweenThem` | adapted (receive loops instead of message listeners; 10 messages of 4 chunks; 15 s) |
| `testInterleavedChunks` (L137-L182) | `Receive_GivenInterleavedChunksFromTwoProducers_ShouldDeliverInCompletionOrder` | adapted: Java publishes straight into `PersistentTopic`; port uses two DotPulsar producers named "A"/"B" with explicit `SequenceId` and chunk fields via reflection (uuid `"{producer}-{seq}"`, `TotalChunkMsgSize = numChunks`, payload "a"); same expected orders `A-0,B-0` → `+B-1` → `+A-1`, then second consumer receives ≥4 after first closes |
| `testNegativeAckChunkedMessage` (L184-L206) | `NegativeAcknowledge_GivenChunkedMessage_ShouldRedeliverIt` | dependent on dead-letter-policy.md negative ack (redelivery delay 1 s, `"X" × 10240`, chunk size 1024) |

C++ tests (`cpp:tests/MessageChunkingTest.cc`): `testEndToEnd` (L125), `testExpireIncompleteChunkMessage` (L185), `testMaxPendingChunkMessages` (L219), `testSeekChunkMessages` (L262), `testResendChunkMessages*` (L340-L460) duplicate the Java coverage above; `testResendChunkWithAckHoleMessages` (L421) → covered by `Process_GivenFirstChunkAgainWithDifferentEntry_ShouldAckPreviousChunks`; `testNegativeAckChunkedMessage` (L461), `testAckTimeoutChunkedMessage` (L502), `testChunkedMessageDLQ` (L543) → add to the ack-timeout / DLQ specs' test lists as cross-feature tests (classification: portable once those features exist).

### 8.4 Transaction test (driver)
`tests/DotPulsar.Tests/Transactions/TransactionConsumeTests.cs`:
```
// Upstream: TransactionConsumeTest#testAckChunkMessage
[Fact]
public async Task Acknowledge_GivenChunkedMessageAndTransaction_ShouldLeaveNoUnackedMessagesAfterCommit()
```
Exact reproduction: producer name `"test-producer"`, subscription `"testAckChunkMessage"`, topic `persistent://public/txn/txn-consume-test` (non-partitioned, created via admin), `Schema.String`, `EnableChunking(true)` + batching disabled, Shared, value `new string('a', 6000 * 1000)`; assert `messageId.FirstChunkMessageId` is not null and `!= messageId` (Java asserts `getLastChunkMessageId() != getFirstChunkMessageId()`); transaction timeout 5 hours; receive; `Acknowledge(msg.MessageId, transaction)`; `Commit`; admin stats `subscriptions["testAckChunkMessage"].unackedMessages == 0`. Broker default max message size 5,242,880 ⇒ 2 chunks.

## 9. Acceptance criteria
1. `EnableChunking` + batching → `ConfigurationException` with the exact Java message.
2. With chunking enabled on a persistent topic, a payload whose (compressed) size exceeds `MaxMessageSize - metadataSize` (or `ChunkMaxMessageSize`) is sent as `ceil(len / chunkSize)` `CommandSend`s sharing one sequence id, with `uuid = "{producerName}-{sequenceId}"`, `chunk_id = 0..n-1`, `num_chunks_from_msg = n`, `total_chunk_msg_size = compressed length`, `is_chunk = true`; the returned `MessageId` is the last chunk's id with `FirstChunkMessageId` = first chunk's id, completed only after the last receipt.
3. Non-persistent topics never chunk; producers without chunking behave exactly as before (byte-identical frames, same exceptions).
4. Consumer/Reader reassemble chunked messages (decrypt per chunk → assemble → decompress), deliver one `IMessage` with the last chunk's metadata/properties/ordering key, and implement duplicate/corrupted/gap/eviction/expiry handling exactly as §6.1 (Java `doProcessMessageChunk`).
5. Individual ack (plain and transactional) of a chunked id acks every chunk entry; cumulative ack acks only the last chunk; redelivery/nack/ack-timeout expand chunk ids; seek uses the first chunk.
6. All §8 unit tests pass; integration tests marked portable/adapted pass against `apachepulsar/pulsar:4.0.9`; `TransactionConsumeTests.Acknowledge_GivenChunkedMessageAndTransaction_ShouldLeaveNoUnackedMessagesAfterCommit` passes.
7. No new allocations on the non-chunked send/receive hot paths (verify by code review: only `bool`/`int` checks added).
8. Builds on netstandard2.0; netstandard2.1; net8.0; net9.0; net10.0 (no API beyond `ReadOnlySequence`, `System.Threading.Timer`, protobuf `Clone`).

## Open decisions (recommendation)
1. **Public representation of chunk ids.** Options: (a) add `MessageId.FirstChunkMessageId` + internal chunk-id array on the same sealed class (C++ model); (b) a derived type (impossible: `MessageId` is sealed; unsealing is a breaking change); (c) a side map in `SubConsumer` like Java's `unAckedChunkedMessageIdSequenceMap`. **Recommend (a)**: keeps one id type, makes ack/redeliver/nack expansion local, avoids map cleanup bugs (Java needed several fixes: `testRepeatedSeekDoesNotRetainFilteredChunkMessageIds`, `testPendingChunkedMessageUuidQueueDoesNotLeak`). Equality ignores `FirstChunkMessageId` (Java-compatible).
2. **Where to chunk.** Recommend in `SubProducer.MessageDispatcher` via `IProducerChannel.PlanChunking` (§5.3) because only the channel knows `MaxMessageSize`, the broker-assigned producer name (needed for the uuid) and schema version; alternative (split in `Producer.InternalSend` before enqueue) can't know those before the first connect.
3. **MaxPendingMessages accounting.** Recommend one slot per chunked message (deviation from Java's per-chunk permit). Per-chunk slots would need the chunk ops to exist before dispatch (see 2).
4. **Concurrent sends and dedup.** With the dispatcher design, all chunks of one `SendOp` are written back-to-back by the single dispatcher loop, so chunks of different messages never interleave on the wire (Java can interleave across threads). No extra lock needed.
5. **Broker max message size in tests.** Recommend per-value `ChunkingFixture` containers (35/50/100/1024) using `WithEnvironment("maxMessageSize", "<N>")`; verify that Testcontainers.Pulsar's startup applies env config (`bin/apply-config-from-env.py conf/standalone.conf`). Fallback: a single container at 50 plus an `[EditorBrowsable(Never)]` test seam is **not** recommended (no InternalsVisibleTo; would leak into public API).
6. **`ChunkMaxMessageSize` type/default.** Recommend `int`, default `0` = "use broker max" (Java uses `-1`; any value ≤ 0 is treated as unset, so Java's `-1` also works).
7. **Malformed chunk overflow.** Recommend drop the context silently (§6.1 step 6) instead of Java's exception (which escapes to Netty); keeps `ConsumerChannel.Receive` alive.
8. **Reader auto-acks.** Recommend no acks from readers (no-op delegate); Java acks on its non-durable reader subscription have no observable effect.
