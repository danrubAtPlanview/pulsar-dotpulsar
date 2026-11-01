# WP-PREREQ-1: Producer batching

Status: draft spec (no implementation). Target repo HEAD `e8f2ba2`.
Upstream pins: Java `apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026` (primary), C++ `apache/pulsar-client-cpp@f71d7e99019d58b1d11858f05ff0dc83b718627b` (cross-reference for performance design).

Depends on: nothing. Consumed by: transaction core (batched transactional sends, `hasSameTxn`), send-timeout (`send-timeout.md`), batch-index-ack (`batch-index-ack.md`, consumer must decode batches this package produces), chunking (mutual exclusion rule).

---

## 1. Goal and scope

Port the Java producer batching path to DotPulsar so that:

1. A producer can be configured to group consecutive messages of the same (sub)producer into one broker entry (`MessageMetadata.num_messages_in_batch = n`, payload = n x `[uint32 size][SingleMessageMetadata][payload]`).
2. Each message's `Send`/`SendChannel` completion still completes **individually**, with a `MessageId` carrying the message's `BatchIndex` (0..n-1) inside the entry.
3. Failures are propagated **per message** (every message of a failed batch fails with the same exception) without faulting the producer for the "per operation" server errors listed in section 6.6.
4. Transactional and non-transactional messages, and messages of different transactions, are never put in the same batch (`hasSameTxn`).
5. The hot path (enqueue -> batch -> send -> receipt) adds **zero per-message heap allocations** compared to today, and at most a constant number of allocations per *batch* (amortised to zero by pooling where specified).

In scope: `DEFAULT` and `KEY_BASED` batchers, `batchingMaxMessages`, `batchingMaxBytes`, `batchingMaxPublishDelay`, flush triggers (full / delay / incompatible message / explicit flush / channel completion), sequence ids incl. `highest_sequence_id` and the "potential duplicate" rule, single-message-batch special case, compression of the whole batch, receipt -> per-message `MessageId`, per-message failure.

Out of scope (owned elsewhere / not ported): end-to-end encryption of batches (encryption WP), chunking (chunking WP; this WP only defines the "never batch a chunk" rule), memory limit controller (`MemoryLimitController`) and `blockIfQueueFull` (DotPulsar always blocks via `MaxPendingMessages`), PIP-486 entry-bucket container (`EntryBucketBatchContainer`, V5 only), `batchingPartitionSwitchFrequencyByPublishDelay` (see Open decisions), `RawBatchMessageContainerImpl` (broker compaction only).

---

## 2. Upstream mapping

| Concern | Java (primary) | C++ (cross-ref) | DotPulsar target |
|---|---|---|---|
| Config defaults | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/conf/ProducerConfigurationData.java#DEFAULT_BATCHING_MAX_MESSAGES (L54)`, `#batchingMaxPublishDelayMicros (L188)`, `#batchingMaxMessages / batchingMaxBytes (L195-L196)`, `#batchingEnabled (L202)`, `#batcherBuilder (L204)`, `#compressMinMsgBodySize (L232)` | `cpp:include/pulsar/ProducerConfiguration.h#BatchingType (L60)`, `#setBatchingMaxMessages (L361)`, `#setBatchingMaxAllowedSizeInBytes (L380)`, `#setBatchingMaxPublishDelayMs (L395)`, `#setBatchingType (L407)` | `ProducerOptions<TMessage>` new properties + `Default*` statics |
| Delay validation (>= 1 ms) | `ProducerConfigurationData.java#setBatchingMaxPublishDelayMicros (L319-L323)` | - | `ProducerBuilder<TMessage>.Create()` + `PulsarClient.CreateProducer` -> `ConfigurationException` |
| Builder API | `java:.../impl/ProducerBuilderImpl.java#enableBatching (L219-L222)`, `#batchingMaxPublishDelay (L274-L277)`, `#batchingMaxMessages (L286-L289)`, `#batchingMaxBytes (L292-L295)`, `#batcherBuilder (L298-L301)`, `#createAsync batching+chunking check (L94-L99)` | - | `IProducerBuilder<TMessage>` / `ProducerBuilder<TMessage>` |
| Space / full rules | `java:.../impl/AbstractBatchMessageContainer.java#haveEnoughSpace (L59-L66)`, `#isBatchFull (L72-L76)`, `#getMaxMessageSize (L67-L70)` | `cpp:lib/BatchMessageContainerBase.h#hasEnoughSpace (L117-L119)`, `#isFull (L121-L123)` | `BatchMessageContainer.HaveEnoughSpace / IsBatchFull` |
| Same-transaction rule | `AbstractBatchMessageContainer.java#hasSameTxn (L133-L149)` | (none - C++ has no txn) | `BatchMessageContainer.HasSameTxn` |
| Same-schema rule | `java:.../impl/BatchMessageContainerImpl.java#hasSameSchema (L491-L504)` | - | `BatchMessageContainer.HasSameSchema` |
| Add / lowest+highest seq | `BatchMessageContainerImpl.java#add (L105-L159)` | `cpp:lib/MessageAndCallbackBatch.cc#add (L36-L44)` | `BatchMessageContainer.Add` |
| Batch metadata init | `java:pulsar-common/.../protocol/Commands.java#initBatchMessageMetadata (L2363-L2393)` | `cpp:lib/Commands.cc#initBatchMessageMetadata` | `BatchMessageContainer` metadata builder |
| SingleMessageMetadata wire format | `Commands.java#serializeSingleMessageInBatchWithPayload (L2395-L2442)` | `cpp:lib/Commands.cc#serializeSingleMessagesToBatchPayload` | `BatchMessageContainer.WriteSingleMessage` |
| Build batch (n>1) / single-message special case | `BatchMessageContainerImpl.java#createOpSendMsg (L361-L457)` (single: L363-L407, batch: L408-L456), `#getCompressedBatchMetadataAndPayload (L178-L236)` | `cpp:lib/MessageAndCallbackBatch.cc#createOpSendMsg (L46-L82)` (NOTE: C++ always sends a batch, even n=1) | `BatchMessageContainer.CreateBatches` |
| Buffer sizing heuristic | `BatchMessageContainerImpl.java#updateMaxBatchSize (L288-L305)`, `AbstractBatchMessageContainer.java#INITIAL_BATCH_BUFFER_SIZE / INITIAL_MESSAGES_NUM (L50-L56)` | - | `BatchMessageContainer.UpdateMaxBatchSize`, `MaxBatchSize`, `MaxMessagesNum` |
| Clear | `BatchMessageContainerImpl.java#clear (L307-L325)` | `cpp:lib/BatchMessageContainer.cc#clear` | `BatchMessageContainer.Clear` |
| Key based container | `java:.../impl/BatchMessageKeyBasedContainer.java#add (L44-L76)`, `#createOpSendMsgs (L110-L145)`, `#getKey (L161-L166)` | `cpp:lib/BatchMessageKeyBasedContainer.cc#getKey (L36)`, `#add (L54)`, `#createOpSendMsgs (L71-L100)` | `KeyBasedBatchMessageContainer` |
| Which messages may batch | `java:.../impl/ProducerImpl.java#canAddToBatch (L1208-L1211)`, `#canAddToCurrentBatch (L1213-L1217)` | `cpp:lib/ProducerImpl.cc#canAddToBatch (L467-L470)` | `SubProducer` dispatcher |
| Add path + potential duplicates | `ProducerImpl.java#serializeAndSendMessage batching branch (L851-L885)`, `#doBatchSendAndAdd (L1227-L1238)`, `#triggerSendIfFullOrScheduleFlush (L1219-L1225)`, fields `lastSequenceIdPushed / isLastSequenceIdPotentialDuplicated (L162-L165)` | `cpp:lib/ProducerImpl.cc#sendAsyncWithStatsUpdate batching branch (L574-L610)` | `SubProducer.BuildNextBatch` |
| Flush timer | `ProducerImpl.java#maybeScheduleBatchFlushTask (L2769-L2775)`, `#scheduleBatchFlushTask (L2777-L2783)`, `#batchFlushTask (L2785-L2805)` | `cpp:lib/ProducerImpl.cc` batch timer armed on first message (L579-L601) | dispatcher deadline (`BatchingMaxPublishDelay` after first message added) |
| Close batch & send | `ProducerImpl.java#batchMessageAndSend (L2807-L2838)`, `#processOpSendMsg (L2840-L2903)` (flushes container before a non-batched op, L2846-L2848) | `cpp:lib/ProducerImpl.cc#batchMessageAndSend (L708-L742)` | dispatcher |
| Explicit flush | `ProducerImpl.java#flushAsync (L2745-L2757)`, `#triggerFlush (L2759-L2766)` | `cpp:lib/ProducerImpl.cc#flushAsync (L390)` | `IProducer.Flush`, `WaitForSendQueueEmpty` |
| Receipt matching | `ProducerImpl.java#ackReceived (L1534-L1617)` (accepts `sequenceId == op.sequenceId` and checks `highestSequenceId`) | `cpp:lib/ProducerImpl.cc#ackReceived (L928-L989)` (accepts any id in `[first, first+count-1]`) | `SubProducer.ProcessReceipt` |
| Per-message ids | `ProducerImpl.java#OpSendMsg.setMessageId (L1993-L2005)` (n==1 -> plain id; n>1 -> batchIndex i) | `cpp:lib/MessageAndCallbackBatch.cc#completeSendCallbacks (L90-L96)` | `SubProducer.ProcessReceipt` |
| Per-message failure fan-out | `ProducerImpl.java#DefaultSendMessageCallback.sendComplete (L468-L479)` + `#addCallback (L512-L515)` (linked callbacks) | `MessageAndCallbackBatch.cc#createSendCallback (L98-L101)` | loop over the batch's `SendOp`s |
| NotAllowed per-op error | `java:.../impl/ClientCnx.java#handleSendError (L1231-L1257)`, `ProducerImpl.java#recoverNotAllowedError (L1704-L1720)` (matches `getHighestSequenceId(op)`) | - | `SubProducer.ResponseProcessor` |
| Broker side: error carries max(seq, highest) | `java:pulsar-broker/src/main/java/org/apache/pulsar/broker/service/Producer.java#MessagePublishContext.completed (L553-L584)`, `callBackSequenceId = Math.max(highestSequenceId, sequenceId)` (L567) | - | `RequestResponseHandler` alias registration (6.5) |
| CommandSend fields | `Commands.java#newSend (L620-L656)` (`highest_sequence_id` only if >= 0; `num_messages` only if > 1) | - | `ProducerChannel.Send` |
| Default max message size | `Commands.java#DEFAULT_MAX_MESSAGE_SIZE (L132)` = 5 MiB | `ClientConnection::getMaxMessageSize` | `IConnection.MaxMessageSize` (already set from `CommandConnected`) |

---

## 3. Public API additions

All new public types use the repo header/style (Apache header, file-scoped namespace, usings inside namespace, XML doc one-line summaries ending in "The default is X.").

### 3.1 `src/DotPulsar/BatcherType.cs` (new)

```csharp
namespace DotPulsar;

/// <summary>
/// The strategy used for grouping messages into batches.
/// </summary>
public enum BatcherType : byte
{
    /// <summary>
    /// Messages are batched in the order they are sent.
    /// </summary>
    Default,

    /// <summary>
    /// Messages are batched by key (the ordering key if set, otherwise the partition key). Required when using batching together with a Key_Shared subscription.
    /// </summary>
    KeyBased
}
```

### 3.2 `src/DotPulsar/ProducerOptions.cs`

Add statics (next to the existing ones, alphabetical by name inside the "default" block is not required; keep existing order and append):

```csharp
/// <summary>
/// The default batcher type.
/// </summary>
public static readonly BatcherType DefaultBatcherType = BatcherType.Default;

/// <summary>
/// The default maximum number of bytes in a batch.
/// </summary>
public static readonly uint DefaultBatchingMaxBytes = 128 * 1024;

/// <summary>
/// The default maximum number of messages in a batch.
/// </summary>
public static readonly uint DefaultBatchingMaxMessages = 1000;

/// <summary>
/// The default maximum delay before a batch is sent.
/// </summary>
public static readonly TimeSpan DefaultBatchingMaxPublishDelay = TimeSpan.FromMilliseconds(1);

/// <summary>
/// The default of whether batching is enabled.
/// </summary>
public static readonly bool DefaultEnableBatching = false;
```

Constructor assigns: `BatcherType = DefaultBatcherType; BatchingMaxBytes = DefaultBatchingMaxBytes; BatchingMaxMessages = DefaultBatchingMaxMessages; BatchingMaxPublishDelay = DefaultBatchingMaxPublishDelay; EnableBatching = DefaultEnableBatching;`

Properties:

```csharp
/// <summary>
/// Set the batcher type. The default is 'Default'.
/// </summary>
public BatcherType BatcherType { get; set; }

/// <summary>
/// Set the maximum number of bytes (uncompressed) in a batch. 0 means the broker's max message size is used. The default is 131072 (128 KB).
/// </summary>
public uint BatchingMaxBytes { get; set; }

/// <summary>
/// Set the maximum number of messages in a batch. 0 means no limit. The default is 1000.
/// </summary>
public uint BatchingMaxMessages { get; set; }

/// <summary>
/// Set the maximum delay between the first message being added to a batch and the batch being sent. Must be at least 1 millisecond. The default is 1 millisecond.
/// </summary>
public TimeSpan BatchingMaxPublishDelay { get; set; }

/// <summary>
/// Whether to batch messages. The default is 'false'.
/// </summary>
public bool EnableBatching { get; set; }
```

Java uses `int` with `<= 0` meaning "no message limit" / "use max message size" (`AbstractBatchMessageContainer.java L63-L65, L73-L75`). DotPulsar uses `uint` (repo convention for counts, cf. `MaxPendingMessages`) with `0` carrying the same meaning.

### 3.3 `src/DotPulsar/Abstractions/IProducerBuilder.cs`

Add (keep alphabetical-ish grouping; place after `AttachTraceInfoToMessages`):

```csharp
/// <summary>
/// Set the batcher type. The default is 'Default'.
/// </summary>
IProducerBuilder<TMessage> BatcherType(BatcherType batcherType);

/// <summary>
/// Set the maximum number of bytes (uncompressed) in a batch. 0 means the broker's max message size is used. The default is 131072 (128 KB).
/// </summary>
IProducerBuilder<TMessage> BatchingMaxBytes(uint batchingMaxBytes);

/// <summary>
/// Set the maximum number of messages in a batch. 0 means no limit. The default is 1000.
/// </summary>
IProducerBuilder<TMessage> BatchingMaxMessages(uint batchingMaxMessages);

/// <summary>
/// Set the maximum delay between the first message being added to a batch and the batch being sent. Must be at least 1 millisecond. The default is 1 millisecond.
/// </summary>
IProducerBuilder<TMessage> BatchingMaxPublishDelay(TimeSpan batchingMaxPublishDelay);

/// <summary>
/// Whether to batch messages. The default is 'false'.
/// </summary>
IProducerBuilder<TMessage> EnableBatching(bool enableBatching);
```

### 3.4 `src/DotPulsar/Internal/ProducerBuilder.cs`

- Fields `_batcherType`, `_batchingMaxBytes`, `_batchingMaxMessages`, `_batchingMaxPublishDelay`, `_enableBatching` initialised from the `ProducerOptions<TMessage>.Default*` statics in the constructor.
- Setters return `this` (no validation in setters, matching existing `ProducerBuilder` style).
- `Create()`:
  - after the topic check add: `if (_batchingMaxPublishDelay < TimeSpan.FromMilliseconds(1)) throw new ConfigurationException("ProducerOptions.BatchingMaxPublishDelay must be at least 1 millisecond");` (Java message: "configured value for batch delay must be at least 1ms", `ProducerConfigurationData.java L321`). Only validated when `_enableBatching` is true? **No** - Java validates on set regardless; validate unconditionally.
  - copy the five values into `ProducerOptions`.
- Chunking WP adds: `if (_enableBatching && _enableChunking) throw new ConfigurationException("Batching and chunking of messages can't be enabled together");` (`ProducerBuilderImpl.java L96-L99`).

### 3.5 `src/DotPulsar/PulsarClient.cs#CreateProducer`

Add the same delay validation after the `MaxPendingMessages` check (options can bypass the builder):
`if (options.BatchingMaxPublishDelay < TimeSpan.FromMilliseconds(1)) throw new ConfigurationException("ProducerOptions.BatchingMaxPublishDelay must be at least 1 millisecond");`

### 3.6 `src/DotPulsar/Abstractions/IProducer.cs` - explicit flush

Needed because ported transaction tests call `producer.flush()` (`TransactionProduceTest.java L135`, `TransactionTest.java L1550`, `TransactionEndToEndTest.java L1718`) and many tests use `batchingMaxPublishDelay(1, HOURS)`.

```csharp
/// <summary>
/// Send all batched messages immediately and wait for the broker to acknowledge the last message sent before this call.
/// </summary>
/// <exception cref="Exception">The exception of the last message sent before this call, if sending it failed.</exception>
ValueTask Flush(CancellationToken cancellationToken = default);
```

Placed on non-generic `IProducer` (implemented only by `Internal/Producer<TMessage>`). For producers without batching it only waits for the last send.

---

## 4. Internal design

### 4.1 Where batching happens (decision and rationale)

Java batches **at enqueue time** (container in front of `pendingMessages`, one `OpSendMsg` per batch). DotPulsar already has, per `SubProducer`, an ordered pending queue of per-message `SendOp`s (`AsyncQueueWithCursor<SendOp> _sendQueue`, `SubProducer.cs L27, L54`) that is the single source of truth for: back-pressure (`MaxPendingMessages`), receipt ordering (`ProcessReceipt`, `SubProducer.cs L174-L191`), and resend after reconnect (`ResetCursor`, `SubProducer.cs L112`).

**DotPulsar batches in the dispatcher** (`SubProducer.MessageDispatcher`): the queue keeps one `SendOp` per message; when the dispatcher takes the next ungrouped op it greedily groups the following compatible ops into a batch, freezes the grouping on the `SendOp`s, sends one `CommandSend` for the group, and `ProcessReceipt` completes and dequeues the whole group on one receipt.

Why:
- No per-message allocation beyond the existing `SendOp`; the per-message `TaskCompletionSource<MessageId>` already exists and becomes the per-message callback (no linked callback chain as in Java `DefaultSendMessageCallback.addCallback`).
- `MaxPendingMessages` keeps its meaning (messages, same as Java `maxPendingMessages`).
- Batches are only built while connected (dispatcher only runs with a channel), which mirrors Java not flushing while disconnected (`ProducerImpl.java L2779` requires `cnx != null`) and naturally produces full batches after a reconnect.
- Resend after reconnect reuses the **frozen** grouping so the same entries (same `sequence_id`/`highest_sequence_id`) are resent, which is what broker deduplication expects (Java resends the same `OpSendMsg`s).
- Timer semantics follow C++ (`ProducerImpl.cc L579-L601`): the delay starts when the first message is added to the batch. Java's flush task additionally rate-limits to one flush per delay since the last send (`ProducerImpl.java L2785-L2805`); with the dispatcher model a batch can never be sent earlier than "first-add + delay" unless full, which is the observable contract the tests rely on.

### 4.2 `SendOp` changes (`src/DotPulsar/Internal/SendOp.cs`)

Becomes (all new members `public`, class stays `public sealed`, `IDisposable` unchanged):

```csharp
public sealed class SendOp : IDisposable
{
    public SendOp(MessageMetadata metadata, ReadOnlySequence<byte> data, TaskCompletionSource<MessageId> receiptTcs, CancellationToken cancellationToken)
    {
        Metadata = metadata;
        Data = data;
        ReceiptTcs = receiptTcs;
        CancellationToken = cancellationToken;
        CreatedTimestamp = Stopwatch.GetTimestamp();
    }

    public MessageMetadata Metadata { get; }
    public ReadOnlySequence<byte> Data { get; }
    public TaskCompletionSource<MessageId> ReceiptTcs { get; }
    public CancellationToken CancellationToken { get; }

    // Stopwatch timestamp taken when the op was created. Used by send timeout (send-timeout.md).
    public long CreatedTimestamp { get; }

    // 0 = not grouped yet. >0 = this op is the first op of a frozen batch of BatchCount ops. -1 = member (not first) of a frozen batch.
    public int BatchCount { get; set; }

    // Only set on the first op of a frozen batch with BatchCount > 1. Owns the pooled batch buffer and batch metadata until the batch completes.
    public PreparedBatch? Batch { get; set; }

    public void Dispose()
    {
        ReceiptTcs.TrySetCanceled();
        Batch?.Return();   // returns buffer + metadata to the pool exactly once (see 4.6)
        Batch = null;
    }
}
```

`ulong SequenceId => Metadata.SequenceId` is read directly from `Metadata`; no copy.

### 4.3 `AsyncQueueWithCursor<T>` additions (`src/DotPulsar/Internal/AsyncQueueWithCursor.cs`)

Single consumer of the cursor is the dispatcher; enqueuers only append. New members (all under `lock (_queue)` and `ThrowIfDisposed()` like existing ones):

```csharp
/// Return the item after the cursor without moving the cursor. Non-blocking.
public bool TryPeekNextItem(out T? item);

/// Move the cursor to the item after it and return that item. Throws AsyncQueueWithCursorNoItemException if none.
public T MoveNext();

/// Wait until an item exists after the cursor, the timeout elapses, or cancellation. Returns true if an item is available.
/// Must not change the cursor and must not interfere with NextItem: uses its own TaskCompletionSource field _cursorWaitTcs that Enqueue also completes.
public ValueTask<bool> WaitForNextItem(TimeSpan timeout, CancellationToken cancellationToken);

/// Overwrite the values of the 'items.Count' nodes ending at (and including) the cursor node, in order, with 'items'.
/// Used by the key-based batcher to reorder not-yet-sent items. Throws if fewer nodes exist.
public void RewriteItemsUpToCursor(IReadOnlyList<T> items);

/// Remove all items, reset the cursor to "before first" WITHOUT cancelling a pending NextItem, notify empty-awaiters, release the pending lock grant.
/// Returns the removed items in queue order (allocation acceptable: rare path, used by send timeout).
public List<T> RemoveAll();

/// Number of items currently in the queue.
public int Count { get; }
```

`Enqueue` change: after `_cursorNextItemTcs?.TrySetResult(node);` add `_cursorWaitTcs?.TrySetResult(true);`.
`WaitForNextItem` implementation: under lock, if `(_currentNode is null ? _queue.First : _currentNode.Next) is not null` return `true` synchronously (no allocation: `new ValueTask<bool>(true)`); else create `_cursorWaitTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)` and outside the lock `await Task.WhenAny(_cursorWaitTcs.Task, Task.Delay(timeout, cancellationToken))`; afterwards clear `_cursorWaitTcs` under lock, `cancellationToken.ThrowIfCancellationRequested()`, and return `_cursorWaitTcs.Task.IsCompleted` (captured local). Allocations happen only when the batch is not full and no further message is queued (at most once per batch).

Note `NextItem` (`AsyncQueueWithCursor.cs L145-L203`) already handles a cursor whose node was removed (`_currentNode.List is null`), which `RemoveAll` relies on.

### 4.4 `IBatchMessageContainer` + implementations

New file `src/DotPulsar/Internal/Abstractions/IBatchMessageContainer.cs`:

```csharp
namespace DotPulsar.Internal.Abstractions;

using DotPulsar.Internal;

public interface IBatchMessageContainer
{
    int NumMessagesInBatch { get; }
    long CurrentBatchSize { get; }
    bool IsEmpty { get; }
    bool HaveEnoughSpace(SendOp sendOp, int maxMessageSize);
    bool HasSameTxn(SendOp sendOp);
    bool HasSameSchema(SendOp sendOp);
    bool Add(SendOp sendOp, int maxMessageSize);              // returns IsBatchFull
    IReadOnlyList<SendOp> Ops { get; }                         // ops in emitted (send) order, valid until Clear()
    void CreateBatches(List<PreparedBatchGroup> destination); // appends groups in send order; does NOT clear (the dispatcher calls Clear() after freezing)
    void Clear();
}
```

`PreparedBatchGroup` (readonly struct, `src/DotPulsar/Internal/PreparedBatchGroup.cs`): `int Start` (index into the container's op list), `int Count`, `PreparedBatch? Batch` (null when `Count == 1`). The container also exposes `IReadOnlyList<SendOp> Ops` (its internal `List<SendOp>`, in emitted order after `CreateBatches`) so the dispatcher can freeze/rewrite without copying.

#### 4.4.1 `BatchMessageContainer` (`src/DotPulsar/Internal/BatchMessageContainer.cs`) - port of `BatchMessageContainerImpl` + `AbstractBatchMessageContainer`

Fields:

```csharp
private const int InitialBatchBufferSize = 1024;   // java AbstractBatchMessageContainer L50
private const int InitialMessagesNum = 32;         // java L51
private const int ShrinkCoolingOffPeriod = 10;     // java BatchMessageContainerImpl L84
private readonly uint _maxNumMessagesInBatch;      // ProducerOptions.BatchingMaxMessages (0 = unlimited)
private readonly uint _maxBytesInBatch;            // ProducerOptions.BatchingMaxBytes (0 = use max message size)
private readonly ObjectPool<PreparedBatch> _batchPool;
private readonly List<SendOp> _ops;                 // capacity InitialMessagesNum, reused (Clear()), never reallocated below MaxMessagesNum
private readonly SingleMessageMetadata _singleMessageMetadata; // reused per message (java LOCAL_SINGLE_MESSAGE_METADATA)
private long _currentBatchSizeBytes;
private ulong _lowestSequenceId;
private ulong _highestSequenceId;
private bool _hasTxn; private ulong _txnMostBits; private ulong _txnLeastBits;
private int _consecutiveShrinkTime;
public int MaxBatchSize { get; private set; } = InitialBatchBufferSize;  // java getMaxBatchSize
public int MaxMessagesNum { get; private set; } = InitialMessagesNum;   // java getMaxMessagesNum
```

Rules (exact ports; `msgSize = (long) sendOp.Data.Length`, i.e. the uncompressed payload, java `msg.getDataBuffer().readableBytes()`):

- `HaveEnoughSpace(op, maxMessageSize)` (java L59-L66):
  `((_maxBytesInBatch == 0 && msgSize + _currentBatchSizeBytes <= maxMessageSize) || (_maxBytesInBatch > 0 && msgSize + _currentBatchSizeBytes <= _maxBytesInBatch)) && (_maxNumMessagesInBatch == 0 || NumMessagesInBatch < _maxNumMessagesInBatch)`
- `IsBatchFull(maxMessageSize)` (java L72-L76):
  `(_maxBytesInBatch > 0 && _currentBatchSizeBytes >= _maxBytesInBatch) || (_maxBytesInBatch == 0 && _currentBatchSizeBytes >= maxMessageSize) || (_maxNumMessagesInBatch > 0 && NumMessagesInBatch >= _maxNumMessagesInBatch)`
- `HasSameTxn(op)` (java L133-L149): if empty -> `true`; `msgHasTxn = op.Metadata.HasTxnidMostBits && op.Metadata.HasTxnidLeastBits`; if `msgHasTxn != _hasTxn` -> `false`; if `!msgHasTxn` -> `true`; else `_txnMostBits == op.Metadata.TxnidMostBits && _txnLeastBits == op.Metadata.TxnidLeastBits`. Pure query, no side effects.
- `HasSameSchema(op)` (java L491-L504, without `schema_id` which DotPulsar never sets): if empty -> `true`; first = `_ops[0].Metadata`; if `!first.HasSchemaVersion` -> `!op.Metadata.HasSchemaVersion`; else `op.Metadata.HasSchemaVersion && first.SchemaVersion.Equals(op.Metadata.SchemaVersion)` (`ByteString.Equals`, no allocation). Rationale: `ProducerChannel.Send` only injects the producer schema version when the per-message `SchemaVersion` is null (`ProducerChannel.cs L74-L78`); a batch must not mix explicit and implicit versions.
- `Add(op, maxMessageSize)` (java L105-L159):
  - if `_ops.Count == 0`: `_lowestSequenceId = op.Metadata.SequenceId`; `_hasTxn/_txnMostBits/_txnLeastBits` from op (adopt the first message's transaction, java L123-L128).
  - `_ops.Add(op)`; `_currentBatchSizeBytes += msgSize`; `_highestSequenceId = op.Metadata.SequenceId` (java L153, the **last added** not the max; with auto-assigned ids it is the max).
  - return `IsBatchFull(maxMessageSize)`.
- `CreateBatches(destination)`:
  - `n == 0`: nothing.
  - `n == 1` (java L363-L407): **not** a batch. Append `new PreparedBatchGroup(0, 1, null)`. The op is later sent with its own `Metadata` and `Data` exactly as a non-batched message (no `num_messages_in_batch`, no `SingleMessageMetadata` framing, `CommandSend.num_messages` = 1). (C++ differs and always frames a batch; Java is authoritative.)
  - `n > 1` (java L408-L456): rent `PreparedBatch b = _batchPool.Get()`; write payload into `b.Buffer` (see 4.5); fill `b.Metadata` (see 4.6); append `new PreparedBatchGroup(0, n, b)`.
  - In both cases then `UpdateMaxBatchSize(uncompressedSize)` (only n>1 writes a buffer; for n==1 call with `msgSize`, as java builds the payload for n==1 too, L191-L192), `MaxMessagesNum = Math.Max(MaxMessagesNum, n)` (java L234); `CreateBatches` does not clear - the dispatcher reads `Ops`, freezes the grouping, then calls `Clear()`.
- `UpdateMaxBatchSize(int uncompressedSize)` (java L288-L305), exact:
  ```
  if (uncompressedSize > MaxBatchSize) { MaxBatchSize = uncompressedSize; _consecutiveShrinkTime = 0; }
  else {
      var shrank = MaxBatchSize - (MaxBatchSize >> 2);
      if (uncompressedSize <= shrank) {
          if (_consecutiveShrinkTime <= ShrinkCoolingOffPeriod) _consecutiveShrinkTime++;
          else { MaxBatchSize = shrank; _consecutiveShrinkTime = 0; }
      } else _consecutiveShrinkTime = 0;
  }
  ```
  `MaxBatchSize` is used as the initial capacity hint when renting the batch buffer: `ArrayPool<byte>.Shared.Rent(Math.Min(MaxBatchSize, maxMessageSize))` (java L119-L120).
- `Clear()` (java L307-L325): `_ops.Clear()` (keeps capacity - unlike java which reallocates `new ArrayList<>(maxMessagesNum)`; reuse is the zero-alloc equivalent), `_currentBatchSizeBytes = 0`, `_lowestSequenceId = _highestSequenceId = 0`, `_hasTxn = false`, `_txnMostBits = _txnLeastBits = 0`.

#### 4.4.2 `KeyBasedBatchMessageContainer` (`src/DotPulsar/Internal/KeyBasedBatchMessageContainer.cs`) - port of `BatchMessageKeyBasedContainer`

- Holds `Dictionary<string, BatchMessageContainer> _batches` (keyed via `GetKey`) plus a pooled free-list `Stack<BatchMessageContainer>` so inner containers are reused across flushes (java uses `computeIfAbsent(new ...)`; reuse removes per-flush allocations). Also keeps the aggregate `NumMessagesInBatch`, `CurrentBatchSize` (java L71-L72) and the container-level txn identity (java L50-L62: captured on the first add of the whole container, assigned unconditionally so a plain first message resets it).
- `GetKey(op)` (java L161-L166): if `op.Metadata.HasOrderingKey` -> `Convert.ToBase64String(op.Metadata.OrderingKey.Span)` (allocation per message: acceptable only for the key-based batcher; document. Optimisation allowed: use `ByteString` as the dictionary key with a custom `IEqualityComparer<ByteString>` to avoid base64 strings - implementer's choice, behaviour identical); else `op.Metadata.PartitionKey` when `HasPartitionKey`; else the sentinel key `""`. Java maps a null key to `HashMap` null key; C# `Dictionary` cannot hold null, and `""` cannot collide with a real base64 of a non-empty ordering key. **Decision:** messages without key share one group (same as Java's null key group).
- `HaveEnoughSpace` / `IsBatchFull` / `HasSameTxn`: evaluated on the aggregate (java inherits `AbstractBatchMessageContainer`): same formulas as 4.4.1 using the aggregate counters; `HasSameTxn` uses the container-level identity.
- `HasSameSchema(op)` (java L155-L159): inner container for the op's key is absent or `inner.HasSameSchema(op)`.
- `CreateBatches(destination)` (java L110-L145):
  1. For each inner container set its batch sequence id to its **highest** sequence id ("lowest = highest", java L122-L124), so both `MessageMetadata.sequence_id` and `highest_sequence_id` (and `CommandSend.sequence_id`/`highest_sequence_id`) equal the group's highest id. Single-op groups keep the op's own metadata (sequence id = op's own id, which is its highest).
  2. Sort the groups ascending by that value (java comparator `(int)(o1.lowest - o2.lowest)` L125-L127; C++ `std::sort` L80; use `ulong.CompareTo`).
  3. For each group in order call the inner `CreateBatches` and append to `Ops`/`destination` with `Start` offsets into the concatenated `Ops` list (the container keeps one `List<SendOp>` `_orderedOps` it refills in emitted order).
- `Clear()` (java L79-L87): clear aggregate counters, return inner containers to the free-list, `_batches.Clear()`, reset txn identity.

### 4.5 Batch payload writer and wire format

`PreparedBatch` (`src/DotPulsar/Internal/PreparedBatch.cs`, `public sealed class`, pooled with `DefaultObjectPool<PreparedBatch>` + a custom `PooledObjectPolicy` whose `Return` resets it):

```csharp
public sealed class PreparedBatch
{
    public PooledBufferWriter Buffer { get; }              // ArrayPool<byte>-backed IBufferWriter<byte>
    public PulsarApi.MessageMetadata Metadata { get; }     // reused, reset by Reset()
    public int Count { get; set; }
    public ulong SequenceId { get; set; }                  // batch sequence_id (lowest, or highest for key-based)
    public ulong HighestSequenceId { get; set; }
    public ReadOnlySequence<byte> Payload => new(Buffer.WrittenMemory);
    public void Reset();                                   // Buffer.Reset() (returns array to ArrayPool), Metadata field reset
    public void Return();                                  // Reset() then pool.Return(this); idempotent via an int flag
}
```

`PooledBufferWriter` (`src/DotPulsar/Internal/PooledBufferWriter.cs`, `public sealed class : IBufferWriter<byte>`): rents from `ArrayPool<byte>.Shared` with an initial size hint, grows by renting `max(2x, needed)` and copying, `Reset()` returns the array. (Do not use `System.Buffers.ArrayBufferWriter<T>`: it allocates new arrays and is not available on netstandard2.0.)

Per message `i` of a batch (java `Commands.serializeSingleMessageInBatchWithPayload` L2395-L2442), using the container's single reused `SingleMessageMetadata smm`:

1. Reset `smm`: `smm.Properties.Clear(); smm.ClearPartitionKey(); smm.ClearPartitionKeyB64Encoded(); smm.ClearOrderingKey(); smm.ClearEventTime(); smm.ClearSequenceId(); smm.ClearNullValue(); smm.ClearNullPartitionKey(); smm.ClearCompactedOut();` (Google.Protobuf proto2 generates `ClearX()` for optional fields.)
2. Copy from `m = op.Metadata` (only when the source has the field):
   - `HasPartitionKey` -> `smm.PartitionKey = m.PartitionKey; smm.PartitionKeyB64Encoded = m.PartitionKeyB64Encoded;`
   - `HasOrderingKey` -> `smm.OrderingKey = m.OrderingKey;` (shares the immutable `ByteString`, no copy)
   - for `j in 0..m.Properties.Count-1`: `smm.Properties.Add(m.Properties[j]);` (index loop; shares `KeyValue` instances - safe because they are only read during serialisation and `smm` is cleared before the caller can mutate them)
   - `HasEventTime` -> `smm.EventTime = m.EventTime;`
   - `smm.SequenceId = m.SequenceId;` (always: DotPulsar always assigns a sequence id before enqueue, `Producer.cs L244-L246`)
   - `HasNullValue` -> `smm.NullValue = m.NullValue;`; `HasNullPartitionKey` -> `smm.NullPartitionKey = m.NullPartitionKey;`
   - `smm.PayloadSize = (int) op.Data.Length;`
3. Write `uint32` big-endian `smm.CalculateSize()` (`BinaryPrimitives.WriteUInt32BigEndian(buffer.GetSpan(4), size); buffer.Advance(4);`), then `smm.WriteTo(buffer)` (Google.Protobuf `MessageExtensions.WriteTo(IMessage, IBufferWriter<byte>)`, available in 3.36.2 on all TFMs), then copy every segment of `op.Data` into the buffer (`segment.Span.CopyTo(buffer.GetSpan(len)); buffer.Advance(len)`).
4. Release the borrowed `KeyValue` references: `smm.Properties.Clear()` (step 1 of the next message does it anyway; do it after the last message too).

Total uncompressed size = `buffer.WrittenCount`; passed to `UpdateMaxBatchSize`.

### 4.6 Batch `MessageMetadata` (n > 1)

Reset of the reused `PreparedBatch.Metadata` (`PreparedBatch.Reset`): `Properties.Clear(); ReplicateTo.Clear(); ClearPartitionKey(); ClearPartitionKeyB64Encoded(); ClearOrderingKey(); ClearReplicatedFrom(); ClearSchemaVersion(); ClearNumMessagesInBatch(); ClearHighestSequenceId(); ClearTxnidMostBits(); ClearTxnidLeastBits(); ClearCompression(); ClearUncompressedSize(); ClearEventTime(); ClearDeliverAtTime(); ClearPublishTime(); ClearProducerName(); ClearSequenceId();` (Required fields `producer_name`, `sequence_id`, `publish_time` are always set again before serialisation.)

Fill from the first op `f = ops[start].Metadata` (java `Commands.initBatchMessageMetadata` L2363-L2393) and the batch (java `createOpSendMsg` L422-L430):
- `SequenceId = batchSequenceId` (lowest; for key-based the group's highest)
- `HasPartitionKey` -> `PartitionKey`, `PartitionKeyB64Encoded`
- `HasOrderingKey` -> `OrderingKey`
- `HasReplicatedFrom` -> `ReplicatedFrom`; `ReplicateTo.Add(f.ReplicateTo)` (index loop)
- `HasSchemaVersion` -> `SchemaVersion` (otherwise left unset: `ProducerChannel.Send` injects the producer schema version, `ProducerChannel.cs L74-L78`)
- `NumMessagesInBatch = n`
- `HighestSequenceId = highest` (java L424; for key-based equals `SequenceId`)
- if container txn identity set: `TxnidMostBits`, `TxnidLeastBits` (java L425-L430)
- `PublishTime` and `ProducerName` are set by `ProducerChannel.Send` (`ProducerChannel.cs L71-L72`) on whatever metadata it is handed - unchanged.
- Compression is applied to the whole batch payload by `ProducerChannel.Send` (`ProducerChannel.cs L85-L94`, sets `Compression` + `UncompressedSize`) - unchanged and identical to java compressing the whole batch (L208-L229). DotPulsar has no `compressMinMsgBodySize` (Java default 4 KB, `ProducerConfigurationData.java L232`): keep DotPulsar's "always compress" behaviour (documented divergence).
- `DeliverAtTime` is never set on a batch (such messages are never batched).

### 4.7 Dispatcher algorithm (`src/DotPulsar/Internal/SubProducer.cs`)

New constructor parameters (append; `Producer.CreateSubProducer`, `Producer.cs L156-L172`, passes them from `ProducerOptions`): `bool enableBatching, BatcherType batcherType, uint batchingMaxMessages, uint batchingMaxBytes, TimeSpan batchingMaxPublishDelay, ulong initialSequenceId`. When `enableBatching` the sub-producer creates its container (`BatcherType.Default` -> `BatchMessageContainer`, `KeyBased` -> `KeyBasedBatchMessageContainer`) and the reusable `List<PreparedBatchGroup> _groups`. New fields: `long _lastSequenceIdPushed = unchecked((long) initialSequenceId - 1)` (java L253-L259: `initialSequenceId` when configured, else -1; DotPulsar's first auto id equals `InitialSequenceId`, so "last pushed" starts one below), `bool _isLastSequenceIdPotentialDuplicated`, `int _flushRequested` (Interlocked flag, see 4.8).

`IProducerChannel` gains `int MaxMessageSize { get; }` (`ProducerChannel` returns `_connection.MaxMessageSize`; `NotReadyChannel` returns `Constants.DefaultMaxMessageSize` = 5 MiB, java `Commands.DEFAULT_MAX_MESSAGE_SIZE` L132 - add the constant to `Internal/Constants.cs`).

`MessageDispatcher` loop body (replaces `SubProducer.cs L112-L147`; non-batching path unchanged when `!enableBatching`):

```
var sendOp = await _sendQueue.NextItem(ct);
if (sendOp.BatchCount == 0 && sendOp.CancellationToken.IsCancellationRequested) { _sendQueue.RemoveCurrentItem(); continue; }   // existing rule, only for ungrouped ops

if (!_enableBatching) { send single as today; continue; }

if (sendOp.BatchCount == 0) await BuildBatches(sendOp, channel.MaxMessageSize, ct);   // freezes the grouping, may emit >1 group (key-based)
// now sendOp is the first op of a frozen group; resend path (after reconnect) arrives here directly
await SendFrozenGroup(sendOp, channel, ct);   // for BatchCount > 1 also MoveNext()s over the BatchCount-1 member ops
```

`CanAddToBatch(op)` (java L1208-L1211): `!op.Metadata.HasDeliverAtTime && !IsChunk(op)` (chunking WP defines `IsChunk`; until then `false`).

`BuildBatches(first, maxMessageSize, ct)`:
1. If `!CanAddToBatch(first)`: freeze `first.BatchCount = 1`; update `_lastSequenceIdPushed = Math.Max(_lastSequenceIdPushed, (long) first.Metadata.SequenceId)` (java `updateLastSeqPushed` in `processOpSendMsg` L2862); return. (The previous batch was already closed because a batch is always fully built before the next op is taken - equivalent of java L2846-L2848 "flush container before a non-batched op".)
2. Potential-duplicate rule for `first` (java L851-L885): if `(long) first.Metadata.SequenceId <= _lastSequenceIdPushed` -> `_isLastSequenceIdPotentialDuplicated = true`, add `first` alone and close immediately (go to step 5). Else if `_isLastSequenceIdPotentialDuplicated` -> it was set by the previous (already closed) batch: clear it and continue normally (java closes the previous batch first, which in the dispatcher model is already closed).
3. `container.Add(first, maxMessageSize)`; if full -> step 5. `deadline = Stopwatch.GetTimestamp() + ToTicks(BatchingMaxPublishDelay)`.
4. Loop:
   - if `!_sendQueue.TryPeekNextItem(out var next)`:
     - if `Interlocked.Exchange(ref _flushRequested, 0) == 1` -> break;
     - `remaining = deadline - now`; if `remaining <= 0` -> break;
     - `if (!await _sendQueue.WaitForNextItem(remaining, ct)) break;` and `continue;` (a `Flush` call also completes the wait, see 4.8).
   - `if (next.BatchCount != 0) break;` (defensive; cannot happen for ops after an ungrouped op)
   - `if (next.CancellationToken.IsCancellationRequested) { _sendQueue.MoveNext(); _sendQueue.RemoveCurrentItem(); continue; }`
   - `if (!CanAddToBatch(next)) break;` (it will be taken by the next `NextItem` and sent alone)
   - `if ((long) next.Metadata.SequenceId <= _lastSequenceIdPushed || ... <= container highest so far)` -> potential duplicate: break (it starts its own batch, java L853-L864).
   - `if (!container.HaveEnoughSpace(next, maxMessageSize) || !container.HasSameSchema(next) || !container.HasSameTxn(next)) break;` (java `canAddToCurrentBatch` L1213-L1217; java "doBatchSendAndAdd" closes the batch and puts `next` in a new one - here `next` simply stays queued and becomes the first op of the next batch).
   - `_sendQueue.MoveNext(); if (container.Add(next, maxMessageSize)) break;`
5. Close: `_groups.Clear(); container.CreateBatches(_groups);`
   - Key-based only: `_sendQueue.RewriteItemsUpToCursor(container.Ops)` (window = all ops the container took, which are exactly the nodes from the original `first` to the cursor, since the dispatcher took them consecutively; cancelled ones were removed).
   - For each group `g`: `ops[g.Start].BatchCount = g.Count; ops[g.Start].Batch = g.Batch;` and `ops[g.Start + k].BatchCount = -1` for `k = 1..g.Count-1`; `_lastSequenceIdPushed = Math.Max(_lastSequenceIdPushed, (long) highest(g))`.
   - `container.Clear();`
   - Reposition the cursor to the first op of the first group: the dispatcher keeps the reference `firstOfFirstGroup = ops[_groups[0].Start]` and resets the cursor to the node **before** it (new queue method not needed: `BuildBatches` returns the first group head, and `SendFrozenGroup` for key-based emits all groups in sequence by walking forward from the window start - implementation detail: record `windowStartIndex` = number of nodes before the window and use `ResetCursor`-free walk. **Simplest correct implementation:** for key-based, after rewriting, call a new queue method `MoveCursorBack(int count)` with `count = container.Ops.Count`, then let the main loop call `NextItem` which returns the (now frozen) first group head; add `public void MoveCursorBack(int count)` to `AsyncQueueWithCursor<T>` (walks `_currentNode = _currentNode.Previous` `count` times; `null` means "before first"). The same is used for the default batcher: after freezing, `MoveCursorBack(ops.Count)` and let the loop's `NextItem` + `SendFrozenGroup` path send it. This makes first-send and resend take the identical code path.)

`SendFrozenGroup(head, channel, ct)`:
- `head.BatchCount == 1`: `channel.Send(head.Metadata, head.Data, tcs, ct)` (today's call, `SubProducer.cs L138`).
- `head.BatchCount > 1`: `for k in 1..head.BatchCount-1: _sendQueue.MoveNext();` (the members are guaranteed to be next because grouping is frozen and only receipts/timeouts remove items, both from the head side or wholesale) then `channel.Send(head.Batch.Metadata, head.Batch.Payload, tcs, ct)`.
- The `TaskCompletionSource<BaseCommand>` + `ContinueWith` forwarding to the response queue is unchanged (one per *entry*, i.e. per batch, not per message).
- Wrap the `channel.Send` call inside the executor lambda in `try { ... } catch (TooLargeMessageException e) when (head.BatchCount > 1) { FailFrozenWindow(head, e); }` so an oversized **batch** fails its messages (java L410-L421: `InvalidMessageException "Message size X is bigger than Y bytes"`) instead of faulting the producer. `FailFrozenWindow` removes the `BatchCount` ops ending at the cursor (`RemoveCurrentItem()` `BatchCount` times, it walks backwards), fails each `ReceiptTcs` with the exception, returns `head.Batch` to the pool. The non-batched oversized message behaviour (producer faults, existing) is unchanged.

### 4.8 Flush and channel completion

`SubProducer.Flush()` (new, internal API used by `Producer.Flush`):
1. `Interlocked.Exchange(ref _flushRequested, 1)`; `_sendQueue.SignalWaiters()` (new tiny queue method that completes `_cursorWaitTcs` with `false`, waking `WaitForNextItem`).
2. Capture the last `SendOp` in the queue (`_sendQueue.TryPeekLast(out var last)` - new method returning `_queue.Last?.Value`); if none, return completed.
3. Return `last.ReceiptTcs.Task` (awaited by the caller; if it faulted, the caller rethrows its exception - java `flushAsync` returns `lastSendFuture`, `ProducerImpl.java L2745-L2757`).

`Producer<TMessage>.Flush(ct)`: `ThrowIfDisposed()`, then `await Task.WhenAll(_producers.Values.Select(p => p.Flush()))` with `.WaitAsync(ct)` semantics (netstandard: `Task.WhenAny(task, Task.Delay(Infinite, ct))`); rethrow the first inner exception unwrapped.

`Producer.WaitForSendQueueEmpty` (`Producer.cs L317-L318`, used by `ISendChannel.Completion`) and `SubProducer.WaitForSendQueueEmpty` (`SubProducer.cs L101-L105`): set `_flushRequested` + `SignalWaiters()` first so `Completion()` does not wait for `BatchingMaxPublishDelay`.

`DisposeAsync`: unchanged semantics (queued ops are cancelled via `SendOp.Dispose`, which now also returns `Batch` to the pool).

### 4.9 Receipt processing and per-message `MessageId`s

`SubProducer.ProcessReceipt(CommandSendReceipt r)` replaces `SubProducer.cs L174-L191`:

```
if (!_sendQueue.TryPeek(out var head) || head is null)
    return;   // receipt for an op that already timed out / was failed (java ackReceived L1538-L1544 "Got ack for timed out msg")
var expected = head.BatchCount > 1 ? head.Batch!.SequenceId : head.Metadata.SequenceId;
if (r.SequenceId < expected) return;   // timed-out op (java L1555-L1563)
if (r.SequenceId > expected) throw new ProducerSendReceiptOrderingException(...);   // existing reconnect behaviour (java closes the connection, L1546-L1554)
// java also cross-checks highest_sequence_id (L1565-L1580); DotPulsar: if (head.BatchCount > 1 && r.HighestSequenceId != 0 && r.HighestSequenceId != head.Batch.HighestSequenceId) throw new ProducerSendReceiptOrderingException(...)
var count = Math.Max(head.BatchCount, 1);
var id = r.MessageId;
for (var i = 0; i < count; ++i)
{
    _sendQueue.TryPeek(out var op);
    _sendQueue.Dequeue();
    var batchIndex = count > 1 ? i : id.BatchIndex;   // n==1 -> plain id (java setMessageId L1996-L1998)
    op!.ReceiptTcs.TrySetResult(new MessageId(id.LedgerId, id.EntryId, _partition, batchIndex, Topic));
}
head.Batch?.Return(); head.Batch = null;
```

NOTE (behaviour change, flagged): today an unexpected receipt sequence id throws (`SubProducer.cs L181-L184`). The `r.SequenceId < expected` -> ignore rule is required by send timeout (`send-timeout.md`) and matches Java/C++; without timeouts it can only occur after `RemoveAll`, so it is safe to add here.

`MessageId` for batched messages carries `BatchIndex = i` and **no batch size** (java producer side creates `BatchMessageIdImpl(ledgerId, entryId, partitionIndex, batchIndex)` with `batchSize = 0`, `ProducerImpl.java L2002`). The consumer side batch size is handled in `batch-index-ack.md`.

### 4.10 Per-message failure (SendError) and `RequestResponseHandler` alias

6.5/6.6 below.

### 4.11 `ProducerChannel.Send` (`src/DotPulsar/Internal/ProducerChannel.cs L63-L111`)

The pooled `SendPackage.Command` is created once with `NumMessages = 1` (`L80`) and never reset. Change to set per send:

```
sendPackage.Command ??= new CommandSend { ProducerId = _id };
sendPackage.Command.SequenceId = metadata.SequenceId;
if (metadata.HasNumMessagesInBatch && metadata.NumMessagesInBatch > 1) sendPackage.Command.NumMessages = metadata.NumMessagesInBatch; else sendPackage.Command.ClearNumMessages();   // java newSend L631-L633
if (metadata.HasHighestSequenceId) sendPackage.Command.HighestSequenceId = metadata.HighestSequenceId; else sendPackage.Command.ClearHighestSequenceId();   // java L628-L630
// txn bits (transaction core WP owns; specified here because the same pooled command is touched): if (metadata.HasTxnidMostBits) { Command.TxnidMostBits = ...; Command.TxnidLeastBits = ...; } else { ClearTxnidMostBits(); ClearTxnidLeastBits(); }
```

`CommandSend.num_messages` default is 1 in the proto (`PulsarApi.proto L535`), so clearing is equivalent to 1. Note `ClearNumMessages()` exists because the field is proto2 optional.

### 4.12 `RequestResponseHandler` (`src/DotPulsar/Internal/RequestResponseHandler.cs L94-L98`)

Broker reports async publish failures with `sequence_id = max(highest_sequence_id, sequence_id)` (`Producer.java L567`) but receipts with `sequence_id` = lowest. The awaiter is keyed by `SendRequest(producerId, sequenceId)`. Change `Outgoing(CommandSend command, TaskCompletionSource<BaseCommand> tcs)`:

```
var request = new SendRequest(command.ProducerId, command.SequenceId);
_requests.AddTaskCompletionSource(request, tcs);
if (command.HasHighestSequenceId && command.HighestSequenceId != command.SequenceId)
{
    var alias = new SendRequest(command.ProducerId, command.HighestSequenceId);
    _requests.AddTaskCompletionSource(alias, tcs);
    _ = tcs.Task.ContinueWith(static (_, state) => { var (awaiter, key) = ((Awaiter<IRequest, BaseCommand>, IRequest)) state!; awaiter.Remove(key); }, (_requests, (IRequest) alias), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
}
```

and add `public void Remove(T item) => _items.TryRemove(item, out _);` to `Awaiter<T,TResult>` (`src/DotPulsar/Internal/Awaiter.cs`). One continuation allocation per *batch* only. The same alias removal is needed in the reverse direction (receipt arrives on the primary key -> alias removed by the continuation; error arrives on the alias key -> primary removed by a symmetric continuation registered for the primary key only when an alias exists).

### 4.13 Per-message failure semantics (`SubProducer.ResponseProcessor`, `SubProducer.cs L151-L172`)

Today any `SendError` throws inside `TryExecuteOnce` -> executor -> (for `DotPulsarException`s) `Rethrow` -> `ExecutorFaulted` -> whole producer `Faulted`.

New rule (Java `ClientCnx.handleSendError L1231-L1257` + `ProducerImpl.recoverNotAllowedError L1704-L1720`): before calling `Expect(SendReceipt)`:

```
if (result.Type == BaseCommand.Types.Type.SendError && IsPerOperationError(result.SendError.Error))
{
    FailHead(result.SendError);   // dequeue the head op group (BatchCount ops, or 1) and TrySetException(mapped exception) on each ReceiptTcs; return the PreparedBatch
    continue;                     // processor keeps running, producer stays Connected
}
```

- Head matching: `result.SendError.SequenceId` must equal the head's batch sequence id **or** its highest sequence id (java matches `getHighestSequenceId(op)` = `max(op.highestSequenceId, op.sequenceId)`); if it matches neither, ignore the error (java L1706 guard).
- Exception mapping: reuse `CommandExtensions.Throw(ServerError, string)` mapping via a new non-throwing helper `public static Exception ToException(this CommandSendError command)` in `Internal/Extensions/CommandExtensions.cs` (refactor the private `Throw` switch into `ToException(ServerError, string)` and make `Throw` call it).
- `IsPerOperationError`: `ServerError.NotAllowedError` (Java parity). See Open decisions for the transaction-related error codes.
- All messages of the failed entry fail with the same exception instance (java fans the same exception out through the callback chain, `ProducerImpl.java L468-L479`).
- Other server errors keep today's behaviour (documented, unchanged).

### 4.14 Interaction with `Producer<TMessage>.InternalSend`

No change to the per-message API flow: `InternalSend` still creates one `SendOp` per message and awaits `SubProducer.Send` (queue enqueue). The per-message `tcs.Task.ContinueWith(...)` for metrics/tracing (`Producer.cs L268-L304`) is existing behaviour and is not touched by this WP (see perf note 7.4).

Transactions (transaction core WP): `IMessageBuilder<T>.Transaction(txn)` sets `MessageMetadata.TxnidMostBits/LeastBits` on the per-message proto metadata before `InternalSend`. Batching reads them via `HasSameTxn` and copies them to the batch metadata. The transaction's send-op registration observes each message's own `ReceiptTcs` task, so a failed batch marks the transaction's `hasOpsFailed` for every message in it (java `TransactionImpl.registerSendOp L124-L141`, `TypedMessageBuilderImpl.sendAsync L131-L141`).

---

## 5. Exact DotPulsar edit points (checklist)

| File | Member | Change |
|---|---|---|
| `src/DotPulsar/BatcherType.cs` | new | enum (3.1) |
| `src/DotPulsar/ProducerOptions.cs` | statics, ctor, properties | 3.2 |
| `src/DotPulsar/Abstractions/IProducerBuilder.cs` | 5 methods | 3.3 |
| `src/DotPulsar/Internal/ProducerBuilder.cs` | fields, ctor, setters, `Create()` | 3.4 |
| `src/DotPulsar/PulsarClient.cs` | `CreateProducer` | delay validation 3.5 |
| `src/DotPulsar/Abstractions/IProducer.cs` | `Flush` | 3.6 |
| `src/DotPulsar/Internal/Producer.cs` | `CreateSubProducer` (L156-L172), new `Flush`, `WaitForSendQueueEmpty` (L317-L318) | pass options, 4.8 |
| `src/DotPulsar/Internal/SubProducer.cs` | ctor (L43-L66), `WaitForSendQueueEmpty` (L101-L105), `MessageDispatcher` (L107-L149), `ResponseProcessor` (L151-L172), `ProcessReceipt` (L174-L191), new `Flush`, `BuildBatches`, `SendFrozenGroup`, `FailHead`, `FailFrozenWindow` | 4.7-4.13 |
| `src/DotPulsar/Internal/SendOp.cs` | new members | 4.2 |
| `src/DotPulsar/Internal/AsyncQueueWithCursor.cs` | `TryPeekNextItem`, `MoveNext`, `WaitForNextItem`, `RewriteItemsUpToCursor`, `MoveCursorBack`, `SignalWaiters`, `TryPeekLast`, `RemoveAll`, `Count`, `Enqueue` | 4.3 |
| `src/DotPulsar/Internal/Abstractions/IBatchMessageContainer.cs` | new | 4.4 |
| `src/DotPulsar/Internal/BatchMessageContainer.cs` | new | 4.4.1 |
| `src/DotPulsar/Internal/KeyBasedBatchMessageContainer.cs` | new | 4.4.2 |
| `src/DotPulsar/Internal/PreparedBatch.cs`, `PreparedBatchGroup.cs`, `PooledBufferWriter.cs` | new | 4.5 |
| `src/DotPulsar/Internal/Abstractions/IProducerChannel.cs` | `int MaxMessageSize { get; }` | 4.7 |
| `src/DotPulsar/Internal/ProducerChannel.cs` | `MaxMessageSize`, `Send` (L63-L111) | 4.11 |
| `src/DotPulsar/Internal/NotReadyChannel.cs` | `MaxMessageSize` | returns `Constants.DefaultMaxMessageSize` |
| `src/DotPulsar/Internal/Constants.cs` | `DefaultMaxMessageSize = 5 * 1024 * 1024` | |
| `src/DotPulsar/Internal/RequestResponseHandler.cs` | `Outgoing(CommandSend, tcs)` | alias 4.12 |
| `src/DotPulsar/Internal/Awaiter.cs` | `Remove` | 4.12 |
| `src/DotPulsar/Internal/Extensions/CommandExtensions.cs` | `ToException(ServerError, string)`, `ToException(this CommandSendError)` | 4.13 |
| `src/DotPulsar/Internal/MessageFactory.cs` | batched `Create` (orderingKey line) | **bug fix**: uses `metadata.OrderingKey` for single messages; must be `singleMetadata.OrderingKey` (required by ported `testOrderingOfKeyBasedBatchMessageContainer`, which asserts per-message ordering keys) |
| `src/DotPulsar/Internal/BatchHandler.cs` | `Add` (`new MessageId(..., i)`) | **bug fix**: batched consumer `MessageId`s are created without `topic` (Topic = ""), unlike non-batched ones (`ConsumerChannel.cs L135`); pass the channel topic (owned by `batch-index-ack.md`, listed here because producer->consumer id round-trip tests depend on it) |

---

## 6. Behaviour reference (normative)

6.1 A message is batchable iff batching is enabled and it has no `DeliverAtTime` and is not a chunk.
6.2 A batch is closed when: (a) full by messages (`BatchingMaxMessages > 0` and count reached), (b) full by bytes (`>= BatchingMaxBytes`, or `>= MaxMessageSize` when 0), (c) `BatchingMaxPublishDelay` elapsed since its first message was added, (d) the next queued message does not fit (`HaveEnoughSpace`), has a different schema version mode/value, a different transaction identity, is not batchable, or is a potential duplicate, (e) `Flush()` / `SendChannel.Completion()` was requested, (f) (key-based) any of the above evaluated on the aggregate container.
6.3 A message larger than the batch byte limit still forms a batch on its own (it is the first message; java adds unconditionally after closing the previous batch, `doBatchSendAndAdd` L1227-L1238) and is sent as a non-batched message (n == 1).
6.4 n == 1 -> sent as a normal message, `MessageId.BatchIndex = -1` (receipt's batch index); n > 1 -> `MessageId.BatchIndex = i`.
6.5 Sequence ids: batch `sequence_id` = first (lowest) message's id (default) or group highest (key-based); `highest_sequence_id` = last added message's id (default) or group highest (key-based); both put in `MessageMetadata` and `CommandSend`.
6.6 Per-op send errors (`NotAllowedError`) fail all messages of the entry and do not change producer state.
6.7 Messages keep their order within a partition for the default batcher; key-based keeps order per key only.
6.8 Cancelling a message's `CancellationToken` removes it only while it is not yet part of a frozen batch.

---

## 7. Performance notes

7.1 Per message: no new allocations. The `SendOp` and its `TaskCompletionSource<MessageId>` already exist; `SingleMessageMetadata` is reused (java `LOCAL_SINGLE_MESSAGE_METADATA`); properties/ordering keys are shared by reference; the batch buffer is written in place via `IBufferWriter<byte>`.
7.2 Per batch (amortised by pooling): `PreparedBatch` from an `ObjectPool`, array from `ArrayPool<byte>.Shared` sized by `MaxBatchSize` heuristic (java `updateMaxBatchSize`), one `TaskCompletionSource<BaseCommand>` + one `ContinueWith` (existing per-send cost, now per entry), at most one `Task.Delay` + TCS when waiting for more messages, one alias continuation when `highest != lowest`. The serialized frame is still built by `Serializer.Serialize` (`Serializer.cs L36-L53`) which allocates the command/metadata byte arrays per *entry* - unchanged, now amortised over n messages.
7.3 Key-based: one base64 string per message when an ordering key is used (avoidable with a `ByteString` comparer; optional).
7.4 Optional follow-up (not required): `Producer.Send` allocates a closure (`OnMessageSent`) and a `ContinueWith` per message (`Producer.cs L207-L224, L268-L304`). These are pre-existing and orthogonal; do not change them in this WP.
7.5 Lock-free hot path: only the existing `AsyncQueueWithCursor` lock is taken per message (enqueue + peek/move). No new locks.
7.6 Benchmark to add (optional, `benchmarks/` if present): 1 producer, 1 KB payload, 100k messages via `SendChannel`, batching off vs on (default limits): expect allocations/message unchanged, throughput >= 5x on a local broker.

---

## 8. Tests

### 8.1 Unit tests (`[Trait("Category", "Unit")]`, mirror src path)

`tests/DotPulsar.Tests/Internal/BatchMessageContainerTests.cs` - ports of `java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/BatchMessageContainerImplTest.java`:

| Java test | C# test | Notes |
|---|---|---|
| `testUpdateMaxBatchSize` (L75-L130) | `UpdateMaxBatchSize_GivenGrowShrinkAndAlternatingSizes_ShouldFollowCoolingOffRules` | exact values: init 1024; 2048; 2 -> 2048; 15x2 -> 2048 for i<10 else 1536 (`2048*0.75`); then 2048, alternating, 2000s, 4096 -> 4096 |
| `testMessagesSize` (L177-L193) | `MaxMessagesNum_GivenBatchesOf10Then200Then10_ShouldBe32Then200Then200` | 32 initial |
| `testPlainMessageIsNotBatchedWithTransactionalMessages` (L856-L870) | `HasSameTxn_GivenTransactionalBatchAndPlainMessage_ShouldReturnFalse` | message ids: seq 1 txn (7,13); seq 2 plain |
| `testTransactionalMessageIsNotBatchedWithPlainMessages` (L873-L889) | `HasSameTxn_GivenPlainBatchAndTransactionalMessage_ShouldReturnFalse` | |
| `testKeyBasedContainerKeepsTransactionalAndPlainMessagesApart` (L892-L904) | `HasSameTxn_GivenKeyBasedPlainBatchAndTransactionalMessage_ShouldReturnFalse` | `KeyBasedBatchMessageContainer` |
| `testSameTransactionStillBatchesTogether` (L907-L920) | `HasSameTxn_GivenSameTransaction_ShouldReturnTrueAndGivenOtherTransaction_ShouldReturnFalse` | (7,13) vs (7,13) true; vs (7,14) false |
| `testPlainMessagesStillBatchTogether` (L923-L934) | `HasSameTxn_GivenTwoPlainMessages_ShouldReturnTrue` | |
| `testKeyBasedContainerDropsTheTransactionIdentityOfAFailedFirstAdd` (L956-L985) | NOT PORTED | DotPulsar inner add cannot fail (no allocator/memory controller); add instead `HasSameTxn_GivenKeyBasedContainerClearedAfterTransactionalBatch_ShouldTreatNextPlainMessageAsPlain` |
| `oversizedBatchReleasesThePayloadExactlyOnce` (L708-...) / `testOversizedBatchReleasesItsPayloadExactlyOnce` (L1059-) / `testOversizedSingleMessageBatchReleasesItsPayloadExactlyOnce` (L1156-) / `oversizedSingleMessageReleasesTheBatchBufferExactlyOnce` (L659-) | `Dispatcher_GivenBatchLargerThanMaxMessageSize_ShouldFailAllMessagesWithTooLargeMessageExceptionAndReturnBufferOnce` (SubProducer test) | semantic port: ByteBuf refcount -> pooled buffer returned exactly once (count `ArrayPool` returns via a test `PreparedBatch` pool policy) |
| `recoveryAfterOom`, `testRecoveryAfterBatchBuildFailure`, `testRecoveryAfterEncryptionFailure`, `testNoCompressionBuildFailureProducesValidFrame`, `testMultiBatchesPartialBuildFailureReleasesBuiltOps`, `discardReleasesTheBatchBufferWhenTheCallbackThrows` | NOT PORTED | Netty ByteBuf ownership / allocator failure injection have no DotPulsar equivalent (managed arrays); `discard...WhenTheCallbackThrows` -> covered by `FailHead_GivenThrowingContinuation_ShouldStillReturnBatchBuffer` |
| `testEntryBucket*` (L195, L541, L987, L1003, L1026) | NOT PORTED | PIP-486 / V5 out of scope |

Additional DotPulsar unit tests:

`BatchMessageContainerTests` (cont.):
- `HaveEnoughSpace_GivenMaxBytesZero_ShouldUseMaxMessageSize`
- `HaveEnoughSpace_GivenMaxMessagesReached_ShouldReturnFalse`
- `Add_GivenMessagesUpToMaxBytes_ShouldReportFull`
- `HasSameSchema_GivenExplicitAndImplicitSchemaVersion_ShouldReturnFalse`
- `CreateBatches_GivenOneMessage_ShouldReturnSingleGroupWithoutPreparedBatch`
- `CreateBatches_GivenThreeMessages_ShouldWriteSingleMessageMetadataFraming` - parse the produced payload with `BatchHandler<byte[]>.Add` (round trip): payloads, properties, partition key + b64 flag, ordering key, event time, per-message sequence ids, null value flags.
- `CreateBatches_GivenThreeMessages_ShouldSetBatchMetadata` - `NumMessagesInBatch = 3`, `SequenceId = first`, `HighestSequenceId = last`, partition key/ordering key/schema version from the first message, txn bits when transactional, no `DeliverAtTime`.

`tests/DotPulsar.Tests/Internal/KeyBasedBatchMessageContainerTests.cs`:
- `CreateBatches_GivenInterleavedKeys_ShouldGroupByKeyAndSortByHighestSequenceId` - keys A(0,3,4), B(1,2) -> emitted B(seq=2,high=2) then A(seq=4,high=4) (the exact example in java L112-L121 comment).
- `CreateBatches_GivenOrderingKey_ShouldPreferOrderingKeyOverPartitionKey`
- `CreateBatches_GivenMessagesWithoutKey_ShouldGroupThemTogether`

`tests/DotPulsar.Tests/Internal/AsyncQueueWithCursorTests.cs` (extend):
- `TryPeekNextItem_GivenItemAfterCursor_ShouldNotMoveCursor`, `MoveNext_GivenNoItem_ShouldThrowAsyncQueueWithCursorNoItemException`, `WaitForNextItem_GivenEnqueueDuringWait_ShouldReturnTrue`, `WaitForNextItem_GivenTimeout_ShouldReturnFalse`, `WaitForNextItem_GivenSignalWaiters_ShouldReturnFalse`, `RewriteItemsUpToCursor_ShouldReorderValuesInPlace`, `MoveCursorBack_ShouldReturnSameItemsAgain`, `RemoveAll_GivenPendingNextItem_ShouldNotCancelIt`.

`tests/DotPulsar.Tests/Internal/SubProducerTests.cs` (extend; fake `IProducerChannel` capturing `(MessageMetadata, payload)` and returning controllable `TaskCompletionSource<BaseCommand>`s):
- `Dispatcher_GivenBatchingMaxMessages3AndSixMessages_ShouldSendTwoEntries`
- `Dispatcher_GivenSlowProducer_ShouldSendAfterBatchingMaxPublishDelay`
- `Dispatcher_GivenTransactionalThenPlainMessage_ShouldSendTwoEntries`
- `Dispatcher_GivenMessagesOfTwoTransactions_ShouldSendTwoEntries`
- `Dispatcher_GivenDeliverAtTimeMessageBetweenMessages_ShouldSendItAlone`
- `Dispatcher_GivenCancelledMessageBeforeGrouping_ShouldNotSendIt`
- `Dispatcher_GivenCancelledMessageAfterGrouping_ShouldStillSendIt`
- `Dispatcher_GivenReconnect_ShouldResendIdenticalBatches` - same sequence ids and payload bytes
- `Dispatcher_GivenExplicitSequenceIdLowerThanLastPushed_ShouldSendItInItsOwnEntry` (potential duplicate rule)
- `Dispatcher_GivenKeyBased_ShouldReorderQueueAndMatchReceiptsInSendOrder`
- `ProcessReceipt_GivenBatchOfThree_ShouldCompleteEachMessageWithBatchIndex`
- `ProcessReceipt_GivenSingleMessageBatch_ShouldCompleteWithReceiptBatchIndex`
- `ProcessReceipt_GivenReceiptForRemovedOp_ShouldIgnoreIt`
- `ResponseProcessor_GivenNotAllowedSendErrorForBatch_ShouldFailAllMessagesOfBatchAndKeepProducerConnected`
- `ResponseProcessor_GivenNotAllowedSendErrorWithHighestSequenceId_ShouldMatchHead`
- `Flush_GivenLongPublishDelay_ShouldSendImmediatelyAndAwaitLastMessage`
- `WaitForSendQueueEmpty_GivenLongPublishDelay_ShouldNotWaitForDelay`

`tests/DotPulsar.Tests/Internal/RequestResponseHandlerTests.cs` (new):
- `Outgoing_GivenBatchCommandSend_ShouldCompleteOnSendErrorWithHighestSequenceId`
- `Outgoing_GivenBatchCommandSend_ShouldRemoveAliasAfterReceipt`

`tests/DotPulsar.Tests/Internal/ProducerChannelTests.cs` (new):
- `Send_GivenBatchMetadata_ShouldSetNumMessagesAndHighestSequenceIdOnCommand`
- `Send_GivenSingleMessageAfterBatch_ShouldResetNumMessagesAndHighestSequenceId` (pooled `SendPackage` reuse)

`tests/DotPulsar.Tests/Internal/ProducerBuilderTests.cs` (extend) - port of `ProducerBuilderImplTest`:
- `// Upstream: ProducerBuilderImplTest#testProducerBuilderImplWhenBatchingMaxPublishDelayPropertyIsNegative` -> `Create_GivenNegativeBatchingMaxPublishDelay_ShouldThrowConfigurationException` (Java uses -1 ms -> C# `TimeSpan.FromMilliseconds(-1)`; Java throws on set, DotPulsar on `Create()` - documented)
- `Create_GivenBatchingMaxPublishDelayBelowOneMillisecond_ShouldThrowConfigurationException`
- `Create_GivenBatchingOptions_ShouldCopyThemToProducerOptions`

`tests/DotPulsar.Tests/Internal/MessageFactoryTests.cs` (new): `Create_GivenSingleMessageMetadataWithOrderingKey_ShouldUseSingleMessageOrderingKey` (bug fix).

### 8.2 Integration tests

Location (see Open decisions): `tests/DotPulsar.Tests/Prerequisites/BatchMessageTests.cs`, `[Collection("Integration"), Trait("Category", "Integration")]`, `IntegrationFixture`. Ports of `java:pulsar-broker/src/test/java/org/apache/pulsar/broker/service/BatchMessageTest.java` with identical parameters (`codecAndContainerBuilder` L69-L79 -> `[Theory]` `[InlineData(CompressionType.None, BatcherType.Default)] [InlineData(CompressionType.Lz4, BatcherType.Default)] [InlineData(CompressionType.Zlib, BatcherType.Default)] [InlineData(CompressionType.None, BatcherType.KeyBased)] [InlineData(CompressionType.Lz4, BatcherType.KeyBased)] [InlineData(CompressionType.Zlib, BatcherType.KeyBased)]`; `containerBuilder` L81-L87 -> `Default`, `KeyBased`). Backlog via admin REST `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/stats` -> `subscriptions[sub].msgBacklog` (java `getBacklog` L64-L67). Java `sendAsync` -> `ISendChannel.Send` (or un-awaited `Send`) collected into a list and awaited.

| Java test | Port | C# name |
|---|---|---|
| `testSimpleBatchProducerWithFixedBatchSize` (L98-L139) | yes | `Send_GivenFixedBatchSizeOf25And50Messages_ShouldCreateTwoEntries` |
| `testSimpleBatchProducerWithFixedBatchBytes` (L141-) | yes | `Send_GivenFixedBatchBytes_ShouldCreateExpectedEntries` |
| `testSimpleBatchProducerWithFixedBatchTime` (L186-) | yes | `Send_GivenFixedBatchTime_ShouldCreateEntriesByTime` |
| `testSimpleBatchProducerWithFixedBatchSizeAndTime` (L219-) | yes | |
| `testBatchProducerWithLargeMessage` (L252-) | yes | |
| `testSimpleBatchProducerConsumer` (L310-) | yes | |
| `testSimpleBatchSyncProducerWithFixedBatchSize` (L366-) | yes | |
| `testSimpleBatchProducerConsumer1kMessages` (L405-) | yes | |
| `testOutOfOrderAcksForBatchMessage` (L466-) | yes, with batch-index-ack WP | |
| `testNonBatchCumulativeAckAfterBatchPublish`, `testBatchAndNonBatchCumulativeAcks`, `testConcurrentBatchMessageAck` | yes (needs cumulative-ack fix from `batch-index-ack.md`) | |
| `testOrderingOfKeyBasedBatchMessageContainer` (L702-L768) | yes (needs MessageFactory ordering-key fix) | `Send_GivenKeyBasedBatcherAndKeySharedConsumer_ShouldPreserveOrderPerKey` |
| `testBatchSendOneMessage` (L769-L799) | yes: assert `BatchIndex == -1` on send and receive, `SequenceId == 0`, property `key1=value1` | `Send_GivenSingleMessageInBatch_ShouldSendItUnbatched` |
| `testRetrieveSequenceIdGenerated` (L801-L832) | yes (sequence ids 0..9) | |
| `testRetrieveSequenceIdSpecify` (L834-L865) | yes (ids `i + 100`) | |
| `testSendOverSizeMessage` (L867-) | yes | |
| `testBatchMessageDispatchingAccordingToPermits` (L895-) | yes | |

Plus DotPulsar-specific:
- `tests/DotPulsar.Tests/Internal/ProducerTests.cs`: `Send_GivenBatchingEnabledAndTenMessages_ShouldReturnMessageIdsWithBatchIndexes`, `SendChannel_GivenBatchingEnabled_ShouldCompleteAllCallbacks`, `Connectivity_GivenBatchingAndConnectionDrop_ShouldResendWithoutDuplicates` (Toxiproxy `DisableThePulsarConnection`, topic with deduplication enabled via admin, consumer count == sent count).

Transaction ports that need batching (owned by the transaction WP, listed for traceability): `TransactionEndToEndTest` methods with `enableBatching(true)` + `batchingMaxPublishDelay(1, HOURS)` + `batchingMaxMessages(count)` (L128-L132, L186-L192), `TransactionProduceTest` (`batchingMaxMessages`, `flush()` L135).

---

## 9. Acceptance criteria

1. With `EnableBatching = false` (default) every existing test passes unchanged and the wire output is byte-identical to today (pooled `CommandSend` now clears `num_messages`/`highest_sequence_id` - equivalent defaults).
2. With batching enabled, 50 messages, `BatchingMaxMessages = 25`, `BatchingMaxPublishDelay = 5 s` produce exactly 2 entries (`msgBacklog == 2`), messages consumed in order, for all 3 compression types and both batcher types.
3. A single message sent with batching enabled has `BatchIndex == -1` on both producer and consumer side.
4. Every message of an n-message batch completes with `MessageId(ledger, entry, partition, i, topic)`, i = 0..n-1 in send order (default batcher).
5. Plain and transactional messages, and messages of different transactions, never share an entry (verified by unit tests and by `TransactionEndToEndTests` batching variants).
6. A `NotAllowedError` send error fails exactly the messages of the affected entry; the producer stays `Connected` and subsequent sends succeed.
7. After a forced reconnect, resent entries have identical `sequence_id`/`highest_sequence_id`/payload; with broker deduplication enabled, the consumer receives each message exactly once.
8. `Flush()` returns within 1 s with `BatchingMaxPublishDelay = 1 h` and all prior messages persisted.
9. Allocation test (unit, `GC.GetAllocatedBytesForCurrentThread` around 10k batched sends through a fake channel after warm-up): per-message allocations do not exceed the non-batching path's.
10. `BatchingMaxPublishDelay < 1 ms` -> `ConfigurationException` from both `ProducerBuilder.Create()` and `PulsarClient.CreateProducer(options)`.

---

## 10. Open decisions (recommendation)

1. **Default of `EnableBatching`**: Java default `true` (`ProducerConfigurationData.java L202`). **Recommendation: `false`** to preserve DotPulsar's existing wire behaviour and message-id shape (`BatchIndex == -1`) for current users; ported Java tests that rely on the Java default must set `.EnableBatching(true)` explicitly - the test-porting WP must audit every Java producer created *without* `enableBatching(...)` and add `.EnableBatching(true)` to keep "identical intent". (Alternative: default `true` = Java parity, breaking change; requires a major version.)
2. **Batching in the dispatcher vs at enqueue time**: recommended dispatcher model (4.1). Observable difference to Java: batches are not formed while disconnected (Java also does not flush while disconnected), and Java's "at most one timed flush per delay since last send" throttle is replaced by "delay from first message added" (C++ semantics).
3. **Key-based batcher**: recommended in scope (needed by `BatchMessageTest` `containerBuilder` data provider and Key_Shared users). If schedule-constrained, ship `Default` first and make `BatcherType.KeyBased` throw `NotSupportedException` from `Create()` until implemented - but then the `KEY_BASED` `[InlineData]` rows must be marked `Skip` with a reason, which violates "reproduce exactly"; hence recommended in scope.
4. **Per-operation send errors**: Java treats only `NotAllowedError` per op. Recommendation: exactly `NotAllowedError` (parity). Transaction send errors from the broker arrive as `NotAllowedError` (broker maps `BrokerServiceException.NotAllowedException`, `Producer.java L590-L591`) or `PersistenceError` (all other exceptions, L593); `PersistenceError` keeps current DotPulsar behaviour (producer faults) - Java instead reconnects and resends (`handleSendError` default branch). Flag for the transaction WP: if any ported transaction test expects a `PersistenceError` publish failure to fail only the message, extend the set; otherwise leave.
5. **`batchingPartitionSwitchFrequencyByPublishDelay`** (Java round-robin router switches partition every `10 x delay` when batching, `ProducerConfigurationData.java L189, L332`): DotPulsar's `RoundRobinPartitionRouter` switches per message, which yields 1-message batches on partitioned topics. Recommendation: out of this WP; add a follow-up "batching-aware round robin router" only if a ported test asserts batch counts on a partitioned topic.
6. **`compressMinMsgBodySize` (4 KB)**: not ported (DotPulsar always compresses when configured). Recommendation: keep.
7. **Integration test location for prerequisite ports**: recommendation `tests/DotPulsar.Tests/Prerequisites/<JavaClassName>s.cs` with `IntegrationFixture`; tests needing broker settings (`acknowledgmentAtBatchIndexLevelEnabled=true`, deduplication) use `[Collection("Transaction")]` + `TransactionFixture`.
8. **`IProducer.Flush` placement**: recommendation non-generic `IProducer` (adds an interface member - acceptable per the decision already taken for `IPulsarClient.NewTransaction()`).
