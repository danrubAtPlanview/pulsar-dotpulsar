# WP-PREREQ-2: Batch index acknowledgement (ack_set / batch_size)

Status: draft spec (no implementation). Target repo HEAD `e8f2ba2` (includes #299 "Fix bulk acknowledgments for batched messages").
Upstream pins: Java `apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026` (primary), C++ `apache/pulsar-client-cpp@f71d7e99019d58b1d11858f05ff0dc83b718627b` (cross-reference).

Depends on: `ack-receipt.md` (transactional acks are request/response). Consumed by: transaction core (transactional ack of a single message inside a batch must carry `ack_set` + `batch_size`, otherwise the broker acks/locks the whole entry), `TransactionEndToEndTest` / `TransactionEndToEndWithoutBatchIndexAckTest` ports.

---

## 1. Goal and scope

1. **Transactional acks** of batched messages always carry `MessageIdData.ack_set` and `MessageIdData.batch_size` (Java ignores the consumer's batch-index-ack option for transactions: `ConsumerImpl.doTransactionAcknowledgeForResponse`).
2. **Non-transactional acks** keep today's DotPulsar semantics by default (entry acked only when every index of the batch was individually acked), and gain an opt-in `BatchIndexAckEnabled` option that sends partial `ack_set`s like Java's `enableBatchIndexAcknowledgment(true)`.
3. **Receive side**: messages whose batch index is reported as already acknowledged by the broker (`CommandMessage.ack_set`, sent on redelivery when the broker has `acknowledgmentAtBatchIndexLevelEnabled=true`) are not delivered again; compacted-out single messages are skipped; flow-control permits stay consistent.
4. Fix existing defects that these paths touch (section 5.6): cumulative ack of a partial batch is silently dropped; batch trackers leak after cumulative acks; batched `MessageId`s have an empty `Topic`; redelivered batches create duplicate trackers.
5. Zero per-ack heap allocation for ack_set construction.

Out of scope: ack grouping (`acknowledgementGroupTime`, DotPulsar sends acks immediately), negative acks, ack timeout tracker (`UnAckedMessageTracker`, owned by ack-timeout WP), DLQ (`possibleSendToDeadLetterTopicMessages`), chunked message acks (chunking WP), seek-to-batch-index `ack_set` (see Open decisions).

---

## 2. Upstream mapping

| Concern | Java (primary) | C++ (cross-ref) | DotPulsar target |
|---|---|---|---|
| Option + default | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/conf/ConsumerConfigurationData.java#batchIndexAckEnabled (L417)` **= true** at the pinned SHA | `cpp:include/pulsar/ConsumerConfiguration.h#setBatchIndexAckEnabled (L644)`, `#isBatchIndexAckEnabled (L649)` | `ConsumerOptions.BatchIndexAckEnabled` |
| Builder | `java:.../impl/ConsumerBuilderImpl.java#enableBatchIndexAcknowledgment (L580-L583)` | - | `IConsumerBuilder.BatchIndexAckEnabled(bool)` |
| Bit polarity / new ack set | `java:.../impl/BatchMessageIdImpl.java#newAckSet (L123-L127)` (bits set = **not yet acked**), ctor `(ledger, entry, partition, batchIndex, batchSize, ackSet)` (L42-L48) | `cpp:lib/BatchMessageAcker.h#BatchMessageAckerImpl (L58-L90)` (`bitSet_.set(0, batchSize)`) | `BatchAckTracker` |
| Ack bookkeeping | `java:.../impl/MessageIdAdvUtils.java#acknowledge (L50-L69)` (individual: `clear(i)`; cumulative: `clear(0, i+1)`; returns `isEmpty()`), `#isBatch (L71-L73)` (`batchIndex >= 0 && batchSize > 0`), `#discardBatch (L75-L81)`, `#prevMessageId (L83-L85)` | `BatchMessageAcker.h#ackIndividual (L72-L76)`, `#ackCumulative (L78-L84)`, `#shouldAckPreviousMessageId (L41-L44)` | `BatchAckTracker.AckIndividual / AckCumulative` |
| Non-txn individual ack decision | `java:.../impl/PersistentAcknowledgmentsGroupingTracker.java#addIndividualAcknowledgment (L211-L229)` | `cpp:lib/ConsumerImpl.cc#prepareIndividualAck (L1499-L1517)` | `ConsumerChannel.Send(CommandAck)` |
| Non-txn cumulative ack decision | `PersistentAcknowledgmentsGroupingTracker.java#addAcknowledgment (private) (L231-L262)` (disabled: ack `prevMessageId` cumulatively, L254-L258) | `cpp:lib/ConsumerImpl.cc#prepareCumulativeAck (L1520-L1535)` | `ConsumerChannel.Send(CommandAck)` |
| Partial individual ack set (non-txn) | `PersistentAcknowledgmentsGroupingTracker.java#doIndividualBatchAckAsync (L362-L388)`, `#doImmediateBatchIndexAck (L419-L446)` | `cpp:lib/ConsumerImpl.cc#doImmediateAck (L2197-L2216)` (uses acker bitset) | `BatchAckTracker.CopyAckSetTo` |
| Partial cumulative ack set (non-txn) | `PersistentAcknowledgmentsGroupingTracker.java#doCumulativeBatchIndexAck (L395-L406)` (fresh set: `set(0,size)`, `clear(0, i+1)`) | - | `AckSetWriter.WriteCumulative` |
| Wire: newAck / multi ack | `java:pulsar-common/.../protocol/Commands.java#newAck (L1206-L1228)` (`batch_size` only when `batchSize >= 0`, i.e. only the transactional overload sets it), `#newMultiMessageAck (L1169-L1198)` | `cpp:lib/Commands.cc#newAck / newMultiMessageAck` | `ConsumerChannel` |
| Transactional ack (single) | `java:.../impl/ConsumerImpl.java#doTransactionAcknowledgeForResponse(MessageId,...) (L3520-L3586)` | (no txn in C++) | `SubConsumer` txn ack path |
| Transactional ack (list) | `ConsumerImpl.java#doTransactionAcknowledgeForResponse(List<MessageId>,...) (L3631-L3665)` | - | `SubConsumer` txn ack path |
| Txn vs non-txn dispatch | `ConsumerImpl.java#doAcknowledge (L653-L675)`, `#doAcknowledge(List) (L677-L697)` | - | `SubConsumer` |
| Receive: ack_set from broker | `ConsumerImpl.java#messageReceived (L1531-L1537)` (`CommandMessage.ack_set`, **not** `MessageIdData.ack_set`) | `cpp:lib/ConsumerImpl.cc` (L781-L787) | `ChannelManager.Incoming(CommandMessage, ...)`, `MessagePackage` |
| Receive: per-index filtering | `ConsumerImpl.java#receiveIndividualMessagesFromBatch (L2017-L2098)`, `#newSingleMessage (L1387-L1440)` (compacted-out skip L1420-L1423, acked skip L1425-L1427), `java:.../impl/ConsumerBase.java#isSingleMessageAcked (L1466-L1468)` | `cpp:lib/ConsumerImpl.cc#receiveIndividualMessagesFromBatch (L935-)`, filter (L981-L986) | `BatchHandler.Add` |
| Shared ack set per received batch | `ConsumerImpl.java L2031-L2036` (`ackSetInMessageId = newAckSet(batchSize).and(brokerAckSet)`) | acker created per batch | `BatchAckTracker` initialised from broker ack_set |
| Permits for skipped indexes | `ConsumerImpl.java L2047-L2056, L2094-L2097` (only skipped messages that the broker did charge are refunded) | `ConsumerImpl.cc` `skippedMessages` | `ConsumerChannel` `_sendWhenZero` adjustment |
| Duplicate check | `PersistentAcknowledgmentsGroupingTracker.java#isDuplicate (L123-L148)` | - | not ported (no grouping) |

Upstream tests (section 8): `java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/BatchMessageIdImplTest.java`, `MessageIdAdvUtilsTest.java`, `AcknowledgementsGroupingTrackerTest.java`; `java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/BatchMessageIndexAckTest.java`, `BatchMessageIndexAckDisableTest.java`, `ConsumerSeekBatchIndexAckTest.java`; `java:pulsar-broker/src/test/java/org/apache/pulsar/broker/service/BatchMessageWithBatchIndexLevelTest.java`, `BatchMessageTest.java` (ack tests).

---

## 3. Semantics (normative)

### 3.1 Ack-set encoding

- An ack set is a bitmap over batch indexes `0..batchSize-1`. **Bit set = index NOT acknowledged** (Java `newAckSet` sets all bits, acks clear bits). The broker interprets the `ack_set` of an ack command the same way (remaining unacked indexes).
- Wire form: `repeated int64 ack_set` = `java.util.BitSet.toLongArray()` - little-endian 64-bit words, bit `i` in word `i / 64` at position `i % 64`; trailing all-zero words are **omitted** (so an all-zero set is an empty list).
- An ack with an **empty** `ack_set` acknowledges the whole entry. Therefore a partial ack must never serialise to an empty list (it cannot: a partial ack has at least one set bit).
- `batch_size` (`MessageIdData` field 6) is set **only** on transactional acks (Java `Commands.newAck` overload with `batchSize` L1206-L1228; the non-transactional overloads pass `-1` -> field absent).

### 3.2 Batch tracker lifecycle (per received entry)

- Created when a batched entry (`MessageMetadata.HasNumMessagesInBatch`) is received by a consumer (not by readers: readers never ack; keep `trackBatches = false` for `Reader`).
- Initial bits: `set(0, batchSize)`; if `CommandMessage.ack_set` is non-empty: `AND` with it (Java L2031-L2036).
- Individual ack of index `i`: `clear(i)`; cumulative ack of index `i`: `clear(0, i + 1)`. Each returns `IsEmpty`.
- Removed when: it becomes empty (entry fully acked); a cumulative ack at or beyond its entry is sent; `Seek` (existing `_batchHandler.Clear()`); a redelivery of the same entry replaces it (merge by AND, 5.3).

### 3.3 Decision table - non-transactional acks (`ConsumerChannel.Send(CommandAck)`)

For each `MessageIdData m` in the command (`m.BatchIndex >= 0` and a tracker exists => "batched"):

| Ack type | Batched? | Tracker result | `BatchIndexAckEnabled = false` (default) | `BatchIndexAckEnabled = true` |
|---|---|---|---|---|
| Individual | no | - | send `m` (entry) | send `m` (entry) |
| Individual | yes | empty after clear | send entry id (no ack_set), remove tracker | same |
| Individual | yes | not empty | send nothing for `m` (today's behaviour) | send entry id with `ack_set = tracker bits` (Java `doImmediateBatchIndexAck`, built from the shared ack set then `clear(i)` - equivalent to current tracker bits) |
| Cumulative | no | - | send `m` cumulative | send `m` cumulative |
| Cumulative | yes | empty after `clear(0,i+1)` | send entry cumulative, remove trackers `<=` entry | same |
| Cumulative | yes | not empty | send **(ledger, entry-1)** cumulative (Java `prevMessageId`, L254-L258) and remove trackers `<` entry. **Fixes today's silent drop.** If `entry == 0` send nothing (no previous entry in that ledger; Java sends `entryId - 1 = -1`, which the broker ignores; DotPulsar `EntryId` is `ulong` - skipping is the safe equivalent) | send entry cumulative with `ack_set = fresh set(0,size).clear(0,i+1)` (Java `doCumulativeBatchIndexAck`, L401-L404) and remove trackers `<` entry |

Batched ids with **no tracker** (user-constructed id, tracker already removed): treat as non-batched => send the entry id (today's `BatchHandler.Acknowledge` returns `null` and drops it for individual acks when not found? - no: today it returns `null` only if a tracker is found and incomplete; if not found it returns `null` too and the ack is dropped). **New rule:** not found => send the entry id as-is with `BatchIndex` cleared, matching Java `isBatch == false` for ids without batch size ("acks the whole entry"). Flagged in Open decisions.

Multiple ids of the same entry in one bulk ack (`Acknowledge(IEnumerable<MessageId>)`): apply all clears first, then emit **one** `MessageIdData` per touched entry: entry id if the tracker became empty, else (enabled) entry id + current bits, else (disabled) nothing. Order of emitted ids: order of first appearance in the request (keeps #299 tests' expectations).

### 3.4 Decision table - transactional acks (always, regardless of `BatchIndexAckEnabled`)

Java `doTransactionAcknowledgeForResponse` (L3520-L3586, L3631-L3665):

| Ack type | Batched (`batchIndex >= 0 && batchSize > 0`) | `MessageIdData` sent |
|---|---|---|
| Individual | yes | `ledger, entry`, `batch_size = batchSize`, `ack_set = set(0,batchSize).clear(batchIndex)` (**only this index** is acked; independent of previous acks - not the shared tracker bits) |
| Cumulative | yes | first mutate shared tracker: `AckCumulative(batchIndex)` (Java `MessageIdAdvUtils.acknowledge(msgId, false)` L3532/L3644); then `ledger, entry`, `batch_size`, `ack_set = set(0,batchSize).clear(0, batchIndex+1)` |
| Individual / Cumulative | no | `ledger, entry` only (no `ack_set`, no `batch_size`, no `batch_index`) |

Additional fields on the command: `txnid_most_bits`, `txnid_least_bits`, `request_id` (ack receipt always, see `ack-receipt.md`), `consumer_id`, `ack_type`. Individual transactional ack does **not** modify the shared tracker (Java only mutates for cumulative). The list overload builds one `MessageIdData` per id (no per-entry merging - Java L3636-L3657) in one command.

`batchSize` source in DotPulsar: the tracker for `(ledger, entry, partition)` (`BatchHandler.TryGetBatchSize`). If no tracker exists, fall back to `MessageId.BatchSize` (new internal property, 4.1) when > 0; otherwise the id is treated as non-batched (Java parity for ids without batch size).

### 3.5 Receive-side filtering (`BatchHandler.Add`)

For each index `i` in `0..numMessagesInBatch-1`, after parsing its `SingleMessageMetadata` (parsing must happen for every index to advance the read position, Java L2042-L2046 comment):
1. `if (brokerAckSet is non-empty && bit i is clear)` -> skip (already acked; broker did **not** charge a permit for it).
2. `if (singleMetadata.CompactedOut)` -> skip (broker **did** charge a permit) - count in `chargedButSkipped`.
3. else create the message with `MessageId(ledger, entry, partition, i, topic)` (topic fix) and internal `BatchSize = numMessagesInBatch`.

`ConsumerChannel.Receive` subtracts `chargedButSkipped` from `_sendWhenZero` (clamped at 0) so the next `SendFlow` is triggered as if those messages had been received (Java `increaseAvailablePermits(cnx, skippedMessages)`, L2094-L2097).

If every index was skipped, `Add` returns `null` and `ConsumerChannel.Receive` continues its loop (do not `return null`; today `Add` dequeues unconditionally and would throw on an empty queue).

---

## 4. Public API additions

### 4.1 `src/DotPulsar/MessageId.cs`

```csharp
// Internal: number of messages in the batch this id belongs to, 0 when unknown/not batched. Not part of equality, hashing, ToString or TryParse (Java BatchMessageIdImpl excludes batchSize from equals/hashCode, MessageIdAdvUtils L27-L41).
internal int BatchSize { get; }

internal MessageId(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic, int batchSize) : this(ledgerId, entryId, partition, batchIndex, topic)
    => BatchSize = batchSize;
```

Public constructor/`TryParse` produce `BatchSize = 0`. No public surface change.

### 4.2 `src/DotPulsar/ConsumerOptions.cs`

```csharp
/// <summary>
/// The default of whether to acknowledge individual messages inside a batch with batch index acknowledgment.
/// </summary>
public static readonly bool DefaultBatchIndexAckEnabled = false;

/// <summary>
/// Whether partial acknowledgments of batched messages are sent to the broker using batch index acknowledgment (requires the broker setting 'acknowledgmentAtBatchIndexLevelEnabled'). Transactional acknowledgments always use batch index acknowledgment. The default is 'false'.
/// </summary>
public bool BatchIndexAckEnabled { get; set; }
```

Constructors (all three, `ConsumerOptions.cs L86-L102`) assign `BatchIndexAckEnabled = DefaultBatchIndexAckEnabled`.

### 4.3 `src/DotPulsar/Abstractions/IConsumerBuilder.cs` + `src/DotPulsar/Internal/ConsumerBuilder.cs`

```csharp
/// <summary>
/// Whether partial acknowledgments of batched messages are sent to the broker using batch index acknowledgment. The default is 'false'.
/// </summary>
IConsumerBuilder<TMessage> BatchIndexAckEnabled(bool batchIndexAckEnabled);
```

`ConsumerBuilder`: field `_batchIndexAckEnabled = ConsumerOptions<TMessage>.DefaultBatchIndexAckEnabled`, setter returns `this`, `Create()` copies into options.

---

## 5. Internal design

### 5.1 `AckSetWriter` (new, `src/DotPulsar/Internal/AckSetWriter.cs`, `public static class`)

Zero-allocation helpers writing Java-compatible words into a `RepeatedField<long>` (which is cleared first; `RepeatedField<long>.Clear()` keeps its backing array, so a pooled `MessageIdData` reaches steady state without allocations):

```csharp
public static int WordCount(int batchSize) => (batchSize + 63) >> 6;

// set(0,batchSize) then clear(batchIndex)
public static void WriteIndividual(RepeatedField<long> target, int batchSize, int batchIndex);

// set(0,batchSize) then clear(0, batchIndex + 1)
public static void WriteCumulative(RepeatedField<long> target, int batchSize, int batchIndex);

// copy tracker words, trimming trailing zero words
public static void WriteWords(RepeatedField<long> target, ReadOnlySpan<ulong> words);
```

Implementation detail for `WriteIndividual/Cumulative`: compute words on the stack (`Span<ulong> words = stackalloc ulong[WordCount(batchSize)]` when `WordCount <= 32` i.e. batchSize <= 2048; otherwise `ArrayPool<ulong>.Shared.Rent` + return in `finally`), fill full words with `ulong.MaxValue`, mask the last word with `(1UL << (batchSize & 63)) - 1` when `batchSize % 64 != 0`, clear the requested bit(s), then `WriteWords`. `WriteWords` finds `last = index of last non-zero word`, then `target.Clear(); for (j = 0..last) target.Add(unchecked((long) words[j]));`.

### 5.2 `BatchAckTracker` (new, `src/DotPulsar/Internal/BatchAckTracker.cs`, `public sealed class`)

```csharp
public sealed class BatchAckTracker
{
    private readonly ulong[] _words;    // bit set = not yet acked
    private int _remaining;             // number of set bits (cardinality), maintained incrementally
    public BatchAckTracker(MessageIdData entryId, int batchSize, RepeatedField<long>? brokerAckSet);
    public MessageIdData EntryId { get; }   // ledger, entry, partition of the entry; BatchIndex left at default
    public int BatchSize { get; }
    public bool IsEmpty => _remaining == 0;
    public bool AckIndividual(int batchIndex);   // clear bit; returns IsEmpty; ignores out-of-range indexes (returns IsEmpty)
    public bool AckCumulative(int batchIndex);   // clear bits [0, min(batchIndex+1, BatchSize)); returns IsEmpty
    public void IntersectWith(RepeatedField<long> brokerAckSet);  // AND (merge on redelivery)
    public bool IsAcked(int batchIndex);          // !bit
    public void CopyAckSetTo(RepeatedField<long> target);          // AckSetWriter.WriteWords(target, _words)
}
```

Thread-safety: all calls happen under `BatchHandler._lock` (Java synchronises on the shared `BitSet`, `MessageIdAdvUtils L61`; port of `MessageIdAdvUtilsTest.testAcknowledgeIndividualConcurrently` verifies "exactly one caller observes empty").

One allocation per received batch (`ulong[WordCount]`), comparable to Java's per-batch `BitSet`. The `MessageIdData EntryId` stored is a **new** `MessageIdData { LedgerId, EntryId, Partition }` copy, not the instance from `CommandMessage` (see 5.6 hazard).

### 5.3 `BatchHandler<TMessage>` rewrite (`src/DotPulsar/Internal/BatchHandler.cs`)

Replace `LinkedList<Batch>` + private `Batch` with:

```csharp
private readonly Dictionary<EntryKey, BatchAckTracker> _trackers;   // O(1) lookup
private readonly LinkedList<BatchAckTracker> _trackerOrder;          // arrival order, for cumulative pruning (LinkedListNode stored on the tracker to avoid lookups)
private readonly string _topic;                                      // new ctor parameter (topic fix)
public readonly record struct EntryKey(ulong LedgerId, ulong EntryId, int Partition);   // nested public readonly struct (netstandard2.0: hand-written readonly struct with IEquatable, no records)
```

API (old `Acknowledge(MessageIdData)` removed; all callers are in `ConsumerChannel`):

```csharp
public BatchHandler(bool trackBatches, IMessageFactory<TMessage> messageFactory, string topic);

// Returns the first deliverable message or null when all indexes were skipped. chargedButSkipped: see 3.5.
public IMessage<TMessage>? Add(MessageIdData messageId, RepeatedField<long> brokerAckSet, uint redeliveryCount, MessageMetadata metadata, ReadOnlySequence<byte> data, out int chargedButSkipped);

public IMessage<TMessage>? GetNext();
public void Clear();

public bool TryGetBatchSize(MessageIdData messageId, out int batchSize);
public BatchAckTracker? AcknowledgeIndividual(MessageIdData messageId, out bool isComplete);   // null when not tracked; removes tracker when complete
public BatchAckTracker? AcknowledgeCumulative(MessageIdData messageId, out bool isComplete);   // null when not tracked; prunes all trackers of the same partition whose (ledger, entry) < messageId's, and the tracker itself when complete
public void PruneUpTo(MessageIdData cumulativeEntry);   // used for non-batched cumulative acks: remove trackers of that partition with (ledger, entry) <= given
```

`Add` (replaces `BatchHandler.cs L27-L57`):
- Parses exactly as today (`ReadUInt32` big-endian size, `SingleMessageMetadata.Parser.ParseFrom`, slice payload) for every index.
- Applies 3.5 filters.
- Creates ids via `new MessageId(l, e, p, i, _topic, numMessagesInBatch)`.
- Under `_lock`: if `_trackBatches`: if a tracker for the entry exists (redelivery) -> `existing.IntersectWith(brokerAckSet)` (keeps acks the client already sent but the broker may not have persisted yet - Java keeps them in the old message ids' shared sets while new ids get the broker set; AND is the conservative union of "acked" knowledge); else create and add to both collections. Enqueue deliverable messages; return `_messages.Count == 0 ? null : _messages.Dequeue()`.
- The `List<IMessage<TMessage>>` allocation per batch (`BatchHandler.cs L29`) is replaced by enqueuing directly into `_messages` inside the lock after parsing into a pooled `IMessage<TMessage>[]` rented from `ArrayPool<IMessage<TMessage>>.Shared` (returned after enqueue, cleared) - optional optimisation, behaviour-neutral.

### 5.4 `MessagePackage` + `ChannelManager` (carry `CommandMessage.ack_set`)

- `src/DotPulsar/Internal/MessagePackage.cs`: add `public RepeatedField<long> AckSet { get; }` (ctor parameter after `redeliveryCount`). Reference to the parsed command's field; no copy.
- `src/DotPulsar/Internal/ChannelManager.cs#Incoming(CommandMessage, ReadOnlySequence<byte>)` (L196-L197): `new MessagePackage(command.MessageId, command.RedeliveryCount, command.AckSet, data)`.
- All other `new MessagePackage(...)` call sites (tests: `ConsumerChannelTests` helpers, reader tests) pass `new RepeatedField<long>()` or a shared static empty instance (`MessagePackage.EmptyAckSet`, never mutated).

### 5.5 `ConsumerChannel<TMessage>` changes (`src/DotPulsar/Internal/ConsumerChannel.cs`)

New ctor parameter `bool batchIndexAckEnabled` (passed by `ConsumerChannelFactory` from `ConsumerOptions.BatchIndexAckEnabled`; `Reader` passes `false`). `ConsumerChannelFactory` ctor (L35-L56) gains the same parameter; `Consumer.CreateSubConsumer` (`Consumer.cs L461-L466`) and `Reader` (`Reader.cs L289` region) pass it, and pass `topic` to the `BatchHandler` ctor.

`Receive` (L73-L145): replace `return _batchHandler.Add(messageId, redeliveryCount, metadata, data);` (L126) with:

```
var message = _batchHandler.Add(messageId, messagePackage.AckSet, redeliveryCount, metadata, data, out var chargedButSkipped);
if (chargedButSkipped > 0) _sendWhenZero = chargedButSkipped >= _sendWhenZero ? 0 : _sendWhenZero - (uint) chargedButSkipped;
if (message is null) continue;   // note: the loop decrements _sendWhenZero at its top; compensate: _sendWhenZero++ before continue (nothing was delivered for this iteration's decrement)
return message;
```

(The decrement at L82 accounts for one delivered message per loop iteration; an all-skipped entry delivered nothing, hence the `++` compensation, mirroring the existing compensation in the `catch` at L137-L142.)

`Send(CommandAck command, CancellationToken)` (L147-L191) - rewrite as the decision tables in 3.3 (non-transactional only; transactional acks use the new method below):

```
if (command.MessageId.Count == 0) return;
var cumulative = command.AckType == CommandAck.Types.AckType.Cumulative;
var write = 0;
for (var i = 0; i < command.MessageId.Count; ++i)
{
    var m = command.MessageId[i];
    if (m.BatchIndex < 0) { if (cumulative) _batchHandler.PruneUpTo(m); command.MessageId[write++] = m; continue; }
    var tracker = cumulative ? _batchHandler.AcknowledgeCumulative(m, out var complete) : _batchHandler.AcknowledgeIndividual(m, out complete);
    if (tracker is null) { m.ClearBatchIndex(); command.MessageId[write++] = m; continue; }               // not tracked -> whole entry (3.3)
    if (complete) { ToEntryId(m, tracker); command.MessageId[write++] = m; continue; }                   // entry id, no ack set
    if (_batchIndexAckEnabled)
    {
        if (cumulative) AckSetWriter.WriteCumulative(m.AckSet, tracker.BatchSize, m.BatchIndex) else tracker.CopyAckSetTo(m.AckSet);
        ToEntryId(m, tracker); command.MessageId[write++] = m; continue;
    }
    if (cumulative && tracker.EntryId.EntryId > 0) { ToEntryId(m, tracker); m.EntryId = tracker.EntryId.EntryId - 1; command.MessageId[write++] = m; }
    // individual + disabled + incomplete -> nothing
}
// de-duplicate multiple ids of the same entry (bulk individual ack): keep the LAST written instance per (ledger, entry, partition) but at the position of the FIRST; with an empty tracker it is an entry id, otherwise the latest bits (CopyAckSetTo after all clears). Implementation: second pass, O(n^2) over 'write' items is acceptable for typical bulk sizes; use a stackalloc'd span of EntryKey when write <= 64, else a pooled array.
if (write == 0) return;
while (command.MessageId.Count > write) command.MessageId.RemoveAt(command.MessageId.Count - 1);
command.ConsumerId = _id;
await _connection.Send(command, cancellationToken).ConfigureAwait(false);   // or SendWithResponse (ack-receipt.md)
```

`ToEntryId(m, tracker)`: **mutate in place** `m.LedgerId/EntryId/Partition` from `tracker.EntryId` and `m.ClearBatchIndex()` - do **not** replace the list element with the tracker's `MessageIdData` instance (today's code does `command.MessageId[i] = batchMessageId`, which hands a non-pooled, broker-originated instance to the pooled `CommandAck`; `SubConsumer` later mutates it via `MapFrom` - see 5.6).

Transactional ack entry point (new, used by the transaction WP through `SubConsumer`):

```csharp
public Task<BaseCommand> SendTransactional(CommandAck command, CancellationToken cancellationToken);
```

Fills, for each `MessageIdData m` of the command, per 3.4 (using `_batchHandler.TryGetBatchSize`, falling back to the `MessageId.BatchSize` the caller copied into `m.BatchSize` beforehand when > 0; cumulative also calls `_batchHandler.AcknowledgeCumulative` to mutate the shared tracker but never drops the id), clears `BatchIndex`, sets `ConsumerId`, then delegates to the request/response send of `ack-receipt.md` and returns the response. It must not apply the non-transactional decision table.

### 5.6 Hazards fixed / required invariants

1. **Pooled `MessageIdData` reuse** (`SubConsumer._commandAckPool`, `SubConsumer.cs L65, L197-L236`): `MessageIdDataExtensions.MapFrom` (`Extensions/MessageIdDataExtensions.cs L24-L30`) must also `destination.AckSet.Clear(); destination.ClearBatchSize(); destination.ClearFirstChunkMessageId();` otherwise ack sets leak into the next ack. `SubConsumer.InternalAcknowledge(IEnumerable<MessageId>)` creates fresh `MessageIdData`s via `ToMessageIdData()` (allocation per id; pre-existing; optional improvement: reuse `commandAck.MessageId` elements by index with `MapFrom`).
2. `CommandAck` pool reset (`SubConsumer`): before reuse `ClearTxnidMostBits(); ClearTxnidLeastBits(); ClearRequestId(); ClearValidationError(); Properties.Clear();` (transaction and ack-receipt WPs set these fields on pooled instances).
3. **Topic on batched ids**: `BatchHandler.Add` today builds `new MessageId(..., i)` with `topic = ""` (`BatchHandler.cs L39`) while non-batched ids carry the topic (`ConsumerChannel.cs L135`). Fix via the new `topic` ctor parameter. Required by every ported test that compares produced vs consumed `MessageId`s.
4. **Cumulative ack of partial batch was dropped**: today `ConsumerChannel.Send` (else-branch L176-L187) calls `_batchHandler.Acknowledge` for cumulative acks too, which clears a single index and returns `null` -> no command sent. New table 3.3 fixes it.
5. **Tracker leak**: trackers of batches covered by a cumulative ack were never removed. Fixed by `AcknowledgeCumulative`/`PruneUpTo`.
6. **Duplicate trackers on redelivery**: `_batches.AddLast` added a second tracker for the same entry; `Acknowledge` found the first (stale) one. Fixed by keyed dictionary + merge.
7. **Empty batch after filtering**: handled (3.5).
8. **MessageFactory ordering key** for batched messages uses `metadata.OrderingKey` instead of `singleMetadata.OrderingKey` (`MessageFactory.cs`, batched `Create`): fixed in `producer-batching.md` edit list; re-stated here because receive-side tests depend on it.

### 5.7 `SubConsumer` (`src/DotPulsar/Internal/SubConsumer.cs`)

- `InternalAcknowledge(MessageId, AckType, ct)` (L197-L214): unchanged apart from `MapFrom` reset (5.6.1). Does **not** copy `BatchSize` (non-transactional path relies on trackers).
- New `InternalTransactionalAcknowledge(IReadOnlyList<MessageId> ids, AckType, TransactionId, ct)` (signature owned by transaction WP): fills `MessageIdData` per id via `MapFrom` + `if (id.BatchSize > 0) data.BatchSize = id.BatchSize;` then calls `_channel.SendTransactional` (through the executor send-only + receipt wait outside the executor, `ack-receipt.md` 5.4).

---

## 6. Exact DotPulsar edit points

| File | Member | Change |
|---|---|---|
| `src/DotPulsar/MessageId.cs` | `BatchSize`, internal ctor | 4.1 |
| `src/DotPulsar/ConsumerOptions.cs` | static default, property, 3 ctors | 4.2 |
| `src/DotPulsar/Abstractions/IConsumerBuilder.cs` | `BatchIndexAckEnabled(bool)` | 4.3 |
| `src/DotPulsar/Internal/ConsumerBuilder.cs` | field, setter, `Create()` | 4.3 |
| `src/DotPulsar/Internal/AckSetWriter.cs` | new | 5.1 |
| `src/DotPulsar/Internal/BatchAckTracker.cs` | new | 5.2 |
| `src/DotPulsar/Internal/BatchHandler.cs` | whole class | 5.3 |
| `src/DotPulsar/Internal/MessagePackage.cs` | `AckSet` | 5.4 |
| `src/DotPulsar/Internal/ChannelManager.cs` | `Incoming(CommandMessage, ...)` L196-L197 | 5.4 |
| `src/DotPulsar/Internal/ConsumerChannel.cs` | ctor, `Receive` L119-L135, `Send(CommandAck)` L147-L191, new `SendTransactional` | 5.5 |
| `src/DotPulsar/Internal/ConsumerChannelFactory.cs` | ctor + `Create` | pass `batchIndexAckEnabled` |
| `src/DotPulsar/Internal/Consumer.cs` | `CreateSubConsumer` (L461-L466) | `new BatchHandler<TMessage>(true, messageFactory, topic)`, pass option |
| `src/DotPulsar/Internal/Reader.cs` | L289 | `new BatchHandler<TMessage>(false, messageFactory, topic)`; channel factory `batchIndexAckEnabled: false` |
| `src/DotPulsar/Internal/Extensions/MessageIdDataExtensions.cs` | `MapFrom` | reset `AckSet`, `BatchSize`, `FirstChunkMessageId` |
| `src/DotPulsar/Internal/SubConsumer.cs` | pooled `CommandAck` reset; txn ack entry | 5.6.2, 5.7 |
| `src/DotPulsar/Internal/MessageFactory.cs` | batched `Create` | ordering key fix |
| `tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` | helpers `CreateBatch` (L171-L200), `CreateChannel` (L202-L206) | new `BatchHandler` ctor (topic) and `MessagePackage` ctor (ack set) |

---

## 7. Performance notes

- Ack path: no allocations per ack in steady state: tracker lookup by struct key, ack set words on the stack, `RepeatedField<long>` of pooled `MessageIdData` reused (capacity retained after `Clear()`), in-place mutation of the command's `MessageIdData`.
- Receive path: one `ulong[]` + one `BatchAckTracker` per batched entry (same order as Java); the per-batch `List<IMessage<T>>` may be removed (5.3).
- Lock: single `BatchHandler._lock` as today; dictionary operations are O(1), cumulative pruning is O(k) in removed trackers.
- `CommandMessage.ack_set` is referenced, not copied.

---

## 8. Tests

### 8.1 Unit tests (`[Trait("Category", "Unit")]`)

`tests/DotPulsar.Tests/Internal/AckSetWriterTests.cs`:
- `WriteIndividual_GivenBatchSize10Index3_ShouldWriteSingleWord0x3F7` (bits 0-9 set except 3: `0b11_1111_0111` = 1015)
- `WriteIndividual_GivenBatchSize64Index63_ShouldWriteOneWord` (`0x7FFFFFFFFFFFFFFF`)
- `WriteIndividual_GivenBatchSize65Index64_ShouldTrimTrailingZeroWord` (one word `-1L`)
- `WriteCumulative_GivenBatchSize10Index4_ShouldClearBits0To4` (`0b11_1110_0000` = 992)
- `WriteCumulative_GivenLastIndex_ShouldWriteEmptyList`
- `WriteWords_GivenBatchSize3000_ShouldUsePooledBufferAndMatchJavaBitSet` (compare with a reference implementation built from `System.Collections.BitArray`)
- `WriteIndividual_AfterWarmup_ShouldNotAllocate` (`GC.GetAllocatedBytesForCurrentThread` delta == 0 over 1000 calls, `net8.0+` only via `#if NET8_0_OR_GREATER`)

`tests/DotPulsar.Tests/Internal/BatchAckTrackerTests.cs` - ports:
- `// Upstream: MessageIdAdvUtilsTest#testAcknowledgeIndividualConcurrently` -> `AckIndividual_GivenConcurrentAcksOfAll32Indexes_ShouldReportEmptyExactlyOnce` (100 iterations, batch size 32, all threads released together via `Barrier`, assert exactly one `true`). Tracker calls go through `BatchHandler.AcknowledgeIndividual` to include the lock.
- `AckCumulative_GivenIndex4OfBatch10_ShouldLeaveIndexes5To9`
- `Constructor_GivenBrokerAckSet_ShouldOnlyKeepUnackedIndexes`
- `IntersectWith_GivenRedeliveryAckSet_ShouldMerge`

`tests/DotPulsar.Tests/MessageIdTests.cs` (extend) - ports of `java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/BatchMessageIdImplTest.java` (DotPulsar has a single `MessageId` type; Java `MessageIdImpl(l,e,p)` == DotPulsar `MessageId(l,e,p,-1)`):

| Java | C# | Expected |
|---|---|---|
| `compareToTest` (L29-L37) | `CompareTo_GivenBatchIds0000And1111_ShouldOrder` | -1 / 1 / 0 |
| `hashCodeTest` (L39-L46) | `GetHashCode_GivenDifferentBatchIds_ShouldDiffer` | |
| `equalsTest` (L48-L70) | `Equals_GivenBatchAndNonBatchIds_ShouldFollowJavaRules` | `(0,0,0,-1)` equals `(0,0,0)` (same type in DotPulsar - trivially true); others not equal |
| `notEqualsMultiTest` (L72-L82) | NOT PORTED | no `MultiMessageIdImpl` in DotPulsar |
| `compareToUnbatchedTest` (L84-L94) | `CompareTo_GivenUnbatchedAndBatchIndex0_ShouldBeLess` | `(1,2,3,-1) < (1,2,3,0)`; `(1,2,3,-1) == (1,2,3,-1)` |
| `equalsUnbatchedTest` (L96-L108) | `Equals_GivenUnbatchedIds_ShouldCompareCoordinates` | |
| `hashCodeUnbatchedTest` (L110-L120) | `GetHashCode_GivenEqualUnbatchedIds_ShouldBeEqual` | |
| `serializeAndDeserializeTest` (L122-L131) | NOT PORTED (no `toByteArray` in DotPulsar); instead `BatchSize_ShouldNotTakePartInEqualityOrHashCode` | |

`tests/DotPulsar.Tests/Internal/BatchHandlerTests.cs` (new):
- `Add_GivenBrokerAckSet_ShouldSkipAcknowledgedIndexes` (batch of 5, ack_set `0b10110` -> delivers indexes 1, 2, 4)
- `Add_GivenAllIndexesAcked_ShouldReturnNull`
- `Add_GivenCompactedOutMessage_ShouldSkipItAndReportChargedButSkipped`
- `Add_ShouldSetTopicAndBatchSizeOnMessageIds`
- `Add_GivenRedeliveryOfTrackedEntry_ShouldMergeTrackers`
- `AcknowledgeCumulative_ShouldPruneEarlierTrackersOfSamePartitionOnly`
- `Clear_ShouldRemoveAllTrackers`

`tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` (keep all 6 tests from e8f2ba2 green, update helpers; add):
- `Send_GivenPartialBatchAndBatchIndexAckEnabled_ShouldSendEntryWithAckSet`
- `Send_GivenTwoIndexesOfSameBatchInOneBulkAckAndBatchIndexAckEnabled_ShouldSendOneEntryWithMergedAckSet`
- `Send_GivenCumulativeAckOfPartialBatchAndBatchIndexAckDisabled_ShouldAckPreviousEntryCumulatively`
- `Send_GivenCumulativeAckOfPartialBatchAndBatchIndexAckEnabled_ShouldSendCumulativeAckSet`
- `Send_GivenCumulativeAckOfFirstEntryInLedgerAndBatchIndexAckDisabled_ShouldNotSend`
- `Send_GivenBatchedIdWithoutTracker_ShouldAcknowledgeWholeEntry`
- `Send_GivenCompletedBatch_ShouldNotLeakBrokerMessageIdDataIntoPooledCommand` (assert the element instance in `command.MessageId` is the original pooled instance)
- `Receive_GivenEntryWithAllIndexesAcked_ShouldContinueToNextEntryAndKeepFlowAccounting`
- `SendTransactional_GivenIndividualAckOfBatchIndex2Of5_ShouldSendBatchSize5AndAckSetWithOnlyIndex2Cleared` (`ack_set = [0b11011] = 27`, `batch_size = 5`, no `batch_index`)
- `SendTransactional_GivenCumulativeAckOfBatchIndex2Of5_ShouldSendAckSetForIndexes3And4AndMutateTracker` (`[0b11000] = 24`)
- `SendTransactional_GivenNonBatchedId_ShouldSendNoAckSetNorBatchSize`
- `SendTransactional_GivenBatchIndexAckDisabled_ShouldStillSendAckSet`

`tests/DotPulsar.Tests/Internal/Extensions/MessageIdDataExtensionsTests.cs`: `MapFrom_GivenPooledInstanceWithAckSet_ShouldResetAckSetAndBatchSize`.

`AcknowledgementsGroupingTrackerTest` (`java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/AcknowledgementsGroupingTrackerTest.java`, 955 lines): NOT PORTED as a class - it tests time-based grouping/flush which DotPulsar does not have. The batch-index assertions it contains are covered by the `ConsumerChannelTests` above.

### 8.2 Integration tests

Location per `producer-batching.md` Open decision 7: `tests/DotPulsar.Tests/Prerequisites/`. Broker must have `acknowledgmentAtBatchIndexLevelEnabled=true` -> `[Collection("Transaction")]` + `TransactionFixture`, except the "Disable" class which needs it `false` -> `IntegrationFixture` (default broker config `false`; verify in fixture and set explicitly via env `PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled=false` if needed).

| Java | Port | Notes |
|---|---|---|
| `BatchMessageIndexAckTest#testBatchMessageIndexAckForSharedSubscription(ackReceiptEnabled)` (L164-L242) | yes, `[Theory] [InlineData(true)] [InlineData(false)]` | **needs negative ack** (`negativeAcknowledge`, `negativeAckRedeliveryDelay(2, SECONDS)`) and `redeliverUnacknowledgedMessages()`; mark dependency on the negative-ack WP; Java client default `batchIndexAckEnabled=true` -> set `.BatchIndexAckEnabled(true)`; producer default batching -> `.EnableBatching(true)`; `receiverQueueSize(100)` -> `MessagePrefetchCount(100)`; mark-delete position via admin internal stats `GET .../internalStats` `cursors.sub.markDeletePosition == "ledger:entry"` of `received[0]` |
| `BatchMessageIndexAckTest#testBatchMessageIndexAckForExclusiveSubscription(ackReceiptEnabled)` (L244-L310) | yes | cumulative ack at i == 49, wait 1000 ms, re-subscribe, expect exactly 50, then `ReceiveOrNull(1 s) == null`, then 50 more |
| `BatchMessageIndexAckTest#testDoNotRecycleAckSetMultipleTimes` (L312-L341) | yes (semantic: no exception) | `batchingMaxMessages(10)`, `acknowledgmentGroupTime(1 ms)` -> no grouping in DotPulsar (ignore), 100 messages, cumulative ack each + `Task.Delay(2)` |
| `BatchMessageIndexAckDisableTest#testBatchMessageIndexAckForSharedSubscription` (L66-L106) | yes after ack-timeout WP (`ackTimeout(1, SECONDS)`) | broker `acknowledgmentAtBatchIndexLevelEnabled=false`; expect 100 received |
| `BatchMessageIndexAckDisableTest#testBatchMessageIndexAckForExclusiveSubscription` (L108-L156) | yes | expect all 100 redelivered |
| `ConsumerSeekBatchIndexAckTest#testSeekReplaysBatchAfterSuccessWhileDisconnected(partialBatchAck)` (L47-) | deferred | needs seek-with-ack_set (Open decision 3) |
| `BatchMessageWithBatchIndexLevelTest#testBatchMessageAck` (L85-), `#testPermitsIfHalfAckBatchMessage` (L661-), `#testMixIndexAndNonIndexUnAckMessageCount` (L452-) | yes | broker-stat assertions via admin REST; the others (`*NAck*`, `*NegativeAck*`) depend on negative-ack WP |
| `BatchMessageTest#testOutOfOrderAcksForBatchMessage` (L466-), `#testNonBatchCumulativeAckAfterBatchPublish` (L526-), `#testBatchAndNonBatchCumulativeAcks` (L574-), `#testConcurrentBatchMessageAck` (L647-) | yes (listed in `producer-batching.md` too) | exercise 3.3 fixes |

Transaction ports relying on this WP (owned by transaction WP): `TransactionEndToEndTest` (txn individual/cumulative acks on batched messages with `acknowledgmentAtBatchIndexLevelEnabled=true`), `TransactionEndToEndWithoutBatchIndexAckTest` (same tests with the broker setting `false`).

---

## 9. Acceptance criteria

1. All tests in `ConsumerChannelTests` from commit e8f2ba2 pass unchanged in intent (only helper signatures updated).
2. Default options: on the wire, non-transactional acks are byte-identical to today except: (a) cumulative ack of a partial batch now acks the previous entry instead of nothing; (b) batched ids without tracker ack the entry instead of nothing; (c) no `batch_index` field on entry acks produced from trackers (today the tracker's broker `MessageIdData` is sent, which also lacks it).
3. `BatchIndexAckEnabled = true` + broker `acknowledgmentAtBatchIndexLevelEnabled = true`: acking every second message of a 100-message batch and re-subscribing redelivers only the 50 unacked messages (`BatchMessageIndexAckTests` exclusive variant).
4. Transactional individual ack of index `i` of an n-message batch sends exactly `{ledgerId, entryId, ack_set = all-but-i, batch_size = n}` with `txnid_*` and `request_id`; after commit only that message is acknowledged on the broker.
5. Redelivered batch with a broker `ack_set` delivers only unacked indexes and keeps `MessagePrefetchCount` flow control working (no stall: a further 50 messages are received - Java assertion `received.size() == 100`).
6. No per-ack heap allocation in steady state (unit allocation tests).
7. Batched consumer `MessageId.Topic` equals the (partition) topic name, same as non-batched ids.

---

## 10. Open decisions (recommendation)

1. **Default of `BatchIndexAckEnabled`**: the task brief assumed Java default `false`; at the pinned SHA Java's default is **`true`** (`ConsumerConfigurationData.java L417`). **Recommendation: DotPulsar default `false`** (preserves current wire behaviour; requires no broker setting). Consequence for test porting: every ported Java test that relies on the Java default must call `.BatchIndexAckEnabled(true)` explicitly, unless the test runs against a broker with `acknowledgmentAtBatchIndexLevelEnabled=false` (where the flag has no observable effect).
2. **Batched id without tracker** (user-constructed, or tracker already removed): recommendation "ack the whole entry" (Java parity for ids with `batchSize == 0`). Alternative "drop silently" (today) risks never acknowledging; alternative "throw" is unfriendly. Flag to reviewers because it can acknowledge sibling messages that were not processed when users fabricate ids.
3. **Seek to a batch index** (`ConsumerImpl.java L2993-L3003` sends `ack_set = set(0,size).clear(0, max(batchIndex,0))` on `CommandSeek`): not required by transaction tests; recommendation: separate small follow-up (needs `MessageId.BatchSize`, available after this WP).
4. **Merge rule on redelivery** (AND of existing tracker and broker ack set): Java instead gives new message ids a new shared set initialised from the broker. AND is strictly safer (never re-sends an ack set that "un-acks" an index the client already acked). Recommendation: AND.
5. **Cumulative ack of partial batch with batch-index-ack disabled at `entry == 0`**: recommendation skip (no previous entry within the ledger); document that the batch is then only acknowledged when its last index is cumulatively acked.
