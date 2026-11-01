# Prerequisite work package: Ack timeout (UnAckedMessageTracker) + redelivery primitive

Status: DRAFT spec (no implementation). Target repo: DotPulsar @ e8f2ba22c7136a35d279451e7278589fb0084d2e.
Upstream pins: Java apache/pulsar @ f3846f4811b6a7d98de40a3be2caff1fe288e026 (PRIMARY), C++ apache/pulsar-client-cpp @ f71d7e99019d58b1d11858f05ff0dc83b718627b (cross-reference).
Path prefixes: `java:` = `pulsar-client/src/main/java/org/apache/pulsar/client/impl/` unless a full path is given; `cpp:` = repo root; `dp:` = `src/DotPulsar/`.

## 0. Scope

In scope:
- WP-AT-A: client-side ack timeout. Port of `UnAckedMessageTracker` / `UnAckedTopicMessageTracker`, options `AckTimeout` (default disabled) and `AckTimeoutTickTime` (default 1 s), tracking on receive, removal on ack (individual, cumulative, **transactional**), negative-ack and redeliver interplay, expiry -> `RedeliverUnacknowledgedMessages(ids)`.
- WP-AT-B: the **redelivery primitive** that WP-AT-A (and `dead-letter-policy.md`, `chunking.md`) call into. Today DotPulsar's `RedeliverUnacknowledgedMessages` only sends a command. Java also:
  1. falls back to redeliver-all for Exclusive/Failover,
  2. converts ids to entry level, de-duplicates them and splits them into chunks of 1000,
  3. purges prefetched copies from the local queue and gives the permits back,
  4. on redeliver-all, clears the local queue and the tracker and bumps the **consumer epoch**, so that stale in-flight messages are dropped.

  Without (3) and (4), the upstream ack-timeout tests and several transaction tests see duplicates or stall. Sibling specs assign this primitive to this file (dead-letter-policy.md §0 and §4.7).

Out of scope (see Open decisions):
- `ackTimeoutRedeliveryBackoff` / `UnAckedMessageRedeliveryTracker` / `UnAckedTopicMessageRedeliveryTracker`.
- The OpenTelemetry counter `pulsar.client.consumer.message.ack.timeout`.
- Consumer interceptors (`onAckTimeoutSend`).
- Message listeners.
- Readers: Java readers never set an ack timeout.

Sibling specs referenced (not redefined here):
- `batching.md` (agent prereq-batching): batch-index ack (`ack_set`), the `BatchHandler` rewrite and ack receipt. The rule "the entry is fully acked" comes from its bookkeeping.
- `chunking.md`: `MessageId.ChunkMessageIds` / `FirstChunkMessageId` and the `ChunkedMessageAssembler`, which hands orphan chunk ids to the tracker (`idsToTrack`).
- `dead-letter-policy.md`: the DLQ filter inside the selective-redelivery path (§4.7 there), `NegativeAcknowledge`, which removes from this tracker, and DLQ candidate removal on ack.
- `encryption.md`: the `ConsumerCryptoFailureAction.Fail` path adds the entry id to this tracker (encryption.md §consumer, `_unAckedMessageTracker?.Add(...)`).
- Transaction core spec: the transactional ack send path must call the tracker-removal helper defined in §4.4.

Driving tests:
- `TransactionEndToEndTest#txnAckTestBatchAndCumulativeSub` / `#txnAckTestNoBatchAndCumulativeSub` (`txnCumulativeAckTest`, `pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionEndToEndTest.java` L821-L919). These use `ackTimeout(1, MINUTES)` with Failover, then abort followed by `redeliverUnacknowledgedMessages()`, and expect exactly 1000 re-received messages and then `null`. That needs WP-AT-B (purge plus epoch).
- Every `DeadLetterTopicTest` case triggered by ack timeout (dead-letter-policy.md §8).

## 1. Upstream mapping

| Concern | Java (primary) | C++ (cross-ref) |
|---|---|---|
| Tracker class, data structure (`HashMap<MessageId, HashSet<MessageId>> messageIdPartitionMap` + `ArrayDeque<HashSet<MessageId>> timePartitions`) | `java:UnAckedMessageTracker.java#UnAckedMessageTracker` L45-L58 | `cpp:lib/UnAckedMessageTrackerEnabled.h` L1-L75 |
| Disabled tracker (no-op, `add`/`remove` return true, `size` 0) | `java:UnAckedMessageTracker.java#UnAckedMessageTrackerDisabled` L60-L93 | `cpp:lib/UnAckedMessageTrackerDisabled.h` |
| ctor: `tickDurationInMs = min(tickDurationMillis, ackTimeoutMillis)`, `checkArgument(tick > 0 && ackTimeout >= tick)`, `blankPartitions = ceil(ackTimeout / tick)`, **`blankPartitions + 1`** buckets, timer first fires after `tick` | `java:UnAckedMessageTracker.java#<init>` L115-L138, L182 | `cpp:lib/UnAckedMessageTrackerEnabled.cc#UnAckedMessageTrackerEnabled` L92-L106 (`+1` inside), `#start` L108 |
| Timer task: under write lock remove head bucket, collect ids (expand chunk ids), remove from map, clear head, append as tail, **reschedule in `finally`**, then outside the lock call `onAckTimeoutSend` + `redeliverUnacknowledgedMessages(ids)` | `java:UnAckedMessageTracker.java#<init>$TimerTask.run` L139-L182 | `cpp:lib/UnAckedMessageTrackerEnabled.cc#timeoutHandler` L35-L50, `#timeoutHandlerHelper` L52-L86 (unlock before redeliver, L80-L85) |
| Chunk expansion at expiry | `java:UnAckedMessageTracker.java#addChunkedMessageIdsAndRemoveFromSequenceMap` L189-L199 | `cpp:lib/ConsumerImpl.cc#redeliverMessages` L1705-L1720 (expansion on the redeliver side; the tracker keeps the chunk id, `UnAckedMessageTrackerEnabled.cc` L110-L131) |
| `add(id)` -> `putIfAbsent` into newest bucket (`peekLast`), returns false if already tracked | `java:UnAckedMessageTracker.java#add` L211-L228, `#add(MessageId,int)` L230-L232 | `cpp:lib/UnAckedMessageTrackerEnabled.cc#add` L110-L131 |
| `remove(id)` | `java:UnAckedMessageTracker.java#remove` L243-L259 | `cpp:…#remove` L138-L158 |
| `removeMessagesTill(id)` (all ids `compareTo(id) <= 0`) | `java:UnAckedMessageTracker.java#removeMessagesTill` L270-L288 | `cpp:…#removeMessagesTill` L165-L176 |
| `clear`, `size`, `isEmpty`, `close`/`stop` | `java:UnAckedMessageTracker.java` L201-L209, L234-L241, L261-L268, L290-L306 | `cpp:…` L133-L136, L160-L163, L192-L204 |
| Topic variant (`removeTopicMessages`) for multi-topic consumers | `java:UnAckedTopicMessageTracker.java#removeTopicMessages` L35-L54 | `cpp:…#removeTopicMessage` L178-L190 |
| Which tracker is created (`ackTimeoutMillis != 0` -> topic tracker, else disabled) | `java:ConsumerBase.java#<init>` L214-L222 | `cpp:lib/ConsumerImpl.cc#ConsumerImpl` L135-L147 |
| Config defaults `ackTimeoutMillis = 0`, `tickDurationMillis = 1000` | `java:conf/ConsumerConfigurationData.java` L189-L202 | `cpp:include/pulsar/ConsumerConfiguration.h#setUnAckedMessagesTimeoutMs` L224-L234 (C++ min is 10 s, Java min is 1 s, **use Java**), `#setTickDurationInMs` L241-L252 |
| Builder validation: `ackTimeout == 0 \|\| >= 1000 ms` ("Ack timeout should be greater than 1000 ms"), tick `>= 100 ms` | `java:ConsumerBuilderImpl.java#ackTimeout` L292-L298, `#ackTimeoutTickTime` L306-L312, constants L76-L77 | — |
| API doc | `pulsar-client-api/…/ConsumerBuilder.java#ackTimeout` L184-L202, `#ackTimeoutTickTime` L220-L233 | — |
| Tracking enabled only if `ackTimeoutMillis > 0` **and topic is persistent** | `java:ConsumerBase.java#isAckTimeoutTrackingEnabled` L266-L268, `java:ConsumerImpl.java#isAckTimeoutTrackingEnabled` L2154-L2164, warning L429-L433 | — |
| Track point = message handed to the application (`messageProcessed`), id = `discardBatch(id)` (chunk id kept) | `java:ConsumerImpl.java#messageProcessed` L2130-L2142, `#trackMessage` L2144-L2177; `java:ConsumerBase.java#trackUnAckedMsgIfNoListener` L270-L275, listener path L1313-L1321; `java:MessageIdAdvUtils.java#discardBatch` L75-L81 | `cpp:lib/ConsumerImpl.cc#notifyPendingReceivedCallback` L925-L932, `#trackMessage` L1972-L1978 |
| Other add sites: crypto FAIL (encryption.md); orphan/expired chunk (chunking.md) | `java:ConsumerImpl.java` L2378-L2381; L1920-L1926, L3500-L3511 | `cpp:lib/ConsumerImpl.cc` L1024-L1025, L1046-L1047 |
| Remove on individual ack (only when the whole batch is acked) | `java:PersistentAcknowledgmentsGroupingTracker.java#addIndividualAcknowledgment` L212-L224 (L217-L219) | `cpp:lib/ConsumerImpl.cc#prepareIndividualAck` L1499-L1510 (L1504-L1507) |
| Remove on cumulative ack (`removeMessagesTill`) | `java:PersistentAcknowledgmentsGroupingTracker.java#doCumulativeAck` L338-L340 | `cpp:lib/ConsumerImpl.cc` L1482-L1488 |
| Remove on **transactional** ack (single id; cumulative -> `removeMessagesTill`, individual -> `remove`) **at send time, before the response** | `java:ConsumerImpl.java#doTransactionAcknowledgeForResponse(MessageId,…)` L3520-L3574 (L3570-L3574) | C++ has no transactions |
| Remove on transactional ack (list form) | `java:ConsumerImpl.java#doTransactionAcknowledgeForResponse(List,…)` L3631-L3666 (L3657-L3661) | — |
| Remove on negative ack (`discardBatch(id)`) | `java:ConsumerImpl.java#negativeAcknowledge` L886-L902 | `cpp:lib/ConsumerImpl.cc#negativeAcknowledge` L1537-L1538 |
| Remove on retry-letter failure | `java:ConsumerImpl.java` L827-L837 (retry topic is out of scope, see dead-letter-policy.md) | — |
| Close | `java:ConsumerImpl.java#closeConsumerTasks` L1350-L1351 | `cpp:lib/ConsumerImpl.cc` L2087 |
| Redeliver-all: epoch++ (Failover/Exclusive only), `clearIncomingMessages()`, `unAckedMessageTracker.clear()`, send `REDELIVER_UNACKNOWLEDGED_MESSAGES{consumer_id, consumer_epoch}`, refund permits | `java:ConsumerImpl.java#redeliverUnacknowledgedMessages()` L2490-L2551 (L2516-L2541); `java:ConsumerBase.java#clearIncomingMessages` L1396-L1406; `pulsar-common/…/Commands.java#newRedeliverUnacknowledgedMessages(long,long)` L1298-L1304 | `cpp:lib/ConsumerImpl.cc#redeliverUnacknowledgedMessages()` L1666-L1670 |
| Selective redeliver: non-Shared/KeyShared -> redeliver-all; purge matching queued messages (`removeExpiredMessagesFromQueue`); DLQ filter; entry-level ids (`partition, ledger, entry`, **no batch index**); chunks of `MAX_REDELIVER_UNACKNOWLEDGED = 1000`; refund permits | `java:ConsumerImpl.java#redeliverUnacknowledgedMessages(Set)` L2553-L2589, `#getRedeliveryMessageIdData` L2610-L2629, `#removeExpiredMessagesFromQueue` L3293-L3308, constant L148; `Commands.java#newRedeliverUnacknowledgedMessages(long,List)` L1306-L1319 | `cpp:lib/ConsumerImpl.cc#redeliverUnacknowledgedMessages(set)` L1672-L1703, `#redeliverMessages` L1705-L1720 |
| Consumer epoch: field (initial 0), subscribe with epoch (`> -1`, so always sent), resend of the current epoch after subscribe if it changed in flight, filter of stale messages | `java:ConsumerBase.java` L126-L135, `#isValidConsumerEpoch` L1449-L1464; `java:ConsumerImpl.java#connectionOpened` L975-L1011, `#messageReceived` L1539-L1545, enqueue filter L1481-L1485; `Commands.java#newSubscribe` L574-L577, `DEFAULT_CONSUMER_EPOCH = -1L` L138 | — |
| Seek: Java clears the incoming queue, **not** the tracker | `java:ConsumerImpl.java#seekAsyncInternal` L2913-L2920 | — |

Unit/integration tests (classification in §7): `pulsar-client/src/test/java/org/apache/pulsar/client/impl/UnAckedMessageTrackerTest.java` (L44-L129), `pulsar-client/src/test/java/org/apache/pulsar/client/impl/UnAckedTopicMessageRedeliveryTrackerTest.java`, `pulsar-broker/src/test/java/org/apache/pulsar/client/impl/UnAcknowledgedMessagesTimeoutTest.java` (L42-L582), `pulsar-broker/src/test/java/org/apache/pulsar/client/impl/PerMessageUnAcknowledgedRedeliveryTest.java` (L33-L480), `pulsar-broker/src/test/java/org/apache/pulsar/client/api/ConsumerRedeliveryTest.java` (L394-L443, L491-L544).

### 1.1 Exact timing semantics (must be reproduced)
- `tick = min(AckTimeoutTickTime, AckTimeout)`. `bucketCount = ceil(AckTimeout / tick) + 1`. With the defaults (`AckTimeout = 2 s`, tick 1 s) there are 3 buckets.
- The timer first fires `tick` after the tracker is constructed (at consumer creation, not at first receive), then every `tick`. Each firing is re-armed after the previous one completes, so firings never overlap.
- `Add` puts the id in the **newest** bucket. A firing removes the **oldest** bucket and appends it, cleared, as the newest. So an id added at time t expires at the first firing at or after `t + (bucketCount - 1) * tick`, i.e. somewhere in `(AckTimeout, AckTimeout + tick]` when AckTimeout is a multiple of tick. The upstream sleeps (`Thread.sleep(ackTimeOutMillis)` twice in `PerMessageUnAcknowledgedRedeliveryTest`, and `1.1x` / `2x` in `UnAcknowledgedMessagesTimeoutTest`) depend on this window.
- An id already tracked is **not** moved to a newer bucket by a second `Add` (`putIfAbsent`). Redelivered messages are re-added only after the expired copy was removed during the firing.

## 2. Public API additions

### 2.1 `dp:ConsumerOptions.cs`
Add these after `DefaultSubscriptionType` (L55-L58), keeping the file's ordering of static defaults:
```csharp
    /// <summary>
    /// The default ack timeout.
    /// </summary>
    public static readonly TimeSpan DefaultAckTimeout = TimeSpan.Zero;

    /// <summary>
    /// The default ack timeout tick time.
    /// </summary>
    public static readonly TimeSpan DefaultAckTimeoutTickTime = TimeSpan.FromSeconds(1);
```
Set them in the private ctor (L62-L81): `AckTimeout = DefaultAckTimeout; AckTimeoutTickTime = DefaultAckTimeoutTickTime;`.

Add these properties (alphabetical position, before `ConsumerName` L101-L104):
```csharp
    /// <summary>
    /// Set the timeout for unacknowledged messages. Must be zero (disabled) or at least 1 second. The default is 'TimeSpan.Zero' (disabled).
    /// </summary>
    /// <remarks>
    /// When enabled, a received message that is not acknowledged within the timeout is redelivered (possibly to another consumer on a shared subscription).
    /// On exclusive and failover subscriptions an expiry redelivers all unacknowledged messages.
    /// The timeout is ignored for non-persistent topics.
    /// </remarks>
    public TimeSpan AckTimeout { get; set; }

    /// <summary>
    /// Set the granularity of the ack timeout redelivery. Must be at least 100 milliseconds. The default is 1 second.
    /// </summary>
    /// <remarks>
    /// A higher tick time reduces the memory overhead of tracking messages when the ack timeout is large (e.g. 1 hour).
    /// </remarks>
    public TimeSpan AckTimeoutTickTime { get; set; }
```

### 2.2 `dp:Abstractions/IConsumerBuilder.cs`
Add at the top of the interface (alphabetical, before `ConsumerName`):
```csharp
    /// <summary>
    /// Set the timeout for unacknowledged messages. Must be zero (disabled) or at least 1 second. The default is 'TimeSpan.Zero' (disabled).
    /// </summary>
    IConsumerBuilder<TMessage> AckTimeout(TimeSpan ackTimeout);

    /// <summary>
    /// Set the granularity of the ack timeout redelivery. Must be at least 100 milliseconds. The default is 1 second.
    /// </summary>
    IConsumerBuilder<TMessage> AckTimeoutTickTime(TimeSpan tickTime);
```
This is a breaking change for third-party `IConsumerBuilder` implementers; it is accepted, following the repo precedent (`AllowOutOfOrderDeliver` was added the same way).

### 2.3 `dp:Internal/ConsumerBuilder.cs`
- Add fields `private TimeSpan _ackTimeout;` and `private TimeSpan _ackTimeoutTickTime;` (field block L23-L39). Initialise them in the ctor (L41-L55) from `ConsumerOptions<TMessage>.DefaultAckTimeout` and `.DefaultAckTimeoutTickTime`.
- Add the two fluent setters, same shape as `PriorityLevel` (L75-L79). They do not validate.
- In `Create()` (L153-L179), set `AckTimeout = _ackTimeout, AckTimeoutTickTime = _ackTimeoutTickTime` in the initializer.

### 2.4 Validation `dp:PulsarClient.cs#CreateConsumer` (L89-L100)
After `ThrowIfDisposed();`, mirroring the `CreateProducer` validation at L63-L64:
```csharp
        if (options.AckTimeout != TimeSpan.Zero && options.AckTimeout < TimeSpan.FromSeconds(1))
            throw new ConfigurationException("ConsumerOptions.AckTimeout must be TimeSpan.Zero (disabled) or at least 1 second");

        if (options.AckTimeoutTickTime < TimeSpan.FromMilliseconds(100))
            throw new ConfigurationException("ConsumerOptions.AckTimeoutTickTime must be at least 100 milliseconds");
```
Negative `AckTimeout` values are rejected by the first check. These match Java's `MIN_ACK_TIMEOUT_MILLIS = 1000` and `MIN_TICK_TIME_MILLIS = 100` (`ConsumerBuilderImpl.java` L76-L77). Sub-millisecond parts are truncated, as in Java ("truncated to the nearest millisecond", `ConsumerBuilder.java` L185): `ackTimeoutMs = (long) options.AckTimeout.TotalMilliseconds`.

No other public type changes. `IConsumer.RedeliverUnacknowledgedMessages` keeps its signatures; its behaviour changes are listed in §4.6 and §6.

## 3. New internal types

### 3.1 `dp:Internal/UnAckedMessageTracker.cs` (namespace `DotPulsar.Internal`, `public sealed class`, implements `IDisposable`)

One instance per `SubConsumer` (i.e. per partition or topic), created only when tracking is enabled (§4.1). It is `null` otherwise; DotPulsar uses `?.` instead of Java's `UNACKED_MESSAGE_TRACKER_DISABLED` singleton. Sibling specs already call it as `_unAckedMessageTracker?.Add(...)` / `?.Remove(...)`.

```csharp
public sealed class UnAckedMessageTracker : IDisposable
{
    public UnAckedMessageTracker(
        TimeSpan ackTimeout,
        TimeSpan tickTime,
        Func<MessageId[], CancellationToken, ValueTask> redeliver);

    public void Start();                                   // creates and arms the timer (idempotent); unit tests never call it and drive Tick() directly

    public bool Add(MessageId messageId);                  // Java add L211-L228
    public bool Remove(MessageId messageId);               // Java remove L243-L259
    public bool Remove(ulong ledgerId, ulong entryId);     // allocation-free overload used by ConsumerChannel after batch resolution
    public int RemoveMessagesTill(MessageId messageId);    // Java removeMessagesTill L270-L288
    public void Clear();                                   // Java clear L201-L209
    public int Count { get; }                              // Java size()
    public bool IsEmpty { get; }                           // Java isEmpty()
    public MessageId[]? Tick();                            // body of the Java timer task (L146-L168), returns the expired ids or null; public for deterministic unit tests
    public void Dispose();                                 // Java close/stop L290-L306: stop timer, cancel pending redeliveries, Clear()
}
```

State (all guarded by a single `private readonly object _lock`; Java's `ReentrantReadWriteLock` is not needed because every mutating Java path takes the write lock):
- `private readonly Dictionary<EntryKey, int> _index;`: key to bucket slot (Java `messageIdPartitionMap`).
- `private readonly Dictionary<EntryKey, MessageId>[] _buckets;`: ring buffer of `bucketCount` dictionaries (Java `timePartitions`; each Java `HashSet<>(16, 1)` becomes `new Dictionary<EntryKey, MessageId>(16)`). The value keeps the original `MessageId` instance, which preserves `Topic`, `Partition` and, for chunked messages, `ChunkMessageIds` (chunking.md; mirrors C++ `UnAckedMessageTrackerEnabled.cc` L112-L123).
- `private int _head;`: slot of the oldest bucket. The newest bucket is `(_head + bucketCount - 1) % bucketCount`.
- `private readonly long _tickMs;` with `_tickMs = Math.Min(tickMs, ackTimeoutMs)`. The ctor throws `ArgumentOutOfRangeException` unless `_tickMs > 0 && ackTimeoutMs >= _tickMs` (Java `checkArgument` L119; unreachable after §2.4 validation).
- `bucketCount = (int) Math.Ceiling((double) ackTimeoutMs / _tickMs) + 1` (Java L135-L138).
- `private readonly System.Threading.Timer? _timer;`, `private readonly CancellationTokenSource _cts;` and `private int _isDisposed;`.

`EntryKey` is a private nested `readonly struct EntryKey : IEquatable<EntryKey>` with `(ulong LedgerId, ulong EntryId)` and `GetHashCode() => HashCode.Combine(LedgerId, EntryId)` (`HashCode` is already used on all targets by `MessageId.GetHashCode`). The partition is implied because there is one tracker per `SubConsumer`, the same convention as dead-letter-policy.md OD-6. The batch index is discarded, as in Java `discardBatch` (`MessageIdAdvUtils.java` L75-L81): a batch message is tracked by its entry. For a chunked `MessageId` the key is that id's own (ledger, entry), i.e. the last chunk (chunking.md §6.2).

Semantics:
- `Add(id)`: under `_lock`, if `_index` has the key return `false`. Otherwise `slot = newest`, `_buckets[slot][key] = id`, `_index[key] = slot`, return `true`. A batch id whose entry is already tracked returns `false` (Java: `discardBatch` + `putIfAbsent`).
- `Remove(id)` / `Remove(ledger, entry)`: under `_lock`, `_index.Remove(key, out slot)` (ns2.0: `TryGetValue` followed by `Remove`), then `_buckets[slot].Remove(key)`. Returns whether it was tracked. The batch index is ignored, so removal is entry-level. Java's `remove(batchId)` would miss; this difference is OD-2 and callers in §4.4 only call `Remove` when the entry is complete.
- `RemoveMessagesTill(id)`: under `_lock`, iterate `_index` into a reusable `List<EntryKey>` field `_scratch`, collecting keys where `(key.LedgerId, key.EntryId) <= (id.LedgerId, id.EntryId)` in lexicographic order; remove them from both structures; return the count. This matches Java's `compareTo(msgId) <= 0`: a tracked entry-level id `(L, E, -1)` compares `<=` to `(L, E, k)` for any `k >= -1` (`MessageIdImpl` / `BatchMessageIdImpl` order: ledger, entry, partition, batch index), so a cumulative ack of a batch index removes its whole entry. The tracker is per partition, so partition and topic never differ.
- `Clear()`: clears `_index` and every bucket; `_head` is unchanged (Java does not rotate).
- `Tick()`: under `_lock`:
  - Take `head = _buckets[_head]`.
  - If it is non-empty, copy its values into a new `MessageId[]`, remove each key from `_index`, and `head.Clear()`.
  - Advance `_head = (_head + 1) % bucketCount` (the cleared head becomes the newest; Java `removeFirst` + `addLast`).
  - Return the array, or `null` if the bucket was empty.

  Chunk expansion is **not** done here; it happens in the redelivery primitive (§4.6 step 3), as in C++ L1709-L1716. That keeps the tracker independent of chunking.md.
- Timer: `Start()` does `_timer ??= new Timer(OnTimer, null, _tickMs, Timeout.Infinite)` under `_lock` (one-shot), unless disposed. The first firing is `tick` after `Start()`, which is called at consumer creation (Java starts the timer in the ctor, L139-L182). `OnTimer`:
  1. If disposed, return.
  2. `MessageId[]? expired = null; try { expired = Tick(); } finally { lock (_lock) { if (_isDisposed == 0) _timer.Change(_tickMs, Timeout.Infinite); } }`. This re-arms the next tick before the redeliver call, as Java does in `finally` (L169-L172).
  3. If `expired is not null`, call `_ = InvokeRedeliver(expired)`, outside the lock, as Java (L173-L178) and C++ (L80-L85) do. `InvokeRedeliver` does `try { await _redeliver(expired, _cts.Token).ConfigureAwait(false); } catch { /* ignored */ }`.
- `Dispose()`: `Interlocked.Exchange(ref _isDisposed, 1)` guard, then `_cts.Cancel()`, `_timer?.Dispose()`, `Clear()`, `_cts.Dispose()`. `Add`/`Remove`/`Count` keep working after dispose: they are no-ops on the empty structures and throw nothing. The ported Java `testAddAndRemove` calls `tracker.close()` first and then uses the tracker (L62-L82).

Zero-alloc and perf:
- `Add` and `Remove` allocate nothing in steady state: dictionaries only grow up to the high-water mark, and `EntryKey` is a struct.
- `Tick` allocates one array only when something expired.
- `RemoveMessagesTill` reuses `_scratch`.
- There is one `Timer` per tracker, and only when ack timeout is enabled; with the feature disabled the hot path costs one null check.
- With 100 partitions there are 100 timers, each firing once per tick. `Timer` is cheap (timer queue), so this is acceptable. A shared wheel is OD-3.

### 3.2 `dp:Internal/ConsumerEpoch.cs` (`public sealed class`)
```csharp
public sealed class ConsumerEpoch
{
    private long _value;                                       // Java ConsumerBase.consumerEpoch, initial 0 (L135)
    public long Value => Interlocked.Read(ref _value);
    public long Increment() => Interlocked.Increment(ref _value);
}
```
There is one per `SubConsumer`, shared with its `ConsumerChannelFactory` and every `ConsumerChannel` it creates. `DEFAULT_CONSUMER_EPOCH` (-1) is represented as `-1L` on `MessagePackage`.

## 4. DotPulsar edit points (exact)

### 4.1 Wiring: `dp:Internal/Consumer.cs#CreateSubConsumer` (L427-L474)
Before the `ConsumerChannelFactory` is constructed (L465):
```csharp
var consumerEpoch = new ConsumerEpoch();
UnAckedMessageTracker? unAckedMessageTracker = null;
if (_consumerOptions.AckTimeout > TimeSpan.Zero && !topic.StartsWith("non-persistent://", StringComparison.Ordinal))   // Java ConsumerImpl L2162-L2164
    unAckedMessageTracker = new UnAckedMessageTracker(_consumerOptions.AckTimeout, _consumerOptions.AckTimeoutTickTime, <redeliver delegate, see below>);
```
The topic names produced by `Monitor` (L106-L157) are the user strings; a bare name like `my-topic` is persistent (Pulsar default). So check the `non-persistent://` prefix ordinally, and treat everything else as persistent.

The redeliver delegate must reach the `SubConsumer`, but the `ConsumerChannelFactory`, which needs the tracker, is constructed before the `SubConsumer`. So construct the tracker first, with a delegate that closes over a local declared above it: `SubConsumer<TMessage>? subConsumer = null;` and the delegate `(ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct)`. The timer is not armed until `Start()` is called, after `subConsumer` has been assigned, so the delegate can never observe `null`.

Pass the following:
- `consumerEpoch` and `unAckedMessageTracker` to `new ConsumerChannelFactory<TMessage>(…)` (L465) as two new trailing ctor parameters.
- `unAckedMessageTracker` and `consumerEpoch` to `new SubConsumer<TMessage>(…)` (L469) as two new trailing ctor parameters.
- `Start()` right after `_processManager.Add(process); process.Start();` (L471-L472).

### 4.2 `dp:Internal/SubConsumer.cs`
- New fields `private readonly UnAckedMessageTracker? _unAckedMessageTracker;` and `private readonly ConsumerEpoch _consumerEpoch;`, assigned in the ctor (L43-L69).
- Test/diagnostic hook: `public int UnAckedMessageCount => _unAckedMessageTracker?.Count ?? 0;`. It is `0` when disabled (Java's disabled tracker reports size 0, `PerMessageUnAcknowledgedRedeliveryTest` L172-L173).
- `DisposeAsync` (L71-L78): call `_unAckedMessageTracker?.Dispose();` before `DisposeChannel()` (Java `closeConsumerTasks` L1350-L1351). Also do this in `ChannelFaulted` (L155-L159).
- `public void Track(IMessage<TMessage> message)`: `_unAckedMessageTracker?.Add(message.MessageId);`. Called by `Consumer` at hand-off (§4.3). The batch index is dropped inside the tracker.
- `Receive` (L86-L87) is unchanged. Tracking is done by the caller, so a message parked in `Consumer._receiveTasks` (multi-topic prefetch, `Consumer.cs` L234-L249) is tracked only when the application actually gets it, which is Java's `messageProcessed` semantics.
- `RedeliverUnacknowledgedMessages(IEnumerable<MessageId>, ct)` (L98-L103) and `RedeliverUnacknowledgedMessages(ct)` (L105-L106) are replaced as described in §4.6.
- Acknowledge paths: no tracker code here. Removal happens in `ConsumerChannel` after batch resolution (§4.4), so it is in one place for every ack kind, including the transactional ones routed through the channel by the transaction core spec.
- `NegativeAcknowledge` (dead-letter-policy.md WP-B §4.8) calls `_unAckedMessageTracker?.Remove(messageId)`. The tracker ignores the batch index, which is Java L892/L901 `discardBatch`.

### 4.3 `dp:Internal/Consumer.cs#Receive` (L213-L260): tracking at hand-off
- Single sub-consumer (L217-L218): `var message = await _singleSubConsumer.Receive(cancellationToken).ConfigureAwait(false); _singleSubConsumer.Track(message); return message;`
- Multi, completed parked task (L224-L231): `var subConsumer = _receivers[i]; … var message = task.Result; subConsumer.Track(message); return message;`
- Multi, synchronously completed (L246-L247): `subConsumer.Track(receiveTask.Result); return receiveTask.Result;`

There is no dictionary lookup and no allocation: the sub-consumer is already known at each return point.

### 4.4 `dp:Internal/ConsumerChannel.cs`: removal on ack, epoch filter, purge
New ctor parameters (appended): `UnAckedMessageTracker? unAckedMessageTracker`, `ConsumerEpoch consumerEpoch` and `bool filterByConsumerEpoch` (`true` for Exclusive and Failover, Java `isValidConsumerEpoch` L1452-L1453). `ConsumerChannelFactory.Create` (L54-L61) passes them through. The factory knows the subscription type from `_subscribe.SubType`.

(a) Removal on ack: `Send(CommandAck command, ct)` (L147-L191).
- At method entry, before any batch resolution, if `command.AckType == Cumulative`: `_unAckedMessageTracker?.RemoveMessagesTill(command.MessageId[0])` using an allocation-free overload `RemoveMessagesTill(ulong ledgerId, ulong entryId)`. This mirrors Java `doCumulativeAck` L340 and C++ L1487. The existing early `return` at L182-L183 (partial batch on cumulative) therefore no longer skips tracker removal.
- After batch resolution (individual paths, L152-L187), for every `MessageIdData` that will actually be sent with `BatchIndex == -1`: `_unAckedMessageTracker?.Remove(id.LedgerId, id.EntryId)`. This covers a non-batched id, or a batch whose last index was just acked (`_batchHandler.Acknowledge` returned the entry). It mirrors Java `addIndividualAcknowledgment` L217-L219 and C++ L1504-L1507. When batching.md adds batch-index acks (`ack_set`), a partial batch ack must **not** remove, and the full-batch completion must remove; the same "entry complete" signal used here also drives dead-letter-policy.md §4.6.
- Extract these two rules into `private void RemoveAcknowledgedFromTracker(CommandAck command, bool beforeBatchResolution)`. They are called at the two points above.
- **Transactional acks** (transaction core spec): `IConsumerChannel.SendTransactional(CommandAck, ct)` returns `Task<BaseCommand>` (implemented in `ConsumerChannel`; its ack_set/batch_size body is in batch-index-ack.md §5.5; it uses the core spec's shared ack-with-response plumbing: `IConnection.SendWithResponse`, then `ChannelManager.OutgoingWithResponse`, then `RequestResponseHandler.OutgoingWithResponse`, keyed by request id only; the caller evaluates `Expect(AckResponse)` / `Throw()` outside `_executor`). It must call `RemoveAcknowledgedFromTracker` and apply the same two rules **at send time, before awaiting the `AckResponse`, and regardless of its outcome**. This is Java L3570-L3574 (single) and L3657-L3661 (list): the tracker entry is removed even if the broker later answers `TransactionConflict` or the transaction is aborted. After an abort the application must call `RedeliverUnacknowledgedMessages()` (as `txnCumulativeAckTest` does, L894-L895) or rely on broker redelivery. For a batch-indexed transactional individual ack, remove only when the entry is complete per batching.md bookkeeping (OD-2).
- `RejectPackage` (L253-L264) goes through `Send(CommandAck)` with a single non-batched id; removal is harmless because a rejected package was never tracked.

(b) Epoch filter in `Receive` (L73-L145). Right after `var messagePackage = await _queue.Dequeue(cancellationToken)` (L90), before checksum validation:
```csharp
if (_filterByConsumerEpoch && messagePackage.ConsumerEpoch != -1 && messagePackage.ConsumerEpoch < _consumerEpoch.Value)
    continue;   // Java ConsumerBase.isValidConsumerEpoch L1451-L1464 + ConsumerImpl L1482-L1485
```
The `_sendWhenZero--` at L82 has already counted this entry, which matches Java `increaseAvailablePermits(cnx())` (L1483) for a dropped message. No extra flow is needed: the broker charged one permit per message, and DotPulsar's counter is per dequeued message. Note that a dropped batch entry still only decrements once; batching.md, which changes permit accounting for batches, must count `NumMessagesInBatch` here too. Until then this is consistent with today's behaviour, which already decrements once per entry when it calls `_batchHandler.Add`.

(c) Purge plus refund in `Send(CommandRedeliverUnacknowledgedMessages command, ct)` (L193-L197). New body:
```csharp
command.ConsumerId = _id;
int purged;
if (command.MessageIds.Count == 0)
    purged = PurgeAll();                                           // Java clearIncomingMessages L2528
else
    purged = PurgeEntries(command.MessageIds);                     // Java removeExpiredMessagesFromQueue L3293-L3308
await _connection.Send(command, cancellationToken).ConfigureAwait(false);
if (purged > 0)
    await _connection.Send(new CommandFlow { ConsumerId = _id, MessagePermits = (uint) purged }, cancellationToken).ConfigureAwait(false);   // Java increaseAvailablePermits(cnx, n) L2539-L2541, L2576-L2578
```
Java's order is clear queue, then send redeliver, then permits.
- `PurgeAll()`: `var packages = _queue.RemoveAll();` plus `_batchHandler.ClearQueuedMessages()`. The result is the sum of the permit weight of each package (below) plus the number of unpacked messages dropped from `BatchHandler`.
- `PurgeEntries(ids)`: build a `HashSet<(ulong, ulong)>` of `(LedgerId, EntryId)` from `ids`. `_queue.RemoveWhere(p => set.Contains((p.MessageId.LedgerId, p.MessageId.EntryId)))` plus `_batchHandler.RemoveQueuedMessages(set)`. The result is summed the same way. Java compares with `NegativeAcksTracker.discardBatchAndPartitionIndex` (L3298), i.e. ledger and entry only, which is what this does.
- Permit weight of a removed package: `1`, or `metadata.NumMessagesInBatch` when `metadata.HasNumMessagesInBatch`. Read it via `messagePackage.GetMetadataSize()` / `ExtractMetadata` inside a `try`; on any parse failure count `1`. This path is rare, so the protobuf parse is acceptable.
- Why the refund is mandatory: DotPulsar's flow invariant is `queued + in-flight + brokerPermits = _sendWhenZero + window`. Purging K messages without a refund shrinks the window by K and can make `_sendWhenZero` unreachable, which stalls the consumer. Sending `CommandFlow{K}` restores it exactly. `_cachedCommandFlow` must **not** be used for this, because `Receive` mutates it (L240-L251).
- No `_lock` (the receive `AsyncLock`) is taken: `Receive` may be parked inside `Dequeue` holding it. The queue and `BatchHandler` have their own locks. A `Receive` that dequeued a package just before the purge may still return it; Java has the same race, and for Exclusive/Failover the epoch filter covers the later redelivered copy.

New helpers:
- `dp:Internal/AsyncQueue.cs`: `public List<T> RemoveAll()` and `public List<T> RemoveWhere(Func<T, bool> predicate)`. Both run under `_lock`, and both `ThrowIfDisposed()`. `RemoveWhere` rebuilds `_queue` keeping order; `Queue<T>` has no removal API, so drain into a new queue or into a reusable `Queue<T>` swap field. Pending dequeuers are untouched.
- `dp:Internal/BatchHandler.cs`: `public int ClearQueuedMessages()` (clears `_messages`, **not** `_batches`; returns the count) and `public int RemoveQueuedMessages(HashSet<(ulong, ulong)> entries)`. The latter filters `_messages` by `(MessageId.LedgerId, MessageId.EntryId)` and returns the count. `_batches` is kept so acks of already-delivered siblings still resolve. A re-delivered entry adds a second `Batch` for the same entry, so batching.md's rewrite must de-duplicate on `Add` (OD-6). These two methods must survive batching.md's rewrite.

### 4.5 `dp:Internal/ConsumerChannelFactory.cs#Create` (L54-L61) and `dp:Internal/ChannelManager.cs`, `dp:Internal/MessagePackage.cs`
- `MessagePackage` (L20-L32): add `public long ConsumerEpoch { get; }` as the 4th ctor parameter.
- `ChannelManager.Incoming(CommandMessage, …)` (L196-L197): `new MessagePackage(command.MessageId, command.RedeliveryCount, data, command.HasConsumerEpoch ? (long) command.ConsumerEpoch : -1)` (Java L1541-L1545). Update every other `new MessagePackage(` call site; grep shows only ChannelManager in `src`. Tests that construct `MessagePackage` get `-1`.
- `Create`:
  1. `var subscribeEpoch = _consumerEpoch.Value; _subscribe.ConsumerEpoch = (ulong) subscribeEpoch;` before `connection.Send(_subscribe, …)`. Java always sends the epoch because it is `> -1` (Commands L574-L577; ConsumerImpl L979-L985).
  2. After the subscribe response, if `_consumerEpoch.Value != subscribeEpoch` (a redeliver-all raced the subscribe): `await connection.Send(new CommandRedeliverUnacknowledgedMessages { ConsumerId = response.ConsumerId, ConsumerEpoch = (ulong) _consumerEpoch.Value }, cancellationToken)` before returning the channel. That is before any `CommandFlow`, because DotPulsar sends the first flow lazily on the first `Receive`. This is Java L1001-L1008.
  3. Pass `_unAckedMessageTracker`, `_consumerEpoch` and `filterByConsumerEpoch: _subscribe.SubType is CommandSubscribe.Types.SubType.Exclusive or CommandSubscribe.Types.SubType.Failover` to the `ConsumerChannel` ctor.
- The tracker is **not** cleared on reconnect or on `Seek`. Java clears only on redeliver-all (L2529) and close; seek clears the queue but not the tracker (L2920).

### 4.6 Redelivery primitive (WP-AT-B): `dp:Internal/SubConsumer.cs`
Replace L98-L106 with:

`public async ValueTask RedeliverUnacknowledgedMessages(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken)` calls `Guard()` and then follows these steps, in order:
0. Materialise `messageIds` once into a reusable list (call it `entries`). If it is empty, **return** (Java L2555-L2557). This check comes before the subscription-type fallback, so `RedeliverUnacknowledgedMessages([])` is a no-op for every type. That is a behaviour change: today an empty enumerable means "redeliver all" (L105-L106).
1. Subscription-type fallback: if `SubscriptionType` is neither `Shared` nor `KeyShared`, `await RedeliverUnacknowledgedMessages(cancellationToken)` (redeliver-all, below) and return (Java L2559-L2564).
2. Expand chunked ids: for each id with `ChunkMessageIds is { Length: > 1 }`, use every chunk id instead (chunking.md §6.5; C++ L1709-L1716; Java L158-L160).
3. Convert to entry level and de-duplicate by `(LedgerId, EntryId)`, keeping the first occurrence and its `Partition`.
4. DLQ filter (dead-letter-policy.md §4.7) removes entries that were dead-lettered. That work runs outside `_executor`.
5. Send in chunks of 1000 (`private const int MaxRedeliverUnacknowledged = 1000;`, Java L148, L2568). Each chunk becomes `new CommandRedeliverUnacknowledgedMessages()` with `MessageIds` = `new MessageIdData { LedgerId, EntryId, Partition }`. There is **no** `BatchIndex` (Java L2619-L2622). Send each via `await _executor.Execute(() => InternalRedeliverUnacknowledgedMessages(command, cancellationToken), cancellationToken)`. The channel purges the matching prefetched entries and refunds permits (§4.4c).

Redeliver-all, `public async ValueTask RedeliverUnacknowledgedMessages(CancellationToken cancellationToken)`:
1. `Guard();`
2. If `SubscriptionType is SubscriptionType.Exclusive or SubscriptionType.Failover`, call `_consumerEpoch.Increment()` (Java L2522-L2525).
3. `_unAckedMessageTracker?.Clear();` (Java L2529).
4. `var command = new CommandRedeliverUnacknowledgedMessages { ConsumerEpoch = (ulong) _consumerEpoch.Value };` (Java L2537-L2538 sends the epoch for every subscription type), then `await _executor.Execute(() => InternalRedeliverUnacknowledgedMessages(command, cancellationToken), cancellationToken)`. The channel purges everything and refunds (§4.4c). If the channel is not ready, `_executor` retries until the next channel exists (DotPulsar semantics). The new channel subscribes with the bumped epoch (§4.5), so the broker resets the dispatch, which is equivalent to Java's "the reconnection sends the new epoch" comment (L2535).

`dp:Internal/Consumer.cs#RedeliverUnacknowledgedMessages(IEnumerable<MessageId>, ct)` (L302-L319) is unchanged: it groups by topic and delegates. Because of step 0, an empty set is a no-op. `RedeliverUnacknowledgedMessages(ct)` (L321-L337) is unchanged.

Callers of the selective path:
- the tracker's redeliver delegate (§4.1);
- user calls;
- dead-letter-policy.md (`RedeliveryCountExceeded`, the negative-ack tracker).

The tracker delegate passes the tracker's `_cts.Token`, so disposal cancels a redeliver that is waiting for reconnection. Java drops the redelivery when disconnected (L2583-L2588). DotPulsar waits for the next channel instead, which is harmless because the broker would redeliver anyway (OD-5).

### 4.7 Files touched (summary)
| File | Change |
|---|---|
| `dp:ConsumerOptions.cs` | 2 defaults, 2 properties, ctor init |
| `dp:Abstractions/IConsumerBuilder.cs` | 2 members |
| `dp:Internal/ConsumerBuilder.cs` | 2 fields, 2 setters, `Create` init |
| `dp:PulsarClient.cs` | validation in `CreateConsumer` |
| `dp:Internal/UnAckedMessageTracker.cs` | **new** |
| `dp:Internal/ConsumerEpoch.cs` | **new** |
| `dp:Internal/Consumer.cs` | wiring in `CreateSubConsumer`, `Track` at the 3 `Receive` return points |
| `dp:Internal/SubConsumer.cs` | ctor params, `Track`, `UnAckedMessageCount`, dispose, redelivery primitive |
| `dp:Internal/ConsumerChannelFactory.cs` | ctor params, subscribe epoch, post-subscribe epoch resend |
| `dp:Internal/ConsumerChannel.cs` | ctor params, epoch filter, ack removal helper, purge plus refund |
| `dp:Internal/MessagePackage.cs`, `dp:Internal/ChannelManager.cs` | `ConsumerEpoch` |
| `dp:Internal/AsyncQueue.cs` | `RemoveAll`, `RemoveWhere` |
| `dp:Internal/BatchHandler.cs` | `ClearQueuedMessages`, `RemoveQueuedMessages` |
| `tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` etc. | update ctor calls (new params) |

`dp:Internal/Abstractions/IConsumerChannel.cs` needs no change: the signatures stay the same.

## 5. Interactions
- Batching (batching.md): the tracker is entry-level. A partial batch-index ack does not untrack the entry; the last index does. A cumulative ack of `(L, E, k)` untracks the whole entry `(L, E)`, which is Java behaviour. The entry stays tracked across partial acks and, on expiry, the **whole entry** is redelivered. The broker applies the stored ack set, so with batch-index ack enabled only the unacked indexes come back (`acknowledgmentAtBatchIndexLevelEnabled=true` in `TransactionFixture`).
- Chunking: the tracked id is the assembled chunk `MessageId`; on expiry all chunk entries are redelivered (§4.6 step 2). Orphan or expired chunk ids from `ChunkedMessageAssembler` go through `Add(MessageId)` with the plain chunk id (chunking.md `idsToTrack`).
- DLQ: on expiry the selective path runs the DLQ filter. For Exclusive/Failover there is no DLQ attempt, because the redeliver-all fallback happens first, exactly as Java's fallback precedes `getRedeliveryMessageIdData` (L2559-L2569).
- Negative ack: removes from the tracker (dead-letter-policy.md WP-B).
- Encryption: the FAIL action adds the entry id without ever handing the message to the application (Java L2378-L2381). The message is therefore redelivered after the timeout, which is the only way a FAIL-ed message comes back.
- Transactions: see §4.4(a). `TransactionEndToEndTest#txnCumulativeAckTest` depends on:
  1. cumulative txn ack calling `RemoveMessagesTill`;
  2. `RedeliverUnacknowledgedMessages()` (Failover) bumping the epoch, purging the prefetched copies and clearing the tracker, so that exactly `messageCnt` (1000) messages are received again and then `receive(waitTimeForCannotReceiveMsgInSec)` returns `null`;
  3. a 1-minute timeout never firing during the test.

## 6. Behaviour changes to existing public API (document in CHANGELOG)
1. `IConsumer.RedeliverUnacknowledgedMessages(IEnumerable<MessageId>)` with an empty sequence is now a no-op. Before, it meant "redeliver all".
2. On Exclusive/Failover subscriptions, `RedeliverUnacknowledgedMessages(ids)` now redelivers **all** unacknowledged messages, as in Java. The broker ignores per-id redelivery for those dispatchers anyway.
3. `RedeliverUnacknowledgedMessages()` now drops locally prefetched messages, so they are not received twice, and on Exclusive/Failover uses the consumer epoch to drop in-flight stale messages.
4. Ids are sent at entry level, without the batch index.

## 7. Tests to add

Conventions follow the spec decisions: `// Upstream: <JavaClass>#<method>` above the attribute, `//Arrange` `//Act` `//Assert`, Shouldly.
- Unit tests go in `tests/DotPulsar.Tests/Internal/UnAckedMessageTrackerTests.cs` with `[Trait("Category", "Unit")]`.
- Integration tests go in `tests/DotPulsar.Tests/Internal/AckTimeoutTests.cs` with `[Collection("Integration"), Trait("Category", "Integration")]` and `IntegrationFixture`. They are not transaction tests, so they do not use `TransactionFixture`.

Mappings:
- `receiverQueueSize(n)` becomes `MessagePrefetchCount(n)`.
- `consumer.receive(x, ms)` returning null becomes `ReceiveOrNull(consumer, TimeSpan.FromMilliseconds(x), ct)`.
- `((ConsumerImpl) consumer).getUnAckedMessageTracker().size()` becomes `((Consumer<T>) consumer).UnAckedMessageCount`. That property is a new `public int UnAckedMessageCount` on `Consumer<TMessage>` summing `SubConsumer.UnAckedMessageCount` (OD-7). `PulsarClient.CreateConsumer` returns the concrete `Consumer<TMessage>` (`PulsarClient.cs` L93), so the cast is valid.
- `Thread.sleep(x)` becomes `await Task.Delay(x, ct)`.
- `enableBatching(false)` is the DotPulsar default today. `acknowledgmentGroupTime(0)` is N/A because DotPulsar acks immediately.

### 7.1 Unit tests (port of `UnAckedMessageTrackerTest` plus DotPulsar-specific)
| Upstream | Class | C# test |
|---|---|---|
| `UnAckedMessageTrackerTest#testAddAndRemove` (L44-L83) | portable. The Java manual head rotation (L73-L75) becomes `tracker.Tick().ShouldBeNull()`: the head is empty and the id stays tracked. Values: ackTimeout 1,000,000 ms, tick 100,000 ms, `Dispose()` first as Java `close()` (L63), id `(1, 1, -1)`. | `Add_GivenSameMessageIdTwice_ShouldTrackItOnceAndRemoveIt` |
| `UnAckedMessageTrackerTest#testTrackChunkedMessageId` (L85-L129) | adapted (no `unAckedChunkedMessageIdSequenceMap`; chunk ids on `MessageId`, depends on chunking.md). ackTimeout 1000 ms, tick 1000 ms, 5 chunk ids `(1, i, -1)`, `i = 0..4`. Assert that two `Tick()` calls return the chunked id, `Count == 0`, and that the selective-redelivery expansion helper (§4.6 step 2, extracted as `public static` method `SubConsumer<T>.ExpandAndNormalize(IEnumerable<MessageId>, List<MessageIdData>)`) yields exactly the 5 entries | `Tick_GivenChunkedMessageIdExpired_ShouldRedeliverAllChunks` |
| — | new: expiry window. ackTimeout 2 s, tick 1 s gives 3 buckets: `Add`; `Tick()` x2 returns null; the 3rd `Tick()` returns the id | `Tick_GivenAckTimeoutTwiceTickTime_ShouldExpireOnThirdTick` |
| — | new: `tick > ackTimeout` is clamped. ackTimeout 1 s, tick 5 s gives 2 buckets | `Constructor_GivenTickTimeGreaterThanAckTimeout_ShouldUseAckTimeoutAsTick` |
| — | new: batch ids collapse. `Add((1,1,-1,0))` true, `Add((1,1,-1,1))` false, `Remove((1,1,-1,-1))` true | `Add_GivenBatchMessageIdsOfSameEntry_ShouldTrackEntryOnce` |
| — | new: `RemoveMessagesTill((1,5,-1,2))` removes entries `(1,1..5)` and keeps `(1,6)`, `(2,0)` | `RemoveMessagesTill_GivenBatchMessageId_ShouldRemoveEntriesUpToAndIncludingItsEntry` |
| — | new: `Clear` empties all buckets | `Clear_GivenTrackedMessages_ShouldBeEmpty` |
| — | new: a re-`Add` after expiry goes to the newest bucket | `Add_GivenExpiredMessageReAdded_ShouldTrackInNewestBucket` |
| — | new: timer path with `Start()`, ackTimeout 1 s, tick 100 ms; the redeliver delegate is called once within 2 s with the id | `Start_GivenUnacknowledgedMessage_ShouldInvokeRedeliverAfterTimeout` |
| — | new: `Dispose` cancels the timer; the delegate is not called after dispose | `Dispose_GivenStartedTracker_ShouldNotRedeliver` |
| `UnAcknowledgedMessagesTimeoutTest#testAckTimeoutMinValue` (L403-L418) | adapted. Java throws in the builder setter; DotPulsar throws `ConfigurationException` from `Create()` (§2.4). Values: 999 ms. The second half (backoff multiplier) is unreachable in Java after `fail()` and is not ported | `Create_GivenAckTimeoutBelowOneSecond_ShouldThrowConfigurationException` (unit, `PulsarClient` with a substituted `IConnectionPool` is not needed, because validation precedes construction; use `PulsarClient.Builder().ServiceUrl(...).Build()`, which does not connect until a consumer is created) |
| — | new: tick 99 ms gives `ConfigurationException` | `Create_GivenAckTimeoutTickTimeBelow100Milliseconds_ShouldThrowConfigurationException` |
| — | new, `ConsumerChannelTests`: purge refund. Queue 3 packages, then `Send(CommandRedeliverUnacknowledgedMessages{})`; the connection receives the redeliver command and then `CommandFlow{MessagePermits=3}` (NSubstitute `IConnection`) | `Send_GivenRedeliverAllWithPrefetchedMessages_ShouldPurgeQueueAndRefundPermits` |
| — | new, `ConsumerChannelTests`: selective purge removes only matching entries | `Send_GivenSelectiveRedeliver_ShouldPurgeOnlyMatchingEntries` |
| — | new, `ConsumerChannelTests`: epoch filter. Failover channel, epoch 1, package with epoch 0 is skipped and the package with epoch 1 is returned; package `-1` is returned | `Receive_GivenStaleConsumerEpoch_ShouldSkipMessage` |
| — | new, `ConsumerChannelTests`: cumulative ack removes from the tracker even when the batch is partial | `Send_GivenCumulativeAckOfPartialBatch_ShouldRemoveEntriesFromTracker` |
| `UnAckedTopicMessageRedeliveryTrackerTest` (all) | not portable (`RedeliveryBackoff`, OD-1) | — |

### 7.2 Integration tests
| Upstream | Class | C# test |
|---|---|---|
| `UnAcknowledgedMessagesTimeoutTest#testExclusiveSingleAckedNormalTopic` row `{false}` (L56-L120) | portable. ackTimeout 2 s, prefetch 7, 10 messages `"my-message-" + i`, 500 ms receive waits, asserts tracker 5 then 0, distinct set size 10 | `Receive_GivenExclusiveSubscriptionAndUnacknowledgedMessages_ShouldRedeliverAfterAckTimeout` |
| same, row `{true}` | not portable (`ackTimeoutRedeliveryBackoff`, OD-1) | — |
| `#testExclusiveCumulativeAckedNormalTopic` `{false}` (L122-L175) | portable. Tracker 10, `AcknowledgeCumulative(last)`, tracker 0, `ReceiveOrNull(4 s)` is null | `AcknowledgeCumulative_GivenAckTimeout_ShouldUntrackAllAndNotRedeliver` |
| `{true}` | not portable | — |
| `#testSharedSingleAckedPartitionedTopic` `{false}` (L177-L280) | portable. 3 partitions (`fixture.CreatePartitionedTopic`), round-robin router (DotPulsar default), prefetch 100, Shared, names `"Consumer-1"`/`"Consumer-2"`, 20 messages, 1 s receive waits, `Task.Delay(2200)` then `Task.Delay(4000)`, same three assertions | `Receive_GivenSharedPartitionedTopicAndOneConsumerNotAcknowledging_ShouldRedeliverAfterAckTimeout` |
| `{true}` | not portable | — |
| `#testFailoverSingleAckedPartitionedTopic` `{false}` (L282-L401) | portable. The upstream test actually uses `SubscriptionType.Shared`; reproduce exactly. Prefetch 7, 10 messages, 500 ms waits, `Task.Delay(4000)`, `messagesReceived + ackCount == 10` | `Receive_GivenTwoSharedConsumersOnPartitionedTopicOneNotAcknowledging_ShouldRedeliverOnlyUnacknowledged` |
| `{true}` | not portable | — |
| `#testCheckUnAcknowledgedMessageTimer` (L420-L461) | portable. Shared, prefetch 7, 3 messages, tracker 1, ack, tracker 0, `Task.Delay(2200)`, tracker 0 | `Acknowledge_GivenAllMessagesAcknowledged_ShouldLeaveTrackerEmptyAfterTimeout` |
| `#testCheckUnAcknowledgedMessageRedeliveryTimer` (L463-L530) | not portable (backoff) | — |
| `#testSingleMessageBatch` (L532-L558, first half) | portable after batching.md (producer batching, `batchingMaxPublishDelay(10 s)`, `flush`). Tracker not empty after receive, empty after ack. The second half (L560-L580, backoff) is not ported | `Acknowledge_GivenSingleMessageBatch_ShouldUntrackEntry` |
| `PerMessageUnAcknowledgedRedeliveryTest#testSharedAckedNormalTopic` (L39-L135) | portable. ackTimeout 2 s, prefetch 50, Shared, 15/3 = 5 per phase, 100 ms waits, two `Task.Delay(2000)`, asserts 5, 5/5, 10, redelivered 5, 5 | `Receive_GivenSharedSubscriptionAndPartiallyAcknowledgedMessages_ShouldRedeliverOnlyExpired` |
| `#testUnAckedMessageTrackerSize` (L137-L177) | adapted. There is no tracker object when disabled; assert `UnAckedMessageCount == 0`. The `add(null)`/`remove(null)`/`removeMessagesTill(null)` asserts target the Java disabled tracker and are not ported | `Receive_GivenAckTimeoutDisabled_ShouldNotTrackMessages` |
| `#testExclusiveAckedNormalTopic` (L179-L275) | portable. Redelivered 10, tracker 0 (redeliver-all) | `Receive_GivenExclusiveSubscriptionAndPartiallyAcknowledgedMessages_ShouldRedeliverAllUnacknowledged` |
| `#testFailoverAckedNormalTopic` (L277-L373) | portable. Failover, redelivered 10, tracker 0 | `Receive_GivenFailoverSubscriptionAndPartiallyAcknowledgedMessages_ShouldRedeliverAllUnacknowledged` |
| `#testSharedAckedPartitionedTopic` (L381-L479) | portable. 3 partitions; redelivered 5, tracker 5 (sum over sub-consumers) | `Receive_GivenSharedPartitionedTopicAndPartiallyAcknowledgedMessages_ShouldRedeliverOnlyExpired` |
| `ConsumerRedeliveryTest#testUnAckMessageRedeliveryWithReceiveAsync` `{false}`, `{true}` (L394-L443) | adapted. 10 concurrent `Receive` tasks stand in for `receiveAsync`. Depends on batching.md (producer batching 5 / 1 s; `{true}` = ack receipt plus batch-index ack). ackTimeout 3 s, Exclusive (default), 20 total received | `Receive_GivenUnacknowledgedBatchedMessages_ShouldRedeliverAfterAckTimeout` with `[Theory][InlineData(false)][InlineData(true)]` |
| `ConsumerRedeliveryTest#testMessageRedeliveryWhenTimeoutInListener` (L491-L544) | adapted. The listener becomes a single background receive loop that, for the first `"Hello-0"`, does `Task.Delay(3000)` and then acks. Shared, ackTimeout 1 s, 10 messages, expect 11 received, two `"Hello-0"` with `RedeliveryCount` 0 and 1. Uses `string` schema (`Schema.String`) on the producer, as upstream | `Receive_GivenSlowProcessingBeyondAckTimeout_ShouldRedeliverOnlyThatMessage` |
| `TransactionEndToEndTest#txnAckTestBatchAndCumulativeSub` / `#txnAckTestNoBatchAndCumulativeSub` | ported by the transaction tests spec (tests-e2e); listed here because they are the acceptance gate for WP-AT-B | — |

C++ `tests/ConsumerTest.cc` ack-timeout cases duplicate the Java cases above; they are not ported separately.

## 8. Acceptance criteria
1. With `AckTimeout = 0` (default) no `UnAckedMessageTracker` or `Timer` is created, and the existing consumer behaviour and all existing tests are unchanged, except for the documented redeliver changes in §6.
2. An `AckTimeout` greater than 0 and less than 1 s, or an `AckTimeoutTickTime` less than 100 ms, makes `CreateConsumer` / `IConsumerBuilder.Create` throw `ConfigurationException`.
3. A received, unacked message on a persistent topic is redelivered in `(AckTimeout, AckTimeout + tick]` after it was handed to the application. Ack (individual of a non-batched or completed batch, cumulative, or transactional at send time) or negative ack before then prevents it.
4. On Exclusive/Failover, the expiry or a user `RedeliverUnacknowledgedMessages()` clears the tracker, purges prefetched messages, bumps the epoch and drops stale in-flight messages. All §7.2 Exclusive/Failover tests pass with exact counts, i.e. no duplicates.
5. On Shared/KeyShared, the expiry redelivers only the expired entries, in chunks of at most 1000 ids, without batch index, after the DLQ filter. Prefetched copies are purged and their permits refunded, so the consumer never stalls.
6. Non-persistent topics never track.
7. All §7.1 unit tests and §7.2 portable integration tests pass; `TransactionEndToEndTest#txnAck*CumulativeSub` passes once the transaction core lands.
8. No allocation on the receive/ack hot path when ack timeout is disabled (one null check). When enabled, `Add` and `Remove` are allocation-free in steady state.

## Open decisions (recommendation)
- **OD-1 Ack-timeout redelivery backoff** (`ackTimeoutRedeliveryBackoff`, `UnAckedMessageRedeliveryTracker` / `UnAckedTopicMessageRedeliveryTracker`, `MultiplierRedeliveryBackoff`). No transaction test uses it; it only affects the `{true}` rows above and `testCheckUnAcknowledgedMessageRedeliveryTimer`. **Recommend: defer.** Keep `UnAckedMessageTracker` sealed; a later WP can add the redelivery variant.
- **OD-2 Batch-indexed transactional individual ack.** Java calls `unAckedMessageTracker.remove(batchMessageId)`, which never matches the entry-level key (L3573 / L3660), so the entry stays tracked until expiry. **Recommend: remove the entry when the transactional ack completes the entry per batching.md bookkeeping, the same rule as a non-transactional ack.** This divergence from Java avoids a pointless redelivery request that the broker filters through pending-ack. No upstream test observes the difference.
- **OD-3 Tracker granularity.** **Recommend one tracker (and one `Timer`) per `SubConsumer`.** Siblings (dead-letter-policy.md, encryption.md, chunking.md) already assume a per-sub-consumer tracker, and redelivery is per sub-consumer anyway. Java uses one tracker per `MultiTopicsConsumerImpl`; the observable sums are equal. A shared timer wheel can come later if profiling shows timer overhead with very many partitions.
- **OD-4 Metrics.** The Java counter `pulsar.client.consumer.message.ack.timeout` (L125-L129). **Recommend: defer.** Optionally add `DotPulsarMeter.MessageAckTimeout(count, tags)` later.
- **OD-5 Redelivery while disconnected.** Java drops the request with a warning. With DotPulsar's `_executor` the request waits for the next channel and is cancelled on dispose. **Recommend the DotPulsar behaviour** (harmless: after reconnect the broker redelivers every unacked message anyway, and the extra request is idempotent).
- **OD-6 `BatchHandler._batches` duplicates after redelivery.** Redeliver-all purges queued messages but keeps batch ack bookkeeping. A redelivered entry adds a second `Batch` for the same entry. **Recommend that batching.md's `BatchHandler` rewrite de-duplicate on `Add`** (reuse the existing `Batch` for the same ledger/entry).
- **OD-7 Test hook.** `Consumer<TMessage>.UnAckedMessageCount` and `SubConsumer<TMessage>.UnAckedMessageCount` are public members on public types in `DotPulsar.Internal` (repo convention, no `InternalsVisibleTo`). **Recommend accepting this** rather than adding `InternalsVisibleTo`.
- **OD-8 Validation location.** Java throws `IllegalArgumentException` from the builder setter. **Recommend `ConfigurationException` from `PulsarClient.CreateConsumer`.** It covers both the builder and `ConsumerOptions`, and follows the `MaxPendingMessages` precedent at `PulsarClient.cs` L63-L64.
- **OD-9 Ownership of the epoch and purge work (WP-AT-B).** These are general consumer correctness fixes. **Recommend landing WP-AT-B first, as its own PR**, because dead-letter-policy.md, chunking.md and the transaction tests depend on it even without ack timeout enabled.
