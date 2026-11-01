# Prerequisite work package: Dead letter policy (DLQ) + negative acknowledgement

Status: DRAFT spec (no implementation). Target repo: DotPulsar @ e8f2ba22c7136a35d279451e7278589fb0084d2e.
Upstream pins: Java apache/pulsar @ f3846f4811b6a7d98de40a3be2caff1fe288e026, C++ apache/pulsar-client-cpp @ f71d7e99019d58b1d11858f05ff0dc83b718627b.

Sibling specs referenced (not redefined here):
- `ack-timeout.md` — defines internal `UnAckedMessageTracker` (`src/DotPulsar/Internal/`), `ConsumerOptions.AckTimeout` (`TimeSpan`, default `TimeSpan.Zero` = disabled), `IConsumerBuilder.AckTimeout(TimeSpan)`, removal from the tracker on ack, and the shared "redeliver" primitive (removing prefetched copies of redelivered entries from the local queue, redeliver-all for Exclusive/Failover, consumer epoch). DLQ is **triggered** by ack-timeout redelivery in most upstream DLQ tests.
- `batching.md` (agent prereq-batching) — producer batching, batch-index ack / `ack_set`, ack receipt, send timeout, rewrite of `BatchHandler`. DLQ for batched entries and "already acked batch indexes are not dead-lettered" depend on it.
- `chunking.md` — producer chunking. The DLQ producer is created with chunking enabled (Java parity); until chunking lands, a DLQ message larger than `MaxMessageSize` fails to dead-letter and is redelivered instead.

---

## 1. Scope decision (stated clearly)

| Item | In scope? | Reason |
|---|---|---|
| `DeadLetterPolicy` (MaxRedeliverCount, DeadLetterTopic, InitialSubscriptionName) + consumer-side DLQ handling | **YES (WP-DLQ-A, required)** | `TransactionEndToEndTest#testSendTxnAckMessageToDLQ` (L1545-1606) and `#testSendTxnAckBatchMessageToDLQ` (L1608-1683) need it (also inherited by `TransactionEndToEndWithoutBatchIndexAckTest`). |
| Negative acknowledgement (`NegativeAcksTracker`) | **YES (WP-DLQ-B, separately mergeable, recommended)** | NOT needed by any transaction test (verified: no `negativeAck` in any txn test; the txn DLQ tests are driven by **broker-side redelivery after `abort()`**, redeliveryCount comes from the broker). Included because 1 DeadLetterTopicTest test, the NegativeAcksTest suite and every cpp DeadLetterQueueTest TEST_P trigger DLQ via nack, and it is ~1 small class. WP-DLQ-A must not depend on it. |
| Retry letter topic (`retryLetterTopic`, `enableRetry`, `reconsumeLater`, `RECONSUMETIMES`/`DELAY_TIME`) | **NO** (open decision OD-1) | No portable transaction test needs it. `RetryTopicTest` and `ConsumerRedeliveryTest#testRedeliverMessagesWithoutValue` are classified not-portable. |
| `DeadLetterProducerBuilderCustomizer` (dead/retry) | **NO** (OD-2) | Only used by `testDeadLetterTopicWithProducerBuilder` / retry tests. |
| `RedeliveryBackoff` / `negativeAckRedeliveryBackoff`, `negativeAckRedeliveryDelayPrecision` | **NO** (OD-3) | Not used by any portable test that needs nack. Internal precision fixed at Java default 8 bits. |
| Multi-topic `TopicMessageId` REAL_TOPIC partition-suffix stripping | N/A | In Java DLQ messages always come from `ConsumerImpl` (per-partition), so REAL_TOPIC is the partition topic; DotPulsar does the same per `SubConsumer` (see 4.6). |

Important behavioural fact (document in XML docs): the DLQ only ever triggers on **Shared** and **Key_Shared** subscriptions. Reasons: (1) the broker only tracks `redelivery_count` for multi-consumer dispatchers (single-active-consumer dispatchers use `RedeliveryTrackerDisabled`, count is always 0); (2) Java `ConsumerImpl#redeliverUnacknowledgedMessages(Set)` (L2554-2590) falls back to redeliver-all for non-Shared types, bypassing `processPossibleToDLQ`. Java does not reject the policy for other types; DotPulsar must not either.

---

## 2. Upstream mapping

| Concern | Java (path#symbol Lx-Ly) | C++ (path#symbol Lx-Ly) | DotPulsar target |
|---|---|---|---|
| Policy type | `java:pulsar-client-api/src/main/java/org/apache/pulsar/client/api/DeadLetterPolicy.java#DeadLetterPolicy (L40-85)` (fields `maxRedeliverCount` L46, `retryLetterTopic` L51, `deadLetterTopic` L56, `initialSubscriptionName` L64, customizers L74/L84) | `cpp:include/pulsar/DeadLetterPolicy.h#DeadLetterPolicy (L36-66)`, `cpp:lib/DeadLetterPolicyImpl.h (L27-29)` (default `maxRedeliverCount{INT_MAX}`), `cpp:lib/DeadLetterPolicyBuilder.cc#build (L48-55)` | new `src/DotPulsar/DeadLetterPolicy.cs` |
| Validation `maxRedeliverCount > 0` | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerBuilderImpl.java#deadLetterPolicy (L527-533)` message `"MaxRedeliverCount must be > 0."` | `cpp:lib/DeadLetterPolicyBuilder.cc#build (L49-50)` same message | `ConsumerBuilder.Create()` + `PulsarClient.CreateConsumer` -> `ConfigurationException` |
| Default DLQ topic `<topic>-<sub>-DLQ` | `java:pulsar-client/src/main/java/org/apache/pulsar/client/util/RetryMessageUtil.java#getDLQTopic (L39-41)`, suffix L33; applied in `java:.../impl/ConsumerImpl.java#<init> (L436-465)` with the **per-partition** `topic` | `cpp:lib/ConsumerImpl.cc#ConsumerImpl (L166-178)`, `cpp:lib/ConsumerImpl.h (L77-79)` | `DeadLetterHandler<TMessage>` ctor, using `SubConsumer.Topic` |
| Config docs | `java:.../impl/conf/ConsumerConfigurationData.java#deadLetterPolicy (L347-378)`; javadoc `java:pulsar-client-api/.../ConsumerBuilder.java#deadLetterPolicy (L644-~680)` | `cpp:include/pulsar/ConsumerConfiguration.h#setDeadLetterPolicy (L450-477)` | XML docs below |
| Register DLQ candidate, non-batched entry | `java:.../impl/ConsumerImpl.java#messageReceived (L1717-1732)` | `cpp:lib/ConsumerImpl.cc (L799-806)` | `ConsumerChannel.Receive` non-batch branch |
| Register DLQ candidate, batched entry | `java:.../impl/ConsumerImpl.java#receiveIndividualMessagesFromBatch (L2017-2098)`: L2026-2029 (create list iff `redeliveryCount >= max`), L2056-2064 (add each delivered message, skip if `> max`), L2081-2089 (put list, redeliver entry if `> max`) | `cpp:lib/ConsumerImpl.cc (L946-996)` | `BatchHandler.Add` |
| Redelivery entry point (DLQ hook) | `java:.../impl/ConsumerImpl.java#redeliverUnacknowledgedMessages(Set) (L2554-2590)`, `#getRedeliveryMessageIdData (L2610-2630)` (ids sent entry-level: partition, ledger, entry — no batch index), `MAX_REDELIVER_UNACKNOWLEDGED = 1000` (L148) | `cpp:lib/ConsumerImpl.cc#redeliverUnacknowledgedMessages (L1672-1703)`, `#redeliverMessages (L1705-1728)` | `SubConsumer.RedeliverUnacknowledgedMessages(IEnumerable<MessageId>, ct)` |
| Send to DLQ + ack original | `java:.../impl/ConsumerImpl.java#processPossibleToDLQ (L2632-2708)` | `cpp:lib/ConsumerImpl.cc#processPossibleToDLQ (L2091-2197)` | `DeadLetterHandler.TryDeadLetter` |
| DLQ producer creation | `java:.../impl/ConsumerImpl.java#initDeadLetterProducerIfNeeded (L2721-2767)` (name `"%s-%s-%s-%s-DLQ"` = topicName, subscription, consumerName, 5 random alphanumerics; `initialSubscriptionName`, `blockIfQueueFull(false)`, `enableBatching(false)`, `enableChunking(true)`, schema `AUTO_PRODUCE_BYTES(consumer schema)` with validation off), `#createProducerWithBackOff (L2769-2794)` | `cpp:lib/ConsumerImpl.cc (L2098-2128)` | `DeadLetterHandler.GetProducer` |
| DLQ message content | `java:.../impl/ConsumerImpl.java#getPropertiesMap (L842-855)`, `#getOriginTopicNameStr (L857-870)`, `#copyMessageKeysIfNeeded (L700-711)`, `#copyMessageEventTime (L881-884)`; property names `java:.../client/util/RetryMessageUtil.java (L25-30)` | `cpp:lib/ConsumerImpl.cc (L2139-2151)` (only ORIGIN_MESSAGE_ID + REAL_TOPIC, keys; no event time, no REAL_SUBSCRIPTION) | `DeadLetterHandler.CreateMetadata` (Java parity) |
| Remove candidate on (full-entry) individual ack | `java:.../impl/PersistentAcknowledgmentsGroupingTracker.java#addIndividualAcknowledgment (L212-232)` (L220-222) | `cpp:lib/ConsumerImpl.cc (L1500-1512)` | `ConsumerChannel.Send(CommandAck)` |
| Clear candidates on reconnect / close | `java:.../impl/ConsumerImpl.java#connectionOpened (L941-943)`, `#closeAsync/cleanup (L1260-1275, L1352-1359)` | `cpp:lib/ConsumerImpl.cc (L338, L1618)` | `ConsumerChannelFactory.Create`, `SubConsumer.DisposeAsync` |
| Negative ack API | `java:.../impl/ConsumerImpl.java#negativeAcknowledge(MessageId) (L886-893)`, `#negativeAcknowledge(Message) (L895-902)` (both remove `discardBatch(id)` from `UnAckedMessageTracker`) | `cpp:lib/ConsumerImpl.cc#negativeAcknowledge (L1537-1549)` | `IConsumer.NegativeAcknowledge`, `SubConsumer` |
| Negative ack tracker | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/NegativeAcksTracker.java (L44-231)`: `MIN_NACK_DELAY_MS = 100` (L62), ctor (L65-72), `triggerRedelivery` (L74-145), `add` (L159-190), `trimLowerBit` (L155-157), `discardBatchAndPartitionIndex` (L198-204), `close` (L217-230); default delay `ConsumerConfigurationData#negativeAckRedeliveryDelayMicros = 1 min` (L156-162), `negativeAckPrecisionBitCnt = 8` (L165-172) | `cpp:lib/NegativeAcksTracker.cc (L40-151)` (`MIN_NACK_DELAY_MILLIS = 100` L44, `add` L121-134) | new `src/DotPulsar/Internal/NegativeAcknowledgeTracker.cs` |
| Prefetch-queue purge on selective redelivery | `java:.../impl/ConsumerImpl.java#removeExpiredMessagesFromQueue (L3293-3307)` | — | owned by `ack-timeout.md` |

---

## 3. Public API additions

### 3.1 `src/DotPulsar/DeadLetterPolicy.cs` (new)

```csharp
namespace DotPulsar;

/// <summary>
/// The dead letter policy for a consumer.
/// </summary>
/// <remarks>
/// When a message has been redelivered more than 'MaxRedeliverCount' times, it is sent to the dead letter topic and acknowledged automatically.
/// The redelivery count is only tracked by the broker for 'Shared' and 'KeyShared' subscriptions, so the policy only has an effect for these subscription types.
/// </remarks>
public sealed class DeadLetterPolicy
{
    /// <summary>
    /// Initializes a new instance using the specified max redeliver count.
    /// </summary>
    public DeadLetterPolicy(uint maxRedeliverCount)
    {
        MaxRedeliverCount = maxRedeliverCount;
    }

    /// <summary>
    /// The maximum number of times a message will be redelivered before being sent to the dead letter topic. Must be greater than 0.
    /// </summary>
    public uint MaxRedeliverCount { get; set; }

    /// <summary>
    /// The name of the dead letter topic. The default is '{TopicName}-{SubscriptionName}-DLQ'.
    /// </summary>
    /// <remarks>
    /// For a partitioned topic the default is computed per partition, e.g. 'persistent://public/default/my-topic-partition-0-my-subscription-DLQ'.
    /// </remarks>
    public string? DeadLetterTopic { get; set; }

    /// <summary>
    /// The name of the subscription to create on the dead letter topic when the dead letter producer is created. This is optional.
    /// </summary>
    /// <remarks>
    /// If this is set and the broker does not allow automatic subscription creation, the dead letter producer will fail to be created.
    /// </remarks>
    public string? InitialSubscriptionName { get; set; }
}
```
Notes: `uint` matches DotPulsar's `IMessage.RedeliveryCount` (`uint`). Java `int`/C++ `INT_MAX` default "disabled" is represented by `ConsumerOptions.DeadLetterPolicy == null`. Settable properties (not `init`) because the repo targets netstandard2.0 without an `IsExternalInit` polyfill and options classes in the repo use `{ get; set; }`.

### 3.2 `ConsumerOptions.cs`

Add (alphabetical position among properties is not enforced in the repo; place after `ConsumerName`):
```csharp
    /// <summary>
    /// Set the dead letter policy. This is optional.
    /// </summary>
    /// <remarks>
    /// Only has an effect for 'Shared' and 'KeyShared' subscriptions.
    /// </remarks>
    public DeadLetterPolicy? DeadLetterPolicy { get; set; }
```
WP-DLQ-B adds:
```csharp
    /// <summary>
    /// The default delay before negatively acknowledged messages are redelivered.
    /// </summary>
    public static readonly TimeSpan DefaultNegativeAcknowledgeRedeliveryDelay = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Set the delay before negatively acknowledged messages are redelivered. The minimum is 100 milliseconds. The default is 1 minute.
    /// </summary>
    public TimeSpan NegativeAcknowledgeRedeliveryDelay { get; set; }
```
(ctor: `NegativeAcknowledgeRedeliveryDelay = DefaultNegativeAcknowledgeRedeliveryDelay;`). Values below 100 ms (including `TimeSpan.Zero`) are accepted and clamped to 100 ms at runtime (Java `Math.max(delay, MIN_NACK_DELAY_MS)` NegativeAcksTracker L68-69); negative values -> `ConfigurationException("NegativeAcknowledgeRedeliveryDelay may not be negative")`.

### 3.3 `Abstractions/IConsumerBuilder.cs` + `Internal/ConsumerBuilder.cs`

```csharp
    /// <summary>
    /// Set the dead letter policy. This is optional.
    /// </summary>
    IConsumerBuilder<TMessage> DeadLetterPolicy(DeadLetterPolicy deadLetterPolicy);
```
WP-DLQ-B:
```csharp
    /// <summary>
    /// Set the delay before negatively acknowledged messages are redelivered. The default is 1 minute.
    /// </summary>
    IConsumerBuilder<TMessage> NegativeAcknowledgeRedeliveryDelay(TimeSpan delay);
```
`ConsumerBuilder`: fields `private DeadLetterPolicy? _deadLetterPolicy;`, `private TimeSpan _negativeAcknowledgeRedeliveryDelay;` (ctor default from `ConsumerOptions<TMessage>.DefaultNegativeAcknowledgeRedeliveryDelay`), copy into options in `Create()`. Validation in `Create()` (before options are built), same text as Java/C++:
```csharp
if (_deadLetterPolicy is not null && _deadLetterPolicy.MaxRedeliverCount == 0)
    throw new ConfigurationException("MaxRedeliverCount must be > 0.");
```
The same check must exist in `PulsarClient.CreateConsumer` (options path bypasses the builder; precedent: `CreateProducer` validates `MaxPendingMessages`). Note: `ConsumerBuilder` already has methods named like their parameter types (`SubscriptionType(SubscriptionType type)`), so `DeadLetterPolicy(DeadLetterPolicy deadLetterPolicy)` compiles; inside `ConsumerOptions` use `DotPulsar.DeadLetterPolicy` if a qualified type reference is needed in an expression (precedent `DotPulsar.RegexSubscriptionMode.Persistent`).

### 3.4 Negative acknowledge API (WP-DLQ-B)

`Abstractions/IConsumer.cs`:
```csharp
    /// <summary>
    /// Negatively acknowledge a single message using the MessageId, so it will be redelivered after the configured negative acknowledge redelivery delay.
    /// </summary>
    ValueTask NegativeAcknowledge(MessageId messageId, CancellationToken cancellationToken = default);
```
`Extensions/ConsumerExtensions.cs` (mirrors existing `Acknowledge(this IConsumer, IMessage, ct)` L28):
```csharp
    /// <summary>
    /// Negatively acknowledge a single message, so it will be redelivered after the configured negative acknowledge redelivery delay.
    /// </summary>
    public static async ValueTask NegativeAcknowledge(this IConsumer consumer, IMessage message, CancellationToken cancellationToken = default)
        => await consumer.NegativeAcknowledge(message.MessageId, cancellationToken).ConfigureAwait(false);
```
(Java's `negativeAcknowledge(Message)` only differs by feeding `redeliveryCount` to a `RedeliveryBackoff`, which is out of scope, so a single `MessageId` overload suffices.)
Implementations: `Internal/Consumer.cs` (route by `messageId.Topic` exactly like `Acknowledge` L262-270), `Internal/SubConsumer.cs`, and every other `IConsumer<T>` implementation in the repo (grep `: IConsumer<` — includes `NotReadyChannel` is a channel, not a consumer; test doubles in `tests/` must be updated).

No public API for the DLQ property names; they are internal constants (4.6).

---

## 4. Internal design

### 4.1 New type `src/DotPulsar/Internal/DeadLetterHandler.cs`

```csharp
namespace DotPulsar.Internal;

using DotPulsar.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Collections.Concurrent;

public sealed class DeadLetterHandler<TMessage> : IAsyncDisposable
{
    // ctor(DeadLetterPolicy policy, string topic, string subscriptionName, string consumerName, SchemaInfo schemaInfo,
    //      Func<ProducerOptions<ReadOnlySequence<byte>>, IProducer<ReadOnlySequence<byte>>> producerFactory)
    public uint MaxRedeliverCount { get; }
    public string DeadLetterTopic { get; }               // resolved
    public Func<MessageId, ValueTask>? RedeliveryCountExceeded { get; set; } // set once by SubConsumer ctor
    public void Register(ulong ledgerId, ulong entryId, IMessage<TMessage> message);          // non-batched
    public void Register(ulong ledgerId, ulong entryId, IMessage<TMessage>[] messages);       // batched (may be empty array -> not stored)
    public void Remove(ulong ledgerId, ulong entryId);
    public void Clear();
    public bool Contains(ulong ledgerId, ulong entryId);  // for unit tests
    public int Count { get; }                             // for unit tests
    public ValueTask<bool> TryDeadLetter(MessageId messageId, Func<MessageId, CancellationToken, ValueTask> acknowledge, CancellationToken cancellationToken);
    public void OnRedeliveryCountExceeded(MessageId entryMessageId); // fire-and-forget wrapper around RedeliveryCountExceeded
    public ValueTask DisposeAsync();
}
```
State:
- `_candidates`: `ConcurrentDictionary<(ulong LedgerId, ulong EntryId), IMessage<TMessage>[]>`. Key is entry level and **excludes partition** (one handler per `SubConsumer`, i.e. per partition, so partition is implied). This deliberately differs from Java's `MessageIdAdv` key (which includes partition and is looked up with the NegativeAcksTracker's `DUMMY_PARTITION_INDEX = -2`, so a Java nack redelivery misses the map once and the message is dead-lettered one broker round-trip later, via the `redeliveryCount > max` branch). Observable outcome (number of deliveries to the app = MaxRedeliverCount + 1, then DLQ) is identical; DotPulsar just dead-letters on the first lookup, as Java already does for ack-timeout.
- `_producer`: `IProducer<ReadOnlySequence<byte>>?` guarded by `AsyncLock` (repo type `Internal/AsyncLock.cs`) for lazy single creation (Java `createProducerLock` L2724-2763).
- No retention issue: DotPulsar `Message<T>` owns its `Data` (not pooled), so storing `IMessage<TMessage>` is safe (Java needs `unpooledCopy()` L1719-1721; not applicable).

Resolution of topic (ctor), Java `ConsumerImpl` L436-447:
```
DeadLetterTopic = string.IsNullOrWhiteSpace(policy.DeadLetterTopic) ? $"{topic}-{subscriptionName}-DLQ" : policy.DeadLetterTopic!;
```
`topic` = the `SubConsumer` topic (partition topic for partitioned topics, e.g. `<topic>-partition-0`), exactly as given by `Consumer.Monitor` (DotPulsar does not normalize topic names; Java's `topic` is the fully-qualified name — tests use fully-qualified names, so results match).

### 4.2 `TryDeadLetter` algorithm (Java `processPossibleToDLQ` L2632-2708 + C++ L2091-2197)

```
key = (messageId.LedgerId, messageId.EntryId)       // batch index discarded (Java MessageIdAdvUtils.discardBatch, L2634)
if !_candidates.TryGetValue(key, out messages): return false
try
    producer = await GetProducer(ct)
    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct); timeout.CancelAfter(DeadLetterSendTimeout /* 30 s */)
    foreach message in messages (in batch-index order):
        await producer.Send(CreateMetadata(message), message.Data, timeout.Token)   // sequential to preserve order within the entry
    _candidates.TryRemove(key, out _)                                               // Java L2658 (remove only after success)
    await acknowledge(new MessageId(LedgerId, EntryId, messageId.Partition, -1, messageId.Topic), ct)  // entry-level, non-transactional, individual
    return true
catch (Exception):
    if exception came from the producer being Faulted/Disposed -> dispose and null _producer so the next call recreates it (Java L2722-2727 isCompletedExceptionally; prevents the producer leak of testCloseDeadLetterTopicProducerOnExceptionToPreventProducerLeak)
    return false     // Java completes false on send failure (L2672-2687), processing failure (L2689-2695), producer failure (L2697-2702), ack failure (L2660-2668)
```
Details:
- 30 s = Java `ProducerConfigurationData.sendTimeoutMs` default; DotPulsar has no send timeout yet (`batching.md` adds one). If `batching.md` lands first, use its send-timeout option on the DLQ producer instead of the linked CTS. Either way `Send` must not hang forever when the DLQ topic is unreachable.
- Ack target: Java acks the id passed in (may carry a batch index, L2659). DotPulsar acks the **entry-level** id because every still-unacked message of the entry has just been dead-lettered; this guarantees the broker drops the whole entry. (Deviation recorded in OD-5.)
- If send succeeded but ack failed -> `false` -> the entry is redelivered with a higher count -> re-registered -> dead-lettered again (duplicate in DLQ). Java parity; do not "fix".
- `acknowledge` delegate is `SubConsumer.Acknowledge(MessageId, CancellationToken)` (non-transactional individual ack) so the normal ack path also removes the id from `UnAckedMessageTracker` (ack-timeout.md) and from `_candidates`.

### 4.3 DLQ producer (`GetProducer`) — Java L2721-2767, C++ L2098-2128

`ProducerOptions<ReadOnlySequence<byte>>`:
- `Topic = DeadLetterTopic`
- `Schema = new DeadLetterSchema(schemaInfo)` — new internal `src/DotPulsar/Internal/DeadLetterSchema.cs : ISchema<ReadOnlySequence<byte>>` with `Encode(x) => x`, `Decode(bytes, _) => bytes`, `SchemaInfo` = the consumer's `ISchema<TMessage>.SchemaInfo` (Java `AUTO_PRODUCE_BYTES(schema)` with `setRequireSchemaValidation(false)`: the consumer's schema is registered on the DLQ topic, payload bytes are passed through unvalidated; DotPulsar has no client-side validation, so pass-through is exact).
- `ProducerName = $"{topic}-{subscriptionName}-{consumerName}-{Random5}-DLQ"` where `Random5` = 5 chars uniformly from `[A-Za-z0-9]` (Java `RandomStringUtils.insecure().nextAlphanumeric(5)`), `consumerName` = the name used in `Consumer.CreateSubConsumer` (`_consumerOptions.ConsumerName ?? $"Consumer-{correlationId:N}"`, Consumer.cs L430). Regex asserted by upstream test: `^persistent://<ns>/<shortTopic>-my-subscription-my-consumer-[a-zA-Z0-9]{5}-DLQ$`.
- `MaxPendingMessages = 500` (DotPulsar builder default, ProducerBuilder.cs L42). Java `blockIfQueueFull(false)`: DotPulsar's `SubProducer` queue blocks when full; the 30 s timeout bounds it.
- Batching disabled (DotPulsar default; once `batching.md` lands set it explicitly to false). Chunking enabled (`chunking.md`; set explicitly once available).
- `CompressionType = None` (Java default for the DLQ producer).
- **Initial subscription**: new internal option `internal string? InitialSubscriptionName { get; set; }` on `ProducerOptions<TMessage>` (not public API — Java keeps it on `ProducerBuilderImpl`, L2734). `Producer.CreateSubProducer` (Producer.cs L156-172) passes it to a new `ProducerChannelFactory` ctor parameter `string? initialSubscriptionName`, which sets `_commandProducer.InitialSubscriptionName` (proto `CommandProducer.initial_subscription_name = 13`, PulsarApi.proto L529) when not null/empty.
- Created via a factory delegate supplied by `Consumer.CreateSubConsumer`:
  `options => new Producer<ReadOnlySequence<byte>>(ServiceUrl, options, _exceptionHandler, _connectionPool, null)`.
  It is NOT registered in `PulsarClient`'s disposables; the handler owns and disposes it (`SubConsumer.DisposeAsync` -> `DeadLetterHandler.DisposeAsync` -> producer `DisposeAsync`; Java L1267-1273). Because a producer keeps the connection referenced, the 60 s inactive-connection rule in `ChannelManager` is unaffected.
- One DLQ producer per `SubConsumer` (Java: per `ConsumerImpl`; `testDeadLetterTopicWithInitialSubscriptionAndMultiConsumers` asserts 2 publishers for 2 consumers). Created lazily on the first `TryDeadLetter` that finds a candidate (Java L2642; `testDeadLetterWithoutConsumerReceiveImmediately` requires no DLQ activity before that).

### 4.4 `CreateMetadata(IMessage<TMessage> message)` — Java parity (L700-711, L842-870, L881-884, L2650-2655)

Returns a fresh `DotPulsar.MessageMetadata` (`new MessageMetadata()`; not pooled — DLQ is a cold path):
1. Properties: start from `message.Properties`; then add only if absent ("putIfAbsent"), in this order:
   - `REAL_TOPIC` = handler `topic` (the SubConsumer/partition topic; Java `message.getTopicName()` for a `ConsumerImpl` message).
   - `ORIGIN_MESSAGE_IDY_TIME` (Java deprecated `SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID`, value identical — yes the key literally is `"ORIGIN_MESSAGE_IDY_TIME"`, RetryMessageUtil L29) = origin id string.
   - `ORIGIN_MESSAGE_ID` = origin id string.
   - `REAL_SUBSCRIPTION` = subscription name.
   Emit properties sorted by key with `StringComparer.Ordinal` (Java `TreeMap<String,String>` natural order = UTF-16 ordinal).
   Origin id string = **Java `MessageId.toString()` format**, not DotPulsar's `MessageId.ToString()` (which appends the topic): `BatchIndex < 0 ? $"{LedgerId}:{EntryId}:{Partition}" : $"{LedgerId}:{EntryId}:{Partition}:{BatchIndex}"` (Java `MessageIdImpl#toString` / `BatchMessageIdImpl#toString`). Put this in an internal static helper `DeadLetterHandler<TMessage>.FormatOriginMessageId(MessageId)` (or `Internal/Extensions/MessageIdExtensions`), unit tested.
2. Key: if `message.Key is not null`: `HasBase64EncodedKey ? metadata.Metadata.SetKey(message.KeyBytes) : metadata.Metadata.SetKey(message.Key)` (extensions in `Internal/Extensions/MessageMetadataExtensions.cs` L60-72).
3. Ordering key: if `message.OrderingKey is not null` -> `metadata.Metadata.OrderingKey = ByteString.CopyFrom(message.OrderingKey)`.
4. Event time: `metadata.Metadata.EventTime = message.EventTime` (Java copies unconditionally; 0 = unset, so copying 0 is a no-op).
5. Do NOT copy sequence id, producer name, publish time, schema version, replication fields (Java does not).
6. Payload = `message.Data` (decompressed, and once `encryption.md` lands, decrypted — same as Java `getData()`).

### 4.5 Registration on receive (ConsumerChannel / BatchHandler)

`ConsumerChannel<TMessage>` gets a new ctor parameter `DeadLetterHandler<TMessage>? deadLetterHandler` (supplied by `ConsumerChannelFactory`, which receives it from `Consumer.CreateSubConsumer`, mirroring how `BatchHandler` is passed today, Consumer.cs L463-465, ConsumerChannelFactory.cs L32-61).

Non-batched branch, replacing ConsumerChannel.cs L135:
```csharp
var message = _messageFactory.Create(messageId.ToMessageId(_topic), redeliveryCount, data, metadata);

if (_deadLetterHandler is not null && redeliveryCount >= _deadLetterHandler.MaxRedeliverCount)
{
    _deadLetterHandler.Register(messageId.LedgerId, messageId.EntryId, message);
    if (redeliveryCount > _deadLetterHandler.MaxRedeliverCount)
    {
        _deadLetterHandler.OnRedeliveryCountExceeded(message.MessageId);
        continue;
    }
}

return message;
```
Permit accounting: the `_sendWhenZero--` at L82 stays applied on `continue` — the skipped message consumed a broker permit and is replenished by the normal flow cycle; this is the DotPulsar equivalent of Java `increaseAvailablePermits(cnx)` (L1729) for a skipped message. Do not `_sendWhenZero++`.

Batched branch (ConsumerChannel.cs L122-133): `BatchHandler<TMessage>` receives the handler in its ctor (`BatchHandler(bool trackBatches, IMessageFactory<TMessage> messageFactory, DeadLetterHandler<TMessage>? deadLetterHandler)`), and `Add` changes to return `IMessage<TMessage>?`:
```
messages = unpack all (existing loop L47-57; with batching.md: skip indexes already acked per ack_set BEFORE this point)
if (dlq is not null && redeliveryCount >= dlq.MaxRedeliverCount)
{
    dlq.Register(messageId.LedgerId, messageId.EntryId, messages.ToArray());   // Java L2081-2084
    if (redeliveryCount > dlq.MaxRedeliverCount)
    {
        dlq.OnRedeliveryCountExceeded(new MessageId(messageId.LedgerId, messageId.EntryId, messageId.Partition, -1, topic)); // Java L2085-2087 (entry-level id)
        return null;    // nothing enqueued, no Batch tracker added (would leak: never acked)
    }
}
lock: track batch + enqueue as today; return _messages.Dequeue()
```
`ConsumerChannel`: `var message = _batchHandler.Add(...); if (message is null) continue; return message;` (inside the existing try; `BatchHandler` needs the topic string — add a `string topic` ctor arg or pass the entry `MessageId` in). `batching.md` rewrites `BatchHandler`; whoever lands second merges these rules (they are independent of ack_set logic, except "unpack skips acked indexes first", which is required by `testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound`).

Clearing: `ConsumerChannelFactory.Create` calls `_deadLetterHandler?.Clear()` before subscribing (Java `connectionOpened` L941-943); `ConsumerChannel.Send(CommandSeek)` also calls it next to `_batchHandler.Clear()` (L211). `RedeliverUnacknowledgedMessages()` (all) does NOT clear (Java parity).

### 4.6 Removal on ack

In `ConsumerChannel.Send(CommandAck)` (L147-191): for `AckType.Individual` only, for every `MessageIdData` that is actually sent with `BatchIndex == -1` after batch resolution (i.e. non-batched ack or the batch became fully acked), call `_deadLetterHandler?.Remove(id.LedgerId, id.EntryId)` (Java PersistentAcknowledgmentsGroupingTracker L218-222). Cumulative acks and transactional acks (`ConsumerChannel.SendTransactional`) do not touch the map (Java parity: `doTransactionAcknowledgeForResponse` and cumulative path never remove). Batch-index (partial) acks from `batching.md` do not remove either (Java only removes when `MessageIdAdvUtils.acknowledge(batchMessageId, true)` reports the whole batch acked).

### 4.7 Redelivery hook (`SubConsumer.RedeliverUnacknowledgedMessages(IEnumerable<MessageId>, ct)`)

Current code (SubConsumer.cs L98-103) builds one command with all ids. New behaviour when `_deadLetterHandler is not null` AND `SubscriptionType is Shared or KeyShared` AND the id set is non-empty:
```
var entries = distinct by (LedgerId, EntryId) preserving first occurrence
var results = await Task.WhenAll(entries.Select(id => _deadLetterHandler.TryDeadLetter(id, Acknowledge, cancellationToken).AsTask()))   // concurrent like Java futures (L2614-2629)
var remaining = entries where result == false, mapped to new MessageIdData { LedgerId, EntryId, Partition }   // NO BatchIndex (Java L2619-2622)
if remaining.Count == 0: return
for each chunk of 1000 (Java MAX_REDELIVER_UNACKNOWLEDGED L148, L2568):
    command = new CommandRedeliverUnacknowledgedMessages(); command.MessageIds.AddRange(chunk)
    await _executor.Execute(() => InternalRedeliverUnacknowledgedMessages(command, ct), ct)
```
Otherwise the existing path is unchanged (and is further defined by `ack-timeout.md`, which owns: purging prefetched copies, entry-level id conversion for the non-DLQ path, redeliver-all fallback for Exclusive/Failover, empty-set semantics). The DLQ work must run outside `_executor.Execute` (it performs producer I/O; only the final command send goes through the executor). `Guard()` is still called first.

`SubConsumer` ctor gets `DeadLetterHandler<TMessage>? deadLetterHandler` and NegativeAcknowledgeTracker (WP-B); it sets
`deadLetterHandler.RedeliveryCountExceeded = id => RedeliverUnacknowledgedMessages(new[] { id }, CancellationToken.None);`
`OnRedeliveryCountExceeded` invokes it as `_ = Invoke(id)` with all exceptions swallowed (Java writes the command fire-and-forget). It is called while `ConsumerChannel.Receive` holds `_lock`; it must not be awaited there (no deadlock: the redelivery path never takes the channel's receive lock).

Callers that reach this hook: user calls to `RedeliverUnacknowledgedMessages(ids)`, the ack-timeout tracker (ack-timeout.md calls the same `SubConsumer` method — Java UnAckedMessageTracker L176-177), the negative-ack tracker (WP-B), and the `> max` path above. The transaction abort path needs no client hook: the broker redelivers and the `> max` registration path handles it.

### 4.8 Negative acknowledge tracker (WP-DLQ-B) `src/DotPulsar/Internal/NegativeAcknowledgeTracker.cs`

Java `NegativeAcksTracker` simplified (no backoff, fixed precision):
- One per `SubConsumer`. ctor `(TimeSpan delay, Func<IEnumerable<MessageId>, ValueTask> redeliver)`.
- `_delayMs = Math.Max((long) delay.TotalMilliseconds, 100)`.
- `Add(MessageId id)`: `due = TrimLowerBits(nowMs + _delayMs, 8)` where `TrimLowerBits(t, bits) = t & (-1L << bits)` (Java L155-157, L180; bucket = 256 ms, never later than requested). Store in `SortedDictionary<long, HashSet<(ulong LedgerId, ulong EntryId)>>` plus a `Dictionary<(ulong,ulong), MessageId>` keeping the first entry-level `MessageId` (with partition and topic) per entry. Batch ids collapse to their entry (Java `testNegativeAcksDeleteFromUnackedTracker`: 3 batch ids of one entry -> count 1). If no timer is armed, arm one-shot timer for `_delayMs` (Java L185-189).
- Timer callback (Java `triggerRedelivery` L74-145): under lock, collect all buckets with `due <= now`, remove them, re-arm for `firstKey - now` (min 0) if non-empty; outside the lock call `redeliver(ids)` (=> `SubConsumer.RedeliverUnacknowledgedMessages(ids, CancellationToken.None)`, exceptions swallowed).
- `Count` (for unit tests), `Dispose()` cancels the timer and clears (Java `close` L217-230).
- Clock: use the timer/clock abstraction introduced by `ack-timeout.md` if it adds one (preferred, makes unit tests deterministic); otherwise `System.Threading.Timer` + `Stopwatch.GetTimestamp()`-based milliseconds (netstandard2.0 has no `Environment.TickCount64`).
- `SubConsumer.NegativeAcknowledge(messageId, ct)`: `Guard()`; `_unackedTracker?.Remove(entry-level id)` (Java L892/L901, owned by ack-timeout.md's tracker API); `_negativeAcknowledgeTracker.Add(messageId)`; return completed `ValueTask` (no I/O).
- For Exclusive/Failover the redelivery goes through the ack-timeout.md redeliver-all fallback (Java L2559-2563), which must also clear prefetched messages; otherwise NegativeAcksTest Failover variants receive duplicates.

### 4.9 Wiring in `Consumer.CreateSubConsumer` (Consumer.cs L427-474)

```
DeadLetterHandler<TMessage>? deadLetterHandler = null;
if (_consumerOptions.DeadLetterPolicy is not null)
    deadLetterHandler = new DeadLetterHandler<TMessage>(_consumerOptions.DeadLetterPolicy, topic, _consumerOptions.SubscriptionName, consumerName, _consumerOptions.Schema.SchemaInfo,
        options => new Producer<ReadOnlySequence<byte>>(ServiceUrl, options, _exceptionHandler, _connectionPool, null));
var batchHandler = new BatchHandler<TMessage>(true, messageFactory, deadLetterHandler);
var consumerChannelFactory = new ConsumerChannelFactory<TMessage>(..., deadLetterHandler);
var subConsumer = new SubConsumer<TMessage>(..., deadLetterHandler, _consumerOptions.NegativeAcknowledgeRedeliveryDelay);
```
`Reader` does not get DLQ (readers have no acks).

---

## 5. Exact DotPulsar edit points

| File | Change |
|---|---|
| `src/DotPulsar/DeadLetterPolicy.cs` | NEW (3.1). |
| `src/DotPulsar/ConsumerOptions.cs` | add `DeadLetterPolicy` (+ WP-B `DefaultNegativeAcknowledgeRedeliveryDelay`, `NegativeAcknowledgeRedeliveryDelay`, init in private ctor L62-81). |
| `src/DotPulsar/Abstractions/IConsumerBuilder.cs` | add `DeadLetterPolicy(...)` (+ WP-B `NegativeAcknowledgeRedeliveryDelay(...)`). |
| `src/DotPulsar/Internal/ConsumerBuilder.cs` | fields, setters, validation and copy in `Create()` (L153-179). |
| `src/DotPulsar/PulsarClient.cs` | `CreateConsumer` (L89-100): validate `MaxRedeliverCount > 0`; WP-B: delay not negative. |
| `src/DotPulsar/Abstractions/IConsumer.cs` | WP-B `NegativeAcknowledge`. |
| `src/DotPulsar/Extensions/ConsumerExtensions.cs` | WP-B `NegativeAcknowledge(this IConsumer, IMessage, ct)`. |
| `src/DotPulsar/Internal/Consumer.cs` | `CreateSubConsumer` wiring (4.9); WP-B `NegativeAcknowledge` routing by `messageId.Topic`. |
| `src/DotPulsar/Internal/SubConsumer.cs` | ctor params; `RedeliverUnacknowledgedMessages(ids)` (4.7); `DisposeAsync` disposes handler + nack tracker; WP-B `NegativeAcknowledge`. |
| `src/DotPulsar/Internal/ConsumerChannel.cs` | ctor param; non-batch registration (L135); batch `null` handling (L122-133); `Send(CommandAck)` removal (L147-191); `Send(CommandSeek)` clear (L211). |
| `src/DotPulsar/Internal/ConsumerChannelFactory.cs` | ctor param; `Clear()` in `Create` (L54-61); pass to channel. |
| `src/DotPulsar/Internal/BatchHandler.cs` | ctor param; `Add` returns `IMessage<TMessage>?` with DLQ rules (4.5). |
| `src/DotPulsar/Internal/Abstractions/IConsumerChannel.cs` / `NotReadyChannel.cs` | no signature change needed (channel API unchanged). |
| `src/DotPulsar/Internal/DeadLetterHandler.cs` | NEW (4.1-4.4). |
| `src/DotPulsar/Internal/DeadLetterSchema.cs` | NEW pass-through schema (4.3). |
| `src/DotPulsar/Internal/NegativeAcknowledgeTracker.cs` | NEW (WP-B, 4.8). |
| `src/DotPulsar/ProducerOptions.cs` | `internal string? InitialSubscriptionName { get; set; }`. |
| `src/DotPulsar/Internal/Producer.cs` | `CreateSubProducer` (L156-172) passes `_options.InitialSubscriptionName`. |
| `src/DotPulsar/Internal/ProducerChannelFactory.cs` | ctor param `string? initialSubscriptionName` -> `_commandProducer.InitialSubscriptionName`. |
| `tests/DotPulsar.Tests/IntegrationFixture.cs` | admin helpers needed by ported tests: `GetTopicsOfNamespace(ns)`, `GetSubscriptions(topic)`, `GetPublisherCount(topic)` (from `/admin/v2/persistent/{tenant}/{ns}/{topic}/stats` `publishers.length`), `SetMaxUnackedMessagesOnConsumer/Subscription(topic, n)` (topic policies; WP-B test only). |
| `CHANGELOG.md` | entries for DLQ and negative ack. |

---

## 6. Zero-alloc / performance notes

- Disabled policy (default): one `is null` check per received message/batch; no allocations, no extra fields touched on the hot path. `SubConsumer.RedeliverUnacknowledgedMessages` keeps its current single-command path.
- Enabled policy: allocation only for messages with `redeliveryCount >= MaxRedeliverCount` (an array per entry). Batch path: `messages.ToArray()` only in that case; use the existing `List<IMessage<TMessage>>` otherwise.
- Do not pool DLQ `MessageMetadata` (cold path); never reuse `MessageMetadataObjectPool` instances across the `await` of the DLQ send (pool reuse hazard noted in shared context).
- `ConcurrentDictionary` with `ValueTuple<ulong, ulong>` keys (no boxing; `ValueTuple` implements `IEquatable`).
- Nack tracker: O(log n) insert, buckets of 256 ms reduce timer churn; one timer per SubConsumer, armed only while non-empty.

---

## 7. Tests to add

C# conventions per shared context: `Method_GivenCondition_ShouldOutcome`, `// Upstream: <JavaClass>#<javaMethod>` above the attribute, `//Arrange //Act //Assert`, Shouldly, Theory+InlineData with identical values. Integration file: `tests/DotPulsar.Tests/DeadLetterTopicTests.cs` and `tests/DotPulsar.Tests/NegativeAcknowledgeTests.cs` with `[Collection("Integration"), Trait("Category", "Integration")]` + `IntegrationFixture` (existing precedent `tests/DotPulsar.Tests/Internal/ConsumerTests.cs` L26). The fixture/broker must run with batch-index ack enabled for the batch tests (same as `TransactionFixture`: `acknowledgmentAtBatchIndexLevelEnabled=true`). Java `consumer.receive()` without timeout -> `Receive(ct)` with the test's cancellation token; `receive(n, SECONDS)` returning null -> `ReceiveOrNull(consumer, TimeSpan.FromSeconds(n), ct)`; Java `newPulsarClient()` -> a second `PulsarClient` built from the fixture's `ServiceUrl`/`Authentication`; Java `receiverQueueSize(n)` -> `MessagePrefetchCount(n)`; Java `newTopicName()` -> `fixture.CreateTopic`/unique `persistent://public/default/<guid>` name.

### 7.1 Unit tests (`[Trait("Category", "Unit")]`)

`tests/DotPulsar.Tests/Internal/DeadLetterHandlerTests.cs` (new; NSubstitute for `IProducer<ReadOnlySequence<byte>>` and the factory delegate):
1. `Constructor_GivenNoDeadLetterTopic_ShouldUseTopicSubscriptionDlqSuffix` (`persistent://public/default/t` + `sub` -> `persistent://public/default/t-sub-DLQ`; partition topic `...-partition-0` -> `...-partition-0-sub-DLQ`). Cross-ref cpp `DeadLetterQueueTest#testAutoSetDLQTopicName`.
2. `Constructor_GivenCustomDeadLetterTopic_ShouldUseIt`.
3. `TryDeadLetter_GivenUnregisteredEntry_ShouldReturnFalseAndNotCreateProducer`.
4. `TryDeadLetter_GivenRegisteredEntry_ShouldSendAllMessagesThenAcknowledgeEntryAndRemove` (batch of 3 -> 3 sends in order, ack called once with BatchIndex -1, `Count == 0`).
5. `TryDeadLetter_GivenBatchIndexMessageId_ShouldResolveEntryAndRemoveIt` (Java `testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId` unit equivalent).
6. `TryDeadLetter_GivenSendFails_ShouldReturnFalseAndKeepEntry`.
7. `TryDeadLetter_GivenAcknowledgeFails_ShouldReturnFalseAndEntryRemoved`.
8. `TryDeadLetter_GivenFaultedProducer_ShouldDisposeAndRecreateProducerOnNextCall`.
9. `TryDeadLetter_GivenCalledTwiceConcurrently_ShouldCreateOneProducer`.
10. `CreateMetadata_GivenMessage_ShouldCopyPropertiesKeyOrderingKeyEventTimeAndAddOriginInfo` (asserts REAL_TOPIC, ORIGIN_MESSAGE_IDY_TIME, ORIGIN_MESSAGE_ID, REAL_SUBSCRIPTION, existing user property with same name NOT overwritten, ordinal sort).
11. `CreateMetadata_GivenBase64Key_ShouldCopyKeyBytes`.
12. `FormatOriginMessageId_GivenNonBatched_ShouldReturnLedgerEntryPartition` (`"1:2:-1"`) / `_GivenBatched_ShouldAppendBatchIndex` (`"1:2:0:3"`).
13. `ProducerName_GivenConsumerName_ShouldMatchJavaPattern` (regex `^<topic>-<sub>-<consumer>-[a-zA-Z0-9]{5}-DLQ$`).
14. `Remove_GivenRegisteredEntry_ShouldRemove`; `Clear_ShouldRemoveAll`.

`tests/DotPulsar.Tests/Internal/BatchHandlerTests.cs` (extend/new):
15. `Add_GivenRedeliveryCountEqualToMax_ShouldRegisterAndReturnFirstMessage`.
16. `Add_GivenRedeliveryCountAboveMax_ShouldRegisterRequestRedeliveryAndReturnNull` (and no batch tracker added).

`tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` (extend; existing file):
17. `Receive_GivenNonBatchedMessageAboveMaxRedeliverCount_ShouldSkipAndRequestRedelivery`.
18. `Send_GivenIndividualAckOfRegisteredEntry_ShouldRemoveCandidate`; `Send_GivenCumulativeAck_ShouldNotRemoveCandidate`.

`tests/DotPulsar.Tests/Internal/ConsumerBuilderTests.cs` (new or existing):
19. `Create_GivenDeadLetterPolicyWithZeroMaxRedeliverCount_ShouldThrowConfigurationException` (Java ConsumerBuilderImpl L529, cpp `DeadLetterPolicyTest`).

`tests/DotPulsar.Tests/Internal/ProducerChannelFactoryTests.cs`:
20. `Create_GivenInitialSubscriptionName_ShouldSetItOnCommandProducer`.

WP-B `tests/DotPulsar.Tests/Internal/NegativeAcknowledgeTrackerTests.cs`:
21. `// Upstream: NegativeAcksTest#testNegativeAcksDeleteFromUnackedTracker` (adapted to unit) `NegativeAcknowledge_GivenBatchMessageIdsOfSameEntry_ShouldTrackOneEntryAndRemoveFromUnackedTracker`.
22. `Add_GivenDelayBelowMinimum_ShouldUse100Milliseconds`.
23. `Trigger_GivenDueEntries_ShouldRedeliverOnlyDueEntriesAndRearm`.
24. `TrimLowerBits_GivenEightBits_ShouldBucketBy256Milliseconds` (Java `trimLowerBit`).
25. `Dispose_ShouldCancelTimerAndClear`.

### 7.2 Integration tests ported from `DeadLetterTopicTest` (java:pulsar-broker/src/test/java/org/apache/pulsar/client/api/DeadLetterTopicTest.java)

All upstream values reproduced exactly (Shared, `ackTimeout(1 s)`, `maxRedeliverCount`, `sendMessages`, `receiverQueueSize`, `Earliest`, value strings like `"Hello Pulsar [%d]"`). Requires ack-timeout.md unless stated.

| Java method (Lx) | Class | C# name | Depends on |
|---|---|---|---|
| `testDeadLetterTopicWithMessageKey` (L88) | portable | `Receive_GivenMessagesWithKeyRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithKey` | ack-timeout |
| `testDeadLetterTopicWithBinaryMessageKey` (L149) | portable | `Receive_GivenMessagesWithBinaryKeyRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithKeyBytes` | ack-timeout |
| `testDeadLetterTopicMessagesWithOrderingKey` (L211) | portable | `Receive_GivenMessagesWithOrderingKeyRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithOrderingKey` | ack-timeout |
| `testDeadLetterTopicMessagesWithEventTime` (L273) | portable | `Receive_GivenMessagesWithEventTimeRedeliveredPastMaxRedeliverCount_ShouldSendThemToDeadLetterTopicWithEventTime` | ack-timeout |
| `testDeadLetterTopicWithProducerName` (L334; public method under class-level `@Test`, so it runs) | portable | `Receive_GivenConsumerName_ShouldSendToDeadLetterTopicWithJavaCompatibleProducerName` | ack-timeout |
| `testMultipleSameNameConsumersToDeadLetterTopic` (L406) | adapted (message listener -> one background `Receive` loop task per consumer that never acks; `negativeAckRedeliveryDelay(1001 ms)` mapped to WP-B option or omitted if WP-B not merged since no nack is issued) | `Receive_GivenThreeConsumersWithSameNameNeverAcknowledging_ShouldDeliverEachMessageMaxRedeliverPlusOneTimesAndDeadLetterEachOnce` | ack-timeout |
| `testDeadLetterTopic(boolean produceLargeMessages)` (L485, data provider L479-482 `{false},{true}`) | portable, `[Theory] [InlineData(false)] [InlineData(true)]` | `Receive_GivenMessagesRedeliveredPastMaxRedeliverCount_ShouldSendAllToDeadLetterTopicAndAcknowledgeOriginals` | ack-timeout; `true` -> chunking.md (10 KiB payload, `enableChunking(true)`), `false` -> batching.md (`enableBatching(true)`) |
| `testDeadLetterTopicHasOriginalInfo` (L573) | portable (origin id compared against Java-format string of the id returned by `Send`, i.e. `FormatOriginMessageId`) | `Receive_GivenMessagesSentToDeadLetterTopic_ShouldContainRealTopicRealSubscriptionAndOriginMessageId` | ack-timeout |
| `testAutoConsumeSchemaDeadLetter` (L650) | not portable (AUTO_PRODUCE_BYTES / AUTO_CONSUME / per-message AVRO schema) | — | — |
| `testDuplicatedMessageSendToDeadLetterTopic` (L720) | adapted (listener -> receive loop tasks) | `Receive_GivenThreeConsumersNeverAcknowledging_ShouldNotSendDuplicatesToDeadLetterTopic` | ack-timeout |
| `testDeadLetterTopicWithMultiTopic` (L796) | not ported (`@Test(enabled = false)` upstream, issue #2647) | — | — |
| `testDeadLetterTopicByCustomTopicName` (L878, `groups = "quarantine"`) | portable (port, flaky upstream; keep same values) | `Receive_GivenCustomDeadLetterTopic_ShouldSendToCustomTopic` | ack-timeout |
| `testDeadLetterWithoutConsumerReceiveImmediately` (L951) | portable (`Thread.sleep(5000)` -> `Task.Delay(5000)`) | `Receive_GivenConsumerNotReceivingForFiveSeconds_ShouldNotSendToDeadLetterTopic` | ack-timeout |
| `testDeadLetterTopicUnderPartitionedTopicWithKeyShareType` (L976) | portable (`keySharedPolicy(autoSplitHashRange())` = DotPulsar KeyShared default) | `Receive_GivenPartitionedTopicWithKeySharedSubscription_ShouldSendToPerPartitionDeadLetterTopics` | ack-timeout, `fixture.CreatePartitionedTopic` |
| `testDeadLetterTopicWithInitialSubscription` (L1076) | portable (Awaitility -> polling helper 10 s / 1 s via admin REST) | `Receive_GivenInitialSubscriptionName_ShouldCreateSubscriptionOnDeadLetterTopic` | ack-timeout, fixture admin helpers |
| `testDeadLetterTopicWithProducerBuilder` (L1150) | not portable (`enableRetry`, `reconsumeLater`, producer customizers) | — | OD-1/OD-2 |
| `testDeadLetterTopicWithInitialSubscriptionAndMultiConsumers` (L1281) | portable (2 consumers, `receiverQueueSize(20)`; publishers count == 2 via admin stats) | `Receive_GivenTwoConsumersWithInitialSubscriptionName_ShouldCreateOneDeadLetterProducerPerConsumer` | ack-timeout, fixture admin helpers |
| `testDeadLetterPolicyDeserialize` (L1365) | not portable (`loadConf`) | — | — |
| `testCloseDeadLetterTopicProducerOnExceptionToPreventProducerLeak` (L1403) | not portable (incompatible AVRO schemas, listener, broker-internal `getTopicReference`); covered by unit test 8 | — | — |
| `testDeadLetterTopicWithMaxUnackedMessagesBlocking` (L1464) | adapted (WP-B: `negativeAckRedeliveryDelay(1 ms)` -> clamped to 100 ms exactly like Java; topic policies via admin REST) | `NegativeAcknowledge_GivenMaxUnackedMessagesPolicy_ShouldEventuallySendAllMessagesToDeadLetterTopic` | WP-B, fixture admin helpers |
| `sendDeadLetterTopicWithMismatchSchemaProducer` (L1562) | not portable (AVRO(Long)/AUTO_CONSUME, broker internals, retry topic) | — | — |
| `testCheckUnnecessaryGetPartitionedTopicMetadataWhenUseRetryAndDQL` (L1620) | not portable (retry + Mockito on client internals) | — | OD-1 |
| `testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound` (L1695) | portable | `Receive_GivenBatchIndexesAcknowledgedOnFinalRound_ShouldNotSendAnyMessageToDeadLetterTopic` | ack-timeout, batching.md (batch-index ack, `batchingMaxPublishDelay(1 s)`) |
| `testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId` (L1775) | adapted (internal map not observable; assert DLQ receives all 5, then redeliver the same batch id again and `ReceiveOrNull(deadLetterConsumer, 5 s)` is null; map emptiness covered by unit test 5) | `RedeliverUnacknowledgedMessages_GivenBatchMessageIdOfDeadLetterCandidate_ShouldSendWholeBatchToDeadLetterTopicOnce` | batching.md |
| `testDeadLetterTopicWithPoolMessages(boolean enableBatching)` (L1855, provider L1843-1846 `{false},{true}`) | adapted (`poolMessages`/`release()` N/A; redeliver the ids received in the second round instead of the internal map keys) | `RedeliverUnacknowledgedMessages_GivenCandidatesFromSecondDelivery_ShouldSendAllToDeadLetterTopic` `[InlineData(false)] [InlineData(true)]` | batching.md for `true` |

### 7.3 Integration tests ported from `NegativeAcksTest` (WP-B; java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/NegativeAcksTest.java)

| Java method | Class | C# name |
|---|---|---|
| `testNegativeAcks` (L92, provider `variations` L50-90, 32 rows `{batching, usePartitions, subType Shared/Failover, negAcksDelayMillis 100/0, ackTimeout 0/1000}`) | adapted: `acknowledgmentGroupTime(0)` N/A (DotPulsar acks immediately); drop the internal `getUnAckedMessageTracker().size() == 0` assertion (covered by unit 21). All 32 `[InlineData]` rows identical. | `NegativeAcknowledge_GivenVariations_ShouldRedeliverAllNegativelyAcknowledgedMessages` (needs batching.md for `batching=true`, ack-timeout.md for `ackTimeout=1000` and Failover redeliver-all) |
| `testNegativeAcksWithBackoff` (L205) | not portable (RedeliveryBackoff, OD-3) | — |
| `testNegativeAcksDeleteFromUnackedTracker` (L293) | adapted to unit test 21 | — |
| `testNegativeAcksWithBatch` (L336) | portable (needs batching.md: `batchingMaxPublishDelay(1 h)`, `batchingMaxMessages(2)`, batch-index ack) | `NegativeAcknowledge_GivenTwoMessagesInSameBatchNackedTwoSecondsApart_ShouldRedeliverFirstWithinTwoSeconds` |
| `testNegativeAcksWithBatchAckEnabled` (L374) | portable | `NegativeAcknowledge_GivenTenMessages_ShouldRedeliverAllAndThenNoMore` |
| `testFailoverConsumerBatchCumulateAck` (L420), `testMultiTopicConsumerConcurrentRedeliverAndReceive` (L494) | out of this WP (no nack; redeliver-all behaviour owned by ack-timeout.md / batching.md) | — |
| `testConfigureNegativeAckPrecisionBitCnt` (L557) | not portable (precision not configurable, OD-3) | — |

### 7.4 Transaction tests that depend on this WP (ported by the tests agents, listed for traceability)

- `TransactionEndToEndTest#testSendTxnAckMessageToDLQ` (L1545-1606): Shared, `maxRedeliverCount(1)`, producer `enableBatching(false)` + `sendTimeout(1 s)` (batching.md), DLQ consumer on `RetryMessageUtil.getDLQTopic(topic, "test")` = `<topic>-test-DLQ`. Flow: receive (count 0) -> txn ack -> abort; receive (count 1, registered as candidate) -> txn ack -> abort; third delivery (count 2 > 1) is skipped and dead-lettered by 4.5/4.7 -> `ReceiveOrNull(consumer, 5 s)` is null; DLQ consumer receives `"test"`. The `getAvailablePermits() == 3` assertion is Java-internal -> dropped (adapted).
- `TransactionEndToEndTest#testSendTxnAckBatchMessageToDLQ` (L1608-1683): same with a batched entry of `"test1"`,`"test2"`; requires batching.md + batch-index ack; `getAvailablePermits() == 6` dropped.
- Both are re-run by `TransactionEndToEndWithoutBatchIndexAckTest` (inherits).
- `TransactionEndToEndTest` L840 `ackTimeout(1, MINUTES)` -> ack-timeout.md, not this WP.

### 7.5 C++ cross-reference tests (not ported; confirm semantics)

`cpp:tests/DeadLetterQueueTest.cc`: `testDLQWithSchema` (L39, JSON schema registered on DLQ, nack), `testWithoutConsumerReceiveImmediately` (L99), `testAutoSetDLQTopicName` (L126 -> unit 1), `testSendDLQTriggerByAckTimeOutAndNeAck` (L184: asserts partition key, ordering key, user property `pk-1`, REAL_TOPIC contains topic, ORIGIN_MESSAGE_ID non-empty), `testSendDLQTriggerByRedeliverUnacknowledgedMessages` (L233), `testSendDLQTriggerByNegativeAcknowledge` (L281), `testInitSubscription` (L326); `cpp:tests/DeadLetterPolicyTest.cc` (builder validation -> unit 19).

---

## 8. Acceptance criteria

1. With `DeadLetterPolicy` unset, behaviour and allocations of receive/ack/redeliver are unchanged (existing test suite green; no new allocation per message — verify with a BenchmarkDotNet run or allocation assertion in an existing benchmark if present).
2. `MaxRedeliverCount == 0` -> `ConfigurationException("MaxRedeliverCount must be > 0.")` from both builder and `PulsarClient.CreateConsumer`.
3. On Shared/KeyShared, a message whose broker `redelivery_count` reaches `MaxRedeliverCount` is still delivered (count == max) and is registered; any later delivery with count > max is never surfaced to the application and the entry (all of its not-yet-acked batch messages) is published to the DLQ topic, then the original entry is individually acknowledged (non-transactional). Total deliveries to the app per message = `MaxRedeliverCount + 1`.
4. Redelivery triggered by ack timeout, by `RedeliverUnacknowledgedMessages(ids)`, by negative ack (WP-B) or by the broker after a transaction abort all dead-letter at the first opportunity at or after the threshold.
5. Default DLQ topic = `<subConsumerTopic>-<subscription>-DLQ` (per partition); custom topic honoured; `InitialSubscriptionName` creates the subscription on the DLQ topic when the DLQ producer is created, and no DLQ producer/topic is created before the first dead-lettering.
6. DLQ messages carry the original payload, properties (not overwritten), key (string or base64 bytes), ordering key, event time, plus `REAL_TOPIC`, `ORIGIN_MESSAGE_IDY_TIME`, `ORIGIN_MESSAGE_ID` (Java id string format), `REAL_SUBSCRIPTION`; producer name matches `^<topic>-<sub>-<consumer>-[a-zA-Z0-9]{5}-DLQ$`; schema info of the consumer is used for the DLQ producer.
7. Fully acknowledged entries are never dead-lettered; batch indexes acked via batch-index ack are excluded (with batching.md).
8. Failure to create the DLQ producer / send / ack never faults the consumer; the entry is redelivered normally; a faulted DLQ producer is disposed before a new one is created (at most one live DLQ producer per SubConsumer).
9. Consumer dispose disposes the DLQ producer(s) and nack timers; reconnect and seek clear candidates.
10. All tests in section 7 classified portable/adapted pass (WP-A tests without WP-B; WP-B tests with WP-B), with batching/chunking-dependent rows gated on those WPs.
11. `TransactionEndToEndTests` ports of `testSendTxnAckMessageToDLQ` (and the batch variant once batching.md lands) pass against `TransactionFixture`.

---

## 9. Open decisions (recommendation)

- **OD-1 Retry letter topic / `ReconsumeLater`** — Recommendation: out of scope for the transaction effort; do not add `RetryLetterTopic` to `DeadLetterPolicy` now (avoid dead configuration). Follow-up WP would add `IConsumer.ReconsumeLater(IMessage, TimeSpan, ...)`, `ConsumerOptions.EnableRetry`, `RECONSUMETIMES`/`DELAY_TIME`/`RETRY_TOPIC` properties, retry producer, subscribing to the retry topic (Java ConsumerBuilderImpl L167-222, ConsumerImpl#doReconsumeLater L715-840).
- **OD-2 DLQ producer customization** — Recommendation: omit `DeadLetterProducerBuilderCustomizer`. If needed later, expose `Action<IProducerBuilder<ReadOnlySequence<byte>>>` on `DeadLetterPolicy`, which requires building the DLQ producer through `ProducerBuilder` instead of `new Producer<>` (plus a way to pass `InitialSubscriptionName`).
- **OD-3 Negative ack backoff / precision** — Recommendation: fixed 8-bit precision, no `RedeliveryBackoff` in this WP.
- **OD-4 Include WP-DLQ-B (negative ack)** — Recommendation: yes, but as a separate PR after WP-DLQ-A; adding `IConsumer.NegativeAcknowledge` is an interface addition (same category as the already-decided transactional `Acknowledge` overloads).
- **OD-5 Ack target after dead-lettering** — Recommendation: ack the entry-level id (BatchIndex -1) rather than the possibly batch-indexed id Java acks (ConsumerImpl L2659). Rationale: all unacked messages of the entry were just published to the DLQ; acking one index would leave the entry pending and cause a second DLQ round. Observable upstream test outcomes are unchanged.
- **OD-6 Candidate key without partition** — Recommendation: key by `(LedgerId, EntryId)` per SubConsumer (4.1); avoids Java's DUMMY_PARTITION_INDEX miss for nack-triggered redelivery; same observable delivery counts.
- **OD-7 DLQ send timeout** — Recommendation: 30 s linked `CancellationTokenSource` until batching.md's send timeout exists, then switch to that option set to 30 s on the DLQ producer.
- **OD-8 Test placement** — Recommendation: `tests/DotPulsar.Tests/DeadLetterTopicTests.cs` and `tests/DotPulsar.Tests/NegativeAcknowledgeTests.cs` (integration, root of the test project next to `PulsarClientTests.cs`, `[Collection("Integration")]`), unit tests mirroring `src` under `tests/DotPulsar.Tests/Internal/`.
