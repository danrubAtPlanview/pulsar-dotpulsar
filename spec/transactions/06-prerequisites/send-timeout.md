# WP-PREREQ-4: Producer send timeout

Status: draft spec (no implementation). Target repo HEAD `e8f2ba2`.
Upstream pins: Java `apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026` (primary), C++ `apache/pulsar-client-cpp@f71d7e99019d58b1d11858f05ff0dc83b718627b` (cross-reference).

Depends on: `producer-batching.md` (shares `SendOp.CreatedTimestamp`, `AsyncQueueWithCursor.RemoveAll`, the relaxed `ProcessReceipt` rule, and the dispatcher). Consumed by: transaction core (a timed-out transactional send marks the transaction as having failed operations), ported tests using `sendTimeout(...)` (transaction corpus: `sendTimeout(0, SECONDS)` x54, `(5, SECONDS)` x5, `(1, SECONDS)` x4, `(60, SECONDS)` x1; `SimpleProducerConsumerTest`).

---

## 1. Goal and scope

Today a DotPulsar send never times out: the `SendOp` stays in the sub-producer's pending queue and is resent after every reconnect until it is acknowledged, the producer is disposed, or the caller's `CancellationToken` fires before the op is dispatched (`SubProducer.MessageDispatcher` drops cancelled ops, `SubProducer.cs L118-L122`; `Producer.Send` links the token to the receipt TCS, `Producer.cs L210`).

Add a producer-level `SendTimeout`:
- When > 0: if the oldest pending message of a sub-producer has not been acknowledged by the broker within `SendTimeout` of its creation, **all** messages pending in that sub-producer at that moment fail with `SendTimeoutException` (Java and C++ fail the whole pending queue, not only the expired message).
- When 0 (DotPulsar default, see Open decisions 1): no timeout (today's behaviour). Java `0` also means "infinite".

Out of scope: client-wide operation timeout (see `ack-receipt.md` Open decision 1), memory-limit/`blockIfQueueFull` interactions, chunk-level timeout accounting (chunking WP reuses this mechanism: a chunked message is one logical message; its chunks are separate ops that fail together).

---

## 2. Upstream mapping

| Concern | Java (primary) | C++ (cross-ref) | DotPulsar target |
|---|---|---|---|
| Option + default | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/conf/ProducerConfigurationData.java#sendTimeoutMs (L72-L76)` = 30000; `#setSendTimeoutMs (L314-L317)` (`checkArgument(sendTimeout >= 0, "sendTimeout needs to be >= 0")`) | `cpp:include/pulsar/ProducerConfiguration.h#setSendTimeout (L148)`, `#getSendTimeout (L160)` (default 30000) | `ProducerOptions.SendTimeout` |
| Builder | `java:.../impl/ProducerBuilderImpl.java#sendTimeout (L158-L161)` | - | `IProducerBuilder.SendTimeout(TimeSpan)` |
| Timer start | `java:.../impl/ProducerImpl.java` ctor (L282-L284) (`if (conf.getSendTimeoutMs() > 0) sendTimeout = client.timer().newTimeout(this, sendTimeoutMs, MILLISECONDS)`) | `cpp:lib/ProducerImpl.cc#startSendTimeoutTimer (L1052-L1056)`, `#asyncWaitSendTimeout (L1058-L1068)` | `SubProducer` timeout loop |
| Timeout task | `ProducerImpl.java#run(Timeout) (L2574-L2632)` | `cpp:lib/ProducerImpl.cc#handleSendTimeout (L840-L893)` | `SubProducer.SendTimeoutLoop` |
| "createdAt" of the oldest message | `ProducerImpl.java L2591-L2603` (first pending op's `createdAt`; if no op but a non-empty batch container: `firstAddedTimestamp + batchingMaxPublishDelay`) | `cpp:lib/OpSendMsg.h#timeout (L54, L99)` (`now + sendTimeoutMs` at op creation) | `SendOp.CreatedTimestamp` (+ `BatchingMaxPublishDelay` when batching) |
| Fail everything pending | `ProducerImpl.java#failPendingMessages (L2638-L2693)` (fails the ops present at the start, keeps ops re-enqueued synchronously by callbacks), `#failPendingBatchMessages (L2732-L2743)` | `cpp:lib/ProducerImpl.cc#getPendingCallbacksWhenFailed (L318-L347)` | `AsyncQueueWithCursor.RemoveAll` |
| Exception | `java:pulsar-client-api/src/main/java/org/apache/pulsar/client/api/PulsarClientException.java#TimeoutException` (carries `sequenceId`); message `"The producer %s can not send message to the topic %s within given timeout"` (`ProducerImpl.java L2612-L2613`), decorated per op in `OpSendMsg.sendComplete (L1925-L1951)` with `createdAt/firstSentAt/lastSentAt/retryCount` | `ResultTimeout` | `SendTimeoutException` |
| Late receipt for a timed-out op | `ProducerImpl.java#ackReceived (L1538-L1544, L1555-L1563)` (empty queue or `sequenceId < op.sequenceId` -> ignore "Got ack for timed out msg") | `cpp:lib/ProducerImpl.cc#ackReceived (L932-L937, L955-L960)` | `SubProducer.ProcessReceipt` (defined in `producer-batching.md` 4.9) |
| Transaction effect | `java:.../impl/transaction/TransactionImpl.java#registerSendOp (L124-L145)` (exceptional completion -> `hasOpsFailed = true`), `#commit (L187-L215)` (`hasOpsFailed` -> abort + `TransactionHasOperationFailedException`), `java:.../impl/TypedMessageBuilderImpl.java#sendAsync (L131-L141)` | (no txn) | transaction WP observes the per-message task |

Upstream tests (section 8): `java:pulsar-broker/src/test/java/org/apache/pulsar/client/api/SimpleProducerConsumerTest.java#testSendTimeout (L709-L744)`, `#testSendTimeoutAndRecover (L747-L...)`, `#testSendCallBackReturnSequenceId (L1390-...)`, sendTimeout validation (L899-L904); `java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/ProducerBuilderImplTest.java#testProducerBuilderImplWhenSendTimeoutIsNegative (L257-L265)`, `#testProducerBuilderImplWhenSendTimeoutPropertyIsNegative (L415-L419)`; `java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/ProducerImplTest.java#testFailPendingMessagesSyncRetry (L182-L258)`, `#testSendTimeoutDuringInFlightWriteDefersCmdRelease (L1432-)`, `#testSendTimeoutReleasesCmdInlineWhenNoWriteInFlight (L1496-)`.

---

## 3. Public API additions

### 3.1 `src/DotPulsar/ProducerOptions.cs`

```csharp
/// <summary>
/// The default send timeout.
/// </summary>
public static readonly TimeSpan DefaultSendTimeout = TimeSpan.Zero;

/// <summary>
/// Set the send timeout. If a message is not acknowledged by the broker within this time, all pending messages of the producer (partition) fail with a SendTimeoutException. TimeSpan.Zero disables the timeout. The default is 'TimeSpan.Zero'.
/// </summary>
public TimeSpan SendTimeout { get; set; }
```

Constructor: `SendTimeout = DefaultSendTimeout;`.

### 3.2 `src/DotPulsar/Abstractions/IProducerBuilder.cs` + `src/DotPulsar/Internal/ProducerBuilder.cs`

```csharp
/// <summary>
/// Set the send timeout. TimeSpan.Zero disables the timeout. The default is 'TimeSpan.Zero'.
/// </summary>
IProducerBuilder<TMessage> SendTimeout(TimeSpan sendTimeout);
```

`ProducerBuilder`: field `_sendTimeout = ProducerOptions<TMessage>.DefaultSendTimeout`; setter returns `this`; `Create()`: `if (_sendTimeout < TimeSpan.Zero) throw new ConfigurationException("ProducerOptions.SendTimeout must be greater than or equal to zero");` (Java: "sendTimeout needs to be >= 0", `IllegalArgumentException` at set time; DotPulsar validates in `Create()` like the topic check), then copy to options.

`src/DotPulsar/PulsarClient.cs#CreateProducer` (L59-L80): same validation (options may bypass the builder).

### 3.3 `src/DotPulsar/Exceptions/SendTimeoutException.cs` (new)

```csharp
namespace DotPulsar.Exceptions;

/// <summary>
/// The message could not be sent within the configured send timeout.
/// </summary>
public sealed class SendTimeoutException : DotPulsarException
{
    /// <summary>
    /// Initializes a new instance using the specified message and sequence id.
    /// </summary>
    public SendTimeoutException(string message, ulong sequenceId) : base(message) => SequenceId = sequenceId;

    /// <summary>
    /// The sequence id of the message that timed out.
    /// </summary>
    public ulong SequenceId { get; }
}
```

(Check `DotPulsarException`'s constructors; use the `(string message)` base constructor as other exceptions in `src/DotPulsar/Exceptions/` do.) Message: `$"The producer '{producerName}' can not send message to the topic '{topic}' within given timeout"` where `producerName` is `ProducerOptions.ProducerName` or, when null, the broker-assigned name of the current channel if known, else empty. Java's per-op suffix (`createdAt X seconds ago, firstSentAt ..., lastSentAt ..., retryCount N`) is omitted (Open decisions 4).

The exception never reaches the executor (it is set directly on the receipt TCS), so `DefaultExceptionHandler` needs no entry.

---

## 4. Behaviour (normative)

4.1 Each sub-producer (partition) has its own timer, started when the sub-producer is created if `SendTimeout > TimeSpan.Zero` (Java: one per `ProducerImpl`, i.e. per partition). It runs independently of the connection state (timeouts fire while disconnected - the main use case: Java tests stop the broker).

4.2 Timer tick (port of `ProducerImpl.run`, L2574-L2632):
```
if disposed: stop
head = oldest SendOp in the pending queue
if head is null: next = SendTimeout
else:
    createdAt = head.CreatedTimestamp + (EnableBatching ? BatchingMaxPublishDelay : 0)
    diff = SendTimeout - elapsed(createdAt)
    if diff <= 0:
        failed = queue.RemoveAll()          // every op pending right now, grouped or not, sent or not
        complete each op of 'failed' with SendTimeoutException(message, op.Metadata.SequenceId) (outside the lock)
        next = SendTimeout
    else next = diff
schedule next tick after 'next'
```
- The `+ BatchingMaxPublishDelay` term mirrors Java's treatment of messages still in the batch container ("we consider them createdAt when they would have otherwise been flushed", L2599-L2602). In DotPulsar every pending message - grouped or still ungrouped - is a `SendOp` in the queue stamped at creation, so the delay is added whenever batching is enabled (upper bound of Java's batch-op creation time).
- Ops that the producer is still blocked on enqueueing (queue full, `MaxPendingMessages`) are not in the queue and are not timed (Java: `canEnqueueRequest` blocks before an op exists).

4.3 Failing: every removed op's `ReceiptTcs.TrySetException(new SendTimeoutException(msg, op.Metadata.SequenceId))` (one instance per op so each carries its own sequence id - Java `failPendingMessages` sets `ex.setSequenceId(op.sequenceId)` per op on a shared instance, L2648; `testSendCallBackReturnSequenceId` asserts per-message sequence ids 0, 1, 2). The first op of each frozen batch returns its `PreparedBatch` to the pool. Completion happens outside any lock; continuations already run asynchronously (`Producer.InternalSend` uses `ContinueWith`). Ops enqueued concurrently after `RemoveAll` are not affected (Java L2643-L2645 counts ops "at the start of the call").

4.4 `Producer.InternalSend` continuation (`Producer.cs L268-L304`): a faulted receipt task with `SendTimeoutException` flows through the existing failure branch: activity failed, auto-assigned `metadata.SequenceId` reset to 0, `onFailed` invoked; `Send(...)` throws the `SendTimeoutException` (unwrapped from the TCS); `SendChannel` callbacks are not invoked for failed messages (existing behaviour for any failure).

4.5 Late broker receipts for removed ops are ignored (`producer-batching.md` 4.9: queue empty -> ignore; `receipt.SequenceId < head` -> ignore). A batch or message that timed out on the client may still have been persisted by the broker (Java has the same at-least-once caveat; with deduplication enabled a later resend of the same sequence id is dropped by the broker).

4.6 Dispatcher consistency: the timeout path and the dispatcher/receipt paths mutate the same queue. All compound queue operations of `SubProducer` (dispatcher batch building and freezing, `SendFrozenGroup` member walk, `ProcessReceipt`, `FailHead`, timeout `RemoveAll`) run under one new `private readonly object _queueSync` (synchronous sections only; never held across an `await`). `RemoveAll` increments `private long _queueGeneration`. The dispatcher records the generation when it starts building/sending a group and re-checks it (under `_queueSync`) after every `await` (`WaitForNextItem`, `channel.Send`): if it changed, it abandons the in-progress group (`container.Clear()`; the ops were already failed) and continues with `NextItem` (which restarts from the queue head because `RemoveAll` reset the cursor). A `channel.Send` already in flight completes normally; its receipt is ignored (4.5).

4.7 `SendTimeout == TimeSpan.Zero`: no timer task is created; behaviour identical to today.

4.8 Disposal: `SubProducer.DisposeAsync` cancels the timer (`CancellationTokenSource`) and awaits the loop; remaining ops are cancelled as today (`AsyncQueueWithCursor.DisposeAsync` -> `SendOp.Dispose` -> `TrySetCanceled`).

4.9 Producer state: a send timeout does **not** change `ProducerState` (Java keeps the producer usable; `testSendTimeoutAndRecover` sends successfully after the broker restarts).

4.10 Transactions: `IMessageBuilder<T>.Transaction(txn).Send()` registers the per-message receipt task with the transaction (transaction WP, Java `TypedMessageBuilderImpl.sendAsync` L131-L141 + `TransactionImpl.registerSendOp` L124-L145). A `SendTimeoutException` therefore sets the transaction's "operation failed" flag; `Commit` then aborts and throws the transaction WP's equivalent of `TransactionHasOperationFailedException` (Java `commit` L192-L196). Note Java's `commit` waits for **all** registered ops to complete (`opFuture`); with `SendTimeout = 0` and an unreachable broker a commit waits until the ops complete or the transaction times out on the coordinator - this is why most Java transaction tests use `sendTimeout(0, SECONDS)` explicitly (to keep Java's default 30 s timeout out of the way). Batching: a timed-out batch fails every message in it; each of those messages' tasks marks the transaction (all messages of one batch belong to the same transaction, `producer-batching.md` `HasSameTxn`).

---

## 5. Internal design

### 5.1 `SubProducer` (`src/DotPulsar/Internal/SubProducer.cs`)

New constructor parameters (appended after the batching ones from `producer-batching.md`): `TimeSpan sendTimeout, string? producerName`. New fields:

```csharp
private readonly TimeSpan _sendTimeout;
private readonly long _sendTimeoutDelayTicks;   // Stopwatch ticks of BatchingMaxPublishDelay when batching enabled, else 0
private readonly object _queueSync;
private long _queueGeneration;
private readonly CancellationTokenSource? _sendTimeoutCts;
private readonly Task? _sendTimeoutTask;
```

Constructor (after existing assignments, `SubProducer.cs L43-L66`): `if (sendTimeout > TimeSpan.Zero) { _sendTimeoutCts = new CancellationTokenSource(); _sendTimeoutTask = Task.Run(() => SendTimeoutLoop(_sendTimeoutCts.Token)); }`.

```csharp
private async Task SendTimeoutLoop(CancellationToken cancellationToken)
{
    var next = _sendTimeout;
    while (!cancellationToken.IsCancellationRequested)
    {
        try { await Task.Delay(next, cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) { return; }

        List<SendOp>? failed = null;
        lock (_queueSync)
        {
            if (!_sendQueue.TryPeek(out var head) || head is null)
                next = _sendTimeout;
            else
            {
                var elapsed = StopwatchElapsed(head.CreatedTimestamp + _sendTimeoutDelayTicks);   // TimeSpan; negative elapsed is possible right after enqueue with batching -> diff > timeout -> fine
                var diff = _sendTimeout - elapsed;
                if (diff <= TimeSpan.Zero)
                {
                    failed = _sendQueue.RemoveAll();
                    _queueGeneration++;
                    next = _sendTimeout;
                }
                else next = diff;
            }
        }

        if (failed is not null)
            FailTimedOut(failed);
    }
}

private void FailTimedOut(List<SendOp> failed)
{
    var message = $"The producer '{_producerName}' can not send message to the topic '{Topic}' within given timeout";
    foreach (var op in failed)
    {
        op.ReceiptTcs.TrySetException(new SendTimeoutException(message, op.Metadata.SequenceId));
        if (op.BatchCount > 1) { op.Batch?.Return(); op.Batch = null; }
    }
}
```

`StopwatchElapsed(long timestamp)` = `TimeSpan.FromTicks((long) ((Stopwatch.GetTimestamp() - timestamp) * (double) TimeSpan.TicksPerSecond / Stopwatch.Frequency))` (netstandard2.0-compatible; on net8.0+ `Stopwatch.GetElapsedTime(timestamp)` via `#if NET7_0_OR_GREATER`).

`InternalDispose` (`SubProducer.cs L77-L93`): `_sendTimeoutCts?.Cancel()`, `await (_sendTimeoutTask ?? Task.CompletedTask)`, dispose the CTS, before disposing the queue.

`_producerName`: from the constructor (`ProducerOptions.ProducerName`); if null, `ProducerChannelFactory`/`ProducerChannel` exposes the broker-assigned name: add `string Name { get; }` to `IProducerChannel` (`ProducerChannel` returns `_name`; `NotReadyChannel` returns `string.Empty`) and use `_channel.Name` when `_producerName` is null.

### 5.2 Queue lock integration (`producer-batching.md` sections 4.7, 4.9, 4.13)

Wrap in `lock (_queueSync)`:
- `ProcessReceipt` (whole body).
- `FailHead` (per-op send error).
- Dispatcher: each synchronous step that inspects/moves the cursor or mutates ops (`NextItem` result validation, `TryPeekNextItem` + `MoveNext` + `container.Add`, `CreateBatches` + freezing + `RewriteItemsUpToCursor` + `MoveCursorBack`, `SendFrozenGroup` member walk). `NextItem` itself is awaited outside the lock (it has its own internal lock); after it returns, re-check under `_queueSync` that the item is still in the queue (`op.BatchCount != -2` - set `BatchCount = -2` ("removed") on every op returned by `RemoveAll`, inside the lock, so the dispatcher can detect removal in O(1)).
- Generation check after each `await` as described in 4.6.

`AsyncQueueWithCursor.RemoveAll` (`producer-batching.md` 4.3) must: take its internal lock, copy all values into a new `List<T>` (in queue order), `_queue.Clear()`, `_currentNode = null`, **not** cancel `_cursorNextItemTcs` (a waiting `NextItem` keeps waiting for new items), `NotifyQueueEmptyAwaiters()` (so `SendChannel.Completion()`/`WaitForSendQueueEmpty` returns), and `ReleasePendingLockGrant()` (unblocks enqueuers waiting for space).

### 5.3 `Producer<TMessage>` (`src/DotPulsar/Internal/Producer.cs`)

`CreateSubProducer` (L156-L172): pass `_options.SendTimeout` and `_options.ProducerName` to the `SubProducer` constructor. No other change (4.4).

### 5.4 Non-goals / unchanged

- `SendOp.CancellationToken` semantics unchanged.
- Executor retry policy unchanged (timeouts do not go through the executor).

---

## 6. Exact DotPulsar edit points

| File | Member | Change |
|---|---|---|
| `src/DotPulsar/ProducerOptions.cs` | static, property, ctor | 3.1 |
| `src/DotPulsar/Abstractions/IProducerBuilder.cs` | `SendTimeout(TimeSpan)` | 3.2 |
| `src/DotPulsar/Internal/ProducerBuilder.cs` | field, setter, `Create()` validation + copy | 3.2 |
| `src/DotPulsar/PulsarClient.cs` | `CreateProducer` validation | 3.2 |
| `src/DotPulsar/Exceptions/SendTimeoutException.cs` | new | 3.3 |
| `src/DotPulsar/Internal/SubProducer.cs` | ctor, `SendTimeoutLoop`, `FailTimedOut`, `InternalDispose`, `_queueSync` around queue sections | 5.1, 5.2 |
| `src/DotPulsar/Internal/SendOp.cs` | `CreatedTimestamp` (from `producer-batching.md` 4.2) | |
| `src/DotPulsar/Internal/AsyncQueueWithCursor.cs` | `RemoveAll` (from `producer-batching.md` 4.3) with semantics of 5.2 | |
| `src/DotPulsar/Internal/Abstractions/IProducerChannel.cs`, `ProducerChannel.cs`, `NotReadyChannel.cs` | `Name` | 5.1 |
| `src/DotPulsar/Internal/Producer.cs` | `CreateSubProducer` (L156-L172) | 5.3 |

---

## 7. Performance notes

- Disabled (default): zero cost (no task, no timestamps read besides the one `Stopwatch.GetTimestamp()` per `SendOp` that `producer-batching.md` already adds; ~20 ns).
- Enabled: one long-lived task per sub-producer waking at most once per `SendTimeout` when idle, or at the head message's deadline; one `Task.Delay` allocation per tick (Java/C++ also allocate a timer task per tick). No per-message work besides the timestamp.
- `_queueSync` adds one uncontended `Monitor` enter/exit per dispatcher step/receipt; contention only with the timer tick.

---

## 8. Tests

### 8.1 Unit tests (`[Trait("Category", "Unit")]`)

`tests/DotPulsar.Tests/Internal/ProducerBuilderTests.cs` (extend) - ports:
- `// Upstream: ProducerBuilderImplTest#testProducerBuilderImplWhenSendTimeoutIsNegative` -> `Create_GivenNegativeSendTimeout_ShouldThrowConfigurationException` (Java `sendTimeout(-1, MILLISECONDS)` -> `TimeSpan.FromMilliseconds(-1)`; Java expects `IllegalArgumentException`, DotPulsar `ConfigurationException`)
- `// Upstream: ProducerBuilderImplTest#testProducerBuilderImplWhenSendTimeoutPropertyIsNegative` -> `Create_GivenSendTimeoutOfMinusOneSecond_ShouldThrowConfigurationException` (`TimeSpan.FromSeconds(-1)`)
- `Create_GivenSendTimeout_ShouldCopyItToProducerOptions`
- `Create_GivenDefaultOptions_ShouldHaveZeroSendTimeout`

`tests/DotPulsar.Tests/PulsarClientTests.cs` (extend): `CreateProducer_GivenNegativeSendTimeout_ShouldThrowConfigurationException`.

`tests/DotPulsar.Tests/Internal/SubProducerTests.cs` (extend; fake channel whose `Send` never produces a receipt, short timeouts such as 200 ms; use generous assertion windows):
- `SendTimeout_GivenNoReceipt_ShouldFailPendingMessageWithSendTimeoutException`
- `SendTimeout_GivenThreePendingMessages_ShouldFailAllWithTheirOwnSequenceIds` (port of the assertion in `SimpleProducerConsumerTest#testSendCallBackReturnSequenceId`: sequence ids 0, 1, 2)
- `SendTimeout_GivenReceiptBeforeDeadline_ShouldNotFail`
- `SendTimeout_GivenLateReceiptAfterTimeout_ShouldIgnoreItAndKeepProducerConnected`
- `SendTimeout_GivenMessageEnqueuedAfterTimeout_ShouldNotBeFailedByThatTimeout` (port of the intent of `ProducerImplTest#testFailPendingMessagesSyncRetry` L182-L258: an op enqueued from the failure continuation of a timed-out op survives the same tick)
- `SendTimeout_GivenBatchingEnabled_ShouldAddBatchingMaxPublishDelayToDeadline`
- `SendTimeout_GivenFrozenBatch_ShouldFailAllMessagesOfTheBatchAndReturnBufferOnce`
- `SendTimeout_GivenTimeoutDuringBatchBuilding_ShouldAbandonGroupAndContinue` (generation check)
- `SendTimeout_GivenZero_ShouldNotStartTimer` (no failure after 1 s with a never-answering channel)
- `SendTimeout_GivenQueueFullAndEnqueuerBlocked_ShouldUnblockEnqueuerAfterTimeout` (`MaxPendingMessages = 1`)
- `SendTimeout_GivenDispose_ShouldStopTimer`
- `WaitForSendQueueEmpty_GivenTimeoutRemovedAllMessages_ShouldComplete`

`ProducerImplTest#testSendTimeoutDuringInFlightWriteDefersCmdRelease` (L1432-) / `#testSendTimeoutReleasesCmdInlineWhenNoWriteInFlight` (L1496-): NOT PORTED as such (Netty event-loop buffer ownership); their intent ("buffers of a timed-out op are released exactly once, even while a write is in flight") is covered by `SendTimeout_GivenFrozenBatch_ShouldFailAllMessagesOfTheBatchAndReturnBufferOnce` plus `SendTimeout_GivenTimeoutWhileChannelSendInFlight_ShouldReturnBufferOnlyAfterSendCompletes` (the dispatcher must not return `PreparedBatch` to the pool while `channel.Send` is still serialising it: `FailTimedOut` only returns the batch if the dispatcher is not currently sending that group - track with `SendOp.IsSending` (`int` set/cleared by the dispatcher under `_queueSync`); if sending, the dispatcher returns it after `channel.Send` completes when it observes the generation change).

### 8.2 Integration tests

`tests/DotPulsar.Tests/Prerequisites/SimpleProducerConsumerTests.cs` (`[Collection("Integration"), Trait("Category", "Integration")]`, `IntegrationFixture`; Java `stopBroker()`/`startBroker()` -> `await using (await _fixture.DisableThePulsarConnection())` Toxiproxy scope):

| Java | C# | Parameters / expectations |
|---|---|---|
| `testSendTimeout(int batchMessageDelayMs)` (L709-L744), provider `batch` (L425-L428) `{0},{1000}` | `Send_GivenBrokerUnreachable_ShouldFailWithSendTimeoutException` `[Theory] [InlineData(0)] [InlineData(1000)]` | `SendTimeout(TimeSpan.FromSeconds(1))`; when `batchMessageDelayMs != 0`: `EnableBatching(true)`, `BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(batchMessageDelayMs))`, `BatchingMaxMessages(5)`; message `"my-message"`; send throws (`SendTimeoutException`); after reconnect `ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), ct)` returns null (Java `RECEIVE_TIMEOUT_SECONDS = 5 * TIMEOUT_MULTIPLIER`, L151; use 5 s) |
| `testSendTimeoutAndRecover(int batchMessageDelayMs)` (L746-...) | `Send_GivenSendTimeoutAndBrokerRecovers_ShouldSendAfterRecovery` | 6-partition topic; Java asserts `flush()` throws once (`handleOnce` semantics of `lastSendFutureWrapper`) and `getOriginalLastSendFuture()` - **adapted**: DotPulsar `Flush` rethrows the last failed send each time (no "handle once"); assert first `Flush` throws `SendTimeoutException`, then a new send after recovery succeeds. The Java `InvalidServiceURL` assertion on the original future is Java-internal and dropped |
| sendTimeout `-1` validation (L899-L904) | `Create_GivenNegativeSendTimeout_ShouldThrow` (integration-free; covered in 8.1) | |
| `testSendCallBackReturnSequenceId` (L1390-...) | `Send_GivenBrokerUnreachable_ShouldFailEachMessageWithItsSequenceId` | `EnableBatching(false)`, `SendTimeout(1 s)`, three messages with `SequenceId = 0, 1, 2` (DotPulsar: `0` means "auto-assign" - use explicit ids `1, 2, 3`? **No**: to keep values identical, set `InitialSequenceId(0)` and send without explicit ids so auto-assignment yields 0, 1, 2; assert `SendTimeoutException.SequenceId` equals 0, 1, 2) |
| `SimpleProducerConsumerTest` L5015-L5035 (`batchingMaxBytes`, `sendTimeout(10, SECONDS)`, message `2 x batchingMaxBytes` must complete in time) | `Send_GivenMessageLargerThanBatchingMaxBytes_ShouldCompleteWithinSendTimeout` | exercises `producer-batching.md` 6.3 + timeout together |

Transaction tests with non-zero send timeouts (`(1, SECONDS)` x4, `(5, SECONDS)` x5, `(60, SECONDS)` x1) and the 54 `sendTimeout(0, SECONDS)` producers are ported by the transaction WP as `.SendTimeout(TimeSpan.FromSeconds(n))` / `.SendTimeout(TimeSpan.Zero)`.

---

## 9. Acceptance criteria

1. Default options: no timer task is created; existing tests unchanged.
2. `SendTimeout = 1 s`, broker unreachable: `Send` throws `SendTimeoutException` after ~1 s (+ `BatchingMaxPublishDelay` when batching), for both batched and non-batched producers; the producer state is not `Faulted`; after the broker is reachable again new sends succeed and the timed-out message is not consumed (port of `testSendTimeout`).
3. All messages pending in the sub-producer at the timeout tick fail, each with its own `SequenceId`; messages enqueued afterwards are unaffected.
4. Enqueuers blocked on `MaxPendingMessages` are released when the timeout empties the queue.
5. Late receipts for timed-out messages are ignored without reconnecting the producer.
6. A timed-out transactional send makes the transaction's `Commit` fail (transaction WP acceptance test).
7. Pooled batch buffers are returned exactly once on timeout, including when the timeout races an in-flight `channel.Send`.

---

## 10. Open decisions (recommendation)

1. **Default `SendTimeout`**: Java `30 s` (`ProducerConfigurationData.java L76`), C++ `30 s`. **Recommendation: `TimeSpan.Zero` (disabled)** to preserve DotPulsar's documented "retry until sent or cancelled" behaviour; users opt in. Consequence for test porting: Java tests that rely on the 30 s default without calling `sendTimeout(...)` and assert a timeout must set `.SendTimeout(TimeSpan.FromSeconds(30))` explicitly (none found in the transaction corpus; all transaction producers set `sendTimeout` explicitly or do not depend on it).
2. **Exception type**: recommendation new `SendTimeoutException : DotPulsarException` with `SequenceId` (needed by `testSendCallBackReturnSequenceId`). Alternative `TimeoutException` (BCL) - rejected: no sequence id, not a `DotPulsarException`.
3. **Fail-all vs fail-expired-only**: recommendation fail-all (Java and C++ parity: once the head expires, ordering guarantees are lost anyway, and Java documents fail-all).
4. **Exception message detail**: Java appends `createdAt/firstSentAt/lastSentAt/retryCount`. Recommendation: omit (DotPulsar does not track send attempts); tests must not assert message text.
5. **Interaction with `SendOp.CancellationToken`**: unchanged - a caller can still cancel earlier than the timeout as long as the message is not yet part of a frozen batch / sent.
6. **Timer granularity**: `Task.Delay` (~15 ms on Windows). Java uses a hashed wheel timer (`client.timer()`, default tick 1 ms). Acceptable; tests must use tolerances >= 500 ms.
