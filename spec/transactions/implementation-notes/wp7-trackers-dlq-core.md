# WP7 core as built: ack-timeout tracker, negative-ack tracker, dead-letter handler (not wired)

New files (no existing file touched): public `DeadLetterPolicy`; internal `UnAckedMessageTracker`, `ConsumerEpoch`, `NegativeAcknowledgeTracker`, `DeadLetterHandler`, `DeadLetterSchema`,
`RetryMessageUtil` (property-name constants, `GetDeadLetterTopic`, `FormatMessageId`), `OneShotTimer`, `Abstractions/IOneShotTimer` (injectable timer, not in the spec).
Mutation checks (bucket count off by one, nack entry not removed after redelivery, DLQ candidate removed before sending) failed 21 tests.

## Differences from the spec

1. `UnAckedMessageTracker` takes an optional `Func<Action, IOneShotTimer>`; `NegativeAcknowledgeTracker` also takes an optional `Func<TimeSpan>` monotonic clock (same convention as `Backoff`).
2. Both trackers use `Func<MessageId[], CancellationToken, ValueTask>` as the redeliver delegate (the token is cancelled on `Dispose`).
3. Keys are `(ulong LedgerId, ulong EntryId)` tuples; bucket dictionaries start empty (a large ack timeout over a small tick must not pre-allocate thousands of arrays).
4. A nack for an entry that is already pending is ignored and keeps its first deadline (Java would redeliver the entry twice). `Add` returns a bool; a test pins Java's `testNegativeAcksWithBatch`.
5. `Dispose` is terminal for the nack tracker (`Add` returns false afterwards); the ack-timeout tracker keeps working after `Dispose` (Java's `testAddAndRemove`). The nack timer is created on the first `Add`; there is no `Start()`;
   the delay is clamped to at least 100 ms.
6. DLQ producer factory: `Func<ProducerOptions<ReadOnlySequence<byte>>, string?, IProducer<ReadOnlySequence<byte>>>`; the `string?` is the initial subscription name.
   `ProducerOptions.InitialSubscriptionName` is an edit to an existing file, left to the wiring step.
7. `TryDeadLetter`: cancellation by the caller's token is rethrown; a send timeout (default 30 s, optional ctor parameter) or any other failure returns false and keeps the entry.
   `Register` with an empty batch array removes any earlier registration for that entry.
8. An empty message key is not copied to the DLQ message (a received message with no key has `Key == ""`; copying it would give DLQ messages a routing key). Differs from Java parity.
9. The DLQ producer does not enable chunking and has no recreation backoff (neither exists in DotPulsar yet); `MaxPendingMessages = 500`.
10. No retry-topic derivation (OD-1). The chunk-id port of `testTrackChunkedMessageId` is reduced; the expansion test is deferred to wiring.

## Wiring call surface

### `UnAckedMessageTracker`, one per `SubConsumer`

Create it only when `AckTimeout > 0` and the topic does not start with `non-persistent://`.

```csharp
SubConsumer<TMessage>? subConsumer = null;
var tracker = new UnAckedMessageTracker(options.AckTimeout, options.AckTimeoutTickTime, (ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct));
// pass the tracker and the ConsumerEpoch to ConsumerChannelFactory and SubConsumer
subConsumer = new SubConsumer<TMessage>(..., tracker, epoch);
tracker.Start();   // after the SubConsumer exists; idempotent; first tick is one tick later
```

| Where | Call |
|---|---|
| `Consumer.Receive`, at the 3 return points | `subConsumer.Track(m)` → `tracker?.Add(m.MessageId)` |
| Individual ack, entry complete (`BatchIndex == -1`) | `tracker?.Remove(ledgerId, entryId)` |
| Cumulative ack, at method entry | `tracker?.RemoveMessagesTill(ledgerId, entryId)` |
| Transactional ack | the same two rules, at send time, before awaiting the response |
| `NegativeAcknowledge` | `tracker?.Remove(messageId)` |
| Redeliver-all | `tracker?.Clear()` |
| Encryption `Fail` action | `tracker?.Add(id)` |
| `SubConsumer.DisposeAsync` and `ChannelFaulted` | `tracker?.Dispose()` |
| Test hook | `tracker?.Count ?? 0` |

The tracker is not cleared on reconnect or seek. The ids passed to `RedeliverUnacknowledgedMessages` may carry a batch index or be chunk ids, so the redelivery primitive must convert them to entry level and de-duplicate.

### `ConsumerEpoch` (one per `SubConsumer`, shared with `ConsumerChannelFactory`)

`epoch.Value` (long); `epoch.Increment()` on redeliver-all for Exclusive and Failover.

### `NegativeAcknowledgeTracker` (one per `SubConsumer`)

```csharp
var nack = new NegativeAcknowledgeTracker(options.NegativeAcknowledgeRedeliveryDelay, (ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct));
```

`SubConsumer.NegativeAcknowledge(id, ct)`: `Guard(); _unAcked?.Remove(id); nack.Add(id); return default;`. Call `nack.Dispose()` in `DisposeAsync`.

### `DeadLetterHandler<TMessage>` (one per `SubConsumer` when the policy is set)

```csharp
var handler = new DeadLetterHandler<TMessage>(policy, topic, subscriptionName, consumerName, schema.SchemaInfo,
    (producerOptions, initialSubscription) =>
    {
        producerOptions.InitialSubscriptionName = initialSubscription;   // internal option added by wiring
        return new Producer<ReadOnlySequence<byte>>(ServiceUrl, producerOptions, exceptionHandler, connectionPool, null);
    });
handler.RedeliveryCountExceeded = id => subConsumer!.RedeliverUnacknowledgedMessages([id], CancellationToken.None);
```

| Where | Call |
|---|---|
| On receive, non-batched, `redeliveryCount >= handler.MaxRedeliverCount` | `handler.Register(l, e, message)`; if `redeliveryCount > Max`, `handler.OnRedeliveryCountExceeded(entryId)` and skip the message |
| On receive, batched | `handler.Register(l, e, messages.ToArray())` (only not-yet-acked ones); if `redeliveryCount > Max`, `handler.OnRedeliveryCountExceeded(entryLevelId)` and enqueue nothing |
| Individual ack, entry complete | `handler.Remove(l, e)` |
| Reconnect and seek | `handler.Clear()` |
| `SubConsumer.RedeliverUnacknowledgedMessages(ids)` on Shared and KeyShared | `Task.WhenAll(entries.Select(id => handler.TryDeadLetter(id, Acknowledge, ct)))`, outside the executor; redeliver only the entries that returned false |
| `SubConsumer.DisposeAsync` | `await handler.DisposeAsync()` |

`OnRedeliveryCountExceeded` is fire-and-forget and never throws. `TryDeadLetter` takes the `SubConsumer.Acknowledge` method group as its acknowledge delegate.
Still to do in wiring: `DeadLetterPolicy` validation (`MaxRedeliverCount > 0`), the `ConsumerOptions` and builder members and defaults, the public negative-acknowledge API.
