# Component: Consumer integration

| | |
|---|---|
| C# | `Abstractions/IConsumer.cs`, `Extensions/ConsumerExtensions.cs`, `Internal/Consumer.cs`, `Internal/SubConsumer.cs`, `Internal/Abstractions/IConsumerChannel.cs`, `Internal/ConsumerChannel.cs`, `Internal/NotReadyChannel.cs` |
| Upstream | `java:J/ConsumerBase.java` (L594-619, L687-776, L867-869), `java:J/ConsumerImpl.java#doAcknowledge` (L654-698), `#doTransactionAcknowledgeForResponse` (L3520-3585, list overload L3631-3666), `java:J/MultiTopicsConsumerImpl.java` (L491-549) |
| WP | WP5 (depends on WP5a batch-index ack) |

All DotPulsar line numbers refer to baseline `e8f2ba22c7136a35d279451e7278589fb0084d2e`. Java citations use `java:<path>#Symbol (Lx-Ly)` at `f3846f4811b6a7d98de40a3be2caff1fe288e026`. Abbreviations: `J=pulsar-client/src/main/java/org/apache/pulsar/client/impl`, `B=pulsar-broker/src/main/java/org/apache/pulsar/broker/service`, `C=pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java`.

> Line numbers drift as earlier WPs land. Locate each edit by the anchor text (method name or neighbouring member), and use the number only as a hint.

**Precedence:** [03-public-api](../03-public-api.md) is authoritative for public signatures. It also adds `ConsumerExtensions.Acknowledge(this IConsumer, IEnumerable<IMessage>, ITransaction, ct)` and its non-transactional twin. [transaction §8](transaction.md#8-helpers-used-by-the-producer-and-consumer-integration) defines `Transaction.From`, `EnsureOpen` and `RegisterAckOp`. [06-prerequisites/batch-index-ack §5.5](../06-prerequisites/batch-index-ack.md) defines how `SendTransactional` fills `ack_set`/`batch_size`. [06-prerequisites/README](../06-prerequisites/README.md) gives the ack-timeout, chunking and DLQ hooks that run before a transactional ack is sent.

## 10. Consumer path

### 10.1 `src/DotPulsar/Abstractions/IConsumer.cs` (L20-L71)

Insert after L35 (after `AcknowledgeCumulative(MessageId, ct)`):

```csharp
/// <summary>
/// Acknowledge the consumption of a single message using the MessageId as part of a transaction.
/// </summary>
ValueTask Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default);

/// <summary>
/// Acknowledge the consumption of multiple messages using the MessageIds as part of a transaction.
/// </summary>
ValueTask Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken = default);

/// <summary>
/// Acknowledge the consumption of all the messages in the topic up to and including the provided MessageId as part of a transaction.
/// </summary>
ValueTask AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default);
```

Implementers: `Internal/Consumer.cs` L24 and `Internal/SubConsumer.cs` L25 (no other implementers in src/tests/samples/benchmarks).

### 10.2 `src/DotPulsar/Extensions/ConsumerExtensions.cs`

After L35 add (same shape as L28-L35):

```csharp
/// <summary>
/// Acknowledge the consumption of a single message as part of a transaction.
/// </summary>
public static async ValueTask Acknowledge(this IConsumer consumer, IMessage message, ITransaction transaction, CancellationToken cancellationToken = default)
    => await consumer.Acknowledge(message.MessageId, transaction, cancellationToken).ConfigureAwait(false);

/// <summary>
/// Acknowledge the consumption of all the messages in the topic up to and including the provided message as part of a transaction.
/// </summary>
public static async ValueTask AcknowledgeCumulative(this IConsumer consumer, IMessage message, ITransaction transaction, CancellationToken cancellationToken = default)
    => await consumer.AcknowledgeCumulative(message.MessageId, transaction, cancellationToken).ConfigureAwait(false);
```

### 10.3 `src/DotPulsar/Internal/Consumer.cs`

Three new members next to their non-txn twins (L262-L300), routing identical to the existing ones:
- `Acknowledge(MessageId, ITransaction, ct)`: `await Guard(ct)`; `_singleSubConsumer` or `_subConsumers[messageId.Topic]` (pattern L262-L270).
- `Acknowledge(IEnumerable<MessageId>, ITransaction, ct)`: grouping by `Topic` and `Task.WhenAll` (pattern L272-L290; Java
  `MultiTopicsConsumerImpl` delegates per topic the same way).
- `AcknowledgeCumulative(MessageId, ITransaction, ct)` (pattern L292-L300).

### 10.4 `src/DotPulsar/Internal/SubConsumer.cs` — transactional ack

Critical hazard: everything inside `_executor.Execute` that ends in a non-Retry fault registers `ExecutorFaulted` (Executor.cs L187-L188)
and `ConsumerProcess.CalculateState` (ConsumerProcess.cs L45-L51) then faults the **whole consumer** permanently. Broker-side ack errors
(`TransactionConflictException`, `NotAllowedException`, `InvalidTransactionStatusException`, ... all `DotPulsarException` => Rethrow,
DefaultExceptionHandler L54) must therefore be evaluated **outside** the executor; only the transport step runs inside it.
Java fails just the ack future (`java:J/ClientCnx.java#handleAckResponse (L925-L946)`).

New members (after L96):

```csharp
public async ValueTask Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken)
{
    var txn = Transaction.From(transaction);
    txn.EnsureOpen(); // Java ConsumerBase#acknowledgeAsync(MessageId, Transaction) (L686-L707) checkIfOpen
    await InternalTransactionAcknowledge([messageId], CommandAck.Types.AckType.Individual, txn, cancellationToken).ConfigureAwait(false);
}

public async ValueTask Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken)
    => await InternalTransactionAcknowledge(messageIds, CommandAck.Types.AckType.Individual, Transaction.From(transaction), cancellationToken).ConfigureAwait(false);

public async ValueTask AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken)
{
    if (SubscriptionType is SubscriptionType.Shared or SubscriptionType.KeyShared)
        throw new ConfigurationException("Cannot use cumulative acks on a non-exclusive/non-failover subscription");

    await InternalTransactionAcknowledge([messageId], CommandAck.Types.AckType.Cumulative, Transaction.From(transaction), cancellationToken).ConfigureAwait(false);
}
```

(`[messageId]` collection expression is fine for all targets because the repo already uses collection expressions, e.g. PulsarClient.cs L42.)
Java parity notes: only the single-id individual ack checks `checkIfOpen`; the list ack and cumulative ack do not (`ConsumerBase` L617-L619,
L714-L735), the TC/broker reject them instead. Cumulative restriction message and type: `PulsarClientException.InvalidConfigurationException(
"Cannot use cumulative acks on a non-exclusive/non-failover subscription")` (`ConsumerBase#acknowledgeCumulativeAsync (L714-L735)`,
`#isCumulativeAcknowledgementAllowed (L867-L869)` = not `Shared` and not `Key_Shared`) -> DotPulsar `ConfigurationException` (existing,
`Exceptions/ConfigurationException.cs`). The broker would silently ignore such an ack (`java:B/Consumer.java#messageAcked (L566-L575)`,
"Received cumulative ack on shared subscription, ignoring"), so the client-side check is the only signal.

```csharp
private async ValueTask InternalTransactionAcknowledge(IEnumerable<MessageId> messageIds, CommandAck.Types.AckType ackType, Transaction transaction, CancellationToken cancellationToken)
{
    var ackTask = TransactionAcknowledge(messageIds, ackType, transaction, cancellationToken);
    transaction.RegisterAckOp(ackTask);          // Java ConsumerBase#doAcknowledgeWithTxn (L742-L776) registerAckOp
    await ackTask.ConfigureAwait(false);
}

private async Task TransactionAcknowledge(IEnumerable<MessageId> messageIds, CommandAck.Types.AckType ackType, Transaction transaction, CancellationToken cancellationToken)
{
    await transaction.RegisterAckedTopic(Topic, SubscriptionName, cancellationToken).ConfigureAwait(false);

    var command = new CommandAck                                  // NOT pooled (_commandAckPool L30/L65): it carries txn ids + request id
    {
        AckType = ackType,
        TxnidMostBits = transaction.Id.MostSigBits,
        TxnidLeastBits = transaction.Id.LeastSigBits
    };

    foreach (var messageId in messageIds)
        command.MessageId.Add(messageId.ToMessageIdData());

    var response = await _executor.Execute(() => InternalSendTransactional(command, cancellationToken), cancellationToken).ConfigureAwait(false);

    response.Expect(BaseCommand.Types.Type.AckResponse);
    if (response.AckResponse.HasError)
        response.AckResponse.Throw();
}

private async Task<BaseCommand> InternalSendTransactional(CommandAck command, CancellationToken cancellationToken)
{
    Guard();
    return await _channel.SendTransactional(command, cancellationToken).ConfigureAwait(false);
}
```

- Why a fresh `CommandAck`: the pooled instance (L197-L236) is reset only for `AckType`/`MessageId`; the broker keys txn acks on the
  has-bits (`java:B/Consumer.java#messageAcked (L590-L594)` cumulative, L622 individual `hasTxn = ack.hasTxnidLeastBits() && ack.hasTxnidMostBits()`).
  The pooled non-txn instance must still be reset on reuse once ack receipt and batch-index ack exist (agreed with prereq-batching):
  `ClearRequestId(); ClearTxnidMostBits(); ClearTxnidLeastBits(); ClearValidationError(); Properties.Clear();`. `MessageIdDataExtensions.MapFrom`
  (MessageIdDataExtensions.cs L24) must also clear `AckSet`, `BatchSize` and `FirstChunkMessageId`.
- Txn acks are never grouped and always carry a request id (Java `ConsumerImpl#doTransactionAcknowledgeForResponse (L3520-L3584)` and the
  list overload (L3631-L3663) -> `cnx.newAckForReceipt(cmd, requestId)`).
- Retry semantics: transport errors inside the executor (`ChannelNotReadyException`, `ConnectionDisposedException`, `OperationCanceledException`
  from `Awaiter` dispose on connection teardown) are Retry (DefaultExceptionHandler L39, L46, L53), so the ack is
  resent on the next channel. Java instead fails the ack with `ConnectException` when there is no connection (L3575-L3583). See D4.
- Existing Java exception expectations served by this path: `TransactionTest` (L280-L293) acks a non-existent `MessageIdImpl(10,10,10)`
  with a txn and expects `TransactionConflictException` -> arrives via `AckResponse.error = TransactionConflict` -> CommandExtensions L76.

### 10.5 `src/DotPulsar/Internal/Abstractions/IConsumerChannel.cs`, `ConsumerChannel.cs`, `NotReadyChannel.cs`

- IConsumerChannel after L22: `Task<BaseCommand> SendTransactional(CommandAck command, CancellationToken cancellationToken);`
  (returns the raw response — callers evaluate it outside the executor, see 10.4).
- NotReadyChannel after L38: `public Task<BaseCommand> SendTransactional(CommandAck command, CancellationToken cancellationToken) => throw GetException();`
  (`ChannelNotReadyException` => Retry).
- ConsumerChannel after `Send(CommandAck)` (L147-L191):

```csharp
public async Task<BaseCommand> SendTransactional(CommandAck command, CancellationToken cancellationToken)
{
    command.ConsumerId = _id;
    // batch messages: set ack_set + batch_size per MessageIdData (requires batch-index-ack prerequisite), see below
    return await _connection.SendWithResponse(command, cancellationToken).ConfigureAwait(false);
}
```

  It must **not** go through `_batchHandler.Acknowledge` (L158-L165 / L179-L186): that swallows partial-batch acks (returns null until all
  indexes are acked) — a txn ack of one batch index would never be sent. Java encodes the batch index in `ack_set` instead
  (`ConsumerImpl#doTransactionAcknowledgeForResponse (L3527-L3540)` single id: `bitSet.set(0, batchSize)`, then Individual =>
  `clear(batchIndex)`, Cumulative => `clear(0, batchIndex + 1)`; list overload L3636-L3654 additionally sets `MessageIdData.batch_size`).
  DotPulsar's public `MessageId` has no batch size (MessageId.cs L44-L76); the batch-index-ack prerequisite must expose it (either on
  `MessageId` or via a `BatchHandler` lookup by ledger/entry). Do not mutate `BatchHandler` state on txn acks (Java only touches its
  batch tracker for cumulative txn acks, `MessageIdAdvUtils.acknowledge(messageIdAdv, false)` L3531).
- `RejectPackage` (L253-L264) and non-txn acks are unchanged.
- Hooks owned by other prerequisite drafts (agreed with prereq-consumer). They run inside `ConsumerChannel.SendTransactional` (or at the
  `SubConsumer.TransactionAcknowledge` call site) **before** the request is sent, whatever the outcome:
  1. **ack-timeout.md §4.4(a)**: `RemoveAcknowledgedFromTracker(command, ...)` is called at send time, before the `AckResponse` is awaited
     (Java `ConsumerImpl` L3570-L3574 single-id path, L3657-L3661 list path). Cumulative removes everything up to the acked id
     (before batch resolution); individual removes an entry only when it is fully acked.
  2. **chunking.md §6.5**: an individual txn ack of a chunked `MessageId` expands into one `MessageIdData` per chunk, all in the same
     `CommandAck` (Java `doTransactionAcknowledgeForResponse` L3541-L3567, multi-message ack). A cumulative txn ack uses the last chunk only.
  3. **dead-letter-policy.md §4.6**: txn acks do **not** remove dead-letter candidates (Java parity).
  4. **Ack receipt (drafts/prereq/ack-receipt.md, prereq-batching; agreed)**: uses the same `RequestResponseHandler.OutgoingWithResponse` /
     `IConnection.SendWithResponse` plumbing. There, `IConsumerChannel.Send(CommandAck)` returns `Task<BaseCommand?>`: null when no receipt
     is needed or nothing was sent, otherwise the raw response. `RejectPackage` never requests a receipt.
- Ownership: the `ack_set`/`batch_size` construction in `ConsumerChannel.SendTransactional` is specified in batch-index-ack.md §5.5
  (prereq-batching). This draft owns only the interface member, the `NotReadyChannel` stub and the executor/outside-evaluation split of 10.4.

### 10.6 `src/DotPulsar/Internal/ConsumerChannelFactory.cs`, `Reader.cs`, `SubReader.cs`, `ReaderProcess.cs`

No source edits. Readers share `ConsumerChannel`/`NotReadyChannel` (SubReader.cs L27-L46, Reader.cs L291) and therefore get the new
`SendWithResponse` member automatically but never call it (readers do not ack). Reading topics with transactional data needs no client
change: the broker bounds dispatch at the transaction buffer's max-read position and filters commit/abort markers
(`java:B/AbstractBaseDispatcher.java` marker filtering), and `GetLastMessageId` already returns the dispatchable position.

---

