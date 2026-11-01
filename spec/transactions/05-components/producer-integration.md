# Component: Producer integration

| | |
|---|---|
| C# | `Abstractions/IMessageBuilder.cs`, `Internal/MessageBuilder.cs`, `MessageMetadata.cs`, `Internal/MessageMetadataObjectPool.cs`, `Internal/Producer.cs`, `Internal/SendOp.cs`, `Internal/SubProducer.cs`, `Internal/ProducerChannel.cs`, `Internal/ProducerChannelFactory.cs` |
| Upstream | `java:J/ProducerBase.java#newMessage(Transaction)` (L91-104), `java:J/TypedMessageBuilderImpl.java` (L104-109, `sendAsync` L131-141), `java:J/ProducerImpl.java#internalSendWithTxnAsync` (L519-530), `java:J/PartitionedProducerImpl.java#internalSendWithTxnAsync` (L256-314), `java:J/ProducerImpl.java#recoverNotAllowedError` (L1704-1720), `java:C#newSend` (L596-650) |
| WP | WP4 |

All DotPulsar line numbers refer to baseline `e8f2ba2`. Line numbers drift, so locate each edit by its anchor text.

Abbreviations:
- `J` = `pulsar-client/src/main/java/org/apache/pulsar/client/impl`
- `B` = `pulsar-broker/src/main/java/org/apache/pulsar/broker/service`
- `C` = `pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java`

## Requirements this component must satisfy

These come from the parity tests in [08](../08-test-parity.md).

| Id | Requirement | Source |
|---|---|---|
| R-OPEN | Sending with a transaction that is not `Open` throws `InvalidTransactionStatusException` before anything is enqueued. | `ProducerImpl#internalSendWithTxnAsync` L524-526 |
| R-REG | The partition topic is registered with the transaction (`CommandAddPartitionToTxn`, once per partition per transaction) **before** the message is written to the wire. | L527-529 |
| R-OP-SYNC | The send op is registered with the transaction (`RegisterSendOp`) **synchronously**, before `ISendChannel.Send` / `IProducer.Send` returns or awaits anything. A `Commit()` issued right after un-awaited sends therefore waits for them. | `TypedMessageBuilderImpl#sendAsync` L131-141 |
| R-ORDER | Messages are written to the wire in the order the sends were started. This includes sends that are not awaited, and the first send of a transaction, which waits for the `AddPartitionToTxn` round trip. Plain sends started after a transactional send must not overtake it: with `brokerDeduplicationEnabled=true`, overtaking would make the broker silently drop the earlier, lower sequence id. | `TransactionEndToEndTest#produceTxnMessageOrderTest`, `TopicTransactionBufferPublishOrderTest` |
| R-SEND-ERROR | A per-message `SendError` of type `NotAllowedError` fails only that message with `NotAllowedException`. It must not fault the producer. | `ClientCnx#handleSendError` L1231-1256, `TransactionTest#testDelayedDeliveryExceedsMaxDelay` |
| R-POOL | Pooled `CommandSend` and `MessageMetadata` objects never leak transaction ids into later sends. The broker treats any send with both txn-id has-bits set as transactional, **even when the ids are (0,0)**. | `B/ServerCnx.java#handleSend` L3122-3128 |
| R-TXN-FLAG | Set `CommandProducer.TxnEnabled = true` when the client has transactions enabled, and leave it unset otherwise, so that the wire bytes of non-transactional clients do not change. | `ProducerImpl` L2232 |

## 9.1 `IMessageBuilder` + `MessageBuilder`

- Interface member and XML: [03 §3.6](../03-public-api.md#36-imessagebuildertmessage--add-after-sequenceid-before-send).
- Implementation: `public IMessageBuilder<TMessage> Transaction(ITransaction transaction) { _metadata.Transaction = transaction; return this; }`.
- `MessageBuilder.Send` is **unchanged**. Registration happens inside `Producer` (§9.3), so every send path registers
  the op: `ISend.Send(MessageMetadata, …)`, `ISendChannel.Send`, and the builder.

## 9.2 `MessageMetadata` + `MessageMetadataObjectPool`

- Add the public property `ITransaction? Transaction` exactly as in
  [03 §3.7](../03-public-api.md#37-messagemetadata--add-property). Its setter also writes or clears the proto
  `TxnidMostBits` / `TxnidLeastBits`.
- `MessageMetadataObjectPool.Return`: add `metadata.Transaction = null;`. The setter clears the proto fields.
- Validation happens in `Producer` (§9.3), not in the setter. A transaction that is not DotPulsar's internal
  `Transaction` is rejected with `ArgumentException("The transaction must be created by DotPulsar", "transaction")`,
  mirroring Java's `checkArgument(txn instanceof TransactionImpl)`.

## 9.3 `Producer.InternalSend`, `SendOp`, `SubProducer` dispatcher (ordering design)

**Design.** The op is enqueued synchronously, in call order, and carries a **registration gate**. The `SubProducer`
dispatcher awaits the gate before writing the op. Waiting at the head of the queue keeps wire order equal to call order
(R-ORDER). Sequence ids are still assigned at enqueue time, so wire order also equals sequence order, which keeps
broker deduplication safe.

1. **`SendOp`** gets one new field, `public ValueTask RegistrationGate;`. It is `default` (already completed) for
   non-transactional sends.
2. **`Producer.InternalSend`** (L233-315). Insert this after the existing `ChoosePartitions` / `subProducer`
   selection, and **before** the `SendOp` is created and passed to `subProducer.Send(sendOp, …)`:
   ```csharp
   var transaction = metadata.Transaction is null ? null : Transaction.From(metadata.Transaction);  // ArgumentException if foreign
   ValueTask registrationGate = default;
   if (transaction is not null)
   {
       transaction.EnsureOpen();                                                                      // R-OPEN (InvalidTransactionStatusException)
       registrationGate = transaction.RegisterProducedTopic(subProducer.Topic, CancellationToken.None); // R-REG; fast path = completed default
   }
   ```
   Then create the `SendOp` with `RegistrationGate = registrationGate`. **Immediately after** creating it, and before
   any `await`, call `transaction?.RegisterSendOp(sendOp.ReceiptTcs.Task)` (R-OP-SYNC).

   `Transaction.From` and `EnsureOpen` are small helpers on the internal `Transaction` class:
   - `From` = `transaction as Transaction ?? throw new ArgumentException(...)`.
   - `EnsureOpen` throws `InvalidTransactionStatusException` with the `checkIfOpen` message from
     [exceptions §3](exceptions.md#3-message-formats-port-exactly-tests-may-compare).

   If `ChoosePartitions` itself awaits (for example while the producer is not yet connected), ordering is only
   guaranteed for sends that start after the producer is ready. That is existing DotPulsar behaviour; transactions do
   not change it.
3. **`SubProducer.MessageDispatcher`**. Before `channel.Send(sendOp.Metadata, sendOp.Data, tcs, ct)`, add:
   ```csharp
   if (!sendOp.RegistrationGate.IsCompletedSuccessfully)
   {
       try { await sendOp.RegistrationGate.ConfigureAwait(false); }
       catch (Exception exception)
       {
           // never written, so no receipt will come
           RemoveHeadWithoutReceipt(sendOp);
           // the caller sees the TC error; the transaction records the op as failed
           sendOp.ReceiptTcs.TrySetException(exception);
           continue;
       }
   }
   ```
   - The `ValueTask` is awaited exactly once, by the dispatcher.
   - This code must not run inside the producer `Executor`, because registration errors must not fault the producer.
   - The skipped sequence id leaves a gap. The broker accepts gaps, since deduplication only requires increasing ids.
   - On netstandard2.0, `ValueTask.IsCompletedSuccessfully` is available through `System.Threading.Tasks.Extensions`;
     verify this when implementing.
   - `RemoveHeadWithoutReceipt` must fit the real dispatcher (`AsyncQueueWithCursor` with a cursor and peek). The
     intent is fixed: the head op is not written until its gate completes. On failure it is removed **without**
     expecting a receipt, and the ops behind it proceed.
4. **Transaction ids on the metadata** were already set by the `MessageMetadata.Transaction` setter.
   `ProducerChannel` copies them to `CommandSend` (§9.5).

**Unit tests** (`SubProducerTests`, and unit-level `ProducerTests` with substitutes):

| Test | Requirement |
|---|---|
| `Send_GivenTransactionNotOpen_ShouldThrowInvalidTransactionStatusExceptionAndNotEnqueue` | R-OPEN |
| `Send_GivenForeignITransaction_ShouldThrowArgumentException` | §9.2 |
| `Send_GivenFirstTransactionalSendThenPlainSend_ShouldWriteInCallOrder` | R-ORDER (substitute TC delays registration) |
| `Send_GivenRegistrationFails_ShouldFailOnlyThatMessageAndContinue` | R-REG, gate failure |
| `SendChannel_GivenTransactionalSend_ShouldRegisterOpBeforeReturning` | R-OP-SYNC |
| `Send_GivenSamePartitionTwice_ShouldRegisterPartitionOnce` | caching |

### 9.4 `src/DotPulsar/Internal/ProducerChannelFactory.cs`

- Ctor L31-L63: add parameter `bool transactionsEnabled`; after L60: `if (transactionsEnabled) _commandProducer.TxnEnabled = true;`
  (proto `txn_enabled = 12`, L523; Java always sends `client.conf.isEnableTransaction()`, `java:J/ProducerImpl.java (L2232)` ->
  `java:C#newProducer` `.setTxnEnabled(isTxnEnabled)`). Leaving it unset when false keeps the wire bytes of non-txn clients identical.

### 9.5 `src/DotPulsar/Internal/ProducerChannel.cs` (pooled `CommandSend`)

`Send` L63-L111: the pooled `SendPackage.Command` (L80, `??= new CommandSend { ProducerId = _id, NumMessages = 1 }`) is reused across
messages. The broker treats a send as transactional when **both** has-bits are set: `if (send.hasTxnidMostBits() && send.hasTxnidLeastBits())`
(`java:B/ServerCnx.java#handleSend (L3064-L3128)`, L3122-L3128). proto2 C# sets the has-bit on assignment (even of 0), so a stale value
would turn the next non-txn message into a txn `(0,0)` message. After L82 insert:

```csharp
if (metadata.HasTxnidMostBits && metadata.HasTxnidLeastBits)
{
    sendPackage.Command.TxnidMostBits = metadata.TxnidMostBits;
    sendPackage.Command.TxnidLeastBits = metadata.TxnidLeastBits;
}
else
{
    sendPackage.Command.ClearTxnidMostBits();
    sendPackage.Command.ClearTxnidLeastBits();
}
```

Java derives `CommandSend` txn ids from the metadata the same way (`java:C#newSend (L596-L618)` uses
`messageMetadata.hasTxnidLeastBits() ? ... : -1`, and `newSend (L620-L652)` sets them only when `>= 0`).
The metadata also keeps the txn ids (Java sets both, L105-L109 of TypedMessageBuilderImpl; broker uses `CommandSend` for routing, metadata is
stored). No reset needed in `finally` (L98-L110) because the next call always writes or clears both fields.
With producer batching (prereq-batching) the batch container must only batch messages of the same txn (`java:J/AbstractBatchMessageContainer.java#hasSameTxn (L137-L149)`) and copy the txn ids to the batch metadata (`BatchMessageContainerImpl` L123-L128, L425-L430); this edit then applies unchanged.

### 9.6 `src/DotPulsar/Internal/SubProducer.cs` — per-message `SendError` instead of faulting the producer

Current behaviour (deep trace):
1. Broker sends `CommandSendError(producer_id, sequence_id, error, message)`; `RequestResponseHandler` L36 maps it to the
   `SendRequest(producerId, sequenceId)` registered by `Outgoing(CommandSend)` (L94-L98) and completes the per-send TCS with the
   `BaseCommand`.
2. `MessageDispatcher` continuation L125-L135 enqueues the completed task into `responseQueue`.
3. `ResponseProcessor` L151-L172: `responseTask.Result.Expect(SendReceipt)` (L162) -> `CommandExtensions.Expect` L31-L32 ->
   `SendError.Throw()` -> mapped `DotPulsarException` (NotAllowedException / PersistenceException / TopicTerminatedException / ...).
4. `_executor.TryExecuteOnce` (L157) -> `Executor.Handle` (Executor.cs L178-L193) -> `DefaultExceptionHandler` L54 `DotPulsarException => Rethrow`
   -> **`ExecutorFaulted` registered** (Executor.cs L187-L188) -> `TryExecuteOnce` rethrows -> `ResponseProcessor` faults.
5. `ProducerProcess.CalculateState` (ProducerProcess.cs L45-L51): `ExecutorState.Faulted` -> `ProducerState.Faulted` ->
   `SubProducer.ChannelFaulted` (L233-L237) -> `InternalDispose`; all later sends throw `ProducerFaultedException` (Guard L239-L248).
   The `SendReceiptWrongOrdering` branch (L169) is only reached for *Retry* outcomes (`ProducerSendReceiptOrderingException`, DefaultExceptionHandler L52).

Java behaviour (`java:J/ClientCnx.java#handleSendError (L1231-L1259)`): `ChecksumError` -> resend; `TopicTerminatedError` -> producer terminated;
**`NotAllowedError` -> fail only that message** (`java:J/ProducerImpl.java#recoverNotAllowedError (L1704-L1720)`: if head-of-queue
op's highest sequence id == `sequenceId`, remove it and complete it with `NotAllowedException(errorMsg)`); default -> `connectionClosed` (reconnect + resend).

Transaction-relevant `SendError`s: the broker maps publish failures via `MessagePublishContext#getServerError`
(`java:B/Producer.java (L586-L596)`): `TopicTerminatedException` -> `TopicTerminatedError`, `BrokerServiceException.NotAllowedException` ->
`NotAllowedError`, else `PersistenceError`. Txn-specific `NotAllowedException`s: "Transaction [x] has been ended. Please use a new
transaction to send message." (`java:pulsar-broker/.../transaction/buffer/impl/TopicTransactionBuffer.java#internalAppendBufferToTxn (L510-L519)`),
and the delayed-delivery limit tested by `TransactionTest` (expects `NotAllowedException` "Exceeds max allowed delivery delay of ... milliseconds",
`java:pulsar-broker/src/test/.../broker/transaction/TransactionTest.java (L2050-L2081)`).

Exact change — `ResponseProcessor` lambda L157-L164 becomes:

```csharp
var success = await _executor.TryExecuteOnce(() =>
{
    if (responseTask.IsFaulted)
        throw responseTask.Exception!;

    var response = responseTask.Result;

    if (response.Type == BaseCommand.Types.Type.SendError && response.SendError.Error == ServerError.NotAllowedError)
    {
        ProcessNotAllowedSendError(response.SendError);
        return;
    }

    response.Expect(BaseCommand.Types.Type.SendReceipt);
    ProcessReceipt(response.SendReceipt);
}, CancellationToken.None).ConfigureAwait(false);
```

New private method after `ProcessReceipt` (L174-L191):

```csharp
private void ProcessNotAllowedSendError(CommandSendError sendError)
{
    var sequenceId = sendError.SequenceId;

    if (!_sendQueue.TryPeek(out var sendOp) || sendOp is null)
        throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId} but send queue is empty");

    var expectedSequenceId = sendOp.Metadata.SequenceId;

    if (sequenceId != expectedSequenceId)
        throw new ProducerSendReceiptOrderingException($"Received send error for sequenceId {sequenceId}. Expected {expectedSequenceId}");

    _sendQueue.Dequeue();
    sendOp.ReceiptTcs.TrySetException(new NotAllowedException(sendError.Message));
}
```

- The failed message's `TaskCompletionSource` is the caller's TCS for `Send` (Producer.cs L209/L264-L265 pass the same `tcs`), so
  `IProducer.Send` / `IMessageBuilder.Send` throw `NotAllowedException` directly; for `Enqueue` the existing continuation (L268-L304) runs
  `onFailed` (null for Enqueue) — no extra work.
- Sequence mismatch -> `ProducerSendReceiptOrderingException` (Retry => `SendReceiptWrongOrdering` => reconnect + resend, the existing recovery).
  Java silently ignores a mismatch; reconnecting is the safer DotPulsar equivalent (D3).
- With producer batching (agreed with prereq-batching, producer-batching.md §4.12-§4.13): match the head op group (`BatchCount` ops) when
  `sendError.SequenceId` equals either the batch `sequence_id` or its `HighestSequenceId`. The broker sends `max(highest, seq)`
  (`java:B/Producer.java` L567). Fail every op of the group with the mapped exception and return the prepared batch.
  `RequestResponseHandler.Outgoing(CommandSend)` (L94-L98) must also register the alias key `SendRequest(producerId, HighestSequenceId)` when
  highest != lowest, or the batch's SendError never reaches the TCS. Java: `getHighestSequenceId(op)`, ProducerImpl L1706.
- All other `SendError`s keep today's behaviour (fault the producer) — see D3 for Java's reconnect-on-`PersistenceError`.

---

## 9.7 Tests for 9.4-9.6

- `ProducerChannelTests.Send_GivenTransactionalThenNonTransactionalMetadata_ShouldClearTxnIdsOnPooledCommand` (R-POOL)
- `MessageMetadataObjectPoolTests.Return_GivenMetadataWithTransaction_ShouldClearTransactionAndTxnIds` (R-POOL)
- `ProducerChannelFactoryTests.Create_GivenTransactionsEnabled_ShouldSetTxnEnabled` and `..._GivenDisabled_ShouldNotSetTxnEnabled` (R-TXN-FLAG)
- `SubProducerTests.ResponseProcessor_GivenNotAllowedSendError_ShouldFailOnlyThatMessage` (R-SEND-ERROR), plus `..._GivenPersistenceSendError_ShouldFaultProducer`, which pins the current behaviour
