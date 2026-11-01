# WP-PREREQ-3: Acknowledgement receipts (CommandAck.request_id / CommandAckResponse)

Status: draft spec (no implementation). Target repo HEAD `e8f2ba2`.
Upstream pins: Java `apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026` (primary), C++ `apache/pulsar-client-cpp@f71d7e99019d58b1d11858f05ff0dc83b718627b` (cross-reference).

Depends on: protocol version bump 14 -> 19 (decided; ack receipts need >= 17). Consumed by: transaction core (every transactional ack is request/response), `batch-index-ack.md` (`ConsumerChannel.SendTransactional`), ported tests using `isAckReceiptEnabled(true)` (10 occurrences in the transaction test corpus, `BatchMessageIndexAckTest`/`BatchMessageIndexAckDisableTest` data providers, `AckReceiptOnTopicUnloadTest`).

---

## 1. Goal and scope

1. New consumer option `AckReceiptEnabled` (default `false`, Java default `false`): when enabled, every acknowledgement command carries a `request_id`, and `IConsumer.Acknowledge*` completes only when the broker's `CommandAckResponse` arrives; a response with `error` makes the call throw the mapped exception.
2. Transactional acknowledgements **always** use a receipt, independent of the option (Java `ConsumerImpl.doTransactionAcknowledgeForResponse` always calls `cnx.newAckForReceipt`).
3. `CommandAckResponse` is routed by `request_id` through the existing request/response machinery; an `AckResponse` frame must never kill the connection (today an unregistered response type throws in `RequestResponseHandler.Incoming` and tears down the connection).
4. A server error in an ack response fails only that ack call; it must **not** fault the consumer.
5. Brokers with protocol < 17: receipts are not requested and acks complete immediately (Java parity).

Out of scope: ack grouping (DotPulsar acks are immediate; Java's grouped-future semantics collapse to per-call futures), a client-wide operation timeout (see Open decisions 1).

---

## 2. Upstream mapping

| Concern | Java (primary) | C++ (cross-ref) | DotPulsar target |
|---|---|---|---|
| Option + default | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/conf/ConsumerConfigurationData.java#ackReceiptEnabled (L419)` = `false` | `cpp:include/pulsar/ConsumerConfiguration.h#setAckReceiptEnabled (L670)`, `#isAckReceiptEnabled (L675)` | `ConsumerOptions.AckReceiptEnabled` |
| Builder | `java:.../impl/ConsumerBuilderImpl.java#isAckReceiptEnabled (L301-L304)` | - | `IConsumerBuilder.AckReceiptEnabled(bool)` |
| Effective enablement (protocol >= 17) | `java:.../impl/ConsumerImpl.java#isAckReceiptEnabled (L3672-L3676)`, warning on connect `#setClientCnx (L3368-L3372)`, `java:pulsar-common/.../protocol/Commands.java#peerSupportsAckReceipt (L2658-L2660)` | - | `IConnection.ProtocolVersion` check |
| Ack with request id | `java:.../impl/PersistentAcknowledgmentsGroupingTracker.java#newMessageAckCommandAndWrite (L648-L716)` (receipt branch L656-L674; fallback completes futures immediately L676-L688) | `cpp:lib/ConsumerImpl.cc#doImmediateAck (L2197-L2216)`, multi (L2218-L2245) | `ConsumerChannel.Send(CommandAck)` |
| Wire: request id on ack | `Commands.java#newAck (L1200-L1290)` (`request_id` only when `requestId >= 0`), `#newMultiMessageAck (L1151-L1198)` | `cpp:lib/Commands.cc#newAck(..., requestId)` | `RequestResponseHandler.Outgoing(CommandAck)` |
| Pending request + timeout | `java:.../impl/ClientCnx.java#newAckForReceipt (L1419-L1421)`, `#newAckForReceiptWithFuture (L1423-L1426)`, `#sendRequestAndHandleTimeout (L1518-L1540)`, `#checkRequestTimeout (L2165-L2193)` ("AckResponse request timeout", `PulsarClientException.TimeoutException`), `RequestType.AckResponse` (L331-L346) | `cpp:lib/ClientConnection.cc#sendRequestWithId` | `Awaiter` + caller `CancellationToken` (+ optional timeout, Open decision 1) |
| Response handling | `ClientCnx.java#handleAckResponse (L924-L945)` (no error -> complete; error -> `getPulsarClientException(error, buildError(requestId, message))`; unknown id -> counted + warning, ignored) | `cpp:lib/ClientConnection.cc#handleAckResponse` | `RequestResponseHandler._getResponseIdentifier[AckResponse]`, `SubConsumer` |
| Error mapping | `ClientCnx.java#getPulsarClientException (L2067-L2108)` | `cpp:lib/ClientConnection.cc#getResult` | `CommandExtensions.ToException(ServerError, string)` (existing switch `CommandExtensions.cs L52-L81`) |
| Transactional ack (always receipt) | `java:.../impl/ConsumerImpl.java#doTransactionAcknowledgeForResponse (L3520-L3586)` (`cnx.newAckForReceipt(cmd, requestId)` L3584), list overload (L3631-L3665, L3664) | (no txn) | `SubConsumer` txn ack path |
| Txn op registration | `java:.../impl/ConsumerBase.java` (L755, L770) `txn.registerAckOp(ackFuture)`, `java:.../impl/transaction/TransactionImpl.java#registerAckOp (L165-L183)` | - | transaction WP |
| Grouped futures with receipt | `PersistentAcknowledgmentsGroupingTracker.java#doIndividualAck (L275-L291)`, `#doCumulativeAck (L321-L343)` (returns `currentIndividualAckFuture` / `currentCumulativeAckFuture`) | `cpp:lib/AckGroupingTrackerEnabled.cc` | not applicable (no grouping): each call has its own response |

Upstream tests: `java:pulsar-broker/src/test/java/org/apache/pulsar/broker/service/AckReceiptOnTopicUnloadTest.java`; `BatchMessageIndexAckTest` / `BatchMessageIndexAckDisableTest` (`ackReceiptEnabled` data provider `{true},{false}`); transaction tests that set `.isAckReceiptEnabled(true)`.

---

## 3. Public API additions

### 3.1 `src/DotPulsar/ConsumerOptions.cs`

```csharp
/// <summary>
/// The default of whether acknowledgments wait for a receipt from the broker.
/// </summary>
public static readonly bool DefaultAckReceiptEnabled = false;

/// <summary>
/// Whether acknowledgments wait for a receipt from the broker, so that acknowledgment errors are reported to the caller. Transactional acknowledgments always wait for a receipt. The default is 'false'.
/// </summary>
public bool AckReceiptEnabled { get; set; }
```

All three constructors assign `AckReceiptEnabled = DefaultAckReceiptEnabled`.

### 3.2 `src/DotPulsar/Abstractions/IConsumerBuilder.cs` + `src/DotPulsar/Internal/ConsumerBuilder.cs`

```csharp
/// <summary>
/// Whether acknowledgments wait for a receipt from the broker. The default is 'false'.
/// </summary>
IConsumerBuilder<TMessage> AckReceiptEnabled(bool ackReceiptEnabled);
```

`ConsumerBuilder`: field `_ackReceiptEnabled` (init from the static default), setter returns `this`, `Create()` copies it into options.

No change to the `IConsumer.Acknowledge*` signatures: they already return `ValueTask` and now complete on receipt when enabled.

---

## 4. Behaviour (normative)

4.1 **Effective enablement** for a consumer channel = `options.AckReceiptEnabled && connection.ProtocolVersion >= 17` (Java `isAckReceiptEnabled` L3672-L3676). Transactional acks require `connection.ProtocolVersion >= 17` unconditionally; with an older broker a transactional ack fails with `NotSupportedException("The broker does not support acknowledgment receipts (protocol version < 17)")` (the broker could not support transactions anyway).

4.2 **Non-transactional ack with receipt**:
- `ConsumerChannel.Send` applies the batch decision table of `batch-index-ack.md` 3.3. If nothing is sent (e.g. partial batch, batch-index ack disabled), the call completes successfully at once (Java `addIndividualAcknowledgment` returns `completedFuture(null)` in that case, L227-L228).
- Otherwise the command gets a fresh `request_id` and the call completes when the matching `CommandAckResponse` arrives:
  - no `error` -> success;
  - `error` set -> throw `CommandExtensions.ToException(error, message)` (e.g. `ServerError.TransactionConflict` -> `TransactionConflictException`, `NotAllowedError` -> `NotAllowedException`, `ConsumerNotFound` -> `ConsumerNotFoundException`, `ServiceNotReady` -> `ServiceNotReadyException`, `UnknownError` -> `UnknownException`). Java wraps the message as JSON via `ClientCnx.buildError` (L2059-L2065: `{"errorMsg":"<message>","reqId":<id>, "remote":"..", "local":".."}`); DotPulsar keeps the broker `message` verbatim (repo convention; the ported test only asserts "message contains broker message", which holds; see Open decision 4).
- The error does **not** change consumer state (no `ExecutorFaulted`), and is **not** retried even when its exception type is retryable in `DefaultExceptionHandler` (e.g. `ConsumerNotFoundException`, `ServiceNotReadyException`): the error is evaluated outside the executor (5.6).

4.3 **Consumer closed by the broker while waiting** (topic unload / ownership change -> `CommandCloseConsumer`): the pending ack request is **not** cancelled. The broker still answers the in-flight ack (with `ServerError.ConsumerNotFound` when the consumer is gone) on the same connection, and the call fails with `ConsumerNotFoundException` carrying the broker message. This is exactly what `AckReceiptOnTopicUnloadTest#testDelayedAckReceipt` asserts for `unload = true` (`response.hasError() == unload`, `getError() == ConsumerNotFound`, ack fails with a message containing the broker message; L111-L121) and is why ack requests are registered **without** consumer id (5.2) so that `RequestResponseHandler.Incoming(CommandCloseConsumer)` (L148-L161) does not cancel them.

4.3a **Connection lost while waiting** (`Awaiter.Dispose` cancels all pending requests -> `TaskCanceledException` inside the executor): the ack is **retried** on the next channel by the existing `SubConsumer` executor loop (`OperationCanceledException` with a non-cancelled caller token -> `FaultAction.Retry`). Acks are idempotent at the broker. Java fails the future (`ConnectionClosedException` from `ClientCnx.channelInactive`) and its grouping tracker re-adds the acks for the next flush (`restoreIndividualAck`, L718-L731), so the observable outcome (ack eventually applied) matches; the call outcome differs (DotPulsar: success after reconnect; Java: exception). See Open decision 2.

4.4 **Caller cancellation**: the caller's `CancellationToken` cancels the wait (throws `OperationCanceledException`/`TaskCanceledException`); the pending request entry is removed from the awaiter (`Awaiter.Cancel(request)`); a late response is ignored.

4.5 **Responses for unknown request ids** (late/duplicate): ignored silently (`Awaiter.SetResult` on a missing key is a no-op) - Java counts `duplicatedResponseCounter` and logs (L937-L943).

4.6 **Responses without `request_id`**: ignored (identifier `null`). The broker only sends `CommandAckResponse` when the ack had a `request_id` (`ServerCnx.handleAck`), but this keeps the connection safe.

4.7 **Transactional ack**: always request/response (4.1 protocol check applies); `txnid_most_bits`/`txnid_least_bits` set by the transaction WP; same error mapping; the resulting task is what the transaction registers as an ack op (Java `registerAckOp`). Broker-side consumer close and connection loss: same rules as 4.3 and 4.3a (transactional acks are idempotent per (txn, position) at the broker's pending-ack store); flagged in Open decision 3.

4.8 **Cumulative acks with receipt**: identical flow (one command, one response).

---

## 5. Internal design

### 5.1 Protocol version on the connection

- `src/DotPulsar/Internal/Abstractions/IConnection.cs`: add `int ProtocolVersion { get; }`.
- `src/DotPulsar/Internal/Connection.cs`: `public int ProtocolVersion { get; set; }` next to `MaxMessageSize` (L63).
- `src/DotPulsar/Internal/ConnectionPool.cs#EstablishNewConnection` (after L181 `connection.MaxMessageSize = ...`): `connection.ProtocolVersion = response.Connected.ProtocolVersion;` (proto: `CommandConnected.protocol_version`, default 0; a broker always sends it).
- `src/DotPulsar/Internal/Constants.cs` (L34): the decided bump to 19 makes the client advertise >= 17 (owned by the transaction core WP; prerequisite here).
- Add `public const int ProtocolVersionAckReceipt = 17;` to `Constants`.

### 5.2 `RequestResponseHandler` (`src/DotPulsar/Internal/RequestResponseHandler.cs`)

Constructor (L28-L48) - register the response type:

```csharp
_getResponseIdentifier.Set(BaseCommand.Types.Type.AckResponse, cmd => cmd.AckResponse.HasRequestId ? StandardRequest.WithRequestId(cmd.AckResponse.RequestId) : null!);
```

(`Incoming` already ignores a `null` identifier, L142-L145. `StandardRequest.Equals` compares request ids only, `Requests/StandardRequest.cs`, so a key registered with consumer id matches.)

New method:

```csharp
public Task<BaseCommand> OutgoingWithResponse(CommandAck command)
{
    command.RequestId = _requestId.FetchNext();
    var request = StandardRequest.WithRequestId(command.RequestId);
    return _requests.CreateTask(request);
}
```

Registered **by request id only** (no consumer id) so that `Incoming(CommandCloseConsumer)` (L148-L161) does not cancel in-flight acks: the broker still answers them (4.3). Pending ack requests are only cancelled by connection teardown (`Awaiter.Dispose`) or by the caller's token (5.4).

### 5.3 `ChannelManager` (`src/DotPulsar/Internal/ChannelManager.cs`)

```csharp
public Task<BaseCommand> OutgoingWithResponse(CommandAck command)
{
    using (TakeConsumerSenderLock(command.ConsumerId))
    {
        return _requestResponseHandler.Outgoing(command);
    }
}
```

(Same pattern as `Outgoing(CommandSeek)` / `Outgoing(CommandGetLastMessageId)`, L177-L191: throws `OperationCanceledException` when the consumer channel is gone -> retried by the executor.)

### 5.4 `Connection` (`src/DotPulsar/Internal/Connection.cs`) + `IConnection`

```csharp
public async Task<BaseCommand> SendWithResponse(CommandAck command, CancellationToken cancellationToken)
{
    ThrowIfDisposed();

    Task<BaseCommand>? responseTask;

    using (await _lock.Lock(cancellationToken).ConfigureAwait(false))
    {
        responseTask = _channelManager.OutgoingWithResponse(command);
        var sequence = Serializer.Serialize(command.AsBaseCommand());
        await _stream.Send(sequence).ConfigureAwait(false);
    }

    return await responseTask.ConfigureAwait(false);
}
```

Identical shape to `Send(CommandGetLastMessageId)` (L196-L210). Add to `IConnection` next to `Task Send(CommandAck command, ...)`. The existing fire-and-forget `Send(CommandAck)` (L117-L118) stays for receipt-less acks; it must send the command **without** a request id (see 5.6 pool reset).

Caller-cancellation cleanup (4.4): if `cancellationToken` fires while awaiting `responseTask`, the awaiter entry would leak until the connection closes. Wrap: `using var registration = cancellationToken.Register(() => _channelManager.CancelRequest(StandardRequest.WithRequestId(command.RequestId)))` where `ChannelManager.CancelRequest` -> `RequestResponseHandler.Cancel(IRequest)` -> `Awaiter.Cancel`. The registration allocation only happens on this path (receipt acks); acceptable.

### 5.5 `ConsumerChannel` / `IConsumerChannel`

- `src/DotPulsar/Internal/Abstractions/IConsumerChannel.cs`: change `Task Send(CommandAck command, CancellationToken cancellationToken)` to `Task<BaseCommand?> Send(CommandAck command, CancellationToken cancellationToken)` - returns `null` when no receipt was requested or nothing was sent, otherwise the raw `AckResponse` command (**not** thrown here; see 5.6).
- `NotReadyChannel<TMessage>.Send(CommandAck)` keeps throwing `ChannelNotReadyException` (retry).
- `ConsumerChannel` ctor gets `bool ackReceiptEnabled` (from `ConsumerChannelFactory`, from `ConsumerOptions.AckReceiptEnabled`; `Reader` passes `false`). At the end of `Send(CommandAck)` (`ConsumerChannel.cs L189-L190`):

```
command.ConsumerId = _id;
if (_ackReceiptEnabled && _connection.ProtocolVersion >= Constants.ProtocolVersionAckReceipt)
    return await _connection.SendWithResponse(command, cancellationToken).ConfigureAwait(false);
command.ClearRequestId();
await _connection.Send(command, cancellationToken).ConfigureAwait(false);
return null;
```

- `RejectPackage` (L253-L264) uses `Send(CommandAck)` internally: it must use the receipt-less path regardless of the option (validation-error acks are internal; Java `discardCorruptedMessage` uses `cnx.ctx().writeAndFlush` without receipt). Add a private `SendWithoutReceipt(CommandAck)` used by `RejectPackage`.
- `SendTransactional(CommandAck, ct)` (defined in `batch-index-ack.md` 5.5) ends with `return await _connection.SendWithResponse(command, cancellationToken)` after the protocol check of 4.1.

### 5.6 `SubConsumer` (`src/DotPulsar/Internal/SubConsumer.cs`)

The executor (`Executor.Handle`, `Executor.cs`) registers `ExecutorFaulted` for every exception whose fault action is not `Retry`, and `DotPulsarException`s map to `Rethrow` -> the consumer would become `Faulted` if a server ack error were thrown inside `_executor.Execute`. Therefore the error is inspected **outside** the executor:

```csharp
private async ValueTask<BaseCommand?> InternalAcknowledge(CommandAck command, CancellationToken cancellationToken)
{
    Guard();
    return await _channel.Send(command, cancellationToken).ConfigureAwait(false);
}

private async ValueTask InternalAcknowledge(MessageId messageId, CommandAck.Types.AckType ackType, CancellationToken cancellationToken)
{
    var commandAck = _commandAckPool.Get();
    ResetPooledAck(commandAck);
    commandAck.AckType = ackType;
    // existing MessageId[0] fill (L201-L204)
    try
    {
        var response = await _executor.Execute(() => InternalAcknowledge(commandAck, cancellationToken), cancellationToken).ConfigureAwait(false);
        response?.ThrowIfAckError();   // outside the executor
    }
    finally
    {
        _commandAckPool.Return(commandAck);
    }
}
```

Same change for `InternalAcknowledge(IEnumerable<MessageId>, ...)` (L216-L236).

`ResetPooledAck(CommandAck c)`: `c.ClearRequestId(); c.ClearTxnidMostBits(); c.ClearTxnidLeastBits(); c.ClearValidationError(); c.Properties.Clear();` (`MessageId` list handled by existing code + `MapFrom` reset from `batch-index-ack.md`).

Executor retry with a pooled command re-enters `ConsumerChannel.Send` on the new channel; `RequestResponseHandler.Outgoing(CommandAck)` assigns a **new** request id each attempt. The batch decision table is idempotent on an already-transformed command (`batch-index-ack.md` 5.5: transformed ids have `BatchIndex` cleared and are forwarded unchanged).

### 5.7 `CommandExtensions` (`src/DotPulsar/Internal/Extensions/CommandExtensions.cs`)

- Refactor the private `Throw(ServerError, string)` switch (L52-L81) into `public static Exception ToException(ServerError error, string message)`; `Throw` becomes `throw ToException(error, message)` (shared with `producer-batching.md` 4.13).
- New:

```csharp
public static void ThrowIfAckError(this BaseCommand command)
{
    command.Expect(BaseCommand.Types.Type.AckResponse);
    if (command.AckResponse.HasError)
        throw ToException(command.AckResponse.Error, command.AckResponse.Message);
}
```

`Expect` (L23-L35) already maps a `CommandError` reply; an ack never receives one, but if it did, the mapping is reused.
- `ServerError.TransactionNotFound` is currently unmapped (falls to `UnknownException`); the transaction core WP adds `TransactionNotFoundException` - not this WP.

---

## 6. Exact DotPulsar edit points

| File | Member | Change |
|---|---|---|
| `src/DotPulsar/ConsumerOptions.cs` | static, property, 3 ctors | 3.1 |
| `src/DotPulsar/Abstractions/IConsumerBuilder.cs` | `AckReceiptEnabled(bool)` | 3.2 |
| `src/DotPulsar/Internal/ConsumerBuilder.cs` | field, setter, `Create()` | 3.2 |
| `src/DotPulsar/Internal/Constants.cs` | `ProtocolVersionAckReceipt = 17` | 5.1 |
| `src/DotPulsar/Internal/Abstractions/IConnection.cs` | `ProtocolVersion`, `SendWithResponse(CommandAck, ...)` | 5.1, 5.4 |
| `src/DotPulsar/Internal/Connection.cs` | `ProtocolVersion`, `SendWithResponse` | 5.4 |
| `src/DotPulsar/Internal/ConnectionPool.cs` | `EstablishNewConnection` (after L181) | 5.1 |
| `src/DotPulsar/Internal/RequestResponseHandler.cs` | ctor (L28-L48), `OutgoingWithResponse(CommandAck)`, `Cancel(IRequest)` | 5.2 |
| `src/DotPulsar/Internal/ChannelManager.cs` | `OutgoingWithResponse(CommandAck)`, `CancelRequest` | 5.3 |
| `src/DotPulsar/Internal/Abstractions/IConsumerChannel.cs` | `Send(CommandAck)` return type | 5.5 |
| `src/DotPulsar/Internal/ConsumerChannel.cs` | ctor, `Send(CommandAck)` tail (L189-L190), `RejectPackage` (L253-L264) | 5.5 |
| `src/DotPulsar/Internal/NotReadyChannel.cs` | `Send(CommandAck)` signature | 5.5 |
| `src/DotPulsar/Internal/ConsumerChannelFactory.cs` | ctor + `Create` | pass `ackReceiptEnabled` |
| `src/DotPulsar/Internal/Consumer.cs` | `CreateSubConsumer` (L461-L466) | pass option |
| `src/DotPulsar/Internal/Reader.cs` | channel factory creation | `ackReceiptEnabled: false` |
| `src/DotPulsar/Internal/SubConsumer.cs` | both `InternalAcknowledge` overloads (L161-L165, L197-L236), pooled reset | 5.6 |
| `src/DotPulsar/Internal/Extensions/CommandExtensions.cs` | `ToException`, `ThrowIfAckError` | 5.7 |
| `tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` | `CreateChannel` helper; substitute `IConnection.Send(CommandAck)` assertions | keep green |

---

## 7. Performance notes

- Receipt disabled (default): one extra branch + `ClearRequestId()` per ack; no allocations added.
- Receipt enabled: per ack one `TaskCompletionSource<BaseCommand>` (awaiter) + its `Task`, one `StandardRequest` boxing into the `ConcurrentDictionary<IRequest, ...>` key (same cost as every other request type today), one cancellation registration when the caller passes a cancellable token. Java's equivalent allocates a `TimedCompletableFuture` and `RequestTime` per request.
- The connection lock (`AsyncLock`) is held only for serialisation + write, as today.

---

## 8. Tests

### 8.1 Unit tests (`[Trait("Category", "Unit")]`)

`tests/DotPulsar.Tests/Internal/RequestResponseHandlerTests.cs` (new or extend from `producer-batching.md`):
- `Outgoing_GivenCommandAck_ShouldAssignIncreasingRequestIds`
- `Incoming_GivenAckResponseWithMatchingRequestId_ShouldCompleteTask`
- `Incoming_GivenAckResponseWithUnknownRequestId_ShouldNotThrow`
- `Incoming_GivenAckResponseWithoutRequestId_ShouldNotThrow` (regression for "unregistered response kills connection")
- `Incoming_GivenCloseConsumer_ShouldNotCancelPendingAckRequests`
- `Incoming_GivenAckResponseWithConsumerNotFoundAfterCloseConsumer_ShouldCompleteTaskWithErrorResponse`

`tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` (extend):
- `Send_GivenAckReceiptEnabledAndProtocol19_ShouldSendWithResponseAndReturnResponse`
- `Send_GivenAckReceiptEnabledAndProtocol14_ShouldSendWithoutRequestIdAndReturnNull`
- `Send_GivenAckReceiptDisabled_ShouldClearRequestIdOnPooledCommand`
- `Send_GivenAckReceiptEnabledAndPartialBatch_ShouldNotSendAndReturnNull`
- `RejectPackage_GivenAckReceiptEnabled_ShouldNotRequestReceipt`

`tests/DotPulsar.Tests/Internal/SubConsumerTests.cs` (new; fake `IConsumerChannel<byte[]>`, real `Executor` with `DefaultExceptionHandler`, NSubstitute `IRegisterEvent`):
- `Acknowledge_GivenAckResponseWithTransactionConflictError_ShouldThrowTransactionConflictExceptionAndNotFaultConsumer` (assert no `ExecutorFaulted` registered)
- `Acknowledge_GivenAckResponseWithoutError_ShouldComplete`
- `Acknowledge_GivenConnectionLostWhileWaiting_ShouldRetryOnNewChannel` (first `Send` throws `TaskCanceledException`, second returns success)
- `Acknowledge_GivenAckResponseWithConsumerNotFound_ShouldThrowConsumerNotFoundExceptionWithoutRetry`
- `Acknowledge_GivenCallerCancellation_ShouldThrowOperationCanceledException`
- `Acknowledge_GivenPooledCommandReuse_ShouldResetTxnAndRequestIdFields`

`tests/DotPulsar.Tests/Internal/Extensions/CommandExtensionsTests.cs` (new): `ThrowIfAckError_GivenEachMappedServerError_ShouldThrowMappedException` (`[Theory]` over `TransactionConflict`, `NotAllowedError`, `InvalidTxnStatus`, `ServiceNotReady`, `UnknownError`), `ThrowIfAckError_GivenNoError_ShouldNotThrow`.

### 8.2 Integration tests

- `tests/DotPulsar.Tests/Internal/ConsumerTests.cs`: `Acknowledge_GivenAckReceiptEnabled_ShouldCompleteAndAdvanceSubscription` (ack 10 messages, assert backlog 0 via admin REST immediately after the awaited acks - only deterministic **with** receipts), `AcknowledgeCumulative_GivenAckReceiptEnabled_ShouldComplete`.
- Port `// Upstream: AckReceiptOnTopicUnloadTest#testDelayedAckReceipt` (`java:pulsar-broker/src/test/java/org/apache/pulsar/broker/service/AckReceiptOnTopicUnloadTest.java L52-L140`, data provider `delayedAckScenarios` L47-L50 = `{false,false},{true,false},{false,true},{true,true}` for `(cumulative, unload)`) -> `Acknowledge_GivenDelayedAckReceipt_ShouldCompleteOrFailWithConsumerNotFound` with `[Theory] [InlineData(false, false)] [InlineData(true, false)] [InlineData(false, true)] [InlineData(true, true)]`. Java: one message produced (batching off), received; the broker-side handler delays processing of the client's `CommandAck`; with `unload` the topic is unloaded before the ack is released; expected: `unload == false` -> ack completes; `unload == true` -> ack fails, broker error `ConsumerNotFound`, exception message contains the broker message; afterwards a control producer on the **same** connection can still publish ("still connected"). DotPulsar adaptation: delay the client->broker direction with a Toxiproxy `latency` toxic on the **upstream** stream only (new fixture helper `IntegrationFixture.DelayUpstream(TimeSpan latency)` returning `IAsyncDisposable`), call `Acknowledge*` (not awaited), assert not completed, unload via admin REST `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/unload` when `unload`, then remove the toxic and assert: `unload == false` -> completes; `unload == true` -> throws `ConsumerNotFoundException`. Then send on a control producer created on the same client and assert success. Broker-internal assertions (`ServerCnx` identity, `command.hasRequestId()`) are not observable from DotPulsar and are dropped (documented in the test comment).
- `BatchMessageIndexAckTests` / `BatchMessageIndexAckDisableTests` (`batch-index-ack.md` 8.2) cover `ackReceiptEnabled = true/false` rows.
- Transaction tests with `.isAckReceiptEnabled(true)` (10 occurrences; owned by the transaction WP) -> `.AckReceiptEnabled(true)`.

---

## 9. Acceptance criteria

1. `AckReceiptEnabled = false`: wire output identical to today (no `request_id` on acks), all existing tests green.
2. `AckReceiptEnabled = true` against Pulsar 4.0.9: every `Acknowledge*` call awaits a `CommandAckResponse`; after the awaited ack of the last message the subscription backlog is 0 without polling.
3. An `AckResponse` frame never tears down the connection (with or without matching request).
4. A server ack error (e.g. transactional ack conflict) throws the mapped exception from `Acknowledge*` and the consumer remains `Active`/`Inactive` (not `Faulted`).
5. Topic unload while an ack receipt is pending: the call fails with `ConsumerNotFoundException` (broker answers the in-flight ack after closing the consumer) and the connection stays usable; without unload the delayed ack completes successfully (port of `AckReceiptOnTopicUnloadTest`).
6. Transactional acks always carry a `request_id` and complete on `CommandAckResponse`, regardless of `AckReceiptEnabled`.

---

## 10. Open decisions (recommendation)

1. **Operation timeout for ack receipts**: Java fails pending ack requests after `operationTimeoutMs` (default 30000 ms, `ClientConfigurationData.java L138`) with `PulsarClientException.TimeoutException("AckResponse request timeout")` via `ClientCnx.checkRequestTimeout` (L2165-L2193). DotPulsar has no client-wide operation timeout; all request/response calls today rely on the caller's `CancellationToken` and connection teardown. Recommendation: **do not** introduce a timeout in this WP; if the transaction core WP introduces a client-level `OperationTimeout`/`TransactionCoordinator` timeout, reuse it here by linking a `CancellationTokenSource(operationTimeout)` in `SubConsumer.InternalAcknowledge` (and map its expiry to the same exception type that WP defines). No ported test asserts an ack-receipt timeout.
2. **Retry vs fail on connection loss** (4.3a): recommendation retry (DotPulsar executor convention; the ack is eventually applied as in Java thanks to Java's re-add-on-failure logic). Alternative: fail the call (Java per-future behaviour). No ported test asserts either outcome for connection loss (the unload case, 4.3, is answered by the broker on the live connection and fails in both clients).
3. **Retry of transactional acks after reconnect**: recommendation retry (same rule). The broker's pending-ack handling makes a duplicate transactional ack of the same position in the same transaction a no-op; a conflicting ack from another transaction returns `TransactionConflict`, which is reported (not retried).
4. **Error message format**: Java prefixes the request id (`ClientCnx.buildError`). Recommendation: keep the broker message verbatim (consistent with all other DotPulsar mapped exceptions); tests must not assert on message text.
5. **Return type change of `IConsumerChannel.Send(CommandAck)`**: internal interface, no public impact; recommendation accept.
