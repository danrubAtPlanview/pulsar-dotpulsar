# Component: Connection-layer integration (protocol plumbing)

| | |
|---|---|
| C# | `Internal/Extensions/CommandExtensions.cs`, `Internal/RequestResponseHandler.cs`, `Internal/ChannelManager.cs`, `Internal/Abstractions/IConnection.cs`, `Internal/Connection.cs`, `Internal/ConnectionPool.cs`, `Internal/Constants.cs` |
| Upstream | `java:J/ClientCnx.java` (txn handlers L1590-1671, `handleAckResponse` L925-944, `idleCheck` L2199-2229, `channelInactive` L529-560), `java:C` (`newTxn`, `newAddPartitionToTxn`, `newAddSubscriptionToTxn`, `newEndTxn`, `newTcClientConnectRequest`) |
| WP | WP0 (unknown-response safety), WP1 (everything else) |

All DotPulsar line numbers refer to baseline `e8f2ba22c7136a35d279451e7278589fb0084d2e`. Java citations use `java:<path>#Symbol (Lx-Ly)` at `f3846f4811b6a7d98de40a3be2caff1fe288e026`. Abbreviations: `J=pulsar-client/src/main/java/org/apache/pulsar/client/impl`, `B=pulsar-broker/src/main/java/org/apache/pulsar/broker/service`, `C=pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java`.

> Line numbers drift as earlier WPs land. Locate each edit by the anchor text (method name or neighbouring member), and use the number only as a hint.

## 0. Decisions that apply to this file (authoritative where the text below differs)

1. **`ITransactionMetaStoreHandler`** is the single internal interface for a TC handler; see
   [transaction-meta-store-handler](transaction-meta-store-handler.md).
   - Besides the request methods, it has `void ConnectionClosed();`, specified in §3.2 below.
   - `ChannelManager.Dispose` calls `ConnectionClosed`. The handler reacts by cancelling `_connectionCts`, which
     makes its connect loop reconnect.
   - The handler keeps the `ulong` id returned by `IConnection.AddTransactionMetaStoreHandler(this)` and passes it to
     `RemoveTransactionMetaStoreHandler` when it leaves that connection.
2. **Proto rename (§1.4):** rename `CommandNewTxn.txn_ttl_seconds` to `txn_ttl_millis` in
   `src/DotPulsar/Internal/PulsarApi.proto`. The field number is still 2 and the wire format is identical; the new
   name matches pinned upstream L1162. The generated property becomes `TxnTtlMillis`, and callers assign
   `(ulong) timeout.TotalMilliseconds`.
3. **`IConnection.ProtocolVersion`** (§4.1, §5) is adopted. The handler sends `CommandTcClientConnectRequest` only
   when `connection.ProtocolVersion >= 19`. Otherwise it registers immediately, as Java does at L238-245.
4. **`Constants.TransactionCoordinatorAssignTopic`** (§6) is the single definition of the assign-topic name.
   `AssignTopicTcDiscovery` uses it and does not declare its own constant.
5. **`DefaultExceptionHandler`** (§11) needs no functional change. Adding `OperationTimeoutException _ => FaultAction.Rethrow`
   explicitly is optional, for readability.
6. **Unknown incoming response types** must not kill the connection. This is handled in WP0; see
   [10-implementation-plan](../10-implementation-plan.md#wp0--test-infrastructure-and-response-type-safety).

## 1. `src/DotPulsar/Internal/Extensions/CommandExtensions.cs`

### 1.1 `Throw` overloads for the new response types — insert after L50 (after `Throw(this CommandPartitionedTopicMetadataResponse)`, L49-L50)

```csharp
public static void Throw(this CommandAckResponse command)
    => Throw(command.Error, command.Message);

public static void Throw(this CommandNewTxnResponse command)
    => Throw(command.Error, command.Message);

public static void Throw(this CommandAddPartitionToTxnResponse command)
    => Throw(command.Error, command.Message);

public static void Throw(this CommandAddSubscriptionToTxnResponse command)
    => Throw(command.Error, command.Message);

public static void Throw(this CommandEndTxnResponse command)
    => Throw(command.Error, command.Message);

public static void Throw(this CommandTcClientConnectResponse command)
    => Throw(command.Error, command.Message);
```

Also (agreed with prereq-batching): factor the private switch L52-L81 into
`public static Exception ToException(ServerError error, string message)` (no throw) and make `Throw(ServerError,string)` =>
`throw ToException(error, message)`. Batching uses it for per-message `SendError` fan-out, and the TC handler can use it for the specific errors.

Why: every TC response and `CommandAckResponse` carries `optional ServerError error` + `optional string message`
(`PulsarApi.proto` L603-610, L877-881, L889-895, L904-910, L923-929, L938-944). Callers must check `HasError` first
(proto2 optional: `error` defaults to `UnknownError` = 0, so calling `Throw` without `HasError` would throw `UnknownException` on success).
Java maps the same way: `java:J/ClientCnx.java#handleAckResponse (L925-L946)` / `#handleTcClientConnectResponse (L1638-L1662)` only fail when `hasError()`.

### 1.2 `TransactionNotFound` mapping — `Throw(ServerError, string)` switch L52-L81

Insert, keeping alphabetical order, between L76 (`TransactionConflict`) and L77 (`TransactionCoordinatorNotFound`)... alphabetical is
`TransactionConflict`, `TransactionCoordinatorNotFound`, `TransactionNotFound`, so insert **after L77**:

```csharp
ServerError.TransactionNotFound => new TransactionNotFoundException(message),
```

and add the new public exception `src/DotPulsar/Exceptions/TransactionNotFoundException.cs`, copied 1:1 from
`TransactionCoordinatorNotFoundException.cs` L15-L25 (summary "Transaction not found error", both constructors).
Why: `ServerError.TransactionNotFound = 24` (`PulsarApi.proto` L245) is currently unmapped and would surface as `UnknownException`;
Java maps it to `TransactionNotFoundException` (`java:J/TransactionMetaStoreHandler.java#getExceptionByServerError (L708-L719)`).

Note on TC-response errors: Java maps them with a *separate* table (`getExceptionByServerError`, L708-L719): `TransactionCoordinatorNotFound`
-> `CoordinatorNotFoundException`, `InvalidTxnStatus` -> `InvalidTxnStatusException`, `TransactionNotFound` -> `TransactionNotFoundException`,
**anything else -> generic `TransactionCoordinatorClientException`**. To keep that parity, the TC handler (internals draft) must not call the
`Throw` overloads of 1.1 for TC responses; it maps `TransactionCoordinatorNotFound`, `InvalidTxnStatus`, `TransactionNotFound` to
`TransactionCoordinatorNotFoundException`, `InvalidTransactionStatusException`, `TransactionNotFoundException`, and everything else to
`new TransactionCoordinatorClientException(message)` (new public exception per API UPDATE). The `Throw(CommandNewTxnResponse)` … overloads of 1.1
may then be omitted; `Throw(CommandAckResponse)` is still needed (Java `handleAckResponse` uses the generic client mapping
`getPulsarClientException`, i.e. the `Throw(ServerError,string)` table). `Throw(CommandTcClientConnectResponse)` is likewise replaced by the
handler's table (Java `handleTcClientConnectResponse` L1658 uses `getExceptionByServerError`), with `NotAllowedError` checked first because Java
closes the handler on `NotAllowedException` (`TransactionMetaStoreHandler#connectionOpened` L230-L235).

### 1.3 `AsBaseCommand` for the five outgoing TC commands — append after L214 (after `AsBaseCommand(CommandGetTopicsOfNamespace)`)

```csharp
public static BaseCommand AsBaseCommand(this CommandTcClientConnectRequest command)
    => new()
    {
        Type = BaseCommand.Types.Type.TcClientConnectRequest,
        TcClientConnectRequest = command
    };

public static BaseCommand AsBaseCommand(this CommandNewTxn command)
    => new()
    {
        Type = BaseCommand.Types.Type.NewTxn,
        NewTxn = command
    };

public static BaseCommand AsBaseCommand(this CommandAddPartitionToTxn command)
    => new()
    {
        Type = BaseCommand.Types.Type.AddPartitionToTxn,
        AddPartitionToTxn = command
    };

public static BaseCommand AsBaseCommand(this CommandAddSubscriptionToTxn command)
    => new()
    {
        Type = BaseCommand.Types.Type.AddSubscriptionToTxn,
        AddSubscriptionToTxn = command
    };

public static BaseCommand AsBaseCommand(this CommandEndTxn command)
    => new()
    {
        Type = BaseCommand.Types.Type.EndTxn,
        EndTxn = command
    };
```

Generated C# names (from proto `BaseCommand.Type` L1045-L1063 and fields L1133-L1146): enum members `NewTxn`, `NewTxnResponse`,
`AddPartitionToTxn`, `AddPartitionToTxnResponse`, `AddSubscriptionToTxn`, `AddSubscriptionToTxnResponse`, `EndTxn`, `EndTxnResponse`,
`TcClientConnectRequest`, `TcClientConnectResponse`, `AckResponse`; properties `NewTxn`, `NewTxnResponse`, ..., `TcClientConnectRequest`,
`TcClientConnectResponse`, `AckResponse`.

`CommandAck.AsBaseCommand` already exists (L83-L88) and is reused for txn acks.

### 1.4 Proto field gotcha (no CommandExtensions edit, but every builder of `CommandNewTxn` must honour it)

`CommandNewTxn.txn_ttl_seconds = 2` in DotPulsar's proto (`PulsarApi.proto` L883-L887) carries **milliseconds** on the wire.
Pinned upstream renamed the same field number to `txn_ttl_millis` with the comment "the value has always been carried in milliseconds
(the client sends unit.toMillis(...) and both coordinators consume it as ms)" (`java:pulsar-common/src/main/proto/PulsarApi.proto (L1157-L1163)`);
Java sends `unit.toMillis(timeout)` (`java:J/TransactionMetaStoreHandler.java#newTransactionAsync (L292-L304)`, `java:C#newTxn (L1529-L1537)`).
Therefore set `CommandNewTxn.TxnTtlMillis = (ulong) timeout.TotalMilliseconds` after the rename decided in §0.2.

---

## 2. `src/DotPulsar/Internal/RequestResponseHandler.cs`

### 2.1 Response registrations — ctor, insert after L46 (after the `Success` registration, before the `Error` one at L47)

```csharp
_getResponseIdentifier.Set(BaseCommand.Types.Type.AckResponse, cmd => cmd.AckResponse.HasRequestId ? StandardRequest.WithRequestId(cmd.AckResponse.RequestId) : null!);
_getResponseIdentifier.Set(BaseCommand.Types.Type.TcClientConnectResponse, cmd => StandardRequest.WithRequestId(cmd.TcClientConnectResponse.RequestId));
_getResponseIdentifier.Set(BaseCommand.Types.Type.NewTxnResponse, cmd => StandardRequest.WithRequestId(cmd.NewTxnResponse.RequestId));
_getResponseIdentifier.Set(BaseCommand.Types.Type.AddPartitionToTxnResponse, cmd => StandardRequest.WithRequestId(cmd.AddPartitionToTxnResponse.RequestId));
_getResponseIdentifier.Set(BaseCommand.Types.Type.AddSubscriptionToTxnResponse, cmd => StandardRequest.WithRequestId(cmd.AddSubscriptionToTxnResponse.RequestId));
_getResponseIdentifier.Set(BaseCommand.Types.Type.EndTxnResponse, cmd => StandardRequest.WithRequestId(cmd.EndTxnResponse.RequestId));
```

Why (hazard): the default lookup at L34 **throws** for unregistered types; the throw escapes `ChannelManager.Incoming` (L193-L194)
into `Connection.ProcessIncomingFrames` whose catch-all (L377-L380) ends the frame loop, so `Setup` (L328-L336) sets the connection
`Disconnected` and every producer/consumer on it reconnects. A single unregistered TC/ack response kills the connection.

### 2.2 Does any TC response lack `request_id`? — No.

All five TC responses and `CommandAckResponse` have `request_id`:
`CommandTcClientConnectResponse.request_id` **required** (L878), `CommandNewTxnResponse` required (L890),
`CommandAddPartitionToTxnResponse` required (L905), `CommandAddSubscriptionToTxnResponse` required (L924),
`CommandEndTxnResponse` required (L939); `CommandAckResponse.request_id` is optional (L609) but the broker always echoes the
ack's request id when the ack carried one (`java:B/ServerCnx.java#handleAck (L3161-L3224)`: `hasRequestId` L3164-L3165, responses at L3187-L3190, L3194-L3197, L3206-L3210, L3218-L3221)
and sends **no** `AckResponse` at all when the ack had no request id. The broker's error paths for TC commands either use the typed
response with `request_id` (e.g. `sendNewTxnErrorResponse`, `newAddPartitionToTxnResponse(..., ServerError.NotAllowedError, ...)`) or, when the
TC is disabled, `CommandError` with the same request id (`java:B/ServerCnx.java#checkTransactionEnableAndSendError (L4055-L4065)`, called at
L4011, L4107, L4181, L4317, L4670), which DotPulsar already routes by `Error.RequestId` (L47) and `Expect()` turns into `NotAllowedException`
(CommandExtensions L28-L29 -> L65).

Java instead routes TC responses by `txnid_most_bits` (= tcId) to a `TransactionMetaStoreHandler` and **closes the channel** if no handler
is registered (`java:J/ClientCnx.java#handleNewTxnResponse (L1590-L1596)`, `#checkAndGetTransactionMetaStoreHandler (L1664-L1671)`), because
Java keeps pending ops in the handler. In DotPulsar, request ids are unique per connection (`RequestId.FetchNext`, called under
`Connection._lock`) and `StandardRequest` equality is by request id only (`Requests/StandardRequest.cs` equality members), so per-connection
request-id correlation suffices. A response whose request id is unknown is a no-op in `Awaiter.SetResult` — do **not** port the
"close the channel" behaviour.

`AckResponse` is keyed `WithRequestId` only, **not** `WithConsumerId` (agreed with prereq-batching / ack-receipt.md). With a consumer id,
`Incoming(CommandCloseConsumer)` (L148-L161) would cancel the in-flight ack. That would surface as `TaskCanceledException`, which the
executor retries (Retry) on the new channel. Java expects the opposite: on topic unload the broker still answers the in-flight ack on the same
connection with `ServerError.ConsumerNotFound`, and the ack must fail with the broker's message
(`AckReceiptOnTopicUnloadTest#testDelayedAckReceipt`, unload=true, L111-L121). `StandardRequest` equality is by request id only, so response
lookup is unaffected. Pending acks are then cancelled only by connection teardown (`Awaiter.Dispose`, retried by the executor) or by the
caller's token (see 4.2). An `AckResponse` without `request_id` maps to `null`, which `Incoming` (L144) skips, so it can never kill the connection.

### 2.3 `Outgoing` overloads — insert after L138 (after `Outgoing(CommandGetLastMessageId)`)

```csharp
public Task<BaseCommand> OutgoingWithResponse(CommandAck command)
{
    command.RequestId = _requestId.FetchNext();
    return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
}

public Task<BaseCommand> Outgoing(CommandTcClientConnectRequest command)
{
    command.RequestId = _requestId.FetchNext();
    return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
}

public Task<BaseCommand> Outgoing(CommandNewTxn command)
{
    command.RequestId = _requestId.FetchNext();
    return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
}

public Task<BaseCommand> Outgoing(CommandAddPartitionToTxn command)
{
    command.RequestId = _requestId.FetchNext();
    return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
}

public Task<BaseCommand> Outgoing(CommandAddSubscriptionToTxn command)
{
    command.RequestId = _requestId.FetchNext();
    return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
}

public Task<BaseCommand> Outgoing(CommandEndTxn command)
{
    command.RequestId = _requestId.FetchNext();
    return _requests.CreateTask(StandardRequest.WithRequestId(command.RequestId));
}
```

`OutgoingWithResponse(CommandAck)` is named distinctly because the fire-and-forget `CommandAck` path has no `Outgoing` today and
must stay request-id-less (a request id on a normal ack would make the broker send an `AckResponse` nobody awaits — harmless after 2.1,
but wasteful). The name is shared with the ack-receipt prerequisite (agreed with `prereq-consumer`).

Important: `RequestId` is assigned **on every (re)send**; a command object that is retried on a new connection gets a fresh id. Pooled or
cached TC command objects are fine for that reason, but the TC handler must not reuse a request id across connections.

---

## 3. `src/DotPulsar/Internal/ChannelManager.cs`

### 3.1 Pass-throughs — insert after L191 (after `Outgoing(CommandGetLastMessageId)`)

```csharp
public Task<BaseCommand> OutgoingWithResponse(CommandAck command)
{
    using (TakeConsumerSenderLock(command.ConsumerId))
    {
        return _requestResponseHandler.OutgoingWithResponse(command);
    }
}

public Task<BaseCommand> Outgoing(CommandTcClientConnectRequest command)
    => _requestResponseHandler.Outgoing(command);

public Task<BaseCommand> Outgoing(CommandNewTxn command)
    => _requestResponseHandler.Outgoing(command);

public Task<BaseCommand> Outgoing(CommandAddPartitionToTxn command)
    => _requestResponseHandler.Outgoing(command);

public Task<BaseCommand> Outgoing(CommandAddSubscriptionToTxn command)
    => _requestResponseHandler.Outgoing(command);

public Task<BaseCommand> Outgoing(CommandEndTxn command)
    => _requestResponseHandler.Outgoing(command);
```

`TakeConsumerSenderLock` (L247-L254) mirrors `Outgoing(CommandSeek)` / `Outgoing(CommandGetLastMessageId)` (L177-L191): it throws
`OperationCanceledException` if the consumer id is no longer registered (DefaultExceptionHandler L53 => Retry on the consumer executor).
TC commands are not tied to a producer/consumer, so no lock (same as `CommandLookupTopic`, L168-L169).

### 3.2 TC handler registration keeps the connection Active and is notified on close

Problem: `Connection.Setup` (Connection.cs L331) closes the connection after `ChannelManagerState.Inactive` persists for
`CloseInactiveConnectionsInterval` (default 60 s, PulsarClientBuilder.cs L60; continuous-Inactive semantics in
`Extensions/StateExtensions.cs` L33-L61). `ChannelManager` is Active only while it has producer/consumer channels (L287-L309).
A connection used only by the TC handler would be closed every 60 s. Java counts TC handlers as "not idle"
(`java:J/ClientCnx.java#idleCheck (L2199-L2229)`, `transactionMetaStoreHandlers` check L2212) and notifies them on channel close
(`java:J/ClientCnx.java#channelInactive (L529-L560)`, L555 `handler.connectionClosed(this)`).

New internal interface `src/DotPulsar/Internal/Abstractions/ITransactionMetaStoreHandler.cs` (namespace `DotPulsar.Internal.Abstractions`,
alongside `IChannel`):

```csharp
public interface ITransactionMetaStoreHandler
{
    void ConnectionClosed();
}
```

ChannelManager edits:
- Field (after L28): `private readonly IdLookup<ITransactionMetaStoreHandler> _transactionMetaStoreHandlers;`
- Ctor (after L38): `_transactionMetaStoreHandlers = new IdLookup<ITransactionMetaStoreHandler>();`
- New public members (after the pass-throughs of 3.1):

```csharp
public ulong AddTransactionMetaStoreHandler(ITransactionMetaStoreHandler handler)
    => AddChannel(_transactionMetaStoreHandlers, handler);

public void RemoveTransactionMetaStoreHandler(ulong handlerId)
    => _ = RemoveChannel(_transactionMetaStoreHandlers, handlerId);
```

- Make the helpers generic: L287 `private ulong AddChannel<T>(IdLookup<T> lookup, T item) where T : class` and L298
  `private T? RemoveChannel<T>(IdLookup<T> lookup, ulong id) where T : class` (bodies unchanged).
- `ChannelRemoved` L305-L309: condition becomes
  `_consumerChannels.IsEmpty() && _producerChannels.IsEmpty() && _transactionMetaStoreHandlers.IsEmpty()`.
- `Dispose` L199-L209: after the producer loop (L207-L208) add

```csharp
foreach (var handler in _transactionMetaStoreHandlers.RemoveAll())
    handler.ConnectionClosed();
```

  Order matters: `_requestResponseHandler.Dispose()` (L202) runs first and cancels every pending TC request
  (`Awaiter.Dispose` -> `TrySetCanceled`), so the handler sees its in-flight requests fail with `TaskCanceledException`
  *before* `ConnectionClosed()`; the handler must treat that as "connection lost, keep op pending and resend after reconnect"
  (Java `resendPendingRequests`, `java:J/TransactionMetaStoreHandler.java (L266-L276)`), not as a user-visible failure.

Registration timing (mirror Java `registerToConnection`, `java:J/TransactionMetaStoreHandler.java (L251-L264)`): the handler calls
`connection.AddTransactionMetaStoreHandler(this)` only after a successful `CommandTcClientConnectResponse` (or immediately when the negotiated
protocol version is < 19, Java L207 / L238-L245), and `RemoveTransactionMetaStoreHandler(id)` when the handler is closed or moves to a new
connection.

---

## 4. `src/DotPulsar/Internal/Abstractions/IConnection.cs` and `src/DotPulsar/Internal/Connection.cs`

### 4.1 IConnection (L20-L44)

- After L22 (`MaxMessageSize`): `public int ProtocolVersion { get; }` (from protocol-bump.md §5; used to gate `TcClientConnectRequest`).
- After L43 (last `Send`):

```csharp
Task<BaseCommand> SendWithResponse(CommandAck command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandTcClientConnectRequest command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandNewTxn command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandAddPartitionToTxn command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandAddSubscriptionToTxn command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandEndTxn command, CancellationToken cancellationToken);
ulong AddTransactionMetaStoreHandler(ITransactionMetaStoreHandler handler);
void RemoveTransactionMetaStoreHandler(ulong handlerId);
```

`SendWithResponse` is a distinct name (not a `Send(CommandAck, ...)` overload) so existing NSubstitute set-ups such as
`connection.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>())` (tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs L33 etc.)
stay unambiguous.

### 4.2 Connection

- After L63: `public int ProtocolVersion { get; set; }` (set by ConnectionPool, see 5).
- Six new request/response methods, each a verbatim copy of the `Send(CommandGetLastMessageId)` body (L196-L210):
  `ThrowIfDisposed(); Task<BaseCommand>? responseTask; using (await _lock.Lock(cancellationToken)...) { responseTask = _channelManager.<Outgoing>(command); var sequence = Serializer.Serialize(command.AsBaseCommand()); await _stream.Send(sequence)...; } return await responseTask...;`
  with `_channelManager.OutgoingWithResponse(command)` for `SendWithResponse(CommandAck)` and `_channelManager.Outgoing(command)` for the five TC commands.
  Insert after L314 (after `Send(CommandGetTopicsOfNamespace)`).
- Registration pass-throughs (after the above):

```csharp
public ulong AddTransactionMetaStoreHandler(ITransactionMetaStoreHandler handler)
{
    ThrowIfDisposed();
    return _channelManager.AddTransactionMetaStoreHandler(handler);
}

public void RemoveTransactionMetaStoreHandler(ulong handlerId)
    => _channelManager.RemoveTransactionMetaStoreHandler(handlerId);
```

  Caller-token cleanup (agreed with prereq-batching): in `SendWithResponse(CommandAck)`, register `cancellationToken` so a cancelled caller
  removes the awaiter entry (`Awaiter.Cancel`), otherwise an unanswered ack leaks an entry until connection teardown. The same applies to the
  five TC sends, whose callers (TC handler) pass their per-op timeout token.
  `ThrowIfDisposed` makes a registration on a dead connection fail with `ConnectionDisposedException`, which the TC handler treats as
  "reconnect" (it is a retryable type in DefaultExceptionHandler L46).
- No change to `ProcessIncomingFrames` (L352-L381): all new responses go through `_channelManager.Incoming(command)` (L374) -> default
  `_requestResponseHandler.Incoming` (ChannelManager L39).

---

## 5. `src/DotPulsar/Internal/ConnectionPool.cs` / `IConnectionPool.cs`

- `EstablishNewConnection` L168-L183: after L181 add `connection.ProtocolVersion = response.Connected.ProtocolVersion;`
  (`CommandConnected.protocol_version`, proto L321+; Java gate `cnx.getRemoteEndpointProtocolVersion() > ProtocolVersion.v18.getValue()`,
  `java:J/TransactionMetaStoreHandler.java#connectionOpened (L193-L249)`, L207).
- **No new lookup helper is required.** The TC handler uses the existing members:
  - `GetNumberOfPartitions(Constants.TransactionCoordinatorAssignTopic, ct)` (L258-L270) to discover the number of coordinators
    (Java `AssignTopicTcDiscovery#start (L58-L83)` uses `getPartitionedTopicMetadata(TRANSACTION_COORDINATOR_ASSIGN...)`; 0 partitions =>
    fail with "The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized", L76-L79);
  - `FindConnectionForTopic($"{Constants.TransactionCoordinatorAssignTopic}-partition-{tcId}", ct)` (L77-L121) to find the TC owner broker
    (Java `getTCAssignTopicName` L85-L88 + `ConnectionHandler.grabCnx()` topic lookup).
  `IConnectionPool` (L20-L27) is unchanged. Note: `GetNumberOfPartitions` itself calls `FindConnectionForTopic` on the assign topic,
  i.e. the lookup requires the client role to be allowed to look up `pulsar/system` (superuser in the test fixture; see fixture.md).

---

## 6. `src/DotPulsar/Internal/Constants.cs`

- L34: `ProtocolVersion = 14;` -> `ProtocolVersion = 19;` (consequences in protocol-bump.md).
- Add (static ctor, after L44, and property after L60):
  `TransactionCoordinatorAssignTopic = "persistent://pulsar/system/transaction_coordinator_assign";` /
  `public static string TransactionCoordinatorAssignTopic { get; }`.
  Value = Java `SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN` (`TopicName` `persistent://pulsar/system/transaction_coordinator_assign`).

---


## 11. `src/DotPulsar/Internal/DefaultExceptionHandler.cs`

**No change** (L35-L64). Rationale:
- Java retries `TransactionCoordinatorNotFound` **only inside the TC handler**, with a per-op backoff (`java:J/TransactionMetaStoreHandler.java#checkIfNeedRetryByError (L602-L615)`,
  backoff initial = client initial backoff, max = maxBackoff/10, `OpForTxnIdCallBack.create (L627-L665)`), plus a `reconnectLater` of the handler.
  The DotPulsar TC handler implements that retry itself (owned by the transaction-internals draft); it must not rely on the pipeline.
- Producer/consumer-side transactional failures (`InvalidTransactionStatusException`, `TransactionConflictException`, `NotAllowedException`,
  `TransactionNotFoundException`, `TransactionCoordinatorNotFoundException`) are `DotPulsarException` => `Rethrow` (L54) — the desired outcome
  for the *caller*; sections 9.3, 9.6 and 10.4 ensure they are raised outside any producer/consumer `Executor`, so they never fault the producer/consumer.
- If the TC handler uses an `Executor` for its transport, it must use one whose `IRegisterEvent` is a no-op (like `Producer`'s own
  executor, Producer.cs L82 + L357 `Register(IEvent) { }`), otherwise `ExecutorFaulted` has no process to go to anyway — but do not reuse a
  producer/consumer `ProcessManager`.

---

