# Component: `TransactionMetaStoreHandler`

| | |
|---|---|
| C# | `src/DotPulsar/Internal/Abstractions/ITransactionMetaStoreHandler.cs`, `src/DotPulsar/Internal/Transactions/TransactionMetaStoreHandler.cs`, `src/DotPulsar/Internal/Transactions/TransactionMetaStoreHandlerState.cs`, `src/DotPulsar/Internal/Transactions/TransactionMetaStoreOp.cs` |
| Upstream | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/TransactionMetaStoreHandler.java` (923 lines); also `ConnectionHandler`, `HandlerState`, `Backoff` (pulsar-client impl) |
| WP | WP2 |

This is the most intricate component. It owns the connection to **one** transaction coordinator and every request
sent to it.

## 1. Java model vs DotPulsar model

| Concern | Java | DotPulsar port |
|---|---|---|
| Connection | `ConnectionHandler.grabCnx()`: lookup of the assign partition topic, then connect | `IConnectionPool.FindConnectionForTopic(assignPartitionTopic, ct)` |
| Request correlation | Client-global request id; `ClientCnx` routes `*TxnResponse` by `txnid_most_bits` (the TC id) to this handler, then by `request_id` | Per-connection `RequestResponseHandler` assigns `request_id` and completes the `Task<BaseCommand>` returned by `IConnection.Send(CommandX)`. **No routing by TC id is needed.** |
| Unknown TC id in a response | `ClientCnx` closes the channel (L1664-1671) | Cannot happen: an unmatched `request_id` is ignored by `Awaiter.SetResult` (no TCS). Record as N/A in parity. |
| Pending ops | `pendingRequests` keyed by request id. They survive reconnect and are resent with the **same** request id. | `_pendingOps: ConcurrentDictionary<long /*opId*/, TransactionMetaStoreOp>`, keyed by a handler-local op id. When a connection is lost, the awaiting `Send` task is cancelled. The op stays pending and is **re-sent** on the next connection; `RequestResponseHandler` assigns a new `request_id`. |
| Executor pinning | `internalPinnedExecutor` serialises all state | Op state transitions go through `Interlocked.CompareExchange` on `op._completed` (0→1), so exactly one of response / timeout / close / resend-failure completes each op. Handler state transitions happen only in the connect loop, plus `DisposeAsync`. |
| Max pending | `Semaphore(1000)`, **blocking** `acquire` | `SemaphoreSlim(1000, 1000)`, `await WaitAsync(ct)`. Never blocks a thread. Deviation recorded in [12](../12-risks-open-questions.md). |
| Timeout | Timer sweep over `timeoutQueue` every `operationTimeout` | `System.Threading.Timer` sweep over `ConcurrentQueue<(long CreatedTicks, long OpId)>`. Same algorithm (§6). |
| Retry backoff | Per-op `Backoff(initial 100 ms, max maxBackoff/10 = 6 s)` | `Backoff` struct (§7). Same numbers. |
| Reconnect backoff | `ConnectionHandler` `Backoff(100 ms, 60 s, mandatoryStop 100 ms)` | Same numbers, in the connect loop. |

## 2. State enum

```csharp
namespace DotPulsar.Internal.Transactions;

// Upstream: HandlerState.State (subset used by TransactionMetaStoreHandler)
public enum TransactionMetaStoreHandlerState : byte { Uninitialized, Connecting, Ready, Closing, Closed, Failed }
```

Expose the state as `IState<TransactionMetaStoreHandlerState> State` through a `StateManager<>`, with final states
`Closed` and `Failed`, plus `CurrentState`, so that tests can await `Ready` (Java `getConnectHandleState`).

## 3. Constructor and fields

```csharp
public TransactionMetaStoreHandler(ulong tcId, string topic, IConnectionPool connectionPool, TimeSpan operationTimeout)
```

| Field | Notes |
|---|---|
| `_tcId`, `_topic`, `_connectionPool`, `_operationTimeout` | |
| `_stateManager` | `StateManager<TransactionMetaStoreHandlerState>(Uninitialized, Closed, Failed)` |
| `_connection` | `IConnection?`, volatile |
| `_connected` | `TaskCompletionSource<object?>`: the first successful registration. Java `connectFuture`. Exposed as `Task Connected`. |
| `_pendingOps` | `ConcurrentDictionary<long, TransactionMetaStoreOp>` |
| `_timeoutQueue` | `ConcurrentQueue<(long CreatedTicks, long OpId)>` |
| `_permits` | `SemaphoreSlim(1000, 1000)` |
| `_nextOpId` | `long`, Interlocked |
| `_lookupDeadline` | `DateTime.UtcNow + operationTimeout` (Java `lookupTimeoutMs` defaults to the operation timeout, L152) |
| `_reconnectBackoff` | `Backoff(100 ms, 60 s)` |
| `_timeoutTimer` | `Timer`, armed at construction with due time `operationTimeout` (Java L138-139) |
| `_cts` | `CancellationTokenSource` cancelled on dispose; drives the connect loop |
| `_connectLoop` | `Task?` |
| `_isDisposed` | `int` |

## 4. Connect loop (Java `start`, `connectionFailed`, `connectionOpened`, `registerToConnection`, `connectionClosed`)

`Start()` sets `Connecting` and starts `_connectLoop = Task.Run(() => ConnectLoop(_cts.Token))`.

```
ConnectLoop(ct):
  while (!ct.IsCancellationRequested)
    try
      connection = await _connectionPool.FindConnectionForTopic(_topic, ct)                 // grabCnx
      if connection.ProtocolVersion >= 19:                                                   // Java L208: remote > v18
          response = await connection.Send(new CommandTcClientConnectRequest { TcId = _tcId }, ct)
          if response.Type == Error / TcClientConnectResponse.HasError:
              error = map(response)                                                              // getExceptionByServerError
              if (state is Closing/Closed) or error is NotAllowedException:                      // L228-232
                  SetState(Closed); FailAllPending(closed); _connected.TrySetException(error); return
              throw error                                                                         // -> catch: connectionFailed
      if (!TrySetState(from Connecting, to Ready))                                          // registerToConnection L251-264
          _connected.TrySetException(new InvalidOperationException($"Failed to change the state from {state} to Ready")); return
      _connection = connection
      _registrationId = connection.AddTransactionMetaStoreHandler(this)   // keeps ChannelManager Active; see connection-integration §3.2
      _connected.TrySetResult(null); _reconnectBackoff.Reset()
      ResendPendingOps()                                                                     // L266-276
      _connectionCts = CancellationTokenSource.CreateLinkedTokenSource(ct)    // also cancelled by ConnectionClosed() and RequestReconnect()
      try { await connection.State.OnStateChangeFrom(ConnectionState.Connected, _connectionCts.Token) }  // connectionClosed
      catch (OperationCanceledException) when !ct.IsCancellationRequested { }                // RequestReconnect()
      connection.RemoveTransactionMetaStoreHandler(_registrationId)
      _connection = null; SetState(Connecting)
    catch (OperationCanceledException) when ct.IsCancellationRequested: return
    catch (Exception e):                                                                     // connectionFailed L166-190
      nonRetriable = !RetriableErrors.IsRetriable(e)        // Upstream: PulsarClientException#isRetriableError
      timedOut = DateTime.UtcNow > _lookupDeadline
      if ((nonRetriable || timedOut) && _connected.TrySetException(e)):  // only before the first success
          SetState(Failed); FailAllPending(MetaStoreHandlerNotReadyException(...not connected.)); return
    await Task.Delay(_reconnectBackoff.Next(), ct)
```

**Notes:**

- Java's `connectionFailed` only stops reconnecting if `connectFuture.completeExceptionally` returns `true`, which
  happens only before the first successful connect. After that, the handler retries forever. Port this exactly: the
  `TrySetException` guard does it.
- `IsRetriable(e)` mirrors `PulsarClientException#isRetriableError` (L1157-1181). Java lists the **non-retriable**
  types; everything else is retriable, **including authentication errors**. The DotPulsar equivalents of that list:

  | Java non-retriable type | DotPulsar type |
  |---|---|
  | `AuthorizationException` | `AuthorizationException` |
  | `InvalidServiceURL` | `InvalidSchemeException` |
  | `InvalidConfigurationException` | `ConfigurationException` |
  | `NotFoundException`, `TopicDoesNotExistException` | `TopicNotFoundException` |
  | `IncompatibleSchemaException` | `IncompatibleSchemaException` |
  | `SubscriptionNotFoundException` | `SubscriptionNotFoundException` |
  | `UnsupportedAuthenticationException` | — (no equivalent) |
  | `InvalidMessageException` | — (no equivalent) |
  | `InvalidTopicNameException` | `InvalidTopicNameException` |
  | `NotSupportedException` | `UnsupportedVersionException` |
  | `NotAllowedException` | `NotAllowedException` |
  | `ChecksumException` | `ChecksumException` |
  | `CryptoException` | `CryptoException` (new in the encryption WP; add it to this list then) |
  | `ConsumerAssignException` | `ConsumerAssignException` |
  | `MessageAcknowledgeException` | — (no equivalent) |
  | `TransactionConflictException` | `TransactionConflictException` |
  | `ProducerBusyException` | `ProducerBusyException` |
  | `ConsumerBusyException` | `ConsumerBusyException` |
  | `TransactionHasOperationFailedException` | `TransactionHasOperationFailedException` |

  Encode this as `public static bool IsRetriable(Exception exception)` in a new static class
  `Internal/RetriableErrors.cs`, with the comment `// Upstream: PulsarClientException#isRetriableError`. The TC
  handler uses it, and so do later WPs (DLQ producer, chunking).

## 5. Requests

Each request method follows the same template. Upstream: `newTransactionAsync` L292-304,
`addPublishPartitionToTxnAsync` L363-379, `addSubscriptionToTxn` L439-454, `endTxnAsync` L526-540.

```csharp
public async ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken)
{
    var command = new CommandNewTxn { TcId = _tcId, TxnTtlMillis = (ulong) timeout.TotalMilliseconds }; // proto field renamed from txn_ttl_seconds, see connection-integration §0
    var response = await Execute(TransactionMetaStoreOp.ForNewTxn(command, $"Create new transaction {_tcId}"), cancellationToken).ConfigureAwait(false);
    return new TransactionId(response.NewTxnResponse.TxnidMostBits, response.NewTxnResponse.TxnidLeastBits);
}
```

| Method | Command | Description string (verbatim from Java, used in timeout messages) | Response type |
|---|---|---|---|
| `NewTransaction(timeout, ct)` | `CommandNewTxn { TcId, TxnTtlMillis = millis }` | `"Create new transaction {tcId}"` | `NewTxnResponse` |
| `AddPublishPartitionToTxn(id, partition, ct)` | `CommandAddPartitionToTxn { TxnidMostBits, TxnidLeastBits, Partitions = { partition } }` | `"Add partition [{partition}] to TXN {id}"` | `AddPartitionToTxnResponse` |
| `AddSubscriptionToTxn(id, topic, sub, ct)` | `CommandAddSubscriptionToTxn { ..., Subscription = { new Subscription { Topic, Subscription } } }` | `"Add subscription {topic} {sub} to TXN {id}"` | `AddSubscriptionToTxnResponse` |
| `EndTxn(id, action, ct)` | `CommandEndTxn { ..., TxnAction = action }` | `"End [{action}] TXN {id}"`, with the action rendered as `COMMIT`/`ABORT` | `EndTxnResponse` |

### `Execute(op, ct)` (Java `canSendRequest` + `registerAndSend` + `checkStateAndSendRequest`)

1. `await _permits.WaitAsync(ct)`. If disposed, fail with `TransactionCoordinatorClientStateException`.
2. `op.Id = Interlocked.Increment(ref _nextOpId)`; `op.CreatedTicks = Stopwatch.GetTimestamp()`;
   `_pendingOps[op.Id] = op`; `_timeoutQueue.Enqueue((op.CreatedTicks, op.Id))`.
3. `CheckStateAndSend(op)`:
   - `Ready` with a non-null `_connection`: `_ = SendOnce(op, _connection)` (fire and forget, observed internally).
   - `Ready` with a null connection: leave the op queued. Java logs a warning and returns `true`.
   - `Connecting` / `Uninitialized`: leave the op queued; `ResendPendingOps` sends it after connect.
   - `Closing` / `Closed`: `Complete(op, new MetaStoreHandlerNotReadyException($"Transaction meta store handler for tcId {_tcId} is closing or closed."))`.
   - `Failed`: `Complete(op, new MetaStoreHandlerNotReadyException($"Transaction meta store handler for tcId {_tcId} not connected."))`.
4. `return await op.Task.WaitAsync(ct)`, using the same cancellation helper as in [transaction](transaction.md) §4.2.

### `SendOnce(op, connection)`

1. `response = await connection.Send(op.Command, CancellationToken.None)`. `op.Command` is the typed command;
   `IConnection` has one overload per TC command.
2. On `TaskCanceledException`/`OperationCanceledException`/`ConnectionDisposedException` (connection lost): return.
   The op stays in `_pendingOps` and is resent after reconnect. This matches Java, where the write is lost with the
   channel and `resendPendingRequests` sends it again.
3. On a response: extract `(hasError, error, message)` from the typed response (each `*Response` has
   `HasError`/`Error`/`Message`).
   - No error: `Complete(op, response)` (`op.TrySetResult`).
   - `error == ServerError.TransactionCoordinatorNotFound` (`checkIfNeedRetryByError` L602-615):
     1. If the state is not `Connecting`, call `RequestReconnect()` (Java `connectionHandler.reconnectLater`).
        **Never dispose the `IConnection`**, because producers and consumers share it.
        `RequestReconnect()` cancels `_connectionCts`, a per-connection `CancellationTokenSource` that the connect loop
        links into its `OnStateChangeFrom(Connected, linkedToken)` wait. The loop then unregisters, sets `Connecting`,
        and looks up the topic again (the TC may have moved to another broker).
     2. Schedule a resend after `op.Backoff.Next()` (L339-350):
        `_ = Task.Delay(delay, _cts.Token).ContinueWith(static (_, s) => ((Resend) s!).Run(), ...)`.
        When it runs, if `_pendingOps.ContainsKey(op.Id)`, call `CheckStateAndSend(op)`.
   - Any other error: `Complete(op, MapServerError(error, message))` (`getExceptionByServerError` L708-719):

     | `ServerError` | Exception |
     |---|---|
     | `TransactionCoordinatorNotFound` | `TransactionCoordinatorNotFoundException(message)` |
     | `InvalidTxnStatus` | `InvalidTransactionStatusException(message)` |
     | `TransactionNotFound` | `TransactionNotFoundException(message)` |
     | anything else | `TransactionCoordinatorClientException(message)` |
4. A `BaseCommand.Types.Type.Error` response (generic `CommandError`) is handled through `response.Expect(type)`,
   which throws the mapped exception. Complete the op with it.

### `Complete(op, result | exception)` (Java `onResponse`)

`if (Interlocked.Exchange(ref op.Completed, 1) == 0)`: remove the op from `_pendingOps`, set the TCS result or
exception, and `_permits.Release()`.

### `ResendPendingOps()` (L266-276)

`foreach (var op in _pendingOps.Values) CheckStateAndSend(op);`. Ops that fail the state check complete themselves.

## 6. Timeout sweep (Java `run(Timeout)` L814-857)

```
OnTimeoutTimer():
  if state is Closing/Closed: return
  now = Stopwatch.GetTimestamp(); timeoutTicks = ToStopwatchTicks(_operationTimeout)
  while _timeoutQueue.TryPeek(out head) && head.CreatedTicks + timeoutTicks - now <= 0:
      _timeoutQueue.TryDequeue(out _)
      if _pendingOps.TryGetValue(head.OpId, out op):
          Complete(op, new OperationTimeoutException($"{op.Description} failed due to timeout. connection: {_connection}. pending-queue: {_pendingOps.Count}"))
  next = queue empty ? _operationTimeout : max(remaining for head, or _operationTimeout if <= 0)
  _timeoutTimer.Change(next, Timeout.InfiniteTimeSpan)
```

Timeout handling holds no lock. `Complete` is idempotent, which makes the sweep race-free against responses.

## 7. `Backoff` helper

`src/DotPulsar/Internal/Backoff.cs`: `public sealed class Backoff` with `Next()` and `Reset()`.
Upstream: `pulsar-client/src/main/java/org/apache/pulsar/client/impl/Backoff.java` (or `common/util/Backoff` at the
pinned SHA; fetch it to port exactly).

- The first `Next()` returns `initial`. Each later call doubles, capped at `max`.
- Java adds random jitter: `next - random(0, next/10)`. **Port the jitter.**
- `mandatoryStop` is only used for lookup-deadline semantics, which the handler implements directly via `_lookupDeadline`.

Check first whether DotPulsar already has a backoff utility. It does not at baseline `e8f2ba2`; reconnects use the
fixed `RetryInterval` of 3 s. Create the class in this WP, because WP7 (DLQ, send-timeout) reuses it.

## 8. `DisposeAsync()` (Java `close` L895-902)

1. Guard with `_isDisposed`.
2. `_timeoutTimer.Dispose()`, `SetState(Closing)`, `_cts.Cancel()`.
3. `await _connectLoop` (swallow exceptions).
4. `SetState(Closed)`.
5. `FailAllPending(new PulsarClientClosedException("Could not get response from transaction meta store when the transaction meta store has already close."))`.
   Java uses `AlreadyClosedException` with this exact message; C# uses `PulsarClientClosedException` (see [exceptions](exceptions.md)).
6. `_connected.TrySetException(new PulsarClientClosedException(...))` if it never connected (same message as step 5).
7. Dispose `_permits` **after** failing the ops.

## 9. Test-visible members (Java `@VisibleForTesting`)

`int PendingRequestsCount => _pendingOps.Count;` `int AvailableRequestPermits => _permits.CurrentCount;`
`TransactionMetaStoreHandlerState CurrentState;` `Task Connected;`

## 10. Tests

These are unit tests with a substituted `IConnectionPool` and `IConnection`. The full step-by-step translations of
`TransactionMetaStoreHandlerTest` and `TransactionClientConnectTest` are in [08-tests/tests-client.md](../08-tests/tests-client.md).
Additional unit tests:

| Test | Asserts |
|---|---|
| `NewTransaction_GivenReadyHandler_ShouldSendCommandNewTxnWithMillisTtlAndTcId` | wire fields |
| `Request_GivenTransactionCoordinatorNotFoundTwiceThenSuccess_ShouldRetryWithBackoffAndSucceed` | L332-351 |
| `Request_GivenInvalidTxnStatus_ShouldThrowInvalidTransactionStatusException` (also TransactionNotFound, other) | L708-719 |
| `Request_GivenNoResponseWithinOperationTimeout_ShouldThrowOperationTimeoutException` | L814-857 |
| `Request_GivenConnectionLostBeforeResponse_ShouldResendOnNewConnection` | resend |
| `Request_Given1000PendingOps_ShouldWaitForPermit` | semaphore |
| `Connect_GivenTcClientConnectNotAllowed_ShouldCloseHandler` | L228-232 |
| `Connect_GivenNonRetriableErrorBeforeFirstConnect_ShouldFail` | L169-185 |
| `Connect_GivenRetriableErrorsUntilDeadline_ShouldFail` | lookup deadline |
| `Connect_GivenErrorAfterFirstConnect_ShouldKeepReconnecting` | |
| `DisposeAsync_GivenPendingOps_ShouldFailThemAndReleasePermits` | `testCloseFailsAndReleasesPendingRequests` |
| `Connected_ShouldRegisterHandlerWithConnectionSoItStaysActive` | hazard 3 |

## 11. `ConnectionClosed()` (`ITransactionMetaStoreHandler`, called by `ChannelManager.Dispose`)

Upstream: `ClientCnx#channelInactive` L555 → `TransactionMetaStoreHandler#connectionClosed` L884-892.

The method is `_connectionCts?.Cancel()`. The connect loop then:

1. leaves its wait;
2. unregisters;
3. switches to `Connecting`;
4. looks up the topic again and reconnects with backoff.

`ChannelManager.Dispose` first disposes the `RequestResponseHandler`, which cancels in-flight sends. Ops whose
`SendOnce` was cancelled therefore stay pending (§5) and are resent after the reconnect.
