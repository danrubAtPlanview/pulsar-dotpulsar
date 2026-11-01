# Test parity — client-unit tests, TC-client tests, and the transaction test sweep

Pinned upstream: `apache/pulsar@f3846f48`.
DotPulsar baseline: `e8f2ba22`. All `java:` paths are relative to the upstream repo root.

Classification legend (shared by all files in this folder):

| Status | Meaning |
|---|---|
| **Port-unit** | C# unit test, `tests/DotPulsar.Tests/Internal/Transactions/<Type>Tests.cs` (or `Internal/<Type>Tests.cs` for batching), `[Trait("Category", "Unit")]`, NSubstitute for DotPulsar internals. Java used Mockito on client internals (or the test needs no broker). |
| **Port** | C# integration test, `tests/DotPulsar.Tests/Transactions/<JavaClass>s.cs`, `[Collection("Transaction"), Trait("Category", "Integration")]`, identical intent/values. |
| **Port-partial** | Ported, but one or more assertions that depend on broker internals / Java-only mechanics are dropped or replaced; the replacement is specified. |
| **Excluded** | Not ported; reason given. |
| **Sample** | Not a test; mapped to a sample/doc. |
| "(adapted)" | Same intent, but a Java-only mechanism (Netty ref-counts, shared executors, `MockedStatic`) is replaced by the stated DotPulsar mechanism. |

Every ported C# test carries `// Upstream: <JavaClass>#<javaMethod>` directly above `[Fact]`/`[Theory]`, uses `//Arrange`, `//Act`, `//Assert`, and Shouldly.

---

## 1. Summary

### 1.1 Per file

| # | Java file | Methods | Port-unit | Port | Port-partial | Excluded | Sample | C# file(s) |
|---|---|---|---|---|---|---|---|---|
| A1 | `pulsar-client/.../impl/TransactionBuilderTest.java` | 1 | 1 | – | – | – | – | `Internal/Transactions/TransactionBuilderTests.cs` |
| A2 | `pulsar-client/.../impl/TransactionMetaStoreHandlerTest.java` | 7 | 7 (2 adapted) | – | – | – | – | `Internal/Transactions/TransactionMetaStoreHandlerTests.cs` |
| A3 | `pulsar-client/.../impl/BatchMessageContainerImplTest.java` | 24 | 17 (10 adapted) | – | 1 | 6 | – | `Internal/BatchMessageContainerTests.cs`, `Internal/KeyBasedBatchMessageContainerTests.cs` |
| A4 | `pulsar-client/.../impl/transaction/WatchTcAssignmentsDiscoveryTest.java` | 1 | – | – | – | 1 | – | – |
| A5 | `pulsar-broker/.../client/impl/RawBatchMessageContainerImplTest.java` | 10 | – | – | – | 10 | – | – |
| A6 | `pulsar-client/.../examples/TransactionExample.java` | (main) | – | – | – | – | 1 | `samples/Transactions/Program.cs` + README snippet |
| B1 | `pulsar-broker/.../client/impl/TransactionClientCloseTest.java` | 5 | 3 (+1 unit companion of #2) | 2 | – | – | – | `Transactions/TransactionClientCloseTests.cs`, `Internal/Transactions/TransactionCoordinatorClientTests.cs` |
| B2 | `pulsar-broker/.../client/impl/TransactionClientConnectTest.java` | 8 | 8 | – | – | – | – | `Internal/Transactions/TransactionCoordinatorClientTests.cs`, `Internal/Transactions/TransactionMetaStoreHandlerTests.cs` (+ 1 additional integration test) |
| B3 | `pulsar-broker/.../transaction/coordinator/TransactionCoordinatorClientTest.java` | 5 | – | 2 | 1 | 2 | – | `Transactions/TransactionCoordinatorClientTests.cs` |
| B4 | `pulsar-broker/.../transaction/coordinator/TransactionCoordinatorConfigTest.java` | 1 | – | 1 | – | – | – | `Transactions/TransactionCoordinatorConfigTests.cs` (dedicated fixture) |
| B5 | `pulsar-broker/.../transaction/coordinator/TransactionMetaStoreAssignmentTest.java` | 2 | – | – | – | 2 | – | – |
| B6 | `pulsar-broker/.../client/impl/AutoCloseUselessClientConTXTest.java` | 1 | – | – | 1 | – | – | `Transactions/AutoCloseUselessClientConTXTests.cs` |
| C | Sweep (§5): 47 further files from `want.txt` (RawBatch is A5) | 240 test methods (setup excluded) | – | 3 | 1 | rest | – | `Transactions/TransactionStablePositionTests.cs`, `Transactions/TransactionMarkerDeleteTests.cs`, `Transactions/TopicTransactionBufferRecoverTests.cs` |

### 1.2 Totals (A + B, excluding the sweep)

| Status | Count |
|---|---|
| Port-unit | 36 Java methods → 37 C# unit tests (B1 #2 additionally gets a unit companion) |
| Port | 5 |
| Port-partial | 3 (A3 #7, B3 #2, B6 #1) |
| Excluded | 21 |
| Sample | 1 |

Sweep (C): **Port 3** (`TransactionStablePositionTest#commitTxnTest`, `#abortTxnTest`, `TransactionMarkerDeleteTest#testMarkerDelete`), **Port-partial 1** (`TopicTransactionBufferRecoverTest#recoverTest`, 2 theory rows), everything else **Excluded** (broker internals, admin API, v5/scalable, geo-replication, broker-to-broker, metrics).

Additional coverage proposed (no Java counterpart, marked `// Additional coverage` instead of `// Upstream:`): 1 integration test (§4.2.9).

---

## 2. Internal API surface required by these tests (proposal)

All types below are in `src/DotPulsar/Internal/Transactions/` (namespace `DotPulsar.Internal.Transactions`) unless noted, `public sealed` per repo convention. Names follow the Java classes so the `// Upstream:` mapping is 1:1. The component spec (05-components) owns the full behaviour; listed here is only what tests touch.

### 2.1 `TransactionMetaStoreHandlerState` (Java `HandlerState.State`, `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/HandlerState.java#State (L37-48)`)

```csharp
public enum TransactionMetaStoreHandlerState : byte
{
    Uninitialized, Connecting, Ready, Closing, Closed, Terminated, Failed
}
```
(`RegisteringSchema`, `ProducerFenced` are producer-only and omitted.)

### 2.2 `TransactionMetaStoreHandler` (Java `TransactionMetaStoreHandler`, `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/TransactionMetaStoreHandler.java`)

```csharp
public sealed class TransactionMetaStoreHandler : IAsyncDisposable
{
    // Upstream: TransactionMetaStoreHandler#TransactionMetaStoreHandler (L106-153)
    public TransactionMetaStoreHandler(
        ulong transactionCoordinatorId,
        string topic,                  // "persistent://pulsar/system/transaction_coordinator_assign-partition-<tcId>"
        IConnectionPool connectionPool,
        TimeSpan operationTimeout,     // Java conf.operationTimeoutMs, default 30 s
        TimeSpan lookupTimeout,        // Java conf.lookupTimeoutMs (defaults to operationTimeout), sets the connect deadline (L152)
        TimeSpan initialBackoff,       // Java conf.initialBackoffIntervalNanos, default 100 ms
        TimeSpan maxBackoff,           // Java conf.maxBackoffIntervalNanos, default 60 s; per-op retry max = maxBackoff / 10 (L637, L678)
        int maxPendingRequests = 1000); // Java Semaphore(1000) (L137)

    public ulong TransactionCoordinatorId { get; }
    public TransactionMetaStoreHandlerState State { get; }          // Java getState()/getConnectHandleState() (L915-917)
    public Task ConnectTask { get; }                                // Java connectFuture (L94)
    public int PendingRequestsCount { get; }                        // Java getPendingRequestsCount() (L904-907)
    public int AvailableRequestPermits { get; }                     // Java getAvailableRequestPermits() (L909-912)

    public void Start();                                            // Java start() (L155-163): FindConnectionForTopic(topic) then ConnectionOpened
    public Task ConnectionOpened(IConnection connection, CancellationToken cancellationToken = default); // Java connectionOpened (L192-249)
    public bool ChangeToReadyState();                               // Java HandlerState.changeToReadyState (L69-76)
    public void SetState(TransactionMetaStoreHandlerState state);   // Java HandlerState.setState (L99-101). XML doc: "Visible for testing."

    public ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken = default);                                 // L292-304
    public ValueTask AddPublishPartitionToTxn(TransactionId txnId, IReadOnlyCollection<string> partitions, CancellationToken cancellationToken = default); // L363-379
    public ValueTask AddSubscriptionToTxn(TransactionId txnId, IReadOnlyCollection<Subscription> subscriptions, CancellationToken cancellationToken = default); // L439-454 (Subscription = DotPulsar.Internal.PulsarApi.Subscription)
    public ValueTask EndTxn(TransactionId txnId, TxnAction action, CancellationToken cancellationToken = default);                                   // L526-540

    public ValueTask DisposeAsync();                                // Java close() (L894-902) + "executor stopped" semantics, see below
}
```

Behaviour the tests pin (normative for the component spec):

1. **Serial queue.** All state transitions, `ConnectionOpened` continuation, resend, response handling, timeout sweep and the close-time "fail pending" run on one per-handler serial queue (Java `internalPinnedExecutor`, L150). `DisposeAsync()` sets `State = Closed` **synchronously**, cancels the timeout timer, then **enqueues** fail-pending (L896-901); it never runs fail-pending inline. (Required by `testFailedResendOnReconnectRemovesPendingRequest`.)
2. **Request admission** (Java `canSendRequest` L727-743 + `registerAndSend` L749-763 + `checkStateAndSendRequest` L765-811):
   - wait for one permit (blocking, `blockIfReachMaxPendingOps = true`, L136);
   - if the handler is **disposed** → fail with `PulsarClientClosedException` (message `"Could not send the request to the transaction meta store: the client is already closed."`, Java L758-762 `RejectedExecutionException` branch), release permit, record nothing;
   - else add to pending + timeout queue, then by `State`: `Ready` → send on the current connection; `Uninitialized`/`Connecting` → keep pending (sent on reconnect); `Closing`/`Closed` → fail `MetaStoreHandlerNotReadyException("Transaction meta store handler for tcId {id} is closing or closed.")`; `Failed` → `MetaStoreHandlerNotReadyException("Transaction meta store handler for tcId {id} not connected.")`; other (`Terminated`) → `MetaStoreHandlerNotReadyException(tcId)`; on failure remove from pending and release permit ("onResponse", L721-725).
3. The op's completion is **independent of the connection's response task**: closing/timeout must complete the op even when `IConnection.Send(...)` never completes ("silent coordinator").
4. Caller cancellation (`CancellationToken`) completes the caller's `ValueTask` as cancelled but **does not** remove the op; it stays pending (permit held) until response, timeout or close — then it is released (Java `commitFuture.cancel(false)` semantics, comment at test L145).
5. Close fails every pending op with `PulsarClientClosedException("Could not get response from transaction meta store when the transaction meta store has already close.")` (L278-290) and releases each permit.
6. Timeout sweep: ops older than `operationTimeout` fail with `OperationTimeoutException("{description} failed due to timeout. connection: {connection}. pending-queue: {n}")` (L813-857), are removed and release their permit.
7. `ServerError.TransactionCoordinatorNotFound` in any TC response → op is re-queued, reconnect is triggered when `State != Connecting` (L602-609), and the op is re-sent after the op backoff (initial `initialBackoff`, doubling, max `maxBackoff / 10`). Never surfaces `TransactionCoordinatorNotFoundException` to the caller while retrying. Other errors map via `GetExceptionByServerError` (L708-719): `TransactionCoordinatorNotFound` → `TransactionCoordinatorNotFoundException`, `InvalidTxnStatus` → `InvalidTransactionStatusException`, `TransactionNotFound` → `TransactionNotFoundException`, default → `TransactionCoordinatorClientException(message)`.
8. `ConnectionOpened`: if `State` is `Closing`/`Closed` → set `Closed`, fail pending, complete (L199-205). If `connection.ProtocolVersion > 18` send `CommandTcClientConnectRequest { TcId = id }` (L208-238); on success: `RegisterToConnection` = `ChangeToReadyState()`; if true → remember connection, subscribe to `connection.State.OnStateChangeFrom(ConnectionState.Connected)` (DotPulsar analog of `cnx.registerTransactionMetaStoreHandler`, L254), complete `ConnectTask`, reset backoff, resend pending (L266-276, failed resends are removed); if false → **do not** subscribe, fault `ConnectTask` with `InvalidOperationException("Failed to change the state from {state} to Ready")` (Java `IllegalStateException`, L258-262). On TC-connect failure: if `Closing`/`Closed` or `NotAllowedException` → `Closed` + complete; else fault the returned task (L223-236).
9. Connection drop (`OnStateChangeFrom(Connected)` fires) → Java `connectionClosed` (L884-892): `State = Connecting`, reconnect with backoff.

### 2.3 `IConnection` additions (`src/DotPulsar/Internal/Abstractions/IConnection.cs`)

```csharp
int ProtocolVersion { get; }  // from CommandConnected.ProtocolVersion (set in ConnectionPool L179-181 next to MaxMessageSize)
Task<BaseCommand> Send(CommandTcClientConnectRequest command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandNewTxn command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandAddPartitionToTxn command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandAddSubscriptionToTxn command, CancellationToken cancellationToken);
Task<BaseCommand> Send(CommandEndTxn command, CancellationToken cancellationToken);
```
Request ids are assigned by `RequestResponseHandler.Outgoing` (existing pattern), so substitutes in tests never need to echo request ids.

### 2.4 `TransactionCoordinatorClient` (Java `TransactionCoordinatorClientImpl`) and discovery

```csharp
public enum TransactionCoordinatorClientState : byte { None, Starting, Ready, Closing, Closed }   // Java TransactionCoordinatorClient.State

public interface ITransactionCoordinatorDiscovery : IAsyncDisposable   // Java TcDiscovery; file Internal/Transactions/ITransactionCoordinatorDiscovery.cs
{
    ValueTask Start(CancellationToken cancellationToken);
    TransactionMetaStoreHandler? HandlerForCoordinator(ulong tcId);
    TransactionMetaStoreHandler? NextHandler();
    IReadOnlyCollection<TransactionMetaStoreHandler> Handlers { get; }
}

public sealed class AssignTopicTcDiscovery : ITransactionCoordinatorDiscovery  // Java AssignTopicTcDiscovery (L43-126)
{
    public AssignTopicTcDiscovery(IConnectionPool connectionPool, Func<ulong, string, TransactionMetaStoreHandler> handlerFactory);
}

// src/DotPulsar/Internal/Abstractions/ITransactionCoordinatorClient.cs (03-public-api)
public interface ITransactionCoordinatorClient : IAsyncDisposable
{
    TransactionCoordinatorClientState State { get; }
    ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken = default);
    ValueTask AddPublishPartitionToTxn(TransactionId txnId, string partition, CancellationToken cancellationToken = default);
    ValueTask AddSubscriptionToTxn(TransactionId txnId, string topic, string subscription, CancellationToken cancellationToken = default);
    ValueTask Commit(TransactionId txnId, CancellationToken cancellationToken = default);
    ValueTask Abort(TransactionId txnId, CancellationToken cancellationToken = default);
}

public sealed class TransactionCoordinatorClient : ITransactionCoordinatorClient
{
    public TransactionCoordinatorClient(ITransactionCoordinatorDiscovery discovery);
    public IReadOnlyCollection<TransactionMetaStoreHandler> Handlers { get; }   // Java getHandlers() (L292-296)
    public ValueTask Start(CancellationToken cancellationToken = default);       // L72-88; invoked lazily by the first NewTransaction().Create()
    public ValueTask DisposeAsync();                                            // Java closeAsync (L121-143)
}
// Test accessor (API UPDATE): ((PulsarClient) client).TransactionCoordinatorClient! — type ITransactionCoordinatorClient.
// Java newTransactionAsync() without timeout = DEFAULT_TXN_TTL_MS (60000 ms) -> C# NewTransaction(TimeSpan.FromMilliseconds(60000)).
```
Errors: request while `State != Ready` → `TransactionCoordinatorClientStateException("Can not send a request while current state is {State}")` (L287-290); second `Start` → `TransactionCoordinatorClientStateException("Can not start while current state is {State}")` (L84-87); `NextHandler()` null → `TransactionCoordinatorClientException("No transaction coordinator is currently available")` (L174-177); `HandlerForCoordinator` null → `MetaStoreHandlerNotExistsException(tcId)` (L197-201); discovery start with 0 partitions → `TransactionCoordinatorClientException("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized")` (`AssignTopicTcDiscovery.java` L77-81).

### 2.5 New exceptions (`src/DotPulsar/Exceptions/`, `public sealed class X : DotPulsarException`, ctors `(string message)` and `(string message, Exception innerException)` per existing pattern)

| C# | Java | Used by |
|---|---|---|
| `TransactionCoordinatorClientException` | `TransactionCoordinatorClientException` | generic TC errors |
| `TransactionCoordinatorClientStateException` | `CoordinatorClientStateException` | B1, B3 |
| `MetaStoreHandlerNotExistsException` (+ ctor `(ulong tcId)` → `$"Transaction meta store handler for transaction meta store {tcId} not exists."`; Java L102-104 has a literal `{}` placeholder bug — C# interpolates the id) | `MetaStoreHandlerNotExistsException` | routing |
| `MetaStoreHandlerNotReadyException` (+ ctor `(ulong tcId)` → `$"Transaction meta store handler for transaction meta store {tcId} not ready now."`; Java L117-119, same `{}` bug) | `MetaStoreHandlerNotReadyException` | A2, B2 |
| `TransactionHasOperationFailedException` | `TransactionHasOperationFailedException` | not used by these tests (listed for completeness) |
| `TransactionNotFoundException` (also map `ServerError.TransactionNotFound` in `CommandExtensions.Throw`) | `TransactionNotFoundException` | error mapping |
| `OperationTimeoutException` | `PulsarClientException.TimeoutException` (also send timeout) | A2 #5, B4 |
| existing `PulsarClientClosedException` + new ctor `(string message)` | `PulsarClientException.AlreadyClosedException` | A2 #3/#6, B1 #4/#5 |
| existing `ConfigurationException` | `InvalidConfigurationException` | A1 |
| existing `InvalidTransactionStatusException`, `TransactionCoordinatorNotFoundException`, `NotAllowedException`, `TooLargeMessageException` | – | – |

(Messages verified against `java:pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TransactionCoordinatorClientException.java (L99-125)`.)

### 2.6 Batching containers (prerequisite, `src/DotPulsar/Internal/`) — surface needed by A3

```csharp
public interface IBatchMessageContainer                           // Java BatchMessageContainerBase
{
    bool Add(SendOp sendOp);                    // returns "batch is full" (Java add, L105-159)
    bool HasSameTxn(MessageMetadata metadata);  // Java hasSameTxn (AbstractBatchMessageContainer L133-149)
    int NumMessagesInBatch { get; }
    bool IsEmpty { get; }
    bool IsMultiBatches { get; }
    BatchSendOp? CreateOpSendMsg();             // null when the batch was rejected as oversized
    IReadOnlyList<BatchSendOp> CreateOpSendMsgs();
    void ResetPayloadAfterFailedPublishing();
    void Discard(Exception? exception);
    void Clear();
}

public sealed class BatchMessageContainer : IBatchMessageContainer   // Java BatchMessageContainerImpl
{
    public BatchMessageContainer(BatchMessageContainerOptions options, ArrayPool<byte>? arrayPool = null);
    public int MaxBatchSize { get; }       // initial 1024 (INITIAL_BATCH_BUFFER_SIZE)
    public int MaxMessagesNum { get; }     // initial 32 (INITIAL_MESSAGES_NUM)
    public void UpdateMaxBatchSize(int uncompressedSize);
}

public sealed class KeyBasedBatchMessageContainer : IBatchMessageContainer   // Java BatchMessageKeyBasedContainer
{
    public KeyBasedBatchMessageContainer(BatchMessageContainerOptions options, ArrayPool<byte>? arrayPool = null);
}

public sealed class BatchMessageContainerOptions
{
    public CompressionType CompressionType { get; init; }          // DotPulsar.CompressionType
    public int CompressMinMsgBodySize { get; init; }               // Java default 4 KiB; tests set 0
    public int BatchingMaxMessages { get; init; }                  // Java default 1000
    public int BatchingMaxBytes { get; init; }                     // Java default 128 KiB
    public Func<int> MaxMessageSize { get; init; }                 // Java producer.getConnectionHandler().getMaxMessageSize(), default 5 MiB
    public ICompressorFactory? CompressorFactory { get; init; }
    public IMessageCrypto? MessageCrypto { get; init; }            // encryption prerequisite seam (name owned by encryption spec)
}

public sealed class BatchSendOp : IDisposable   // Java ProducerImpl.OpSendMsg; Dispose returns its buffers to the pool (Java op.cmd.release())
{
    public MessageMetadata Metadata { get; }
    public ReadOnlySequence<byte> Data { get; }
    public IReadOnlyList<SendOp> SendOps { get; }
}
```
The Java tests count Netty reference counts; the DotPulsar analog is **ArrayPool rent/return balance**. Tests use a test-only `RecordingArrayPool : ArrayPool<byte>` (in `tests/DotPulsar.Tests/Internal/RecordingArrayPool.cs`) that records every `Rent` (array, size), every `Return`, and exposes `Rented`, `Outstanding` (rented minus returned), `DoubleReturns` (an array returned more than once or never rented), plus `Func<int, byte[]>? OnRent` to inject failures (e.g. throw `OutOfMemoryException`).

---

## 3. Part A — client-unit tests

### A1. `TransactionBuilderTest` → `tests/DotPulsar.Tests/Internal/Transactions/TransactionBuilderTests.cs`

#### A1.1 `java:pulsar-client/src/test/java/org/apache/pulsar/client/impl/TransactionBuilderTest.java#checkClientEnableTransactionConfiguration (L30-46)` — **Port-unit**

C#: `Create_GivenClientWithTransactionsDisabled_ShouldThrowConfigurationException`

1. Arrange: `await using var client = PulsarClient.Builder().ServiceUrl(new Uri("pulsar://localhost:6650")).EnableTransaction(false).Build();` (no broker needed: `Build()` does not connect, and Java's builder short-circuits before contacting a TC, `TransactionBuilderImpl.java` L55-58).
2. Act: `var exception = await Record.ExceptionAsync(() => client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create().AsTask());`
3. Assert: `exception.ShouldBeOfType<ConfigurationException>()` and `exception.Message.ShouldBe("Transactions are not enabled")`.
4. Requirement pinned: the failure is reported through the returned `ValueTask`, not thrown synchronously by `NewTransaction()`/`Create()`.

### A2. `TransactionMetaStoreHandlerTest` → `tests/DotPulsar.Tests/Internal/Transactions/TransactionMetaStoreHandlerTests.cs`

Shared test helpers (private static, in the test class; C# analogs of Java L225-280):

- `CreateHandler(IConnectionPool? pool = null, TimeSpan? operationTimeout = null)` → `new TransactionMetaStoreHandler(0, "topic", pool ?? Substitute.For<IConnectionPool>(), operationTimeout ?? TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(60))` (Java: `new TransactionMetaStoreHandler(0L, client, "topic", connectFuture)` with default client config: operation timeout 30 s, backoff 100 ms / 60 s).
- `CreateConnection()` (Java `answerConnectRequests` L262-268): `Substitute.For<IConnection>()`; `ProtocolVersion.Returns(19)`; `Send(Arg.Any<CommandTcClientConnectRequest>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(new BaseCommand { Type = BaseCommand.Types.Type.TcClientConnectResponse, TcClientConnectResponse = new CommandTcClientConnectResponse() }))`; `State.OnStateChangeFrom(ConnectionState.Connected, Arg.Any<CancellationToken>()).Returns(new ValueTask<ConnectionState>(new TaskCompletionSource<ConnectionState>().Task))` (connection never drops).
- `ConnectToSilentCoordinator(handler)` (Java L244-260): `CreateConnection()`; record sends with `Arg.Do` into a `ConcurrentQueue<IMessage> sentRequests` for `CommandNewTxn`, `CommandAddPartitionToTxn`, `CommandAddSubscriptionToTxn`, `CommandEndTxn`, each `.Returns(new TaskCompletionSource<BaseCommand>().Task)` (never answers); `await handler.ConnectionOpened(connection).WaitAsync(TimeSpan.FromSeconds(10))`; return `sentRequests`.
- `AwaitFirstSentRequest(sentRequests)` (L270-273): poll (every 10 ms, max 10 s — Awaitility default) until `sentRequests.Count == 1`; return it.
- `AwaitReleased(handler)` (L275-280; replaces `request.refCnt() == 0`): poll (max 10 s) until `handler.PendingRequestsCount == 0` **and** `handler.AvailableRequestPermits == 1000`.
- Java's `assertThat(request.refCnt()).isEqualTo(1)` "reference held by the pending request" → `handler.PendingRequestsCount.ShouldBe(1); handler.AvailableRequestPermits.ShouldBe(999);`.

#### A2.1 `#testStateChangeFailure (L53-78)` — **Port-unit**

C#: `ConnectionOpened_GivenTerminatedHandler_ShouldFaultConnectTaskAndNotRegister`

1. Arrange: `var handler = CreateHandler(); var connection = CreateConnection(); handler.SetState(TransactionMetaStoreHandlerState.Terminated);`
2. Act: `await handler.ConnectionOpened(connection);`
3. Assert: `handler.ConnectTask` completes within 3 s (`await Should.ThrowAsync<InvalidOperationException>(handler.ConnectTask.WaitAsync(TimeSpan.FromSeconds(3)))`, i.e. faulted, Java `isCompletedExceptionally`); `handler.State.ShouldBe(TransactionMetaStoreHandlerState.Terminated)`; not registered: `_ = connection.State.DidNotReceive().OnStateChangeFrom(Arg.Any<ConnectionState>(), Arg.Any<CancellationToken>())` (Java `verify(cnx, times(0)).registerTransactionMetaStoreHandler`).
4. `await handler.DisposeAsync()` in `finally` (Java `client.close()`).

#### A2.2 `#testFailedResendOnReconnectRemovesPendingRequest (L80-111)` — **Port-unit**

C#: `ConnectionOpened_GivenHandlerClosedBeforeResend_ShouldFailPendingRequestWithMetaStoreHandlerNotReadyException`

1. Arrange: `var handler = CreateHandler();` (not connected, state `Uninitialized`).
2. `var txnTask = handler.NewTransaction(TimeSpan.FromMinutes(1)).AsTask();` then poll until `handler.PendingRequestsCount == 1` (request stays queued, §2.2 rule 2).
3. `var connection = CreateConnection();` and make registration close the handler: `connection.State.OnStateChangeFrom(ConnectionState.Connected, Arg.Any<CancellationToken>()).Returns(_ => { _ = handler.DisposeAsync().AsTask(); return new ValueTask<ConnectionState>(new TaskCompletionSource<ConnectionState>().Task); });` (Java `doAnswer(... handler.close() ...).when(cnx).registerTransactionMetaStoreHandler`, L99-102).
4. Act: `await handler.ConnectionOpened(connection).WaitAsync(TimeSpan.FromSeconds(10));`
5. Assert: `await Should.ThrowAsync<MetaStoreHandlerNotReadyException>(txnTask.WaitAsync(TimeSpan.FromSeconds(10)));` and `handler.PendingRequestsCount.ShouldBe(0)`; also `connection.DidNotReceive().Send(Arg.Any<CommandNewTxn>(), Arg.Any<CancellationToken>())`.
6. Requirement pinned: `DisposeAsync` sets `Closed` synchronously but enqueues fail-pending (§2.2 rule 1), so the resend path observes `Closed` and fails with `MetaStoreHandlerNotReadyException` (not `PulsarClientClosedException`).

#### A2.3 `#testCloseFailsAndReleasesPendingRequests (L113-132)` — **Port-unit**

C#: `DisposeAsync_GivenPendingEndTxnRequest_ShouldFailItWithPulsarClientClosedExceptionAndReleaseIt`

1. Arrange: `var handler = CreateHandler(); var sentRequests = await ConnectToSilentCoordinator(handler);`
2. `var commitTask = handler.EndTxn(new TransactionId(1, 1), TxnAction.Commit).AsTask();`
3. `var request = AwaitFirstSentRequest(sentRequests)` → `((CommandEndTxn) request).TxnAction.ShouldBe(TxnAction.Commit)`, `TxnidMostBits.ShouldBe(1UL)`, `TxnidLeastBits.ShouldBe(1UL)`; pending held: `PendingRequestsCount == 1`, `AvailableRequestPermits == 999`; `commitTask.IsCompleted.ShouldBeFalse()`.
4. Act: `await handler.DisposeAsync();`
5. Assert: `await Should.ThrowAsync<PulsarClientClosedException>(commitTask.WaitAsync(TimeSpan.FromSeconds(10)));` then `await AwaitReleased(handler);`

#### A2.4 `#testCloseReleasesCancelledPendingRequests (L134-151)` — **Port-unit**

C#: `DisposeAsync_GivenCancelledPendingRequest_ShouldReleaseIt`

1. Arrange: handler + `ConnectToSilentCoordinator`; `using var cts = new CancellationTokenSource();` `var commitTask = handler.EndTxn(new TransactionId(1, 1), TxnAction.Commit, cts.Token).AsTask();`
2. `AwaitFirstSentRequest`; assert `PendingRequestsCount == 1`, `AvailableRequestPermits == 999`.
3. `cts.Cancel();` (Java `commitFuture.cancel(false)`); `await Should.ThrowAsync<OperationCanceledException>(commitTask)`; still pending: `PendingRequestsCount.ShouldBe(1)` (rule 4).
4. Act: `await handler.DisposeAsync();`
5. Assert: `await AwaitReleased(handler);`

#### A2.5 `#testTimeoutReleasesCancelledPendingRequests (L153-169)` — **Port-unit**

C#: `TimeoutSweep_GivenCancelledPendingRequest_ShouldReleaseIt`

1. Arrange: `var handler = CreateHandler(operationTimeout: TimeSpan.FromSeconds(1));` (Java `.operationTimeout(1, SECONDS)`); `ConnectToSilentCoordinator`.
2. `using var cts = new CancellationTokenSource(); var commitTask = handler.EndTxn(new TransactionId(1, 1), TxnAction.Commit, cts.Token).AsTask(); cts.Cancel();` (cancel right away, L164-165).
3. `AwaitFirstSentRequest(sentRequests)`.
4. Assert: `await AwaitReleased(handler);` (released by the 1 s timeout sweep, not by close). `finally await handler.DisposeAsync()`.

#### A2.6 `#testClientCloseFailsPendingRequestsQueuedBehindBusyExecutor (L171-195)` — **Port-unit (adapted)**

Java occupies the client's single internal executor so the close-time cleanup task is still queued when the client shuts the executor down. DotPulsar has no shared client executor; the adapted invariant is "handler dispose followed by client (pool) dispose still fails and releases the pending request".

C#: `DisposeAsync_GivenConnectionPoolDisposedRightAfter_ShouldStillFailAndReleasePendingRequest`

1. Arrange: `var pool = Substitute.For<IConnectionPool>(); var handler = CreateHandler(pool); var sentRequests = await ConnectToSilentCoordinator(handler); var commitTask = handler.EndTxn(new TransactionId(1, 1), TxnAction.Commit).AsTask(); AwaitFirstSentRequest(sentRequests);` assert pending 1 / permits 999.
2. Act: `var disposeHandler = handler.DisposeAsync().AsTask(); await pool.DisposeAsync(); await disposeHandler;` (Java `handler.close(); client.close();`).
3. Assert: `await Should.ThrowAsync<PulsarClientClosedException>(commitTask.WaitAsync(TimeSpan.FromSeconds(10)));` `await AwaitReleased(handler);`

#### A2.7 `#testClientCloseFailsRequestsQueuedBeforeBeingSent (L197-223)` — **Port-unit (adapted)**

Java queues the commit behind a busy executor so it is neither pending nor sent when the handler closes; the cleanup task then sees `Closed` → `MetaStoreHandlerNotReadyException`. In DotPulsar the equivalent observable case is "a request admitted while the handler is closed but not yet disposed".

C#: `EndTxn_GivenClosedHandler_ShouldThrowMetaStoreHandlerNotReadyExceptionAndSendNothing`

1. Arrange: handler + `ConnectToSilentCoordinator`; `var availablePermits = handler.AvailableRequestPermits;` (1000); `handler.SetState(TransactionMetaStoreHandlerState.Closed);`
2. Act: `var exception = await Record.ExceptionAsync(() => handler.EndTxn(new TransactionId(1, 1), TxnAction.Commit).AsTask().WaitAsync(TimeSpan.FromSeconds(10)));` then `await handler.DisposeAsync();`
3. Assert: `exception.ShouldBeOfType<MetaStoreHandlerNotReadyException>()`; `sentRequests.ShouldBeEmpty()`; `handler.AvailableRequestPermits.ShouldBe(availablePermits)`; `handler.PendingRequestsCount.ShouldBe(0)`.

### A3. `BatchMessageContainerImplTest` → `tests/DotPulsar.Tests/Internal/BatchMessageContainerTests.cs` and `KeyBasedBatchMessageContainerTests.cs`

Helpers (C# analogs of Java L226-263, L760-848, L936-947):

- `CreateSendOp(ulong sequenceId, string payload = "payload", ulong? txnMost = null, ulong? txnLeast = null, string? partitionKey = null)` → `new SendOp(new MessageMetadata { SequenceId = sequenceId, ProducerName = "producer", PublishTime = (ulong) DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), [TxnidMostBits/TxnidLeastBits when given], [PartitionKey] }, new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(payload)), new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously), CancellationToken.None)`. `newMessage(seq, most, least)` (L936-947) uses producer name `"producer1"` and payload `"payload-" + seq`.
- `CreateOptions(CompressionType compressionType = CompressionType.None, IMessageCrypto? crypto = null, Func<int>? maxMessageSize = null)` → `CompressMinMsgBodySize = 0` (Java L805), `BatchingMaxMessages = 1000`, `BatchingMaxBytes = 128 * 1024`, `MaxMessageSize = maxMessageSize ?? (() => 5 * 1024 * 1024)` (Java `Commands.DEFAULT_MAX_MESSAGE_SIZE`).
- `RecordingArrayPool` (§2.6).

#### A3.1 `#testUpdateMaxBatchSize (L75-130)` — **Port-unit**

C#: `UpdateMaxBatchSize_GivenGrowShrinkSequence_ShouldFollowCoolingOffRules`. Exact sequence (shrinkCoolingOffPeriod = 10):
1. `new BatchMessageContainer(CreateOptions())`; `MaxBatchSize.ShouldBe(1024)`.
2. `UpdateMaxBatchSize(2048)` → 2048. `UpdateMaxBatchSize(2)` → 2048.
3. for i in 0..14: `UpdateMaxBatchSize(2)`; i < 10 → 2048, else → `1536` (2048 * 0.75).
4. `UpdateMaxBatchSize(2048)`; for i in 0..14: i == 8 → `UpdateMaxBatchSize(2000)` else `UpdateMaxBatchSize(2)`; each iteration → 2048.
5. for i in 0..29: even → 2, odd → 2000; each → 2048.
6. 15 × `UpdateMaxBatchSize(2000)` → 2048 each.
7. `UpdateMaxBatchSize(4096)` → 4096.
(Algorithm: `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageContainerImpl.java#updateMaxBatchSize (L288-305)`.)

#### A3.2 `#recoveryAfterOom (L132-175)` — **Port-unit (adapted)**

C#: `Add_GivenBufferRentThrowsOutOfMemory_ShouldDiscardAndRecoverOnNextAdd`
1. Arrange: `var pool = new RecordingArrayPool { OnRent = _ => throw new OutOfMemoryException("test") }; var sut = new BatchMessageContainer(CreateOptions(), pool); var op1 = CreateSendOp(1, "payload1")` (producer `"producer1"`).
2. Act: `var full = sut.Add(op1);`
3. Assert: rent was attempted (`pool.RentAttempts.ShouldBe(1)`); `full.ShouldBeFalse()`; `sut.IsEmpty.ShouldBeTrue()`; `op1.ReceiptTcs.Task.IsFaulted.ShouldBeTrue()` (Java `discard(new PulsarClientException(e))`, L129-137).
4. Act 2: `var op2 = CreateSendOp(1, "payload2"); var exception = Record.Exception(() => sut.Add(op2));` Assert: `exception.ShouldBeNull()` (self-healing; the OOM keeps being thrown by the pool but `Add` never throws).

#### A3.3 `#testMessagesSize (L177-193)` — **Port-unit**

C#: `CreateOpSendMsg_GivenGrowingBatches_ShouldTrackMaxMessagesNum`. `MaxMessagesNum` starts 32; add 10 ops (sequence 0..9) + `CreateOpSendMsg()` + `Clear()` → 32; 200 ops → 200; 10 ops → 200. Dispose each returned `BatchSendOp`.

#### A3.4 `#testEntryBucketHashRangeIsStampedWhenCreatingSendOperation (L195-224)` — **Excluded**: PIP-486 entry-bucket hash range (`entry_hash_min/max`), part of the v5 scalable-topic producer; out of scope.

#### A3.5 `#testRecoveryAfterBatchBuildFailure (L273-336)`, data provider `compressionTypes` (L265-271) — **Port-unit (adapted)**

C#: `[Theory] [InlineData(CompressionType.None)] [InlineData(CompressionType.Zlib)] CreateOpSendMsg_GivenEncryptionFailsOnce_ShouldRecoverAfterReset`
1. Arrange: `var fail = true; var crypto = Substitute.For<IMessageCrypto>();` encrypt returns its input unless `fail`, in which case it throws `new InvalidOperationException("mocked encryption failure")`. Compressor: real `CompressorFactory` for Zlib wrapped in a spy that counts calls. `var pool = new RecordingArrayPool(); var sut = new BatchMessageContainer(CreateOptions(compressionType, crypto), pool);` add 2 ops (`"payload-0"`, `"payload-1"`).
2. Act/Assert 1: `Should.Throw<Exception>(() => sut.CreateOpSendMsg()).Message.ShouldContain("mocked")`.
3. If Zlib: the container no longer owns its batch buffer: `pool.Outstanding.ShouldBe(0)` (Java `compressedRef.refCnt() == 0` + buffer handed to compression); re-entry without reset: `Should.Throw<InvalidOperationException>(() => sut.CreateOpSendMsg()).Message.ShouldContain("not owned")` (Java `checkState`, L182-183). If None: `pool.Outstanding.ShouldBe(1)` (container keeps it).
4. `sut.ResetPayloadAfterFailedPublishing(); fail = false;`
5. Act 2: `using var op = sut.CreateOpSendMsg();` Assert: `op.ShouldNotBeNull()`; `op.Data.Length.ShouldBeGreaterThan(0)`; `op.Metadata.NumMessagesInBatch.ShouldBe(2)`; if Zlib: compressor called at least once and `op.Metadata.Compression.ShouldBe(PulsarApi.CompressionType.Zlib)`.
6. After `op.Dispose(); sut.Clear();`: `pool.Outstanding.ShouldBe(0)`, `pool.DoubleReturns.ShouldBe(0)`.

#### A3.6 `#testRecoveryAfterEncryptionFailure (L338-384)` — **Excluded**: failure is injected in `producer.sendMessage(...)` (SEND frame construction) *after* encryption; in DotPulsar the frame is serialized by `Connection`/`Serializer`, not by the container, so there is no post-encryption failure point inside the container. Encryption-failure recovery is covered by A3.5 and A3.7.

#### A3.7 `#testNoCompressionBuildFailureProducesValidFrame (L386-456)` — **Port-partial (adapted)**

Java runs two cases: fail at encrypt (L392) and fail at send (L393). Only the fail-at-encrypt case is portable (see A3.6).
C#: `CreateOpSendMsg_GivenEncryptionFailedOnceWithoutCompression_ShouldProduceValidSendFrame`
1. Arrange: crypto substitute throws `"mocked encryption failure"` once then returns input; `CompressionType.None`; `RecordingArrayPool`; add 3 ops (sequence 0, 1, 2).
2. `Should.Throw<Exception>(() => sut.CreateOpSendMsg()); sut.ResetPayloadAfterFailedPublishing();`
3. Act: `using var op = sut.CreateOpSendMsg();`
4. Assert: `op.ShouldNotBeNull()`; `op.Metadata.SequenceId.ShouldBe(0UL)`, `HighestSequenceId.ShouldBe(2UL)`, `NumMessagesInBatch.ShouldBe(3)`; frame: `var frame = Serializer.Serialize(new CommandSend { ProducerId = 0, SequenceId = 0, NumMessages = 1 }.AsBaseCommand(), op.Metadata, op.Data);` first 4 bytes big-endian == `frame.Length - 4` ("TOTAL_SIZE must equal the number of bytes following the total-size field"); next 4 bytes = cmdSize; `BaseCommand.Parser.ParseFrom(frame.Slice(8, cmdSize))` has `Type == BaseCommand.Types.Type.Send`. Ownership: `pool.Outstanding.ShouldBe(1)` before `op.Dispose()`, `0` after, `DoubleReturns == 0` (Java `payloadBuf.refCnt()` 1 → 0).

#### A3.8 `#testMultiBatchesPartialBuildFailureReleasesBuiltOps (L458-534)` — **Port-unit (adapted)** (requires key-based batching, see Open decision 4)

C#: `[Theory] [InlineData(CompressionType.None)] [InlineData(CompressionType.Zlib)] CreateOpSendMsgs_GivenSecondSubBatchFails_ShouldReleaseFirstBuiltOp` in `KeyBasedBatchMessageContainerTests.cs`.
1. Arrange: crypto substitute: call #2 throws `"mocked second sub-batch failure"`, others return input (Java fails `sendMessage` #2; failure point moved to the encryption step, the last per-sub-batch step the container owns). 4 ops seq 0..3, payload `"payload-" + i`, partition key `"a"` for i < 2 else `"b"`.
2. Act: `Should.Throw<Exception>(() => sut.CreateOpSendMsgs()).Message.ShouldContain("mocked second")`.
3. Assert: exactly one sub-batch had been built (crypto called twice); the built op's buffers are back in the pool: for None, sub-batch "a" buffer is still owned by the container (`Outstanding` == 2 sub-batch buffers); for Zlib, `Outstanding` == 1 (sub-batch "b" only; "a"'s compressed output was released) — Java L514-520; `DoubleReturns == 0`; `sut.NumMessagesInBatch.ShouldBe(4)`.
4. `sut.ResetPayloadAfterFailedPublishing(); var ops = sut.CreateOpSendMsgs(); ops.Count.ShouldBe(2);` dispose all; `sut.Clear()`; `pool.Outstanding.ShouldBe(0)`, `DoubleReturns.ShouldBe(0)`.

#### A3.9 `#testEntryBucketPartialBuildFailureReleasesBuiltOps (L536-610)` — **Excluded**: `EntryBucketBatchContainer` is the v5 scalable-topic batcher (PIP-486).

#### A3.10 `#discardReleasesTheBatchBufferWhenTheCallbackThrows (L612-648)` — **Port-unit (adapted)**

In DotPulsar the "callback" is the op's `TaskCompletionSource`; completing it cannot throw into the container. The portable invariant is "Discard faults the receipt, returns the buffer exactly once and clears".
C#: `Discard_GivenOneMessage_ShouldFaultReceiptReturnBufferOnceAndClear`
1. Arrange: `RecordingArrayPool`; `CompressionType.None`; `var op = CreateSendOp(0);` `sut.Add(op);`
2. Act: `sut.Discard(new UnknownException("mocked discard"));` (Java `new PulsarClientException("mocked discard")`).
3. Assert: `pool.Rented.Count.ShouldBe(1)` ("the batch buffer is the container's only allocation"); `pool.Outstanding.ShouldBe(0)`; `pool.DoubleReturns.ShouldBe(0)`; `sut.IsEmpty.ShouldBeTrue()`; `await Should.ThrowAsync<UnknownException>(op.ReceiptTcs.Task)`.

#### A3.11 `#oversizedSingleMessageReleasesTheBatchBufferExactlyOnce (L650-696)` — **Port-unit (adapted)**

C#: `CreateOpSendMsg_GivenOversizedSingleMessage_ShouldReturnNullAndReturnBufferExactlyOnce`
1. Arrange: `RecordingArrayPool`; `CompressionType.None`; one op with a zero-filled payload of `5 * 1024 * 1024 + 2048` bytes.
2. Act: `var op = sut.CreateOpSendMsg();`
3. Assert: `op.ShouldBeNull()` ("an oversized single message must not produce an op"); `pool.Rented.Count.ShouldBe(1)`; `pool.Outstanding.ShouldBe(0)`; `pool.DoubleReturns.ShouldBe(0)` ("released exactly once"); the op's receipt faulted with `TooLargeMessageException` (Java `InvalidMessageException("Message size is bigger than ...")`, L401-402).

#### A3.12 `#oversizedBatchReleasesThePayloadExactlyOnce (L698-758)` — **Port-unit (adapted)**

C#: `[Theory] [InlineData(CompressionType.None)] [InlineData(CompressionType.Zlib)] CreateOpSendMsg_GivenOversizedBatch_ShouldReturnNullAndReturnBuffersExactlyOnce`
1. Arrange: `var incompressible = new byte[5 * 1024 * 1024 + 2048]; new Random(42).NextBytes(incompressible); var half = incompressible.Length / 2;` op 0 = first half, op 1 = second half.
2. Act: `sut.CreateOpSendMsg().ShouldBeNull()`.
3. Assert: `pool.Rented` contains exactly one batch buffer (plus, for Zlib, any compression scratch buffer rented from the same pool); `pool.Outstanding.ShouldBe(0)`; `pool.DoubleReturns.ShouldBe(0)`; both receipts faulted with `TooLargeMessageException`.

#### A3.13 `#testPlainMessageIsNotBatchedWithTransactionalMessages (L850-867)` — **Port-unit**

C#: `HasSameTxn_GivenTransactionalBatchAndPlainMessage_ShouldReturnFalse`. `sut.Add(newMessage(1, 7, 13))`; `sut.HasSameTxn(newMessage(2, null, null).Metadata).ShouldBeFalse("a plain message was accepted into a transactional batch")`; `finally sut.Discard(null)`.

#### A3.14 `#testTransactionalMessageIsNotBatchedWithPlainMessages (L869-884)` — **Port-unit**

C#: `HasSameTxn_GivenPlainBatchAndTransactionalMessage_ShouldReturnFalse`. Add `newMessage(1, null, null)`; `HasSameTxn(newMessage(2, 7, 13))` → false ("a transactional message was accepted into a plain batch").

#### A3.15 `#testKeyBasedContainerKeepsTransactionalAndPlainMessagesApart (L886-905)` — **Port-unit** (key-based)

C#: `HasSameTxn_GivenKeyBasedPlainBatchAndTransactionalMessage_ShouldReturnFalse` in `KeyBasedBatchMessageContainerTests.cs`. Same as A3.14 against `KeyBasedBatchMessageContainer`.

#### A3.16 `#testSameTransactionStillBatchesTogether (L906-920)` — **Port-unit**

C#: `HasSameTxn_GivenSameAndDifferentTransactions_ShouldOnlyAcceptSame`. Add `newMessage(1, 7, 13)`; `HasSameTxn(newMessage(2, 7, 13))` → true; `HasSameTxn(newMessage(3, 7, 14))` → false.

#### A3.17 `#testPlainMessagesStillBatchTogether (L922-934)` — **Port-unit**

C#: `HasSameTxn_GivenPlainBatchAndPlainMessage_ShouldReturnTrue`. Add `newMessage(1, null, null)`; `HasSameTxn(newMessage(2, null, null))` → true.

#### A3.18 `#testKeyBasedContainerDropsTheTransactionIdentityOfAFailedFirstAdd (L949-979)` — **Port-unit (adapted)** (key-based)

Java fails the first inner allocation via `MemoryLimitController.forceReserveMemory`; DotPulsar has no memory-limit controller, so the first `Rent` throws instead.
C#: `HasSameTxn_GivenFailedFirstTransactionalAdd_ShouldNotKeepItsTransactionIdentity`
1. Arrange: `var first = true; var pool = new RecordingArrayPool { OnRent = size => { if (first) { first = false; throw new OutOfMemoryException("test"); } return null; /* default */ } };`
2. `sut.Add(newMessage(1, 7, 13)); sut.NumMessagesInBatch.ShouldBe(0, "the failed add should have left the container empty");`
3. `sut.Add(newMessage(2, null, null));`
4. Assert: `sut.HasSameTxn(newMessage(3, 7, 13).Metadata).ShouldBeFalse(...)`; `finally sut.Discard(null)`.

#### A3.19 `#testEntryBucketContainerKeepsPlainMessagesOutOfTransactionalBatches (L981-1000)` — **Excluded** (PIP-486 / v5).
#### A3.20 `#testEntryBucketContainerBatchesMessagesOfOneTransactionTogether (L1002-1019)` — **Excluded** (PIP-486 / v5).
#### A3.21 `#testEntryBucketContainerDropsTheTransactionIdentityOfAFailedFirstAdd (L1021-1051)` — **Excluded** (PIP-486 / v5).

#### A3.22 `#testOversizedBatchReleasesItsPayloadExactlyOnce (L1053-1094)` — **Port-unit (adapted)**

C#: `CreateOpSendMsg_GivenTwoMessagesOverMaxMessageSize_ShouldReturnBufferExactlyOnce`. `MaxMessageSize = () => 1` (Java `connectionHandler.getMaxMessageSize() == 1`); crypto returns input; add `newMessage(1)`, `newMessage(2)`; `pool.Rented.Count.ShouldBe(1)` ("exactly one batch payload allocation"); `sut.CreateOpSendMsg().ShouldBeNull()`; `pool.DoubleReturns.ShouldBe(0)` ("released more than once") and `pool.Outstanding.ShouldBe(0)`. (Java's `payload.retain()` trick only exists to make a double release observable; `DoubleReturns` does that directly.)

#### A3.23 `#testOversizedEncryptedBatchReleasesItsBuffersExactlyOnce (L1096-1148)` — **Port-unit (adapted)**

C#: `CreateOpSendMsg_GivenEncryptedBatchOverMaxMessageSize_ShouldReturnEachBufferExactlyOnce`. `MaxMessageSize = () => 1`; crypto substitute rents a new buffer from the same `RecordingArrayPool`, copies the input into it, returns it (Java L1115-1122 "release the input and return a different buffer"); 2 messages. Assert: `CreateOpSendMsg()` null; crypto called once ("expected exactly one encryption"); `pool.Rented.Count.ShouldBe(2)`; `pool.Outstanding.ShouldBe(0)`; `pool.DoubleReturns.ShouldBe(0)`.

#### A3.24 `#testOversizedSingleMessageBatchReleasesItsPayloadExactlyOnce (L1150-1197)` — **Port-unit (adapted)**

C#: `CreateOpSendMsg_GivenSingleMessageOverMaxMessageSize_ShouldReturnBufferExactlyOnce`. `MaxMessageSize = () => 1`; one message `newMessage(1)`; `Rented.Count == 1`; `CreateOpSendMsg()` null; `DoubleReturns == 0`; `Outstanding == 0`.

### A4. `WatchTcAssignmentsDiscoveryTest#testUnsupportedBrokerFailsWithoutRetrying (L38-57)` — **Excluded**: v5 metadata-store discovery (`WatchTcAssignmentsDiscovery`), out of scope per shared context.

### A5. `RawBatchMessageContainerImplTest` — **Excluded (all)**: `RawBatchMessageContainerImpl` is used by broker-side compaction (`RawReader`/`RawMessage`), not by the client producer. One line each:

| Method (line) | Status / reason |
|---|---|
| `testToByteBufWithBatchLimit` (L122-176) | Excluded — broker compaction container |
| `testToByteBufWithCompressionAndEncryption` (L178-229) | Excluded — same |
| `testToByteBufReleasesTheOutputBufferWhenSerializationFails` (L236-261, helper class L264-274) | Excluded — same (+ Netty ref-count) |
| `testToByteBufWithSingleMessage` (L276-308) | Excluded — same |
| `testAddDifferentBatchMessage` (L310-324) | Excluded — same |
| `testCreateOpSendMsg` (L326-330) | Excluded — same |
| `testToByteBufWithEncryptionWithoutCryptoKeyReader` (L332-348) | Excluded — same |
| `testToByteBufWithEncryptionWithInvalidEncryptKeys` (L350-369) | Excluded — same |
| `testToByteBufReleasesPayloadWhenEncryptionFailsUnexpectedly` (L375-417) | Excluded — same |
| `testToByteBufReleasesPayloadAndDiscardsWhenEncryptionFailsWithClientException` (L423-456) | Excluded — same |

### A6. `TransactionExample#main (L40-97)` — **Sample**

New project `samples/Transactions/Transactions.csproj` + `samples/Transactions/Program.cs` (top-level statements, same layout/style as `samples/Reading/Program.cs`), added to `DotPulsar.sln` under the samples folder. Exact translation:
1. `await using var client = PulsarClient.Builder().EnableTransaction(true).ExceptionHandler(...).Build(); // pulsar://localhost:6650` (Java's example omits `enableTransaction(true)` — an upstream bug since `TransactionBuilderImpl.build()` would fail; the sample must enable it and comment why. `Build()` never throws; the TC client starts lazily on the first `Create`).
2. Topics `"input-topic"`, `"output-topic-1"`, `"output-topic-2"`; consumer `Schema.String`, `SubscriptionType.Exclusive`, subscription `"transactional-sub"`; two `Schema.String` producers (Java `sendTimeout(0)` → default, DotPulsar send timeout disabled by default).
3. Loop over `consumer.Messages(ct)`: `outputMessage1 = message.Value() + "-output-1"`, `outputMessage2 = message.Value() + "-output-2"`; `var txn = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(ct)`; `var send1 = producer1.NewMessage().Transaction(txn).Send(outputMessage1, ct)`; `var send2 = ...Send(outputMessage2, ct)`; `var ack = consumer.Acknowledge(message.MessageId, txn, ct)`; `await txn.Commit(ct)`; `var msgId1 = await send1; var msgId2 = await send2;` (await `ack` before commit is not required by Java; keep Java order, but await it after commit to observe errors).
4. README (`src/DotPulsar/README.md` / `docs/api`) gets the same snippet under "Transactions".

---

## 4. Part B — broker-module TC/client tests

Test infrastructure assumed (owned by 09-test-infrastructure, listed here because these tests need it):
- `TransactionFixture` (shared context) with `CreateClient(Action<IPulsarClientBuilder>? configure = null)` → `EnableTransaction(true)`, fixture auth, `ServiceUrl`.
- TC client of a real client: `((PulsarClient) client).TransactionCoordinatorClient!` (`ITransactionCoordinatorClient`). Tests needing `Start`/`Handlers` cast to the concrete `TransactionCoordinatorClient`.
- `TransactionFixture.CreateAdminClient()` (raw HTTP, as `IntegrationFixture.CreateAdminClient()` L76).

Unit-test helpers for `TransactionCoordinatorClientTests` (`tests/DotPulsar.Tests/Internal/Transactions/TransactionCoordinatorClientTests.cs`):
- `CreatePool(IConnection connection, uint partitions = 1)`: `pool.GetNumberOfPartitions("persistent://pulsar/system/transaction_coordinator_assign", Arg.Any<CancellationToken>()).Returns(partitions)`; `pool.FindConnectionForTopic("persistent://pulsar/system/transaction_coordinator_assign-partition-0", Arg.Any<CancellationToken>()).Returns(connection)`.
- `CreateClient(IConnectionPool pool)` → `new TransactionCoordinatorClient(new AssignTopicTcDiscovery(pool, (id, topic) => new TransactionMetaStoreHandler(id, topic, pool, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(60))))`.
- Response builders: `NewTxnOk(ulong most, ulong least)`, `Error<T>(ServerError error, string message)` for each response type (`NewTxnResponse`, `AddPartitionToTxnResponse`, `AddSubscriptionToTxnResponse`, `EndTxnResponse`), e.g. `new BaseCommand { Type = BaseCommand.Types.Type.EndTxnResponse, EndTxnResponse = new CommandEndTxnResponse { Error = ServerError.TransactionCoordinatorNotFound, Message = "TC not found" } }`.

### B1. `TransactionClientCloseTest` (`java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionClientCloseTest.java`)

Java `TIMEOUT = 10 s` (L49) → every C# "fails within" uses `.WaitAsync(TimeSpan.FromSeconds(10))`.

#### B1.1 `#testEndTransactionAfterClientClose (L51-63)` — **Port**

C# `tests/DotPulsar.Tests/Transactions/TransactionClientCloseTests.cs`: `Commit_GivenClientDisposedAfterCreatingTransactions_ShouldThrowTransactionCoordinatorClientStateException`
1. Arrange: `var client = _fixture.CreateClient(); var txnToCommit = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(ct); var txnToAbort = await ...same...;`
2. Act: `await client.DisposeAsync();`
3. Assert: `await Should.ThrowAsync<TransactionCoordinatorClientStateException>(txnToCommit.Commit(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(10)))`; same for `txnToAbort.Abort(ct)` (Java "must fail instead of never completing").

#### B1.2 `#testCoordinatorRequestsFailAfterClientClose (L65-77)` — **Port** (integration) + **Port-unit companion**

Integration (`TransactionClientCloseTests.Requests_GivenDisposedClient_ShouldThrowTransactionCoordinatorClientStateException`):
1. `var client = _fixture.CreateClient(); var tcClient = ((PulsarClient) client).TransactionCoordinatorClient!;` (Java `client.getTransactionCoordinatorClient()`; with lazy start the accessor must already be non-null after `Build()` when `EnableTransaction(true)`); `await client.DisposeAsync();`
2. `await Should.ThrowAsync<TransactionCoordinatorClientStateException>(client.NewTransaction().Create(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(10)))` — requires `NewTransaction()` on a disposed client **not** to throw `PulsarClientDisposedException` synchronously (Open decision 2).
3. `assertRequestsFail(tcClient)` (L142-151), `txnId = new TransactionId(0, 0)`: `tcClient.NewTransaction(TimeSpan.FromMilliseconds(60000))`, `AddPublishPartitionToTxn(txnId, "topic")`, `AddSubscriptionToTxn(txnId, "topic", "subscription")`, `Commit(txnId)`, `Abort(txnId)` → each `TransactionCoordinatorClientStateException` within 10 s.
4. `tcClient.State.ShouldBe(TransactionCoordinatorClientState.Closed)`.

Unit companion (`TransactionCoordinatorClientTests.Requests_GivenDisposedClient_ShouldThrowTransactionCoordinatorClientStateException`, `// Upstream: TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose`):
1. Arrange: connection substitute (TC connect OK) + `CreatePool` + `CreateClient`; `await sut.Start();` `sut.State.ShouldBe(Ready)`.
2. Act: `await sut.DisposeAsync();`
3. Assert (Java `assertRequestsFail`, L142-151, `txnId = new TransactionId(0, 0)`): each of `NewTransaction(TimeSpan.FromMilliseconds(60000))`, `AddPublishPartitionToTxn(txnId, "topic")`, `AddSubscriptionToTxn(txnId, "topic", "subscription")`, `Commit(txnId)`, `Abort(txnId)` → `TransactionCoordinatorClientStateException` within 10 s; `sut.State.ShouldBe(TransactionCoordinatorClientState.Closed)`; no TC command reached the connection.

#### B1.3 `#testCoordinatorRequestsFailBeforeStart (L79-82)` — **Port-unit**

`TransactionCoordinatorClientTests.Requests_GivenClientNotStarted_ShouldThrowTransactionCoordinatorClientStateException`: `CreateClient(Substitute.For<IConnectionPool>())` without `Start`; the same five requests → `TransactionCoordinatorClientStateException` with message `"Can not send a request while current state is None"`; pool received no calls.

#### B1.4 `#testNewTransactionWhenClientClosesDuringCoordinatorSelection (L84-102)` — **Port-unit (adapted)**

Java mocks `MathUtils.signSafeMod` (inside `AssignTopicTcDiscovery.nextHandler`) to close the client mid-selection. DotPulsar seam: substitute `ITransactionCoordinatorDiscovery`.
C#: `NewTransaction_GivenClientDisposedWhileSelectingHandler_ShouldThrowPulsarClientClosedException`
1. Arrange: a real connected `TransactionMetaStoreHandler` (`ConnectionOpened` with OK TC-connect substitute); `var discovery = Substitute.For<ITransactionCoordinatorDiscovery>(); discovery.Start(default).ReturnsForAnyArgs(ValueTask.CompletedTask); discovery.DisposeAsync().Returns(_ => handler.DisposeAsync());` `var sut = new TransactionCoordinatorClient(discovery); await sut.Start();` `var closed = false; discovery.NextHandler().Returns(_ => { if (!closed) { closed = true; sut.DisposeAsync().AsTask().GetAwaiter().GetResult(); } return handler; });`
2. Act: `var exception = await Record.ExceptionAsync(() => sut.NewTransaction().AsTask().WaitAsync(TimeSpan.FromSeconds(10)));`
3. Assert: `closed.ShouldBeTrue("closed while picking the coordinator")`; `exception.ShouldBeOfType<PulsarClientClosedException>()` (Java `AlreadyClosedException`; disposed handler rule, §2.2 rule 2).

#### B1.5 `#testCommitWhenClientClosesBeforeTheRequestIsSent (L104-133)` — **Port-unit (adapted)**

Java mocks `Commands.newEndTxn` to close the client while the handler builds the END_TXN request and asserts the serialized request is released. DotPulsar seam: dispose inside `discovery.HandlerForCoordinator`.
C#: `Commit_GivenClientDisposedBeforeRequestIsSent_ShouldThrowPulsarClientClosedExceptionAndReleaseRequest`
1. Arrange: as B1.4 but `discovery.HandlerForCoordinator(0UL).Returns(_ => { sut.DisposeAsync().AsTask().GetAwaiter().GetResult(); return handler; });`
2. Act: `var exception = await Record.ExceptionAsync(() => sut.Commit(new TransactionId(0, 0)).AsTask().WaitAsync(TimeSpan.FromSeconds(10)));` (Java commits a real `Transaction`; at TC-client level the call is `commitAsync(txnId)`.)
3. Assert: `exception.ShouldBeOfType<PulsarClientClosedException>()`; `connection.DidNotReceive().Send(Arg.Any<CommandEndTxn>(), Arg.Any<CancellationToken>())`; `handler.PendingRequestsCount.ShouldBe(0)`; `handler.AvailableRequestPermits.ShouldBe(1000)` (Java "END_TXN request reference count is zero").

### B2. `TransactionClientConnectTest` (`java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionClientConnectTest.java`)

Java setup: 1 broker, 1 TC (`setUpBase(1, NUM_PARTITIONS=1, ...)`, L59-64). The five reconnect tests share `tryCommandReconnect` (L86-108): (a) `start()` (L185-197) creates two transactions (TC 0, least bits 0 and 1) then **removes the TC metadata store** → callable1 gets `TransactionCoordinatorNotFound` from the broker, the handler reconnects (TC connect re-loads the store) and retries; any failure must not be `CoordinatorNotFoundException`; (b) **fence** the store's managed ledger → callable2 must not complete within 3 s (or fail with a non-`CoordinatorNotFound` error); (c) **unfence** → the same callable2 future completes successfully. Ported as unit tests per the lead's directive: the substitute connection returns `ServerError.TransactionCoordinatorNotFound` N times then success.

Shared unit helper `TryCommandReconnect(Func<TransactionCoordinatorClient, Task> callable1, Func<TransactionCoordinatorClient, Task> callable2, Func<IConnection, Func<CallInfo, Task<BaseCommand>>> ...)` — concretely, per test:
1. Arrange connection: TC connect OK. For the command under test (`CommandNewTxn` / `CommandAddSubscriptionToTxn` / `CommandEndTxn` / `CommandAddPartitionToTxn`) configure a stateful `Returns` callback:
   - phase A ("store removed"): first **3** sends → `Error(TransactionCoordinatorNotFound, "TC not found")`, 4th → success;
   - phase B ("fenced", flag `fenced = true`): every send → `Error(TransactionCoordinatorNotFound, ...)` while `fenced`; success once `fenced == false`.
2. `pool = CreatePool(connection); sut = CreateClient(pool); await sut.Start();`
3. Act/Assert A: `var exception = await Record.ExceptionAsync(() => callable1(sut));` → `exception.ShouldBeNull()` (stronger than Java, which tolerates a non-`CoordinatorNotFound` failure and retries once after `waitToReady`; in the deterministic unit setup the retry succeeds). Reconnect happened: `pool.Received(Quantity.Within(2, int.MaxValue)).FindConnectionForTopic("persistent://pulsar/system/transaction_coordinator_assign-partition-0", Arg.Any<CancellationToken>())` and `connection.Received(Quantity.Within(2, int.MaxValue)).Send(Arg.Any<CommandTcClientConnectRequest>(), Arg.Any<CancellationToken>())`; exactly 4 sends of the command under test.
4. Act B: `fenced = true; var task = callable2(sut); var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(3))) == task;` Assert: if `completed` then `task.Exception!.InnerException.ShouldNotBeOfType<TransactionCoordinatorNotFoundException>()` else not completed (expected). Java: `completableFuture.get(3, SECONDS)` with `TimeoutException` ignored.
5. `fenced = false; await task.WaitAsync(TimeSpan.FromSeconds(30));` (Java `completableFuture.get()` unbounded; 30 s guard = default operation timeout) → completes successfully.

| Java method (lines) | C# method (`TransactionCoordinatorClientTests`) | callable1 / callable2 | Extra assertion |
|---|---|---|---|
| `#testTransactionNewReconnect (L71-76)` | `NewTransaction_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` | both `sut.NewTransaction(TimeSpan.FromMilliseconds(200))` (Java `pulsarClient.newTransaction().withTransactionTimeout(200, MILLISECONDS).build()`; public equivalent `NewTransaction().Timeout(TimeSpan.FromMilliseconds(200)).Create()`) | every captured `CommandNewTxn` has `TcId == 0` and `TxnTtlSeconds == 200` (Java sends **milliseconds** in `txn_ttl_seconds`, `TransactionMetaStoreHandler.java` L299 `unit.toMillis(timeout)`); result `TransactionId` equals the success response's bits |
| `#testTransactionAddSubscriptionToTxnAsyncReconnect (L78-84)` | `AddSubscriptionToTxn_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` | both `sut.AddSubscriptionToTxn(new TransactionId(0, 0), "test", "test")` | captured command: `TxnidMostBits 0`, `TxnidLeastBits 0`, one `Subscription { Topic = "test", Subscription_ = "test" }` |
| `#testTransactionAbortToTxnAsyncReconnect (L110-118)` | `Abort_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` | `sut.Abort(new TransactionId(0, 0))` / `sut.Abort(new TransactionId(0, 1))` | `TxnAction == TxnAction.Abort`; least bits 0 then 1 |
| `#testTransactionCommitToTxnAsyncReconnect (L120-128)` | `Commit_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` | `sut.Commit(new TransactionId(0, 0))` / `sut.Commit(new TransactionId(0, 1))` | `TxnAction == TxnAction.Commit`; least bits 0 then 1 |
| `#testTransactionAddPublishPartitionToTxnReconnect (L130-137)` | `AddPublishPartitionToTxn_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` | both `sut.AddPublishPartitionToTxn(new TransactionId(0, 0), "test")` | `Partitions` == `["test"]` |

All five: **Port-unit**.

#### B2.6 `#testPulsarClientCloseThenCloseTcClient (L139-164)` — **Port-unit**

C# `TransactionMetaStoreHandlerTests.NewTransaction_GivenHandlerStateSetToClosed_ShouldThrowMetaStoreHandlerNotReadyException`
1. Arrange: handler + connection whose `Send(CommandNewTxn)` returns `NewTxnOk(0, 0)`; `await handler.ConnectionOpened(connection)`; `await handler.NewTransaction(TimeSpan.FromSeconds(10))` succeeds (Java first loop, L145-147).
2. Act: `handler.SetState(TransactionMetaStoreHandlerState.Closed)` (Java reflection on `HandlerState.state`, L148-152).
3. Assert: `handler.State.ToString().ShouldBe("Closed")` (Java `getConnectHandleState().toString()`); `await Should.ThrowAsync<MetaStoreHandlerNotReadyException>(handler.NewTransaction(TimeSpan.FromSeconds(10)).AsTask())` (Java only checks the type *if* it throws; the C# test asserts it throws — rule 2 makes it deterministic).

#### B2.7 `#testHandlerStateChangeToReady (L166-174)` — **Port-unit**

C# `TransactionMetaStoreHandlerTests.ChangeToReadyState_GivenReadyHandler_ShouldReturnTrue`: connect with OK substitute; `handler.State.ShouldBe(TransactionMetaStoreHandlerState.Ready)`; `handler.ChangeToReadyState().ShouldBeTrue()`.

#### B2.8 `#testNotEnableTransactionInBroker (L177-183)` — **Port-unit**

Java deletes the `transaction_coordinator_assign` partitioned-topic metadata so `PulsarClient.build()` (which starts the TC client synchronously, `PulsarClientImpl.java` L345-353) throws `PulsarClientException`.
C# `TransactionCoordinatorClientTests.Start_GivenNoTransactionCoordinatorPartitions_ShouldThrowTransactionCoordinatorClientException`: `CreatePool(connection, partitions: 0)`; `await Should.ThrowAsync<TransactionCoordinatorClientException>(sut.Start().AsTask())` with message `"The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized"`; `pool.DidNotReceiveWithAnyArgs().FindConnectionForTopic(default!, default)`. Publicly (lazy start) the first `client.NewTransaction().Create()` faults with this exception; `IPulsarClientBuilder.Build()` never throws.

#### B2.9 Additional coverage (integration) — `TransactionClientConnectTests.NewTransaction_GivenConnectionDroppedAndRestored_ShouldSucceed`

`// Additional coverage` (exercises the real reconnect path the unit tests simulate): `_fixture.CreateClient()`, create+commit one transaction; `await using (await _fixture.DisableThePulsarConnection())` (Toxiproxy, `IntegrationFixture.cs` L205) start `client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(ct)` while disconnected; after re-enable the `Create` completes and the transaction commits. Requires `TransactionFixture` to route through Toxiproxy like `IntegrationFixture`.

### B3. `TransactionCoordinatorClientTest` (`java:pulsar-broker/src/test/java/org/apache/pulsar/broker/transaction/coordinator/TransactionCoordinatorClientTest.java`, base `TransactionMetaStoreTestBase.java`)

Java base: 5 brokers, 16 TCs, `TransactionBufferClient` mocked to succeed (L46-65), TC client started in setup (L93-99). C#: `tests/DotPulsar.Tests/Transactions/TransactionCoordinatorClientTests.cs` on `TransactionFixture` (1 TC, real transaction buffer); the TC client is `((PulsarClient) client).TransactionCoordinatorClient!` of a fixture client; class-level `IAsyncLifetime.InitializeAsync` creates and commits one transaction via `client.NewTransaction().Create()` to trigger the lazy start (Java base starts it in setup, L96-97).

#### B3.1 `#testClientStart (L67-79)` — **Port**

`Start_GivenAlreadyStartedClient_ShouldThrowTransactionCoordinatorClientStateException`: started client; `await Should.ThrowAsync<TransactionCoordinatorClientStateException>(((TransactionCoordinatorClient) sut).Start(ct).AsTask())` ("should failed here because the transaction metas store already started!"); `sut.State.ShouldBe(TransactionCoordinatorClientState.Ready)`.

#### B3.2 `#testNewTxn (L81-86)` — **Port-partial**

`NewTransaction_GivenStartedClient_ShouldReturnTransactionId`: `var txnId = await sut.NewTransaction(TimeSpan.FromMilliseconds(60000), ct);` `txnId.ShouldNotBe(default)`; `txnId.MostSigBits.ShouldBe(0UL)` (single TC). Dropped: `LeastSigBits == 0` — holds only on a freshly started TC; the shared `TransactionFixture` broker has served other transactions.

#### B3.3 `#testCommitAndAbort (L88-100)` — **Port**

`Abort_GivenCommittedTransaction_ShouldThrow`: `NewTransaction(TimeSpan.FromMilliseconds(60000))`; `AddPublishPartitionToTxn(txnId, "persistent://public/default/testCommitAndAbort")` (create the topic first via `_fixture.CreateTopic("persistent://public/default/testCommitAndAbort", ct)` so the real TB can end it — Java mocks the TB client); `Commit(txnId)`; `var exception = await Record.ExceptionAsync(() => sut.Abort(txnId, ct).AsTask());` → `exception.ShouldNotBeNull()` and `exception.ShouldBeAssignableTo<DotPulsarException>()` and is one of `InvalidTransactionStatusException` / `TransactionNotFoundException` (Java catches the `TransactionCoordinatorClientException` base: "Should be fail, because the txn is in committing state, can't abort now.").

#### B3.4 `#testTransactionCoordinatorExceptionUnwrap (L102-115)` — **Excluded**: tests Java `ExecutionException` unwrapping (`TransactionCoordinatorClientException.unwrap`); `await` surfaces the original exception in .NET.

#### B3.5 `#testClientStartWithRetry (L117-135)` — **Excluded**: multi-host service URL (`"<valid>,localhost:0"`); DotPulsar supports a single service URL only.

### B4. `TransactionCoordinatorConfigTest#testMaxActiveTxn (L63-97)` — **Port** (dedicated fixture)

C# `tests/DotPulsar.Tests/Transactions/TransactionCoordinatorConfigTests.cs`, `[Collection("TransactionMaxActive"), Trait("Category", "Integration")]`, fixture `TransactionMaxActiveFixture` = `TransactionFixture` + broker env `PULSAR_PREFIX_maxActiveTransactionsPerCoordinator=2` (Java L44-54: TC enabled, 1 TC partition).
C#: `Create_GivenMaxActiveTransactionsReached_ShouldTimeOutUntilTransactionsEnd`
1. Arrange: `await using var client = _fixture.CreateClient(b => b.OperationTimeout(TimeSpan.FromSeconds(3)));` (Java `.operationTimeout(3, SECONDS)`; builder member `OperationTimeout(TimeSpan)`, default 30 s).
2. `var commitTxn = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(ct); var abortTxn = ...;`
3. Third `Create` → `await Should.ThrowAsync<OperationTimeoutException>(...Create(ct).AsTask())` ("broker will return any response").
4. `await commitTxn.Commit(ct); await abortTxn.Abort(ct);`
5. Two more `Create` calls (1 minute timeout) succeed.
6. Third `Create` again → `OperationTimeoutException`.

### B5. `TransactionMetaStoreAssignmentTest` (`java:pulsar-broker/src/test/java/org/apache/pulsar/broker/transaction/coordinator/TransactionMetaStoreAssignmentTest.java`)

| Method | Status / reason |
|---|---|
| `#testTransactionMetaStoreAssignAndFailover (L44-74)` | **Excluded** — needs a 3-broker cluster, a broker crash and broker-side store counts (`getStores().size() == 16`). |
| `#testTransactionMetaStoreUnload (L76-99)` | **Excluded** — relies on `removeTransactionMetadataStore` (broker internal) and store counts; the client-observable part ("a client re-initialises a TC that is gone via `TcClientConnectRequest`") is covered by B2.1–B2.5. |

### B6. `AutoCloseUselessClientConTXTest#testConnectionAutoReleaseUnPartitionedTopicWithTransaction (L124-153)` — **Port-partial**

Java: 5 brokers, connections to every broker, manual "mark and release useless connections", pool size `>= 5` then `<= 3`, transactions keep working (helpers in `AutoCloseUselessClientConSupports.java` L68-93, L180-198). DotPulsar's equivalent mechanism is `CloseInactiveConnectionsInterval` (connection closed when its `ChannelManager` has no producers/consumers, `Connection.cs` L331) — a **hazard** for the TC handler, whose connection has no channels.
C# `tests/DotPulsar.Tests/Transactions/AutoCloseUselessClientConTXTests.cs`: `Transactions_GivenInactiveConnectionsClosed_ShouldKeepWorking`
1. Arrange: `await using var client = _fixture.CreateClient(b => b.CloseInactiveConnectionsInterval(TimeSpan.FromSeconds(1)));` (Java shortens max idle to 1 s, L87); `Build()` never throws and the first `Create` starts the TC client lazily; topic `persistent://public/default/<32-hex-guid>` (Java L49-50, non-partitioned, created first).
2. `EnsureTransactionWorks(client, producer, consumer)` where producer/consumer are created *after* step 3 (so the only traffic before is TC traffic): first `var txn0 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(ct); await txn0.Commit(ct);`
3. Wait 3 s (Java: `Thread.sleep(2000)` + up to 5 s await, L91 and L143-146) — the TC-only connection is closed as inactive.
4. Create consumer (subscription `"my-subscription-x"`) and producer; `EnsureTransactionWorks` (Java L180-198): send `Guid` string plain → receive and assert equal; send `Guid` string with a 5-minute transaction; ack the plain message with the transaction; commit; receive the transactional message and assert equal; ack.
5. Dropped: pool-size assertions (`>= 5`, `<= 3`) — single-broker fixture, and DotPulsar exposes no pool size.
6. Requirement pinned: the handler must reconnect when its connection is closed (§2.2 rule 9) *or* hold the connection active; either way step 2 → 4 succeeds.

---

## 5. Part C — classification sweep of the remaining `want.txt` transaction tests

Covered by sibling agents and therefore not listed: `TransactionEndToEndTest`, `TransactionEndToEndWithoutBatchIndexAckTest`, `TransactionTestBase`, `TransactionTest`, `TransactionProduceTest`, `TransactionConsumeTest`, `PendingAckPersistentTest`, `ConsumerRedeliveryTest`, `TopicTransactionBufferTest`, `TopicTransactionBufferPublishOrderTest`, `AuthenticatedTransactionProducerConsumerTest`, `TransactionAndSchemaAuthZTest`. Paths below are under `pulsar-broker/src/test/java/org/apache/pulsar/`.

### 5.1 Portable

#### C1 `broker/transaction/buffer/TransactionStablePositionTest.java` (setup L62-68: 1 broker, 16 TCs, topic `tnx/ns1/test-topic`)

C# `tests/DotPulsar.Tests/Transactions/TransactionStablePositionTests.cs`. Topic: unique `persistent://public/default/test-topic-<guid>` (see Open decision 6).

- `#commitTxnTest (L75-120)` — **Port**: `Commit_GivenTransactionalMessageBetweenPlainMessages_ShouldBlockReadsUntilCommit`
  1. txn timeout 5 s; producer batching disabled (Java `enableBatching(false)`, `sendTimeout(0)`); consumer subscription `"test"`, `SubscriptionInitialPosition.Earliest`, `SubscriptionType.Failover`.
  2. send plain `"test1"`, txn `"test2"`, plain `"test3"` (UTF-8 bytes).
  3. `ReceiveOrNull(2 s)` → `"test1"`; `ReceiveOrNull(2 s)` → null.
  4. `await txn.Commit(ct)`; receive → `"test2"`; receive → `"test3"`; `ReceiveOrNull(2 s)` → null.
- `#abortTxnTest (L122-162)` — **Port**: `Abort_GivenTransactionalMessageBetweenPlainMessages_ShouldBlockReadsUntilAbortAndDropIt`: same arrange; after abort receive → `"test3"`, then null.
- `#testSyncNormalPositionWhenTBRecover (L164-259)` — **Excluded**: injects `TopicTransactionBufferState` and inspects the broker's max-read position.

#### C2 `broker/service/TransactionMarkerDeleteTest.java`

- `#testMarkerDeleteTimes (L70-91)` — **Excluded**: Mockito spies on `ManagedLedgerImpl`/`PersistentSubscription` (broker-only).
- `#testMarkerDelete (L94-158)` — **Port**: `tests/DotPulsar.Tests/Transactions/TransactionMarkerDeleteTests.cs`, `Acknowledge_GivenTransactionMarkers_ShouldMoveMarkDeletePositionPastMarkers`. Needs ack receipt (prerequisite) and admin internal stats via HTTP `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/internalStats` → `cursors["testMarkerDelete"].markDeletePosition` (string `"{ledgerId}:{entryId}"`).
  1. Topic `.../testMarkerDelete-<guid>`, subscription `"testMarkerDelete"`, `SubscriptionType.Shared`, ack receipt enabled; producer (batching default as Java: enabled? Java uses default `newProducer()` → batching enabled; use DotPulsar default once batching exists; message ids are compared at ledger/entry granularity so either works); four transactions with 10 s timeout (`getTxn`, L160-165).
  2. `msgId1 = send(txn1)`, `msgId2 = send(txn2)` (empty payload); `ReceiveOrNull(1 s)` → null; commit txn1.
  3. receive + `Acknowledge` (await receipt); `ReceiveOrNull(1 s)` → null; markDeletePosition == `$"{msgId1.LedgerId}:{msgId1.EntryId}"`.
  4. `msgId3 = send(txn3)`; commit txn2; receive + ack; null; markDeletePosition == `$"{msgId2.LedgerId}:{msgId2.EntryId + 1}"`.
  5. `msgId4 = send(txn4)`; commit txn3; receive + ack; null; markDeletePosition == `$"{msgId3.LedgerId}:{msgId3.EntryId + 1}"`.
  6. abort txn4; poll (max 10 s) until markDeletePosition == `$"{msgId4.LedgerId}:{msgId4.EntryId + 2}"`.

#### C3 `broker/transaction/TopicTransactionBufferRecoverTest.java` (setup L106-112: 1 broker, 16 TCs; topics `tnx/ns1/recover-commit`, `tnx/ns1/recover-abort`)

- `#recoverTest (L148-255)`, data provider `testTopic` = {RECOVER_ABORT, RECOVER_COMMIT} (L132-138) — **Port-partial**. Note: declared `private` upstream; port regardless (intent explicit).
  C# `tests/DotPulsar.Tests/Transactions/TopicTransactionBufferRecoverTests.cs`, `[Theory] [InlineData("recover-abort")] [InlineData("recover-commit")] Commit_GivenTopicUnloadedWithOpenTransactions_ShouldRecoverTransactionBuffer(string topicSuffix)`.
  1. `tnx1`, `tnx2` timeout 30 s; consumer `Schema.String`, subscription `"test-recover"`; producer `Schema.String`, batching disabled.
  2. for i in 0..9: `"Hello Txn - " + i` → even i with tnx1, odd with tnx2.
  3. `ReceiveOrNull(2 s)` → null; commit tnx1; receive → not null (only `"Hello Txn - 0"` is below tnx2's first position) and ack; `ReceiveOrNull(2 s)` → null.
  4. Unload topic via admin HTTP `PUT /admin/v2/persistent/public/default/<topic>/unload`; **replacement** for the broker-internal `TopicTransactionBuffer.checkIfReady()` await (L203-222): await consumer and producer `StateChangedTo(Connected)` after a `Disconnected` transition, max 30 s.
  5. commit case: commit tnx2; receive 9 messages (loop `i = 10 .. 2`), ack each; `ReceiveOrNull(2 s)` → null. abort case: abort tnx2; receive 4 messages (loop `i = 5 .. 2`), ack each; null.
- `#testTBRecoverCanRetryIfTimeoutRead (L312-405)` — **Excluded**: mocks the broker's snapshot system-topic reader.
- `#testTopicTransactionBufferDeleteAbort (L406-507)` — **Excluded**: toggles broker config and reads `ManagedLedgerImpl.ledgers` / aborted-txn processor via reflection.
- `#clearTransactionBufferSnapshotTest (L509-598)` — **Excluded**: broker snapshot topic internals.
- `#testTransactionBufferRecoverThrowException (L600-708)` — **Excluded**: broker snapshot reader fault injection.
- `#testTransactionBufferIndexSystemTopic (L710-755)`, `#testTransactionBufferSegmentSystemTopic (L757-849)`, `#testSnapshotSegment (L851-941)` — **Excluded**: broker snapshot system topics.

### 5.2 Excluded (one line per file; every test method in the file is excluded)

| File (methods) | Reason |
|---|---|
| `PulsarTransactionCoordinatorMetadataSetupTest` (`testMainGenerateDocs`) | Broker CLI tool. |
| `broker/admin/v3/AdminApiTransactionMultiBrokerTest` (2) | Admin API, multi-broker; DotPulsar has no admin client (out of scope). |
| `broker/admin/v3/AdminApiTransactionTest` (24) | Admin API (stats, peek, abort-by-admin, coordinator number). |
| `broker/admin/v3/AdminApiTransactionWithAuthTest` (7) | Admin API abort authorization. |
| `broker/loadbalance/extensions/ExtensibleLoadManagerImplWithTransactionCoordinatorTest` (`testUnloadAdminAPI`) | Broker load manager. |
| `broker/service/ReplicatedSubscriptionWithTransactionTest` (inherits `ReplicatedSubscriptionTest`) | Geo-replication, multi-cluster. |
| `broker/service/ReplicationTxnTest` (2) | Geo-replication, multi-cluster. |
| `broker/service/TransactionMetadataStoreServiceTest` (9) | Broker TC service internals. |
| `broker/service/TransactionalReplicateSubscriptionTest` (1) | Geo-replication, multi-cluster. |
| `broker/service/persistent/SegmentSubscriptionDeleteTxnAckRecordsTest` (1) | v5 scalable segments. |
| `broker/service/scalable/ScalableTopicSegmentDeleteTxnStateTest` (8) | v5 scalable topics. |
| `broker/stats/TransactionBatchWriterMetricsTest` (1) | Broker metrics. |
| `broker/stats/TransactionMetricsTest` (5) | Broker metrics. |
| `broker/transaction/SegmentAbortedTxnProcessorTest` (6) | Broker TB snapshot processor. |
| `broker/transaction/buffer/InMemTransactionBufferReaderTest` (3) | Broker TB internals. |
| `broker/transaction/buffer/TransactionBufferClientTest` (15) | Broker-to-broker `TransactionBufferClient` (out of scope). |
| `broker/transaction/buffer/TransactionBufferCloseTest` (2, ×2 data rows) | Asserts broker snapshot-publisher counts. |
| `broker/transaction/buffer/TransactionBufferHandlerImplTest` (2) | Broker-to-broker handler (out of scope). |
| `broker/transaction/buffer/TransactionBufferTest` (9) | Broker TB internals. |
| `broker/transaction/buffer/TransactionEntryImplTest` (1) | Broker TB internals. |
| `broker/transaction/buffer/TransactionLowWaterMarkTest` (4) | Forces a committed client transaction back to `OPEN` via reflection and inspects broker stores / pending write ops; the client-observable rejection paths (`NotAllowedException`, `TransactionNotFoundException` on stale txns) are covered by sibling TransactionTest/EndToEnd ports. |
| `broker/transaction/buffer/impl/AbstractSnapshotAbortedTxnProcessorTest` (4) | Broker TB internals. |
| `broker/transaction/buffer/impl/DispatchingTransactionBufferProviderTest` (2) | Broker provider routing (v5). |
| `broker/transaction/buffer/impl/MetadataTransactionBufferTest` (24) | v5 metadata transaction buffer. |
| `broker/transaction/buffer/impl/SnapshotSegmentAbortedTxnProcessorCloseTest` (1) | Broker TB internals. |
| `broker/transaction/buffer/impl/SnapshotSegmentAbortedTxnProcessorWorkerTest` (2) | Broker TB internals. |
| `broker/transaction/buffer/impl/TopicTransactionBufferCloseTest` (6) | Broker TB internals. |
| `broker/transaction/buffer/impl/TopicTransactionBufferRecoveryTest` (6) | Broker TB recovery internals. |
| `broker/transaction/buffer/impl/TransactionPersistentTopicTest` (1) | Broker topic internals. |
| `broker/transaction/buffer/utils/TransactionBufferTestImpl`, `TransactionBufferTestProvider` (0) | Test utilities, no tests. |
| `broker/transaction/coordinator/TransactionMetaStoreTestBase` (0) | Base class (used by B3). |
| `broker/transaction/coordinator/v5/TransactionCoordinatorV5Test` (22) | v5 coordinator. |
| `broker/transaction/metadata/TxnIdsTest` (7), `TxnMetadataStoreShardedOxiaTest` (6), `TxnMetadataStoreTest` (13), `TxnPathsTest` (1) | v5 metadata store. |
| `broker/transaction/pendingack/impl/DispatchingTransactionPendingAckStoreProviderTest` (2) | Broker provider routing (v5). |
| `broker/transaction/recover/TransactionRecoverTrackerTest` (2) | Broker TC recovery. |
| `client/api/v5/V5TransactionPublishOrderTest` (1), `V5TransactionRecoveryTest` (4), `V5TransactionScalableTest` (8), `V5TransactionTest` (10), `V5TransactionTimeoutTest` (1) | v5 / PIP-473 client. |

(Method counts per file are from the `@Test` methods at the pinned SHA; class-level `@Test` annotations are not counted.)

---

## 6. Open decisions (recommendation)

1. ~~When the TC client starts~~ — **resolved** (API UPDATE): lazy start on the first `NewTransaction().Create()`; a broker without TC makes that `Create()` throw `TransactionCoordinatorClientException`; client `Build()` never throws.
2. **`NewTransaction()` after client disposal.** DotPulsar's `Create*` methods throw `PulsarClientDisposedException` synchronously after dispose; Java fails the future with `CoordinatorClientStateException` (B1.1, B1.2). *Recommendation:* `NewTransaction()` never throws; `Create()` faults with `TransactionCoordinatorClientStateException` (parity with upstream tests).
3. ~~Operation timeout knob~~ — **resolved** (API UPDATE): `IPulsarClientBuilder.OperationTimeout(TimeSpan)` (default 30 s) and `OperationTimeoutException`.
4. **Key-based batching.** A3.8, A3.15, A3.18 need `KeyBasedBatchMessageContainer` (Java `BatcherBuilder.KEY_BASED`). *Recommendation:* port it with the producer-batching prerequisite (option `ProducerOptions.BatcherType` (06-prerequisites/producer-batching.md §3.1)); otherwise mark those three Excluded.
5. **Batch buffer ownership seam.** A3 ref-count tests are translated to ArrayPool rent/return balance. *Recommendation:* the batching prerequisite spec must rent the batch buffer from an injectable `ArrayPool<byte>` (default `ArrayPool<byte>.Shared`) and transfer/return ownership exactly as Java's `batchPayloadOwned` (`BatchMessageContainerImpl.java` L62-64, L244-286, L459-477); `BatchSendOp.Dispose()` returns what the op owns. If that spec picks a different buffer strategy, these tests convert to its seam with the same "returned exactly once, none leaked" assertions.
6. **Topic/namespace names in integration tests.** Java uses `tnx/ns1/...` created by `TransactionTestBase`. *Recommendation:* use `persistent://public/default/<javaName>-<guid:N>` (unique per test, shared fixture) unless the fixture spec creates `tnx/ns1`; keep the Java name as the prefix for traceability.
7. **Handler failure on registration.** Java closes the shared channel when `changeToReadyState` fails (L259). *Recommendation:* do **not** dispose the pooled DotPulsar `Connection` (other producers/consumers share it); only skip registration and fault `ConnectTask` (A2.1 asserts this).
8. **`AlreadyClosedException` mapping.** *Recommendation:* reuse `PulsarClientClosedException` with a new `(string message)` ctor rather than adding a new type (the only producer of this error is client/handler disposal).
9. ~~Test access to the TC client~~ — **resolved** (API UPDATE): `((PulsarClient) client).TransactionCoordinatorClient!` (`ITransactionCoordinatorClient`). Implementer note: with lazy start the property must be non-null right after `Build()` when `EnableTransaction(true)` (B1.2 reads it before any `Create`); `Start`/`Handlers` live on the concrete type only.
