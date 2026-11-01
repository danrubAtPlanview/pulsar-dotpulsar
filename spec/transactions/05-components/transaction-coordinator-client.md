# Component: `TransactionCoordinatorClient`

| | |
|---|---|
| C# | `src/DotPulsar/Internal/Abstractions/ITransactionCoordinatorClient.cs`, `src/DotPulsar/Internal/Transactions/TransactionCoordinatorClient.cs`, `src/DotPulsar/Internal/Transactions/TransactionCoordinatorClientState.cs` |
| Upstream | `java:pulsar-client-api/.../transaction/TransactionCoordinatorClient.java` (API, `State` L41-47, `DEFAULT_TXN_TTL_MS` L36); `java:pulsar-client/.../impl/transaction/TransactionCoordinatorClientImpl.java` (307 lines) |
| WP | WP2 |

## 1. Interface

```csharp
namespace DotPulsar.Internal.Abstractions;

using DotPulsar.Internal.Transactions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TransactionCoordinatorClient.java
public interface ITransactionCoordinatorClient : IAsyncDisposable
{
    TransactionCoordinatorClientState State { get; }
    ValueTask Start(CancellationToken cancellationToken);
    ValueTask EnsureStarted(CancellationToken cancellationToken);
    ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken);
    ValueTask AddPublishPartitionToTxn(TransactionId transactionId, string partition, CancellationToken cancellationToken);
    ValueTask AddSubscriptionToTxn(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken);
    ValueTask Commit(TransactionId transactionId, CancellationToken cancellationToken);
    ValueTask Abort(TransactionId transactionId, CancellationToken cancellationToken);
}
```

**Differences from Java:**

- Only the async forms are ported. Java's synchronous wrappers (`start()`, `commit(TxnID)` and so on) wrap
  `unwrap(e)` around `.get()`. In C# the awaited exception is already unwrapped, so they add nothing.
- `newTransactionAsync()` without a timeout uses `DEFAULT_TXN_TTL_MS = 60000`. Callers pass `TransactionOptions.DefaultTimeout` instead.
- `AddPublishPartitionToTxn` takes a single partition. Every Java caller passes a one-element list
  (`TransactionImpl` L114 `Lists.newArrayList(topic)`). The handler still builds the repeated `Partitions` field.

## 2. State enum

```csharp
namespace DotPulsar.Internal.Transactions;

public enum TransactionCoordinatorClientState : byte { None, Starting, Ready, Closing, Closed }
```

## 3. Class

```csharp
public sealed class TransactionCoordinatorClient : ITransactionCoordinatorClient
{
    public TransactionCoordinatorClient(IConnectionPool connectionPool, TimeSpan operationTimeout, Func<ITcDiscovery>? discoveryFactory = null)
```

The `discoveryFactory` parameter is a test seam; it defaults to `() => new AssignTopicTcDiscovery(connectionPool, operationTimeout)`.

**Fields:**

| Field | Type | Purpose |
|---|---|---|
| `_state` | `int` (Interlocked) | Client state |
| `_discovery` | `ITcDiscovery?` (volatile) | Set by start |
| `_startLock` | `object` | Guards `_startTask` |
| `_startTask` | `Task?` | Shared start task for `EnsureStarted` |

## 4. Methods

| Method | Behaviour | Upstream |
|---|---|---|
| `Start(ct)` | 1. CAS `None→Starting`. If that fails, throw `TransactionCoordinatorClientStateException($"Can not start while current state is {State}")`. 2. `_discovery = _discoveryFactory()`. 3. `await _discovery.Start(ct)`. 4. CAS `Starting→Ready`. **C# addition:** if step 3 throws, dispose the discovery, set `_discovery = null`, set `None`, and rethrow, so that start can be retried ([ADR 0008](../decisions/0008-lazy-transaction-coordinator-start.md)). | `startAsync` L73-88 |
| `EnsureStarted(ct)` | If `State == Ready`, return. If `Closing`/`Closed`, throw the state exception. Otherwise, under `_startLock`, reuse `_startTask` or set `_startTask = Start(CancellationToken.None).AsTask()`. Await it with `ct` applied to the wait only. If the task faulted, clear `_startTask` under the lock before rethrowing. | C# only |
| `NewTransaction(timeout, ct)` | If not `Ready`, throw `TransactionCoordinatorClientStateException($"Can not send a request while current state is {State}")`. `handler = _discovery.NextHandler()`; if `null`, throw `TransactionCoordinatorClientException("No transaction coordinator is currently available")`. Return `handler.NewTransaction(timeout, ct)`. | L166-178, `notReadyFailure` L287-290 |
| `AddPublishPartitionToTxn(id, partition, ct)` | Ready check; `handler = _discovery.HandlerForCoordinator(id.MostSigBits)`; if `null`, throw `MetaStoreHandlerNotExistsException(id.MostSigBits)`; then `handler.AddPublishPartitionToTxn(id, partition, ct)`. | L190-202 |
| `AddSubscriptionToTxn(id, topic, sub, ct)` | Same pattern; the handler builds one `Subscription { Topic = topic, Subscription = sub }`. | L214-231 |
| `Commit(id, ct)` / `Abort(id, ct)` | Same pattern; `handler.EndTxn(id, TxnAction.Commit / TxnAction.Abort, ct)`. | L240-275 |
| `DisposeAsync()` | Atomically: if the previous state is `Closing` or `Closed`, return. Otherwise set `Closing`, `await _discovery?.DisposeAsync()` (swallow and ignore exceptions, as Java logs and continues), then set `Closed`. | `closeAsync` L122-143 |

**Ready checks return failures; they never block.** The state exception is thrown from the `async` method, so callers
observe it as a faulted task, matching Java's failed futures.

## 5. Tests (`tests/DotPulsar.Tests/Internal/Transactions/TransactionCoordinatorClientTests.cs`, Unit, mocked `ITcDiscovery`)

| Test | Upstream |
|---|---|
| `Start_GivenAlreadyStarted_ShouldThrowTransactionCoordinatorClientStateException` | `TransactionCoordinatorClientTest#testClientStart` |
| `Start_GivenDiscoveryFails_ShouldResetToNoneAndAllowRetry` | C# addition |
| `EnsureStarted_GivenConcurrentCallers_ShouldStartOnce` | C# addition |
| `NewTransaction_GivenNotStarted_ShouldThrowStateException` | `TransactionClientCloseTest#testCoordinatorRequestsFailBeforeStart` |
| `NewTransaction_GivenNoHandler_ShouldThrowTransactionCoordinatorClientException` | L173-176 |
| `Commit_GivenUnknownCoordinatorId_ShouldThrowMetaStoreHandlerNotExistsException` | L246-250 |
| `AllRequests_GivenDisposed_ShouldThrowStateExceptionAndStateClosed` | `TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose` |
| `DisposeAsync_GivenCalledTwice_ShouldBeNoOp` | L127-129 |
| `Commit_ShouldRouteToHandlerForMostSigBits` / `Abort_...` / `AddSubscriptionToTxn_...` / `AddPublishPartitionToTxn_...` | routing |
