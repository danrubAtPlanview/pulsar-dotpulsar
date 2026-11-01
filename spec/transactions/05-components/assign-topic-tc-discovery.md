# Component: `AssignTopicTcDiscovery`

| | |
|---|---|
| C# | `src/DotPulsar/Internal/Abstractions/ITcDiscovery.cs`, `src/DotPulsar/Internal/Transactions/AssignTopicTcDiscovery.cs` |
| Upstream | `java:pulsar-client/.../impl/transaction/TcDiscovery.java`, `java:pulsar-client/.../impl/transaction/AssignTopicTcDiscovery.java` (126 lines) |
| WP | WP2 |

## 1. Interface

```csharp
namespace DotPulsar.Internal.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TcDiscovery.java
public interface ITcDiscovery : IAsyncDisposable
{
    ValueTask Start(CancellationToken cancellationToken);
    ITransactionMetaStoreHandler? HandlerForCoordinator(ulong tcId);
    ITransactionMetaStoreHandler? NextHandler();
    IReadOnlyList<ITransactionMetaStoreHandler> Handlers { get; }
}
```

`ITransactionMetaStoreHandler` (in `Internal/Abstractions`) exposes the handler's request methods, `State` and
`DisposeAsync`. It exists so that `TransactionCoordinatorClient` can be unit-tested with substitutes.

## 2. Constants

```csharp
// Use Constants.TransactionCoordinatorAssignTopic = "persistent://pulsar/system/transaction_coordinator_assign" (connection-integration §6)
```

This mirrors `SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN`. The partition topic name is
`$"{TransactionCoordinatorAssignTopic}-partition-{i}"`, matching `getTCAssignTopicName` L86-89 and the DotPulsar
helper `Producer.GetPartitionedTopicName`. Reuse that helper if it is accessible; otherwise use the same format string.

## 3. Behaviour

| Member | Behaviour | Upstream |
|---|---|---|
| ctor `(IConnectionPool connectionPool, TimeSpan operationTimeout, Func<ulong, string, ITransactionMetaStoreHandler>? handlerFactory = null)` | Stores its arguments. `_epoch = 0` (`long`). The factory is a test seam; it defaults to `new TransactionMetaStoreHandler(tcId, topic, connectionPool, operationTimeout)`. | L45-57 |
| `Start(ct)` | See the steps below. | L59-84 |
| `HandlerForCoordinator(tcId)` | Returns `_handlers[tcId]` if `tcId < (ulong) _handlers.Length`; otherwise `null`. Java uses a long-keyed hash map; here the ids are dense `0..N-1`, so an array index is equivalent and allocation-free. | L91-94 |
| `NextHandler()` | `var snapshot = _handlers; if (snapshot is null or { Length: 0 }) return null; var index = (int) ((ulong) Interlocked.Increment(ref _epoch) % (ulong) snapshot.Length); return snapshot[index];`. The first call returns index **1** when N > 1, as in Java (`signSafeMod(epoch.incrementAndGet(), N)`). | L97-105 |
| `Handlers` | Returns `_handlers` or an empty array. | L107-111 |
| `DisposeAsync()` | Dispose every handler, swallowing exceptions, then set `_handlers = null`. | L114-125 |

### `Start(ct)` steps

1. `var partitions = await _connectionPool.GetNumberOfPartitions(TransactionCoordinatorAssignTopic, ct)`.
   - **Wire note:** Java passes `metadataAutoCreationEnabled = true` (L61). Check whether DotPulsar's
     `CommandPartitionedTopicMetadata` sets `metadata_auto_creation_allowed` (proto field 6 upstream). If the field is
     missing from DotPulsar's proto, leave it unset; the broker default is `true`, which gives the same behaviour.
     See [04-protocol](../04-protocol.md).
2. If `partitions == 0`, throw
   `TransactionCoordinatorClientException("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized")`.
   The message is verbatim from L78-80.
3. Create `_handlers = new ITransactionMetaStoreHandler[partitions]`. For `i` in `0..partitions-1`, create the
   handler for `(i, AssignPartition(i))` and call `handler.Start()`. `Start` begins the background connect loop and
   returns at once.
4. Wait for every handler's first connection: `await Task.WhenAll(handlers.Select(h => h.Connected))`, honouring `ct`.
   - Startup succeeds only when **all** handlers connect (Java `FutureUtil.waitForAll`).
   - If any handler's `Connected` task faults, rethrow its exception. A handler fails when its first connect hits a
     non-retriable error or exceeds the lookup deadline (`operationTimeout`).

## 4. Tests (`tests/DotPulsar.Tests/Internal/Transactions/AssignTopicTcDiscoveryTests.cs`, Unit)

| Test | Upstream |
|---|---|
| `Start_GivenZeroPartitions_ShouldThrowTransactionCoordinatorClientException` (exact message) | L78-80; `TransactionClientConnectTest#testNotEnableTransactionInBroker` (unit form) |
| `Start_GivenSixteenPartitions_ShouldCreateHandlerPerPartitionWithAssignTopicNames` | L64-77 |
| `NextHandler_GivenSixteenHandlers_ShouldStartAtIndexOneAndRoundRobin` | L97-105 |
| `NextHandler_GivenOneHandler_ShouldAlwaysReturnIt` | |
| `HandlerForCoordinator_GivenUnknownId_ShouldReturnNull` | L91-94 |
| `Start_GivenHandlerConnectFails_ShouldThrow` | `waitForAll` |
| `DisposeAsync_ShouldDisposeAllHandlers` | L114-125 |
