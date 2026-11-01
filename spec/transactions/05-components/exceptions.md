# Component: Exceptions and error mapping

| | |
|---|---|
| C# | `src/DotPulsar/Exceptions/*.cs` (new files), `src/DotPulsar/Internal/Extensions/CommandExtensions.cs`, `src/DotPulsar/Internal/DefaultExceptionHandler.cs` |
| Upstream | `java:pulsar-client-api/.../transaction/TransactionCoordinatorClientException.java` (L40-145), `java:pulsar-client-api/.../PulsarClientException.java` (L932-960, L1036-1141, L1157-1181), `java:pulsar-client/.../ClientCnx.java#getPulsarClientException` (L2067-2109), `java:pulsar-client/.../TransactionMetaStoreHandler.java#getExceptionByServerError` (L708-719) |
| WP | WP1 (mapping), WP2/WP3 (new types) |

## 1. Flat hierarchy

Java has a class hierarchy (`TransactionCoordinatorClientException` → `CoordinatorNotFoundException`, …). DotPulsar
exceptions are **flat**: every one is `public sealed class X : DotPulsarException`. Keep it flat. Callers that want
"any TC error" can catch `DotPulsarException`.

Every new class uses the template in [01-conventions §6](../01-conventions.md#6-exceptions): two constructors,
`(string message)` and `(string message, Exception innerException)`. Where Java has a parameterless or `tcId`
constructor, add that overload too, as listed below.

## 2. New exception types

| File | Summary XML | Extra ctor(s) | Java counterpart |
|---|---|---|---|
| `TransactionNotFoundException.cs` | `Transaction not found` | — | `TransactionCoordinatorClientException.TransactionNotFoundException` |
| `TransactionHasOperationFailedException.cs` | `Commit not allowed because operations of the transaction have failed` | `()` → message `"Now allowed to commit the transaction due to failed operations of producing or acknowledgment"` (verbatim, upstream typo included) | `PulsarClientException.TransactionHasOperationFailedException` L955-960 |
| `TransactionCoordinatorClientException.cs` | `Transaction coordinator client error` | — | `TransactionCoordinatorClientException` (base, generic use) |
| `TransactionCoordinatorClientStateException.cs` | `Unexpected state for transaction coordinator client` | `()` → `"Unexpected state for transaction metadata client."` | `CoordinatorClientStateException` L46-55 |
| `MetaStoreHandlerNotExistsException.cs` | `Transaction meta store handler not found` | `(ulong tcId)` → `$"Transaction meta store handler for transaction meta store {tcId} not exists."`. Java has a format bug here: its message contains a literal `{}`. **Deliberately fixed** in C#; tests assert only the type. | `MetaStoreHandlerNotExistsException` L99-112 |
| `MetaStoreHandlerNotReadyException.cs` | `Transaction meta store handler not ready` | `(ulong tcId)` → `$"Transaction meta store handler for transaction meta store {tcId} not ready now."` (same fix) | `MetaStoreHandlerNotReadyException` L114-124 |
| `OperationTimeoutException.cs` | `Operation timed out` | — | `PulsarClientException.TimeoutException`. Do **not** name it `TimeoutException`: that clashes with `System.TimeoutException` under implicit usings. |

Existing types that are reused:

| Existing | Java counterpart |
|---|---|
| `InvalidTransactionStatusException` | `InvalidTxnStatusException` |
| `TransactionConflictException` | `PulsarClientException.TransactionConflictException` |
| `TransactionCoordinatorNotFoundException` | `CoordinatorNotFoundException` |
| `NotAllowedException` | `PulsarClientException.NotAllowedException` |
| `ConfigurationException` | `InvalidConfigurationException` |
| `PulsarClientClosedException` (**add** a `(string message)` constructor; the parameterless one keeps `"Client has closed"`) | `PulsarClientException.AlreadyClosedException` (TC handler disposed: pending ops failed, or a request made after disposal) |

## 3. Message formats (port exactly; tests may compare)

| Producer of the message | Format | Upstream |
|---|---|---|
| `Transaction.CheckIfOpen` (send/ack/register on a non-open transaction) | `$"[{Id}] with unexpected state : {CurrentState}, expect Open state!"`. `Id` renders as `(most,least)`, so the full text is e.g. `"[(0,5)] with unexpected state : TimedOut, expect Open state!"`. | `InvalidTxnStatusException(String, String, String)` L79-82, called from `TransactionImpl#checkIfOpen` L258-267 with `txnId.toString()` |
| `Transaction.CheckState` (commit/abort wrong state) | `$"[{most}:{least}] with unexpected state: {actual}, expect: [{string.Join(", ", expected)}]"`, e.g. `"[0:5] with unexpected state: Committed, expect: [Open, Committing]"` | `TransactionImpl#checkState` L269-279 (`Arrays.toString` gives `[A, B]`) |
| TC client not ready | `$"Can not send a request while current state is {State}"` | L287-290 |
| TC client start twice | `$"Can not start while current state is {State}"` | L84-87 |
| No coordinator | `"No transaction coordinator is currently available"` | L173-176 |
| TC disabled on broker | `"The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized"` | `AssignTopicTcDiscovery` L78-80 |
| Handler closing | `$"Transaction meta store handler for tcId {tcId} is closing or closed."` | L789-793 |
| Handler failed | `$"Transaction meta store handler for tcId {tcId} not connected."` | L797-801 |
| Handler closed with pending ops | `"Could not get response from transaction meta store when the transaction meta store has already close."` | L283-285 |
| Op timeout | `$"{description} failed due to timeout. connection: {connection}. pending-queue: {count}"` | L831-833 |
| Transactions disabled on client | `"Transactions are not enabled"` | `TransactionBuilderImpl` L55-58 |
| Cumulative ack on Shared/KeyShared | `"Cannot use cumulative acks on a non-exclusive/non-failover subscription"` | `ConsumerBase` L715-735 |

State names are rendered with the C# enum names (`Open`, `TimedOut`, …), not Java's (`OPEN`, `TIME_OUT`).

## 4. `ServerError` → exception mapping

There are **two** mapping tables, because Java has two.

### 4.1 Client-side commands (`CommandExtensions.Throw(ServerError, string)`)

Covers send errors, ack responses, generic `CommandError`, and so on. Java `ClientCnx#getPulsarClientException`.

| `ServerError` | Today | Change |
|---|---|---|
| `TransactionConflict` | `TransactionConflictException` | keep |
| `NotAllowedError` | `NotAllowedException` | keep |
| `InvalidTxnStatus` | `InvalidTransactionStatusException` | keep. Java's `ClientCnx` maps it to a generic `PulsarClientException`, but the specific type is strictly more useful, and every upstream test asserting it goes through the TC path. |
| `TransactionCoordinatorNotFound` | `TransactionCoordinatorNotFoundException` | keep |
| `TransactionNotFound` | `UnknownException` (unmapped) | **add** `ServerError.TransactionNotFound => new TransactionNotFoundException(message)` |

### 4.2 TC responses (`TransactionMetaStoreHandler.MapServerError`, Java `getExceptionByServerError`)

| `ServerError` | Exception |
|---|---|
| `TransactionCoordinatorNotFound` | `TransactionCoordinatorNotFoundException` (normally retried rather than surfaced; see the handler spec) |
| `InvalidTxnStatus` | `InvalidTransactionStatusException` |
| `TransactionNotFound` | `TransactionNotFoundException` |
| anything else (incl. `NotAllowedError` from auth checks) | `TransactionCoordinatorClientException(message)` |

`AuthenticatedTransactionProducerConsumerTest` expects a `TransactionCoordinatorClientException` when a non-owner
ends a transaction. The broker answers `NotAllowedError` on the TC path, and that falls into the "anything else" row.
**As built (WP10, `AuthenticatedTransactionProducerConsumerTests`):** against apachepulsar/pulsar:4.0.9 the broker answers a request from a role that does not own the transaction with `TransactionNotFound`, so the client throws `TransactionNotFoundException`, not the "anything else" exception.
Keep it.

## 5. `DefaultExceptionHandler` (producer/consumer executor policy)

Every new exception derives from `DotPulsarException`, so it already maps to `FaultAction.Rethrow` through the catch-all
`DotPulsarException _ => FaultAction.Rethrow` arm. That is what Java does too: transactional operations are not
retried by the producer or consumer. Retries happen only inside the TC handler.

**One addition:** `OperationTimeoutException _ => FaultAction.Rethrow`, explicit for readability. Place it before the
`DotPulsarException` arm.

**Hazard 5** (a `SendError` faults the whole producer) is resolved in
[producer-integration](producer-integration.md), not here.

## 6. `CommandExtensions` additions (WP1)

```csharp
public static void Throw(this CommandAckResponse command) => Throw(command.Error, command.Message);
public static void Throw(this CommandNewTxnResponse command) => Throw(command.Error, command.Message);
public static void Throw(this CommandAddPartitionToTxnResponse command) => Throw(command.Error, command.Message);
public static void Throw(this CommandAddSubscriptionToTxnResponse command) => Throw(command.Error, command.Message);
public static void Throw(this CommandEndTxnResponse command) => Throw(command.Error, command.Message);
public static void Throw(this CommandTcClientConnectResponse command) => Throw(command.Error, command.Message);
```

The TC handler does **not** use these `Throw` overloads, because it needs the §4.2 mapping. They exist for
consistency and for `CommandAckResponse`.

## 7. Tests (`tests/DotPulsar.Tests/Internal/Extensions/CommandExtensionsTests.cs`, Unit; create it if missing)

`Throw_GivenTransactionNotFound_ShouldThrowTransactionNotFoundException`, a theory over every txn-related
`ServerError` for both tables, and the message-format tests listed in §3.
