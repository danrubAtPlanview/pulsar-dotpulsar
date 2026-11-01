# Component: Client integration (`PulsarClient`, `PulsarClientBuilder`)

| | |
|---|---|
| C# | `Abstractions/IPulsarClient.cs`, `PulsarClient.cs`, `Abstractions/IPulsarClientBuilder.cs`, `Internal/PulsarClientBuilder.cs`, `Internal/PulsarClientFactory.cs`, `Extensions/PulsarClientExtensions.cs` |
| Upstream | `java:J/PulsarClientImpl.java` (L345-353 TC start, L1491-1498 close, L2111-2122 `newTransaction`/`getTransactionCoordinatorClient`), `java:J/conf/ClientConfigurationData.java` (L138 `operationTimeoutMs`, L475 `enableTransaction`), `java:pulsar-client-api/.../ClientBuilder.java#enableTransaction` (L686-692) |
| WP | WP1 (`OperationTimeout`), WP3 (everything else) |

All DotPulsar line numbers refer to baseline `e8f2ba2`. Line numbers drift, so locate each edit by its anchor text.

## 1. `IPulsarClientBuilder` + `PulsarClientBuilder`

**Interface.** Append the two members after `CloseInactiveConnectionsInterval`. Later additions in this repo are
appended at the end, not inserted alphabetically; see [01 §7](../01-conventions.md#7-options-and-builders). The XML
docs are in [03 §3.3](../03-public-api.md#33-ipulsarclientbuilder--append).

**Implementation (`Internal/PulsarClientBuilder.cs`).**

| Item | Code |
|---|---|
| Fields | `private bool _enableTransaction;` and `private TimeSpan _operationTimeout;` |
| Constructor defaults | `_enableTransaction = false;` (Java default `false`) and `_operationTimeout = TimeSpan.FromSeconds(30);` (Java `operationTimeoutMs = 30000`) |
| Methods | Fluent setters `EnableTransaction(bool)` and `OperationTimeout(TimeSpan)`, each `return this;` |
| `Build()` validation, next to the existing checks | `if (_operationTimeout <= TimeSpan.Zero) throw new ConfigurationException("OperationTimeout must be greater than zero");` |
| `Build()` construction, after the `ConnectionPool` is created | `var transactionCoordinatorClient = _enableTransaction ? new TransactionCoordinatorClient(connectionPool, _operationTimeout) : null;` then `return new PulsarClient(connectionPool, exceptionHandlerPipeline, _serviceUrl, transactionCoordinatorClient);` |

`Build()` performs **no I/O**. The TC client starts lazily
([ADR 0008](../decisions/0008-lazy-transaction-coordinator-start.md)).

**`PulsarClientFactory.CreatePulsarClient`.** It is public and used by tests. Keep the 3-argument overload, which
forwards `null` for the TC client, and add a 4-argument overload that takes `ITransactionCoordinatorClient?`. Unit tests
use that overload to inject a substitute.

## 2. `IPulsarClient` + `PulsarClient`

1. `IPulsarClient`: add `CreateTransaction(TransactionOptions, CancellationToken)` after `CreateReader`
   ([03 §3.1](../03-public-api.md#31-ipulsarclient--add-member-breaking-for-external-implementers)).
2. `PulsarClient`:
   - Add the field `private readonly ITransactionCoordinatorClient? _transactionCoordinatorClient;`.
   - The internal constructor gains a 4th parameter, `ITransactionCoordinatorClient? transactionCoordinatorClient`.
     Assign it in the constructor body.
   - Add the public property `public ITransactionCoordinatorClient? TransactionCoordinatorClient => _transactionCoordinatorClient;`
     ([03 §3.8](../03-public-api.md#38-pulsarclient-sealed-class--add-property-not-on-the-interface)).
     It is **non-null immediately after `Build()`** when transactions are enabled, even though the client has not
     started yet. Ported tests depend on this (`TransactionClientCloseTest#testCoordinatorRequestsFailAfterClientClose`).
   - `CreateTransaction` follows [transaction-builder §2](transaction-builder.md#2-pulsarclientcreatetransaction-java-transactionbuilderimplbuild-body).
   - `DisposeAsync`: dispose the TC client **after** producers, consumers and readers, and **before** the
     `ConnectionPool`. That is: after the `_disposables` loop, before `await _connectionPool.DisposeAsync()`. Wrap it
     so that a failure there never prevents the pool from being disposed. This follows Java `shutdown`, L1491-1498.
3. `CreateProducer` must tell the producer whether transactions are enabled, so that `CommandProducer.TxnEnabled` can
   be set ([producer-integration §9.4](producer-integration.md)). Pass
   `transactionsEnabled: _transactionCoordinatorClient is not null` to `Producer<TMessage>`.
4. `PulsarClientExtensions.NewTransaction(this IPulsarClient)` returns `new TransactionBuilder(pulsarClient)`
   ([03 §3.2](../03-public-api.md#32-pulsarclientextensions--add)).

## 3. Start, retry and failure semantics

These are specified in [transaction-coordinator-client §4](transaction-coordinator-client.md#4-methods) (`EnsureStarted`)
and [02 §4](../02-architecture.md#4-start-up-decision-lazy-shared-retryable). In summary:

- A failed start is not cached. The next `CreateTransaction` retries.
- A broker without a TC causes `CreateTransaction` to throw `TransactionCoordinatorClientException` with the verbatim
  Java message.
- Tests that commit or abort by `TransactionId` on a fresh client call
  `((PulsarClient) client).TransactionCoordinatorClient!.EnsureStarted(ct)` first.

## 4. Tests

| Test | Type |
|---|---|
| `Build_GivenEnableTransaction_ShouldExposeNotStartedTransactionCoordinatorClient` | Unit |
| `Build_GivenTransactionsNotEnabled_ShouldHaveNullTransactionCoordinatorClient` | Unit |
| `Build_GivenZeroOperationTimeout_ShouldThrowConfigurationException` | Unit |
| `DisposeAsync_GivenTransactionCoordinatorClient_ShouldDisposeItBeforeConnectionPool` | Unit (call-order check with substitutes) |
| `CreateTransaction_GivenBrokerWithoutTransactionCoordinator_ShouldThrowTransactionCoordinatorClientException` | Integration (`IntegrationFixture`, TC disabled); port of `TransactionClientConnectTest#testNotEnableTransactionInBroker` |
| `CreateTransaction_GivenFailedStartThenCoordinatorAvailable_ShouldSucceedOnRetry` | Unit (substitute discovery fails once) |
