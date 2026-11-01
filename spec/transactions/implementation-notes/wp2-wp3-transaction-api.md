# WP2/WP3 as built: `Transaction` internal API and client wiring

All members are on `DotPulsar.Internal.Transactions.Transaction` unless stated.

- `Transaction.From(ITransaction)` returns the internal type, or throws `ArgumentException("The transaction must be created by DotPulsar", "transaction")`.
- `bool IsOpen` is the fast check. `void EnsureOpen()` throws `InvalidTransactionStatusException("[(0,5)] with unexpected state : TimedOut, expect Open state!")`.
  The spec calls this `EnsureOpen` / `CheckIfOpen`.
- `ValueTask RegisterProducedTopic(topic, ct)` and `RegisterAckedTopic(topic, subscription, ct)`:
  - call `EnsureOpen()` first, so they throw **synchronously**, not as a faulted `ValueTask`;
  - after the first call per key they return a default completed `ValueTask` with no allocation;
  - a failed registration stays cached, so later sends to that key fail with the same exception;
  - the caller's token only abandons the wait; the TC request itself uses `CancellationToken.None`.
- `void RegisterSendOp(Task)` and `RegisterAckOp(Task)`: pass the task that completes on receipt / ack. A faulted or cancelled task marks the transaction as having
  failed ops; `Commit` then aborts and throws `TransactionHasOperationFailedException`. The exception is observed internally.
- `Commit` and `Abort` wait for all registered ops, so each op must be registered **synchronously** before the send returns.
- Cancelling `Commit`/`Abort` only stops the wait for the pending ops. Once the TC request is sent the wait uses no token, so the state stays consistent.
- `PulsarClient` internal ctor: `(pool, handler, serviceUrl, operationTimeout, tcClient)`. Use `((PulsarClient) client).TransactionCoordinatorClient` (non-null when
  transactions are enabled, state `None` until the first `Create`). `PulsarClientFactory.CreatePulsarClient` has 4-arg (`ITransactionCoordinatorClient?`) and 5-arg overloads for substitutes.
- `CreateProducer` does not pass `transactionsEnabled` to the producer channel factory before WP4 (client-integration §2.3).

## Behaviour that is not in the specs

- The handler sends a request at most once per connection at a time, and has one retry timer per op (avoids a duplicate `NewTxn` leaking a transaction on the broker). Java does not.
- Each op has its own `CancellationTokenSource`; a timed-out or disposed request is removed from the shared connection's awaiter table.
- `ConnectionClosed()` only cancels the session if the connection's state is already final.
- A Failed handler fails pending ops at once with `MetaStoreHandlerNotReadyException`. NotAllowed closes the handler but not the shared connection.
- A request to a disposed handler throws `PulsarClientClosedException`. `CreateTransaction` has no `ThrowIfDisposed`; `EnsureStarted` already faults with
  `TransactionCoordinatorClientStateException` (tests-client open decision 2).
- A typed `TcClientConnectResponse` error uses the handler's table (NotAllowedError is retried, as in Java); a generic `CommandError` goes through `ToException`.
- `Transaction` clamps its timer to 49 days. Commit and abort check and set the state atomically under one lock.
- The subscription-op description is `"Add subscription [topic sub] to TXN id"` (Java's text, with brackets).
- The broker removes ended transactions from its metadata store, so Committed/Aborted can not be read via the admin API. Integration tests check `OPEN`
  plus the produced partition, then HTTP 404 afterwards, then `TransactionNotFoundException` on a second end call.

## Not done

- `TransactionClientConnectTest` B2.9 (needs Toxiproxy; `TransactionFixture` has none).
- `AutoCloseUselessClientConTXTests`.
- Hazard 3 (a TC-only connection staying Active) is covered only by `FakeCoordinator`, not against a real idle connection.
