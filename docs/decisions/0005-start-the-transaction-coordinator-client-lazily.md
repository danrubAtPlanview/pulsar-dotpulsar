# Start the transaction coordinator client lazily

## Context and Problem Statement

The Java client starts the transaction coordinator client synchronously while the client is built, so building the client fails if no coordinator can be reached.
`IPulsarClientBuilder.Build()` in DotPulsar is synchronous and performs no I/O.

## Decision Drivers

* `Build()` must keep being free of I/O
* A client that never uses transactions must not open connections to the transaction coordinator
* A failure to start must be observed by the caller, and must not stop the next attempt

## Considered Options

* Block in `Build()` (sync-over-async)
* Start eagerly in the background during `Build()`, and surface failures on first use
* Start lazily on the first `CreateTransaction`

## Decision Outcome

Chosen option: "Start lazily on the first `CreateTransaction`", with one shared start task. A failed start is cleared, so the next call tries again.

Blocking in `Build()` can deadlock under a synchronization context. Starting in the background creates failures that nobody observes.
A caller that wants to know early whether transactions work can create and abort a transaction.

## Validation

Unit tests cover the shared start task, a failed start that is tried again on the next call, and a caller that cancels while the start continues. An integration test creates a transaction against a broker without the transaction coordinator, expects `TransactionCoordinatorClientException`, and the integration tests of transactions run against a broker with the coordinator enabled.

## Pros and Cons of the Options

### Block in `Build()`

* Good, because a wrong setup is found at once
* Bad, because it can deadlock, and `Build()` would do I/O

### Start eagerly in the background

* Good, because the first transaction does not wait for the connection
* Bad, because a failure has no caller to report it to, and every client opens the connection, also those that never use transactions

### Start lazily on the first `CreateTransaction`

* Good, because `Build()` stays free of I/O and the failure reaches the caller that wants a transaction
* Bad, because the first transaction pays for the connection
