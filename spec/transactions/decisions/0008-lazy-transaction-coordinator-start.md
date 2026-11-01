# Start the transaction coordinator client lazily

## Context and Problem Statement

Java starts the TC client synchronously while building the client, so `build()` fails if no coordinator is
reachable. DotPulsar's `IPulsarClientBuilder.Build()` is synchronous and performs no I/O.

## Considered Options

* Block in `Build()` (sync-over-async)
* Start eagerly in the background during `Build()`, and surface failures on first use
* Start lazily on the first `CreateTransaction`

## Decision Outcome

Chosen option: "Start lazily on the first `CreateTransaction`", with a shared start task. A failed start is cleared,
so the next call retries.

This keeps `Build()` free of I/O, and no connections to the coordinator are opened by clients that never use
transactions. Blocking was rejected because it can deadlock under a synchronization context. Eager background start
was rejected because it creates unobserved failures.

Consequence for parity: upstream tests that assert `build()` throws instead assert that the first
`NewTransaction().Create()` throws `TransactionCoordinatorClientException`.
