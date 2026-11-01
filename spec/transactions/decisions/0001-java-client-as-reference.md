# Use the Java client as the transaction reference implementation

## Context and Problem Statement

The goal was to port the transaction implementation of apache/pulsar-client-cpp to DotPulsar. As of 2026-10-30,
pulsar-client-cpp has no transaction implementation:
- The classic API has none.
- `pulsar::st::Transaction` returns `ResultOperationNotSupported`.
- Issue #366 is open.
- PR #523 was closed unmerged.

We need an upstream that is complete, tested and maintained.

## Decision Drivers

* The reference must be a complete and correct implementation of PIP-31.
* A large test suite must exist, so we can reach test parity.
* Upstream changes must be easy to follow.

## Considered Options

* Java client (apache/pulsar)
* Go client (apache/pulsar-client-go)
* Wait for the C++ client

## Decision Outcome

Chosen option: "Java client (apache/pulsar)". It is the reference implementation, it has by far the largest test
suite (client unit tests and broker end-to-end tests), and it lives in the same repository as the broker, so
protocol changes land there first. The user confirmed this choice. The C++ client remains a cross-reference for the
prerequisite features it implements.

## Pros and Cons of the Options

### Java client

* Good, because it is complete and battle-tested
* Good, because broker and client tests are co-located
* Bad, because its threading model (Netty event loops, CompletableFuture) differs from .NET and needs adaptation

### Go client

* Good, because it is smaller
* Bad, because it is itself a port with a smaller test suite

### Wait for C++

* Bad, because there is no timeline
