# Port the client features that the upstream transaction tests depend on

## Context and Problem Statement

Many upstream transaction tests use client features DotPulsar does not have:
- producer batching
- batch-index acks
- ack receipt
- send timeout
- ack timeout
- dead-letter policy
- chunking
- end-to-end encryption

## Considered Options

* Port the missing features too
* Use test-only helpers, and skip tests that need missing features
* Exclude those tests

## Decision Outcome

Chosen option: "Port the missing features too" (user decision). Each feature is its own work package with its own
upstream mapping and parity tests, ordered by how many transaction tests it unblocks. Each feature is **off by
default** (the batching default is an open decision; see [12](../12-risks-open-questions.md)), so existing behaviour
is unchanged.
