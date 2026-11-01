# DotPulsar Transactions — Implementation Specification

> **Read this first (2026-11-01, [decision 0009](decisions/0009-no-breaking-public-api.md)).** Wherever a file of this specification says that a member is added to a public interface (`IConsumer`, `IProducer`, `IPulsarClient`, `IMessage`, `IConsumerBuilder`, `IProducerBuilder`, `IReaderBuilder`, `IPulsarClientBuilder`, `IMessageBuilder`), the code has an **extension method** with the same name and parameters instead, and the interface is unchanged: a public API break is a breaking change and the requester ruled it out. The mapping of all 40 members, the rule for new work and the recipe for a branch that still has the members are in [implementation-notes/wp12-non-breaking-public-api.md](implementation-notes/wp12-non-breaking-public-api.md).

This folder is the complete, self-contained specification for adding **Apache Pulsar transaction support** to DotPulsar,
together with every client feature DotPulsar is missing that the upstream transaction test-suite depends on.

It is written for an implementing agent/developer who will **not** have access to the research session that produced it.
Everything needed — upstream references, design, exact edit points, API signatures, test-by-test translations,
conventions, acceptance criteria — is in here. If something is ambiguous, the "Open decisions" section of
[`12-risks-open-questions.md`](12-risks-open-questions.md) lists the recommendation to follow.

> This is a **specification only**. No production or test code was changed when it was written.

> **Status at the end of WP10:** the specification is implemented on the branch `feature/transactions`. The final verification (test runs, Release, stress, sample, benchmarks) is in `implementation-notes/wp10-parity-completion.md`, the places where the code differs from this text are in `implementation-notes/deviations-summary.md`, and the work that was deferred on purpose is in `followups/README.md` (0001-0018). All the work of the specification is done. The requester decided on 2026-11-01 that the measured batching throughput (1.55x, against 3x in `07-performance.md`) is accepted, and that followup 0002 (it needs a broker image that has apache/pulsar#27128) and the run of the EC encryption tests on macOS are external items of the maintainers. The deviations that remain are in `deviations-summary.md`, and every followup is closed, see `followups/README.md`. Everything that is not done, done differently or not verified is collected in `followups/0019-known-gaps-at-the-end-of-wp10.md`. **WP11 (2026-11-01, on request):** deduplication of producers without a name after a reconnect, `ackTimeoutRedeliveryBackoff` and seek with `ack_set` were implemented after all, see `implementation-notes/wp11-deferred-features.md`. A draft of the pull request description, with the results and the points for the reviewer, is in `PULL-REQUEST.md` (nothing was pushed or opened).

## 1. Background: why the reference is the Java client, not the C++ client

The original request was to port the transaction implementation of
[apache/pulsar-client-cpp](https://github.com/apache/pulsar-client-cpp). Investigation (2026-10-30) showed that
**pulsar-client-cpp has no transaction implementation**:

| Evidence | Detail |
|---|---|
| Classic `pulsar::` API | No transaction type, no `newTransaction`, no TC client. Only the generated proto enums/fields exist (`lib/ProtoApiEnums.h`, `proto/PulsarApi.proto`). |
| `pulsar::st` (scalable-topics SDK, 2026) | `include/pulsar/st/Transaction.h` declares the API, but `lib/st/Transaction.cc` returns `ResultOperationNotSupported` — *"transactions are not implemented yet in the scalable-topics client"*. |
| [Issue #366](https://github.com/apache/pulsar-client-cpp/issues/366) | *"There is no Transaction Support in C++ client API"* — open. |
| [PR #523](https://github.com/apache/pulsar-client-cpp/pull/523) | Header-only attempt, closed unmerged. |

The user therefore selected the **Java client** in [apache/pulsar](https://github.com/apache/pulsar) — the reference
implementation of PIP-31 — as the upstream to port and to track. The C++ client is still used as a
**cross-reference** for the *prerequisite* features (batching, chunking, DLQ, ack-timeout, encryption) that it does
implement, because its design is closer to DotPulsar's performance goals. See [`00-upstream-reference.md`](00-upstream-reference.md).

## 2. Pinned upstream revisions

| Upstream | Role | Pinned commit |
|---|---|---|
| `apache/pulsar` (Java client + broker tests) | **Primary** — semantics, behaviour, tests | `f3846f4811b6a7d98de40a3be2caff1fe288e026` (master, 2026-10-30) |
| `apache/pulsar-client-cpp` | Cross-reference for prerequisite features only | `f71d7e99019d58b1d11858f05ff0dc83b718627b` (main, 2026-10-30) |
| DotPulsar (this repo) | Baseline | `e8f2ba22c7136a35d279451e7278589fb0084d2e` |

All line numbers in this spec refer to these commits. Short form used throughout: `java:<path>#Symbol (Lx-Ly)`,
`cpp:<path>#Symbol (Lx-Ly)`, `dp:<path>#Symbol (Lx-Ly)`.

## 3. Scope

**In scope**

- Transactions, Java v4 path (PIP-31): TC discovery via the `persistent://pulsar/system/transaction_coordinator_assign`
  partitioned topic, TC client, per-TC meta-store handler, transaction object, builder, transactional produce,
  transactional acknowledge (individual, list, cumulative, batch-index), commit/abort, client-side timeout.
- Protocol upgrade from v14 to v19 and the request/response plumbing for TC commands and `CommandAckResponse`.
- Prerequisite client features required by the portable upstream tests (see [`06-prerequisites/`](06-prerequisites/)):
  ack receipt, batch-index acknowledgement, producer batching, send timeout, ack timeout, dead-letter policy,
  chunking (producer + consumer), end-to-end encryption.
- Test parity with every client-observable upstream transaction test ([`08-test-parity.md`](08-test-parity.md)),
  plus additional coverage.
- Benchmarks proving the zero-allocation hot-path goal ([`07-performance.md`](07-performance.md)).
- Traceability from every ported type/method to upstream ([`11-upstream-sync.md`](11-upstream-sync.md), [`upstream-map.yaml`](upstream-map.yaml)).

**Out of scope** (recorded as future work)

- v5 / PIP-473 "scalable" transactions (`WatchTcAssignmentsDiscovery`, `scalable` proto fields, `V5*` tests).
- Broker-to-broker components (`TransactionBufferClient`, `TransactionBufferHandler`, `EndTxnOnPartition`, `EndTxnOnSubscription`).
- Admin API for transactions (DotPulsar has no admin client; tests use the fixture's raw HTTP admin helper).
- Pulsar Functions transactions (PIP-439).

## 4. Reading order

| # | File | What it gives you |
|---|---|---|
| 0 | [`00-upstream-reference.md`](00-upstream-reference.md) | Upstream file inventory, what maps to what, C++ cross-refs |
| 1 | [`01-conventions.md`](01-conventions.md) | **Mandatory** code/test/commit style so the code looks native |
| 2 | [`02-architecture.md`](02-architecture.md) | Components, ownership, state machines, threading, data flows |
| 3 | [`03-public-api.md`](03-public-api.md) | Exact public C# surface with XML docs |
| 4 | [`04-protocol.md`](04-protocol.md) | Wire commands, error mapping, protocol v19 bump |
| 5 | [`05-components/`](05-components/) | One spec per internal component, method-by-method. Integration specs carry exact edit points; also see `core-open-decisions.md` and `test-and-instrumentation-impact.md` |
| 6 | [`06-prerequisites/`](06-prerequisites/) | Specs for the missing client features. Start with its `README.md`, whose decisions override the individual files |
| 7 | [`07-performance.md`](07-performance.md) | Zero-allocation design rules and benchmarks |
| 8 | [`08-test-parity.md`](08-test-parity.md) + [`08-tests/`](08-tests/) | Every upstream test → C# test, step by step |
| 9 | [`09-test-infrastructure.md`](09-test-infrastructure.md) | Fixtures, broker config, helpers |
| 10 | [`10-implementation-plan.md`](10-implementation-plan.md) | Ordered work packages (PR-sized) with acceptance criteria |
| 11 | [`11-upstream-sync.md`](11-upstream-sync.md) + [`upstream-map.yaml`](upstream-map.yaml) | Traceability + how to follow upstream changes |
| 12 | [`12-risks-open-questions.md`](12-risks-open-questions.md) | Risks, open decisions with recommendations |
| — | [`decisions/`](decisions/) | ADRs for the key choices |
| — | [`implementation-notes/`](implementation-notes/) | As-built API notes per work package: what the next WP must call, and where the code differs from the spec |
| — | [`implementation-notes/deviations-summary.md`](implementation-notes/deviations-summary.md) | The deviations from the spec in one place; `upstream-map.yaml` is the final test-parity record |
| — | [`followups/`](followups/) | Issues found while implementing; need review after parity (WP10) |

## 5. Glossary

| Term | Meaning |
|---|---|
| **TC** | Transaction Coordinator. Broker-side component; one per partition of `persistent://pulsar/system/transaction_coordinator_assign`. TC id = partition index. |
| **TB** | Transaction Buffer. Broker-side, per topic; holds transactional messages until commit/abort. |
| **Pending ack** | Broker-side, per subscription; holds transactional acks until commit/abort. |
| **TxnID / `TransactionId`** | `(mostSigBits, leastSigBits)`; `mostSigBits` is the TC id that owns the transaction. |
| **Meta-store handler** | Client-side object (`TransactionMetaStoreHandler`) that owns the connection to one TC and all requests to it. |
| **TC client** | Client-side `TransactionCoordinatorClient`: discovery + routing to the handlers. |
| **Op** | A transactional send or ack in flight; commit waits for all ops. |
| **WP** | Work package — a PR-sized unit of implementation in [`10-implementation-plan.md`](10-implementation-plan.md). |

## 6. Definition of done (whole feature)

1. All work packages in [`10-implementation-plan.md`](10-implementation-plan.md) are merged in order, each satisfying its acceptance criteria.
2. Every row in [`08-test-parity.md`](08-test-parity.md) with status **Port**, **Port-partial** or **Port-unit** exists as a C# test with the
   specified name, passes locally and in CI, and keeps the upstream intent (same counts, values, timeouts, variants).
3. `dotnet build` is warning-free for the new code on all target frameworks (`netstandard2.0; netstandard2.1; net8.0; net9.0; net10.0`).
4. All existing tests still pass (no regression from the protocol bump, pool resets, or ack-path changes).
5. Benchmarks in `benchmarks/Transactions` meet the thresholds in [`07-performance.md`](07-performance.md).
6. Every new/changed type and ported method carries the `// Upstream:` reference and is listed in [`upstream-map.yaml`](upstream-map.yaml).
7. `CHANGELOG.md`, `README.md` feature list and `docs/api` are updated as described in WP-DOCS.
