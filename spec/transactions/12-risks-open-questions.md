# 12 — Risks, deviations and open decisions

## 1. Open decisions (follow the recommendation unless the maintainers decide otherwise)

| # | Decision | Recommendation | Rationale |
|---|---|---|---|
| D1 | Producer batching default (Java: **on**, 1000 msgs / 1 ms / 128 KB) | **Off** in DotPulsar (`ProducerOptions.DefaultEnableBatching = false`) | Avoids changing latency and ordering semantics for existing users. Tests set it explicitly. |
| D2 | Send timeout default (Java: 30 s) | **Infinite (`TimeSpan.Zero` = disabled)** | DotPulsar retries sends until cancelled today, so keep that. |
| D3 | Batch-index ack for **non-transactional** acks (Java consumer option `batchIndexAckEnabled`, default **true** at the pinned SHA, `ConsumerConfigurationData` L417) | Add `ConsumerOptions.BatchIndexAckEnabled`, default **false**. Transactional acks **always** send `ack_set` (Java always does for txn acks). | DotPulsar keeps its own default (false), so the existing `BatchHandler` semantics, including the e8f2ba2 fix, are kept for plain acks. |
| D4 | TC client start | Lazy and retryable ([ADR 0008](decisions/0008-lazy-transaction-coordinator-start.md)) | `Build()` is synchronous |
| D5 | Encryption on netstandard2.0 (no `AesGcm`) | See [06/encryption](06-prerequisites/encryption.md) open decision | |
| D6 | Expose `ITransactionCoordinatorClient` publicly | Only through `PulsarClient.TransactionCoordinatorClient` on the concrete class, typed in the `Internal.Abstractions` namespace | Matches Java's `@Evolving` status, and the parity tests need it |
| D7 | `// Upstream:` comments | Keep them. If the maintainers object, drop the comments and keep `upstream-map.yaml` | [ADR 0006](decisions/0006-upstream-traceability-comments.md) |
| D8 | Redelivered batch tracker (batch-index-ack.md open decision 4: AND-merge) | **Replace** the tracker with one built from the broker's `ack_set`; never AND-merge | AND-merge loses messages after an aborted transactional cumulative ack. See [followups/0004](followups/0004-batch-tracker-redelivery-and-test-infra.md). |

## 2. Deliberate deviations from Java (each must also be listed in `upstream-map.yaml`)

| # | Java | C# | Why |
|---|---|---|---|
| X1 | Max-pending-ops `Semaphore.acquire()` blocks the thread | `SemaphoreSlim.WaitAsync` | Never block threads in async .NET |
| X2 | `ClientBuilder.build()` fails if TC unavailable | First `CreateTransaction` fails; start retryable | D4 |
| X3 | TC responses routed by `txnid_most_bits` to the handler; unknown TC id closes the connection | Correlated by per-connection `request_id`; no routing needed | DotPulsar's connection model |
| X4 | Pending TC ops resent with the same request id | Resent with a new request id on the new connection | `RequestResponseHandler` owns request ids |
| X5 | `MetaStoreHandlerNotExistsException(tcId)` message contains a literal `{}` (format bug) | Real id interpolated | Bug fix; tests assert type only |
| X6 | Exception hierarchy (`TransactionCoordinatorClientException` subclasses) | Flat sealed exceptions | Repo convention |
| X7 | `AlreadyClosedException` | Existing `PulsarClientClosedException`, with a new `(string message)` constructor | Closest existing type; keeps it distinct from `TransactionCoordinatorClientStateException`, as Java does |
| X8 | `newMessage(Schema, Transaction)` | N/A, producers are typed | API model |
| X9 | `TransactionBuilder` accepts any timeout | Timeout ≤ 0 is rejected with `ConfigurationException` | Prevents `ulong` wrap-around |
| X10 | `InvalidTxnStatus` from client-side commands maps to a generic `PulsarClientException` | `InvalidTransactionStatusException` | The existing DotPulsar mapping is more specific |

## 3. Risks

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | Protocol bump v14→v19 changes broker behaviour | Low. The analysis in [04 §4](04-protocol.md#4-protocol-version-bump-14--19-adr-0004) found no broker behaviour change for 4.0.9 or master, and no new unsolicited commands as long as the version stays ≤ 19 and broker-entry-metadata stays off | WP1 is isolated. [04-protocol](04-protocol.md) has a checklist with a test per item. The full suite runs on v19 before any transaction code. |
| R2 | Docker image config: `PULSAR_PREFIX_*` env not applied by Testcontainers.Pulsar's start command | Transaction fixture has no TC | WP0 verifies first; [09](09-test-infrastructure.md) gives the fallback (custom command / config file) |
| R3 | Standalone has only **1 TC**; upstream E2E uses 16 | Round-robin and multi-TC routing are not exercised end-to-end | Covered by unit tests with 16 substituted handlers; documented in parity |
| R4 | Timing-based upstream tests (7 s broker txn timeout, 5 s receive waits) | Flaky CI | Keep the upstream values (parity). WP10 runs a 20× loop. If a test still flakes, use a `[Trait("Flaky","true")]` filter rather than changing values. |
| R5 | netstandard2.0 API gaps (`IsCompletedSuccessfully`, `Task.WaitAsync`, `ConcurrentDictionary.GetOrAdd<TArg>`, `AesGcm`) | Build breaks on ns2.0 | `#if` patterns from [01 §4](01-conventions.md#4-async-rules); a helper in `Internal/Extensions/TaskExtensions.cs` |
| R6 | Changing ack behaviour for batch messages affects existing consumers | Regression | D3: off by default; the existing `ConsumerChannelTests` must stay green |
| R7 | `SendError` handling change (hazard 5) alters producer fault behaviour | Users relying on producer faulting | Only txn-related and specific per-message errors fail the message; others keep the current behaviour. Spec in [producer-integration](05-components/producer-integration.md). |
| R8 | Large scope (8 prerequisite features) delays the core feature | Schedule | WP order delivers usable transactions after WP5. Prerequisite WPs are independent PRs. |
| R9 | Maintainers may reject the breaking interface additions | Rework | Follows the existing precedent. Raise it in the tracking issue before WP3. **Resolved (decision 0009): no public interface gets a member, so there is nothing to reject.** |
| R10 | Upstream moves (master) | Drift | Pinned SHA plus the [11](11-upstream-sync.md) procedure |
| R11 | The test image `apachepulsar/pulsar:4.0.9` lacks broker fixes that some upstream tests (pinned master) depend on | Tests fail for broker reasons | Use the skip-with-reason rule in [09](09-test-infrastructure.md#cross-cutting-rules-for-integration-tests); bump the image in a separate PR |
| R12 | Encryption: the upstream test EC keys use explicit curve parameters (secp128r1, secp521r1) that .NET may not import on every OS | `testEncryptionRequired` / interop tests fail on some platforms | [06/encryption](06-prerequisites/encryption.md) open decisions: EC on net8+ only; netstandard2.0 throws `PlatformNotSupportedException`; fall back to a BouncyCastle-based add-on if Windows rejects the curve (acceptance criterion A6) |
| R13 | Testcontainers.Pulsar applies broker env config only with `.WithAuthentication()` | Fixture silently runs without transactions | All transaction fixtures use `.WithAuthentication()` and `PULSAR_PREFIX_*` keys; the WP0 smoke test proves the TC is up ([09](09-test-infrastructure.md)) |

## 4. Where the remaining open decisions live

Every detailed spec ends with its own "Open decisions (recommendation)" section. **Follow the recommendation unless a
maintainer decides otherwise**, and record any change in this file:

- [04-protocol](04-protocol.md)
- [09-test-infrastructure](09-test-infrastructure.md)
- [08-tests/tests-e2e §7](08-tests/tests-e2e.md)
- [08-tests/tests-broker §11](08-tests/tests-broker.md)
- [08-tests/tests-client](08-tests/tests-client.md)
- every file in [06-prerequisites](06-prerequisites/)
- the decisions in the individual [05-components](05-components/) files

**Precedence when specs disagree:** this file, then `06-prerequisites/README.md` "Decisions that override",
`connection-integration §0`, `03-public-api` and the `05-components` specs, then `08-tests`, then the other
`06-prerequisites` files.
