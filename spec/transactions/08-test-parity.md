# 08 — Test parity

## 1. Rules (non-negotiable)

1. **Every** upstream transaction test that is observable from a client is listed, with exactly one status.

   | Status | Meaning |
   |---|---|
   | **Port** | Integration test. Every upstream assertion is reproduced, either through the public API or through admin REST in places where Java also uses `PulsarAdmin`. |
   | **Port-partial** | Integration test where one or more assertions or mechanisms that depend on broker internals are **substituted**, for example by admin REST stats or the TC client in place of reflection. Each substitution is written down. Nothing is dropped silently. |
   | **Port-unit** | Upstream used Mockito on client internals. The port is an NSubstitute unit test against the corresponding DotPulsar internals. |
   | **Excluded** | Cannot be reproduced against a real broker container: broker internals, v5, or broker-to-broker. The reason is given. |
   | **Sample** | Not a test. It maps to `samples/`. |
2. **Intent is fixed.** Keep message counts, values (`"Hello Txn - " + i`, …), timeouts (5 s receive waits, transaction
   timeouts), subscription types and names, batching variants, and DataProvider rows exactly as upstream. Only the
   language changes. A Java DataProvider becomes `[Theory]` + `[InlineData]` rows in the same order.
3. **Traceability:** put `// Upstream: <JavaClass>#<javaMethod>` above each ported test. Use
   `// Additional coverage` for tests with no upstream counterpart.
4. **Naming and layout:** follow [01-conventions §8](01-conventions.md#8-tests).
   - Integration tests go in `tests/DotPulsar.Tests/Transactions/<JavaClass>s.cs`.
   - Unit tests mirror the `src` path.
5. **Exceptions:** Java exception types map to DotPulsar types as in
   [05/exceptions](05-components/exceptions.md#2-new-exception-types).
6. **Prerequisite gating:** a test that needs a prerequisite feature that has not landed yet is committed with
   `Skip = "Requires <feature> (WPn)"`. The WP that lands the feature removes the skip, and that WP's acceptance
   requires the test to pass.
7. **Broker version gating:** see [09 §Cross-cutting rules](09-test-infrastructure.md#cross-cutting-rules-for-integration-tests).
8. **Flaky upstream tests** (`groups = "flaky"`) are ported as normal tests. If one is unstable in CI, add
   `[Trait("Category","Flaky")]`. Never delete it.

## 2. Totals

| Detailed spec | Upstream scope | Methods | Port | Port-partial | Port-unit | Excluded | Sample |
|---|---|---|---|---|---|---|---|
| [08-tests/tests-e2e.md](08-tests/tests-e2e.md) | `TransactionEndToEndTest`, `TransactionEndToEndWithoutBatchIndexAckTest`, `TransactionTestBase` | 29 (40 runs) | 12 | 16 | – | 1 | – |
| [08-tests/tests-broker.md](08-tests/tests-broker.md) | `TransactionTest`, `TransactionProduceTest`, `TransactionConsumeTest`, `PendingAckPersistentTest`, `ConsumerRedeliveryTest` (txn), `TopicTransactionBufferTest`, `TopicTransactionBufferPublishOrderTest`, `AuthenticatedTransactionProducerConsumerTest`, `TransactionAndSchemaAuthZTest` | 78 | 17 | 28 | – | 33 | – |
| [08-tests/tests-client.md](08-tests/tests-client.md) | `TransactionBuilderTest`, `TransactionMetaStoreHandlerTest`, `BatchMessageContainerImplTest`, `WatchTcAssignmentsDiscoveryTest`, `RawBatchMessageContainerImplTest`, `TransactionExample`, `TransactionClientCloseTest`, `TransactionClientConnectTest`, `TransactionCoordinatorClientTest`, `TransactionCoordinatorConfigTest`, `TransactionMetaStoreAssignmentTest`, `AutoCloseUselessClientConTXTest` | 65 + sample | 5 | 3 | 36 (37 C# tests) | 21 | 1 |
| [08-tests/tests-client.md §5](08-tests/tests-client.md) (sweep) | 47 further transaction test files (≈ 240 methods) | ≈ 240 | 3 | 1 | – | rest | – |
| [06-prerequisites/*](06-prerequisites/) | Upstream unit and integration tests **of the prerequisite features themselves** (batching, ack-set, ack receipt, ack timeout, DLQ, chunking, crypto) | see each file | | | | | |

Each detailed spec starts with a per-method summary table. Each table lists the Java method with its line range, the
C# test name, the prerequisites, and the status. That is followed by a numbered Arrange/Act/Assert translation of
each test.

## 3. Cross-cutting implementation requirements discovered by the parity analysis

The tests can only pass if the implementation provides the behaviours below. Each one is specified in a component
file.

| Id | Requirement | Spec |
|---|---|---|
| R-OP-SYNC / R-ORDER | Transactional sends are registered with the transaction synchronously and written in call order | [producer-integration](05-components/producer-integration.md) |
| R-SEND-ERROR | A `NotAllowedError` send error fails only that message | same |
| R-POOL | No transaction-id leakage through pooled commands or metadata | same; [consumer-integration](05-components/consumer-integration.md) |
| R-COMMIT-WAITS | Commit and abort wait for in-flight sends and acks | [transaction §4.5](05-components/transaction.md) |
| R-ACK-OUTSIDE-EXECUTOR | Transactional ack errors such as `TransactionConflictException` fail only that ack and never fault the consumer | [consumer-integration §10.4](05-components/consumer-integration.md) |
| R-BATCH-TXN | A batch never mixes transactions, or transactional and plain messages | [06/producer-batching](06-prerequisites/producer-batching.md) |
| R-ACK-BATCH-TXN | A transactional ack of a batch message always sends `ack_set` + `batch_size` | [06/batch-index-ack](06-prerequisites/batch-index-ack.md) |
| R-ACK-CHUNK-TXN | A transactional ack of a chunked message acks every chunk | [06/chunking](06-prerequisites/chunking.md) |
| R-TC-CLIENT-ACCESS | `PulsarClient.TransactionCoordinatorClient` is non-null right after `Build()`, and `EnsureStarted` is available | [client-integration](05-components/client-integration.md) |

## 4. Additional tests beyond parity (required)

| Area | Tests | Where specified |
|---|---|---|
| Pool reset regressions | `CommandSend`, `CommandAck` and `MessageMetadata` contain no transaction fields after a transactional use | [producer-integration §9.7](05-components/producer-integration.md), [consumer-integration](05-components/consumer-integration.md) |
| Protocol | registration of every response type; unknown-command safety; negotiated version | [04 §6](04-protocol.md#6-regression-checklist-wp1-acceptance) |
| Transaction unit tests | caching, op tracking, state machine, timer | [transaction §7](05-components/transaction.md) |
| Handler unit tests | retry, timeout, resend after reconnect, permits | [transaction-meta-store-handler §10](05-components/transaction-meta-store-handler.md) |
| Ordering | first transactional send followed by a plain send keeps call order | [producer-integration §9.3](05-components/producer-integration.md) |
| Cancellation | cancelling `Commit`/`Create` only stops the caller's await; the transaction state is unchanged | add to `TransactionTests` (unit): `Commit_GivenCallerCancels_ShouldNotChangeStateAndAllowRetry` |
| Disposal races | disposing the client while a commit is in flight fails the commit with `PulsarClientClosedException` and leaks nothing | [08-tests/tests-client](08-tests/tests-client.md) B1 |
| Multi-partition | sends to a 3-partition topic register each partition once (verified through admin `transactionMetadata`) | add to `TransactionProduceTests`: `Send_GivenPartitionedTopic_ShouldRegisterEachPartitionOnce` |
| Reconnect | Toxiproxy drops the TC connection mid-transaction; commit still succeeds after reconnect | [08-tests/tests-client](08-tests/tests-client.md) §4.2.9 |
