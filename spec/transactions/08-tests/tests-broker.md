# Test parity spec: broker-side Java transaction tests → DotPulsar integration tests

Scope: the Java test classes under `pulsar-broker/src/test/java` listed below, at apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026.
Every `@Test` method is listed (78 methods). Line numbers are from the pinned SHA. `java:` paths are relative to
`pulsar-broker/src/test/java/org/apache/pulsar/`.

| Java class (path) | `@Test` methods | Port | Port-partial | Excluded |
|---|---|---|---|---|
| `broker/transaction/TransactionTest.java` | 37 | 9 | 9 | 19 |
| `broker/transaction/TransactionProduceTest.java` | 7 | 1 | 5 | 1 |
| `broker/transaction/TransactionConsumeTest.java` | 5 | 1 | 3 | 1 |
| `broker/transaction/pendingack/PendingAckPersistentTest.java` | 11 | 4 | 3 | 4 |
| `client/api/ConsumerRedeliveryTest.java` (transaction methods only) | 2 | 1 | 0 | 1 |
| `broker/transaction/buffer/TopicTransactionBufferTest.java` | 10 | 1 | 3 | 6 |
| `broker/transaction/buffer/impl/TopicTransactionBufferPublishOrderTest.java` | 1 | 0 | 1 | 0 |
| `broker/transaction/AuthenticatedTransactionProducerConsumerTest.java` | 4 | 0 | 4 | 0 |
| `broker/admin/TransactionAndSchemaAuthZTest.java` | 1 | 0 | 0 | 1 |
| **Total** | **78** | **17** | **28** | **33** |

**API names.** This draft uses the names in [03-public-api](../03-public-api.md) :
`client.NewTransaction().Timeout(..).Create(ct)` (not `Build`), `ITransaction.CurrentState`, `ITransaction.Id`,
`producer.NewMessage().Transaction(txn).Send(v, ct)`, `consumer.Acknowledge(id, txn, ct)`,
`consumer.AcknowledgeCumulative(id, txn, ct)`, `ConsumerExtensions.Acknowledge(IConsumer, IEnumerable<IMessage>, ITransaction, ct)`,
the awaitable `txn.State.OnStateChangeTo(TransactionState.X, ct)`, and the test hook `((PulsarClient) client).TransactionCoordinatorClient!`
(`ITransactionCoordinatorClient`: `Commit/Abort(TransactionId, ct)`, `AddPublishPartitionToTxn(TransactionId, string, ct)`,
`AddSubscriptionToTxn(TransactionId, string topic, string subscription, ct)`). The default transaction timeout (no `.Timeout`) is 1 minute, like Java.
TC client start is lazy: `Build()` never throws, and the first `Create()` fails if there is no TC or no authentication.

---

## 0. Summary table

Legend: **Port** means a 1:1 port through the public API against a real broker. **Port-partial** means some assertions
are substituted (admin REST) or dropped; the exact list is in the method section. **Excluded** means the test
depends on broker internals, so it is not ported (reason given).

| # | Java class#method | Classification | C# class.method | Prerequisites | Status |
|---|---|---|---|---|---|
| T1 | TransactionTest#testTopicTransactionMetrics | (c) | TransactionTests.Commit_GivenCommittedAbortedAndOngoingTransactions_ShouldReportTransactionCountsInTopicStats | SendTimeout, admin topic stats | Port-partial (OTel dropped) |
| T2 | TransactionTest#testCreateTransactionSystemTopic | (c) | TransactionTests.Subscribe_GivenTransactionPendingAckSystemTopic_ShouldFaultWithNotAllowedException | admin REST | Port-partial (namespace delete/recreate → fresh namespace) |
| T3 | TransactionTest#testCanDeleteNamespaceWhenEnableTxnSegmentedSnapshot | (b) | — | — | Excluded: mutates broker config at runtime (`transactionBufferSegmentedSnapshotEnabled`); no transaction client API involved |
| T4 | TransactionTest#brokerNotInitTxnManagedLedgerTopic | (c) | TransactionTests.Create_GivenSystemNamespaceUnloaded_ShouldCreateTransaction | admin unload | Port-partial (broker topic-map asserts dropped) |
| T5 | TransactionTest#testAsyncSendOrAckForSingleFuture | (c) | TransactionTests.Commit_GivenConcurrentTransactionalSendsAndAcks_ShouldCompleteAllOperations | SendTimeout, topic dedup policy | Port-partial (client `opCount` reflection dropped; broker config → topic policy) |
| T6 | TransactionTest#testSendAndAckWithSchema | (c) | TransactionTests.Send_GivenStringSchemaProducerAndTransaction_ShouldDeliverValueWithSchemaVersion | SendTimeout | Port-partial (per-message schema → String producer, 03 §1) |
| T7 | TransactionTest#testGetTxnID | (a) | TransactionTests.Create_GivenPreviousTransaction_ShouldIncrementLeastSigBits | — | Port |
| T8 | TransactionTest#testSubscriptionRecreateTopic | (b) | — | — | Excluded: reflection on `ManagedLedgerFactoryImpl.ledgers`, `MLPendingAckStoreProvider` |
| T9 | TransactionTest#testPendingAckStoreEntriesArentPulsarMessages | (b) | — | — | Excluded: asserts `ManagedLedgerConfig.isPulsarMessageEntries` |
| T10 | TransactionTest#testTakeSnapshotBeforeBuildTxnProducer | (b) | — | — | Excluded: decodes broker-internal `TransactionBufferSnapshot` Avro records from a system topic |
| T11 | TransactionTest#testAppendBufferWithNotManageLedgerExceptionCanCastToMLE | (b) | — | — | Excluded: closes a managed ledger and calls `PersistentTopic.publishTxnMessage` directly |
| T12 | TransactionTest#testMaxReadPositionForNormalPublish | (c) | TransactionTests.Send_GivenNormalAndTransactionalMessages_ShouldMoveMaxReadPositionOnlyWhenNoOpenTransaction | EnableTransaction(false) client, admin TB stats | Port-partial (last reflection step dropped) |
| T13 | TransactionTest#testEndTBRecoveringWhenManagerLedgerDisReadable | (b) | — | — | Excluded: mocked `ManagedCursorImpl` injected into the managed ledger |
| T14 | TransactionTest#testEndTPRecoveringWhenManagerLedgerDisReadable | (b) | — | — | Excluded: mocked pending ack store provider |
| T15 | TransactionTest#testEndTCRecoveringWhenManagerLedgerDisReadable | (b) | — | — | Excluded: constructs `MLTransactionMetadataStore` with mocks |
| T16 | TransactionTest#testEndTxnWhenCommittingOrAborting | (b) | — (unit test, §10) | — | Excluded: reflection sets client txn state + broker interceptor counters |
| T17 | TransactionTest#testNoEntryCanBeReadWhenRecovery | (b) | — | — | Excluded: spies on `ManagedLedgerImpl` |
| T18 | TransactionTest#testRetryExceptionOfEndTxn | (b) | — (unit test, §10) | — | Excluded: reflection sets TC store state |
| T19 | TransactionTest#testCancelTxnTimeout | (b) | — (unit test, §10) | — | Excluded: reads the client `Timeout` field |
| T20 | TransactionTest#testNotChangeMaxReadPositionCountWhenCheckIfNoSnapshot | (b) | — | — | Excluded: reflection on `TopicTransactionBuffer` |
| T21 | TransactionTest#testAutoCreateSchemaForTransactionSnapshot | (b) | — | — | Excluded: runtime broker config (`allowAutoUpdateSchemaEnabled`), system-topic schemas only |
| T22 | TransactionTest#testPendingAckMarkDeletePosition | (c) | TransactionPendingAckLogIndexTests.Acknowledge_GivenCommittedTransactionalAck_ShouldMarkDeletePendingAckLogUpToUncommittedEntries | dedicated fixture (MinLag=1) | Port-partial (runtime config → fixture config) |
| T23 | TransactionTest#testConsistencyOfTransactionStatsAtEndTxn | (b) | — | — | Excluded: closes the TC log cursor via reflection |
| T24 | TransactionTest#testTransactionAckMessageList | (a) | TransactionTests.Acknowledge_GivenMessageIdListAndAbortedThenCommittedTransaction_ShouldRedeliverOnlyAfterAbort | SendTimeout | Port |
| T25 | TransactionTest#testTransactionAckMessages | (a) | TransactionTests.Acknowledge_GivenMessagesOnPartitionedTopicAndAbortedThenCommittedTransaction_ShouldRedeliverOnlyAfterAbort | SendTimeout | Port (reflection only built the Java `Messages` container) |
| T26 | TransactionTest#testGetConnectExceptionForAckMsgWhenCnxIsNull | (b) | — | — | Excluded: invokes the private `connectionClosed` via reflection; DotPulsar has different reconnect semantics (OD-13) |
| T27 | TransactionTest#testPendingAckBatchMessageCommit | (a) | TransactionTests.Acknowledge_GivenBatchMessageAckedInAbortedTransaction_ShouldAllowAckInNewTransaction | batching, Flush, batch-index txn ack | Port |
| T28 | TransactionTest#testPendingAckReplayChangeStateError | (b) | — | — | Excluded: all broker objects mocked |
| T29 | TransactionTest#testTBRecoverChangeStateError | (b) | — | — | Excluded: all broker objects mocked |
| T30 | TransactionTest#testGetTxnState | (c) | TransactionTests.CurrentState_GivenTransactionLifecycle_ShouldReportEachState | batching (hold a pending send), admin abortTransaction | Port-partial (two client/broker internals substituted) |
| T31 | TransactionTest#testEncryptionRequired | (a) | TransactionTests.Send_GivenEncryptionRequiredNamespaceAndEncryptedTransactionalMessage_ShouldSucceed | encryption, SendTimeout | Port |
| T32 | TransactionTest#testDeleteNamespace | (a) | TransactionTests.DeleteNamespace_GivenTransactionalProduceAndAck_ShouldSucceed | admin REST | Port |
| T33 | TransactionTest#testTBSnapshotWriter | (b) | — | — | Excluded: injects failed writer futures into `SystemTopicTxnBufferSnapshotService` |
| T34 | TransactionTest#testReadCommittedWithReadCompacted | (a) | TransactionTests.Receive_GivenReadCompactedConsumerAndOpenTransaction_ShouldDeliverTransactionalMessagesOnlyAfterCommit | topic policy REST | Port |
| T35 | TransactionTest#testReadCommittedWithCompaction | (a) | TransactionTests.Receive_GivenCompactedTopicWithCommittedTransactions_ShouldDeliverLatestValuePerKey | compaction REST | Port |
| T36 | TransactionTest#testDelayedDeliveryExceedsMaxDelay | (a) | TransactionTests.Send_GivenTransactionalMessageExceedingMaxDeliveryDelay_ShouldThrowNotAllowedException | encryption, SendTimeout, per-send error (OD-12) | Port |
| T37 | TransactionTest#testPersistentTopicGetLastDispatchablePositionWithTxn | (c) | TransactionTests.GetLastMessageIds_GivenOpenAndAbortedTransactions_ShouldReturnLastNonTransactionalMessage | `ReadAllAvailable` helper | Port-partial (`getLastDispatchablePosition` → `GetLastMessageIds`) |
| P1 | TransactionProduceTest#testAbortedTransactionDoesNotDiscardPlainMessageBatchedWithIt | (a) | TransactionProduceTests.Abort_GivenPlainMessageBatchedAfterTransactionalMessage_ShouldDeliverPlainMessage | batching, Flush, SendTimeout | Port |
| P2 | TransactionProduceTest#produceAndCommitTest | (c) | TransactionProduceTests.Commit_GivenTransactionalMessagesOnPartitionedTopic_ShouldWriteCommitMarkerAndDeliverAllMessages | SendTimeout, admin internal stats | Port-partial (ReadOnlyCursor → internal stats + consumer) |
| P3 | TransactionProduceTest#produceAndAbortTest | (c) | TransactionProduceTests.Abort_GivenTransactionalMessagesOnPartitionedTopic_ShouldWriteAbortMarkerAndDeliverNoMessages | same | Port-partial |
| P4 | TransactionProduceTest#testSkipMaxReadPositionTimestampWithoutReplicatedSubscriptions | (b) | — | — | Excluded: `PersistentTopic.getLastMaxReadPositionMovedForwardTimestamp()` has no external view |
| P5 | TransactionProduceTest#ackCommitTest | (c) | TransactionProduceTests.Commit_GivenTransactionalAcks_ShouldClearPendingAcksAndNotRedeliver | batching(1), admin position-in-pending-ack | Port-partial (pending ack count via REST) |
| P6 | TransactionProduceTest#ackAbortTest | (c) | TransactionProduceTests.Abort_GivenTransactionalAcks_ShouldClearPendingAcksAndRedeliver | same | Port-partial |
| P7 | TransactionProduceTest#testCommitFailure | (c) | TransactionProduceTests.Commit_GivenFailedTransactionalSend_ShouldThrowTransactionHasOperationFailedExceptionAndAbort | batching, max message size check | Port-partial (`getTxnMeta` → admin transactionMetadata 404) |
| C1 | TransactionConsumeTest#noSortedTest | (c) | TransactionConsumeTests.Receive_GivenOpenTransactionBetweenNormalMessages_ShouldHoldBackLaterMessagesUntilCommit | batching | Port-partial (raw broker append → real transaction) |
| C2 | TransactionConsumeTest#sortedTest | (c) | TransactionConsumeTests.Receive_GivenCommittedTransactionBetweenNormalMessages_ShouldDeliverInSendOrder | batching | Port-partial (same substitution) |
| C3 | TransactionConsumeTest#testMessageRedelivery | (b) | — | — | Excluded: reflection on `PersistentDispatcherMultipleConsumers.redeliveryMessages` |
| C4 | TransactionConsumeTest#testAckChunkMessage | (a) | TransactionConsumeTests.Acknowledge_GivenChunkedMessageAndTransaction_ShouldLeaveNoUnackedMessages | chunking, chunk MessageId (OD-11) | Port |
| C5 | TransactionConsumeTest#testFirstTnxBufferSnapshotAndRecoveryConcurrently ×2 dp ×3 invocations | (c) | TransactionConsumeTests.Send_GivenConcurrentFirstTransactionalSendsFromManyClients_ShouldDeliverOnlyCommitted | admin TB stats | Port-partial (TB state via REST) |
| A1 | PendingAckPersistentTest#testBuildConsumerEncounterPendingAckInitFailure ×3 | (b) | — | — | Excluded: Mockito replaces `transactionPendingAckStoreProvider` |
| A2 | PendingAckPersistentTest#individualPendingAckReplayTest | (c) | PendingAckPersistentTests.Acknowledge_GivenIndividualPendingAcksAndTopicUnload_ShouldReplayPendingAckState | batching(200), admin unload, pending ack internal stats | Port-partial (cursor reflection → REST) |
| A3 | PendingAckPersistentTest#testPendingAckMetrics | (c) | PendingAckPersistentTests.AcknowledgeCumulative_GivenCommittedAndAbortedTransactions_ShouldReportPendingAckMetrics | Prometheus `/metrics/` | Port-partial (OTel dropped) |
| A4 | PendingAckPersistentTest#cumulativePendingAckReplayTest | (c) | TransactionPendingAckReplayTests.AcknowledgeCumulative_GivenPendingAcksAndTopicUnload_ShouldReplayPendingAckState | dedicated fixture (MinLag=4002) | Port-partial (cursor reflection → REST; runtime config → fixture) |
| A5 | PendingAckPersistentTest#testDeleteSubThenDeletePendingAckManagedLedger | (a) | PendingAckPersistentTests.DeleteSubscription_GivenSubscription_ShouldNotLeavePendingAckTopic | admin REST | Port (private method, OD-15) |
| A6 | PendingAckPersistentTest#testDeleteTopicThenDeletePendingAckManagedLedger | (a) | PendingAckPersistentTests.DeleteTopic_GivenSubscriptions_ShouldNotLeavePendingAckTopics | admin REST | Port (private method, OD-15) |
| A7 | PendingAckPersistentTest#testDeleteUselessLogDataWhenSubCursorMoved | (b) | — | — | Excluded: `groups = "quarantine"`; reflection on `MLPendingAckStore.pendingAckLogIndex` / `maxIndexLag` |
| A8 | PendingAckPersistentTest#testPendingAckLowWaterMarkRemoveFirstTxn | (b) | — | — | Excluded: re-opens committed client transactions via reflection; reflection on `PendingAckHandleImpl` |
| A9 | PendingAckPersistentTest#testTransactionConflictExceptionWhenAckBatchMessage | (a) | PendingAckPersistentTests.Acknowledge_GivenBatchIndexAckedInCommittedTransaction_ShouldThrowTransactionConflictException | batching, AckReceiptEnabled, batch-index txn ack | Port (`setDeletionAtBatchIndexLevelEnabled` → fixture `acknowledgmentAtBatchIndexLevelEnabled=true`) |
| A10 | PendingAckPersistentTest#testGetSubPatternTopicFilterTxnInternalTopic | (a) | PendingAckPersistentTests.Subscribe_GivenTopicsPatternAndPendingAckTopics_ShouldReceiveFromUserTopicsOnly | TopicsPattern (exists) | Port |
| A11 | PendingAckPersistentTest#testGetManagedLegerConfigFailThenUnload | (b) | — | — | Excluded: spies on `BrokerService.getManagedLedgerConfig` |
| R1 | ConsumerRedeliveryTest#testSelectiveRedeliveryAfterConflictingTransactionalBatchAck ×4 | (a) | ConsumerRedeliveryTests.RedeliverUnacknowledgedMessages_GivenConflictingTransactionalBatchAck_ShouldRedeliverUntouchedBatchIndexes | key-based batching, EnableBatchIndexAcknowledgment, AckReceiptEnabled | Port (broker fix, OD-10) |
| R2 | ConsumerRedeliveryTest#testZeroCountSelectiveRedeliveryPreservesBackpressure ×2 | (b) | — | — | Excluded: pauses the broker `PendingAckHandleImpl` pinned executor; asserts broker `Consumer` internals |
| B1 | TopicTransactionBufferTest#testTransactionBufferAppendMarkerWriteFailState | (b) | — | — | Excluded: writes `ManagedLedgerImpl.state` via reflection; OTel |
| B2 | TopicTransactionBufferTest#testCheckDeduplicationFailedWhenCreatePersistentTopic | (b) | — | — | Excluded: spies on `BrokerService.newTopic` |
| B3 | TopicTransactionBufferTest#testCloseTransactionBufferWhenTimeout | (b) | — | — | Excluded: spies on `BrokerService.newTopic` (sleep injection) |
| B4 | TopicTransactionBufferTest#testGetMaxPositionAfterTBReady | (b) | — | — | Excluded: mocked `TransactionBufferProvider` |
| B5 | TopicTransactionBufferTest#testGetLastMessageIdsWithOngoingTransactions | (a) | TopicTransactionBufferTests.GetLastMessageIds_GivenOngoingTransactions_ShouldNotAdvancePastOpenTransactions | batching default | Port |
| B6 | TopicTransactionBufferTest#testGetLastMessageIdsWithOpenTransactionAtLedgerHead | (c) | TopicTransactionBufferTests.HasMessageAvailable_GivenOpenTransactionAtLedgerHead_ShouldReturnTrue | `HasMessageAvailable` helper, admin internal stats | Port-partial (ML state via REST) |
| B7 | TopicTransactionBufferTest#testWriteSnapshotWhenFirstTxnMessageSend | (c) | TopicTransactionBufferTests.Send_GivenFirstTransactionalMessage_ShouldMoveTransactionBufferFromNoSnapshotToReady | admin TB stats | Port-partial (TB state via REST) |
| B8 | TopicTransactionBufferTest#testMessagePublishInOrder | (c) | TopicTransactionBufferTests.Commit_GivenTransactionalMessagesSentWhileBufferBecomesReady_ShouldDeliverInSendOrder | send-order guarantee (OD-9) | Port-partial (test TB provider + `registerProducedTopic` dropped) |
| B9 | TopicTransactionBufferTest#testRefCountWhenAppendBufferToTxn | (b) | — | — | Excluded: calls `TransactionBufferTestImpl.appendBufferToTxn` with Netty `ByteBuf`s |
| B10 | TopicTransactionBufferTest#testAppendBufferToTxnWithInMemTransactionBuffer | (b) | — | — | Excluded: `InMemTransactionBuffer` direct call |
| O1 | TopicTransactionBufferPublishOrderTest#testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt ×2 | (c) | TopicTransactionBufferPublishOrderTests.Send_GivenPlainMessageAfterTransactionalMessage_ShouldStoreInSendOrder | send-order guarantee (OD-9), namespace dedup REST | Port-partial (mocked `AbortedTxnProcessor` hold → natural first-snapshot / recovery window) |
| U1 | AuthenticatedTransactionProducerConsumerTest#testEndTxn ×6 | (c) | AuthenticatedTransactionProducerConsumerTests.Commit_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser | auth fixture, TC client hook | Port-partial (`removeTransactionMetadataStore` → unload `pulsar/system`) |
| U2 | AuthenticatedTransactionProducerConsumerTest#testAddPartitionToTxn ×6 | (c) | AuthenticatedTransactionProducerConsumerTests.AddPublishPartitionToTxn_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser | same + admin transactionMetadata | Port-partial (`getTxnMeta` → REST) |
| U3 | AuthenticatedTransactionProducerConsumerTest#testAddSubscriptionToTxn ×6 | (c) | AuthenticatedTransactionProducerConsumerTests.AddSubscriptionToTxn_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser | same | Port-partial |
| U4 | AuthenticatedTransactionProducerConsumerTest#testNoAuth | (c) | AuthenticatedTransactionProducerConsumerTests.Create_GivenClientWithoutAuthentication_ShouldThrowAuthenticationException | auth fixture | Port-partial (failure point: `Create` instead of `build()`) |
| Z1 | TransactionAndSchemaAuthZTest#testSchemaAndTransactionAuthorization ×24 | (b) | — | — | Excluded: tests broker admin-REST authorization of schema/transaction admin endpoints with the Java admin client and an injected broker `AuthorizationService` checker. No DotPulsar client surface. |

Out of scope (not transaction-related): `ConsumerRedeliveryTest#testStaleSelectiveRedeliveryDoesNotUnblockConsumer` (L235-291, uses
no transaction), `#testOrderedRedelivery`, `#testUnAckMessageRedeliveryWithReceiveAsync`,
`#testConsumerWithPermitReceiveBatchMessages`, `#testMessageRedeliveryWhenTimeoutInListener`,
`#testMessageRedeliveryAfterUnloadedWithEarliestPosition`, `#testAckNotSent`, `#testRedeliverMessagesWithoutValue`.

---

## 1. Conventions applied in every ported test

These conventions add to the spec ([README](../README.md), [03-public-api](../03-public-api.md)).

1. **File/class.** `tests/DotPulsar.Tests/Transactions/<JavaClass>s.cs`, `public sealed class <JavaClass>s : IDisposable`,
   `[Collection("Transaction"), Trait("Category", "Integration")]`. The constructor takes `(TransactionFixture fixture, ITestOutputHelper output)`
   and creates `_cts = new CancellationTokenSource(TimeSpan.FromMinutes(1))`, like `tests/DotPulsar.Tests/Internal/ConsumerTests.cs` L36-41.
   If Java declares `timeOut = N`, the method makes a local `using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(N));`
   and uses it instead of `_cts`.
2. **Comment.** The line `// Upstream: <JavaClass>#<javaMethod>` goes directly above `[Fact]`/`[Theory]`. The body uses `//Arrange`, `//Act` and `//Assert`.
3. **Topic and namespace isolation.** Java `@BeforeMethod` classes get a fresh broker per method. `@BeforeClass` classes reuse
   topic names such as `tnx/ns1/test` across methods. Our fixture is shared per collection, so each Java topic literal `X` becomes
   `_fixture.UniqueTopic(X)`, which is `X + "-" + Guid.NewGuid().ToString("N")` (the scheme is added if Java adds it). The tenant, namespace,
   local-name prefix, partition count and subscription names stay exactly as in Java. If Java deletes, recreates or force-deletes a
   namespace, use a fresh `tnx/ns-<guid>` namespace instead (stated per test).
4. **Java default producer batching.** A Java producer that does not call `enableBatching` has batching **on** (1000 msgs, 128 KB,
   1 ms delay). Translate it as `.EnableBatching(true)` with defaults `BatchingMaxMessages(1000)`, `BatchingMaxBytes(131072)`,
   `BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1))`, unless DotPulsar's own defaults are those values (OD-2). The sketches
   write `.EnableBatching(true) /*Java default*/`.
5. **Java default consumer.** `SubscriptionType.Exclusive`, `SubscriptionInitialPosition.Latest`, receiver queue 1000 maps to
   DotPulsar's defaults (`MessagePrefetchCount` default 1000). Nothing extra is set.
6. **`sendTimeout(0, SECONDS)`** → `.SendTimeout(TimeSpan.Zero)` (0 = disabled, OD-3). `sendTimeout(N, SECONDS)` → `.SendTimeout(TimeSpan.FromSeconds(N))`.
7. **Java `receive(n, unit)` returning null** → `await _fixture.ReceiveOrNull(consumer, TimeSpan.From…(n), ct)`.
   Java blocking `receive()` → `await consumer.Receive(ct)`.
8. **Fire-and-forget Java calls.** Java `acknowledgeAsync(...)`, `sendAsync()`, `commit()`, `abort()` whose future is never joined
   become `var t = <call>.AsTask();`. Every such task is awaited at the end of the test through `await _fixture.Observe(t)`, which swallows
   exceptions and only stops unobserved-task noise. That is the only difference from Java. If Java joins later, the C# awaits at the same point.
9. **Empty Java value** (`producer.newMessage().send()` without `.value`) → `Array.Empty<byte>()` with `Schema.ByteArray`.
10. **Java `byte[]` default schema** → `Schema.ByteArray`. `Schema.STRING` → `Schema.String`. `Schema.INT32` → `Schema.Int32`.
11. **Message id equality.** `MessageId.Equals` compares ledger, entry, partition, batch index and topic (`src/DotPulsar/MessageId.cs` L117-118).
    Ids from the same consumer are compared directly. Ids compared against `Send` results compare `LedgerId`/`EntryId` only, as Java does.
12. **Awaitility.** `Awaitility.await().until/untilAsserted(...)` (default at-most 10 s, poll 100 ms) →
    `await _fixture.Eventually(async () => …assert…, TimeSpan.FromSeconds(10), ct)`. An explicit `atMost` replaces the 10 s.

---

## 2. Fixtures and broker configuration

### 2.1 `TransactionFixture` (collection `"Transaction"`)

Standalone `apachepulsar/pulsar:4.0.9` (the version is open, OD-10). It has authentication like `IntegrationFixture` and these broker
settings (`PULSAR_PREFIX_<key>=<value>` env, or `conf/standalone.conf` overrides). Each one mirrors
`java:broker/transaction/TransactionTestBase.java#startBroker (L144-167)`:

| Key | Value | Java source |
|---|---|---|
| `transactionCoordinatorEnabled` | `true` | L161 |
| `brokerDeduplicationEnabled` | `true` | L162 |
| `transactionBufferSnapshotMaxTransactionCount` | `2` | L163 |
| `transactionBufferSnapshotMinTimeInMillis` | `2000` | L164 |
| `acknowledgmentAtBatchIndexLevelEnabled` | `true` | the spec decisions; also covers PendingAckPersistentTest L815-817 |
| `allowAutoTopicCreationType` | `non-partitioned` | L153 |
| `forceDeleteNamespaceAllowed` | `true` | L155 |
| `activeConsumerFailoverDelayTimeMillis` | `0` | L149 |
| `defaultNumberOfNamespaceBundles` | `1` | L150 |
| `dispatcherRetryBackoffInitialTimeInMs` / `dispatcherRetryBackoffMaxTimeInMs` | `0` / `0` | L166-167 |
| `numTransactionReplayThreadPoolSize` | `1` | TransactionTest#setup L186 |
| `managedLedgerNumSchedulerThreads` | `1` | TransactionTest#setup L187 |
| `topicLevelPoliciesEnabled` / `systemTopicEnabled` | `true` / `true` | needed by T34-T36 topic policies (4.x default true; set explicitly) |

Created at `InitializeAsync` through admin REST (super-user token):
- tenant `tnx` (admin roles `appid1`, allowed cluster = standalone cluster), namespace `tnx/ns1` with **4** bundles
  (TransactionTestBase L117-119);
- namespace `public/txn` with **10** bundles (TransactionConsumeTest#setup L96-98).

There is one TC (standalone). Java uses 1 TC (TransactionTest L181, TransactionConsumeTest L104 and AuthenticatedTransactionProducerConsumerTest L124)
or 16 TCs (TransactionProduceTest L82, PendingAckPersistentTest L104 and TopicTransactionBufferTest L92). No ported test depends on more than one TC;
T7 depends on exactly one (`MostSigBits == 0`).

### 2.2 Extra fixtures for non-dynamic broker configuration

`transactionPendingAckLogIndexMinLag` and `managedLedgerDefaultMarkDeleteRateLimit` are not dynamic
(`pulsar-broker-common/.../ServiceConfiguration.java` L4408 and L2829: no `dynamic = true`). Java mutates them at runtime, so we use
derived fixtures (OD-5):

| Fixture | Collection | Extra settings | Used by |
|---|---|---|---|
| `TransactionPendingAckLogIndexFixture : TransactionFixture` | `"TransactionPendingAckLogIndex"` | `transactionPendingAckLogIndexMinLag=1`, `managedLedgerDefaultMarkDeleteRateLimit=5` | T22 (class `TransactionPendingAckLogIndexTests`) |
| `TransactionPendingAckReplayFixture : TransactionFixture` | `"TransactionPendingAckReplay"` | `transactionPendingAckLogIndexMinLag=4002` (`4 * 1000 + 2`), `managedLedgerDefaultMarkDeleteRateLimit=10` | A4 (class `TransactionPendingAckReplayTests`) |

`TransactionFixture` exposes `protected virtual IReadOnlyDictionary<string,string> BrokerSettings` so the derived fixtures only add keys.

### 2.3 `AuthenticatedTransactionFixture` (collection `"AuthenticatedTransaction"`)

This mirrors `java:broker/transaction/AuthenticatedTransactionProducerConsumerTest.java#setup (L102-130)`:
- `authenticationEnabled=true`, `authorizationEnabled=true`,
  `authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken`;
- `superUserRoles=admin` (Java L107-109). Testcontainers' `WithAuthentication()` adds its own super-user subject; keep both:
  `superUserRoles=admin,<testcontainers subject>`;
- token signing: Java uses an RSA key pair (L80-100). We use the container's secret key (`tokenSecretKey`). The signing algorithm has no bearing
  on the test intent. `brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken`,
  `brokerClientAuthenticationParameters=token:<admin token>` (L120-121);
- `transactionCoordinatorEnabled=true` plus every §2.1 setting;
- created at init: tenant `tnx` (admin role `appid1`), namespace `tnx/ns1` (4 bundles), partitioned topic `tnx/ns1/txn-auth` with **1** partition
  (`setUpBase(1, 1, TOPIC, 1)` L124);
- grants (L126-129, L132-138): `POST /admin/v2/namespaces/pulsar/system/permissions/client` body `["consume"]`;
  `POST /admin/v2/namespaces/tnx/ns1/permissions/client` body = all `AuthAction` values
  `["produce","consume","functions","sources","sinks","packages"]`; `POST /admin/v2/namespaces/pulsar/system/permissions/client2` body `["consume"]`.
- helper `Task<string> CreateToken(string subject, CancellationToken ct)` runs
  `bin/pulsar tokens create --secret-key <container key path> --subject <subject> --expiry-time 1h` in the container (Java expiry 1 h, L92).

### 2.4 Fixture helper signatures (all on `TransactionFixture`; the auth fixture derives from it)

```csharp
// connection
Uri ServiceUrl { get; }
Uri AdminUrl { get; }
IAuthentication Authentication { get; }                                 // super-user token
IPulsarClient CreateClient(bool enableTransaction = true, IAuthentication? authentication = null, IHandleException? exceptionHandler = null);

// names
string UniqueTopic(string javaTopic);                                    // §1.3
string UniqueNamespace(string javaNamespacePrefix);                      // "tnx/ns-" + guid

// admin REST (all use the super-user bearer token unless a token is passed)
Task<AdminResponse> AdminSend(HttpMethod method, string path, object? jsonBody, CancellationToken ct, string? token = null);
// AdminResponse(int StatusCode, string? Reason /* JSON "reason" */, string Body, JsonElement? Json)
Task CreateNamespace(string ns, int? bundles, CancellationToken ct);                       // PUT /admin/v2/namespaces/{ns} body {"bundles":{"numBundles":n}}
Task DeleteNamespace(string ns, bool force, CancellationToken ct);                         // DELETE /admin/v2/namespaces/{ns}?force=true
Task UnloadNamespace(string ns, CancellationToken ct);                                     // PUT /admin/v2/namespaces/{ns}/unload
Task SetNamespaceEncryptionRequired(string ns, bool required, CancellationToken ct);       // POST /admin/v2/namespaces/{ns}/encryptionRequired body true
Task SetNamespaceDeduplication(string ns, bool enabled, CancellationToken ct);             // POST /admin/v2/namespaces/{ns}/deduplication body false
Task GrantNamespacePermission(string ns, string role, string[] actions, CancellationToken ct);
Task CreateTopic(string topic, CancellationToken ct);                                      // PUT /admin/v2/persistent/{t}/{ns}/{local}
Task CreatePartitionedTopic(string topic, int partitions, CancellationToken ct);           // PUT …/{local}/partitions body n
Task DeleteTopic(string topic, bool force, CancellationToken ct);
Task UnloadTopic(string topic, CancellationToken ct);                                      // PUT …/{local}/unload
Task<string[]> GetTopicList(string ns, CancellationToken ct);                              // GET /admin/v2/persistent/{ns}  (admin.topics().getList)
Task<string[]> GetNamespaceTopics(string ns, CancellationToken ct);                        // GET /admin/v2/namespaces/{ns}/topics (admin.namespaces().getTopics)
Task DeleteSubscription(string topic, string subscription, CancellationToken ct);         // DELETE …/{local}/subscription/{sub}
Task<JsonElement> GetTopicStats(string topic, CancellationToken ct);                       // GET …/{local}/stats
Task<JsonElement> GetPartitionedTopicStats(string topic, CancellationToken ct);            // GET …/{local}/partitioned-stats
Task<JsonElement> GetInternalStats(string topic, CancellationToken ct);                    // GET …/{local}/internalStats
Task SetTopicDeduplication(string topic, bool enabled, CancellationToken ct);              // POST …/{local}/deduplicationEnabled body false
Task SetTopicCompactionThreshold(string topic, long bytes, CancellationToken ct);          // POST …/{local}/compactionThreshold body bytes
Task SetTopicDelayedDeliveryPolicy(string topic, bool active, long tickTimeMillis, long maxDeliveryDelayInMillis, CancellationToken ct); // POST …/{local}/delayedDelivery
Task TriggerCompaction(string topic, CancellationToken ct);                                // PUT …/{local}/compaction
Task<string> GetCompactionStatus(string topic, CancellationToken ct);                      // GET …/{local}/compaction → "status"
// admin v3 transactions (java: broker/admin/v3/Transactions.java paths)
Task<JsonElement> GetTransactionBufferStats(string topic, CancellationToken ct);           // GET /admin/v3/transactions/transactionBufferStats/{t}/{ns}/{local}   (L189-190)
Task<JsonElement> GetPendingAckInternalStats(string topic, string sub, CancellationToken ct); // GET /admin/v3/transactions/pendingAckInternalStats/{t}/{ns}/{local}/{sub} (L344-345)
Task<string> GetPositionStateInPendingAck(string topic, string sub, ulong ledgerId, ulong entryId, int? batchIndex, CancellationToken ct); // GET …/positionStatsInPendingAck/{t}/{ns}/{local}/{sub}/{ledger}/{entry}?batchIndex= (L467-491) → "state"
Task<AdminResponse> GetTransactionMetadata(TransactionId id, CancellationToken ct);       // GET /admin/v3/transactions/transactionMetadata/{most}/{least} (L274-275); 404 when not found (TransactionsBase L209-210)
Task AbortTransaction(TransactionId id, CancellationToken ct);                             // POST /admin/v3/transactions/abortTransaction/{most}/{least} (L514-515)
Task<string> GetPrometheusMetrics(CancellationToken ct);                                   // GET {AdminUrl}/metrics/
IReadOnlyList<PrometheusSample> ParsePrometheus(string text);                              // PrometheusSample(string Name, IReadOnlyDictionary<string,string> Tags, double Value)
Task WaitForManagedLedgerState(string topic, string state, CancellationToken ct);          // polls internalStats "state" (e.g. "LedgerOpened")

// client helpers
ValueTask<IMessage<T>?> ReceiveOrNull<T>(IConsumer<T> consumer, TimeSpan timeout, CancellationToken ct);
Task<List<T>> DrainValues<T>(IConsumer<T> consumer, TimeSpan perReceiveTimeout, CancellationToken ct); // loop ReceiveOrNull until null
Task WaitForReconnect(IConsumer consumer, CancellationToken ct);   // after a topic unload: StateChangedFrom(Active) then StateChangedTo(Active); returns at once if no state change happened within 5 s
Task<bool> HasMessageAvailable<T>(IReader<T> reader, MessageId startMessageId, MessageId? lastRead, CancellationToken ct); // §2.5
Task<List<IMessage<T>>> ReadAllAvailable<T>(IReader<T> reader, MessageId startMessageId, CancellationToken ct);            // while (HasMessageAvailable) Receive
Task Eventually(Func<Task> assertion, TimeSpan atMost, CancellationToken ct);              // Awaitility; poll 100 ms; rethrows last failure
Task Observe(Task task);                                                                   // §1.8
```

### 2.5 `HasMessageAvailable` semantics

This is Java `ConsumerImpl#hasMessageAvailableAsync (L3016-3105)` restricted to the paths the tests use (`startMessageId = earliest`,
non-inclusive) plus `#hasMoreMessages (L3111-3119)`:
1. Get `last = (await reader.GetLastMessageIds(ct)).Single()`.
2. If `last.EntryId == ulong.MaxValue` (Java `-1`), return `false`.
3. `reference = lastRead ?? startMessageId`. If `reference == MessageId.Earliest`, return `true`, because DotPulsar's `Earliest` is `(ulong.MaxValue,…)`
   (`MessageId.cs` L27) and must not be compared numerically. Otherwise return `last > reference`.

---

## 3. Prerequisite API names used by this draft

Keep these names (team-lead list):
`IProducerBuilder<T>.EnableBatching(bool)`, `.BatchingMaxMessages(uint)`, `.BatchingMaxPublishDelay(TimeSpan)`, `.BatchingMaxBytes(uint)`,
`.SendTimeout(TimeSpan)`, `.EnableChunking(bool)`, `.CryptoKeyReader(ICryptoKeyReader)`, `.EncryptionKey(string)`;
`IConsumerBuilder<T>.AckReceiptEnabled(bool)`, `.AckTimeout(TimeSpan)`, `.DeadLetterPolicy(...)` (the last two are not used here).

Added by this draft (need an owner in 06-prerequisites):

| API | Used by | Java equivalent |
|---|---|---|
| `IProducer.Flush(CancellationToken)` → `ValueTask` (sends the open batch now; completes when that batch is handed to the connection) | T27, P1 | `Producer.flush()` |
| `IProducerBuilder<T>.BatcherType(BatcherType)` with `enum BatcherType` (see 06-prerequisites/producer-batching.md §3.1) | R1 | `batcherBuilder(BatcherBuilder.KEY_BASED)` |
| `IConsumerBuilder<T>.BatchIndexAckEnabled(bool)` | R1 | `enableBatchIndexAcknowledgment(true)` |
| `DotPulsar.DefaultCryptoKeyReader` (`ICryptoKeyReader`), ctor `(string? publicKeyPath, string? privateKeyPath)`, reads PEM files | T31, T36 | `defaultCryptoKeyReader("file:…")` |
| Chunked-send `MessageId` exposing first/last chunk: `MessageId.FirstChunkMessageId` (`MessageId?`, null unless chunked); `Send` returns the last chunk's id | C4 | `ChunkMessageIdImpl#getFirstChunkMessageId/getLastChunkMessageId` (OD-11) |
| `PulsarClient.TransactionCoordinatorClient` (`ITransactionCoordinatorClient?`) with `Commit(TransactionId, CancellationToken)`, `AddPublishPartitionToTxn(TransactionId, string topic, CancellationToken)`, `AddSubscriptionToTxn(TransactionId, string topic, string subscription, CancellationToken)` | U1-U3 | `PulsarClientImpl#getTransactionCoordinatorClient()` (03 §3.8; match method names to 05/transaction-coordinator-client) |

Behavioural prerequisites (spec requirements, not new names):
- **R-BATCH-TXN.** A batch carries one txn id, so the batch container must never mix messages from different transactions or with
  non-transactional messages (P1; `TransactionProduceTest` L96-100).
- **R-ACK-BATCH-TXN.** A transactional ack of a batched message id sends `ack_set` = all `batchSize` bits set except the acked index (individual),
  or except `[0..batchIndex]` (cumulative), plus `batch_size`. This applies even when `EnableBatchIndexAcknowledgment(false)`
  (`java:pulsar-client/.../ConsumerImpl.java#doTransactionAcknowledgeForResponse (L3520-3540)`). Used by T27, A9, R1.
- **R-ACK-CHUNK-TXN.** A transactional individual ack of a chunked message acks every chunk entry (ConsumerImpl L3541-3546+, `unAckedChunkedMessageIdSequenceMap`). Used by C4.
- **R-ORDER.** Messages sent on one producer are written in `Send` call order, whether or not they carry a transaction, including while the first
  send of a transaction waits for `AddPartitionToTxn` (OD-9). Used by B8, O1.
- **R-COMMIT-WAITS.** `Commit`/`Abort` wait for every send and ack that was started with the transaction before the call
  (Java `TransactionImpl` op futures). Used by T5, T24, A10, T30, P5.
- **R-SEND-ERROR.** A per-message broker `SendError` (e.g. `NotAllowedError` for an excessive delivery delay) fails only that `Send` with the
  mapped exception and does not fault the producer (OD-12). Used by T36.
- **R-MAX-SIZE.** A message bigger than the broker's max message size (no chunking) fails that `Send` with `TooLargeMessageException`
  (exists: `src/DotPulsar/Internal/Connection.cs` L253) and is recorded as a failed transaction op (Java `ProducerImpl` L607-621). Used by P7.

---

## 4. TransactionTest → `tests/DotPulsar.Tests/Transactions/TransactionTests.cs`

Java class: `java:broker/transaction/TransactionTest.java`, `@Test(groups = "broker")` (L177). It is `@BeforeClass`, so one broker is shared,
the same as our fixture. Setup L183-189: 1 broker, 1 TC, topic `tnx/ns1/test` created non-partitioned.

### T1 `#testTopicTransactionMetrics (L196-275)`
Classification (c). Ported: the 3 topic-stats assertions (L229-231), using the REST topic stats JSON fields `committedTxnCount`, `abortedTxnCount`
and `ongoingTxnCount` (public fields of `TopicStatsImpl`). Dropped: the 5 OpenTelemetry assertions (L233-274), because they read the
in-process OTel reader of the Java test broker and the standalone container has no OTel metric reader.

```csharp
// Upstream: TransactionTest#testTopicTransactionMetrics
[Fact]
public async Task Commit_GivenCommittedAbortedAndOngoingTransactions_ShouldReportTransactionCountsInTopicStats()
```
1. Arrange: `topic = _fixture.UniqueTopic("persistent://tnx/ns1/test_transaction_topic")` (Java `newUniqueName`).
   `client.NewProducer(Schema.ByteArray).Topic(topic).SendTimeout(TimeSpan.Zero).EnableBatching(true)/*Java default*/.Create()`.
2. `txn = Create(Timeout 1 min)`. Send `Encoding.UTF8.GetBytes(Guid.NewGuid().ToString())` with txn, then `await txn.Commit()`.
3. `txn1` (1 min): send the same kind of value, then `await txn1.Abort()`.
4. `txn2` (1 min): send. It is not ended.
5. Assert: `stats = await _fixture.GetTopicStats(topic)`. `committedTxnCount == 1`, `abortedTxnCount == 1`, `ongoingTxnCount == 1`.
   The values are read once with no polling, as Java does (L227).

### T2 `#testCreateTransactionSystemTopic (L277-333)`
Classification (c). Substituted: `deleteNamespace(NAMESPACE1, true)` + `createNamespace(NAMESPACE1)` (L281-282) → a fresh namespace
`ns = _fixture.UniqueNamespace("tnx/ns")` created with default bundles. Java's purpose is a clean namespace; deleting the shared `tnx/ns1`
would break the other tests. Every assertion is kept. The admin assertions go through REST.

```csharp
// Upstream: TransactionTest#testCreateTransactionSystemTopic
[Fact]
public async Task Subscribe_GivenTransactionPendingAckSystemTopic_ShouldFaultWithNotAllowedException()
```
1. `topicName = $"persistent://{ns}/testCreateTransactionSystemTopic"`, `subName = "test"`.
2. Consumer `Schema.ByteArray`, `SubscriptionType.Shared`, sub `"test"`. `txn` timeout 10 s.
3. `try { await consumer.Acknowledge(new MessageId(10, 10, 10, -1), txn, ct); } catch (Exception e) { e.ShouldBeOfType<TransactionConflictException>(); }`
   (L289-293: an exception is allowed but must be a conflict.)
4. `pendingAck = topicName + "-test__transaction_pending_ack"` (`MLPendingAckStore.getTransactionPendingAckStoreSuffix`: origin + `-` + sub + `__transaction_pending_ack`).
5. `list = await _fixture.GetTopicList(ns)`. `list.ShouldNotBeEmpty()`, and no element contains `"__transaction_pending_ack"` (L297-299).
6. A new client with `exceptionHandler` capturing the exception. Its consumer on `pendingAck`, sub `"test"`, `Shared`.
   `await Should.ThrowAsync<ConsumerFaultedException>(consumer.Receive(ct))`. The inner (or captured) exception is `NotAllowedException`
   and `Message.ShouldContain("Can not create transaction system topic")` (L301-308).
7. `GET …/{pendingAck local}/subscriptions` → `Reason.ShouldBe("Can not create transaction system topic " + pendingAck)` (L311-316).
8. `PUT …/{pendingAck local}/partitions` body `3` → `Reason.ShouldBe("Cannot create topic in system topic format!")` (L319-324).
9. `PUT …/{pendingAck local}` → `Reason.ShouldBe("Cannot create topic in system topic format!")` (L327-332).
10. Cleanup: `DeleteNamespace(ns, force: true)`.

### T3 `#testCanDeleteNamespaceWhenEnableTxnSegmentedSnapshot (L335-354)`: Excluded
It flips `transactionBufferSegmentedSnapshotEnabled` on the live broker config (L338, L353) and only tests broker namespace deletion. The producer
on L348 uses no transaction.

### T4 `#brokerNotInitTxnManagedLedgerTopic (L356-399)`
Classification (c). Ported: L363-391 (the TC client can create transactions again after `pulsar/system` and the user namespace are unloaded).
Dropped: the 3 `assertNull(topics.get(...))` at L393-398. They read the broker's in-memory topic map (`BrokerService.getTopics()`), and no admin
endpoint reports "loaded but no producer" without loading the topic itself.

```csharp
// Upstream: TransactionTest#brokerNotInitTxnManagedLedgerTopic
[Fact]
public async Task Create_GivenSystemNamespaceUnloaded_ShouldCreateTransaction()
```
1. `topicName = _fixture.UniqueTopic("persistent://tnx/ns1/test")`, sub `"test"`. Shared consumer (helper `getConsumer` L401-407). Wait until it is active, then dispose it.
2. `Eventually(() => client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(ct))` (L367-375; any exception means retry).
3. `UnloadNamespace("pulsar/system")`, then `UnloadNamespace("tnx/ns1")`.
4. A new Shared consumer on the same topic and sub.
5. `Eventually(() => Create(30 s))` must succeed (L383-391).

### T5 `#testAsyncSendOrAckForSingleFuture (L409-490)`
Classification (c).
- Substituted: `getConfig().setBrokerDeduplicationEnabled(false)` (L415) → `SetTopicDeduplication(topic, false)` before the producer is created.
  It has the same effect on this topic and does not leak to other tests (in Java it leaks to later methods in the class).
- Dropped: the `TransactionImpl.opCount == 0` reflection (L471-474). It is client-internal, and its intent ("no op left in flight") is covered by
  the next assertion (all 600 tasks complete) plus R-COMMIT-WAITS.

```csharp
// Upstream: TransactionTest#testAsyncSendOrAckForSingleFuture
[Fact]
public async Task Commit_GivenConcurrentTransactionalSendsAndAcks_ShouldCompleteAllOperations()
```
1. `topic = UniqueTopic("tnx/ns1/testSingleFuture")`, `totalMessage = 10`, `threadSize = 30`, sub `"subscription"`.
2. Producer `ByteArray`, `ProducerName("producer")`, `SendTimeout(Zero)`, batching Java default. Consumer `Exclusive`, sub `"subscription"`.
3. `txn1` timeout 10 s.
4. Send 300 (`totalMessage * threadSize`) empty messages with no transaction, awaited one by one.
5. Start 30 `Task.Run` workers. Each loops 10 times: `sendTasks.Add(producer.NewMessage().Transaction(txn1).Send(empty, ct).AsTask())`;
   `msg = await consumer.Receive(ct)`; `ackTasks.Add(consumer.Acknowledge(msg.MessageId, txn1, ct).AsTask())`.
   The lists are `ConcurrentQueue<Task>`. Worker exceptions are caught and logged (L460-462). This needs concurrent `Receive` on one consumer
   (OD-17).
6. `await Task.WhenAny(Task.WhenAll(workers), Task.Delay(10 s))` (latch `await(10, SECONDS)` L467). Then `await txn1.Commit(ct)`.
7. Assert: for `i in 0..299`, `sendTasks[i].IsCompleted` and `ackTasks[i].IsCompleted` (L476-479).
8. `txn2` timeout 10 s. `Eventually(() => txn2.Commit(ct))` succeeds (L482-489).

### T6 `#testSendAndAckWithSchema (L492-526)`
Classification (c). Java sends one message through a `byte[]` producer using `newMessage(Schema.STRING, txn)` (multi-schema, PIP-407 path).
03 §1 says this is "Not applicable … create a producer with that schema". So we use `Schema.String`. Kept: value equality and `SchemaVersion != null`.
Topic dedup is disabled (L496) the same way as T5.

```csharp
// Upstream: TransactionTest#testSendAndAckWithSchema
[Fact]
public async Task Send_GivenStringSchemaProducerAndTransaction_ShouldDeliverValueWithSchemaVersion()
```
1. `topic = UniqueTopic("tnx/ns1/testAsyncSendAndAckWithSchema")`, `SetTopicDeduplication(topic,false)`.
2. Producer `Schema.String`, `ProducerName("producer")`, `SendTimeout(Zero)`, batching Java default. Consumer `Schema.ByteArray`, Exclusive, sub `"subscription"`.
3. `txn` 10 s. `await producer.NewMessage().Transaction(txn).Send("testSendAndAckWithSchema", ct)`. `await txn.Commit()`.
4. `msg = await consumer.Receive(ct)`. `Encoding.UTF8.GetString(msg.Value()) == "testSendAndAckWithSchema"`. `msg.SchemaVersion.ShouldNotBeNull()`.

### T7 `#testGetTxnID (L528-540)` (Port)
```csharp
// Upstream: TransactionTest#testGetTxnID
[Fact]
public async Task Create_GivenPreviousTransaction_ShouldIncrementLeastSigBits()
```
1. `txn = await client.NewTransaction().Create(ct)` (default timeout). `id = txn.Id`.
2. `var abort = txn.Abort(ct).AsTask();`. Java does not join it (L534).
3. `txn1 = Create()`. Assert `txn1.Id.LeastSigBits == id.LeastSigBits + 1` and `txn1.Id.MostSigBits == 0UL`.
4. `await _fixture.Observe(abort)`.
Precondition: no other test in the `"Transaction"` collection runs at the same time (xUnit runs a collection sequentially) and there is 1 TC.

### T8-T11, T13-T21, T23, T26, T28, T29, T33: Excluded
The reasons are in the summary table. T16, T18 and T19 test client-side state-machine behaviour. §10 suggests replacement unit tests.

### T12 `#testMaxReadPositionForNormalPublish (L755-830)`
Classification (c).
- Substituted: `TopicTransactionBuffer.getMaxReadPosition()` → `transactionBufferStats.maxReadPosition` (format `"ledgerId:entryId"`), and
  `checkIfNoSnapshot()` → `transactionBufferStats.state == "NoSnapshot"`.
- Dropped: L816-829 (forces TB `state = Initializing` via reflection).

```csharp
// Upstream: TransactionTest#testMaxReadPositionForNormalPublish
[Fact]
public async Task Send_GivenNormalAndTransactionalMessages_ShouldMoveMaxReadPositionOnlyWhenNoOpenTransaction()
```
1. `topic = UniqueTopic("persistent://tnx/ns1/NormalPublish")`, `CreateTopic(topic)`.
2. `noTxnClient = _fixture.CreateClient(enableTransaction: false)`. Producer `Schema.String`, `ProducerName("testNormalPublish")`, `SendTimeout(Zero)`,
   batching Java default.
3. `Eventually(state == "NoSnapshot")`.
4. `id = await normal.Send("normal message")`. Assert `maxReadPosition == $"{id.LedgerId}:{id.EntryId}"`.
5. `txnProducer` from the txn client: `ProducerName("testTransactionPublish")`, `SendTimeout(Zero)`. `Eventually(state == "NoSnapshot")` (L790).
6. `txn` 5 s. `id1 = txnProducer…Transaction(txn).Send("txn message")`. Assert `maxReadPosition == pos(id)`.
7. `id2 = normal.Send("normal message")`. Assert `maxReadPosition == pos(id)`.
8. `await txn.Commit()`. Assert `maxReadPosition == $"{id2.LedgerId}:{id2.EntryId + 1}"` (commit marker, L806-807).
9. `id4 = normal.Send("normal message")`. Assert `maxReadPosition == pos(id4)`.
Note: the txn timeout is 5 s and steps 6-8 must finish within it, as in Java.

### T22 `#testPendingAckMarkDeletePosition (L1247-1320)` → class `TransactionPendingAckLogIndexTests` (fixture §2.2, collection `"TransactionPendingAckLogIndex"`)
Classification (c). Substituted: the runtime config mutation (L1249-1250) → fixture config. The assertions already use admin
(`getPendingAckInternalStats`) → REST.

```csharp
// Upstream: TransactionTest#testPendingAckMarkDeletePosition
[Fact]
public async Task Acknowledge_GivenCommittedTransactionalAck_ShouldMarkDeletePendingAckLogUpToUncommittedEntries()
```
1. `topic = UniqueTopic("tnx/ns1/testPendingAckMarkDeletePosition")`. Producer `ByteArray`, `SendTimeout(Zero)`, batching Java default. Consumer sub `"sub"` (Exclusive).
2. Send `"test"u8`. `txn` 5 min. `m1 = ReceiveOrNull(10 s)`. `var a1 = consumer.Acknowledge(m1.MessageId, txn).AsTask()` (not joined in Java). `await txn.Commit()`.
3. `txn = Create(5 min)`. Send `"test"`. `m2 = ReceiveOrNull(10 s)`. `var a2 = consumer.Acknowledge(m2.MessageId, txn).AsTask()`.
4. `Eventually`: `s = GetPendingAckInternalStats(topic, "sub")`. `ml = s.pendingAckLogStats.managedLedgerInternalStats`.
   `md = ml.cursors["__pending_ack_state"].markDeletePosition.Split(':')`, `lce = ml.lastConfirmedEntry.Split(':')`.
   Assert `md[0] == lce[0]` and `int.Parse(md[1]) == int.Parse(lce[1]) - 2` (L1306-1319).
5. Observe `a1`, `a2`.

### T24 `#testTransactionAckMessageList (L1345-1408)` (Port), `timeOut = 30000`
```csharp
// Upstream: TransactionTest#testTransactionAckMessageList
[Fact]
public async Task Acknowledge_GivenMessageIdListAndAbortedThenCommittedTransaction_ShouldRedeliverOnlyAfterAbort()
```
1. `topic = UniqueTopic("persistent://tnx/ns1/test")`, sub `"testSub"`. Producer `ByteArray`, `SendTimeout(5 s)`, batching Java default. Consumer (Exclusive, Latest).
2. Send 5 empty messages (awaited). Receive 4 and collect `messages` (ids).
3. `txn` 5 min. `var a = consumer.Acknowledge(messages, txn, ct).AsTask();`, then `await txn.Abort()` (R-COMMIT-WAITS: abort waits for the ack).
   Dispose the consumer.
4. New consumer on the same sub. Receive 4. Each `messages.ShouldContain(m.MessageId)`.
5. `txn` 5 min. `a2 = Acknowledge(messages, txn).AsTask()`. `await txn.Commit()`. Dispose the consumer.
6. New consumer. `m = ReceiveOrNull(5 s)`: not null, and `messages.ShouldNotContain(m.MessageId)`. `ReceiveOrNull(5 s).ShouldBeNull()`.

### T25 `#testTransactionAckMessages (L1410-1484)` (Port), `timeOut = 30000`
The reflection at L1430-1444 only builds a Java `Messages<byte[]>` holding the 4 received messages. The C# equivalent is
`consumer.Acknowledge(IEnumerable<IMessage>, txn, ct)` (03 §1).
1. `topic = UniqueTopic("persistent://tnx/ns1/testTransactionAckMessages")`, `CreatePartitionedTopic(topic, 2)`, sub `"testSub"`.
2. Producer `SendTimeout(5 s)`, batching Java default, DotPulsar default router. Consumer default.
3. Send 4 empty messages. Receive 4 into `messageList`.
4. `txn` 5 min. `a = consumer.Acknowledge(messageList, txn).AsTask()`. `await txn.Abort()`. Dispose the consumer.
5. New consumer. `ids = messageList.Select(m => m.MessageId)`. 4 × `m = ReceiveOrNull(5 s)`, which is not null and `ids.ShouldContain(m.MessageId)`.
6. `txn` 5 min. `Acknowledge(messageList, txn)` (not joined). `await txn.Commit()`. Dispose the consumer.
7. New consumer. `ReceiveOrNull(5 s).ShouldBeNull()`.
The ack in step 6 reuses the original message ids after the consumer was recreated, as Java does at L1473.

### T27 `#testPendingAckBatchMessageCommit (L1523-1571)` (Port)
```csharp
// Upstream: TransactionTest#testPendingAckBatchMessageCommit
[Fact]
public async Task Acknowledge_GivenBatchMessageAckedInAbortedTransaction_ShouldAllowAckInNewTransaction()
```
1. `topic = UniqueTopic("tnx/ns1/testPendingAckBatchMessageCommit")`. Producer `ByteArray`, `EnableBatching(true)`,
   `BatchingMaxPublishDelay(TimeSpan.FromSeconds(3))`, `SendTimeout(Zero)`. Consumer `Shared`, sub `"sub"`.
2. For i in 0..4: `producer.Send(UTF8("test"+i))` not awaited. Then `await producer.Flush(ct)`.
3. `txn1` 10 min. `await consumer.Acknowledge((await consumer.Receive()).MessageId, txn1)`.
4. `txn2` 10 min. `id = (await consumer.Receive()).MessageId`. `await consumer.Acknowledge(id, txn2)`.
5. `await txn1.Commit()`. `await txn2.Abort()`.
6. `txn3` 10 min. `await consumer.Acknowledge(id, txn3)` completes without an exception.
Requires R-ACK-BATCH-TXN.

### T30 `#testGetTxnState (L1742-1790)`
Classification (c). Substituted:
(a) the broker-internal `getTransactionMetadataStoreService().endTransaction(txnId, 0 /*COMMIT*/, false)` (L1769-1770) →
`_fixture.AbortTransaction(txn.Id)` (admin REST). Either way the broker has ended and removed the txn, so the client's `Commit` fails with
`TransactionNotFound`. The asserted client outcome (`Error`) is unchanged.
(b) the client-internal `registerSendOp(new CompletableFuture<>())` (L1778, L1786) → a real pending transactional send held in an unflushed
batch (`EnableBatching(true)`, `BatchingMaxMessages(1000)`, `BatchingMaxPublishDelay(TimeSpan.FromHours(1))`). Under R-COMMIT-WAITS the
commit/abort cannot proceed, so the state stays `Committing`/`Aborting`. The assertions are unchanged.

```csharp
// Upstream: TransactionTest#testGetTxnState
[Fact]
public async Task CurrentState_GivenTransactionLifecycle_ShouldReportEachState()
```
1. `txn` timeout 1 s. Assert `txn.CurrentState == TransactionState.Open`. `await txn.State.OnStateChangeTo(TransactionState.TimedOut, ct)` (Java Awaitility L1750).
   Steps 4-6 can use `OnStateChangeTo(Error/Committing/Aborting, ct)` the same way, in place of `Eventually`.
2. `txn` 3 s. `await Abort()`. Assert `Aborted`.
3. `txn` 3 s. `await Commit()`. Assert `Committed`.
4. `txn` 1 s. `await _fixture.AbortTransaction(txn.Id)`. `var c = txn.Commit(ct).AsTask();` (not joined). `Eventually(CurrentState == Error)`. Observe `c`.
5. Holding producer (topic `UniqueTopic("tnx/ns1/testGetTxnState")`, settings in (b)). `txn` 3 s. `var s = producer.NewMessage().Transaction(txn).Send(empty).AsTask()`.
   `var c2 = txn.Commit(ct).AsTask()`. `Eventually(CurrentState == Committing)`.
6. `txn` 3 s. Another held send. `var a = txn.Abort(ct).AsTask()`. `Eventually(CurrentState == Aborting)`.
7. Cleanup: dispose the producer, then Observe `s`, `c2`, `a` and the second send.
Risk: in step 4 the client timer (1 s) could fire before `Commit`. The admin call takes well under 1 s, which matches Java's order.

### T31 `#testEncryptionRequired (L1792-1814)` (Port)
Prerequisite: encryption. Resource: copy `pulsar-broker/src/test/resources/certificate/public-key.client-rsa.pem` (pinned SHA) to
`tests/DotPulsar.Tests/Resources/certificate/public-key.client-rsa.pem` with `CopyToOutputDirectory` (OD-16).
1. `ns = UniqueNamespace("tnx/testEncryptionRequired")` (Java `tnx/testEncryptionRequired`). `CreateNamespace(ns)`. `SetNamespaceEncryptionRequired(ns, true)`.
   `topic = $"persistent://{ns}/test_transaction_topic"`. `CreateTopic(topic)`.
2. Producer `ByteArray`, `SendTimeout(5 s)`, `EncryptionKey("my-app-key")`,
   `CryptoKeyReader(new DefaultCryptoKeyReader("Resources/certificate/public-key.client-rsa.pem", null))`, batching Java default.
3. `txn` 5 s. `await producer.NewMessage().Transaction(txn).Send(UTF8(Guid))` completes without an exception. `var c = txn.Commit().AsTask()` (not joined). Observe `c`.

### T32 `#testDeleteNamespace (L1816-1846)` (Port)
1. `ns = "tnx/ns-" + 5 random ASCII letters` (Java `RandomStringUtils.randomAlphabetic(5)`). `CreateNamespace(ns)`. `topic = ns + "/test-delete-ns"`.
2. Producer (Java defaults). Send `"test"u8`.
3. `txn` 5 s. Consumer `InitialPosition(Earliest)`, sub `"sub"`. `m = Receive()`. `await consumer.Acknowledge(m.MessageId, txn)`. Dispose the consumer.
4. `outProducer` topic `topic + "-out"`. `await …Transaction(txn).Send("output"u8)`. Dispose it.
5. `var c = txn.Commit().AsTask()` (not joined, L1843). Dispose the producer.
6. `await _fixture.DeleteNamespace(ns, force: true)` succeeds (2xx). Observe `c`.

### T34 `#testReadCommittedWithReadCompacted (L1933-1986)` (Port)
1. `ns = UniqueNamespace("tnx/ns-read-committed-compacted")`. `topic = $"persistent://{ns}/test_transaction_topic"`. Create both.
   `SetTopicCompactionThreshold(topic, 100L * 1024 * 1024)`.
2. Consumer `Schema.String`, sub `"sub"`, Exclusive, `ReadCompacted(true)`. Then producer `Schema.String` (batching Java default).
3. Send key `"K1"` value `"V1"`. `txn` 1 min. Send `K2/V2` and `K3/V3` with txn.
4. `DrainValues(consumer, 5 s)` → `["V1"]`.
5. `var c = txn.Commit().AsTask()` (not joined).
6. `DrainValues(consumer, 5 s)` → `["V2","V3"]`. Observe `c`.

### T35 `#testReadCommittedWithCompaction (L1988-2044)` (Port)
1. `ns = UniqueNamespace("tnx/ns-read-committed-compaction")`. `topic = $"persistent://{ns}/test_transaction_topic{Guid}"`. Create both. Compaction threshold 100 MB.
2. Producer `String`. Send K1/V1. `txn` 1 min: K2/V2, K3/V3, then `await Commit()`. Send K1/V4. `txn2` 1 min: K2/V5, K3/V6, then `await Commit()`.
3. `TriggerCompaction(topic)`. `Eventually(GetCompactionStatus(topic) == "SUCCESS")`.
4. Consumer `String`, sub `"sub"`, Exclusive, `InitialPosition(Earliest)`, `ReadCompacted(true)`.
5. `DrainValues(consumer, 2 s)` → `["V4","V5","V6"]`.

### T36 `#testDelayedDeliveryExceedsMaxDelay (L2046-2081)` (Port)
1. `maxDeliveryDelayInMillis = 5000`. `ns = UniqueNamespace("tnx/testDelayedDeliveryExceedsMaxDelay")`. `topic = $"persistent://{ns}/test_transaction_topic{Guid}"`. Create both.
   `SetTopicDelayedDeliveryPolicy(topic, active: true, tickTimeMillis: 100, maxDeliveryDelayInMillis: 5000)`.
2. Producer like T31 (`SendTimeout(5 s)`, `EncryptionKey("my-app-key")`, the same key reader).
3. `txn` 5 s. `var ex = await Should.ThrowAsync<NotAllowedException>(producer.NewMessage().Transaction(txn).DeliverAt(DateTimeOffset.UtcNow.AddSeconds(6)).Send(UTF8(Guid)).AsTask())`.
   `ex.Message.ShouldBe("Exceeds max allowed delivery delay of 5000 milliseconds")`.
   Java `deliverAfter(6, SECONDS)` = `deliver_at_time = now + 6000`, which is equivalent.
4. Java's `txn.commit()` is never reached on the expected path. C#: `var c = txn.Commit().AsTask()` after the assertion; Observe it.
Requires R-SEND-ERROR.

### T37 `#testPersistentTopicGetLastDispatchablePositionWithTxn (L2083-2148)`
Classification (c). Substituted: `persistentTopic.getLastDispatchablePosition()` (L2107, L2115) → `probe.GetLastMessageIds(ct)` on a reader (`probe`)
created at the start. `CommandGetLastMessageId` is served from the broker's last dispatchable position: §B5 asserts the same contract.
Compare `LedgerId`/`EntryId`.
1. `topic = UniqueTopic("persistent://tnx/ns1/testPersistentTopicGetLastDispatchablePositionWithTxn")`. Producer `String`, `EnableBatching(false)`.
   `probe = client.NewReader(Schema.String).Topic(topic).StartMessageId(MessageId.Earliest).Create()`.
2. `body = Guid`. `msgId = Send(body)`.
3. `txn` (default). Send 3 Guid strings with txn.
4. Assert `last = (await probe.GetLastMessageIds()).Single()` has `(LedgerId, EntryId) == (msgId.LedgerId, msgId.EntryId)`.
5. `await txn.Abort()` (Java `get(5, SECONDS)`: use a 5 s token). Assert the same.
6. `reader` from Earliest. `txn1` (default). Send 3 with txn1. `messages = ReadAllAvailable(reader, Earliest)`. Count 1, value `body`.
7. `await txn1.Abort()` (5 s). `reader1` from Earliest. `ReadAllAvailable` → count 1, value `body`.

---

## 5. TransactionProduceTest → `tests/DotPulsar.Tests/Transactions/TransactionProduceTests.cs`

Java: `java:broker/transaction/TransactionProduceTest.java`, `@Test(groups = "broker")`, `@BeforeClass`. Setup L83-89: 16 TCs, and the partitioned
topics `tnx/ns1/produce-commit`, `produce-abort`, `ack-commit`, `ack-abort` each with **3** partitions. C#: each test creates
`UniqueTopic(<that literal>)` partitioned 3 itself.

### P1 `#testAbortedTransactionDoesNotDiscardPlainMessageBatchedWithIt (L101-149)` (Port)
```csharp
// Upstream: TransactionProduceTest#testAbortedTransactionDoesNotDiscardPlainMessageBatchedWithIt
[Fact]
public async Task Abort_GivenPlainMessageBatchedAfterTransactionalMessage_ShouldDeliverPlainMessage()
```
1. `topic = UniqueTopic("tnx/ns1/txn-batch-isolation")`, `CreateTopic`. Consumer `String`, sub `"txn-batch-isolation-sub"`.
2. `txn` 60 s. Producer `String`, `SendTimeout(Zero)`, `EnableBatching(true)`, `BatchingMaxMessages(100)`, `BatchingMaxPublishDelay(5 s)`.
3. `await producer.NewMessage().Transaction(txn).Send("warm-up")` with a 30 s token.
4. `var t = …Transaction(txn).Send("in-txn").AsTask()`. `var plain = producer.Send("plain").AsTask()`. `await producer.Flush()`. `await plain` (30 s).
5. `await txn.Abort()` (60 s).
6. `m = ReceiveOrNull(30 s)`: `ShouldNotBeNull("the plain message was discarded by the aborted transaction")`, `m.Value() == "plain"`.
7. `ReceiveOrNull(3 s).ShouldBeNull()`. Observe `t`.
Requires R-BATCH-TXN.

### P2/P3 `#produceAndCommitTest (L151-154)` / `#produceAndAbortTest (L156-159)` → shared helper `ProduceTest(bool endAction)` (Java `produceTest` L161-242)
Two `[Fact]`s, one per Java method, each calling `ProduceTest(true|false)`.
Classification (c).
- Substituted: the Java `ReadOnlyCursor` reads of raw managed-ledger entries.
  - Entry counts before the end (L198-203, 3 per partition) and after it (L225-229, 4 per partition) → `GetInternalStats(partitionTopic).numberOfEntries`.
    For a fresh topic with no trimming, that equals the number of entries written.
  - Marker type `TXN_COMMIT`/`TXN_ABORT` on the 4th entry (L231-237) → the observable effect of the marker. On commit, a consumer created
    `InitialPosition(Earliest)` before the commit receives exactly the 9 contents (consumes `messageSet` to empty, L214 + L240). On abort it
    receives nothing within 5 s.
- Dropped: `messageMetadata.txnid_*` equality per entry (L207-209). The entries are not readable via the public API or admin without the payload
  decoding that `examinemessage` does not expose. Cover it with a unit test asserting `MessageMetadata.TxnidMostBits/LeastBits` are set on send (§10).
- Kept: `txnIdMostBits > -1 && txnIdLeastBits > -1` (L171-172) → `txn.Id.MostSigBits != ulong.MaxValue && txn.Id.LeastSigBits != ulong.MaxValue`
  (Java `-1` as `long` is `ulong.MaxValue`). Also kept: every send returns an id within 1 s (`checkMessageId` L277-301).

Steps (`endAction` true = commit):
1. `topic = UniqueTopic(endAction ? "tnx/ns1/produce-commit" : "tnx/ns1/produce-abort")`, `CreatePartitionedTopic(topic, 3)`.
2. `txn` timeout 5 s. Assert the id bits.
3. Observer consumer: `ByteArray`, sub `"observer"`, `InitialPosition(Earliest)`.
4. Producer `ByteArray`, `SendTimeout(Zero)`, `EnableBatching(false)`.
5. For i in 0..8 (`3 partitions * 3`): `msg = "Hello Txn - " + i`. `messageSet.Add(msg)`. `futures.Add(producer.NewMessage().Transaction(txn).Send(UTF8(msg)).AsTask())`.
6. For each future: `(await f.WaitAsync(1 s)).ShouldNotBeNull()`.
7. For p in 0..2: `GetInternalStats($"{topic}-partition-{p}").numberOfEntries == 3`.
8. `await (endAction ? txn.Commit() : txn.Abort())`.
9. For p in 0..2: `numberOfEntries == 4`.
10. Commit: 9 × `m = ReceiveOrNull(5 s)`, not null, `messageSet.Remove(UTF8(m.Value())).ShouldBeTrue()`; then `messageSet.Count == 0`.
    Abort: `ReceiveOrNull(5 s).ShouldBeNull()`. Java's final `messageSet.size() == 0` (L240) was satisfied from the raw entries, which the abort
    case cannot see through the API, so for abort that assertion is replaced by the null receive.

### P4 `#testSkipMaxReadPositionTimestampWithoutReplicatedSubscriptions (L244-269)`: Excluded
`PersistentTopic.getLastMaxReadPositionMovedForwardTimestamp()` has no admin or client view.

### P5 `#ackCommitTest (L318-379)` and P6 `#ackAbortTest (L381-442)`
Classification (c). Substituted: `getPendingAckCount` (L444-472, reflection on `PendingAckHandleImpl.individualAckPositions`) →
`PendingAckCount(topic, sub, receivedIds)` = number of received ids for which
`GetPositionStateInPendingAck($"{topic}-partition-{id.Partition}", sub, id.LedgerId, id.EntryId, batchIndex: null) == "PendingAck"`.
Java counts positions in the map. Each message is its own entry (batch of 1), so the per-position count is the same.

Common arrange: producer `ByteArray`, `EnableBatching(true)`, `BatchingMaxMessages(1)`, round-robin per message (Java
`roundRobinRouterBatchingPartitionSwitchFrequency(1)`; DotPulsar `RoundRobinPartitionRouter` already switches per message, so no extra API).
Consumer `ByteArray`, `InitialPosition(Earliest)`, `Shared`. `incomingMessageCnt = 10`. Wait for the consumer to be active (Java `isConnected`).

P5 `Commit_GivenTransactionalAcks_ShouldClearPendingAcksAndNotRedeliver`: topic `UniqueTopic("tnx/ns1/ack-commit")` (3 partitions), sub `"ackCommitTest"`, `txn` **5 s**.
1. 10 × `Send("Hello Txn."u8)` not awaited (L335).
2. 10 × `m = Receive()`. `acks.Add(Acknowledge(m.MessageId, txn).AsTask())` (not joined).
3. `Eventually(PendingAckCount == 10)`.
4. `RedeliverUnacknowledgedMessages()`. `ReceiveOrNull(2 s).ShouldBeNull()`. `PendingAckCount == 10`.
5. `await txn.Commit()`. `Eventually(PendingAckCount == 0)`.
6. `RedeliverUnacknowledgedMessages()`. 10 × `ReceiveOrNull(2 s).ShouldBeNull()`.

P6 `Abort_GivenTransactionalAcks_ShouldClearPendingAcksAndRedeliver`: topic `"tnx/ns1/ack-abort"`, sub `"ackAbortTest"`, `txn` **30 s**, and the sends **are** awaited (L398).
Steps 2-4 are the same. 5: `await txn.Abort()`. `Eventually(PendingAckCount == 0)`. 6: `RedeliverUnacknowledgedMessages()`. 10 × `ReceiveOrNull(2 s).ShouldNotBeNull()`.

### P7 `#testCommitFailure (L474-495)`
Classification (c). Substituted: `getTransactionMetadataStoreService().getTxnMeta(id).getNow(null)` throwing `TransactionNotFoundException`
(L488-494) → `GetTransactionMetadata(txn.Id)` returning HTTP **404** (`TransactionsBase#internalGetTransactionMetadata` L209-210 maps
`TransactionNotFoundException` to `NOT_FOUND`).
1. `txn = Create()` (default). `topic = UniqueTopic("tnx/ns1/test-commit-failure")`. Producer `ByteArray`, batching Java default.
2. `var s = producer.NewMessage().Transaction(txn).Send(new byte[1024 * 1024 * 10]).AsTask()` (not joined).
3. `await Should.ThrowAsync<TransactionHasOperationFailedException>(txn.Commit(ct).AsTask())`. `txn.CurrentState.ShouldBe(TransactionState.Aborted)`.
4. `(await GetTransactionMetadata(txn.Id)).StatusCode.ShouldBe(404)`. Observe `s` (expected `TooLargeMessageException`).
Requires R-MAX-SIZE.

---

## 6. TransactionConsumeTest → `tests/DotPulsar.Tests/Transactions/TransactionConsumeTests.cs`

Java: `java:broker/transaction/TransactionConsumeTest.java`, `@Test(groups = "broker")`, **`@BeforeMethod`** (fresh broker per method, L87-105).
Topic `persistent://public/txn/txn-consume-test` (L83) → `UniqueTopic(...)`, created non-partitioned per test.

### C1 `#noSortedTest (L112-193)` and C2 `#sortedTest (L195-251)`
Classification (c). Substituted: `appendTransactionMessages` (L320-392) writes 10 entries straight into the topic through
`PersistentTopic.publishTxnMessage` with a fabricated `TxnID(2, 5)` and metadata producer `"producerName"`/sequence `i`. `persistentTopic.endTxn(txnID, COMMIT, 0)`
ends it. Replacement: a real transaction. A separate transaction-enabled client creates a producer `Schema.ByteArray`, `SendTimeout(Zero)`
(Java's helper creates the same producer, L323-329) and sends `"Txn - " + i` for i in 0..9 with `txn` (timeout **5 min**, OD-7). The commit is `txn.Commit()`.
The values, counts, order and every assertion are unchanged. Java's helper producer is `Schema.STRING` and only switches the TB to Ready;
in C# that producer also sends the txn messages, as `Schema.ByteArray` with UTF-8 bytes, which is byte-identical to Java's payload.

Common: producer `ByteArray` (Java defaults: batching on), `exclusive` = consumer sub `"exclusive-test"` (Exclusive), `shared` = sub `"shared-test"` `Shared`. Both
are created before any send and awaited until active. Counts: before = 10, txn = 10, after = 10, total = 30. Normal values `"Normal - " + n`.

C1 `Receive_GivenOpenTransactionBetweenNormalMessages_ShouldHoldBackLaterMessagesUntilCommit`:
1. Send `Normal - 0..9`. Send `Txn - 0..9` with txn. Send `Normal - 10..19`.
2. For i in 0..29: if i < 10, `ReceiveOrNull(exclusive, 2 s)` and `ReceiveOrNull(shared, 2 s)` are both not null; otherwise both `ReceiveOrNull(…, 500 ms)` are null.
3. `await txn.Commit()`.
4. 20 × (`ReceiveOrNull(exclusive, 5 s)` not null; `ReceiveOrNull(shared, 5 s)` not null).

C2 `Receive_GivenCommittedTransactionBetweenNormalMessages_ShouldDeliverInSendOrder`:
1. Send `Normal - 0..9`. Send `Txn - 0..9` with txn. `await txn.Commit()`. Send `Normal - 10..19`.
2. For i in 0..29: `m = ReceiveOrNull(exclusive, 2 s)`, not null, `UTF8(m.Value()) == sendMessageList[i]`; the same for shared.

### C3 `#testMessageRedelivery (L253-308)`: Excluded
It replaces `PersistentDispatcherMultipleConsumers.redeliveryMessages`/`totalAvailablePermits` via reflection and calls `readMoreEntries()`.

### C4 `#testAckChunkMessage (L394-434)` (Port)
```csharp
// Upstream: TransactionConsumeTest#testAckChunkMessage
[Fact]
public async Task Acknowledge_GivenChunkedMessageAndTransaction_ShouldLeaveNoUnackedMessages()
```
1. `topic = UniqueTopic(...)`, `CreateTopic`. A separate txn client (L398-400). Producer `String`, `ProducerName("test-producer")`, `EnableChunking(true)`, `EnableBatching(false)`.
   Consumer `String`, `Shared`, sub `"testAckChunkMessage"`.
2. `message = new string('a', 6000 * 1000)`. `id = await producer.Send(message)`.
   Assert `id.FirstChunkMessageId.ShouldNotBeNull()` and `id.FirstChunkMessageId.ShouldNotBe(id)` (L419-421; OD-11).
3. `txn` timeout 5 h. `msg = Receive()`. `var a = consumer.Acknowledge(msg.MessageId, txn).AsTask()` (not joined). `await txn.Commit()`.
4. `GetTopicStats(topic).subscriptions["testAckChunkMessage"].unackedMessages == 0`. Observe `a`.
Requires R-ACK-CHUNK-TXN and the broker default `maxMessageSize` (5 MB), so 6 MB is chunked.

### C5 `#testFirstTnxBufferSnapshotAndRecoveryConcurrently (L436-526)`, DataProvider `doCommitTxn` = `{true}, {false}` (L436-442), `timeOut = 60_000`, `invocationCount = 3`
Classification (c). Substituted: TB state/future (L478-492) → `Eventually(GetTransactionBufferStats(topic).state == "Ready")`. Java's
`getTransactionBufferFuture().isDone() && !isCompletedExceptionally()` is implied by the `Ready` state. `invocationCount = 3` → an extra
`invocation` parameter (OD-14).
```csharp
// Upstream: TransactionConsumeTest#testFirstTnxBufferSnapshotAndRecoveryConcurrently
[Theory]
[InlineData(true, 1)] [InlineData(true, 2)] [InlineData(true, 3)]
[InlineData(false, 1)] [InlineData(false, 2)] [InlineData(false, 3)]
public async Task Send_GivenConcurrentFirstTransactionalSendsFromManyClients_ShouldDeliverOnlyCommitted(bool doCommitTxn, int invocation)
```
1. 60 s token. `topic = UniqueTopic("persistent://public/txn/tp")`. `producerCount = 10`: 10 txn clients, each with a producer `String`
   (batching Java default). Consumer (fixture's txn client) `String`, sub `"s1"`.
2. For i in 0..9: `txn_i = clients[i].NewTransaction().Timeout(5 h).Create()`. Then
   `_ = Task.Run(() => sendResults.Add(producers[i].NewMessage().Transaction(txn_i).Send(i.ToString()).AsTask()))`
   (a thread per send, L472-474; `sendResults` is a thread-safe list).
3. `Eventually(TB state == "Ready")`.
4. Wait until `sendResults.Count == 10`. For i: `await sendResults[i]`; then `doCommitTxn ? txn_i.Commit() : txn_i.Abort()` (not joined, L498-500).
5. `DrainValues(consumer, 2 s)` into a set. Commit: contains `"0".."9"`. Abort: empty.
6. Cleanup: dispose the consumer, producers and clients. `DeleteTopic(topic, force: false)`. Observe the end tasks.

---

## 7. PendingAckPersistentTest → `tests/DotPulsar.Tests/Transactions/PendingAckPersistentTests.cs`

Java: `java:broker/transaction/pendingack/PendingAckPersistentTest.java`, `@Test(groups = "broker")`, **`@BeforeMethod`** (L106-109): 16 TCs, topic
`tnx/ns1/pending-ack-replay` non-partitioned. C# uses `UniqueTopic("tnx/ns1/pending-ack-replay")` per test.

### A1 `#testBuildConsumerEncounterPendingAckInitFailure (L130-208)`, DataProvider `retryableErrors` (L116-123: `ManagedLedgerException`, `MetadataStoreException`, `BKException(-1)`), `timeOut = 60000`: Excluded
Mockito replaces the broker's `transactionPendingAckStoreProvider` field (L141-174).

### A2 `#individualPendingAckReplayTest (L210-323)`
Classification (c). Substituted: the final reflection on `MLPendingAckStore.cursor` (L301-322) →
`Eventually`: `ml = GetPendingAckInternalStats(topic, "individual-test").pendingAckLogStats.managedLedgerInternalStats`.
`ParsePosition(ml.cursors["__pending_ack_state"].markDeletePosition) < ParsePosition(ml.lastConfirmedEntry)` (Java `compareTo == -1`), comparing
(ledger, entry) lexicographically.
```csharp
// Upstream: PendingAckPersistentTest#individualPendingAckReplayTest
[Fact]
public async Task Acknowledge_GivenIndividualPendingAcksAndTopicUnload_ShouldReplayPendingAckState()
```
1. `messageCount = 1000`, sub `"individual-test"`. Producer `ByteArray`, `EnableBatching(true)`, `BatchingMaxMessages(200)`. Consumer `Shared`.
2. `abortTxn` 30 s. For i in 0..999: `await producer.Send("Hello Pulsar!"u8)`. `m = Receive()`. Even i: `await Acknowledge(m.MessageId, abortTxn)` and `pending.Add(id)`; odd i: `normal.Add(id)`.
3. `UnloadTopic(topic)`. `WaitForReconnect(consumer)`. `commitTxn` 30 s. `txn` 30 s.
4. Each of `pending`: `Should.ThrowAsync<TransactionConflictException>(Acknowledge(id, txn))` (500 times).
5. Each of `normal`: `await Acknowledge(id, commitTxn)`.
6. `await txn.Abort()`. `await commitTxn.Commit()`. `await abortTxn.Abort()`.
7. `UnloadTopic`. `WaitForReconnect`. New `abortTxn` 30 s and `commitTxn` 30 s.
8. Each of `normal`: `ThrowAsync<TransactionConflictException>(Acknowledge(id, abortTxn))`. Each of `pending`: `await Acknowledge(id, commitTxn)`.
9. `await abortTxn.Abort()`. `await commitTxn.Commit()`.
10. The substituted assertion above.
Timeout: `_cts` 5 min (Java has none). Note `abortTxn` (30 s) must survive steps 2-6, as in Java.

### A3 `#testPendingAckMetrics (L325-429)`
Classification (c). Ported: the Prometheus assertions (L362-392) through `GET {AdminUrl}/metrics/` (standalone exposes topic-level metrics by
default, `exposeTopicLevelMetricsInPrometheus=true`). Dropped: the 4 OTel `ACK_COUNTER` assertions (L394-428, values 50/0/50/0), because the
standalone has no OTel reader (OD-5 alternative: enable an OTel Prometheus exporter, not recommended).
```csharp
// Upstream: PendingAckPersistentTest#testPendingAckMetrics
[Fact]
public async Task AcknowledgeCumulative_GivenCommittedAndAbortedTransactions_ShouldReportPendingAckMetrics()
```
1. `messageCount = 100`. `subName = "testMetric" + Guid`. Producer `String` (Java defaults). Consumer `String`, `Exclusive`.
2. Send 100 `Guid` strings.
3. For a in 0..99: `m = ReceiveOrNull(10 s)`, break if null. `txn` 10 s. `await AcknowledgeCumulative(m.MessageId, txn)`. Even a: `Abort()`; odd a: `Commit()`.
4. `metrics = ParsePrometheus(await GetPrometheusMetrics())`.
   `commitLatency = metrics.Where(Name == "pulsar_txn_tp_commit_latency")`. `commitLatency.Count() > 0`.
   `commitLatency.Count(s => s.Tags["topic"].EndsWith(topicLocal) && s.Value > 0) > 0`.
   For each of `pulsar_txn_tp_aborted_count_total` and `pulsar_txn_tp_committed_count_total` with `Tags["subscription"] == subName && Tags["status"] == "succeed"`:
   `Tags["topic"].EndsWith(topicLocal)` and `Value > 0`. Here `topicLocal = "tnx/ns1/" + local name`; Java uses `endsWith(PENDING_ACK_REPLAY_TOPIC)`.
   Java `Metric.value` for histogram-like `commit_latency` series: match every sample whose base name is `pulsar_txn_tp_commit_latency`, including
   label variants, the same as Java's `parseMetrics` keying.

### A4 `#cumulativePendingAckReplayTest (L431-523)` → class `TransactionPendingAckReplayTests` (fixture §2.2)
Classification (c). Substituted: the runtime config (L434-435) → fixture, and the cursor reflection (L501-522) → pending ack internal stats with
`markDeletePosition == lastConfirmedEntry` (Java `compareTo == 0`).
```csharp
// Upstream: PendingAckPersistentTest#cumulativePendingAckReplayTest
[Fact]
public async Task AcknowledgeCumulative_GivenPendingAcksAndTopicUnload_ShouldReplayPendingAckState()
```
1. `messageCount = 1000`, sub `"cumulative-test"`. Producer batching on, `BatchingMaxMessages(200)`. Consumer `Failover`.
2. `abortTxn` 30 s. Send 1000 × `"Hello Pulsar!"` (awaited). For 1000 receives: record the id, then `await AcknowledgeCumulative(id, abortTxn)`.
3. `UnloadTopic`. `txn` 30 s. `WaitForReconnect`.
4. For every id: `ThrowAsync<TransactionConflictException>(AcknowledgeCumulative(id, txn))`.
5. `commitTxn` 30 s. `await abortTxn.Abort()`. For every id: `await AcknowledgeCumulative(id, commitTxn)`. `await commitTxn.Commit()`.
6. `UnloadTopic`. `WaitForReconnect`. For every id: `ThrowAsync<TransactionConflictException>(AcknowledgeCumulative(id, txn))`.
   This reuses `txn`, which may be past its 30 s timeout, as in Java (L494). If the client raises `InvalidTransactionStatusException` first because
   the client-side timer moved it to `TimedOut`, that differs from Java, whose client only checks state in `checkIfOpen`. See OD-18.
7. `Eventually(markDeletePosition == lastConfirmedEntry)` for cursor `"__pending_ack_state"`.

### A5 `#testDeleteSubThenDeletePendingAckManagedLedger (L525-550)`: Port. A6 `#testDeleteTopicThenDeletePendingAckManagedLedger (L552-585)`: Port
Both Java methods are `private` with `@Test` (OD-15). Neither performs a transactional operation; they assert there is no leftover pending-ack topic.
A5 `DeleteSubscription_GivenSubscription_ShouldNotLeavePendingAckTopic`: `topic = UniqueTopic("persistent://tnx/ns1/test-delete")`. Consumer
`Failover`, sub `"test-delete"`; dispose it. `DeleteSubscription`. `topics = GetNamespaceTopics("tnx/ns1")`. `GetTopicStats(topic)` (result unused, L545).
`topics.ShouldNotContain(topic + "-test-delete__transaction_pending_ack")`, `topics.ShouldContain(topic)`.
A6 `DeleteTopic_GivenSubscriptions_ShouldNotLeavePendingAckTopics`: consumer1 and consumer2 (both sub `"test-delete"`, Failover), each disposed.
`DeleteTopic(topic, force: false)`. `topics` contains neither pending-ack name (both identical, L582-583) nor `topic`.

### A7, A8, A11: Excluded (see table)

### A9 `#testTransactionConflictExceptionWhenAckBatchMessage (L809-874)` (Port)
`setDeletionAtBatchIndexLevelEnabled(true)` on the topic's ML config (L815-817) is what the broker derives from
`acknowledgmentAtBatchIndexLevelEnabled=true` (fixture §2.1).
```csharp
// Upstream: PendingAckPersistentTest#testTransactionConflictExceptionWhenAckBatchMessage
[Fact]
public async Task Acknowledge_GivenBatchIndexAckedInCommittedTransaction_ShouldThrowTransactionConflictException()
```
1. `topic = UniqueTopic("persistent://tnx/ns1/test")`, sub `"my-subscription-batch"`. Producer `String`, `EnableBatching(true)`, `BatchingMaxMessages(3)`,
   `BatchingMaxPublishDelay(10 s)`. Consumer `String`, `Exclusive`, `AckReceiptEnabled(true)`.
2. Start 3 sends `"my-message-0..2"`, then await all 3 (one batch).
3. `txn` 5 days (`TimeSpan.FromDays(5)`). `m1 = Receive()`, `m2 = Receive()`. Assert `m2.MessageId.BatchIndex == 1` (Java casts to `BatchMessageIdImpl`).
4. `await Acknowledge(m2.MessageId, txn)`. `txn2` 5 days. `await txn.Commit()`.
5. `await Should.ThrowAsync<TransactionConflictException>(Acknowledge(m2.MessageId, txn2))`.

### A10 `#testGetSubPatternTopicFilterTxnInternalTopic (L876-932)` (Port)
Namespace: Java's `tnx/ns1` is fresh per method. Ours is shared and holds many topics, so use `ns = UniqueNamespace("tnx/ns")` (§1.3) to keep the
pattern subscription's topic set the same as Java's.
1. `topic = $"persistent://{ns}/testGetSubPatternTopicFilterTxnInternalTopic"`, `CreatePartitionedTopic(topic, 3)`, sub `"sub"`.
   Producer `String`, `EnableBatching(false)`. Consumer `String`, `Shared`.
2. Send `"test"` 3 times.
3. 3 × (`txn` 5 s; `var a = Acknowledge((await consumer.Receive()).MessageId, txn).AsTask()`; `await txn.Commit()`). R-COMMIT-WAITS orders ack before commit.
4. Dispose the consumer. Each `GetNamespaceTopics(ns)` element is not a transaction-internal name: it does not end with `__transaction_pending_ack`,
   `__transaction_log_`, `__transaction_buffer_snapshot` or `__transaction_buffer_snapshot_segments`/`_indexes`
   (`SystemTopicNames.isTransactionInternalName`).
5. Pattern consumer `String`, sub `"patternSub"`, `Shared`, `TopicsPattern(new Regex($"persistent://{ns}/.*"))`.
6. Send `"test0".."test2"`. 3 × `m = Receive()`, then `Acknowledge(m.MessageId)` (non-txn, not joined). Dispose.

---

## 8. ConsumerRedeliveryTest (transaction methods) → `tests/DotPulsar.Tests/Transactions/ConsumerRedeliveryTests.cs`

Java: `java:client/api/ConsumerRedeliveryTest.java`, `@Test(groups = "broker-api")`, base `SharedPulsarBaseTest`. Its broker config
(`SharedPulsarCluster` L155-179) has dedup on, TC on, snapshot 2/2000 ms and 1 TC, which matches §2.1. Topics come from `newTopicName()` =
`persistent://<per-method namespace>/topic-<8 hex>`; C# uses `UniqueTopic("persistent://tnx/ns1/topic")`.

### R1 `#testSelectiveRedeliveryAfterConflictingTransactionalBatchAck (L72-139)`, DataProvider `conflictingBatchAckRedeliveryVariants` (L64-70), `timeOut = 60_000`
```csharp
// Upstream: ConsumerRedeliveryTest#testSelectiveRedeliveryAfterConflictingTransactionalBatchAck
[Theory]
[InlineData(SubscriptionType.Shared, false)]
[InlineData(SubscriptionType.Shared, true)]
[InlineData(SubscriptionType.KeyShared, false)]
[InlineData(SubscriptionType.KeyShared, true)]
public async Task RedeliverUnacknowledgedMessages_GivenConflictingTransactionalBatchAck_ShouldRedeliverUntouchedBatchIndexes(SubscriptionType subscriptionType, bool includePendingPosition)
```
1. 60 s token. A dedicated txn client (L78). Producer `Schema.Int32`, `EnableBatching(true)`, `BatcherType(BatcherType.KeyBased)`,
   `BatchingMaxMessages(2)`, `BatchingMaxPublishDelay(TimeSpan.FromHours(1))`.
   Consumer `Int32`, sub `"sub"`, `subscriptionType`, `EnableBatchIndexAcknowledgment(true)`, `AckReceiptEnabled(true)`.
   Java `acknowledgmentGroupTime(0)` needs nothing: DotPulsar sends acks immediately.
2. Send key `"key"`, values 0 and 1, both started, then await both (10 s). `first = ReceiveOrNull(10 s)`, `last = ReceiveOrNull(10 s)`, both not null. `unacked = [last]`.
3. If `includePendingPosition`: send key `"key"`, values 2 and 3 (awaited, 10 s). 2 × `ReceiveOrNull(10 s)`, not null, add to `unacked`.
4. `txn` 2 min, `conflicting` 2 min (each `Create` with a 10 s token).
5. `await Acknowledge(first.MessageId, txn)` (10 s). `ThrowAsync<TransactionConflictException>(Acknowledge(first.MessageId, conflicting))` (10 s). `await txn.Commit()` (10 s).
6. `requested = unacked.Select(m => m.MessageId).ToHashSet()`. `await consumer.RedeliverUnacknowledgedMessages(requested)`.
7. For `unacked.Count` times: `m = ReceiveOrNull(10 s)`, `ShouldNotBeNull("untouched batch indexes must remain selectively redeliverable")`,
   `redelivered.Add(m.MessageId)`, `await Acknowledge(m.MessageId)` (10 s).
8. `redelivered.ShouldBe(requested, ignoreOrder: true)`.
9. `finally`: `if (txn.CurrentState == TransactionState.Open) await txn.Abort()`. `await conflicting.Abort()` (10 s each).
Requires R-ACK-BATCH-TXN and batch-index redelivery; OD-10 covers the broker version.

### R2 `#testZeroCountSelectiveRedeliveryPreservesBackpressure (L141-233)`, DataProvider `individualRedeliverySubscriptionTypes` (L59-62): Excluded
It pauses `PendingAckHandleImpl.getInternalPinnedExecutor()` (L183-190) and asserts the broker `Consumer`'s `getPendingAcks`, `isBlocked`,
`getUnackedMessages` and `getAvailablePermits` (L192-222). It also uses the client-internal `ConsumerImpl.increaseAvailablePermits`.

---

## 9. Buffer, publish-order and auth classes

### 9.1 TopicTransactionBufferTest → `tests/DotPulsar.Tests/Transactions/TopicTransactionBufferTests.cs`
Java: `java:broker/transaction/buffer/TopicTransactionBufferTest.java`. There is no class-level `@Test`, so the methods are in the default group. `@BeforeMethod` (L89-109): 16 TCs,
waits for all 16 stores `Ready`.

**B5 `#testGetLastMessageIdsWithOngoingTransactions (L296-358)` (Port)**
```csharp
// Upstream: TopicTransactionBufferTest#testGetLastMessageIdsWithOngoingTransactions
[Fact]
public async Task GetLastMessageIds_GivenOngoingTransactions_ShouldNotAdvancePastOpenTransactions()
```
`AssertLast(expected)` = `actual = (await consumer.GetLastMessageIds()).First()`; `actual.EntryId == expected.EntryId && actual.LedgerId == expected.LedgerId` (L416-420).
1. `topic = UniqueTopic("persistent://tnx/ns1/testGetLastMessageIdsWithOngoingTransactions")`, sub `"my-subscription"`. Producer `ByteArray` (batching Java default). Consumer.
2. 3 × `expected = Send(empty)`. `AssertLast(expected)`.
3. `txn1` 5 h, `txn2` 5 h. Send empty with txn1. `expected1 = Send(empty)`. Send empty with txn2.
4. `AssertLast(expected)`. `await txn1.Commit()` (5 s). `AssertLast(expected1)`. `await txn2.Abort()` (5 s). `AssertLast(expected1)`.
5. `txn3` (default). Send with txn3. `AssertLast(expected1)`. `await txn3.Abort()` (5 s). `AssertLast(expected1)`.

**B6 `#testGetLastMessageIdsWithOpenTransactionAtLedgerHead (L367-396)`**: classification (c). `triggerLedgerSwitch` (L398-414) = admin unload
plus polling the broker ML state for `LedgerOpened` → `UnloadTopic(topic)` + `WaitForManagedLedgerState(topic, "LedgerOpened")` (internal stats `state`).
`reader.hasMessageAvailable()` → `HasMessageAvailable(reader, MessageId.Earliest, null)` (§2.5).
`HasMessageAvailable_GivenOpenTransactionAtLedgerHead_ShouldReturnTrue`:
1. Topic `UniqueTopic("persistent://tnx/ns1/testGetLastMessageIdsWithOpenTransactionAtLedgerHead")`, sub `"my-subscription"`. Producer `ByteArray` (Java defaults). Consumer.
2. 3 × `Send(UTF8(i.ToString()))` for i 0..2. Trigger the ledger switch.
3. `txn` 5 h. Send empty with txn.
4. Reader `ByteArray`, `StartMessageId(MessageId.Earliest)`. `(await HasMessageAvailable(reader, MessageId.Earliest, null)).ShouldBeTrue()`.

**B7 `#testWriteSnapshotWhenFirstTxnMessageSend (L429-490)`**: classification (c). `topicTransactionBuffer.getState()` → `GetTransactionBufferStats(topic).state`.
`Send_GivenFirstTransactionalMessage_ShouldMoveTransactionBufferFromNoSnapshotToReady`:
1. `topic = UniqueTopic("persistent://tnx/ns1/testWriteSnapshotWhenFirstTxnMessageSend")`, `CreateTopic`. Producer `String` (Java defaults). Consumer `String`, sub `"my-sub"`.
2. `Eventually(state == "NoSnapshot")`.
3. `Send("normal message")`. `state == "NoSnapshot"` (read once).
4. `txn` 5 h. `Send("transaction message")` with txn. `state == "Ready"` (read once, L466).
5. `await txn.Abort()` (5 s). `Send("normal message")`. `UnloadTopic`. `Eventually(state == "Ready")`.
6. `RedeliverUnacknowledgedMessages()`. 2 × `ReceiveOrNull(5 s)!.Value() == "normal message"`. `ReceiveOrNull(5 s).ShouldBeNull()`.

**B8 `#testMessagePublishInOrder (L496-537)`**: classification (c). Dropped: `setTransactionBufferProvider(new TransactionBufferTestProvider())` (L500-502, a broker test impl) and
`((TransactionImpl) txn).registerProducedTopic(topic)` (L523, client internal; R-ORDER makes it unnecessary). Precondition replaced: a fresh topic's first
transactional message makes the TB take its first snapshot (NoSnapshot → Ready), so messages sent during that window are held back. That is the
"sent before TB ready" phase Java simulates. Assertions unchanged.
`Commit_GivenTransactionalMessagesSentWhileBufferBecomesReady_ShouldDeliverInSendOrder`:
1. `topic = UniqueTopic("persistent://tnx/ns1/testMessagePublishInOrder")`, `CreateTopic`. Producer `Int32` (batching Java default). Consumer `Int32`, sub `"sub"`.
2. `txn` 5 h. For i in 0..49 and then 50..99: `sends.Add(producer.NewMessage().Transaction(txn).Send(i).AsTask())` (not awaited).
3. `await txn.Commit()` (R-COMMIT-WAITS).
4. For i in 0..99: `ReceiveOrNull(5 s)!.Value().ShouldBe(i)`.

### 9.2 TopicTransactionBufferPublishOrderTest → `tests/DotPulsar.Tests/Transactions/TopicTransactionBufferPublishOrderTests.cs`
**O1 `#testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt (L65-152)`**, DataProvider `recovering` = `{false}, {true}` (L60-63), `timeOut = 60_000`.
Classification (c).
- Dropped: the mocked `AbortedTxnProcessor` that holds recovery/first snapshot (L72-80); the provider override and the `addEntryInPublishOrder` latch
  (L81-97, L130-136); and `registerProducedTopic` (L126; R-ORDER).
- Precondition replaced (best effort, not deterministic):
  - `recovering = false`: a fresh topic, so the first transactional message triggers the first snapshot.
  - `recovering = true`: before the measured sends, a throwaway transaction sends and commits one message on the topic, then `UnloadTopic`
    makes the TB recover from its snapshot on reload. The measured sends start as soon as the producer reconnects.
  - The received list must still be exactly `["txn","plain"]`, which excludes the throwaway message: it uses a different subscription created after it.
- Kept: `SetNamespaceDeduplication` off (L69) → `SetTopicDeduplication(topic, false)` (scoped to our topic; Java scopes it to a per-method namespace),
  `txnSend id < plainSend id`, and the exact received list.
```csharp
// Upstream: TopicTransactionBufferPublishOrderTest#testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt
[Theory]
[InlineData(false)]
[InlineData(true)]
public async Task Send_GivenPlainMessageAfterTransactionalMessage_ShouldStoreInSendOrder(bool recovering)
```
1. 60 s token. Topic, dedup off. Txn client. Producer `String`, `EnableBatching(false)`. Consumer `String`, sub `"sub"` (after the recovery prep when `recovering`).
2. If not recovering: `Eventually(TB state is "NoSnapshot" or "Ready")` (Java L117-119 `checkIfTBRecoverCompletely`).
3. `txn` 1 min (10 s token).
4. `txnSend = …Transaction(txn).Send("txn").AsTask()`. `plainSend = Send("plain").AsTask()`.
5. `(await txnSend) < (await plainSend)` (10 s each, `MessageId` operator `<`). `await txn.Commit()` (10 s).
6. `received`: the first `ReceiveOrNull(10 s)`, then `ReceiveOrNull(1 s)` until null. `received.ShouldBe(["txn","plain"])`.

### 9.3 AuthenticatedTransactionProducerConsumerTest → `tests/DotPulsar.Tests/Transactions/AuthenticatedTransactionProducerConsumerTests.cs`
`[Collection("AuthenticatedTransaction"), Trait("Category","Integration")]`, fixture §2.3. Java `@BeforeMethod` (fresh broker per method). The TC state
does not carry across tests here because each test uses its own transaction.

DataProvider `actors` (L160-170) → for each of U1-U3:
```csharp
[Theory]
[InlineData("client", true)] [InlineData("client", false)]
[InlineData("client2", true)] [InlineData("client2", false)]
[InlineData("admin", true)] [InlineData("admin", false)]
```
Common arrange: `owner = CreateClient(true, AuthenticationFactory.Token(await CreateToken("client")))`,
`other = CreateClient(true, AuthenticationFactory.Token(await CreateToken(actor)))`, `topic = "tnx/ns1/txn-auth"` (Java `TOPIC`, short form, exact).
`otherTc = ((PulsarClient) other).TransactionCoordinatorClient!`. `txn = owner.NewTransaction().Timeout(60 s).Create()`.
`afterUnload` substitution: `pulsarServiceList.get(0).getTransactionMetadataStoreService().removeTransactionMetadataStore(TC id)` (L204-207 etc.) →
`await _fixture.UnloadNamespace("pulsar/system")` (super-user REST). Unloading the system namespace bundle removes the TC store from that broker;
the next TC request makes the client reconnect, and that reload is the path Java exercises. Expected exception for a non-owner, non-super-user:
the broker's `failedFutureTxnNotOwned` returns `CoordinatorException.TransactionNotFoundException` → `ServerError.TransactionNotFound`
(`java:pulsar-broker/src/main/java/.../ServerCnx.java#failedFutureTxnNotOwned (L4291-4298)`, used at L4215-4219, L4350-4355). Java maps it to
`TransactionCoordinatorClientException.TransactionNotFoundException` (`TransactionMetaStoreHandler#getExceptionByServerError` L708-719). In C#
that is `TransactionNotFoundException` (03 §4), which Java's assertion `instanceof TransactionCoordinatorClientException` covers.

**U1 `#testEndTxn (L172-224)`** `Commit_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser(string actor, bool afterUnload)`:
1. Owner consumer `String`, sub `"test"`, topic. Owner producer `String`, `SendTimeout(60 s)` (batching Java default).
2. `Send("beforetxn")`. `m = ReceiveOrNull(5 s)`. `var a = owner consumer.Acknowledge(m!.MessageId, txn).AsTask()` (not joined). `Send("message")` with txn.
3. If `afterUnload`: unload `pulsar/system`.
4. `ex = await Record.ExceptionAsync(() => otherTc.Commit(txn.Id, ct).AsTask())`.
5. actor `"client"` or `"admin"`: `ex.ShouldBeNull()`. `ReceiveOrNull(5 s)!.Value() == "message"`.
   Otherwise: `ex.ShouldBeOfType<TransactionNotFoundException>()`. `ReceiveOrNull(5 s).ShouldBeNull()`. `await txn.Commit()`. `ReceiveOrNull(5 s)!.Value() == "message"`.

**U2 `#testAddPartitionToTxn (L226-264)`**: classification (c). `txnMeta.producedPartitions()` (broker `TxnMeta`) → REST `transactionMetadata.producedPartitions` (map keys).
1. `txn` as above. If `afterUnload`, unload `pulsar/system`.
2. `ex = Record.ExceptionAsync(otherTc.AddPublishPartitionToTxn(txn.Id, "tnx/ns1/txn-auth", ct))`.
3. `meta = (await GetTransactionMetadata(txn.Id)).Json!.Value` (super-user).
4. client/admin: `ex` null and `meta.producedPartitions` keys `== ["tnx/ns1/txn-auth"]`. Otherwise: `ex` is `TransactionNotFoundException` and `producedPartitions` is empty.

**U3 `#testAddSubscriptionToTxn (L266-303)`**: the same with `otherTc.AddSubscriptionToTxn(txn.Id, "tnx/ns1/txn-auth", "sub", ct)`.
client/admin: `ackedPartitions` has exactly 1 entry. Otherwise: the exception type as above and `ackedPartitions` is empty.

**U4 `#testNoAuth (L305-317)`**: classification (c). Java fails inside `build()` because `enableTransaction(true)` starts the TC client synchronously.
In DotPulsar the TC client starts lazily on the first `Create` (02-architecture L96-98), so the same failure appears there. Assertion kept:
`AuthenticationException` somewhere in the chain.
`Create_GivenClientWithoutAuthentication_ShouldThrowAuthenticationException`:
1. `client = CreateClient(enableTransaction: true, authentication: null)`.
2. `ex = await Record.ExceptionAsync(() => client.NewTransaction().Create(ct).AsTask())`. `ex.ShouldNotBeNull()`.
   Walk `ex` and its `InnerException`s: one is `DotPulsar.Exceptions.AuthenticationException` (Java `Exceptions.areExceptionsPresentInChain`).

### 9.4 TransactionAndSchemaAuthZTest: Excluded
`java:broker/admin/TransactionAndSchemaAuthZTest.java#testSchemaAndTransactionAuthorization (L262-316)` with DataProvider `authFunction` (L107-260, 24 rows,
including duplicate rows L234-243 and L244-253) checks the broker admin-REST authorization of schema and `/admin/v3/transactions/*` endpoints through
`PulsarAdmin` and an injected broker authorization checker (`setAuthorizationTopicOperationChecker`, L290-298). DotPulsar has no admin client.
The only client-side work, in the DataProvider (super-user produce + transactional ack, L111-127), is already covered by T24/T27.

---

## 10. Excluded-but-replaceable by unit tests (recommendation for `tests/DotPulsar.Tests/Internal/Transactions/TransactionTests.cs`, `[Trait("Category","Unit")]`)

| Java | Recommended unit test (NSubstitute `ITransactionCoordinatorClient`) |
|---|---|
| T16 `#testEndTxnWhenCommittingOrAborting` (L1060-1088) | `Commit_GivenStateCommitting_ShouldSendEndTxnCommit` and `Abort_GivenStateAborting_ShouldSendEndTxnAbort`, if the state machine allows retrying an end call in those states (Java `TransactionImpl#commit/abort` `checkState(OPEN, COMMITTING)`). Otherwise record it as a known difference. |
| T18 `#testRetryExceptionOfEndTxn` (L1124-1161) | A TC handler test: the end-txn response `ServiceNotReady`/TC-not-ready is retried until `Ready`. It belongs with the TC handler unit tests. |
| T19 `#testCancelTxnTimeout` (L1163-1185) | `Commit_GivenOpenTransaction_ShouldDisposeTimeoutTimer`, `Abort_…`: after commit/abort, advancing a fake clock past the timeout does not move the state to `TimedOut`. |
| P2/P3 txn-id metadata (L207-209) | A producer/serializer unit test: a message sent with a transaction has `MessageMetadata.TxnidMostBits/TxnidLeastBits` equal to `Id`. |

---

## 11. Open decisions (recommendation)

- **OD-1 API naming.** Resolved by the [03-public-api](../03-public-api.md): `Create(ct)`, `CurrentState` (sync), `State` (awaitable `IState`). No action needed.
- **OD-2 Java default batching.** Java producers batch by default, and DotPulsar's new `EnableBatching` default is probably `false`. *Recommendation:*
  the translation always writes `.EnableBatching(true)` where Java relies on the default (§1.4). This keeps entry/batch layout identical, e.g. T27 and B5.
- **OD-3 `SendTimeout(TimeSpan.Zero)`** means "disabled", as Java `0`. *Recommendation:* 06/send-timeout documents it.
- **OD-4 Shared fixture vs per-method broker.** *Recommendation:* use unique topic/namespace names (§1.3) and topic-scoped policies instead of
  broker-wide runtime config. Never delete `tnx/ns1`.
- **OD-5 Non-dynamic broker configs.** T22 and A4 need `transactionPendingAckLogIndexMinLag`/`managedLedgerDefaultMarkDeleteRateLimit` values. Two extra
  containers cost about 30-60 s each. *Recommendation:* add the two derived fixtures (§2.2). The alternative is to exclude T22 and A4's final assertion.
- **OD-6 TC client hook.** U1-U3 need commit/add-partition/add-subscription by `TransactionId` from another client. They use
  `((PulsarClient) c).TransactionCoordinatorClient!` (03 §3.8). The method names must match 05/transaction-coordinator-client.
- **OD-7 Transaction timeout for the C1/C2 substitution.** Java's fabricated txn has no client timeout. *Recommendation:* 5 min.
- **OD-8 Fire-and-forget Java futures** → `Observe` (§1.8). The C# tests never add assertions on them.
- **OD-9 R-ORDER.** Java needs `registerProducedTopic` before sending (PublishOrderTest L124-126) because its plain message can overtake a
  transactional one that waits for registration. DotPulsar assigns sequence ids at `Send`, so it must keep send order anyway. *Recommendation:* make R-ORDER
  a producer-integration requirement. B8 and O1 depend on it.
- **OD-10 Broker image.** Several upstream tests target fixes on Pulsar master (R1, O1, P1's broker side, B5/B6/T37 last-message-id semantics).
  `apachepulsar/pulsar:4.0.9` may not have them. *Recommendation:* make the image tag a fixture constant. The implementer runs each ported test.
  A test that fails only because the broker lacks a fix gets `[Fact(Skip = "Requires Pulsar broker fix not in 4.0.9")]` with the upstream PR noted, and the
  image is not bumped silently.
- **OD-11 Chunk message id.** Java returns `ChunkMessageIdImpl`, and DotPulsar `MessageId` is `sealed`. *Recommendation:* `MessageId.FirstChunkMessageId`
  (nullable) on the id returned by `Send` and on received chunked messages, with `Send` returning the last chunk's id. This belongs to 06/chunking.
- **OD-12 R-SEND-ERROR.** Today a `SendError` faults the whole sub-producer (the spec decisions hazards). T36 expects a `NotAllowedException` from `Send`.
  *Recommendation:* map a per-message `CommandSendError` with a non-retryable `ServerError` to that send's exception.
- **OD-13 Transactional ack while disconnected** (T26). Java fails fast with `ConnectException`. *Recommendation:* DotPulsar waits for reconnect and honours
  cancellation, consistent with `Send`. T26 stays excluded.
- **OD-14 `invocationCount = 3`** (C5). *Recommendation:* an extra `int invocation` theory parameter (1..3) per DataProvider value.
- **OD-15 Private `@Test` methods** (A5, A6). TestNG may not run them. *Recommendation:* port them anyway. They are cheap and do not contradict anything.
- **OD-16 Key file.** Copy `public-key.client-rsa.pem` (and `private-key.client-rsa.pem` for possible consume-side tests) from the pinned
  `pulsar-broker/src/test/resources/certificate/` into `tests/DotPulsar.Tests/Resources/certificate/`, with an Apache header note in the folder README.
- **OD-17 Concurrent `Receive`** (T5). 30 tasks call `consumer.Receive` at the same time. *Recommendation:* confirm `IReceive.Receive` is safe for
  concurrent callers. If not, the workers share a `SemaphoreSlim(1)` around `Receive` only, so the transactional send/ack calls stay concurrent.
- **OD-18 Ack with a client-timed-out transaction** (A4 step 6). Java's client still sends the ack and the broker answers. If DotPulsar checks
  `CurrentState == Open` first and throws `InvalidTransactionStatusException`, A4 step 6 must give `txn` a timeout above the test runtime. That changes
  a Java value (30 s), so *recommendation:* keep 30 s and run the test. If it fails only because of the timeout, raise `txn`'s timeout to 5 min and note it.
