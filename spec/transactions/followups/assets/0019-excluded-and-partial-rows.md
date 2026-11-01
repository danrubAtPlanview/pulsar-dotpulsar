# Followup 0019, appendix: the test rows that are excluded or ported with fewer assertions

Generated from `upstream-map.yaml` on 2026-11-01 (the end of WP10). The map is the source of truth; this file is a reading aid for the reviewer. `Excluded` rows have no C# test; `Port-partial` rows have a C# test that checks less than the Java test, or checks it by another route (the part in brackets of the status says how); `Port-unit` rows are covered by client unit tests because the Java test needs a mocked or in-process broker.

Counts: 335 rows, 81 excluded, 87 port-partial, 46 port-unit.

## Excluded rows (81), by Java test class

| Java class | Rows |
|---|---|
| `TransactionTest` | 19 |
| `RawBatchMessageContainerImplTest` | 10 |
| `DeadLetterTopicTest` | 7 |
| `BatchMessageContainerImplTest` | 6 |
| `TopicTransactionBufferTest` | 6 |
| `NegativeAcksTest` | 4 |
| `PendingAckPersistentTest` | 4 |
| `MessageChunkingTest` | 3 |
| `BatchMessageIdImplTest` | 2 |
| `ProducerImplTest` | 2 |
| `TransactionCoordinatorClientTest` | 2 |
| `TransactionMetaStoreAssignmentTest` | 2 |
| `UnAckedTopicMessageRedeliveryTrackerTest` | 2 |
| `AcknowledgementsGroupingTrackerTest` | 1 |
| `BatchMessageWithBatchIndexLevelTest` | 1 |
| `ChunkMessageIdImplTest` | 1 |
| `ConsumerRedeliveryTest` | 1 |
| `SimpleProducerConsumerTest` | 1 |
| `TransactionAndSchemaAuthZTest` | 1 |
| `TransactionConsumeTest` | 1 |
| `TransactionEndToEndTest` | 1 |
| `TransactionProduceTest` | 1 |
| `WatchTcAssignmentsDiscoveryTest` | 1 |
| `cpp:EncryptionTest.cc` | 1 |
| `cpp:MessageChunkingTest.cc` | 1 |

### Every excluded row with its reason

| Upstream test | Reason |
|---|---|
| `AcknowledgementsGroupingTrackerTest#` | Excluded: class-level; the time-based acknowledgment grouping and flush do not exist in DotPulsar |
| `BatchMessageContainerImplTest#testEntryBucketHashRangeIsStampedWhenCreatingSendOperation` | PIP-486 entry-bucket hash range (entry_hash_min/max), part of the v5 scalable-topic producer; out of scope (08-tests/tests-client.md A3.4) |
| `BatchMessageContainerImplTest#testRecoveryAfterEncryptionFailure` | the managed batch container does not encrypt (encryption happens in ProducerChannel.Send), so the Java test has no direct counterpart; the recovery property (a batch whose encryption failed is left as it was and is written as a valid frame when it is sent again) is covered by BatchSendThroughProd... |
| `BatchMessageContainerImplTest#testEntryBucketPartialBuildFailureReleasesBuiltOps` | EntryBucketBatchContainer is the v5 scalable-topic batcher (PIP-486); out of scope (08-tests/tests-client.md A3.9) |
| `BatchMessageContainerImplTest#testEntryBucketContainerKeepsPlainMessagesOutOfTransactionalBatches` | PIP-486 / v5 entry-bucket batch container; out of scope (08-tests/tests-client.md A3.19) |
| `BatchMessageContainerImplTest#testEntryBucketContainerBatchesMessagesOfOneTransactionTogether` | PIP-486 / v5 entry-bucket batch container; out of scope (08-tests/tests-client.md A3.20) |
| `BatchMessageContainerImplTest#testEntryBucketContainerDropsTheTransactionIdentityOfAFailedFirstAdd` | PIP-486 / v5 entry-bucket batch container; out of scope (08-tests/tests-client.md A3.21) |
| `BatchMessageIdImplTest#notEqualsMultiTest` | Excluded: no MultiMessageIdImpl in DotPulsar |
| `BatchMessageIdImplTest#serializeAndDeserializeTest` | Excluded: MessageId has no ToByteArray/FromByteArray; replaced by MessageIdTests#BatchSize_ShouldNotTakePartInEqualityOrHashCode |
| `BatchMessageWithBatchIndexLevelTest#testUnAckMessagesWhenConcurrentDeliveryAndAck` | it replaces the broker's service Consumer in the dispatcher with a Mockito spy that holds back sendMessages until a signal, and then deletes a position with an ack set from the ManagedCursor directly; the race between the delivery of the replayed entries and an in-flight acknowledgment inside the... |
| `ChunkMessageIdImplTest#serializeAndDeserializeTest` | Excluded: MessageId has no ToByteArray/FromByteArray |
| `ConsumerRedeliveryTest#testZeroCountSelectiveRedeliveryPreservesBackpressure ×2` | Excluded: pauses the broker `PendingAckHandleImpl` pinned executor; asserts broker `Consumer` internals |
| `DeadLetterTopicTest#testAutoConsumeSchemaDeadLetter` | Excluded: AUTO_PRODUCE_BYTES and AUTO_CONSUME are not built |
| `DeadLetterTopicTest#testDeadLetterTopicWithMultiTopic` | Excluded: @Test(enabled = false) upstream (issue #2647) |
| `DeadLetterTopicTest#testDeadLetterTopicWithProducerBuilder` | Excluded: enableRetry, reconsumeLater and the producer customizer are not built (OD-1, OD-2) |
| `DeadLetterTopicTest#testDeadLetterPolicyDeserialize` | Excluded: loadConf (configuration deserialization) |
| `DeadLetterTopicTest#testCloseDeadLetterTopicProducerOnExceptionToPreventProducerLeak` | Excluded: AVRO schemas and broker internals; covered by DeadLetterHandlerTests#TryDeadLetter_GivenFaultedProducer_ShouldDisposeAndRecreateProducerOnNextCall |
| `DeadLetterTopicTest#sendDeadLetterTopicWithMismatchSchemaProducer` | Excluded: AVRO(Long)/AUTO_CONSUME and a retry topic |
| `DeadLetterTopicTest#testCheckUnnecessaryGetPartitionedTopicMetadataWhenUseRetryAndDQL` | Excluded: retry topic and Mockito on client internals |
| `MessageChunkingTest#testPublishWithFailure` | stale exclusion: the send timeout it waited for exists now (SendTimeout option and tests) |
| `MessageChunkingTest#testNonPersistentMessageOverLimitOnlyWithMetadataIsRejectedLocally` | stale exclusion: an equivalent test exists (MessageChunkingSmallMessageSizeTests), its Upstream comment names another Java method |
| `MessageChunkingTest#testChunksEnqueueFailed` | Excluded: memory limit controller and ProducerQueueIsFullError are not built |
| `NegativeAcksTest#testNegativeAcksWithBackoff` | WP11-B added IRedeliveryBackoff and MultiplierRedeliveryBackoff for the ack timeout; a negative-ack option and the use of the policy in the negative-ack tracker are still missing (followup 0019) |
| `NegativeAcksTest#testFailoverConsumerBatchCumulateAck` | stale exclusion: the spec defers it to ack-timeout/batching specs that do not list it; no owner and no C# test |
| `NegativeAcksTest#testMultiTopicConsumerConcurrentRedeliverAndReceive` | stale exclusion: the spec defers it to ack-timeout/batching specs that do not list it; no owner and no C# test |
| `NegativeAcksTest#testConfigureNegativeAckPrecisionBitCnt` | Excluded: the precision is not configurable (OD-3) |
| `PendingAckPersistentTest#testBuildConsumerEncounterPendingAckInitFailure ×3` | Excluded: Mockito replaces `transactionPendingAckStoreProvider` |
| `PendingAckPersistentTest#testDeleteUselessLogDataWhenSubCursorMoved` | Excluded: `groups = \"quarantine\"`; reflection on `MLPendingAckStore.pendingAckLogIndex` / `maxIndexLag` |
| `PendingAckPersistentTest#testPendingAckLowWaterMarkRemoveFirstTxn` | Excluded: re-opens committed client transactions via reflection; reflection on `PendingAckHandleImpl` |
| `PendingAckPersistentTest#testGetManagedLegerConfigFailThenUnload` | Excluded: spies on `BrokerService.getManagedLedgerConfig` |
| `ProducerImplTest#testSendTimeoutDuringInFlightWriteDefersCmdRelease` | intent covered by SubProducerSendTimeoutBatchTests#SendTimeout_GivenTimeoutWhileBatchIsBeingWritten_ShouldReturnBufferOnlyAfterTheWrite (no Upstream comment) |
| `ProducerImplTest#testSendTimeoutReleasesCmdInlineWhenNoWriteInFlight` | intent covered by SubProducerSendTimeoutBatchTests#SendTimeout_GivenFrozenBatch_ShouldFailAllMessagesOfTheBatchAndReturnBuffer (no Upstream comment) |
| `RawBatchMessageContainerImplTest#testToByteBufWithBatchLimit` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufWithCompressionAndEncryption` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufReleasesTheOutputBufferWhenSerializationFails` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufWithSingleMessage` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testAddDifferentBatchMessage` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testCreateOpSendMsg` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufWithEncryptionWithoutCryptoKeyReader` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufWithEncryptionWithInvalidEncryptKeys` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufReleasesPayloadWhenEncryptionFailsUnexpectedly` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `RawBatchMessageContainerImplTest#testToByteBufReleasesPayloadAndDiscardsWhenEncryptionFailsWithClientException` | Excluded: the broker compaction container RawBatchMessageContainerImpl; DotPulsar has no raw batch container |
| `SimpleProducerConsumerTest#testRedeliveryOfFailedMessages` | Excluded: groups = quarantine upstream |
| `TopicTransactionBufferTest#testTransactionBufferAppendMarkerWriteFailState` | Excluded: writes `ManagedLedgerImpl.state` via reflection; OTel |
| `TopicTransactionBufferTest#testCheckDeduplicationFailedWhenCreatePersistentTopic` | Excluded: spies on `BrokerService.newTopic` |
| `TopicTransactionBufferTest#testCloseTransactionBufferWhenTimeout` | Excluded: spies on `BrokerService.newTopic` (sleep injection) |
| `TopicTransactionBufferTest#testGetMaxPositionAfterTBReady` | Excluded: mocked `TransactionBufferProvider` |
| `TopicTransactionBufferTest#testRefCountWhenAppendBufferToTxn` | Excluded: calls `TransactionBufferTestImpl.appendBufferToTxn` with Netty `ByteBuf`s |
| `TopicTransactionBufferTest#testAppendBufferToTxnWithInMemTransactionBuffer` | Excluded: `InMemTransactionBuffer` direct call |
| `TransactionAndSchemaAuthZTest#testSchemaAndTransactionAuthorization ×24` | Excluded: tests broker admin-REST authorization of schema/transaction admin endpoints with the Java admin client and an injected broker `AuthorizationService` checker. No DotPulsar client surface. |
| `TransactionConsumeTest#testMessageRedelivery` | Excluded: reflection on `PersistentDispatcherMultipleConsumers.redeliveryMessages` |
| `TransactionCoordinatorClientTest#testTransactionCoordinatorExceptionUnwrap` | Tests the unwrapping of the Java ExecutionException (TransactionCoordinatorClientException.unwrap); await surfaces the original exception in .NET (08-tests/tests-client.md B3.4) |
| `TransactionCoordinatorClientTest#testClientStartWithRetry` | the reason still holds (the service URL is a single URL) |
| `TransactionEndToEndTest#txnTransactionRedeliverNullDispatcher` | Broker internals only: the test sets the private field PersistentSubscription.dispatcher to null by reflection on the in-process broker; a client cannot put a real broker into that state (08-tests/tests-e2e.md row 21) |
| `TransactionMetaStoreAssignmentTest#testTransactionMetaStoreAssignAndFailover` | Excluded: needs a 3-broker cluster and a broker crash |
| `TransactionMetaStoreAssignmentTest#testTransactionMetaStoreUnload` | Excluded: broker internals; the coordinator unload is covered by TransactionMetaStoreHandlerTests as additional coverage |
| `TransactionProduceTest#testSkipMaxReadPositionTimestampWithoutReplicatedSubscriptions` | Excluded: `PersistentTopic.getLastMaxReadPositionMovedForwardTimestamp()` has no external view |
| `TransactionTest#testCanDeleteNamespaceWhenEnableTxnSegmentedSnapshot` | Excluded: mutates broker config at runtime (`transactionBufferSegmentedSnapshotEnabled`); no transaction client API involved |
| `TransactionTest#testSubscriptionRecreateTopic` | Excluded: reflection on `ManagedLedgerFactoryImpl.ledgers`, `MLPendingAckStoreProvider` |
| `TransactionTest#testPendingAckStoreEntriesArentPulsarMessages` | Excluded: asserts `ManagedLedgerConfig.isPulsarMessageEntries` |
| `TransactionTest#testTakeSnapshotBeforeBuildTxnProducer` | Excluded: decodes broker-internal `TransactionBufferSnapshot` Avro records from a system topic |
| `TransactionTest#testAppendBufferWithNotManageLedgerExceptionCanCastToMLE` | Excluded: closes a managed ledger and calls `PersistentTopic.publishTxnMessage` directly |
| `TransactionTest#testEndTBRecoveringWhenManagerLedgerDisReadable` | Excluded: mocked `ManagedCursorImpl` injected into the managed ledger |
| `TransactionTest#testEndTPRecoveringWhenManagerLedgerDisReadable` | Excluded: mocked pending ack store provider |
| `TransactionTest#testEndTCRecoveringWhenManagerLedgerDisReadable` | Excluded: constructs `MLTransactionMetadataStore` with mocks |
| `TransactionTest#testEndTxnWhenCommittingOrAborting` | replaced by the unit test Internal/Transactions/TransactionTests#Commit_GivenCommittingState_ShouldSendEndTxnAgain |
| `TransactionTest#testNoEntryCanBeReadWhenRecovery` | Excluded: spies on `ManagedLedgerImpl` |
| `TransactionTest#testRetryExceptionOfEndTxn` | Excluded: reflection sets TC store state |
| `TransactionTest#testCancelTxnTimeout` | replaced by the unit test Internal/Transactions/TransactionTests#Commit_ShouldCancelTimer |
| `TransactionTest#testNotChangeMaxReadPositionCountWhenCheckIfNoSnapshot` | Excluded: reflection on `TopicTransactionBuffer` |
| `TransactionTest#testAutoCreateSchemaForTransactionSnapshot` | Excluded: runtime broker config (`allowAutoUpdateSchemaEnabled`), system-topic schemas only |
| `TransactionTest#testConsistencyOfTransactionStatsAtEndTxn` | Excluded: closes the TC log cursor via reflection |
| `TransactionTest#testGetConnectExceptionForAckMsgWhenCnxIsNull` | Excluded: invokes the private `connectionClosed` via reflection; DotPulsar has different reconnect semantics (OD-13) |
| `TransactionTest#testPendingAckReplayChangeStateError` | Excluded: all broker objects mocked |
| `TransactionTest#testTBRecoverChangeStateError` | Excluded: all broker objects mocked |
| `TransactionTest#testTBSnapshotWriter` | Excluded: injects failed writer futures into `SystemTopicTxnBufferSnapshotService` |
| `UnAckedTopicMessageRedeliveryTrackerTest#testRemoveTopicMessages` | decided in WP11-B: the multi-topic tracker of Java only needs removeTopicMessages when a topic goes away; here the tracker of a partition goes away with its sub consumer |
| `UnAckedTopicMessageRedeliveryTrackerTest#testRemoveTopicMessagesDoesNotMatchPrefixTopic` | decided in WP11-B, see testRemoveTopicMessages |
| `WatchTcAssignmentsDiscoveryTest#testUnsupportedBrokerFailsWithoutRetrying` | Excluded: v5 metadata-store discovery (WatchTcAssignments); DotPulsar has no such type |
| `cpp:EncryptionTest.cc#testDecryptionSuccess/Failure` | Excluded: C++-only; covered by the CLI interop tests EncryptionInteropTests#Receive_GivenJavaProduced* |
| `cpp:MessageChunkingTest.cc#ChunkMessageIdTest.testSetChunkMessageId` | Excluded: C++ serialization API |

## Port-partial rows (87)

| Upstream test | How it is ported (status) | C# test |
|---|---|---|
| `TransactionEndToEndTest#produceAbortTest` | broker cursor check -> admin internal stats | `Abort_GivenProducedMessages_ShouldNotDeliverAndShouldMoveMarkDeleteToEnd` |
| `TransactionEndToEndTest#testAckWithTransactionReduceUnAckMessageCount` | broker `Consumer#getUnackedMessages` -> admin stats) (skipped in NOBIA | `Commit_GivenHalfOfMessagesAckedWithTransaction_ShouldReduceUnackedMessageCount` |
| `TransactionEndToEndTest#txnIndividualAckTestNoBatchAndSharedSub` | reflection re-commit -> TC client commit | `Acknowledge_GivenNoBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| `TransactionEndToEndTest#txnIndividualAckTestBatchAndSharedSub` | same | `Acknowledge_GivenBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| `TransactionEndToEndTest#txnIndividualAckTestNoBatchAndFailoverSub` | same | `Acknowledge_GivenNoBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| `TransactionEndToEndTest#txnIndividualAckTestBatchAndFailoverSub` | same) (NOBIA override: skipped, see §3 | `Acknowledge_GivenBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| `TransactionEndToEndTest#txnMessageAckTest` | broker cursor check -> admin internal stats | `Acknowledge_GivenCommittedTransactionalMessages_ShouldAckAndMoveMarkDeleteToEnd` |
| `TransactionEndToEndTest#txnAckTestBatchAndCumulativeSub` | reflection re-commit -> TC client commit | `AcknowledgeCumulative_GivenBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit` |
| `TransactionEndToEndTest#txnAckTestNoBatchAndCumulativeSub` | same | `AcknowledgeCumulative_GivenNoBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit` |
| `TransactionEndToEndTest#produceAndConsumeCloseStateTxnTest` | TC store lookup -> admin `transactionMetadata`; reflection ctor -> public internal ctor | `SendAndAcknowledge_GivenEndedOrTimedOutTransaction_ShouldThrow` |
| `TransactionEndToEndTest#testTxnTimeoutAtTransactionMetadataStore` | broker TC service -> client `Create` + admin + `/metrics` | `Create_GivenTimeoutElapsesAtCoordinator_ShouldIncrementCoordinatorTimeoutCount` |
| `TransactionEndToEndTest#oneTransactionOneTopicWithMultiSubTest` | TC store -> admin `transactionMetadata.ackedPartitions` | `Acknowledge_GivenOneTransactionAndTwoSubscriptions_ShouldRegisterBothSubscriptions` |
| `TransactionEndToEndTest#testSendTxnMessageTimeout` | mocked `ClientCnx` -> black-hole TCP relay | `Send_GivenTransactionalMessageWithoutReceipt_ShouldThrowOperationTimeoutException` |
| `TransactionEndToEndTest#testAckWithTransactionReduceUnackCountNotInPendingAcks` | broker consumer -> admin stats | `Acknowledge_GivenOneBatchIndexAckedWithTransaction_ShouldReduceUnackedCountByOne` |
| `TransactionEndToEndTest#testSendTxnAckMessageToDLQ` | client permit counter dropped -> unit test | `Abort_GivenMessageExceedsMaxRedeliverCount_ShouldSendMessageToDeadLetterTopic` |
| `TransactionEndToEndTest#testSendTxnAckBatchMessageToDLQ` | same | `Abort_GivenBatchMessageExceedsMaxRedeliverCount_ShouldSendBatchToDeadLetterTopic` |
| `TransactionTest#testTopicTransactionMetrics` | OTel dropped | `Commit_GivenCommittedAbortedAndOngoingTransactions_ShouldReportTransactionCountsInTopicStats` |
| `TransactionTest#testCreateTransactionSystemTopic` | namespace delete/recreate → fresh namespace | `Subscribe_GivenTransactionPendingAckSystemTopic_ShouldThrowNotAllowedException` |
| `TransactionTest#brokerNotInitTxnManagedLedgerTopic` | broker topic-map asserts dropped | `Create_GivenSystemNamespaceUnloaded_ShouldCreateTransaction` |
| `TransactionTest#testAsyncSendOrAckForSingleFuture` | client `opCount` reflection dropped; broker config → topic policy | `Commit_GivenConcurrentTransactionalSendsAndAcks_ShouldCompleteAllOperations` |
| `TransactionTest#testSendAndAckWithSchema` | per-message schema → String producer, 03 §1 | `Send_GivenStringSchemaProducerAndTransaction_ShouldDeliverValueWithSchemaVersion` |
| `TransactionTest#testMaxReadPositionForNormalPublish` | last reflection step dropped | `Send_GivenNormalAndTransactionalMessages_ShouldMoveMaxReadPositionOnlyWhenNoOpenTransaction` |
| `TransactionTest#testPendingAckMarkDeletePosition` | runtime config → fixture config | `Acknowledge_GivenCommittedTransactionalAck_ShouldMarkDeletePendingAckLogUpToUncommittedEntries` |
| `TransactionTest#testGetTxnState` | two client/broker internals substituted | `CurrentState_GivenTransactionLifecycle_ShouldReportEachState` |
| `TransactionTest#testPersistentTopicGetLastDispatchablePositionWithTxn` | `getLastDispatchablePosition` → `GetLastMessageIds` | `GetLastMessageIds_GivenOpenAndAbortedTransactions_ShouldReturnLastNonTransactionalMessage` |
| `TransactionProduceTest#produceAndCommitTest` | ReadOnlyCursor → internal stats + consumer | `Commit_GivenTransactionalMessagesOnPartitionedTopic_ShouldWriteCommitMarkerAndDeliverAllMessages` |
| `TransactionProduceTest#produceAndAbortTest` | Port-partial | `Abort_GivenTransactionalMessagesOnPartitionedTopic_ShouldWriteAbortMarkerAndDeliverNoMessages` |
| `TransactionProduceTest#ackCommitTest` | pending ack count via REST | `Commit_GivenTransactionalAcks_ShouldClearPendingAcksAndNotRedeliver` |
| `TransactionProduceTest#ackAbortTest` | Port-partial | `Abort_GivenTransactionalAcks_ShouldClearPendingAcksAndRedeliver` |
| `TransactionProduceTest#testCommitFailure` | `getTxnMeta` → admin transactionMetadata 404 | `Commit_GivenFailedTransactionalSend_ShouldThrowTransactionHasOperationFailedExceptionAndAbort` |
| `TransactionConsumeTest#noSortedTest` | raw broker append → real transaction | `Receive_GivenOpenTransactionBetweenNormalMessages_ShouldHoldBackLaterMessagesUntilCommit` |
| `TransactionConsumeTest#sortedTest` | same substitution | `Receive_GivenCommittedTransactionBetweenNormalMessages_ShouldDeliverInSendOrder` |
| `TransactionConsumeTest#testFirstTnxBufferSnapshotAndRecoveryConcurrently ×2 dp ×3 invocations` | TB state via REST | `Send_GivenConcurrentFirstTransactionalSendsFromManyClients_ShouldDeliverOnlyCommitted` |
| `PendingAckPersistentTest#individualPendingAckReplayTest` | cursor reflection → REST | `Acknowledge_GivenIndividualPendingAcksAndTopicUnload_ShouldReplayPendingAckState` |
| `PendingAckPersistentTest#testPendingAckMetrics` | OTel dropped | `AcknowledgeCumulative_GivenCommittedAndAbortedTransactions_ShouldReportPendingAckMetrics` |
| `PendingAckPersistentTest#cumulativePendingAckReplayTest` | cursor reflection → REST; runtime config → fixture | `AcknowledgeCumulative_GivenPendingAcksAndTopicUnload_ShouldReplayPendingAckState` |
| `TopicTransactionBufferTest#testGetLastMessageIdsWithOpenTransactionAtLedgerHead` | ML state via REST | `HasMessageAvailable_GivenOpenTransactionAtLedgerHead_ShouldReturnTrue` |
| `TopicTransactionBufferTest#testWriteSnapshotWhenFirstTxnMessageSend` | TB state via REST | `Send_GivenFirstTransactionalMessage_ShouldMoveTransactionBufferFromNoSnapshotToReady` |
| `TopicTransactionBufferTest#testMessagePublishInOrder` | test TB provider + `registerProducedTopic` dropped | `Commit_GivenTransactionalMessagesSentWhileBufferBecomesReady_ShouldDeliverInSendOrder` |
| `TopicTransactionBufferPublishOrderTest#testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt ×2` | mocked `AbortedTxnProcessor` hold → natural first-snapshot / recovery window | `Send_GivenPlainMessageAfterTransactionalMessage_ShouldStoreInSendOrder` |
| `AuthenticatedTransactionProducerConsumerTest#testEndTxn ×6` | `removeTransactionMetadataStore` → unload `pulsar/system` | `Commit_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser` |
| `AuthenticatedTransactionProducerConsumerTest#testAddPartitionToTxn ×6` | `getTxnMeta` → REST | `AddPublishPartitionToTxn_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser` |
| `AuthenticatedTransactionProducerConsumerTest#testAddSubscriptionToTxn ×6` | Port-partial | `AddSubscriptionToTxn_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser` |
| `AuthenticatedTransactionProducerConsumerTest#testNoAuth` | failure point: `Create` instead of `build()` | `Create_GivenClientWithoutAuthentication_ShouldThrowAuthenticationException` |
| `BatchMessageContainerImplTest#testNoCompressionBuildFailureProducesValidFrame` | adapted | `Send_GivenFailureOnceWithoutCompressionAndTheBatchIsSentAgain_ShouldWriteAValidFrame` |
| `TransactionCoordinatorClientTest#testNewTxn` | Port-partial | `NewTransaction_GivenStartedClient_ShouldCommitAndAbortTransactionsById` |
| `AutoCloseUselessClientConTXTest#testConnectionAutoReleaseUnPartitionedTopicWithTransaction` | Port-partial | `Transactions_GivenInactiveConnectionsClosed_ShouldKeepWorking` |
| `TopicTransactionBufferRecoverTest#recoverTest` | Port-partial | `Receive_GivenTopicUnloadedWithOpenTransactions_ShouldRecoverTransactionBuffer` |
| `AckReceiptOnTopicUnloadTest#testDelayedAckReceipt` | Port-partial | `Acknowledge_GivenDelayedAckReceipt_ShouldCompleteOrFailWithConsumerNotFound` |
| `UnAckedMessageTrackerTest#testTrackChunkedMessageId` | Port-partial | `Tick_GivenChunkedMessageIdExpired_ShouldRedeliverAllChunks` |
| `UnAckedTopicMessageRedeliveryTrackerTest#testLocalRedeliveryCountForExclusiveSubscription` | the tracker of one sub consumer instead of the multi-topic tracker | `GetLocalRedeliveryCount_GivenSubscriptionThatIsNotShared_ShouldCountExpiriesAndKeepCountsUntilTheEntryIsRemoved` |
| `UnAcknowledgedMessagesTimeoutTest#testAckTimeoutMinValue` | Port-partial | `CreateConsumer_GivenAckTimeoutBelowOneSecond_ShouldThrowConfigurationException` |
| `UnAcknowledgedMessagesTimeoutTest#testCheckUnAcknowledgedMessageRedeliveryTimer` | 4 rows; the lower bound of the timing has a tolerance of 100 ms | `Receive_GivenAckTimeoutRedeliveryBackoff_ShouldRedeliverWhenTheAckTimeoutAndTheDelayHavePassed` |
| `MultiplierRedeliveryBackoffTest#testNext` | no next(-1): the redelivery count is unsigned | `Next_GivenMinDelayMaxDelayAndDefaultMultiplier_ShouldDoubleTheDelayUntilMaxDelay` |
| `PerMessageUnAcknowledgedRedeliveryTest#testUnAckedMessageTrackerSize` | Port-partial | `Receive_GivenAckTimeoutDisabled_ShouldNotTrackMessages` |
| `ConsumerRedeliveryTest#testUnAckMessageRedeliveryWithReceiveAsync` | Port-partial | `Receive_GivenUnacknowledgedBatchedMessages_ShouldRedeliverAfterAckTimeout` |
| `ConsumerRedeliveryTest#testMessageRedeliveryWhenTimeoutInListener` | Port-partial | `Receive_GivenSlowProcessingBeyondAckTimeout_ShouldRedeliverOnlyThatMessage` |
| `ConsumerSeekBatchIndexAckTest#testSeekReplaysBatchAfterSuccessWhileDisconnected` | the upstream test pauses the consumer and delays its reconnect; the port checks the replay only | `Seek_GivenBatchWithFirstMessageAcknowledgedOrNot_ShouldReplayAllMessagesOfTheBatch` |
| `ProducerBuilderImplTest#testProducerBuilderImplWhenBatchingMaxPublishDelayPropertyIsNegative` | Port-partial | `Create_GivenBatchingMaxPublishDelayBelowOneMillisecond_ShouldThrowConfigurationException` |
| `ProducerBuilderImplTest#testProducerBuilderImplWhenSendTimeoutIsNegative` | Port-partial | `Create_GivenNegativeSendTimeout_ShouldThrowConfigurationException` |
| `ProducerBuilderImplTest#testProducerBuilderImplWhenSendTimeoutPropertyIsNegative` | Port-partial | `Create_GivenNegativeSendTimeout_ShouldThrowConfigurationException` |
| `SimpleProducerConsumerTest#testSendTimeout` | Port-partial | `Send_GivenBrokerUnreachable_ShouldFailWithOperationTimeoutException` |
| `SimpleProducerConsumerTest#testSendTimeoutAndRecover` | Port-partial | `Send_GivenSendTimeoutAndBrokerRecovers_ShouldSendAfterRecovery` |
| `SimpleProducerConsumerTest#testSendCallBackReturnSequenceId` | Port-partial | `Send_GivenBrokerUnreachable_ShouldFailEachMessageWithItsSequenceId` |
| `ProducerImplTest#testFailPendingMessagesSyncRetry` | Port-partial | `SendTimeout_GivenMessageEnqueuedAfterTimeout_ShouldNotBeFailedByThatTimeout` |
| `MessageChunkingTest#testMaxPendingChunkMessages` | Port-partial | `Receive_GivenMaxPendingChunkedMessageExceededAndAutoAckOldest_ShouldAcknowledgeTheOldestMessageAndNotDeliverIt` |
| `MessageChunkingTest#testResendChunkMessagesWithoutAckHole` | Port-partial | `Acknowledge_GivenFirstChunkThatWasSentAgain_ShouldLeaveNoAckHole` |
| `MessageChunkingTest#testReaderChunkingConfiguration` | Port-partial | `Create_GivenChunkSettings_ShouldPassThemOnInTheOptions` |
| `MessageChunkingTest#testLargeMessage` | Port-partial | `Send_GivenMessagesAboveMaxMessageSize_ShouldDeliverThemAssembledInOrderAndAcknowledgeEveryChunk` |
| `MessageChunkingTest#testLargeMessageAckTimeOut` | Port-partial | `Receive_GivenChunkedMessagesNotAcknowledgedBeforeAckTimeout_ShouldDeliverThemAgainInOrderAndAcknowledgeAllWithCumulativeAcknowledgment` |
| `MessageChunkingTest#testOrphanedLastChunkDoesNotStallDispatch` | Port-partial | `Receive_GivenTwelveOrphanedLastChunks_ShouldStillDeliverTheNextMessage` |
| `MessageChunkingTest#testLargeMessageOnNonPersistentTopicAboveBrokerLimitIsRejectedLocally` | Port-partial | `Send_GivenNonPersistentMessageAboveBrokerLimit_ShouldThrowTooLargeMessageException` |
| `MessageChunkingTest#testResendChunkMessages` | Port-partial | `Receive_GivenResentAndInterleavedChunks_ShouldAssembleBothMessages` |
| `MessageChunkingTest#testSeekChunkMessages` | Port-partial | `Seek_GivenChunkedMessageId_ShouldDeliverTheMessageAgain` |
| `MessageChunkingTest#testRepeatedSeekDoesNotRetainFilteredChunkMessageIds` | Port-partial | `Seek_GivenIdOfEachChunkedMessage_ShouldDeliverTheMessagesFromThatMessageAndTheMarkerThatIsNotChunked` |
| `MessageChunkingTest#testChunkSize` | Port-partial | `Send_GivenSizesUpToMaxMessageSize_ShouldChunkOnlyAbovePayloadChunkSize` |
| `MessageChunkingSharedTest#testMultiConsumers` | Port-partial | `Receive_GivenTwoSharedConsumers_ShouldDeliverEveryChunkedMessageToOneOfThem` |
| `MessageChunkingSharedTest#testInterleavedChunks` | Port-partial | `Receive_GivenInterleavedChunksOfTwoProducers_ShouldDeliverTheMessagesInTheOrderTheyAreCompleted` |
| `DeadLetterTopicTest#testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId` | Port-partial | `RedeliverUnacknowledgedMessages_GivenBatchMessageIdOfDeadLetterCandidate_ShouldSendWholeBatchToDeadLetterTopicOnce` |
| `DeadLetterTopicTest#testMultipleSameNameConsumersToDeadLetterTopic` | Port-partial | `Receive_GivenThreeConsumersWithSameNameNeverAcknowledging_ShouldDeliverEachMessageMaxRedeliverPlusOneTimesAndDeadLetterEachOnce` |
| `DeadLetterTopicTest#testDuplicatedMessageSendToDeadLetterTopic` | Port-partial | `Receive_GivenThreeConsumersNeverAcknowledging_ShouldNotSendDuplicatesToDeadLetterTopic` |
| `DeadLetterTopicTest#testDeadLetterTopicWithMaxUnackedMessagesBlocking` | Port-partial | `NegativeAcknowledge_GivenMaxUnackedMessagesPolicy_ShouldEventuallySendAllMessagesToDeadLetterTopic` |
| `DeadLetterTopicTest#testDeadLetterTopicWithPoolMessages` | Port-partial | `RedeliverUnacknowledgedMessages_GivenCandidatesFromLastDelivery_ShouldSendAllToDeadLetterTopic` |
| `NegativeAcksTest#testNegativeAcks` | Port-partial | `NegativeAcknowledge_GivenVariations_ShouldRedeliverAllNegativelyAcknowledgedMessages` |
| `SimpleProducerConsumerTest#testCryptoWithChunking` | Port-partial | `Receive_GivenEcdsaEncryptedChunkedMessage_ShouldDecryptEveryChunkAndAssembleThem` |
| `SimpleProducerConsumerTest#testEncryptionFailure` | Port-partial | `Receive_GivenEncryptionFailureActionsOneAfterTheOther_ShouldFailConsumeDecryptAndDiscard` |
| `ProducerEncryptionKeyFailureTest#testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind` | Port-partial | `Create_GivenKeyThatCannotBeLoaded_ShouldThrowCryptoExceptionAndNotRegisterPublisher` |

## Port-unit rows (46)

| Upstream test | C# test |
|---|---|
| `TransactionBuilderTest#checkClientEnableTransactionConfiguration` | `TransactionBuilderTests.cs#Create_GivenClientWithTransactionsDisabled_ShouldThrowConfigurationException` |
| `TransactionMetaStoreHandlerTest#testStateChangeFailure` | `TransactionMetaStoreHandlerTests.cs#Connect_GivenHandlerDisposedWhileConnecting_ShouldFaultConnectedAndNotRegister` |
| `TransactionMetaStoreHandlerTest#testFailedResendOnReconnectRemovesPendingRequest` | `TransactionMetaStoreHandlerTests.cs#Connect_GivenHandlerDisposedBeforePendingRequestIsResent_ShouldFailPendingRequestWithMetaStoreHandlerNotReadyException` |
| `TransactionMetaStoreHandlerTest#testCloseFailsAndReleasesPendingRequests` | `TransactionMetaStoreHandlerTests.cs#DisposeAsync_GivenPendingEndTxnRequest_ShouldFailItWithPulsarClientClosedExceptionAndReleaseIt` |
| `TransactionMetaStoreHandlerTest#testCloseReleasesCancelledPendingRequests` | `TransactionMetaStoreHandlerTests.cs#DisposeAsync_GivenCancelledPendingRequest_ShouldReleaseIt` |
| `TransactionMetaStoreHandlerTest#testTimeoutReleasesCancelledPendingRequests` | `TransactionMetaStoreHandlerTests.cs#Request_GivenCancelledRequestAndNoResponse_ShouldBeReleasedByTheTimeoutSweep` |
| `TransactionMetaStoreHandlerTest#testClientCloseFailsPendingRequestsQueuedBehindBusyExecutor` | `TransactionMetaStoreHandlerTests.cs#DisposeAsync_GivenConnectionPoolDisposedRightAfter_ShouldStillFailAndReleasePendingRequest` |
| `TransactionMetaStoreHandlerTest#testClientCloseFailsRequestsQueuedBeforeBeingSent` | `TransactionMetaStoreHandlerTests.cs#EndTxn_GivenClosedHandler_ShouldThrowMetaStoreHandlerNotReadyExceptionAndSendNothing` |
| `BatchMessageContainerImplTest#testUpdateMaxBatchSize` | `BatchMessageContainerTests.cs#UpdateMaxBatchSize_GivenGrowShrinkAndAlternatingSizes_ShouldFollowCoolingOffRules` |
| `BatchMessageContainerImplTest#recoveryAfterOom` | `BatchMessageContainerTests.cs#CreateBatches_GivenMessageThatFailsWhileTheBatchIsBuilt_ShouldReturnTheBatchAndBuildItAgainAfterwards` |
| `BatchMessageContainerImplTest#testMessagesSize` | `BatchMessageContainerTests.cs#MaxMessagesNum_GivenBatchesOf10Then200Then10_ShouldBe32Then200Then200` |
| `BatchMessageContainerImplTest#testMultiBatchesPartialBuildFailureReleasesBuiltOps` | `KeyBasedBatchMessageContainerTests.cs#CreateBatches_GivenSecondBatchThatFailsToBuild_ShouldReturnTheBatchesThatWereBuiltAndKeepAllMessages` |
| `BatchMessageContainerImplTest#discardReleasesTheBatchBufferWhenTheCallbackThrows` | `SubProducerBatchOwnershipTests.cs#Dispatcher_GivenBatchThatFailsAndContinuationsOfTheMessagesThatThrow_ShouldStillReturnTheBatchAndNotFaultTheProducer` |
| `BatchMessageContainerImplTest#oversizedSingleMessageReleasesTheBatchBufferExactlyOnce` | `SubProducerBatchOwnershipTests.cs#Dispatcher_GivenSingleMessageThatIsTooLargeAndBatching_ShouldFailItWithoutRentingABatchAndWriteTheNextMessage` |
| `BatchMessageContainerImplTest#oversizedBatchReleasesThePayloadExactlyOnce` | `SubProducerBatchOwnershipTests.cs#Dispatcher_GivenBatchThatIsTooLarge_ShouldFailTheMessagesAndReturnTheBatchToThePoolExactlyOnce` |
| `BatchMessageContainerImplTest#testPlainMessageIsNotBatchedWithTransactionalMessages` | `BatchMessageContainerTests.cs#HasSameTxn_GivenTransactionalBatchAndPlainMessage_ShouldReturnFalse` |
| `BatchMessageContainerImplTest#testTransactionalMessageIsNotBatchedWithPlainMessages` | `BatchMessageContainerTests.cs#HasSameTxn_GivenPlainBatchAndTransactionalMessage_ShouldReturnFalse` |
| `BatchMessageContainerImplTest#testKeyBasedContainerKeepsTransactionalAndPlainMessagesApart` | `KeyBasedBatchMessageContainerTests.cs#HasSameTxn_GivenKeyBasedPlainBatchAndTransactionalMessage_ShouldReturnFalse` |
| `BatchMessageContainerImplTest#testSameTransactionStillBatchesTogether` | `BatchMessageContainerTests.cs#HasSameTxn_GivenSameTransaction_ShouldReturnTrueAndGivenOtherTransaction_ShouldReturnFalse` |
| `BatchMessageContainerImplTest#testPlainMessagesStillBatchTogether` | `BatchMessageContainerTests.cs#HasSameTxn_GivenTwoPlainMessages_ShouldReturnTrue` |
| `BatchMessageContainerImplTest#testKeyBasedContainerDropsTheTransactionIdentityOfAFailedFirstAdd` | `KeyBasedBatchMessageContainerTests.cs#HasSameTxn_GivenKeyBasedContainerClearedAfterTransactionalBatch_ShouldTreatNextPlainMessageAsPlain` |
| `BatchMessageContainerImplTest#testOversizedBatchReleasesItsPayloadExactlyOnce` | `SubProducerBatchOwnershipTests.cs#Dispatcher_GivenBatchThatIsTooLarge_ShouldFailTheMessagesAndReturnTheBatchToThePoolExactlyOnce` |
| `BatchMessageContainerImplTest#testOversizedEncryptedBatchReleasesItsBuffersExactlyOnce` | `BatchSendThroughProducerChannelTests.cs#Send_GivenBatchThatIsLargerThanMaxMessageSizeAfterCompressionAndEncryption_ShouldThrowTooLargeMessageExceptionAndLeaveTheBatchToItsOwner` |
| `BatchMessageContainerImplTest#testOversizedSingleMessageBatchReleasesItsPayloadExactlyOnce` | `SubProducerBatchOwnershipTests.cs#Dispatcher_GivenSingleMessageThatIsTooLargeAndBatching_ShouldFailItWithoutRentingABatchAndWriteTheNextMessage` |
| `TransactionClientCloseTest#testCoordinatorRequestsFailBeforeStart` | `TransactionCoordinatorClientTests.cs#Requests_GivenClientNotStarted_ShouldThrowTransactionCoordinatorClientStateException` |
| `TransactionClientCloseTest#testNewTransactionWhenClientClosesDuringCoordinatorSelection` | `TransactionCoordinatorClientTests.cs#NewTransaction_GivenClientDisposedWhileSelectingHandler_ShouldThrowPulsarClientClosedException` |
| `TransactionClientCloseTest#testCommitWhenClientClosesBeforeTheRequestIsSent` | `TransactionCoordinatorClientTests.cs#Commit_GivenClientDisposedBeforeRequestIsSent_ShouldThrowPulsarClientClosedExceptionAndReleaseRequest` |
| `TransactionClientConnectTest#testPulsarClientCloseThenCloseTcClient` | `TransactionMetaStoreHandlerTests.cs#NewTransaction_GivenHandlerClosedAfterReconnect_ShouldThrowMetaStoreHandlerNotReadyException` |
| `TransactionClientConnectTest#testHandlerStateChangeToReady` | `TransactionMetaStoreHandlerTests.cs#Connected_GivenReadyHandler_ShouldCompleteAndKeepReadyState` |
| `TransactionClientConnectTest#testNotEnableTransactionInBroker` | `TransactionCoordinatorClientTests.cs#Start_GivenNoTransactionCoordinatorPartitions_ShouldThrowTransactionCoordinatorClientException` |
| `BatchMessageContainerImplTest#testRecoveryAfterBatchBuildFailure` | `BatchSendThroughProducerChannelTests.cs#Send_GivenEncryptionFailsOnceAndTheBatchIsSentAgain_ShouldLeaveTheBatchAsItWasAndWriteItAsAValidFrame` |
| `TransactionClientConnectTest#testTransactionNewReconnect` | `TransactionCoordinatorClientTests.cs#NewTransaction_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` |
| `TransactionClientConnectTest#testTransactionAddSubscriptionToTxnAsyncReconnect` | `TransactionCoordinatorClientTests.cs#AddSubscriptionToTxn_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` |
| `TransactionClientConnectTest#testTransactionAbortToTxnAsyncReconnect` | `TransactionCoordinatorClientTests.cs#Abort_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` |
| `TransactionClientConnectTest#testTransactionCommitToTxnAsyncReconnect` | `TransactionCoordinatorClientTests.cs#Commit_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` |
| `TransactionClientConnectTest#testTransactionAddPublishPartitionToTxnReconnect` | `TransactionCoordinatorClientTests.cs#AddPublishPartitionToTxn_GivenCoordinatorNotFoundThenAvailable_ShouldRetryUntilSuccess` |
| `MessageChunkingTest#testExpiryWorksAfterAnEarlierChunkedMessageCompletes` | `ChunkedMessageAssemblerTests.cs#RemoveExpired_GivenCompletedThenIncompleteMessage_ShouldExpireIncomplete` |
| `MessageChunkingTest#testExpirySkipsStaleQueueHeadFromDiscardPath` | `ChunkedMessageAssemblerTests.cs#RemoveExpired_GivenStaleQueueHeadFromDiscardPath_ShouldStillExpireLiveEntries` |
| `MessageChunkingTest#testExpiryStaleHeadSkipDoesNotDropConcurrentLiveEntry` | `ChunkedMessageAssemblerTests.cs#RemoveExpired_GivenCompletionOfOtherMessage_ShouldKeepLiveEntryQueued` |
| `MessageChunkingTest#testNonHeadCompletionKeepsIncompleteHeadExpirable` | `ChunkedMessageAssemblerTests.cs#Process_GivenNonHeadCompletion_ShouldKeepIncompleteHeadExpirable` |
| `MessageChunkingTest#testPendingChunkedMessageUuidQueueDoesNotLeak` | `ChunkedMessageAssemblerTests.cs#Process_GivenFiftyCompletedMessages_ShouldLeaveNoPendingContexts` |
| `MessageChunkingTest#testExpireIncompleteChunkMessage` | `ChunkedMessageAssemblerTests.cs#RemoveExpired_GivenIncompleteMessageOlderThanExpiry_ShouldRemoveContext` |
| `cpp:MessageChunkingTest.cc#testResendChunkWithAckHoleMessages` | `ChunkedMessageAssemblerTests.cs#Process_GivenFirstChunkAgainWithDifferentEntry_ShouldAckPreviousChunks` |
| `cpp:DeadLetterQueueTest.cc#testAutoSetDLQTopicName` | `DeadLetterHandlerTests.cs#Constructor_GivenNoDeadLetterTopic_ShouldUseTopicSubscriptionDlqSuffix` |
| `NegativeAcksTest#testNegativeAcksDeleteFromUnackedTracker` | `NegativeAcknowledgeTrackerTests.cs#Add_GivenBatchMessageIdsOfSameEntry_ShouldTrackOneEntryAndRemoveFromUnackedTracker` |
| `DefaultCryptoKeyReaderTest` | `DefaultCryptoKeyReaderTests.cs#GetKeys_GivenDefaultsAndKeyNameUris_ShouldUseTheKeyNameUriBeforeTheDefault` |
