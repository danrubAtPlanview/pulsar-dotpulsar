# Test parity — `TransactionEndToEndTest`, `TransactionEndToEndWithoutBatchIndexAckTest`, `TransactionTestBase`

Upstream (pinned `apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026`):

- `java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionEndToEndTest.java` (1749 lines) — short form **E2E**
- `java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionEndToEndWithoutBatchIndexAckTest.java` (59 lines) — short form **NOBIA**
- `java:pulsar-broker/src/test/java/org/apache/pulsar/broker/transaction/TransactionTestBase.java` (248 lines) — short form **TTB**

All line numbers below are of those files at the pinned SHA. Names of C# API members follow the repo spec as it
stands now ([03-public-api](../03-public-api.md), `05-components/*.md`), which refines the the spec decisions
names, aligned with [03-public-api](../03-public-api.md): `client.NewTransaction().Timeout(TimeSpan).Create(ct)` (not `Build`; interface member `IPulsarClient.CreateTransaction(TransactionOptions, ct)`), `ITransaction.CurrentState` (sync, = Java `getState()`) + `ITransaction.State`
(`IState<TransactionState>`), `TransactionId(ulong mostSigBits, ulong leastSigBits)`, `MessageMetadata.Transaction`,
`PulsarClient.TransactionCoordinatorClient` (`ITransactionCoordinatorClient?`), public ctor
`DotPulsar.Internal.Transactions.Transaction(ITransactionCoordinatorClient tcClient, TransactionId id, TimeSpan timeout)`,
`OperationTimeoutException` for the Java `PulsarClientException.TimeoutException`. Fixture names follow
[09-test-infrastructure](../09-test-infrastructure.md) (`TransactionFixture`, `TransactionWithoutBatchIndexAckFixture`, `TransactionFixture.CreateTopicName`,
`TransactionFixture.ReceiveOrNull`, `GetTopicStats`, `GetInternalStats`, `GetTransactionMetadata`, ...).

---

## 0. Summary table

Status legend: **Port** = all assertions ported through the public API (+ fixture admin REST that Java also uses through
`PulsarAdmin`); **Port-partial** = one or more assertions/mechanisms substituted (each substitution listed in the test's
section; none dropped silently); **Excluded** = cannot be reproduced against a real broker container.
"Exec" = number of TestNG executions in `TransactionEndToEndTest` (DataProvider rows).

| # | Java method (E2E) | Exec | Class. | C# method (`TransactionEndToEndTestsBase<TFixture>`) | Prerequisites | Status |
|---|---|---|---|---|---|---|
| 1 | `testIndividualAckAbortFilterAckSetInPendingAckState` L123-176 | 1 | (a) | `Abort_GivenBatchIndexesAckedByTwoTransactionsAndPlainAck_ShouldRedeliverOnlyTheAbortedIndexes` | batching, max publish delay, batch-index ack (txn ack_set + receive-side ack_set filter), ack receipt | Port |
| 2 | `testFilterMsgsInPendingAckStateWhenConsumerDisconnect` L179-252 | 2 (`enableBatch` true/false) | (a) | `Abort_GivenPendingAcksAndReconnectedConsumer_ShouldDeliverOnlyAbortedMessages` | batching, max publish delay, batch-index ack, ack receipt | Port |
| 3 | `testMsgsInPendingAckStateWouldNotGetTheConsumerStuck` L254-311 | 1 | (a) | `Receive_GivenMessagesInPendingAckStateAndSmallPrefetch_ShouldNotGetConsumerStuck` | batching (Java default on) | Port |
| 4 | `produceCommitTest` L313-368 | 2 (`enableBatch`) | (a) | `Commit_GivenTwoTransactionsProducingToPartitionedTopic_ShouldDeliverAllMessagesOnlyAfterCommit` | batching, send timeout (0 = disabled), commit waits for in-flight sends | Port |
| 5 | `produceAbortTest` L370-453 | 1 | (c) | `Abort_GivenProducedMessages_ShouldNotDeliverAndShouldMoveMarkDeleteToEnd` | send timeout | Port-partial (broker cursor check -> admin internal stats) |
| 6 | `testUnackMessageAfterAckAllMessages` L470-521 | 8 (`unackMessagesCountParams`) | (a) | `Commit_GivenAllMessagesAckedWithTransaction_ShouldLeaveNoUnackedMessages` | batching, batching max messages, ack receipt, list ack with txn | Port (skipped in NOBIA) |
| 7 | `testAckWithTransactionReduceUnAckMessageCount` L523-583 | 2 (`enableBatch`) | (c) | `Commit_GivenHalfOfMessagesAckedWithTransaction_ShouldReduceUnackedMessageCount` | batching, batching max messages, ack receipt | Port-partial (broker `Consumer#getUnackedMessages` -> admin stats) (skipped in NOBIA) |
| 8 | `txnIndividualAckTestNoBatchAndSharedSub` L585-588 (+`txnAckTest` L605-682) | 1 | (c) | `Acknowledge_GivenNoBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` | batching off, batching max messages | Port-partial (reflection re-commit -> TC client commit) |
| 9 | `txnIndividualAckTestBatchAndSharedSub` L590-593 | 1 | (c) | `Acknowledge_GivenBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` | batching, batching max messages, batch-index ack | Port-partial (same) |
| 10 | `txnIndividualAckTestNoBatchAndFailoverSub` L595-598 | 1 | (c) | `Acknowledge_GivenNoBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` | batching off | Port-partial (same) |
| 11 | `txnIndividualAckTestBatchAndFailoverSub` L600-603 | 1 | (c) | `Acknowledge_GivenBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` | batching, batch-index ack | Port-partial (same) (NOBIA override: skipped, see §3) |
| 12 | `testAfterDeleteTopicOtherTopicCanRecover` L684-704 | 1 | (a) | `Send_GivenOtherTopicWithSubscriptionWasDeleted_ShouldProduceAndConsume` | — (admin create topic/subscription/delete) | Port |
| 13 | `txnMessageAckTest` L706-819 | 1 | (c) | `Acknowledge_GivenCommittedTransactionalMessages_ShouldAckAndMoveMarkDeleteToEnd` | send timeout, `RedeliverUnacknowledgedMessages` (exists) | Port-partial (broker cursor check -> admin internal stats) |
| 14 | `txnAckTestBatchAndCumulativeSub` L821-824 (+`txnCumulativeAckTest` L831-932) | 1 | (c) | `AcknowledgeCumulative_GivenBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit` | batching, max publish delay, batch-index cumulative ack, ack timeout | Port-partial (reflection re-commit -> TC client commit) |
| 15 | `txnAckTestNoBatchAndCumulativeSub` L826-829 | 1 | (c) | `AcknowledgeCumulative_GivenNoBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit` | batching off, max publish delay, ack timeout | Port-partial (same) |
| 16 | `txnMetadataHandlerRecoverTest` L941-996 | 1 | (a) | `Commit_GivenTransactionsCommittedByIdFromAnotherClient_ShouldDeliverAllMessages` | batching (Java default), send timeout, `PulsarClient.TransactionCoordinatorClient` | Port |
| 17 | `produceTxnMessageOrderTest` L998-1038 | 1 | (a) | `Commit_GivenThousandTransactionalMessages_ShouldDeliverInSendOrder` | batching (Java default), send timeout, ordered fire-and-forget send | Port |
| 18 | `produceAndConsumeCloseStateTxnTest` L1040-1148 | 1 | (c) | `SendAndAcknowledge_GivenEndedOrTimedOutTransaction_ShouldThrow` | send timeout, internal `Transaction` ctor | Port-partial (TC store lookup -> admin `transactionMetadata`; reflection ctor -> public internal ctor) |
| 19 | `testTxnTimeoutAtTransactionMetadataStore` L1150-1169 | 1 | (c) | `Create_GivenTimeoutElapsesAtCoordinator_ShouldIncrementCoordinatorTimeoutCount` | — | Port-partial (broker TC service -> client `Create` + admin + `/metrics`) |
| 20 | `transactionTimeoutTest` L1171-1214 | 1 | (a) | `Acknowledge_GivenTransactionTimesOut_ShouldRedeliverMessage` | send timeout | Port |
| 21 | `txnTransactionRedeliverNullDispatcher` L1221-1283 | 2 (`ackType` Cumulative/Individual) | (b) | — | — | Excluded |
| 22 | `oneTransactionOneTopicWithMultiSubTest` L1285-1347 | 1 | (c) | `Acknowledge_GivenOneTransactionAndTwoSubscriptions_ShouldRegisterBothSubscriptions` | send timeout | Port-partial (TC store -> admin `transactionMetadata.ackedPartitions`) |
| 23 | `testTxnTimeOutInClient` L1349-1386 | 1 | (a) | `SendAndAcknowledge_GivenTransactionTimedOutInClient_ShouldThrowInvalidTransactionStatusException` | send timeout | Port |
| 24 | `testCumulativeAckRedeliverMessages` L1388-1459 | 1 | (a) | `AcknowledgeCumulative_GivenConflictingTransactionThenAbort_ShouldRedeliverMessages` | batching (Java default), send timeout | Port |
| 25 | `testSendTxnMessageTimeout` L1461-1498 | 1 | (c) | `Send_GivenTransactionalMessageWithoutReceipt_ShouldThrowOperationTimeoutException` | send timeout (1 s), batching (Java default) | Port-partial (mocked `ClientCnx` -> black-hole TCP relay) |
| 26 | `testAckWithTransactionReduceUnackCountNotInPendingAcks` L1500-1543 | 1 | (c) | `Acknowledge_GivenOneBatchIndexAckedWithTransaction_ShouldReduceUnackedCountByOne` | batching, max publish delay, send timeout, batch-index ack | Port-partial (broker consumer -> admin stats) |
| 27 | `testSendTxnAckMessageToDLQ` L1545-1608 | 1 | (c) | `Abort_GivenMessageExceedsMaxRedeliverCount_ShouldSendMessageToDeadLetterTopic` | DLQ, send timeout | Port-partial (client permit counter dropped -> unit test) |
| 28 | `testSendTxnAckBatchMessageToDLQ` L1610-1683 | 1 | (c) | `Abort_GivenBatchMessageExceedsMaxRedeliverCount_ShouldSendBatchToDeadLetterTopic` | DLQ, batching (Java default), batch-index ack, send timeout | Port-partial (same) |
| 29 | `testDelayedTransactionMessages` L1685-1748, `groups = "flaky"` | 1 | (a) | `Commit_GivenDelayedTransactionalMessages_ShouldDeliverImmediatelyToFailoverAndDelayedToShared` | `DeliverAt` (exists) | Port |

NOBIA (subclass, runs all inherited tests on a broker with `acknowledgmentAtBatchIndexLevelEnabled=false`):

| Java (NOBIA) | Lines | Effect | C# (`TransactionEndToEndWithoutBatchIndexAckTests`) | Status |
|---|---|---|---|---|
| `setup` | L33-38 | `conf.setAcknowledgmentAtBatchIndexLevelEnabled(false)` then same `setUpBase` as E2E | `TransactionWithoutBatchIndexAckFixture` (collection `"TransactionWithoutBatchIndexAck"`) | Port |
| `txnIndividualAckTestBatchAndFailoverSub` override | L40-45 | flips the running broker's config back to `true`, then runs `txnAckTest(true, 200, Failover)` | skipped in NOBIA with reason; identical scenario runs in `TransactionEndToEndTests` (row 11) | Port (covered by row 11) |
| `testUnackMessageAfterAckAllMessages` override | L47-52 | `enabled = false` | skipped in NOBIA (8 rows) | Skipped as upstream |
| `testAckWithTransactionReduceUnAckMessageCount` override | L54-58 | `enabled = false` | skipped in NOBIA (2 rows) | Skipped as upstream |
| all other inherited tests | — | run with batch-index ack disabled | inherited from the base class | as rows above |

Counts (E2E methods, 29): **Port 12** (21 executions), **Port-partial 16** (17 executions), **Excluded 1** (2 executions).
NOBIA: 25 inherited methods run (rows 1-5, 8-10, 12-20, 22-29), rows 6 and 7 skipped as upstream (10 executions), row 11 skipped because the upstream override is covered by the base class, row 21 excluded.

---

## 1. Fixture mapping (`TransactionTestBase`) and class layout

### 1.1 What `TransactionTestBase` configures

| Java | Lines | Value | C# fixture ([09-test-infrastructure](../09-test-infrastructure.md)) |
|---|---|---|---|
| `brokerCount` | TTB L60, set by `setUpBase(1, ...)` E2E L102 | 1 broker | standalone container (1 broker) — exact |
| `createTransactionCoordinatorAssign(numPartitionsOfTC)` | TTB L136-142, E2E L97 `NUM_PARTITIONS = 16` | 16 TCs | standalone creates the assign topic with **1** partition (TC id 0). No test in this class depends on the TC count (row 19 uses TC 0, which exists). Accepted deviation. |
| cluster `test`, tenant `pulsar` + ns `pulsar/system` | TTB L110-115 | — | standalone creates `pulsar/system`; cluster is `standalone` (no test asserts the name) |
| tenant `tnx` (admin role `appid1`, cluster `test`), namespace `tnx/ns1` with 4 bundles | TTB L70-71, L117-119 | — | fixture `InitializeAsync`: `CreateTenant("tnx")`, `CreateNamespace("tnx/ns1", 4)` |
| `TOPIC_OUTPUT = tnx/ns1/output`, 3 partitions | E2E L94-95, TTB L120-126 via `setUpBase(..., TOPIC_OUTPUT, TOPIC_PARTITION)` | partitioned (3) | per test: `CreateTopicName("output")` + `CreatePartitionedTopic(topic, 3)` (replaces `resetTopicOutput` E2E L106-111) |
| `TOPIC_MESSAGE_ACK_TEST = tnx/ns1/message-ack-test`, 1 partition | E2E L96, L103 | partitioned (1) | per test: `CreateTopicName("message-ack-test")` + `CreatePartitionedTopic(topic, 1)` |
| `conf.setTransactionCoordinatorEnabled(true)` | TTB L161 | true | `PULSAR_PREFIX_transactionCoordinatorEnabled=true` |
| `conf.setBrokerDeduplicationEnabled(true)` | TTB L162 | true | `PULSAR_PREFIX_brokerDeduplicationEnabled=true` |
| `conf.setTransactionBufferSnapshotMaxTransactionCount(2)` | TTB L163 | 2 | env, same |
| `conf.setTransactionBufferSnapshotMinTimeInMillis(2000)` | TTB L164 | 2000 | env, same |
| `conf.setDispatcherRetryBackoffInitialTimeInMs(0)` / `MaxTimeInMs(0)` | TTB L166-167 | 0 / 0 | env, same |
| `conf.setActiveConsumerFailoverDelayTimeMillis(0)` | TTB L149 | 0 | env, same (matters for rows 10, 11, 14, 15, 29: Failover redelivery is not delayed by 1 s) |
| `conf.setAllowAutoTopicCreationType(NON_PARTITIONED)` | TTB L153 | — | standalone default `non-partitioned` (tests rely on auto-created non-partitioned topics) |
| `acknowledgmentAtBatchIndexLevelEnabled` | not set in TTB; `ServiceConfiguration` default at the pinned SHA is `true` (`java:pulsar-broker-common/.../ServiceConfiguration.java` L500-501); NOBIA L35 sets `false` | true / false | `PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled=true` / `false` (4.0.9 default is `false`, so it must be explicit) |
| `conf.setDefaultNumberOfNamespaceBundles(1)`, `setManagedLedgerCacheSizeMB(8)`, `setForceDeleteNamespaceAllowed(true)`, `setBrokerShutdownTimeoutMs(0)`, ports/addresses/metadata URLs, `setBookkeeperClientExposeStatsToPrometheus`, `setLoadBalancerOverrideBrokerNicSpeedGbps` | TTB L146-160 | — | not needed (in-JVM mock plumbing or not observable by these tests) |
| `CounterBrokerInterceptor`, `spyByDefault`, OpenTelemetry | TTB L170-175 | — | test instrumentation, not used by these tests |
| client: `statsInterval(0)`, `enableTransaction(true)` | TTB L130-133 | — | `PulsarClient.Builder()...EnableTransaction(true)` (DotPulsar has no stats timer) |
| shared `pulsarClient` for the whole class | TTB L68, L130 | one client | one client per test (repo pattern, `await using var client = CreateClient();`) — no test depends on client sharing except row 16 which explicitly creates a second client |
| `checkSnapshotPublisherCount` | TTB L233-246 | — | not used by these classes |
| `deleteNamespaceWithRetry` | TTB L220-231 | — | not used by these classes |

`TransactionEndToEndTest` cleanup (`resetTopicOutput` L106-111, `admin.topics().delete(...)` at the end of tests) is
replaced by unique per-test topics (`TransactionFixture.CreateTopicName("<java local name>")` -> `persistent://tnx/ns1/<name>-<guid>`),
so no C# test deletes topics. The cleanup steps are therefore not translated; the explicit `producer.close()` /
`consumer.close()` become `await using`.

### 1.2 Class layout (agrees with [09-test-infrastructure](../09-test-infrastructure.md) §4.3)

```
tests/DotPulsar.Tests/Transactions/TransactionEndToEndTestsBase.cs                 abstract class TransactionEndToEndTestsBase<TFixture> : IDisposable where TFixture : TransactionFixture
tests/DotPulsar.Tests/Transactions/TransactionEndToEndTests.cs                     [Collection("Transaction"), Trait("Category", "Integration")] public sealed class TransactionEndToEndTests : TransactionEndToEndTestsBase<TransactionFixture>
tests/DotPulsar.Tests/Transactions/TransactionEndToEndWithoutBatchIndexAckTests.cs [Collection("TransactionWithoutBatchIndexAck"), Trait("Category", "Integration")] public sealed class TransactionEndToEndWithoutBatchIndexAckTests : TransactionEndToEndTestsBase<TransactionWithoutBatchIndexAckFixture>
tests/DotPulsar.Tests/Transactions/BlackHoleRelay.cs                               test helper for row 25 (§4)
```

Base class skeleton:

```csharp
namespace DotPulsar.Tests.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal.Transactions;
using System.Buffers;
using System.Text;

// Upstream: TransactionEndToEndTest
public abstract class TransactionEndToEndTestsBase<TFixture> : IDisposable where TFixture : TransactionFixture
{
    protected const int TopicPartition = 3;                                              // E2E L94
    private static readonly TimeSpan WaitTimeForCanReceiveMsg = TimeSpan.FromSeconds(5);    // E2E L98
    private static readonly TimeSpan WaitTimeForCannotReceiveMsg = TimeSpan.FromSeconds(5); // E2E L99
    private readonly CancellationTokenSource _cts;
    private readonly ITestOutputHelper _testOutputHelper;

    protected TransactionEndToEndTestsBase(TFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3)); // see Open decision 4
        Fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    protected TFixture Fixture { get; }

    // NOBIA overrides these (Java NOBIA L40-58)
    protected virtual string? SkipReasonForUnackMessageCountTests => null;
    protected virtual string? SkipReasonForBatchAndFailoverIndividualAck => null;

    public void Dispose() => _cts.Dispose();

    // E2E#getTxn L934-940
    private async ValueTask<ITransaction> GetTxn(IPulsarClient client)
        => await client.NewTransaction().Timeout(TimeSpan.FromMinutes(10)).Create(_cts.Token);

    private IPulsarClient CreateClient() => Fixture.CreateClient(_testOutputHelper.Log);

    // Java Producer#sendAsync: enqueue in call order, return a future completed by the receipt (see §2.2)
    private async ValueTask<Task<MessageId>> SendAsync<TMessage>(IProducer<TMessage> producer, TMessage value, ITransaction? transaction = null, DateTimeOffset? deliverAt = null)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);
        var metadata = new MessageMetadata { Transaction = transaction };
        if (deliverAt.HasValue)
            metadata.DeliverAtTimeAsDateTimeOffset = deliverAt.Value;
        await producer.SendChannel.Send(metadata, value, id => { receipt.TrySetResult(id); return ValueTask.CompletedTask; }, _cts.Token);
        return receipt.Task;
    }

    // Java Awaitility.await() defaults: atMost 10 s, poll interval 100 ms
    private async Task WaitUntil(Func<Task<bool>> condition, TimeSpan? atMost = null)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(atMost ?? TimeSpan.FromSeconds(10));
        while (!await condition())
            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }
}

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionEndToEndTests : TransactionEndToEndTestsBase<TransactionFixture>
{
    public TransactionEndToEndTests(TransactionFixture fixture, ITestOutputHelper outputHelper) : base(fixture, outputHelper) { }
}

// Upstream: TransactionEndToEndWithoutBatchIndexAckTest
[Collection("TransactionWithoutBatchIndexAck"), Trait("Category", "Integration")]
public sealed class TransactionEndToEndWithoutBatchIndexAckTests : TransactionEndToEndTestsBase<TransactionWithoutBatchIndexAckFixture>
{
    public TransactionEndToEndWithoutBatchIndexAckTests(TransactionWithoutBatchIndexAckFixture fixture, ITestOutputHelper outputHelper) : base(fixture, outputHelper) { }

    // Upstream: TransactionEndToEndWithoutBatchIndexAckTest#testUnackMessageAfterAckAllMessages / #testAckWithTransactionReduceUnAckMessageCount (enabled = false, L47-58)
    protected override string? SkipReasonForUnackMessageCountTests => "Disabled upstream in TransactionEndToEndWithoutBatchIndexAckTest (enabled = false)";

    // Upstream: TransactionEndToEndWithoutBatchIndexAckTest#txnIndividualAckTestBatchAndFailoverSub (L40-45)
    protected override string? SkipReasonForBatchAndFailoverIndividualAck => "Upstream re-enables acknowledgmentAtBatchIndexLevelEnabled at runtime for this test; the identical scenario runs in TransactionEndToEndTests";
}
```

Skipping uses xUnit v3 `Assert.SkipWhen(reason is not null, reason!)` as the first statement of the affected tests.

Java `Awaitility.await().until(consumer::isConnected)` -> `await consumer.OnStateChangeTo(ConsumerState.Active, _cts.Token)`;
`until(producer::isConnected)` -> `await producer.OnStateChangeTo(ProducerState.Connected, _cts.Token)`
(`StateHolderExtensions`). A Failover subscription with one consumer reaches `Active` (broker sends
`ActiveConsumerChange(is_active=true)`).

---

## 2. Cross-cutting translation rules

### 2.1 Java client defaults that the tests rely on

| Java default | Value | C# |
|---|---|---|
| `ProducerBuilder.enableBatching` | **true** | Tests that do not call `enableBatching` (rows 3, 16, 17, 24, 25, 26, 28) must set `.EnableBatching(true)` explicitly, because the recommended DotPulsar default stays `false` (Open decision 1). |
| `batchingMaxMessages` | 1000 | `.BatchingMaxMessages(1000)` default of the batching prerequisite |
| `batchingMaxPublishDelay` | 1 ms | `.BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1))` default of the batching prerequisite |
| `sendTimeout` | 30 s | default of the send-timeout prerequisite; `sendTimeout(0, SECONDS)` -> `.SendTimeout(TimeSpan.Zero)` (= disabled) |
| `subscriptionType` | Exclusive | DotPulsar default `Exclusive` (`ConsumerOptions.DefaultSubscriptionType`) — same |
| `subscriptionInitialPosition` | Latest | DotPulsar default `Latest` — same |
| `receiverQueueSize` | 1000 | DotPulsar `MessagePrefetchCount` default 1000 — same |
| `acknowledgmentGroupTime` | 100 ms | DotPulsar sends acks immediately (no grouping). `acknowledgmentGroupTime(0, ms)` therefore has no C# equivalent and is a no-op (rows 13, 21, 22). |
| `isAckReceiptEnabled` | false | `.AckReceiptEnabled(true)` where Java sets it. Transactional acks always await `CommandAckResponse` regardless. |
| `ackTimeout` | 0 (off) | `.AckTimeout(TimeSpan.FromMinutes(1))` where Java sets it (rows 14, 15) |
| `newProducer()` / `newConsumer()` (byte[]) | `Schema.BYTES` | `client.NewProducer(Schema.ByteArray)` / `client.NewConsumer(Schema.ByteArray)` |
| `Schema.INT32`, `Schema.STRING` | — | `Schema.Int32`, `Schema.String` |
| `"...".getBytes()` (platform charset) / `getBytes(UTF_8)` / `new String(bytes)` | — | `Encoding.UTF8.GetBytes(...)` / `Encoding.UTF8.GetString(...)` (all values are ASCII) |
| `"..." + enableBatch` in topic names | `"true"`/`"false"` | `enableBatch ? "true" : "false"` |

### 2.2 Send semantics

- Java `producer.send(v)` / `newMessage(txn).value(v).send()` -> `await producer.NewMessage().Transaction(txn).Send(v, _cts.Token)`
  (or `await producer.Send(v, _cts.Token)` without a transaction).
- Java `sendAsync(...)` that is *not* awaited (rows 1, 2, 4, 6, 7, 8-11, 13, 14, 15, 17, 26, 28, 29) ->
  `var receipt = await SendAsync(producer, v, txn);` (helper in §1.2). Awaiting the helper only awaits the enqueue
  through `ISendChannel.Send`, which keeps call order and does not wait for the broker receipt, exactly like Java
  `sendAsync`. This is required for:
  - batching tests with `batchingMaxPublishDelay(1, HOURS)` (rows 1, 2): awaiting each receipt would deadlock
    because the batch only flushes when `batchingMaxMessages` is reached;
  - ordering (row 17) and "commit waits for in-flight sends" (rows 4, 13, 17).
- **Requirement on the implementation** (flag to `05-components/producer-integration` / transaction spec): when a
  transactional send has been accepted by `ISendChannel.Send` (its returned `ValueTask` completed), the send op
  must already be registered with the transaction (`Transaction.RegisterSendOp`), so that a following
  `Commit()` waits for its receipt. Java registers the op synchronously in `TypedMessageBuilderImpl#sendAsync`
  (L131-141) before the future completes. Rows 4, 13, 17 commit right after un-awaited sends and would otherwise
  race.
- Java `latestSendFuture.join()` (row 6) -> `await lastReceipt;`. Java `CountDownLatch` over all send futures
  (row 7) -> `await Task.WhenAll(receipts);`. Java `producer.flush()` with batching disabled (row 29) ->
  `await Task.WhenAll(receipts);`.

### 2.3 Receive semantics

- `consumer.receive()` (no timeout) -> `await consumer.Receive(_cts.Token)`.
- `consumer.receive(5, SECONDS)` expected non-null -> `var m = await TransactionFixture.ReceiveOrNull(consumer, WaitTimeForCanReceiveMsg, _cts.Token); m.ShouldNotBeNull();`
  (when Java dereferences the result without a null check, a null would throw in Java; C# asserts `ShouldNotBeNull()` first).
- `consumer.receive(5, SECONDS)` expected null -> `(await TransactionFixture.ReceiveOrNull(consumer, WaitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();`

### 2.4 Exception mapping

| Java (cause of `ExecutionException` / thrown) | C# expected exception | Notes |
|---|---|---|
| `PulsarClientException.TransactionConflictException` | `DotPulsar.Exceptions.TransactionConflictException` (existing) | from `CommandAckResponse.error = TransactionConflict` |
| `TransactionCoordinatorClientException.InvalidTxnStatusException` | `DotPulsar.Exceptions.InvalidTransactionStatusException` (existing) | client-side state check or TC `InvalidTxnStatus` |
| `TransactionCoordinatorClientException.TransactionNotFoundException` | `DotPulsar.Exceptions.TransactionNotFoundException` (new) | TC `TransactionNotFound` |
| `PulsarClientException.TimeoutException` (send timeout) | `DotPulsar.Exceptions.OperationTimeoutException` (new, `03-public-api.md` §4) | |

Assert with `await Should.ThrowAsync<T>(async () => await ...)`. Java's `e.getCause()` / `e.getCause().getCause()` unwrapping
disappears because DotPulsar throws the exception directly.

### 2.5 Re-commit of a finished transaction (rows 8-11, 14, 15)

Java forces `TransactionImpl.state = OPEN` by reflection and calls `commit()` again (E2E L664-675, L911-922). With the
state forced to `OPEN`, `TransactionImpl#commit` (`java:pulsar-client/.../transaction/TransactionImpl.java` L189-217)
waits for the (completed) ops and calls `tcClient.commitAsync(txnId)`; the TC answers `TransactionNotFound` because the
transaction already ended. Substitution with the same wire behaviour and the same expected exception:

```csharp
var tcClient = ((PulsarClient) client).TransactionCoordinatorClient!; // already started: this client created transactions
await Should.ThrowAsync<TransactionNotFoundException>(async () => await tcClient.Commit(commitTxn.Id, _cts.Token));
```

(`ITransactionCoordinatorClient` in `DotPulsar.Internal.Abstractions`, member `Commit(TransactionId, CancellationToken)`; `05-components/transaction-coordinator-client.md` L17-27.)

---

## 3. Per-test specifications

Every C# test below lives in `TransactionEndToEndTestsBase<TFixture>`, is `public async Task`, and has
`// Upstream: TransactionEndToEndTest#<javaMethod>` directly above its `[Fact]`/`[Theory]`. Unless stated otherwise
`client` is `await using var client = CreateClient();` and `Fixture` is the collection fixture.

### Row 1 — `java:.../client/impl/TransactionEndToEndTest.java#testIndividualAckAbortFilterAckSetInPendingAckState (L123-176)`

- TestNG: method-level `@Test`, method is `private` (TestNG runs method-level `@Test` methods regardless of visibility; Open decision 6). Class group `broker-impl` (L91). Also runs in NOBIA.
- Classification: (a) portable.
- Prerequisites: producer batching (`EnableBatching`, `BatchingMaxPublishDelay`, `BatchingMaxMessages`), batch-index ack with transaction (client sends `ack_set` for the single index), receive-side filtering of `CommandMessage.ack_set` (deliver only indexes the broker marks as unacked), ack receipt.
- C#: `[Fact] Abort_GivenBatchIndexesAckedByTwoTransactionsAndPlainAck_ShouldRedeliverOnlyTheAbortedIndexes`
- Steps:
  1. Arrange: `topic = TransactionFixture.CreateTopicName("testIndividualAckAbortFilterAckSetInPendingAckState")` (L125); `count = 9` (L126).
  2. Producer `client.NewProducer(Schema.Int32).Topic(topic).EnableBatching(true).BatchingMaxPublishDelay(TimeSpan.FromHours(1)).BatchingMaxMessages(9).Create()` (L127-132).
  3. Consumer `client.NewConsumer(Schema.Int32).Topic(topic).AckReceiptEnabled(true).SubscriptionName("test").SubscriptionType(SubscriptionType.Shared).Create()` (L135-141).
  4. For `i` in 0..8: `await SendAsync(producer, i)` (not awaiting receipts) (L143-145) — the 9 messages form one batch.
  5. `firstTransaction = await GetTxn(client)`; `secondTransaction = await GetTxn(client)` (L147-149).
  6. Act: 3 times `await consumer.Acknowledge((await consumer.Receive(ct)).MessageId, firstTransaction, ct)` (L152-154).
  7. 3 times `await consumer.Acknowledge((await consumer.Receive(ct)).MessageId, secondTransaction, ct)` (L157-159).
  8. 3 times `await consumer.Acknowledge(await consumer.Receive(ct), ct)` (plain ack, awaits the receipt) (L162-164).
  9. `await secondTransaction.Abort(ct)` (L167). `firstTransaction` stays open (as in Java).
  10. Assert: for `i` in 0..2: `(await consumer.Receive(ct)).Value().ShouldBe(i + 3)` (L170-172).
  11. `(await ReceiveOrNull(consumer, 5 s)).ShouldBeNull()` (L175).

### Row 2 — `#testFilterMsgsInPendingAckStateWhenConsumerDisconnect(boolean enableBatch) (L179-252)`

- DataProvider `enableBatch` (L118-121): `{true}`, `{false}`. Method-level `@Test(dataProvider)`, `private`. Also runs in NOBIA.
- Classification: (a).
- Prerequisites: batching, max publish delay, batch-index ack, ack receipt.
- C#: `[Theory] [InlineData(true)] [InlineData(false)] Abort_GivenPendingAcksAndReconnectedConsumer_ShouldDeliverOnlyAbortedMessages(bool enableBatch)`
- Steps:
  1. `topic = CreateTopicName($"testFilterMsgsInPendingAckStateWhenConsumerDisconnect-{(enableBatch ? "true" : "false")}")`; `count = 10` (L181-182).
  2. Producer `Schema.Int32`: if `enableBatch` -> `.EnableBatching(true).BatchingMaxPublishDelay(TimeSpan.FromHours(1)).BatchingMaxMessages(10)`; else `.EnableBatching(false)` (L186-198).
  3. Consumer `Schema.Int32`, `AckReceiptEnabled(true)`, sub `"test"`, `Shared` (L201-207).
  4. For `i` in 0..9 `await SendAsync(producer, i)` (L209-211).
  5. `txn1 = GetTxn`, `txn2 = GetTxn` (L213-215).
  6. 5 times: receive (no timeout) and `Acknowledge(id, txn1)` awaited (L219-221).
  7. 5 times: receive and `Acknowledge(id, txn2)` awaited (L224-226).
  8. `await txn2.Commit(ct)` (L228).
  9. `await consumer.DisposeAsync()`; create a new consumer with the identical builder (L231-238) (`await using` the new one).
  10. Assert `ReceiveOrNull(consumer, 5 s)` is null (L240-241).
  11. `await txn1.Abort(ct)` (L244).
  12. `receiveCounter = 0`; loop `while ((message = ReceiveOrNull(consumer, 5 s)) is not null) { message.Value().ShouldBe(receiveCounter); receiveCounter++; }` (L246-250).
  13. `receiveCounter.ShouldBe(5)` (L251).

### Row 3 — `#testMsgsInPendingAckStateWouldNotGetTheConsumerStuck (L254-311)`

- `private`, method-level `@Test`. Also runs in NOBIA.
- Classification: (a).
- Prerequisites: batching (Java default `enableBatching=true`, each `send` awaited so every batch holds one message).
- C#: `[Fact] Receive_GivenMessagesInPendingAckStateAndSmallPrefetch_ShouldNotGetConsumerStuck`
- Steps:
  1. `topic = CreateTopicName("testMsgsInPendingAckStateWouldNotGetTheConsumerStuck")`; `subscription = "test"` (L256-257).
  2. Producer `Schema.Int32`, `.EnableBatching(true)` (Java default) (L260-262).
  3. Consumer `Schema.Int32`, sub `"test"`, `Shared`, `InitialPosition(Earliest)` (L264-269).
  4. `numStep1Receive = 2, numStep2Receive = 2, numStep3Receive = 2`, total 6 (L271-272).
  5. For `i` in 0..5: `await producer.Send(i, ct)` (L274-276).
  6. `step1Txn = GetTxn`, `step2Txn = GetTxn` (L278-279).
  7. 2 times receive (no timeout) + `Acknowledge(id, step1Txn)` awaited (L282-284). `step1Txn` is never ended.
  8. 2 times receive + `Acknowledge(id, step2Txn)` awaited (L287-289); `await step2Txn.Commit(ct)` (L292).
  9. Dispose consumer (L295). `consumer2` = same builder plus `.MessagePrefetchCount(2)` (L297-303).
  10. Assert: for `i` in 0..1: `msg = ReceiveOrNull(consumer2, TimeSpan.FromSeconds(3))` (note **3 s**, L308); `msg.ShouldNotBeNull(); msg.Value().ShouldBe(4 + i)` (L309).

### Row 4 — `#produceCommitTest(boolean enableBatch) (L313-368)`

- DataProvider `enableBatch`: `{true}`, `{false}`. `private`. Also runs in NOBIA.
- Classification: (a).
- Prerequisites: batching on/off, send timeout `0` (disabled), commit waits for un-awaited transactional sends (§2.2), partitioned producer + consumer (exist).
- C#: `[Theory] [InlineData(true)] [InlineData(false)] Commit_GivenTwoTransactionsProducingToPartitionedTopic_ShouldDeliverAllMessagesOnlyAfterCommit(bool enableBatch)`
- Steps:
  1. `topic = CreateTopicName("output"); await Fixture.CreatePartitionedTopic(topic, TopicPartition /*3*/, ct)` (TOPIC_OUTPUT).
  2. Consumer `Schema.ByteArray`, topic, sub `"test"` (Exclusive, Latest) (L316-320); `await consumer.OnStateChangeTo(ConsumerState.Active, ct)` (L321).
  3. Producer `Schema.ByteArray`, `.EnableBatching(enableBatch).SendTimeout(TimeSpan.Zero)` (L323-329).
  4. `txn1 = GetTxn`, `txn2 = GetTxn` (L331-332).
  5. `messageCnt = 1000`; for `i` in 0..999: `await SendAsync(producer, UTF8("Hello Txn - " + i), i % 5 == 0 ? txn1 : txn2)`; `txnMessageCnt++` (L334-343).
  6. Assert `ReceiveOrNull(consumer, 5 s)` is null (L346-347).
  7. Act: `await txn1.Commit(ct); await txn2.Commit(ct)` (L349-350).
  8. Assert: 1000 times `ReceiveOrNull(consumer, 5 s).ShouldNotBeNull()`, `receiveCnt++` (L352-357); `receiveCnt.ShouldBe(1000)` (L358).
  9. `ReceiveOrNull(consumer, 5 s)` is null (L360-361).

### Row 5 — `#produceAbortTest (L370-453)`

- Plain `@Test`. Also runs in NOBIA.
- Classification: (c) partially portable.
  - Ported: no message visible before abort (L396-397) and after abort (L402-403).
  - Substituted: the Awaitility block L404-446 reads, per partition, the broker's `PersistentSubscription` cursor
    (`getMarkDeletedPosition`) and `ManagedLedgerImpl#getLastConfirmedEntry` / `getNextValidPosition`. Substitute with
    admin `GET /admin/v2/persistent/{tnx}/{ns1}/{topic}-partition-{n}/internalStats` (`Fixture.GetInternalStats`) and the
    helper `IsMarkDeleteAtEnd(stats, "test")` (§5): condition holds when
    `cursors.test.markDeletePosition == lastConfirmedEntry` or `NextValidPosition(markDeletePosition) == lastConfirmedEntry`
    (the abort marker is the only entry after the mark-delete position). Java's `assertTrue(exist)` (L443) = the
    stats call succeeds and the cursor `"test"` exists for every partition. Poll every 100 ms, at most 10 s (Awaitility default).
- Prerequisites: send timeout `0`, batching off.
- C#: `[Fact] Abort_GivenProducedMessages_ShouldNotDeliverAndShouldMoveMarkDeleteToEnd`
- Steps:
  1. `topic = CreateTopicName("output"); CreatePartitionedTopic(topic, 3)`.
  2. `txn = GetTxn` (L372); `subName = "test"` (L373).
  3. Producer `Schema.ByteArray`, `.SendTimeout(TimeSpan.Zero).EnableBatching(false)` (L375-380).
  4. For `i` in 0..9: `await producer.NewMessage().Transaction(txn).Send(UTF8("Hello Txn - " + i), ct)` (L382-385).
  5. Consumer `Schema.ByteArray`, `InitialPosition(Earliest)`, sub `"test"` (L387-392); wait `Active` (L393).
  6. Assert null receive (5 s) (L396-397).
  7. Act `await txn.Abort(ct)` (L399).
  8. Assert null receive (5 s) (L402-403).
  9. `await WaitUntil(async () => { foreach partition p in 0..2: stats = await Fixture.GetInternalStats($"{topic}-partition-{p}", ct); stats has cursor "test" (else fail like assertTrue(exist)); if (!IsMarkDeleteAtEnd(stats, "test")) return false; return true; })` (L404-446).

### Row 6 — `#testUnackMessageAfterAckAllMessages(boolean batchSend, boolean batchAck, boolean asyncAck) (L470-521)`

- DataProvider `unackMessagesCountParams` (L455-468), 8 rows in this order:
  `(T,T,T) (T,T,F) (T,F,T) (T,F,F) (F,T,T) (F,T,F) (F,F,T) (F,F,F)`.
- NOBIA: `enabled = false` (NOBIA L47-52) -> skipped.
- Classification: (a) — Java itself reads `admin.topics().getStats(...)` (public admin REST).
- Prerequisites: batching on/off, `BatchingMaxMessages(10)`, ack receipt, list ack with transaction.
- C#: `[Theory]` with 8 `[InlineData(bool, bool, bool)]` in the order above;
  `Commit_GivenAllMessagesAckedWithTransaction_ShouldLeaveNoUnackedMessages(bool batchSend, bool batchAck, bool asyncAck)`
- Steps:
  0. `Assert.SkipWhen(SkipReasonForUnackMessageCountTests is not null, SkipReasonForUnackMessageCountTests!)`.
  1. `messageCount = 50; subName = "s1"; topic = CreateTopicName("tp")` (Java `BrokerTestUtil.newUniqueName(NAMESPACE1 + "/tp")`) (L473-475).
  2. Consumer `Schema.ByteArray`, sub `"s1"`, `Shared`, `AckReceiptEnabled(true)` (L477-478); wait `Active` (L479).
  3. Producer `Schema.ByteArray`, `.EnableBatching(batchSend).BatchingMaxMessages(10)` (L480-481); `await producer.OnStateChangeTo(ProducerState.Connected, ct)` (L482).
  4. For `i` in 0..49: `last = await SendAsync(producer, UTF8(i.ToString()))` (L484-487); `await last` (L488).
  5. `txn = GetTxn` (L491). Receive 50 (5 s each, not null) and collect `MessageId`s (L492-496).
  6. Act:
     - `batchAck`: `await consumer.Acknowledge(msgIds, txn, ct)` (L497-498);
     - else `asyncAck`: `Task? latest = null; foreach id: latest = consumer.Acknowledge(id, txn, ct).AsTask(); await latest!;` (L499-504);
     - else: `foreach id: await consumer.Acknowledge(id, txn, ct)` (L505-509).
  7. `await txn.Commit(ct)` (L510).
  8. Assert: `(await Fixture.GetConsumerUnackedMessages(topic, "s1", 0, ct)).ShouldBe(0)` — Java reads `subscriptions[s1].consumers[0].unackedMessages` (L513-515), immediately, no polling.

### Row 7 — `#testAckWithTransactionReduceUnAckMessageCount(boolean enableBatch) (L523-583)`

- DataProvider `enableBatch`: `{true}`, `{false}`. NOBIA: `enabled = false` (L54-58) -> skipped.
- Classification: (c).
  - Substituted: L561-577 reads the broker's in-memory `PersistentSubscription.getConsumers().get(0).getUnackedMessages()`;
    substitute with admin topic stats `subscriptions[sub].consumers[0].unackedMessages` — the same counter
    (`Consumer#getUnackedMessages`) serialised by `ConsumerStatsImpl`. `assertTrue(flag)` (L577) = the stats call succeeds and the
    subscription has a consumer.
- Prerequisites: batching on/off, `BatchingMaxMessages(10)`, ack receipt.
- C#: `[Theory] [InlineData(true)] [InlineData(false)] Commit_GivenHalfOfMessagesAckedWithTransaction_ShouldReduceUnackedMessageCount(bool enableBatch)`
- Steps:
  0. `Assert.SkipWhen(SkipReasonForUnackMessageCountTests is not null, ...)`.
  1. `messageCount = 50; subName = "testAckWithTransactionReduceUnAckMessageCount"; topic = CreateTopicName($"testAckWithTransactionReduceUnAckMessageCount-{(enableBatch ? "true" : "false")}")` (L526-528).
  2. Consumer `Schema.ByteArray`, sub `subName`, `Shared`, `AckReceiptEnabled(true)` (L530-536); wait `Active` (L537).
  3. Producer `Schema.ByteArray`, `.EnableBatching(enableBatch).BatchingMaxMessages(10)` (L539-544).
  4. 50 times `receipts.Add(await SendAsync(producer, UTF8(i.ToString())))`; `await Task.WhenAll(receipts)` (L546-551).
  5. `txn = GetTxn` (L553). 25 times: `m = ReceiveOrNull(5 s)` (not null) and `await consumer.Acknowledge(m.MessageId, txn, ct)` (L555-558).
  6. Act `await txn.Commit(ct)` (L560).
  7. Assert `(await Fixture.GetConsumerUnackedMessages(topic, subName, 0, ct)).ShouldBe(25)` (L572).

### Rows 8-11 — `txnIndividualAckTest*` (L585-603) + shared helper `txnAckTest(boolean batchEnable, int maxBatchSize, SubscriptionType) (L605-682)`

| Row | Java method | Lines | Arguments | C# `[Fact]` |
|---|---|---|---|---|
| 8 | `txnIndividualAckTestNoBatchAndSharedSub` | L585-588 | `(false, 1, Shared)` | `Acknowledge_GivenNoBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| 9 | `txnIndividualAckTestBatchAndSharedSub` | L590-593 | `(true, 200, Shared)` | `Acknowledge_GivenBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| 10 | `txnIndividualAckTestNoBatchAndFailoverSub` | L595-598 | `(false, 1, Failover)` | `Acknowledge_GivenNoBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |
| 11 | `txnIndividualAckTestBatchAndFailoverSub` | L600-603 | `(true, 200, Failover)` | `Acknowledge_GivenBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit` |

- Four separate `[Fact]`s (Java has four `@Test` methods, not a DataProvider), each one line calling
  `private async Task TxnAckTest(bool batchEnable, uint maxBatchSize, SubscriptionType subscriptionType)`.
  Row 11 starts with `Assert.SkipWhen(SkipReasonForBatchAndFailoverIndividualAck is not null, ...)` (NOBIA override, see §0).
- Classification: (c). Ported: everything except the reflection re-commit (L664-675), which is substituted per §2.5
  (same broker request `CommandEndTxn{COMMIT}` for the ended txn id, same expected `TransactionNotFoundException`).
- Prerequisites: batching on/off, `BatchingMaxMessages(maxBatchSize)`, batch-index ack with transaction (rows 9, 11),
  broker redelivery of aborted pending acks (broker feature, no client work beyond honouring redelivered entries).
- `TxnAckTest` steps:
  1. `normalTopic = CreateTopicName("normal-topic")` (L607).
  2. Consumer `Schema.ByteArray`, sub `"test"`, `.SubscriptionType(subscriptionType)` (L610-614); wait `Active` (L615).
  3. Producer `Schema.ByteArray`, `.EnableBatching(batchEnable).BatchingMaxMessages(maxBatchSize)` (L618-622).
  4. Repeat 2 times (`retryCnt` 0..1, L624):
     1. `txn = GetTxn` (L625); `messageCnt = 1000` (L627).
     2. 1000 times `await SendAsync(producer, UTF8("hello"))` (no transaction) (L629-631).
     3. 1000 times: `m = ReceiveOrNull(5 s)`, `ShouldNotBeNull()`, `await consumer.Acknowledge(m.MessageId, txn, ct)` (L634-639).
     4. Assert `ReceiveOrNull(5 s)` null (L642-643).
     5. `await txn.Abort(ct)` (L646).
     6. `commitTxn = GetTxn` (L649). 1000 times: receive (5 s, not null), `Acknowledge(m.MessageId, commitTxn)` awaited (L650-655).
     7. `await commitTxn.Commit(ct)` (L658).
     8. Assert `ReceiveOrNull(5 s)` null (L661-662).
     9. Re-commit per §2.5: `Should.ThrowAsync<TransactionNotFoundException>(tcClient.Commit(commitTxn.Id))` (L664-675).

### Row 12 — `#testAfterDeleteTopicOtherTopicCanRecover (L684-704)`

- Classification: (a) (admin calls are public admin API in Java too).
- Prerequisites: none new.
- C#: `[Fact] Send_GivenOtherTopicWithSubscriptionWasDeleted_ShouldProduceAndConsume`
- Steps:
  1. `topicOne = CreateTopicName("topic-one")`, `topicTwo = CreateTopicName("topic-two")`, `sub = "test"` (L686-688).
  2. `await Fixture.CreateTopic(topicOne, ct)` (non-partitioned, L689); `await Fixture.CreateSubscription(topicOne, "test", earliest: true, ct)` (L690); `await Fixture.DeleteTopic(topicOne, force: false, ct)` (L691).
  3. Producer `Schema.String` on `topicTwo` (L693); consumer `Schema.String` on `topicTwo`, sub `"test"` (L694-695).
  4. Act `await producer.Send("test", ct)` (L696-697).
  5. Assert `ReceiveOrNull(5 s)` not null and `.Value().ShouldBe("test")` (L698).

### Row 13 — `#txnMessageAckTest (L706-819)`

- Classification: (c).
  - Ported: no delivery before commit; 10 received after commit; acks of even messages; no further delivery;
    `markDeletePosition != lastConfirmedEntry` via admin internal stats (Java also uses `admin.topics().getInternalStats(checkTopic, false)` L758-761);
    `RedeliverUnacknowledgedMessages`; 5 redelivered; no further delivery.
  - Substituted: final broker-internal check L777-812 (same logic as row 5 but **without polling**: Java checks once and
    `fail()`s). Substitute: one `GetInternalStats($"{topic}-partition-0")` call and `IsMarkDeleteAtEnd(stats, "test").ShouldBeTrue()`;
    `assertTrue(exist)` = stats call succeeds and the cursor exists.
- Prerequisites: send timeout `0`, batching off; `IConsumer.RedeliverUnacknowledgedMessages(ct)` (exists).
- C#: `[Fact] Acknowledge_GivenCommittedTransactionalMessages_ShouldAckAndMoveMarkDeleteToEnd`
- Steps:
  1. `topic = CreateTopicName("message-ack-test"); CreatePartitionedTopic(topic, 1)`; `subName = "test"` (L708-709).
  2. Consumer `Schema.ByteArray`, sub `"test"` (Exclusive). Java `acknowledgmentGroupTime(0)` is a no-op (§2.1) (L711-716); wait `Active` (L717).
  3. Producer `Schema.ByteArray`, `.SendTimeout(TimeSpan.Zero).EnableBatching(false)` (L720-725).
  4. `txn = GetTxn` (L727). 10 times `await SendAsync(producer, UTF8("Hello Txn - " + i), txn)` (L729-732).
  5. Assert null receive (5 s) (L736-737).
  6. `await txn.Commit(ct)` (L740).
  7. For `i` in 0..9: receive (5 s, not null), `receiveCnt++`; if `i % 2 == 0`: `await consumer.Acknowledge(m, ct)`, `ackedMessageCount++` (L742-752). Assert `receiveCnt == 10` (L753).
  8. Assert null receive (5 s) (L755-756).
  9. `stats = await Fixture.GetInternalStats($"{topic}-partition-0", ct)`; `stats.cursors.test.markDeletePosition.ShouldNotBe(stats.lastConfirmedEntry)` (L758-761).
  10. `await consumer.RedeliverUnacknowledgedMessages(ct)` (L763).
  11. 5 (= `10 - ackedMessageCount`) times: receive (5 s, not null), `await consumer.Acknowledge(m, ct)`, `receiveCnt++` (L765-771); assert `receiveCnt == 5` (L772).
  12. Assert null receive (5 s) (L774-775).
  13. `stats = GetInternalStats(partition-0)`; `IsMarkDeleteAtEnd(stats, "test").ShouldBeTrue()` (L777-812).

### Rows 14-15 — `txnAckTest{Batch,NoBatch}AndCumulativeSub` (L821-829) + helper `txnCumulativeAckTest(boolean, int, SubscriptionType) (L831-932)`

| Row | Java method | Lines | Arguments | C# `[Fact]` |
|---|---|---|---|---|
| 14 | `txnAckTestBatchAndCumulativeSub` | L821-824 | `(true, 200, Failover)` | `AcknowledgeCumulative_GivenBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit` |
| 15 | `txnAckTestNoBatchAndCumulativeSub` | L826-829 | `(false, 1, Failover)` | `AcknowledgeCumulative_GivenNoBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit` |

- Classification: (c): reflection re-commit (L911-922) substituted per §2.5; all else ported.
- Prerequisites: batching on/off, `BatchingMaxMessages`, `BatchingMaxPublishDelay(1 s)`, cumulative ack with
  transaction incl. batch index (`ack_set` "up to index"), ack timeout `AckTimeout(1 min)` (configured; it must
  not fire during the test), `RedeliverUnacknowledgedMessages`.
- `private async Task TxnCumulativeAckTest(bool batchEnable, uint maxBatchSize, SubscriptionType subscriptionType)` steps:
  1. `normalTopic = CreateTopicName("normal-topic")` (L833).
  2. Consumer `Schema.ByteArray`, sub `"test"`, `.SubscriptionType(subscriptionType).AckTimeout(TimeSpan.FromMinutes(1))` (L836-841); wait `Active` (L842).
  3. Producer `Schema.ByteArray`, `.EnableBatching(batchEnable).BatchingMaxMessages(maxBatchSize).BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))` (L845-850).
  4. Repeat 2 times (L852):
     1. `abortTxn = GetTxn` (L853); 1000 times `await SendAsync(producer, UTF8("hello"))` (L854-858).
     2. `await Task.Delay(TimeSpan.FromMilliseconds(1000), ct)` (L860).
     3. For `i` in 0..999: `message = ReceiveOrNull(5 s)`, not null; if `i % 3 == 0`: `await consumer.AcknowledgeCumulative(message.MessageId, abortTxn, ct)` (L861-872).
     4. Assert `await Should.ThrowAsync<TransactionConflictException>(() => consumer.AcknowledgeCumulative(message.MessageId, abortTxn, ct))` — same (last, already cumulatively acked) id again (L873-878).
     5. Assert `TransactionConflictException` for `consumer.AcknowledgeCumulative(new MessageId(message.MessageId.LedgerId, message.MessageId.EntryId - 1, -1, -1), abortTxn, ct)` (L880-888; Java `newMessageId(ledgerId, entryId - 1, partitionIndex = -1)` = non-batch id, batch index -1).
     6. Assert null receive (5 s) (L891-892).
     7. `await abortTxn.Abort(ct)` (L894); `await consumer.RedeliverUnacknowledgedMessages(ct)` (L895).
     8. `commitTxn = GetTxn` (L896). For `i` in 0..999: receive (5 s, not null); if `i % 3 == 0` `AcknowledgeCumulative(id, commitTxn)` awaited (L897-908).
     9. `await commitTxn.Commit(ct)` (L910).
     10. Re-commit per §2.5 -> `TransactionNotFoundException` (L911-922).
     11. Assert null receive (5 s) (L924-925).

### Row 16 — `#txnMetadataHandlerRecoverTest (L941-996)`

- `@SuppressWarnings("deprecation")` L941, `@Test` L943.
- Classification: (a): Java uses `PulsarClientImpl#getTransactionCoordinatorClient()` — public on the impl class; the C#
  counterpart is the public `PulsarClient.TransactionCoordinatorClient` property (`03-public-api.md` §3.8), which exists for this test.
- Prerequisites: batching (Java default on), send timeout `0`, `((PulsarClient) client).TransactionCoordinatorClient!.Commit(TransactionId, ct)`; an explicit start of the lazy TC client (see step 5).
- C#: `[Fact] Commit_GivenTransactionsCommittedByIdFromAnotherClient_ShouldDeliverAllMessages`
- Steps:
  1. `topic = CreateTopicName("tc-metadata-handler-recover")` (L945).
  2. Producer `Schema.ByteArray`, `.EnableBatching(true).SendTimeout(TimeSpan.Zero)` (L947-950).
  3. `txnCnt = 20; messageCnt = 10` (L954-955). For each of 20: `txn = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(ct)` (L957-959); 10 times `await producer.NewMessage().Transaction(txn).Send(UTF8("Hello"), ct)` (L960-962); `ids.Add(txn.Id)` (L963).
  4. `await using var recoverClient = CreateClient();` (new client, transactions enabled) (L967-971).
  5. `tcClient = ((PulsarClient) recoverClient).TransactionCoordinatorClient!;` then start it: `await tcClient.EnsureStarted(ct);` (L973). Java starts the TC client inside `build()`; DotPulsar starts it lazily on the first `Create()`, and `recoverClient` has created no transaction. `EnsureStarted` is a member of `ITransactionCoordinatorClient` in `05-components/transaction-coordinator-client.md` L21 but is not in the 03-public-api list; if it is not kept on the interface, the TC client's `Commit` must start the client itself (Open decision 10). Do **not** create a throwaway transaction to start it, because that would add a transaction Java does not have.
  6. Act: `foreach id: await tcClient.Commit(id, ct)` (L974-976).
  7. Consumer on `recoverClient`, `Schema.ByteArray`, sub `"test"`, `InitialPosition(Earliest)` (L979-983); wait `Active` (L984).
  8. Assert: 200 times `ReceiveOrNull(5 s).ShouldNotBeNull()` (L986-989).

### Row 17 — `#produceTxnMessageOrderTest (L998-1038)`

- Classification: (a).
- Prerequisites: batching (Java default on), send timeout `0`, `ProducerName`, ordered fire-and-forget send (§2.2), commit waits for sends.
- C#: `[Fact] Commit_GivenThousandTransactionalMessages_ShouldDeliverInSendOrder`
- Steps:
  1. `topic = CreateTopicName("txn-produce-order")` (L1000).
  2. Consumer `Schema.ByteArray`, sub `"test"` (L1003-1006); wait `Active` (L1007).
  3. Producer `Schema.ByteArray`, `.EnableBatching(true).SendTimeout(TimeSpan.Zero).ProducerName("txn-publish-order")` (L1010-1014).
  4. Repeat 10 times (`ti`, L1016):
     1. `txn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(2)).Create(ct)` (L1017-1020).
     2. For `i` in 0..999: `await SendAsync(producer, UTF8(i.ToString()), txn)` (L1022-1024).
     3. `await txn.Commit(ct)` (L1025).
     4. For `i` in 0..999: `m = ReceiveOrNull(5 s)`, not null, `int.Parse(UTF8String(m.Value())).ShouldBe(i)` (L1027-1031).

### Row 18 — `#produceAndConsumeCloseStateTxnTest (L1040-1148)`

- Classification: (c).
  - Ported: send after commit -> `InvalidTransactionStatusException` (L1069-1074); second commit ->
    `InvalidTransactionStatusException` (L1076-1081); same two for the consume side (L1087-1099).
  - Substituted (1): L1105-1122 looks up the TC's `TransactionMetadataStore` in the broker and polls `getTxnMeta` until it
    fails. Substitute: poll admin `GET /admin/v3/transactions/transactionMetadata/{most}/{least}` until it answers **404**
    (`TransactionsBase#internalGetTransactionMetadata` maps `TransactionNotFoundException` to `NOT_FOUND`), every 100 ms,
    at most 10 s. Helper `Fixture.TransactionMetadataExists(id, ct)` (§5).
  - Substituted (2): L1124-1130 builds a `TransactionImpl` by reflection with the timed-out txn id and a 5 ms timeout.
    Substitute: the public constructor of the internal class
    `new DotPulsar.Internal.Transactions.Transaction(((PulsarClient) client).TransactionCoordinatorClient!, timeoutTxn.Id, TimeSpan.FromMilliseconds(5))`
    (`05-components/transaction.md` §3). Java argument order `(client, 5, least, most)` maps to the `TransactionId`.
    The commit must start within 5 ms of construction (otherwise the client timer flips to `TimedOut` and the commit fails
    with `InvalidTransactionStatusException`); Java has the same race.
  - Ported after substitution: commit -> `TransactionNotFoundException` (L1132-1137); state -> `TransactionState.Error` (L1138-1141, reflection read -> `CurrentState`).
- Prerequisites: send timeout `0`, `ProducerName`.
- C#: `[Fact] SendAndAcknowledge_GivenEndedOrTimedOutTransaction_ShouldThrow`
- Steps:
  1. `topic = CreateTopicName("txn-close-state")` (L1042).
  2. Consumer `Schema.ByteArray`, sub `"test"` (L1045-1048) (no connected wait in Java).
  3. Producer `Schema.ByteArray`, `.SendTimeout(TimeSpan.Zero).ProducerName("txn-close-state")` (L1051-1055).
  4. `produceTxn` and `consumeTxn` with `Timeout(TimeSpan.FromSeconds(2))` (L1057-1065).
  5. `await producer.NewMessage().Transaction(produceTxn).Send(UTF8("Hello Pulsar!"), ct)` (L1067); `await produceTxn.Commit(ct)` (L1068).
  6. Assert `InvalidTransactionStatusException` for `producer.NewMessage().Transaction(produceTxn).Send(UTF8("Hello Pulsar!"), ct)` (L1069-1074).
  7. Assert `InvalidTransactionStatusException` for `produceTxn.Commit(ct)` (L1076-1081).
  8. `message = ReceiveOrNull(5 s)` (not null); `await consumer.Acknowledge(message.MessageId, consumeTxn, ct)`; `await consumeTxn.Commit(ct)` (L1084-1086).
  9. Assert `InvalidTransactionStatusException` for `consumer.Acknowledge(message.MessageId, consumeTxn, ct)` (L1087-1092).
  10. Assert `InvalidTransactionStatusException` for `consumeTxn.Commit(ct)` (L1094-1099).
  11. `timeoutTxn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(ct)` (L1101-1104).
  12. `await WaitUntil(async () => !await Fixture.TransactionMetadataExists(timeoutTxn.Id, ct))` (L1105-1122).
  13. `uut = new Transaction(tcClient, timeoutTxn.Id, TimeSpan.FromMilliseconds(5))` (L1124-1130).
  14. Assert `await Should.ThrowAsync<TransactionNotFoundException>(async () => await uut.Commit(ct))` (L1132-1137).
  15. Assert `uut.CurrentState.ShouldBe(TransactionState.Error)` (L1138-1141).
  16. Cleanup (L1144): Java calls `timeoutTxn.abort()` without waiting; C#: `_ = timeoutTxn.Abort(ct).AsTask().ContinueWith(t => _ = t.Exception, TaskScheduler.Default);` or omit — the result is not asserted (Open decision 7).

### Row 19 — `#testTxnTimeoutAtTransactionMetadataStore (L1150-1169)`

- Classification: (c). Java calls the broker's `TransactionMetadataStoreService#newTransaction(TransactionCoordinatorID(0), 1 /*ms*/, null)`
  directly and compares the sum of `getMetadataStoreStats().timeoutCount` over all TC stores before and after.
  - Substituted (1): create the transaction through the client on TC 0 (standalone has only TC 0):
    `client.NewTransaction().Timeout(TimeSpan.FromMilliseconds(1)).Create(ct)`, which sends `CommandNewTxn` with `txn_ttl_seconds = 1` (the field carries **milliseconds** in Java: `TransactionMetaStoreHandler` L299 passes `unit.toMillis(timeout)` to `Commands#newTxn(tcId, requestId, ttlMillis)` L1525-1529), i.e. the same 1 ms TTL as the Java call. The client-side timer is irrelevant (not asserted).
    The owner is the client's role instead of `null`; not observable here.
  - Substituted (2): `getTxnMeta` polling -> admin `transactionMetadata` until 404 (as row 18).
  - Substituted (3): `timeoutCount` -> broker Prometheus metric `pulsar_txn_timeout_total` (`TransactionAggregator.java` L257,
    sourced from `TransactionMetadataStoreStats.timeoutCount`, L102), summed over all series, read from `GET {AdminUrl}/metrics/`.
    Assert equality `after == before + 1` exactly as Java (L1168); because metrics may be generated from a cache,
    poll up to 10 s until `after == before + 1`, then assert equality (Open decision 5).
- C#: `[Fact] Create_GivenTimeoutElapsesAtCoordinator_ShouldIncrementCoordinatorTimeoutCount`
- Steps:
  1. `before = await Fixture.GetTransactionTimeoutTotal(ct)` (L1152-1155).
  2. Act `txn = await client.NewTransaction().Timeout(TimeSpan.FromMilliseconds(1)).Create(ct)` (L1156-1157).
  3. `await WaitUntil(async () => !await Fixture.TransactionMetadataExists(txn.Id, ct))` (L1158-1165).
  4. Assert: `await WaitUntil(async () => await Fixture.GetTransactionTimeoutTotal(ct) == before + 1)`; then `(await Fixture.GetTransactionTimeoutTotal(ct)).ShouldBe(before + 1)` (L1166-1168).
- Risk (same as Java): other tests in the collection leave transactions open with a 10 min timeout (rows 1, 3); if one
  times out during this test the count is `+2`. Java shares this risk with its shared broker.

### Row 20 — `#transactionTimeoutTest (L1171-1214)`

- Classification: (a).
- Prerequisites: send timeout `0`, batching off, `ProducerName`.
- C#: `[Fact] Acknowledge_GivenTransactionTimesOut_ShouldRedeliverMessage`
- Steps:
  1. `topic = CreateTopicName("txn-timeout")` (L1173).
  2. Consumer `Schema.String`, sub `"test"` (L1176-1179). Producer `Schema.String`, `.SendTimeout(TimeSpan.Zero).EnableBatching(false).ProducerName("txn-timeout")` (L1182-1187).
  3. `await producer.Send("Hello Pulsar!", ct)` (L1189).
  4. `consumeTimeoutTxn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(7)).Create(ct)` (L1191-1194).
  5. `message = ReceiveOrNull(5 s)` (not null) (L1196); `await consumer.Acknowledge(message.MessageId, consumeTimeoutTxn, ct)` (L1198).
  6. Assert `ReceiveOrNull(5 s)` null (L1200-1201).
  7. Assert `reReceive = ReceiveOrNull(5 s)` not null (transaction timed out at ~7 s and the TC aborted it) (L1203);
     `reReceive.Value().ShouldBe(message.Value())` (L1205); `reReceive.MessageId.ShouldBe(message.MessageId)` (L1207).
  8. Cleanup L1210: `consumeTimeoutTxn.abort()` not awaited in Java — same handling as row 18 step 16.

### Row 21 — `#txnTransactionRedeliverNullDispatcher(CommandAck.AckType ackType) (L1221-1283)` — **Excluded**

- DataProvider `ackType` (L1216-1219): `Cumulative`, `Individual`.
- Classification: (b) broker-internal only. The test sets the private field `PersistentSubscription.dispatcher` to
  `null` by reflection on the in-process broker (L1257-1275) and checks that `txn.abort()` still succeeds, i.e. that the
  broker's pending-ack abort path tolerates a null dispatcher. There is no client-observable way to put a real broker's
  subscription into "no dispatcher" state while it has a pending transactional ack (the dispatcher is created with the
  first consumer and never nulled). The client part (ack with txn, abort) is already covered by rows 8-11 and 14-15.
  Excluded: regression guard for broker code, not client behaviour.

### Row 22 — `#oneTransactionOneTopicWithMultiSubTest (L1285-1347)`

- Classification: (c). Java reads the TC's `TxnMeta.ackedPartitions()` (list of `TransactionSubscription(topic, sub)`)
  from the in-process broker (L1321-1339). Substitute: admin `GET /admin/v3/transactions/transactionMetadata/{most}/{least}`
  -> `ackedPartitions` (map topic -> map subscription -> stats, `TransactionMetadata.java` L43; built from
  `txnMeta.ackedPartitions()` in `TransactionsBase` L232-298). The Java list-size-2 assertion becomes: total number of
  (topic, subscription) pairs is 2; Java's two order-dependent branches (L1331-1336) become: the set of subscription names
  for `topic` equals `{ "test1", "test2" }`. `assertTrue(flag)` = the metadata call succeeds (TC 0 owns the txn).
- Prerequisites: send timeout `0`, batching off.
- C#: `[Fact] Acknowledge_GivenOneTransactionAndTwoSubscriptions_ShouldRegisterBothSubscriptions`
- Steps:
  1. `topic = CreateTopicName("oneTransactionOneTopicWithMultiSubTest")`; `subName1 = "test1"`, `subName2 = "test2"` (L1287-1289).
  2. `consumer1` (`Schema.ByteArray`, sub `"test1"`) wait `Active`; `consumer2` (sub `"test2"`) wait `Active` (L1291-1306). `acknowledgmentGroupTime(0)` no-op.
  3. Producer `Schema.ByteArray`, `.SendTimeout(TimeSpan.Zero).EnableBatching(false)` (L1309-1314).
  4. `messageId = await producer.Send(UTF8("Hello Pulsar"), ct)` (L1316); `txn = GetTxn` (L1317).
  5. Act: `await consumer1.Acknowledge(messageId, txn, ct); await consumer2.Acknowledge(messageId, txn, ct)` (L1318-1319) (ack by id without receiving, as Java).
  6. Assert: `meta = await Fixture.GetTransactionMetadata(txn.Id, ct)`; `acked = meta.GetProperty("ackedPartitions")`;
     pairs count over all topics `== 2`; `acked.GetProperty(topic)` property names `ShouldBe(new[] { "test1", "test2" }, ignoreOrder: true)` (L1321-1339).
  7. `await txn.Abort(ct)` (L1342).

### Row 23 — `#testTxnTimeOutInClient (L1349-1386)`

- Classification: (a).
- Prerequisites: send timeout `0`, batching off, `ProducerName`, `ConsumerName`, client-side transaction timer -> `TimedOut`.
- C#: `[Fact] SendAndAcknowledge_GivenTransactionTimedOutInClient_ShouldThrowInvalidTransactionStatusException`
- Steps:
  1. `topic = CreateTopicName("testTxnTimeOutInClient")` (L1351).
  2. Producer `Schema.String`, `.ProducerName("testTxnTimeOut_producer").SendTimeout(TimeSpan.Zero).EnableBatching(false)` (L1353-1354).
  3. Consumer `Schema.String`, `.ConsumerName("testTxnTimeOut_consumer")`, sub `"testTxnTimeOut_sub"` (L1356-1357).
  4. `transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(ct)` (L1359-1360).
  5. `await producer.Send(string.Empty, ct)` — Java `producer.newMessage().send()` without a value sends an empty payload (L1361).
  6. Wait (Awaitility default, at most 10 s) until `transaction.CurrentState == TransactionState.TimedOut`:
     `await WaitUntil(() => Task.FromResult(transaction.CurrentState == TransactionState.TimedOut))` (L1362-1364)
     (or `await transaction.State.OnStateChangeTo(TransactionState.TimedOut, ct)` with `ct` linked to a 10 s timeout).
  7. Assert `InvalidTransactionStatusException` for `producer.NewMessage().Transaction(transaction).Send(string.Empty, ct)` (L1366-1372).
  8. `message = ReceiveOrNull(5 s)`, `ShouldNotBeNull()`; assert `InvalidTransactionStatusException` for `consumer.Acknowledge(message.MessageId, transaction, ct)` (L1373-1380).

### Row 24 — `#testCumulativeAckRedeliverMessages (L1388-1459)`

- Classification: (a).
- Prerequisites: batching (Java default on), send timeout `0`, cumulative ack with transaction (Exclusive), `RedeliverUnacknowledgedMessages`.
- C#: `[Fact] AcknowledgeCumulative_GivenConflictingTransactionThenAbort_ShouldRedeliverMessages`
- Steps:
  1. `topic = CreateTopicName("testCumulativeAckRedeliverMessages")`; `count = 5; transactionCumulativeAck = 3` (L1390-1393).
  2. Consumer `Schema.ByteArray`, sub `"test"` (Exclusive) (L1395-1398). Producer `Schema.ByteArray`, `.EnableBatching(true).SendTimeout(TimeSpan.Zero)` (L1401-1404).
  3. 5 times `await producer.Send(UTF8(i.ToString()), ct)` (L1407-1409).
  4. `transaction = GetTxn; invalidTransaction = GetTxn` (L1411-1412).
  5. 3 times `message = ReceiveOrNull(5 s)` (L1415-1417); assert `message.Value().ShouldBe(UTF8("2"))` (L1420).
  6. `await consumer.AcknowledgeCumulative(message.MessageId, transaction, ct)` (L1423).
  7. Assert `TransactionConflictException` for `consumer.AcknowledgeCumulative(message.MessageId, invalidTransaction, ct)` (L1426-1430).
  8. `await transaction.Abort(ct)`; `await consumer.RedeliverUnacknowledgedMessages(ct)` (L1432-1434).
  9. 5 times `message = ReceiveOrNull(5 s)` (L1438-1440). `commitTransaction = GetTxn` (L1442).
  10. Assert `message.Value().ShouldBe(UTF8("4"))` (L1445).
  11. `await consumer.AcknowledgeCumulative(message.MessageId, commitTransaction, ct)`; `await commitTransaction.Commit(ct)` (L1447-1449).
  12. Assert null receive (5 s) (L1452-1453).

### Row 25 — `#testSendTxnMessageTimeout (L1461-1498)`

- Classification: (c). Java replaces the producer's `ClientCnx` with a Mockito mock (L1473-1483) so that the
  `CommandSend` is swallowed while the producer believes it is connected and the transaction's TC requests still use the
  real connection. Expected: `send()` fails with `PulsarClientException.TimeoutException` after the 1 s send timeout (L1486-1492).
  - Substitution: two clients and an in-test black-hole TCP relay (`BlackHoleRelay`, §4):
    - `txnClient = CreateClient()` connects directly to the broker; it owns the transaction, so `CommandNewTxn` and
      `CommandAddPartitionToTxn` (sent through the transaction's own TC client, as in Java `TransactionImpl#registerProducedTopic`)
      are unaffected.
    - `producerClient` connects through the relay. After the producer is connected, `relay.SwallowSendCommands()` makes the relay
      read and discard all further client->broker bytes while keeping both sockets open, which is the mocked-`cnx`
      behaviour ("send message can't receive response"). DotPulsar's `ConnectionPool` reuses the service-URL connection when
      the lookup result is a loopback address (standalone in Docker, `src/DotPulsar/Internal/ConnectionPool.cs` L115-118),
      so the producer's traffic stays on the relay connection.
  - Ported: `Send` with the transaction throws `OperationTimeoutException`; the transaction is then aborted successfully (L1495).
  - Dropped: nothing. (Toxiproxy is an equivalent alternative if the fixture adds it; it would cut the TC traffic too unless
    the transaction is created on a direct client, so the two-client layout stays.)
- Prerequisites: send timeout `TimeSpan.FromSeconds(1)` that covers an op waiting for its receipt; batching (Java default on —
  the single message is flushed after 1 ms); producer using a transaction created by another `PulsarClient` instance
  (allowed: the check is "created by DotPulsar", `02-architecture.md` §5.3 rule applies to acks; sends must accept it too).
- C#: `[Fact] Send_GivenTransactionalMessageWithoutReceipt_ShouldThrowOperationTimeoutException`
- Steps:
  1. `topic = CreateTopicName("testSendTxnMessageTimeout")` (L1463).
  2. `await using var relay = await BlackHoleRelay.Start(Fixture.ServiceUrl, ct)`; `await using var producerClient = PulsarClient.Builder().Authentication(Fixture.Authentication).ServiceUrl(relay.ServiceUrl).ExceptionHandler(_testOutputHelper.Log).Build();`
     `await using var txnClient = CreateClient();`
  3. Producer on `producerClient`, `Schema.ByteArray`, `.EnableBatching(true).SendTimeout(TimeSpan.FromSeconds(1))` (L1465-1468); `await producer.OnStateChangeTo(ProducerState.Connected, ct)` (the Java producer is connected when `create()` returns).
  4. `transaction = await txnClient.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(ct)` (L1470-1471).
  5. `relay.SwallowSendCommands()` (L1473-1483).
  6. Act/Assert: `await Should.ThrowAsync<OperationTimeoutException>(async () => await producer.NewMessage().Transaction(transaction).Send(UTF8("Hello Pulsar!"), ct))` (L1486-1492).
  7. `await transaction.Abort(ct)` (must succeed; within the 5 s transaction timeout) (L1495).

### Row 26 — `#testAckWithTransactionReduceUnackCountNotInPendingAcks (L1500-1543)`

- Classification: (c). Java reads `getBrokerService().getTopic(topic).getSubscription(subName).getConsumers().get(0).getUnackedMessages()`
  (L1535-1536). Substitute: admin topic stats `subscriptions.test.consumers[0].unackedMessages` (`Fixture.GetConsumerUnackedMessages`), read once, immediately after the ack.
- Prerequisites: batching (Java default on) with `BatchingMaxPublishDelay(1 s)`, `SendTimeout(1 s)`, batch-index ack with
  transaction (ack of index 1 only), Shared.
- C#: `[Fact] Acknowledge_GivenOneBatchIndexAckedWithTransaction_ShouldReduceUnackedCountByOne`
- Steps:
  1. `topic = CreateTopicName("testAckWithTransactionReduceUnackCountNotInPendingAcks")`; `subName = "test"` (L1502-1503).
  2. Producer `Schema.ByteArray`, `.EnableBatching(true).BatchingMaxPublishDelay(TimeSpan.FromSeconds(1)).SendTimeout(TimeSpan.FromSeconds(1))` (L1505-1509).
  3. Consumer `Schema.ByteArray`, `Shared`, sub `"test"` (L1512-1516).
  4. 5 times `await SendAsync(producer, UTF8(i.ToString()))` — one batch (L1519-1521).
  5. 5 times `messageIds.Add(ReceiveOrNull(5 s)!.MessageId)` (not null) (L1523-1528).
  6. `txn = GetTxn`; Act `await consumer.Acknowledge(messageIds[1], txn, ct)` (L1531-1532).
  7. Assert `(await Fixture.GetConsumerUnackedMessages(topic, "test", 0, ct)).ShouldBe(4)` (L1535-1536).
  8. `await txn.Abort(ct)` (L1539).

### Row 27 — `#testSendTxnAckMessageToDLQ (L1545-1608)`

- Classification: (c).
  - Ported: message received twice (redelivery counts 0 and 1), not received a third time (L1595), DLQ consumer receives it (L1599-1600).
  - Dropped with replacement: `assertEquals(((ConsumerImpl<?>) consumer).getAvailablePermits(), 3)` (L1597) asserts Java's
    client-internal flow-permit counter (permits handed back for the message that went to the DLQ instead of the
    application). DotPulsar has no such public counter and its flow model differs. Replace with a unit test in the DLQ
    prerequisite spec: "a message skipped because `RedeliveryCount > MaxRedeliverCount` returns one flow permit"
    (`java:pulsar-client/.../ConsumerImpl.java` L1717-1731 `increaseAvailablePermits(cnx)`), and in this test indirectly
    covered by the DLQ consumer receiving the message.
- Prerequisites: DLQ (`DeadLetterPolicy { MaxRedeliverCount = 1 }`; default DLQ topic `{topic}-{subscription}-DLQ`,
  `RetryMessageUtil.getDLQTopic` L39-41 / `DLQ_GROUP_TOPIC_SUFFIX = "-DLQ"` L33; messages with `RedeliveryCount > MaxRedeliverCount` are
  not delivered but published to the DLQ and acked; `RedeliveryCount >= MaxRedeliverCount` are remembered as DLQ candidates,
  `ConsumerImpl` L1717-1731), batching off, `SendTimeout(1 s)`.
- C#: `[Fact] Abort_GivenMessageExceedsMaxRedeliverCount_ShouldSendMessageToDeadLetterTopic`
- Steps:
  1. `topic = CreateTopicName("testSendTxnAckMessageToDLQ")`; `subName = "test"`; `value = "test"` (L1547-1549); `dlqTopic = $"{topic}-test-DLQ"`.
  2. Producer `Schema.ByteArray`, `.EnableBatching(false).SendTimeout(TimeSpan.FromSeconds(1))` (L1551-1555).
  3. Consumer `Schema.ByteArray`, `Shared`, `.DeadLetterPolicy(new DeadLetterPolicy { MaxRedeliverCount = 1 })`, sub `"test"` (L1558-1564).
  4. `deadLetterConsumer` on `dlqTopic`, `Shared`, same `DeadLetterPolicy`, sub `"test"` (L1567-1572).
  5. `await producer.Send(UTF8(value), ct)` (L1574).
  6. `transaction = Create(Timeout 1 min)` (L1575-1576); receive (5 s, not null) and `Acknowledge(id, transaction)` (L1579-1580); `await transaction.Abort(ct)` (L1582).
  7. `transaction = Create(Timeout 5 min)` (L1584-1585); receive (5 s, not null; redelivery count 1) and `Acknowledge(id, transaction)` (L1588-1589); `await transaction.Abort(ct)` (L1591).
  8. Assert `ReceiveOrNull(consumer, 5 s)` null (L1595).
  9. (L1597 dropped, see above.)
  10. Assert `m = ReceiveOrNull(deadLetterConsumer, 5 s)` not null and `UTF8String(m.Value()).ShouldBe("test")` (L1599-1600).

### Row 28 — `#testSendTxnAckBatchMessageToDLQ (L1610-1683)`

- Classification: (c): `getAvailablePermits() == 6` (L1670) dropped and replaced as in row 27; all else ported.
- Prerequisites: DLQ, batching (Java default on; two un-awaited sends form one batch of 2), batch-index ack with
  transaction, `SendTimeout(1 s)`.
- C#: `[Fact] Abort_GivenBatchMessageExceedsMaxRedeliverCount_ShouldSendBatchToDeadLetterTopic`
- Steps:
  1. `topic = CreateTopicName("testSendTxnAckBatchMessageToDLQ")`; `subName = "test"`; `value1 = "test1"`, `value2 = "test2"` (L1612-1615); `dlqTopic = $"{topic}-test-DLQ"`.
  2. Producer `Schema.ByteArray`, `.EnableBatching(true).SendTimeout(TimeSpan.FromSeconds(1))` (L1617-1620).
  3. Consumer and `deadLetterConsumer` exactly as row 27 steps 3-4 (L1623-1637).
  4. `await SendAsync(producer, UTF8(value1)); await SendAsync(producer, UTF8(value2));` (L1639-1640).
  5. `transaction = Create(Timeout 1 min)` (L1641-1642).
  6. `message = ReceiveOrNull(5 s)` not null; `UTF8String(message.Value()).ShouldBe("test1")` (L1644-1645); `Acknowledge(message.MessageId, transaction)` (L1647); `await transaction.Abort(ct)` (L1649).
  7. 3 times `message = ReceiveOrNull(5 s)` (value2 of the first delivery, then value1 and value2 redelivered with count 1) (L1653-1655).
  8. `transaction = Create(Timeout 5 min)` (L1657-1658); assert `UTF8String(message.Value()).ShouldBe("test2")` (L1660); `Acknowledge(message.MessageId, transaction)` (L1662); `await transaction.Abort(ct)` (L1664).
  9. Assert `ReceiveOrNull(consumer, 5 s)` null (L1668).
  10. (L1670 dropped.)
  11. Assert DLQ consumer receives `"test1"` then `"test2"` (5 s each, not null) (L1672-1675).

### Row 29 — `#testDelayedTransactionMessages (L1685-1748)`, `@Test(groups = "flaky")`

- Classification: (a).
- Prerequisites: batching off, delayed delivery `DeliverAt` (exists; Java `deliverAfter(7, SECONDS)` = now + 7 s at build time), Failover + Shared subscriptions.
- C#: `[Fact] Commit_GivenDelayedTransactionalMessages_ShouldDeliverImmediatelyToFailoverAndDelayedToShared` with comment `// Upstream group: flaky` (Open decision 3).
- Steps:
  1. `topic = CreateTopicName("testDelayedTransactionMessages")` (L1687).
  2. `failoverConsumer` `Schema.String`, sub `"failover-sub"`, `Failover` (L1690-1694); `sharedConsumer` `Schema.String`, sub `"shared-sub"`, `Shared` (L1697-1701).
  3. Producer `Schema.String`, `.EnableBatching(false)` (L1704-1707).
  4. `transaction = Create(Timeout 10 s)` (L1709-1710).
  5. For `i` in 0..9: `receipts.Add(await SendAsync(producer, $"msg-{i}", transaction, deliverAt: DateTimeOffset.UtcNow.AddSeconds(7)))` (L1711-1716).
  6. `await Task.WhenAll(receipts)` (Java `producer.flush()`, L1718).
  7. Act `await transaction.Commit(ct)` (L1720).
  8. Assert for `i` in 0..9: `ReceiveOrNull(failoverConsumer, 5 s)` not null, `.Value().ShouldBe($"msg-{i}")` (L1722-1725).
  9. Assert `ReceiveOrNull(sharedConsumer, 5 s)` null (L1729-1730).
  10. 10 times add `ReceiveOrNull(sharedConsumer, 5 s)!.Value()` (not null) to a `SortedSet<string>` (L1732-1736).
  11. Assert set count 10 and contains every `$"msg-{i}"` (L1738-1741).

---

## 4. Test helper: `BlackHoleRelay` (row 25 only)

`tests/DotPulsar.Tests/Transactions/BlackHoleRelay.cs`, `public sealed class BlackHoleRelay : IAsyncDisposable`:

- `static Task<BlackHoleRelay> Start(Uri brokerServiceUrl, CancellationToken)`: `TcpListener(IPAddress.Loopback, 0)`; for every accepted
  client socket, open a socket to `brokerServiceUrl.Host:Port` and pump bytes both ways.
- `Uri ServiceUrl` => `pulsar://127.0.0.1:{listenerPort}`.
- `void SwallowSendCommands()`: from now on the client->broker pump keeps reading from the client but discards the bytes
  (does not close either socket); broker->client keeps flowing.
- `DisposeAsync()`: stop the listener, close all sockets.

---

## 5. Fixture / admin helpers required by these tests

Already in [09-test-infrastructure](../09-test-infrastructure.md): `CreateTopicName(string)`, `CreateTopic(string, ct)`, `CreatePartitionedTopic(string, int, ct)`,
`GetTopicStats(string, ct)`, `GetInternalStats(string, ct)`, `GetTransactionMetadata(TransactionId, ct)`, `GetStatusCode(string, ct)`,
`ReceiveOrNull<T>(IReceive<IMessage<T>>, TimeSpan, ct)`, `CreateClient(IHandleException?, bool)`, `ServiceUrl`, `AdminUrl`, `Authentication`.

To add:

| Signature | Implementation | Used by |
|---|---|---|
| `Task<long> GetConsumerUnackedMessages(string topic, string subscription, int consumerIndex, CancellationToken cancellationToken)` | `GetTopicStats(topic)` -> `subscriptions.<sub>.consumers[consumerIndex].unackedMessages` (Java `getConsumers().get(0).getUnackedMessages()`; the existing `GetUnackedMessages` reads the subscription total, which is equal only with one consumer) | rows 6, 7, 26 |
| `Task CreateSubscription(string topic, string subscription, bool earliest, CancellationToken cancellationToken)` | `bin/pulsar-admin topics create-subscription {topic} -s {subscription} -m {(earliest ? "earliest" : "latest")}` (Java `createSubscription(topic, "test", MessageId.earliest)`) | row 12 |
| `Task DeleteTopic(string topic, bool force, CancellationToken cancellationToken)` | `bin/pulsar-admin topics delete {topic}{(force ? " -f" : "")}` | row 12 |
| `Task<bool> TransactionMetadataExists(TransactionId transactionId, CancellationToken cancellationToken)` | `GetStatusCode("/admin/v3/transactions/transactionMetadata/{most}/{least}")`: `200` -> true, `404` -> false, anything else -> throw | rows 18, 19 |
| `Task<long> GetTransactionTimeoutTotal(CancellationToken cancellationToken)` | `GET /metrics/` (with the bearer header), sum the values of every line starting with `pulsar_txn_timeout_total` (`TransactionAggregator.java` L257) | row 19 |
| `static bool IsMarkDeleteAtEnd(JsonElement internalStats, string subscription)` (static helper in the test base) | parse `lastConfirmedEntry` and `cursors.<sub>.markDeletePosition` (`"ledgerId:entryId"`); true if equal; otherwise compute `NextValidPosition(md)`: if `md.ledger == lac.ledger` -> `(md.ledger, md.entry + 1)`; else look up `md.ledger` in `ledgers[]`: if `md.entry + 1 < entries` -> `(md.ledger, md.entry + 1)`, else `(next ledger id in ledgers[] with entries > 0 or == lac.ledger, 0)` (mirrors `ManagedLedgerImpl#getNextValidPosition`); true if that equals `lastConfirmedEntry` | rows 5, 13 |

---

## 6. Prerequisite API used by these tests

All names are the ones the prerequisite specs must use (append to the builder interfaces per `01-conventions.md` §7).

| API | Java counterpart | Rows |
|---|---|---|
| `IProducerBuilder<T>.EnableBatching(bool enableBatching)` | `enableBatching` (default **true** in Java) | 1, 2, 3, 4, 5, 6, 7, 8-11, 13-15, 16, 17, 20, 22, 23, 24, 25, 26, 27, 28, 29 |
| `IProducerBuilder<T>.BatchingMaxMessages(uint batchingMaxMessages)` (default 1000) | `batchingMaxMessages` | 1, 2, 6, 7, 8-11, 14, 15 |
| `IProducerBuilder<T>.BatchingMaxPublishDelay(TimeSpan batchingMaxPublishDelay)` (default 1 ms) | `batchingMaxPublishDelay` | 1, 2, 14, 15, 26 |
| `IProducerBuilder<T>.SendTimeout(TimeSpan sendTimeout)` (`TimeSpan.Zero` = disabled; expiry -> `OperationTimeoutException`) | `sendTimeout` | 4, 5, 13, 16, 17, 18, 20, 22, 23, 24, 25, 26, 27, 28 |
| `IConsumerBuilder<T>.AckReceiptEnabled(bool ackReceiptEnabled)` | `isAckReceiptEnabled` | 1, 2, 6, 7 |
| `IConsumerBuilder<T>.AckTimeout(TimeSpan acknowledgementTimeout)` | `ackTimeout` | 14, 15 |
| `IConsumerBuilder<T>.DeadLetterPolicy(DeadLetterPolicy deadLetterPolicy)` with `DeadLetterPolicy { MaxRedeliverCount = 1 }` | `deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(1).build())` | 27, 28 |
| Batch-index ack: transactional individual/cumulative ack of a batch message sends `ack_set` + `batch_size`; receive path honours `CommandMessage.ack_set` | `ConsumerImpl#doTransactionAcknowledgeForResponse`, `receiveIndividualMessagesFromBatch` L2018-2060 | 1, 2, 9, 11, 14, 26, 28 |
| `MessageMetadata.Transaction` + `ISendChannel.Send(metadata, value, onMessageSent, ct)` (ordered fire-and-forget; op registered before the enqueue completes) | `newMessage(txn)...sendAsync()` | 1, 2, 4, 6, 7, 8-11, 13-15, 17, 26, 28, 29 |
| `MessageMetadata.DeliverAtTimeAsDateTimeOffset` (exists) | `deliverAfter` | 29 |
| `IConsumer.RedeliverUnacknowledgedMessages(ct)` (exists) | `redeliverUnacknowledgedMessages()` | 13, 14, 15, 24 |
| `IConsumerBuilder<T>.MessagePrefetchCount(uint)` (exists) | `receiverQueueSize` | 3 |
| `IProducerBuilder<T>.ProducerName`, `IConsumerBuilder<T>.ConsumerName` (exist) | `producerName`, `consumerName` | 17, 18, 20, 23 |
| `((PulsarClient) client).TransactionCoordinatorClient!` (`ITransactionCoordinatorClient`, `DotPulsar.Internal.Abstractions`) -> `Commit(TransactionId, ct)` (+ `EnsureStarted(ct)` for row 16) | `PulsarClientImpl#getTransactionCoordinatorClient()` | 8-11, 14, 15, 16, 18 |
| `new DotPulsar.Internal.Transactions.Transaction(ITransactionCoordinatorClient, TransactionId, TimeSpan)` | reflective `TransactionImpl` ctor | 18 |

Not needed by these tests: chunking, end-to-end encryption, `IProducer.Flush` (row 29 awaits receipts instead).

---

## 7. Open decisions (recommendation)

1. **DotPulsar default for `EnableBatching`.** Java defaults to `true`; changing DotPulsar's default would change
   existing users' wire behaviour. Recommendation: keep the DotPulsar default `false` and set `.EnableBatching(true)`
   explicitly in every ported test where Java relies on the default (rows 3, 16, 17, 24, 25, 26, 28), with defaults
   `BatchingMaxMessages = 1000`, `BatchingMaxPublishDelay = 1 ms` equal to Java's.
2. **Java `sendAsync` translation.** Recommendation: the `SendAsync` helper over `ISendChannel` (§1.2, §2.2), plus the
   implementation requirement that the send op is registered with the transaction before `ISendChannel.Send` returns. Un-awaited
   `IMessageBuilder.Send` calls are not used because `Producer.InternalSend` awaits partition choice and partition registration
   before enqueueing, so their order is not guaranteed.
3. **`groups = "flaky"` (row 29).** Recommendation: port as a normal `[Fact]` with `// Upstream group: flaky`; do not skip.
   If it proves unstable in CI, add `[Trait("Category", "Flaky")]` rather than deleting it.
4. **Per-test timeout.** Repo integration tests use a 1-minute `_cts`. Rows 8-11, 14-15 (2 rounds x 2000 receives,
   several 5 s empty waits, 1 s sleeps) and 17 (10 000 messages) can approach it. Recommendation: 3 minutes for this class.
5. **Row 19 metric source.** `pulsar_txn_timeout_total` is the only client-reachable view of `timeoutCount`. Recommendation:
   poll up to 10 s until `before + 1`, then assert equality (keeps Java's exact `+1`).
6. **Private `@Test` methods (rows 1-4).** TestNG executes methods that carry a method-level `@Test` even when `private`;
   treat them as active upstream tests and port them. (Not executed against TestNG in this drafting step.)
7. **Fire-and-forget `abort()` in Java cleanups (rows 18, 20).** The aborts are expected to fail (transaction already timed
   out) and Java ignores the future. Recommendation: omit them in C# (nothing is asserted and nothing leaks; the transaction
   no longer exists at the TC).
8. **NOBIA layout.** Recommendation: abstract generic base + two sealed classes + `SkipReason...` virtual properties and
   `Assert.SkipWhen` (§1.2), agreeing with [09-test-infrastructure](../09-test-infrastructure.md) Open decision 6. The Java subclass's runtime config flip
   (NOBIA L43) also leaks `acknowledgmentAtBatchIndexLevelEnabled=true` into the NOBIA tests that TestNG happens to run
   afterwards; C# does not reproduce that ordering artefact — all NOBIA tests run with `false`, which is the class's stated intent.
9. **Black-hole relay vs Toxiproxy (row 25).** Recommendation: the in-test relay (§4); no container change needed and only
   the producer's connection is affected, as with the Java mock.
10. **Starting the lazy TC client without creating a transaction (row 16).** The API UPDATE lists `Commit`, `Abort`,
    `NewTransaction`, `AddPublishPartitionToTxn`, `AddSubscriptionToTxn` and `State` on `ITransactionCoordinatorClient`.
    It does not list `EnsureStarted`. Row 16 commits by id on a fresh client that has never called `Create()`.
    Recommendation: keep `EnsureStarted(ct)` on the interface (already specified in `05-components/transaction-coordinator-client.md` L21).
    Otherwise, `Commit`/`Abort` on a `None`-state client must call `EnsureStarted` internally instead of throwing
    `TransactionCoordinatorClientStateException`.
