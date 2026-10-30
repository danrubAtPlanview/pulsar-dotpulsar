/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Tests.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Text;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenTransactionsEnabled_ShouldCreateOpenTransactionKnownByCoordinator()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);
        _testOutputHelper.WriteLine($"The metadata of the transaction {transaction.Id} is {metadata}");

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Open);
        transaction.State.IsFinalState().ShouldBeFalse();
        transaction.Id.MostSigBits.ShouldBe(0UL);
        metadata.GetProperty("txnId").GetString().ShouldBe(transaction.Id.ToString());
        metadata.GetProperty("status").GetString().ShouldBe("OPEN");
        metadata.GetProperty("owner").GetString().ShouldNotBeNullOrEmpty();
        metadata.GetProperty("timeoutAt").GetInt64().ShouldBe(300000);

        await transaction.Abort(_cts.Token);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenTimeoutOfOneMinute_ShouldTellTheCoordinatorTheTimeoutInMilliseconds()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transaction = await client.NewTransaction().Create(_cts.Token);
        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);

        //Assert
        metadata.GetProperty("timeoutAt").GetInt64().ShouldBe(60000);

        await transaction.Abort(_cts.Token);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenSeveralTransactions_ShouldCreateTransactionsWithDistinctIds()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transactions = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token).AsTask()));

        //Assert
        transactions.Select(transaction => transaction.Id).Distinct().Count().ShouldBe(20);
        transactions.ShouldAllBe(transaction => transaction.CurrentState == TransactionState.Open);

        await Task.WhenAll(transactions.Select(transaction => transaction.Abort(_cts.Token).AsTask()));
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenEmptyTransaction_ShouldCommitTransaction()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

        //Act
        await transaction.Commit(_cts.Token);
        var status = await _fixture.GetTransactionMetadataStatusCode(transaction.Id, _cts.Token);

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
        transaction.State.IsFinalState().ShouldBeTrue();
        status.ShouldBe(HttpStatusCode.NotFound); // The broker removes a transaction that has ended
    }

    // Additional coverage
    [Fact]
    public async Task Abort_GivenEmptyTransaction_ShouldAbortTransaction()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

        //Act
        await transaction.Abort(_cts.Token);
        var status = await _fixture.GetTransactionMetadataStatusCode(transaction.Id, _cts.Token);

        //Assert
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
        transaction.State.IsFinalState().ShouldBeTrue();
        status.ShouldBe(HttpStatusCode.NotFound); // The broker removes a transaction that has ended
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenCommittedTransaction_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        await transaction.Commit(_cts.Token);

        //Act
        var commitException = await Record.ExceptionAsync(() => transaction.Commit(_cts.Token).AsTask());
        var abortException = await Record.ExceptionAsync(() => transaction.Abort(_cts.Token).AsTask());

        //Assert
        commitException.ShouldBeOfType<InvalidTransactionStatusException>();
        abortException.ShouldBeOfType<InvalidTransactionStatusException>();
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenShortTimeout_ShouldBecomeTimedOutAndRejectCommitAndAbort()
    {
        //Arrange
        await using var client = _fixture.CreateClient();

        //Act
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        var state = await transaction.State.OnStateChangeTo(TransactionState.TimedOut, _cts.Token);
        var commitException = await Record.ExceptionAsync(() => transaction.Commit(_cts.Token).AsTask());
        var abortException = await Record.ExceptionAsync(() => transaction.Abort(_cts.Token).AsTask());

        //Assert
        state.ShouldBe(TransactionState.TimedOut);
        transaction.CurrentState.ShouldBe(TransactionState.TimedOut);
        commitException.ShouldBeOfType<InvalidTransactionStatusException>();
        abortException.ShouldBeOfType<InvalidTransactionStatusException>();
    }

    // Upstream: TransactionTest#testGetTxnState
    // Upstream ends the transaction of step 4 through the transaction metadata store of the broker, which the admin API does here.
    // Upstream registers an operation of the transaction that never ends in steps 5 and 6, which keeps the transaction committing or aborting.
    // A send that is held in a batch that is not sent until the end of the test does the same here.
    [Fact]
    public async Task CurrentState_GivenTransactionLifecycle_ShouldReportEachState()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testGetTxnState");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(true)
            .BatchingMaxMessages(1000)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .SendTimeout(TimeSpan.Zero)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        //Act
        var timedOut = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        var stateBeforeTimeout = timedOut.CurrentState;
        await timedOut.State.OnStateChangeTo(TransactionState.TimedOut, _cts.Token);

        var aborted = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(3)).Create(_cts.Token);
        await aborted.Abort(_cts.Token);

        var committed = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(3)).Create(_cts.Token);
        await committed.Commit(_cts.Token);

        var endedByBroker = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        await _fixture.AbortTransaction(endedByBroker.Id, _cts.Token);
        var commitException = await Record.ExceptionAsync(() => endedByBroker.Commit(_cts.Token).AsTask());

        var committing = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(3)).Create(_cts.Token);
        var committingSend = producer.NewMessage().Transaction(committing).Send([], _cts.Token).AsTask();
        var commit = committing.Commit(_cts.Token).AsTask();
        await Until(() => committing.CurrentState == TransactionState.Committing, "the transaction is committing");
        var committingStateAfterWait = committing.CurrentState;
        var isCommitDone = commit.IsCompleted;

        var aborting = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(3)).Create(_cts.Token);
        var abortingSend = producer.NewMessage().Transaction(aborting).Send([], _cts.Token).AsTask();
        var abort = aborting.Abort(_cts.Token).AsTask();
        await Until(() => aborting.CurrentState == TransactionState.Aborting, "the transaction is aborting");
        var abortingStateAfterWait = aborting.CurrentState;
        var isAbortDone = abort.IsCompleted;

        //Assert
        stateBeforeTimeout.ShouldBe(TransactionState.Open);
        timedOut.CurrentState.ShouldBe(TransactionState.TimedOut);
        aborted.CurrentState.ShouldBe(TransactionState.Aborted);
        committed.CurrentState.ShouldBe(TransactionState.Committed);
        commitException.ShouldBeOfType<TransactionNotFoundException>();
        endedByBroker.CurrentState.ShouldBe(TransactionState.Error);
        committingStateAfterWait.ShouldBe(TransactionState.Committing);
        isCommitDone.ShouldBeFalse("the commit waits for the send that is held");
        abortingStateAfterWait.ShouldBe(TransactionState.Aborting);
        isAbortDone.ShouldBeFalse("the abort waits for the send that is held");

        // The messages that are held are sent, so that every task ends
        await producer.Flush(_cts.Token);
        await Task.WhenAll(committingSend, commit, abortingSend, abort);
        committing.CurrentState.ShouldBe(TransactionState.Committed);
        aborting.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Upstream: TransactionBuilderTest#checkClientEnableTransactionConfiguration
    [Fact]
    public async Task Create_GivenClientWithTransactionsDisabled_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = _fixture.CreateClient(enableTransaction: false);

        //Act
        var exception = await Record.ExceptionAsync(() => client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token).AsTask());

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("Transactions are not enabled");
        ((PulsarClient) client).TransactionCoordinatorClient.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenNewClient_ShouldStartTheCoordinatorClientWithTheFirstTransaction()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transactionCoordinatorClient = ((PulsarClient) client).TransactionCoordinatorClient!;
        var stateBefore = transactionCoordinatorClient.State;

        //Act
        var transaction = await client.NewTransaction().Create(_cts.Token);

        //Assert
        stateBefore.ShouldBe(TransactionCoordinatorClientState.None);
        transactionCoordinatorClient.State.ShouldBe(TransactionCoordinatorClientState.Ready);

        await transaction.Abort(_cts.Token);
    }

    // Upstream: TransactionTest#testMaxReadPositionForNormalPublish
    [Fact]
    public async Task Send_GivenNormalAndTransactionalMessages_ShouldMoveMaxReadPositionOnlyWhenNoOpenTransaction()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("NormalPublish");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var noTransactionClient = _fixture.CreateClient(_testOutputHelper.Log, false);
        await using var normalProducer = noTransactionClient.NewProducer(Schema.String).ProducerName("testNormalPublish").Topic(topic).SendTimeout(TimeSpan.Zero).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await WaitUntilTransactionBufferState(topic, "NoSnapshot");

        //Act
        var normalId = await normalProducer.Send("normal message", _cts.Token);

        //Assert
        (await GetMaxReadPosition(topic)).ShouldBe(ToPosition(normalId));

        //Arrange
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var transactionProducer = client.NewProducer(Schema.String).ProducerName("testTransactionPublish").Topic(topic).SendTimeout(TimeSpan.Zero).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await WaitUntilTransactionBufferState(topic, "NoSnapshot");
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);

        //Act
        _ = await transactionProducer.NewMessage().Transaction(transaction).Send("txn message", _cts.Token);

        //Assert
        (await GetMaxReadPosition(topic)).ShouldBe(ToPosition(normalId));

        //Act
        var secondNormalId = await normalProducer.Send("normal message", _cts.Token);

        //Assert
        (await GetMaxReadPosition(topic)).ShouldBe(ToPosition(normalId));

        //Act
        await transaction.Commit(_cts.Token);

        //Assert
        (await GetMaxReadPosition(topic)).ShouldBe($"{secondNormalId.LedgerId}:{secondNormalId.EntryId + 1}"); // The commit marker

        //Act
        var thirdNormalId = await normalProducer.Send("normal message", _cts.Token);

        //Assert
        (await GetMaxReadPosition(topic)).ShouldBe(ToPosition(thirdNormalId));
    }

    // Upstream: TransactionTest#testDelayedDeliveryExceedsMaxDelay
    [Fact]
    public async Task Send_GivenTransactionalMessageExceedingMaxDeliveryDelay_ShouldThrowNotAllowedException()
    {
        //Arrange
        const long maxDeliveryDelayInMillis = 5000;
        var ns = $"{TransactionFixture.Tenant}/testDelayedDeliveryExceedsMaxDelay-{Guid.NewGuid():N}";
        var topic = $"persistent://{ns}/test_transaction_topic{Guid.NewGuid():N}";
        await _fixture.CreateNamespace(ns, 4, _cts.Token);
        await _fixture.CreateTopic(topic, _cts.Token);
        await _fixture.SetTopicDelayedDeliveryPolicy(topic, true, 100, maxDeliveryDelayInMillis, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).SendTimeout(TimeSpan.FromSeconds(5)).EncryptionKey("my-app-key").DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Public("ClientRsa"))).Create();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);
        var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

        //Act
        var exception = await Record.ExceptionAsync(async () => await producer.NewMessage().Transaction(transaction).DeliverAt(DateTimeOffset.UtcNow.AddSeconds(6)).Send(payload, _cts.Token));

        //Assert
        exception.ShouldBeOfType<NotAllowedException>().Message.ShouldBe($"Exceeds max allowed delivery delay of {maxDeliveryDelayInMillis} milliseconds");
        producer.State.IsFinalState().ShouldBeFalse("A message that is not allowed must not fault the producer"); // Additional coverage
        _ = await producer.Send(payload, _cts.Token); // Additional coverage
        await Should.ThrowAsync<TransactionHasOperationFailedException>(async () => await transaction.Commit(_cts.Token)); // Additional coverage: the failed send makes the commit abort
    }

    // Upstream: TransactionTest#testEncryptionRequired
    // Upstream does not wait for the send or the commit. The message is received and decrypted here, because a commit that is not awaited proves nothing.
    [Fact]
    public async Task Send_GivenEncryptionRequiredNamespaceAndEncryptedTransactionalMessage_ShouldCommit()
    {
        //Arrange
        var ns = $"{TransactionFixture.Tenant}/testEncryptionRequired-{Guid.NewGuid():N}";
        var topic = $"persistent://{ns}/test_transaction_topic";
        await _fixture.CreateNamespace(ns, 4, _cts.Token);
        await _fixture.SetNamespaceEncryptionRequired(ns, true, _cts.Token);
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).SendTimeout(TimeSpan.FromSeconds(5)).EncryptionKey("my-app-key").DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Public("ClientRsa"))).Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("subscription")
            .Topic(topic)
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Private("ClientRsa")))
            .Create();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);
        var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

        //Act
        _ = await producer.NewMessage().Transaction(transaction).Send(payload, _cts.Token);
        var receivedBeforeCommit = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token);
        await transaction.Commit(_cts.Token);
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        receivedBeforeCommit.ShouldBeNull("the message of a transaction is not visible before the commit");
        actual.ShouldNotBeNull();
        actual.Data.ToArray().ShouldBe(payload);
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        actual.EncryptionContext.Keys.Keys.ShouldBe(["my-app-key"]);
    }

    // Upstream: TransactionTest#testTopicTransactionMetrics
    // Upstream also asserts five OpenTelemetry metrics of the broker, which the broker of the fixture does not give.
    [Fact]
    public async Task Commit_GivenCommittedAbortedAndOngoingTransactions_ShouldReportTransactionCountsInTopicStats()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("test_transaction_topic");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).SendTimeout(TimeSpan.Zero).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching

        //Act
        var committed = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(committed).Send(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()), _cts.Token);
        await committed.Commit(_cts.Token);

        var aborted = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(aborted).Send(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()), _cts.Token);
        await aborted.Abort(_cts.Token);

        var ongoing = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(ongoing).Send(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()), _cts.Token);
        var stats = await _fixture.GetTopicStats(topic, _cts.Token);

        //Assert
        stats.GetProperty("committedTxnCount").GetInt64().ShouldBe(1);
        stats.GetProperty("abortedTxnCount").GetInt64().ShouldBe(1);
        stats.GetProperty("ongoingTxnCount").GetInt64().ShouldBe(1);

        await ongoing.Abort(_cts.Token);
    }

    // Upstream: TransactionTest#testCreateTransactionSystemTopic
    // Upstream deletes the namespace of the other tests and creates it again, to start with a namespace that is clean. This test has a namespace of its own.
    [Fact]
    public async Task Subscribe_GivenTransactionPendingAckSystemTopic_ShouldThrowNotAllowedException()
    {
        //Arrange
        const string subscription = "test";
        const string localName = "testCreateTransactionSystemTopic";
        var ns = $"{TransactionFixture.Tenant}/{localName}-{Guid.NewGuid():N}";
        var topic = $"persistent://{ns}/{localName}";
        var pendingAckLocalName = $"{localName}-{subscription}__transaction_pending_ack";
        var pendingAckTopic = $"persistent://{ns}/{pendingAckLocalName}";
        await _fixture.CreateNamespace(ns, 4, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        Exception? acknowledgeException;

        // init pending ack
        await using (var consumer = CreateSharedConsumer(client, topic, subscription))
        {
            await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
            var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);
            acknowledgeException = await Record.ExceptionAsync(async () => await consumer.Acknowledge(new MessageId(10, 10, 10, -1), transaction, _cts.Token));
        }

        //Act
        var topics = await _fixture.GetTopicList(ns, _cts.Token);
        await using var systemTopicConsumer = CreateSharedConsumer(client, pendingAckTopic, subscription);
        var consumerException = await Record.ExceptionAsync(async () => await systemTopicConsumer.Receive(_cts.Token));
        var subscriptionsError = await _fixture.GetErrorReason(HttpMethod.Get, $"/admin/v2/persistent/{ns}/{pendingAckLocalName}/subscriptions", null, _cts.Token);
        var partitionedTopicError = await _fixture.GetErrorReason(HttpMethod.Put, $"/admin/v2/persistent/{ns}/{pendingAckLocalName}/partitions", "3", _cts.Token);
        var topicError = await _fixture.GetErrorReason(HttpMethod.Put, $"/admin/v2/persistent/{ns}/{pendingAckLocalName}", "{}", _cts.Token);

        //Assert
        if (acknowledgeException is not null)
            acknowledgeException.ShouldBeOfType<TransactionConflictException>();

        // getList does not include transaction system topic
        topics.ShouldNotBeEmpty();
        topics.ShouldAllBe(name => !name.Contains("__transaction_pending_ack"));

        // can't create transaction system topic
        consumerException.ShouldBeOfType<NotAllowedException>().Message.ShouldContain("Can not create transaction system topic");
        systemTopicConsumer.State.IsFinalState().ShouldBeTrue("the consumer faulted, because the broker does not allow the subscription");
        subscriptionsError.Reason.ShouldBe("Can not create transaction system topic " + pendingAckTopic);
        partitionedTopicError.Reason.ShouldBe("Cannot create topic in system topic format!");
        topicError.Reason.ShouldBe("Cannot create topic in system topic format!");
    }

    // Upstream: TransactionTest#brokerNotInitTxnManagedLedgerTopic
    // Upstream also asserts that the broker has not loaded three of its topics, which the admin API cannot tell without loading them.
    [Fact]
    public async Task Create_GivenSystemNamespaceUnloaded_ShouldCreateTransaction()
    {
        //Arrange
        const string subscription = "test";
        var topic = TransactionFixture.CreateTopicName("test");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);

        await using (var consumer = CreateSharedConsumer(client, topic, subscription))
        {
            await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        }

        await Until(token => CanCreateTransaction(client, token), "a transaction can be created");

        //Act
        await _fixture.UnloadNamespace("pulsar/system", _cts.Token);
        await _fixture.UnloadNamespace(TransactionFixture.Namespace, _cts.Token);
        await using var consumer1 = CreateSharedConsumer(client, topic, subscription);
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Assert
        await Until(token => CanCreateTransaction(client, token), "a transaction can be created after the namespaces were unloaded");

        // Upstream asks the broker for the topics it has loaded. The metrics of the broker are written for the topics that are loaded.
        var metrics = await _fixture.GetPrometheusMetricsText(_cts.Token);
        metrics.ShouldContain($"topic=\"{topic}\"", customMessage: "the broker has loaded the topic of the consumer, so it writes metrics for it, and the assertions below can fail");
        metrics.ShouldNotContain("topic=\"persistent://pulsar/system/__transaction_log_0\"");
        metrics.ShouldNotContain("topic=\"persistent://pulsar/system/transaction_coordinator_assign-partition-0\"");
        metrics.ShouldNotContain($"topic=\"{topic}-{subscription}__transaction_pending_ack\"");
    }

    // Upstream: TransactionTest#testAsyncSendOrAckForSingleFuture
    // Upstream disables the deduplication of the broker. The topic of this test does it here, so that the other tests of the broker are not affected.
    // Upstream reads the number of operations that the transaction has not ended by reflection. That the commit waits for every operation, which the assertions that follow check, covers it.
    // The 30 tasks receive from one consumer at the same time. The channel of a consumer lets one call of Receive at a time wait for a message, so that is safe.
    [Fact]
    public async Task Commit_GivenConcurrentTransactionalSendsAndAcks_ShouldCompleteAllOperations()
    {
        //Arrange
        const int totalMessage = 10;
        const int threadSize = 30;
        var topic = TransactionFixture.CreateTopicName("testSingleFuture");
        await _fixture.CreateTopic(topic, _cts.Token);
        await _fixture.SetTopicDeduplication(topic, false, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .ProducerName("producer")
            .SendTimeout(TimeSpan.Zero)
            .EnableBatching(true) // Upstream relies on the default of the Java client, which is batching
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionType(SubscriptionType.Exclusive)
            .SubscriptionName("subscription")
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //store the send/ack result tasks
        var sendTasks = new ConcurrentQueue<Task<MessageId>>();
        var ackTasks = new ConcurrentQueue<Task>();

        //send and ack messages with transaction
        var transaction1 = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);

        for (var i = 0; i < totalMessage * threadSize; i++)
        {
            _ = await producer.Send([], _cts.Token);
        }

        //Act
        var workers = Enumerable.Range(0, threadSize).Select(_ => Task.Run(async () =>
        {
            try
            {
                for (var j = 0; j < totalMessage; j++)
                {
                    sendTasks.Enqueue(producer.NewMessage().Transaction(transaction1).Send([], _cts.Token).AsTask());
                    var message = await consumer.Receive(_cts.Token);
                    ackTasks.Enqueue(consumer.Acknowledge(message.MessageId, transaction1, _cts.Token).AsTask());
                }
            }
            catch (Exception exception)
            {
                _testOutputHelper.Log($"Failed to send/ack messages with transaction: {exception}");
            }
        })).ToList();

        //wait the all send/ack op is executed and store its tasks in the queues.
        await Task.WhenAny(Task.WhenAll(workers), Task.Delay(TimeSpan.FromSeconds(10), _cts.Token));
        await transaction1.Commit(_cts.Token);

        //verify opFuture without any operation.
        var transaction2 = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);
        using var commitCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        commitCts.CancelAfter(TimeSpan.FromSeconds(10));
        var secondCommitException = await Record.ExceptionAsync(async () => await transaction2.Commit(commitCts.Token));

        //Assert
        sendTasks.Count.ShouldBe(totalMessage * threadSize);
        ackTasks.Count.ShouldBe(totalMessage * threadSize);
        sendTasks.ShouldAllBe(task => task.IsCompleted);
        ackTasks.ShouldAllBe(task => task.IsCompleted);
        secondCommitException.ShouldBeNull();
    }

    // Upstream: TransactionTest#testSendAndAckWithSchema
    // Upstream sends the message through a producer of bytes, with the schema of a string given to the message. The producer has the schema of a string here.
    // Upstream disables the deduplication of the broker. The topic of this test does it here, so that the other tests of the broker are not affected.
    [Fact]
    public async Task Send_GivenStringSchemaProducerAndTransaction_ShouldDeliverValueWithSchemaVersion()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testAsyncSendAndAckWithSchema");
        await _fixture.CreateTopic(topic, _cts.Token);
        await _fixture.SetTopicDeduplication(topic, false, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topic)
            .ProducerName("producer")
            .SendTimeout(TimeSpan.Zero)
            .EnableBatching(true) // Upstream relies on the default of the Java client, which is batching
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionType(SubscriptionType.Exclusive)
            .SubscriptionName("subscription")
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);

        //Act
        _ = await producer.NewMessage().Transaction(transaction).Send("testSendAndAckWithSchema", _cts.Token);
        await transaction.Commit(_cts.Token);
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        message.ShouldNotBeNull();
        Encoding.UTF8.GetString(message.Value()).ShouldBe("testSendAndAckWithSchema");
        message.SchemaVersion.ShouldNotBeNull();
    }

    // Upstream: TransactionTest#testGetTxnID
    // Upstream does not wait for the abort of the first transaction.
    [Fact]
    public async Task Create_GivenPreviousTransaction_ShouldIncrementLeastSigBits()
    {
        //Arrange
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var transaction = await client.NewTransaction().Create(_cts.Token);
        var abort = transaction.Abort(_cts.Token).AsTask();

        //Act
        var transaction1 = await client.NewTransaction().Create(_cts.Token);

        //Assert
        transaction1.Id.LeastSigBits.ShouldBe(transaction.Id.LeastSigBits + 1);
        transaction1.Id.MostSigBits.ShouldBe(0UL);

        await abort;
        await transaction1.Abort(_cts.Token);
    }

    // Upstream: TransactionTest#testDeleteNamespace
    // Upstream does not wait for the commit. The broker deletes the topics while it ends the transaction, so the commit may fail, which this test does not care about.
    [Fact]
    public async Task DeleteNamespace_GivenTransactionalProduceAndAck_ShouldSucceed()
    {
        //Arrange
        var ns = $"{TransactionFixture.Tenant}/ns-{Guid.NewGuid():N}";
        var topic = $"persistent://{ns}/test-delete-ns";
        await _fixture.CreateNamespace(ns, 4, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        Task commit;

        await using (var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).StateChangedHandler(_testOutputHelper.Log).Create()) // Upstream relies on the default of the Java client, which is batching
        {
            _ = await producer.Send(Encoding.UTF8.GetBytes("test"), _cts.Token);

            var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);

            await using (var consumer = client.NewConsumer(Schema.ByteArray)
                .Topic(topic)
                .InitialPosition(SubscriptionInitialPosition.Earliest)
                .SubscriptionName("sub")
                .StateChangedHandler(_testOutputHelper.Log)
                .Create())
            {
                var message = await consumer.Receive(_cts.Token);
                await consumer.Acknowledge(message.MessageId, transaction, _cts.Token);
            }

            await using (var outProducer = client.NewProducer(Schema.ByteArray).Topic(topic + "-out").EnableBatching(true).StateChangedHandler(_testOutputHelper.Log).Create()) // Upstream relies on the default of the Java client, which is batching
            {
                _ = await outProducer.NewMessage().Transaction(transaction).Send(Encoding.UTF8.GetBytes("output"), _cts.Token);
            }

            commit = transaction.Commit(_cts.Token).AsTask();
        }

        //Act
        var exception = await Record.ExceptionAsync(async () => await _fixture.DeleteNamespace(ns, true, _cts.Token));

        //Assert
        exception.ShouldBeNull();

        var commitException = await Record.ExceptionAsync(() => commit);
        _testOutputHelper.Log($"The commit that was not waited for ended with: {commitException?.ToString() ?? "no exception"}");
    }

    // Upstream: TransactionTest#testReadCommittedWithReadCompacted
    [Fact]
    public async Task Receive_GivenReadCompactedConsumerAndOpenTransaction_ShouldDeliverTransactionalMessagesOnlyAfterCommit()
    {
        //Arrange
        var ns = $"{TransactionFixture.Tenant}/ns-read-committed-compacted-{Guid.NewGuid():N}";
        var topic = $"persistent://{ns}/test_transaction_topic";
        await _fixture.CreateNamespace(ns, 4, _cts.Token);
        await _fixture.CreateTopic(topic, _cts.Token);
        await _fixture.SetTopicCompactionThreshold(topic, 100L * 1024 * 1024, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .SubscriptionName("sub")
            .SubscriptionType(SubscriptionType.Exclusive)
            .ReadCompacted(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(true).StateChangedHandler(_testOutputHelper.Log).Create(); // Upstream relies on the default of the Java client, which is batching
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        _ = await producer.NewMessage().Key("K1").Send("V1", _cts.Token);

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Key("K2").Send("V2", _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Key("K3").Send("V3", _cts.Token);

        //Act
        var messagesBeforeCommit = await TransactionFixture.DrainValues(consumer, TimeSpan.FromSeconds(5), _cts.Token);
        var commit = transaction.Commit(_cts.Token).AsTask();
        var messagesAfterCommit = await TransactionFixture.DrainValues(consumer, TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        messagesBeforeCommit.ShouldBe(["V1"]);
        messagesAfterCommit.ShouldBe(["V2", "V3"]);

        await commit;
    }

    // Upstream: TransactionTest#testReadCommittedWithCompaction
    [Fact]
    public async Task Receive_GivenCompactedTopicWithCommittedTransactions_ShouldDeliverLatestValuePerKey()
    {
        //Arrange
        var ns = $"{TransactionFixture.Tenant}/ns-read-committed-compaction-{Guid.NewGuid():N}";
        var topic = $"persistent://{ns}/test_transaction_topic{Guid.NewGuid():N}";
        await _fixture.CreateNamespace(ns, 4, _cts.Token);
        await _fixture.CreateTopic(topic, _cts.Token);
        await _fixture.SetTopicCompactionThreshold(topic, 100L * 1024 * 1024, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(true).StateChangedHandler(_testOutputHelper.Log).Create(); // Upstream relies on the default of the Java client, which is batching

        _ = await producer.NewMessage().Key("K1").Send("V1", _cts.Token);

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Key("K2").Send("V2", _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Key("K3").Send("V3", _cts.Token);
        await transaction.Commit(_cts.Token);

        _ = await producer.NewMessage().Key("K1").Send("V4", _cts.Token);

        var transaction2 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction2).Key("K2").Send("V5", _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction2).Key("K3").Send("V6", _cts.Token);
        await transaction2.Commit(_cts.Token);

        await _fixture.TriggerCompaction(topic, _cts.Token);
        await Until(async token => await _fixture.GetCompactionStatus(topic, token) == "SUCCESS", "the compaction of the topic has succeeded");

        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .SubscriptionName("sub")
            .SubscriptionType(SubscriptionType.Exclusive)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .ReadCompacted(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token); // The subscribe of the Java client returns when the consumer is connected, so the first receive does not wait for it

        //Act
        // A consumer that receives right after it has become active can wait for 3 seconds for its channel, which is longer than the 2 seconds that upstream waits for a message,
        // so the first message is waited for longer. The wait of 2 seconds that ends the loop is as upstream.
        var first = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        var result = first is null ? [] : new List<string> { first.Value() };
        result.AddRange(await TransactionFixture.DrainValues(consumer, TimeSpan.FromSeconds(2), _cts.Token));

        //Assert
        result.ShouldBe(["V4", "V5", "V6"]);
    }

    // Upstream: TransactionTest#testPersistentTopicGetLastDispatchablePositionWithTxn
    // Upstream asks the topic of the broker for its last dispatchable position. The reader asks for the last message ids, which the broker answers with that position.
    [Fact]
    public async Task GetLastMessageIds_GivenOpenAndAbortedTransactions_ShouldReturnLastNonTransactionalMessage()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testPersistentTopicGetLastDispatchablePositionWithTxn");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(false).StateChangedHandler(_testOutputHelper.Log).Create();
        await using var probe = client.NewReader(Schema.String).Topic(topic).StartMessageId(MessageId.Earliest).StateChangedHandler(_testOutputHelper.Log).Create();
        await probe.StateChangedTo(ReaderState.Connected, _cts.Token);

        // send a normal message
        var body = Guid.NewGuid().ToString();
        var messageId = await producer.Send(body, _cts.Token);

        // send 3 txn messages
        var transaction = await client.NewTransaction().Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Send(Guid.NewGuid().ToString(), _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Send(Guid.NewGuid().ToString(), _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Send(Guid.NewGuid().ToString(), _cts.Token);

        //Act
        // get last dispatchable position
        var lastWithOpenTransaction = (await probe.GetLastMessageIds(_cts.Token)).Single();

        // abort the txn
        using (var abortCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token))
        {
            abortCts.CancelAfter(TimeSpan.FromSeconds(5));
            await transaction.Abort(abortCts.Token);
        }

        // get last dispatchable position
        var lastWithAbortedTransaction = (await probe.GetLastMessageIds(_cts.Token)).Single();

        await using var reader = client.NewReader(Schema.String).Topic(topic).StartMessageId(MessageId.Earliest).StateChangedHandler(_testOutputHelper.Log).Create();
        await reader.StateChangedTo(ReaderState.Connected, _cts.Token);
        var transaction1 = await client.NewTransaction().Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction1).Send(Guid.NewGuid().ToString(), _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction1).Send(Guid.NewGuid().ToString(), _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction1).Send(Guid.NewGuid().ToString(), _cts.Token);
        var messages = await TransactionFixture.ReadAllAvailable(reader, MessageId.Earliest, _cts.Token);

        using (var abortCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token))
        {
            abortCts.CancelAfter(TimeSpan.FromSeconds(5));
            await transaction1.Abort(abortCts.Token);
        }

        await using var reader1 = client.NewReader(Schema.String).Topic(topic).StartMessageId(MessageId.Earliest).StateChangedHandler(_testOutputHelper.Log).Create();
        await reader1.StateChangedTo(ReaderState.Connected, _cts.Token);
        var messages1 = await TransactionFixture.ReadAllAvailable(reader1, MessageId.Earliest, _cts.Token);

        //Assert
        // the last dispatchable position should be the message id of the normal message
        (lastWithOpenTransaction.LedgerId, lastWithOpenTransaction.EntryId).ShouldBe((messageId.LedgerId, messageId.EntryId));
        (lastWithAbortedTransaction.LedgerId, lastWithAbortedTransaction.EntryId).ShouldBe((messageId.LedgerId, messageId.EntryId));
        messages.Select(message => message.Value()).ShouldBe([body]);
        messages1.Select(message => message.Value()).ShouldBe([body]);
    }

    public void Dispose()
        => _cts.Dispose();

    private static string CreateDataUri(byte[] key)
        => "data:application/x-pem-file;base64," + Convert.ToBase64String(key);

    private IConsumer<byte[]> CreateSharedConsumer(IPulsarClient client, string topic, string subscription)
        => client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Shared)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();

    private static async Task<bool> CanCreateTransaction(IPulsarClient client, CancellationToken cancellationToken)
    {
        try
        {
            _ = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(cancellationToken);
            return true;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }
    }

    // Java Awaitility.await().until(condition): the condition is checked 100 milliseconds after the call and then every 100 milliseconds, for at most 10 seconds.
    // An exception of the condition ends the wait, like it does in Awaitility.
    private async Task Until(Func<CancellationToken, Task<bool>> condition, string description)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        try
        {
            do
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
            }
            while (!await condition(cts.Token));
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !_cts.IsCancellationRequested)
        {
            throw new TimeoutException($"Did not happen within 10 seconds: {description}");
        }
    }

    private Task Until(Func<bool> condition, string description)
        => Until(_ => Task.FromResult(condition()), description);

    private async Task WaitUntilTransactionBufferState(string topic, string state)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        while ((await _fixture.GetTransactionBufferStats(topic, false, false, _cts.Token)).GetProperty("state").GetString() != state)
            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }

    private async Task<string> GetMaxReadPosition(string topic)
        => (await _fixture.GetTransactionBufferStats(topic, false, false, _cts.Token)).GetProperty("maxReadPosition").GetString()!;

    private static string ToPosition(MessageId messageId)
        => $"{messageId.LedgerId}:{messageId.EntryId}";
}
