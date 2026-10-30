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
using System.Text;
using System.Text.Json;

// Upstream: TransactionEndToEndTest
// The tests are run against a broker with batch index acknowledgment on and against one with it off, see the subclasses.
//
// A batch is produced by TransactionFixtureBase.ProduceBatchedMessages. It produces numbered payloads, so the tests that produce batches identify the messages by their batch index instead of by their value.
public abstract class TransactionEndToEndTestsBase<TFixture> : IDisposable where TFixture : TransactionFixture
{
    protected const int TopicPartition = 3; // Upstream: TransactionEndToEndTest#TOPIC_PARTITION

    private static readonly TimeSpan _waitTimeForCanReceiveMsg = TimeSpan.FromSeconds(5); // Upstream: TransactionEndToEndTest#waitTimeForCanReceiveMsgInSec
    private static readonly TimeSpan _waitTimeForCannotReceiveMsg = TimeSpan.FromSeconds(5); // Upstream: TransactionEndToEndTest#waitTimeForCannotReceiveMsgInSec

    private readonly CancellationTokenSource _cts;
    private readonly ITestOutputHelper _testOutputHelper;

    protected TransactionEndToEndTestsBase(TFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        Fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    protected TFixture Fixture { get; }

    // Upstream: TransactionEndToEndWithoutBatchIndexAckTest#testUnackMessageAfterAckAllMessages and #testAckWithTransactionReduceUnAckMessageCount (enabled = false)
    protected virtual string? SkipReasonForUnackMessageCountTests => null;

    // Upstream: TransactionEndToEndWithoutBatchIndexAckTest#txnIndividualAckTestBatchAndFailoverSub (the test turns batch index acknowledgment on at runtime)
    protected virtual string? SkipReasonForBatchAndFailoverIndividualAck => null;

    // Upstream: TransactionEndToEndTest#produceCommitTest
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Commit_GivenTwoTransactionsProducingToPartitionedTopic_ShouldDeliverAllMessagesOnlyAfterCommit(bool enableBatch)
    {
        //Arrange
        _testOutputHelper.Log($"Batching is {(enableBatch ? "enabled" : "disabled")}");
        var topic = TransactionFixture.CreateTopicName("output");
        await Fixture.CreatePartitionedTopic(topic, TopicPartition, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("test").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(enableBatch).SendTimeout(TimeSpan.Zero).Create();
        var txn1 = await GetTxn(client);
        var txn2 = await GetTxn(client);
        var txnMessageCnt = 0;
        var messageCnt = 1000;

        for (var i = 0; i < messageCnt; ++i)
        {
            _ = await SendAsync(producer, Encoding.UTF8.GetBytes("Hello Txn - " + i), i % 5 == 0 ? txn1 : txn2);
            ++txnMessageCnt;
        }

        // Can't receive transaction messages before commit.
        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        message.ShouldBeNull();

        //Act
        await txn1.Commit(_cts.Token);
        await txn2.Commit(_cts.Token);

        //Assert
        var receiveCnt = 0;
        for (var i = 0; i < txnMessageCnt; ++i)
        {
            message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCanReceiveMsg, _cts.Token);
            message.ShouldNotBeNull();
            ++receiveCnt;
        }

        receiveCnt.ShouldBe(txnMessageCnt);

        message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        message.ShouldBeNull();
    }

    // Upstream: TransactionEndToEndTest#produceAbortTest
    [Fact]
    public async Task Abort_GivenProducedMessages_ShouldNotDeliverAndShouldMoveMarkDeleteToEnd()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("output");
        await Fixture.CreatePartitionedTopic(topic, TopicPartition, _cts.Token);
        await using var client = CreateClient();
        var txn = await GetTxn(client);
        const string subName = "test";
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).SendTimeout(TimeSpan.Zero).Create();
        var messageCnt = 10;

        for (var i = 0; i < messageCnt; ++i)
            _ = await producer.NewMessage().Transaction(txn).Send(Encoding.UTF8.GetBytes("Hello Txn - " + i), _cts.Token);

        await using var consumer = client.NewConsumer(Schema.ByteArray).Topic(topic).InitialPosition(SubscriptionInitialPosition.Earliest).SubscriptionName(subName).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Can't receive transaction messages before abort.
        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        message.ShouldBeNull();

        //Act
        await txn.Abort(_cts.Token);

        //Assert
        // Can't receive transaction messages after abort.
        message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        message.ShouldBeNull();

        // Upstream reads the mark delete position and the last confirmed entry of the subscription from the broker, here they come from the internal stats of every partition
        await WaitUntil(async () =>
        {
            for (var partition = 0; partition < TopicPartition; ++partition)
            {
                var stats = await Fixture.GetInternalStats($"{topic}-partition-{partition}", _cts.Token);
                stats.GetProperty("cursors").TryGetProperty(subName, out _).ShouldBeTrue($"The subscription {subName} does not exist for partition {partition}");

                if (!IsMarkDeleteAtEnd(stats, subName))
                    return false;
            }

            return true;
        });
    }

    // Upstream: TransactionEndToEndTest#txnMetadataHandlerRecoverTest
    [Theory]
    [InlineData(true)] // Upstream relies on the default of the Java client, which is batching
    [InlineData(false)]
    public async Task Commit_GivenTransactionsCommittedByIdFromAnotherClient_ShouldDeliverAllMessages(bool enableBatch)
    {
        //Arrange
        _testOutputHelper.Log($"Batching is {(enableBatch ? "enabled" : "disabled")}");
        var topic = TransactionFixture.CreateTopicName("tc-metadata-handler-recover");
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(enableBatch).SendTimeout(TimeSpan.Zero).Create();
        var txnCnt = 20;
        var messageCnt = 10;
        var ids = new List<TransactionId>();

        for (var i = 0; i < txnCnt; ++i)
        {
            var txn = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

            for (var j = 0; j < messageCnt; ++j)
                _ = await producer.NewMessage().Transaction(txn).Send(Encoding.UTF8.GetBytes("Hello"), _cts.Token);

            ids.Add(txn.Id);
        }

        await using var recoverClient = CreateClient();
        var tcClient = ((PulsarClient) recoverClient).TransactionCoordinatorClient!;

        // The coordinator client of a client is started by its first transaction, but this client has not created any
        await tcClient.EnsureStarted(_cts.Token);

        //Act
        foreach (var id in ids)
            await tcClient.Commit(id, _cts.Token);

        //Assert
        await using var consumer = recoverClient.NewConsumer(Schema.ByteArray).SubscriptionName("test").Topic(topic).InitialPosition(SubscriptionInitialPosition.Earliest).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < txnCnt * messageCnt; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCanReceiveMsg, _cts.Token);
            message.ShouldNotBeNull();
        }
    }

    // Upstream: TransactionEndToEndTest#produceTxnMessageOrderTest
    [Theory]
    [InlineData(true)] // Upstream relies on the default of the Java client, which is batching
    [InlineData(false)]
    public async Task Commit_GivenThousandTransactionalMessages_ShouldDeliverInSendOrder(bool enableBatch)
    {
        //Arrange
        _testOutputHelper.Log($"Batching is {(enableBatch ? "enabled" : "disabled")}");
        var topic = TransactionFixture.CreateTopicName("txn-produce-order");
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("test").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray).ProducerName("txn-publish-order").Topic(topic).EnableBatching(enableBatch).SendTimeout(TimeSpan.Zero).Create();

        for (var ti = 0; ti < 10; ++ti)
        {
            var txn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(2)).Create(_cts.Token);

            for (var i = 0; i < 1000; ++i)
                _ = await SendAsync(producer, Encoding.UTF8.GetBytes(i.ToString()), txn);

            //Act
            await txn.Commit(_cts.Token);

            //Assert
            for (var i = 0; i < 1000; ++i)
            {
                var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCanReceiveMsg, _cts.Token);
                message.ShouldNotBeNull();
                int.Parse(Encoding.UTF8.GetString(message.Value())).ShouldBe(i);
            }
        }
    }

    // Upstream: TransactionEndToEndTest#testSendTxnMessageTimeout
    // Java replaces the connection of the producer with a mock that answers every request but never acknowledges a message. Here the producer connects through a relay that swallows the send commands
    // of the producer after it is connected, and the transaction is created by another client that connects to the broker directly, so the requests of the transaction are answered.
    [Fact]
    public async Task Send_GivenTransactionalMessageWithoutReceipt_ShouldThrowOperationTimeoutException()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testSendTxnMessageTimeout");
        await using var relay = await BlackHoleRelay.Start(Fixture.ServiceUrl, _cts.Token);
        await using var producerClient = PulsarClient.Builder().Authentication(Fixture.Authentication).ServiceUrl(relay.ServiceUrl).ExceptionHandler(_testOutputHelper.Log).Build();
        await using var transactionClient = CreateClient();
        await using var producer = producerClient.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).SendTimeout(TimeSpan.FromSeconds(1)).StateChangedHandler(_testOutputHelper.Log).Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var transaction = await transactionClient.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);
        relay.SwallowSendCommands();

        //Act
        var act = async () => await producer.NewMessage().Transaction(transaction).Send(Encoding.UTF8.GetBytes("Hello Pulsar!"), _cts.Token);

        //Assert
        await Should.ThrowAsync<OperationTimeoutException>(act);
        await transaction.Abort(_cts.Token);
    }

    // Upstream: TransactionEndToEndTest#testTxnTimeOutInClient
    [Fact]
    public async Task SendAndAcknowledge_GivenTransactionTimedOutInClient_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testTxnTimeOutInClient");
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String).ProducerName("testTxnTimeOut_producer").Topic(topic).SendTimeout(TimeSpan.Zero).Create();
        await using var consumer = client.NewConsumer(Schema.String).ConsumerName("testTxnTimeOut_consumer").SubscriptionName("testTxnTimeOut_sub").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token); // The subscription is created when the consumer is created upstream, but here it is done in the background
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        _ = await producer.Send(string.Empty, _cts.Token);
        await WaitUntil(() => Task.FromResult(transaction.CurrentState == TransactionState.TimedOut));

        //Act
        var sendException = await Record.ExceptionAsync(async () => await producer.NewMessage().Transaction(transaction).Send(string.Empty, _cts.Token));
        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCanReceiveMsg, _cts.Token);
        message.ShouldNotBeNull();
        var acknowledgeException = await Record.ExceptionAsync(async () => await consumer.Acknowledge(message.MessageId, transaction, _cts.Token));

        //Assert
        sendException.ShouldBeOfType<InvalidTransactionStatusException>();
        acknowledgeException.ShouldBeOfType<InvalidTransactionStatusException>();
    }

    // Upstream: TransactionEndToEndTest#testDelayedTransactionMessages (upstream group: flaky)
    [Fact]
    public async Task Commit_GivenDelayedTransactionalMessages_ShouldDeliverImmediatelyToFailoverAndDelayedToShared()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testDelayedTransactionMessages");
        await using var client = CreateClient();
        await using var failoverConsumer = client.NewConsumer(Schema.String).SubscriptionName("failover-sub").SubscriptionType(SubscriptionType.Failover).Topic(topic).Create();
        await using var sharedConsumer = client.NewConsumer(Schema.String).SubscriptionName("shared-sub").SubscriptionType(SubscriptionType.Shared).Topic(topic).Create();
        await failoverConsumer.StateChangedTo(ConsumerState.Active, _cts.Token); // The subscriptions are created when the consumers are created upstream, but here it is done in the background
        await sharedConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(false).Create();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < 10; ++i)
            receipts.Add(await SendAsync(producer, $"msg-{i}", transaction, DateTimeOffset.UtcNow.AddSeconds(7)));

        // Upstream flushes the producer here
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Act
        await transaction.Commit(_cts.Token);

        //Assert
        for (var i = 0; i < 10; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(failoverConsumer, _waitTimeForCanReceiveMsg, _cts.Token);
            message.ShouldNotBeNull();
            message.Value().ShouldBe($"msg-{i}");
        }

        var notDelivered = await TransactionFixture.ReceiveOrNull(sharedConsumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        notDelivered.ShouldBeNull();

        var received = new SortedSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 10; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(sharedConsumer, _waitTimeForCanReceiveMsg, _cts.Token);
            message.ShouldNotBeNull();
            _ = received.Add(message.Value());
        }

        received.Count.ShouldBe(10);
        for (var i = 0; i < 10; ++i)
            received.ShouldContain($"msg-{i}");
    }

    // Upstream: TransactionEndToEndTest#testIndividualAckAbortFilterAckSetInPendingAckState
    // The values 0 to 8 of the upstream test are the batch indexes of the messages of the batch.
    [Fact]
    public async Task Abort_GivenBatchIndexesAckedByTwoTransactionsAndPlainAck_ShouldRedeliverOnlyTheAbortedIndexes()
    {
        //Arrange
        const int count = 9;
        var topicName = TransactionFixture.CreateTopicName("testIndividualAckAbortFilterAckSetInPendingAckState");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .BatchIndexAckEnabled(true) // The default of the Java consumer
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("test")
            .SubscriptionType(SubscriptionType.Shared)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Fixture.ProduceBatchedMessages(topicName, 1, count, _cts.Token);
        var firstTransaction = await GetTxn(client);
        var secondTransaction = await GetTxn(client);

        // firstTransaction ack the first three messages and don't end the firstTransaction
        for (var i = 0; i < count / 3; i++)
        {
            await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, firstTransaction, _cts.Token);
        }

        // if secondTransaction abort we only can receive the middle three messages
        for (var i = 0; i < count / 3; i++)
        {
            await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, secondTransaction, _cts.Token);
        }

        // consumer normal ack the last three messages
        for (var i = 0; i < count / 3; i++)
        {
            await consumer.Acknowledge(await consumer.Receive(_cts.Token), _cts.Token);
        }

        //Act
        await secondTransaction.Abort(_cts.Token);

        //Assert
        // can receive 3 4 5 bit sit message
        for (var i = 0; i < count / 3; i++)
        {
            (await consumer.Receive(_cts.Token)).MessageId.BatchIndex.ShouldBe(i + 3);
        }

        // can't receive message anymore
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();
    }

    // Upstream: TransactionEndToEndTest#testFilterMsgsInPendingAckStateWhenConsumerDisconnect
    // The values 0 to 9 of the upstream test are the batch indexes when the messages are produced as a batch.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Abort_GivenPendingAcksAndReconnectedConsumer_ShouldDeliverOnlyAbortedMessages(bool enableBatch)
    {
        //Arrange
        const int count = 10;
        var topicName = TransactionFixture.CreateTopicName($"testFilterMsgsInPendingAckStateWhenConsumerDisconnect-{(enableBatch ? "true" : "false")}");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        var consumer = CreateSharedConsumer(client, topicName, "test");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await ProduceNumbers(client, topicName, count, enableBatch);
        var txn1 = await GetTxn(client);
        var txn2 = await GetTxn(client);

        // txn1 ack half of messages and don't end the txn1
        for (var i = 0; i < count / 2; i++)
        {
            await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, txn1, _cts.Token);
        }

        // txn2 ack the rest half of messages and commit tnx2
        for (var i = count / 2; i < count; i++)
        {
            await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, txn2, _cts.Token);
        }

        // commit txn2
        await txn2.Commit(_cts.Token);

        // close and re-create consumer
        await consumer.DisposeAsync();
        await using var recreatedConsumer = CreateSharedConsumer(client, topicName, "test");
        await recreatedConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var message = await TransactionFixture.ReceiveOrNull(recreatedConsumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        message.ShouldBeNull();

        //Act
        // abort txn1
        await txn1.Abort(_cts.Token);

        //Assert
        // after txn1 aborted, consumer will receive messages txn1 contains
        var receiveCounter = 0;
        while ((message = await TransactionFixture.ReceiveOrNull(recreatedConsumer, _waitTimeForCanReceiveMsg, _cts.Token)) is not null)
        {
            GetNumber(message, enableBatch).ShouldBe(receiveCounter);
            receiveCounter++;
        }

        receiveCounter.ShouldBe(count / 2);
    }

    // Upstream: TransactionEndToEndTest#testMsgsInPendingAckStateWouldNotGetTheConsumerStuck
    // The Java producer batches by default, but every message is sent before the next one is produced, so every batch has one message.
    // DotPulsar does not batch by default.
    [Fact]
    public async Task Receive_GivenMessagesInPendingAckStateAndSmallPrefetch_ShouldNotGetConsumerStuck()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("testMsgsInPendingAckStateWouldNotGetTheConsumerStuck");
        const string subscription = "test";
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.Int32)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        var consumer = client.NewConsumer(Schema.Int32)
            .Topic(topicName)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Shared)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        const int numStep1Receive = 2;
        const int numStep2Receive = 2;
        const int numStep3Receive = 2;
        const int numTotalMessage = numStep1Receive + numStep2Receive + numStep3Receive;

        for (var i = 0; i < numTotalMessage; i++)
        {
            await producer.Send(i, _cts.Token);
        }

        var step1Txn = await GetTxn(client);
        var step2Txn = await GetTxn(client);

        // Step 1, try to consume some messages but do not commit the transaction
        for (var i = 0; i < numStep1Receive; i++)
        {
            await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, step1Txn, _cts.Token);
        }

        // Step 2, try to consume some messages and commit the transaction
        for (var i = 0; i < numStep2Receive; i++)
        {
            await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, step2Txn, _cts.Token);
        }

        // commit step2Txn
        await step2Txn.Commit(_cts.Token);

        // close and re-create consumer
        await consumer.DisposeAsync();
        await using var consumer2 = client.NewConsumer(Schema.Int32)
            .Topic(topicName)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(numStep3Receive)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Shared)
            .Create();
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        var received = new List<int>();

        // Step 3, try to consume the rest messages and should receive all of them
        for (var i = 0; i < numStep3Receive; i++)
        {
            // should get the message instead of timeout. The first receive can wait for the retry interval of 3 seconds, because the consumer is active before its channel is usable (followup 0016)
            var message = await TransactionFixture.ReceiveOrNull(consumer2, i == 0 ? TimeSpan.FromSeconds(10) : TimeSpan.FromSeconds(3), _cts.Token);
            message.ShouldNotBeNull();
            received.Add(message.Value());
        }

        //Assert
        received.ShouldBe([numStep1Receive + numStep2Receive, numStep1Receive + numStep2Receive + 1]);
    }

    // Upstream: TransactionEndToEndTest#testUnackMessageAfterAckAllMessages
    [Theory]
    [InlineData(true, true, true)]
    [InlineData(true, true, false)]
    [InlineData(true, false, true)]
    [InlineData(true, false, false)]
    [InlineData(false, true, true)]
    [InlineData(false, true, false)]
    [InlineData(false, false, true)]
    [InlineData(false, false, false)]
    public async Task Commit_GivenAllMessagesAckedWithTransaction_ShouldLeaveNoUnackedMessages(bool batchSend, bool batchAck, bool asyncAck)
    {
        Assert.SkipWhen(SkipReasonForUnackMessageCountTests is not null, SkipReasonForUnackMessageCountTests ?? string.Empty);

        //Arrange
        const int messageCount = 50;
        const string subName = "s1";
        var topicName = TransactionFixture.CreateTopicName("tp");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subName)
            .SubscriptionType(SubscriptionType.Shared)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Publish messages.
        if (batchSend)
        {
            await Fixture.ProduceBatchedMessages(topicName, messageCount / 10, 10, _cts.Token);
        }
        else
        {
            await using var producer = client.NewProducer(Schema.ByteArray)
                .Topic(topicName)
                .StateChangedHandler(_testOutputHelper.Log)
                .Create();
            await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

            Task<MessageId>? latestSendFuture = null;
            for (var i = 0; i < messageCount; i++)
            {
                latestSendFuture = await SendAsync(producer, Encoding.UTF8.GetBytes(i.ToString()));
            }

            await latestSendFuture!;
        }

        // Ack messages with TXN.
        var txn = await GetTxn(client);
        var msgIds = new List<MessageId>();
        for (var i = 0; i < messageCount; i++)
        {
            var message = await ReceiveMessage(consumer);
            msgIds.Add(message.MessageId);
        }

        //Act
        if (batchAck)
        {
            await consumer.Acknowledge(msgIds, txn, _cts.Token);
        }
        else if (asyncAck)
        {
            Task? latestAckFuture = null;
            foreach (var msgId in msgIds)
            {
                latestAckFuture = consumer.Acknowledge(msgId, txn, _cts.Token).AsTask();
            }

            await latestAckFuture!;
        }
        else
        {
            foreach (var msgId in msgIds)
            {
                await consumer.Acknowledge(msgId, txn, _cts.Token);
            }
        }

        await txn.Commit(_cts.Token);

        //Assert
        // Verify: the quantity of un-ack messages is 0.
        (await GetConsumerUnackedMessages(topicName, subName, 0)).ShouldBe(0);
    }

    // Upstream: TransactionEndToEndTest#testAckWithTransactionReduceUnAckMessageCount
    // The broker is not accessible from the test, so the unacknowledged messages of the consumer are read from the topic stats of the admin API.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Commit_GivenHalfOfMessagesAckedWithTransaction_ShouldReduceUnackedMessageCount(bool enableBatch)
    {
        Assert.SkipWhen(SkipReasonForUnackMessageCountTests is not null, SkipReasonForUnackMessageCountTests ?? string.Empty);

        //Arrange
        const int messageCount = 50;
        const string subName = "testAckWithTransactionReduceUnAckMessageCount";
        var topicName = TransactionFixture.CreateTopicName($"testAckWithTransactionReduceUnAckMessageCount-{(enableBatch ? "true" : "false")}");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subName)
            .SubscriptionType(SubscriptionType.Shared)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        if (enableBatch)
        {
            await Fixture.ProduceBatchedMessages(topicName, messageCount / 10, 10, _cts.Token);
        }
        else
        {
            await using var producer = client.NewProducer(Schema.ByteArray)
                .Topic(topicName)
                .StateChangedHandler(_testOutputHelper.Log)
                .Create();

            var receipts = new List<Task<MessageId>>();
            for (var i = 0; i < messageCount; i++)
            {
                receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes(i.ToString())));
            }

            await Task.WhenAll(receipts);
        }

        var txn = await GetTxn(client);

        for (var i = 0; i < messageCount / 2; i++)
        {
            var message = await ReceiveMessage(consumer);
            await consumer.Acknowledge(message.MessageId, txn, _cts.Token);
        }

        //Act
        await txn.Commit(_cts.Token);

        //Assert
        (await GetConsumerUnackedMessages(topicName, subName, 0)).ShouldBe(messageCount / 2);
    }

    // Upstream: TransactionEndToEndTest#txnIndividualAckTestNoBatchAndSharedSub
    [Fact]
    public async Task Acknowledge_GivenNoBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnAckTest(false, 1, SubscriptionType.Shared);

    // Upstream: TransactionEndToEndTest#txnIndividualAckTestBatchAndSharedSub
    [Fact]
    public async Task Acknowledge_GivenBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnAckTest(true, 200, SubscriptionType.Shared);

    // Upstream: TransactionEndToEndTest#txnIndividualAckTestNoBatchAndFailoverSub
    [Fact]
    public async Task Acknowledge_GivenNoBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnAckTest(false, 1, SubscriptionType.Failover);

    // Upstream: TransactionEndToEndTest#txnIndividualAckTestBatchAndFailoverSub
    [Fact]
    public async Task Acknowledge_GivenBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
    {
        Assert.SkipWhen(SkipReasonForBatchAndFailoverIndividualAck is not null, SkipReasonForBatchAndFailoverIndividualAck ?? string.Empty);

        await TxnAckTest(true, 200, SubscriptionType.Failover);
    }

    // Upstream: TransactionEndToEndTest#testAfterDeleteTopicOtherTopicCanRecover
    [Fact]
    public async Task Send_GivenOtherTopicWithSubscriptionWasDeleted_ShouldProduceAndConsume()
    {
        //Arrange
        var topicOne = TransactionFixture.CreateTopicName("topic-one");
        var topicTwo = TransactionFixture.CreateTopicName("topic-two");
        const string sub = "test";
        await Fixture.CreateTopic(topicOne, _cts.Token);
        await Fixture.CreateSubscription(topicOne, sub, true, _cts.Token);
        await Fixture.DeleteTopic(topicOne, false, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topicTwo)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topicTwo)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(sub)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token); // The subscription is created when the consumer is created upstream, but here it is done in the background
        const string content = "test";

        //Act
        _ = await producer.Send(content, _cts.Token);
        var message = await ReceiveMessage(consumer);

        //Assert
        message.Value().ShouldBe(content);
    }

    // Upstream: TransactionEndToEndTest#txnMessageAckTest
    // Upstream reads the cursor of the broker, here it comes from the internal stats of the partition. Upstream sets acknowledgmentGroupTime to 0, DotPulsar acknowledges immediately.
    [Fact]
    public async Task Acknowledge_GivenCommittedTransactionalMessages_ShouldAckAndMoveMarkDeleteToEnd()
    {
        //Arrange
        const string subName = "test";
        const int messageCnt = 10;
        var topic = TransactionFixture.CreateTopicName("message-ack-test");
        await Fixture.CreatePartitionedTopic(topic, 1, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(false)
            .SendTimeout(TimeSpan.Zero)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        var txn = await GetTxn(client);

        for (var i = 0; i < messageCnt; i++)
        {
            _ = await SendAsync(producer, Encoding.UTF8.GetBytes("Hello Txn - " + i), txn);
        }

        // Can't receive transaction messages before commit.
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();

        //Act
        await txn.Commit(_cts.Token);

        //Assert
        var ackedMessageCount = 0;
        var receiveCnt = 0;
        for (var i = 0; i < messageCnt; i++)
        {
            var message = await ReceiveMessage(consumer);
            receiveCnt++;

            if (i % 2 == 0)
            {
                await consumer.Acknowledge(message, _cts.Token);
                ackedMessageCount++;
            }
        }

        receiveCnt.ShouldBe(messageCnt);
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();

        var checkTopic = $"{topic}-partition-0";
        var stats = await Fixture.GetInternalStats(checkTopic, _cts.Token);
        GetMarkDeletePosition(stats, subName).ShouldNotBe(stats.GetProperty("lastConfirmedEntry").GetString());

        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);

        receiveCnt = 0;
        for (var i = 0; i < messageCnt - ackedMessageCount; i++)
        {
            var message = await ReceiveMessage(consumer);
            await consumer.Acknowledge(message, _cts.Token);
            receiveCnt++;
        }

        receiveCnt.ShouldBe(messageCnt - ackedMessageCount);
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();

        // Upstream checks it once, so there is no polling. The marker of the commit is the only entry that the subscription does not acknowledge.
        stats = await Fixture.GetInternalStats(checkTopic, _cts.Token);
        IsMarkDeleteAtEnd(stats, subName).ShouldBeTrue("The mark delete position is not the position of the marker of the commit");
    }

    // Upstream: TransactionEndToEndTest#txnAckTestBatchAndCumulativeSub
    [Fact]
    public async Task AcknowledgeCumulative_GivenBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnCumulativeAckTest(true, 200, SubscriptionType.Failover);

    // Upstream: TransactionEndToEndTest#txnAckTestNoBatchAndCumulativeSub
    [Fact]
    public async Task AcknowledgeCumulative_GivenNoBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnCumulativeAckTest(false, 1, SubscriptionType.Failover);

    // Upstream: TransactionEndToEndTest#produceAndConsumeCloseStateTxnTest
    // Upstream looks the transaction up in the transaction metadata store of the broker, here it is read from the admin API until the broker has forgotten it. Upstream creates a transaction by reflection
    // that does not know that it has timed out, here it is created with the constructor of the internal class. The commit has to start within the 5 milliseconds of the timeout of that transaction.
    // Upstream aborts the transaction that has timed out without waiting for the result, which is not asserted, so it is not done here.
    [Fact]
    public async Task SendAndAcknowledge_GivenEndedOrTimedOutTransaction_ShouldThrow()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("txn-close-state");
        await Fixture.CreateTopic(topic, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("test")
            .Create();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .ProducerName("txn-close-state")
            .SendTimeout(TimeSpan.Zero)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token); // The subscription is created when the consumer is created upstream, but here it is done in the background
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var produceTxn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(2)).Create(_cts.Token);
        var consumeTxn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(2)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(produceTxn).Send(Encoding.UTF8.GetBytes("Hello Pulsar!"), _cts.Token);
        await produceTxn.Commit(_cts.Token);

        //Act
        var sendException = await Record.ExceptionAsync(async () => await producer.NewMessage().Transaction(produceTxn).Send(Encoding.UTF8.GetBytes("Hello Pulsar!"), _cts.Token));
        var produceCommitException = await Record.ExceptionAsync(async () => await produceTxn.Commit(_cts.Token));
        var message = await ReceiveMessage(consumer);
        await consumer.Acknowledge(message.MessageId, consumeTxn, _cts.Token);
        await consumeTxn.Commit(_cts.Token);
        var acknowledgeException = await Record.ExceptionAsync(async () => await consumer.Acknowledge(message.MessageId, consumeTxn, _cts.Token));
        var consumeCommitException = await Record.ExceptionAsync(async () => await consumeTxn.Commit(_cts.Token));

        //Assert
        sendException.ShouldBeOfType<InvalidTransactionStatusException>();
        produceCommitException.ShouldBeOfType<InvalidTransactionStatusException>();
        acknowledgeException.ShouldBeOfType<InvalidTransactionStatusException>();
        consumeCommitException.ShouldBeOfType<InvalidTransactionStatusException>();

        //Arrange
        var timeoutTxn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        await WaitUntil(async () => !await Fixture.TransactionMetadataExists(timeoutTxn.Id, _cts.Token));
        var timeoutTxnSkipClientTimeout = new DotPulsar.Internal.Transactions.Transaction(((PulsarClient) client).TransactionCoordinatorClient!, timeoutTxn.Id, TimeSpan.FromMilliseconds(5));

        //Act
        var commitException = await Record.ExceptionAsync(async () => await timeoutTxnSkipClientTimeout.Commit(_cts.Token));

        //Assert
        commitException.ShouldBeOfType<TransactionNotFoundException>();
        timeoutTxnSkipClientTimeout.CurrentState.ShouldBe(TransactionState.Error);
    }

    // Upstream: TransactionEndToEndTest#testTxnTimeoutAtTransactionMetadataStore
    // Upstream creates the transaction in the transaction metadata store service of the broker with a timeout of 1 millisecond, and sums the timeout counts of the stores of the coordinators.
    // Here the transaction is created by a client, and the count is the metric of the broker. The broker publishes the metrics from time to time, so it may take a while before the count is increased.
    [Fact]
    public async Task Create_GivenTimeoutElapsesAtCoordinator_ShouldIncrementCoordinatorTimeoutCount()
    {
        //Arrange
        var timeoutCountOriginal = await Fixture.GetTransactionTimeoutTotal(_cts.Token);
        await using var client = CreateClient();

        //Act
        var txnId = (await client.NewTransaction().Timeout(TimeSpan.FromMilliseconds(1)).Create(_cts.Token)).Id;
        await WaitUntil(async () => !await Fixture.TransactionMetadataExists(txnId, _cts.Token));
        await WaitUntil(async () => await Fixture.GetTransactionTimeoutTotal(_cts.Token) == timeoutCountOriginal + 1);

        //Assert
        (await Fixture.GetTransactionTimeoutTotal(_cts.Token)).ShouldBe(timeoutCountOriginal + 1);
    }

    // Upstream: TransactionEndToEndTest#transactionTimeoutTest
    [Fact]
    public async Task Acknowledge_GivenTransactionTimesOut_ShouldRedeliverMessage()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("txn-timeout");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("test")
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topicName)
            .ProducerName("txn-timeout")
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await producer.Send("Hello Pulsar!", _cts.Token);
        var consumeTimeoutTxn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(7)).Create(_cts.Token);
        var message = await ReceiveMessage(consumer);
        await consumer.Acknowledge(message.MessageId, consumeTimeoutTxn, _cts.Token);

        //Act
        var reReceiveMessage = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token);
        reReceiveMessage.ShouldBeNull();
        reReceiveMessage = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCanReceiveMsg, _cts.Token);

        //Assert
        reReceiveMessage.ShouldNotBeNull();
        reReceiveMessage.Value().ShouldBe(message.Value());
        reReceiveMessage.MessageId.ShouldBe(message.MessageId);
    }

    // Upstream: TransactionEndToEndTest#oneTransactionOneTopicWithMultiSubTest
    // The metadata of the transaction is read from the admin API, because the transaction metadata store of the broker is not accessible from the test.
    [Fact]
    public async Task Acknowledge_GivenOneTransactionAndTwoSubscriptions_ShouldRegisterBothSubscriptions()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("oneTransactionOneTopicWithMultiSubTest");
        const string subName1 = "test1";
        const string subName2 = "test2";
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer1 = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subName1)
            .Create();
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var consumer2 = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subName2)
            .Create();
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        var messageId = await producer.Send(Encoding.UTF8.GetBytes("Hello Pulsar"), _cts.Token);
        var txn = await GetTxn(client);

        //Act
        await consumer1.Acknowledge(messageId, txn, _cts.Token);
        await consumer2.Acknowledge(messageId, txn, _cts.Token);
        var metadata = await Fixture.GetTransactionMetadata(txn.Id, _cts.Token);

        //Assert
        var ackedPartitions = metadata.GetProperty("ackedPartitions");
        ackedPartitions.EnumerateObject().Sum(topic => topic.Value.EnumerateObject().Count()).ShouldBe(2);
        ackedPartitions.GetProperty(topicName).EnumerateObject().Select(subscription => subscription.Name).ShouldBe([subName1, subName2], true);

        await txn.Abort(_cts.Token);
    }

    // Upstream: TransactionEndToEndTest#testCumulativeAckRedeliverMessages
    // The Java producer has batching enabled, but it sends each message with a blocking send, so each message is a batch of its own. The row with batching sends the 5 messages as one batch, so the
    // cumulative acknowledgment of the third message is an acknowledgment of a batch index. The conflict is also tested without redelivery, in TransactionConsumerTests.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AcknowledgeCumulative_GivenConflictingTransactionThenAbort_ShouldRedeliverMessages(bool batching)
    {
        //Arrange
        const int count = 5;
        const int transactionCumulativeAck = 3;
        var topicName = TransactionFixture.CreateTopicName("testCumulativeAckRedeliverMessages");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("test")
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .BatchingMaxMessages(count)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5))
            .EnableBatching(batching)
            .SendTimeout(TimeSpan.Zero)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

        // Send 5 messages
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < count; i++)
        {
            receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes(i.ToString())));
        }

        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Select(messageId => messageId.BatchIndex >= 0).Distinct().ShouldBe([batching]);
        var transaction = await GetTxn(client);
        var invalidTransaction = await GetTxn(client);
        IMessage<byte[]>? message = null;
        for (var i = 0; i < transactionCumulativeAck; i++)
        {
            message = await ReceiveMessage(consumer);
        }

        // Receive the messages in order
        Encoding.UTF8.GetString(message!.Value()).ShouldBe("2");

        // Acknowledge the last message
        await consumer.AcknowledgeCumulative(message.MessageId, transaction, _cts.Token);

        // Another acknowledgment throws TransactionConflictException
        await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.AcknowledgeCumulative(message.MessageId, invalidTransaction, _cts.Token));

        //Act
        // Abort the transaction, and then redeliver the messages
        await transaction.Abort(_cts.Token);
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);

        //Assert
        // All the messages are received again, and the ones that were already received before the redelivery are not received twice
        for (var i = 0; i < count; i++)
        {
            message = await ReceiveMessage(consumer);
        }

        Encoding.UTF8.GetString(message!.Value()).ShouldBe("4");
        var commitTransaction = await GetTxn(client);
        await consumer.AcknowledgeCumulative(message.MessageId, commitTransaction, _cts.Token);
        await commitTransaction.Commit(_cts.Token);

        // Then the redelivery does not receive any message
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();
    }

    // Upstream: TransactionEndToEndTest#testSendTxnAckMessageToDLQ
    // The assertion of the Java test on the available permits of the consumer is dropped, it is a counter of the Java client. The dead letter consumer receiving the message covers it.
    [Fact]
    public async Task Abort_GivenMessageExceedsMaxRedeliverCount_ShouldSendMessageToDeadLetterTopic()
    {
        //Arrange
        const string value = "test";
        var topicName = TransactionFixture.CreateTopicName("testSendTxnAckMessageToDLQ");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = CreateDeadLetterConsumer(client, topicName);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, $"{topicName}-test-DLQ");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await deadLetterConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        _ = await producer.Send(Encoding.UTF8.GetBytes(value), _cts.Token);

        //Act
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var message = await ReceiveMessage(consumer);
        message.RedeliveryCount.ShouldBe(0U);
        await consumer.Acknowledge(message.MessageId, transaction, _cts.Token);
        await transaction.Abort(_cts.Token);

        transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        message = await ReceiveMessage(consumer);
        message.RedeliveryCount.ShouldBe(1U);
        await consumer.Acknowledge(message.MessageId, transaction, _cts.Token);
        await transaction.Abort(_cts.Token);

        //Assert
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();
        var deadLetter = await ReceiveMessage(deadLetterConsumer);
        Encoding.UTF8.GetString(deadLetter.Value()).ShouldBe(value);
    }

    // Upstream: TransactionEndToEndTest#testSendTxnAckBatchMessageToDLQ
    // The two messages are sent as one batch. Java relies on the default maximum publish delay of 1 millisecond, so two sends that follow each other form a batch in practice. The test
    // makes it certain with a batch of exactly two messages. The send timeout is 5 seconds and not the 1 second of the upstream test, so that a loaded machine does not fail the test.
    [Fact]
    public async Task Abort_GivenBatchMessageExceedsMaxRedeliverCount_ShouldSendBatchToDeadLetterTopic()
    {
        //Arrange
        const string value1 = "test1";
        const string value2 = "test2";
        var topicName = TransactionFixture.CreateTopicName("testSendTxnAckBatchMessageToDLQ");
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .EnableBatching(true)
            .BatchingMaxMessages(2)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .SendTimeout(TimeSpan.FromSeconds(5))
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = CreateDeadLetterConsumer(client, topicName);
        await using var deadLetterConsumer = CreateDeadLetterConsumer(client, $"{topicName}-test-DLQ");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await deadLetterConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receipts = new[] { await SendAsync(producer, Encoding.UTF8.GetBytes(value1)), await SendAsync(producer, Encoding.UTF8.GetBytes(value2)) };
        await Task.WhenAll(receipts);

        //Act
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var message = await ReceiveMessage(consumer);
        Encoding.UTF8.GetString(message.Value()).ShouldBe(value1);
        await consumer.Acknowledge(message.MessageId, transaction, _cts.Token);
        await transaction.Abort(_cts.Token);

        // The second message of the first delivery, and then the two messages of the batch again
        for (var i = 0; i < 3; i++)
            message = await ReceiveMessage(consumer);

        transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        Encoding.UTF8.GetString(message.Value()).ShouldBe(value2);
        await consumer.Acknowledge(message.MessageId, transaction, _cts.Token);
        await transaction.Abort(_cts.Token);

        //Assert
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();
        var deadLetter = await ReceiveMessage(deadLetterConsumer);
        Encoding.UTF8.GetString(deadLetter.Value()).ShouldBe(value1);
        deadLetter = await ReceiveMessage(deadLetterConsumer);
        Encoding.UTF8.GetString(deadLetter.Value()).ShouldBe(value2);
    }

    // Upstream: TransactionEndToEndTest#testAckWithTransactionReduceUnackCountNotInPendingAcks
    // The unacknowledged messages of the consumer are read from the topic stats of the admin API.
    [Fact]
    public async Task Acknowledge_GivenOneBatchIndexAckedWithTransaction_ShouldReduceUnackedCountByOne()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("testAckWithTransactionReduceUnackCountNotInPendingAcks");
        const string subName = "test";
        await Fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subName)
            .SubscriptionType(SubscriptionType.Shared)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // send 5 messages with one batch
        await Fixture.ProduceBatchedMessages(topicName, 1, 5, _cts.Token);

        // receive the batch messages add to a list
        var messageIds = new List<MessageId>();
        for (var i = 0; i < 5; i++)
        {
            messageIds.Add((await ReceiveMessage(consumer)).MessageId);
        }

        // Ack one message in the batch with a transaction.
        var txn = await GetTxn(client);

        //Act
        await consumer.Acknowledge(messageIds[1], txn, _cts.Token);

        //Assert
        // ack one message, the unack count is 4
        (await GetConsumerUnackedMessages(topicName, subName, 0)).ShouldBe(4);

        await txn.Abort(_cts.Token);
    }

    // Upstream: TransactionEndToEndTest#txnAckTest
    private async Task TxnAckTest(bool batchEnable, uint maxBatchSize, SubscriptionType subscriptionType)
    {
        //Arrange
        const int messageCnt = 1000;
        var normalTopic = TransactionFixture.CreateTopicName("normal-topic");
        await Fixture.CreateTopic(normalTopic, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(normalTopic)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("test")
            .SubscriptionType(subscriptionType)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(normalTopic)
            .EnableBatching(batchEnable)
            .BatchingMaxMessages(maxBatchSize)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();

        for (var retryCnt = 0; retryCnt < 2; retryCnt++)
        {
            var txn = await GetTxn(client);

            // produce normal messages
            var receipts = new List<Task<MessageId>>();
            for (var i = 0; i < messageCnt; i++)
            {
                receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes("hello")));
            }

            // consume and ack messages with txn
            for (var i = 0; i < messageCnt; i++)
            {
                var message = await ReceiveMessage(consumer);
                await consumer.Acknowledge(message.MessageId, txn, _cts.Token);
            }

            // the messages are pending ack state and can't be received
            (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();

            //Act
            // 1) txn abort
            await txn.Abort(_cts.Token);

            // after transaction abort, the messages could be received
            var commitTxn = await GetTxn(client);
            for (var i = 0; i < messageCnt; i++)
            {
                var message = await ReceiveMessage(consumer);
                await consumer.Acknowledge(message.MessageId, commitTxn, _cts.Token);
            }

            // 2) ack committed by a new txn
            await commitTxn.Commit(_cts.Token);

            //Assert
            // after transaction commit, the messages can't be received
            (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();

            // Upstream sets the state of the transaction to open using reflection and commits it again. The transaction coordinator is asked to
            // commit the transaction that has ended, as it is done there, and the expected exception is the same.
            var tcClient = ((PulsarClient) client).TransactionCoordinatorClient!;
            await Should.ThrowAsync<TransactionNotFoundException>(async () => await tcClient.Commit(commitTxn.Id, _cts.Token));

            await Task.WhenAll(receipts);
        }
    }

    // Upstream: TransactionEndToEndTest#txnCumulativeAckTest
    private async Task TxnCumulativeAckTest(bool batchEnable, uint maxBatchSize, SubscriptionType subscriptionType)
    {
        //Arrange
        const int messageCnt = 1000;
        var normalTopic = TransactionFixture.CreateTopicName("normal-topic");
        await Fixture.CreateTopic(normalTopic, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(normalTopic)
            .StateChangedHandler(_testOutputHelper.Log)
            .AckTimeout(TimeSpan.FromMinutes(1)) // It must not expire while the test runs
            .SubscriptionName("test")
            .SubscriptionType(subscriptionType)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(normalTopic)
            .EnableBatching(batchEnable)
            .BatchingMaxMessages(maxBatchSize)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();

        for (var retryCnt = 0; retryCnt < 2; retryCnt++)
        {
            var abortTxn = await GetTxn(client);

            // produce normal messages
            var receipts = new List<Task<MessageId>>();
            for (var i = 0; i < messageCnt; i++)
            {
                receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes("hello")));
            }

            await Task.WhenAll(receipts);
            await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);

            // consume and ack messages with txn
            IMessage<byte[]>? message = null;
            for (var i = 0; i < messageCnt; i++)
            {
                message = await ReceiveMessage(consumer);

                if (i % 3 == 0)
                    await consumer.AcknowledgeCumulative(message.MessageId, abortTxn, _cts.Token);
            }

            // the last message was acknowledged by the transaction, so the same acknowledgment conflicts
            var lastMessageId = message!.MessageId;
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.AcknowledgeCumulative(lastMessageId, abortTxn, _cts.Token));

            // the entry before the last entry is before the position of the transaction
            var previousEntryId = new MessageId(lastMessageId.LedgerId, lastMessageId.EntryId - 1, -1, -1, lastMessageId.Topic);
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.AcknowledgeCumulative(previousEntryId, abortTxn, _cts.Token));

            // the messages are pending ack state and can't be received
            (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();

            //Act
            await abortTxn.Abort(_cts.Token);
            await consumer.RedeliverUnacknowledgedMessages(_cts.Token);

            //Assert
            // after transaction abort and redelivery, exactly the messages that were not acknowledged are received
            var commitTxn = await GetTxn(client);
            for (var i = 0; i < messageCnt; i++)
            {
                message = await ReceiveMessage(consumer);

                if (i % 3 == 0)
                    await consumer.AcknowledgeCumulative(message.MessageId, commitTxn, _cts.Token);
            }

            await commitTxn.Commit(_cts.Token);

            // Upstream sets the state of the transaction to open using reflection and commits it again. The transaction coordinator is asked to
            // commit the transaction that has ended, as it is done there, and the expected exception is the same.
            var tcClient = ((PulsarClient) client).TransactionCoordinatorClient!;
            await Should.ThrowAsync<TransactionNotFoundException>(async () => await tcClient.Commit(commitTxn.Id, _cts.Token));

            // after transaction commit, the messages can't be received
            (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCannotReceiveMsg, _cts.Token)).ShouldBeNull();
        }
    }

    public void Dispose()
        => _cts.Dispose();

    // Upstream: TransactionEndToEndTest#getTxn
    private async ValueTask<ITransaction> GetTxn(IPulsarClient client)
        => await client.NewTransaction().Timeout(TimeSpan.FromMinutes(10)).Create(_cts.Token);

    private IPulsarClient CreateClient()
        => Fixture.CreateClient(_testOutputHelper.Log);

    // Upstream: Producer#sendAsync. It returns when the message is enqueued, in the order of the calls, and the task it returns completes when the receipt arrives
    private async ValueTask<Task<MessageId>> SendAsync<TMessage>(IProducer<TMessage> producer, TMessage value, ITransaction? transaction = null, DateTimeOffset? deliverAt = null)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);
        var metadata = new MessageMetadata { Transaction = transaction };

        if (deliverAt.HasValue)
            metadata.DeliverAtTimeAsDateTimeOffset = deliverAt.Value;

        await producer.SendChannel.Send(metadata, value, id =>
        {
            _ = receipt.TrySetResult(id);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }

    // Upstream: Awaitility.await(). It waits at most 10 seconds and polls every 100 milliseconds
    private async Task WaitUntil(Func<Task<bool>> condition, TimeSpan? atMost = null)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(atMost ?? TimeSpan.FromSeconds(10));

        while (!await condition())
            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }

    // Upstream: TransactionEndToEndTest#testSendTxnAckMessageToDLQ (the dead letter consumer has the same dead letter policy)
    private IConsumer<byte[]> CreateDeadLetterConsumer(IPulsarClient client, string topicName)
        => client.NewConsumer(Schema.ByteArray)
        .DeadLetterPolicy(new DeadLetterPolicy(1))
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName("test")
        .SubscriptionType(SubscriptionType.Shared)
        .Topic(topicName)
        .Create();

    private IConsumer<byte[]> CreateSharedConsumer(IPulsarClient client, string topicName, string subscription)
        => client.NewConsumer(Schema.ByteArray)
        .Topic(topicName)
        .AckReceiptEnabled(true)
        .BatchIndexAckEnabled(true) // The default of the Java consumer
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(subscription)
        .SubscriptionType(SubscriptionType.Shared)
        .Create();

    // Produces the numbers 0 to count - 1, not in a transaction. A batch has the numbers as batch indexes, see the remarks of this class.
    private async Task ProduceNumbers(IPulsarClient client, string topicName, int count, bool enableBatch)
    {
        if (enableBatch)
        {
            await Fixture.ProduceBatchedMessages(topicName, 1, count, _cts.Token);
            return;
        }

        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();

        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < count; i++)
        {
            receipts.Add(await SendAsync(producer, BitConverter.GetBytes(i)));
        }

        await Task.WhenAll(receipts);
    }

    private static int GetNumber(IMessage<byte[]> message, bool isBatch)
        => isBatch ? message.MessageId.BatchIndex : BitConverter.ToInt32(message.Value(), 0);

    // Java: consumer.receive(5, SECONDS) that is not null
    private async Task<IMessage<TMessage>> ReceiveMessage<TMessage>(IReceive<IMessage<TMessage>> receiver)
    {
        var message = await TransactionFixture.ReceiveOrNull(receiver, _waitTimeForCanReceiveMsg, _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    // Java: getConsumers().get(consumerIndex).getUnackedMessages()
    private async Task<long> GetConsumerUnackedMessages(string topicName, string subscription, int consumerIndex)
    {
        var stats = await Fixture.GetTopicStats(topicName, _cts.Token);
        return stats.GetProperty("subscriptions").GetProperty(subscription).GetProperty("consumers")[consumerIndex].GetProperty("unackedMessages").GetInt64();
    }

    // Upstream: the broker checks that the mark delete position of the subscription is the last confirmed entry, or that the last confirmed entry is the next valid position after it,
    // which is the case when only the marker of the transaction is left
    private static bool IsMarkDeleteAtEnd(JsonElement internalStats, string subscription)
    {
        var lastConfirmedEntry = ParsePosition(internalStats.GetProperty("lastConfirmedEntry").GetString()!);
        var markDeletePosition = ParsePosition(GetMarkDeletePosition(internalStats, subscription));

        if (markDeletePosition == lastConfirmedEntry)
            return true;

        // Upstream: ManagedLedgerImpl#getNextValidPosition
        if (markDeletePosition.LedgerId == lastConfirmedEntry.LedgerId)
            return (markDeletePosition.LedgerId, markDeletePosition.EntryId + 1) == lastConfirmedEntry;

        var ledgers = internalStats.GetProperty("ledgers").EnumerateArray().Select(x => (LedgerId: x.GetProperty("ledgerId").GetInt64(), Entries: x.GetProperty("entries").GetInt64())).OrderBy(x => x.LedgerId).ToArray();
        var entriesOfCurrentLedger = ledgers.Where(x => x.LedgerId == markDeletePosition.LedgerId).Select(x => (long?) x.Entries).FirstOrDefault();

        if (entriesOfCurrentLedger.HasValue && markDeletePosition.EntryId + 1 < entriesOfCurrentLedger.Value)
            return (markDeletePosition.LedgerId, markDeletePosition.EntryId + 1) == lastConfirmedEntry;

        var nextLedgerId = ledgers.Where(x => x.LedgerId > markDeletePosition.LedgerId && (x.Entries > 0 || x.LedgerId == lastConfirmedEntry.LedgerId)).Select(x => (long?) x.LedgerId).FirstOrDefault();
        return nextLedgerId.HasValue && (nextLedgerId.Value, 0L) == lastConfirmedEntry;
    }

    private static string GetMarkDeletePosition(JsonElement internalStats, string subscription)
        => internalStats.GetProperty("cursors").GetProperty(subscription).GetProperty("markDeletePosition").GetString()!;

    private static (long LedgerId, long EntryId) ParsePosition(string position)
    {
        var parts = position.Split(':');
        return (long.Parse(parts[0]), long.Parse(parts[1]));
    }
}
