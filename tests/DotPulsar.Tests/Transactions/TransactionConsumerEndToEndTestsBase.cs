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

// Upstream: TransactionEndToEndTest
// The tests of TransactionEndToEndTest that use transactions only to acknowledge messages. The messages are produced without a transaction,
// so these tests do not depend on the transactional producer. The tests that produce messages as part of a transaction are ported separately.
//
// Until the producer can batch messages (TODO(WP6)), a batch is produced by the perf tool of the broker image (TransactionFixtureBase.ProduceBatchedMessages).
// The tool produces its own payloads, so the tests that produce batches identify the messages by their batch index instead of by their value.
public abstract class TransactionConsumerEndToEndTestsBase<TFixture> : IDisposable where TFixture : TransactionFixture
{
    private static readonly TimeSpan _waitTimeForCanReceiveMsg = TimeSpan.FromSeconds(5); // Upstream: TransactionEndToEndTest#waitTimeForCanReceiveMsgInSec
    private static readonly TimeSpan _waitTimeForCannotReceiveMsg = TimeSpan.FromSeconds(5); // Upstream: TransactionEndToEndTest#waitTimeForCannotReceiveMsgInSec
    private readonly CancellationTokenSource _cts;
    private readonly ITestOutputHelper _testOutputHelper;

    protected TransactionConsumerEndToEndTestsBase(TFixture fixture, ITestOutputHelper outputHelper)
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

    public void Dispose() => _cts.Dispose();

    // Upstream: TransactionEndToEndTest#testIndividualAckAbortFilterAckSetInPendingAckState
    // The values 0 to 8 of the upstream test are the batch indexes, because the batch is produced by the perf tool (TODO(WP6)).
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
    // The values 0 to 9 of the upstream test are the batch indexes when the messages are produced as a batch (TODO(WP6)).
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
            // should get the message instead of timeout
            var message = await TransactionFixture.ReceiveOrNull(consumer2, TimeSpan.FromSeconds(3), _cts.Token);
            message.ShouldNotBeNull();
            received.Add(message.Value());
        }

        //Assert
        received.ShouldBe([numStep1Receive + numStep2Receive, numStep1Receive + numStep2Receive + 1]);
    }

    // Upstream: TransactionEndToEndTest#testUnackMessageAfterAckAllMessages
    // TODO(WP6): Remove the skips of the rows with batchSend. The body produces the 5 batches of 10 messages with the perf tool, which takes a minute per row.
    // When the producer can batch, produce them with EnableBatching(true) and BatchingMaxMessages(10) instead.
    [Theory]
    [InlineData(true, true, true, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, false, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, true, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, false, Skip = "Requires producer batching (WP6)")]
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
    // TODO(WP6): Remove the skip of the batch row. The body produces the 5 batches of 10 messages with the perf tool, which takes a minute.
    // When the producer can batch, produce them with EnableBatching(true) and BatchingMaxMessages(10) instead.
    [Theory]
    [InlineData(true, Skip = "Requires producer batching (WP6)")]
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

    // TODO(WP6): Remove the skip. The body produces the 5 batches of 200 messages with the perf tool, which takes more than 2 minutes. When the producer can
    // batch, produce them with EnableBatching(true) and BatchingMaxMessages(200) instead (see TxnAckTest).
    // Upstream: TransactionEndToEndTest#txnIndividualAckTestBatchAndSharedSub
    [Fact(Skip = "Requires producer batching (WP6)")]
    public async Task Acknowledge_GivenBatchAndSharedSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnAckTest(true, 200, SubscriptionType.Shared);

    // Upstream: TransactionEndToEndTest#txnIndividualAckTestNoBatchAndFailoverSub
    [Fact]
    public async Task Acknowledge_GivenNoBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
        => await TxnAckTest(false, 1, SubscriptionType.Failover);

    // TODO(WP6): Remove the skip. See txnIndividualAckTestBatchAndSharedSub.
    // Upstream: TransactionEndToEndTest#txnIndividualAckTestBatchAndFailoverSub
    [Fact(Skip = "Requires producer batching (WP6)")]
    public async Task Acknowledge_GivenBatchAndFailoverSubscription_ShouldRedeliverAfterAbortAndRemoveAfterCommit()
    {
        Assert.SkipWhen(SkipReasonForBatchAndFailoverIndividualAck is not null, SkipReasonForBatchAndFailoverIndividualAck ?? string.Empty);

        await TxnAckTest(true, 200, SubscriptionType.Failover);
    }

    // TODO(WP7a): Remove the skip. The test needs the redelivery primitive (purging the prefetch queue when the unacknowledged messages are redelivered), and
    // the batch variant needs producer batching (WP6) and the ack timeout (WP7), which the consumer of the test is configured with (AckTimeout of 1 minute).
    // Steps of TransactionEndToEndTest#txnCumulativeAckTest(batchEnable = true, maxBatchSize = 200, SubscriptionType.Failover), twice:
    // 1000 sends without a transaction (batching, BatchingMaxPublishDelay of 1 second), wait 1 second, receive 1000 messages and cumulatively ack every third in abortTxn,
    // expect TransactionConflictException for the last id again and for the id of the previous entry, expect no message to be received, abort, RedeliverUnacknowledgedMessages,
    // receive 1000 messages and cumulatively ack every third in commitTxn, commit, expect TransactionNotFoundException for a second commit, expect no message to be received.
    // Upstream: TransactionEndToEndTest#txnAckTestBatchAndCumulativeSub
    [Fact(Skip = "Requires redelivery primitive (WP7a)")]
    public Task AcknowledgeCumulative_GivenBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit()
        => Task.CompletedTask;

    // TODO(WP7a): Remove the skip. See the test above. This is the variant without batching, so only the redelivery primitive and the ack timeout (WP7) are needed.
    // Upstream: TransactionEndToEndTest#txnAckTestNoBatchAndCumulativeSub
    [Fact(Skip = "Requires redelivery primitive (WP7a)")]
    public Task AcknowledgeCumulative_GivenNoBatchAndFailoverSubscription_ShouldConflictRedeliverAfterAbortAndRemoveAfterCommit()
        => Task.CompletedTask;

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

    // TODO(WP7a): Remove the skip. The test needs the redelivery primitive: the messages that were received before the redelivery must not be received again.
    // Steps of TransactionEndToEndTest#testCumulativeAckRedeliverMessages: 5 sends (the Java producer batches), receive 3 messages, cumulatively ack the third in a transaction,
    // expect TransactionConflictException for the same cumulative ack in another transaction, abort, RedeliverUnacknowledgedMessages, receive 5 messages (the last is "4"),
    // cumulatively ack it in a new transaction, commit, expect no message to be received. The conflict is also tested without redelivery, in TransactionConsumerTests.
    // Upstream: TransactionEndToEndTest#testCumulativeAckRedeliverMessages
    [Fact(Skip = "Requires redelivery primitive (WP7a)")]
    public Task AcknowledgeCumulative_GivenConflictingTransactionThenAbort_ShouldRedeliverMessages()
        => Task.CompletedTask;

    // TODO(WP7): Remove the skip. The test needs the dead letter policy (DeadLetterPolicy with a MaxRedeliverCount of 1) and the send timeout.
    // Steps of TransactionEndToEndTest#testSendTxnAckMessageToDLQ: a Shared consumer on the topic and a Shared consumer on the dead letter topic "{topic}-test-DLQ", send "test", receive the
    // message and ack it in a transaction that is aborted (twice, the redelivery count is 1 the second time), expect no more message on the consumer and "test" on the dead letter consumer.
    // Upstream: TransactionEndToEndTest#testSendTxnAckMessageToDLQ
    [Fact(Skip = "Requires dead letter policy (WP7)")]
    public Task Abort_GivenMessageExceedsMaxRedeliverCount_ShouldSendMessageToDeadLetterTopic()
        => Task.CompletedTask;

    // TODO(WP7): Remove the skip. The test needs the dead letter policy and the send timeout, and also producer batching (WP6).
    // Steps of TransactionEndToEndTest#testSendTxnAckBatchMessageToDLQ: as above, but with "test1" and "test2" in one batch. Acknowledging "test1" and "test2" in an aborted
    // transaction sends the batch to the dead letter topic, where "test1" and then "test2" are received.
    // Upstream: TransactionEndToEndTest#testSendTxnAckBatchMessageToDLQ
    [Fact(Skip = "Requires dead letter policy (WP7)")]
    public Task Abort_GivenBatchMessageExceedsMaxRedeliverCount_ShouldSendBatchToDeadLetterTopic()
        => Task.CompletedTask;

    // Upstream: TransactionEndToEndTest#testAckWithTransactionReduceUnackCountNotInPendingAcks
    // The batch of 5 messages is produced by the perf tool (TODO(WP6)). The unacknowledged messages of the consumer are read from the topic stats of the admin API.
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
    // TODO(WP6): The batches of the test are produced by the perf tool. When the producer can batch, produce the messages with
    // EnableBatching(batchEnable) and BatchingMaxMessages(maxBatchSize) instead.
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
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();

        for (var retryCnt = 0; retryCnt < 2; retryCnt++)
        {
            var txn = await GetTxn(client);

            // produce normal messages
            var receipts = new List<Task<MessageId>>();
            if (batchEnable)
            {
                await Fixture.ProduceBatchedMessages(normalTopic, messageCnt / (int) maxBatchSize, (int) maxBatchSize, _cts.Token);
            }
            else
            {
                for (var i = 0; i < messageCnt; i++)
                {
                    receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes("hello")));
                }
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

    private IPulsarClient CreateClient() => Fixture.CreateClient(_testOutputHelper.Log);

    // Upstream: TransactionEndToEndTest#getTxn
    private async ValueTask<ITransaction> GetTxn(IPulsarClient client)
        => await client.NewTransaction().Timeout(TimeSpan.FromMinutes(10)).Create(_cts.Token);

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

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync<TMessage>(IProducer<TMessage> producer, TMessage value)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);
        await producer.SendChannel.Send(new MessageMetadata(), value, messageId =>
        {
            receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);
        return receipt.Task;
    }

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
}
