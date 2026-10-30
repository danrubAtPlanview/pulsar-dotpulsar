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

// The tests of the Java classes TransactionTest, TransactionProduceTest, PendingAckPersistentTest and ConsumerRedeliveryTest that use
// transactions only to acknowledge messages, and the tests of the transactional acknowledgments of DotPulsar that have no upstream counterpart.
// The messages are produced without a transaction. The broker has acknowledgmentAtBatchIndexLevelEnabled=true.
//
// A batch is produced by TransactionFixtureBase.ProduceBatchedMessages. It produces numbered payloads, so the tests that produce batches identify the messages by their batch index.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionConsumerTests : IDisposable
{
    private static readonly TimeSpan _waitTimeToReceive = TimeSpan.FromSeconds(5);
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionConsumerTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // TODO(WP7): The producer of the upstream test has a send timeout of 5 seconds. DotPulsar has no send timeout yet, and its sends are not limited in time by default.
    // Upstream: TransactionTest#testTransactionAckMessageList
    [Fact]
    public async Task Acknowledge_GivenMessageIdListAndAbortedThenCommittedTransaction_ShouldRedeliverOnlyAfterAbort()
    {
        //Arrange
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        var topicName = TransactionFixture.CreateTopicName("test");
        const string subName = "testSub";
        await _fixture.CreateTopic(topicName, cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        var consumer = await CreateConsumer(client, topicName, subName, cts.Token);

        for (var i = 0; i < 5; i++)
        {
            await producer.Send([], cts.Token);
        }

        //verify using aborted transaction to ack message list
        var messages = new List<MessageId>();
        for (var i = 0; i < 4; i++)
        {
            var message = await consumer.Receive(cts.Token);
            messages.Add(message.MessageId);
        }

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(cts.Token);

        //Act
        var abortedAck = consumer.Acknowledge(messages, transaction, cts.Token).AsTask();
        await transaction.Abort(cts.Token);
        await abortedAck;
        await consumer.DisposeAsync();
        consumer = await CreateConsumer(client, topicName, subName, cts.Token);
        var redelivered = new List<MessageId>();
        for (var i = 0; i < 4; i++)
        {
            var message = await consumer.Receive(cts.Token);
            redelivered.Add(message.MessageId);
        }

        //verify using committed transaction to ack message list
        transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(cts.Token);
        var committedAck = consumer.Acknowledge(messages, transaction, cts.Token).AsTask();
        await transaction.Commit(cts.Token);
        await committedAck;
        await consumer.DisposeAsync();
        await using var lastConsumer = await CreateConsumer(client, topicName, subName, cts.Token);
        var remaining = await TransactionFixture.ReceiveOrNull(lastConsumer, _waitTimeToReceive, cts.Token);
        var noMoreMessage = await TransactionFixture.ReceiveOrNull(lastConsumer, _waitTimeToReceive, cts.Token);

        //Assert
        redelivered.ShouldAllBe(messageId => messages.Contains(messageId));
        remaining.ShouldNotBeNull();
        messages.ShouldNotContain(remaining.MessageId);
        noMoreMessage.ShouldBeNull();
    }

    // TODO(WP7): The producer of the upstream test has a send timeout of 5 seconds. DotPulsar has no send timeout yet, and its sends are not limited in time by default.
    // Upstream: TransactionTest#testTransactionAckMessages
    // The Java test sets the messages of a Messages container with reflection. The equivalent is to acknowledge the IMessages.
    [Fact]
    public async Task Acknowledge_GivenMessagesOnPartitionedTopicAndAbortedThenCommittedTransaction_ShouldRedeliverOnlyAfterAbort()
    {
        //Arrange
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        var topicName = TransactionFixture.CreateTopicName("testTransactionAckMessages");
        const string subName = "testSub";
        await _fixture.CreatePartitionedTopic(topicName, 2, cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        var consumer = await CreateConsumer(client, topicName, subName, cts.Token);

        for (var i = 0; i < 4; i++)
        {
            await producer.Send([], cts.Token);
        }

        var messageList = new List<IMessage<byte[]>>();
        for (var i = 0; i < 4; i++)
        {
            messageList.Add(await consumer.Receive(cts.Token));
        }

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(cts.Token);

        //Act
        var abortedAck = consumer.Acknowledge(messageList, transaction, cts.Token).AsTask();
        await transaction.Abort(cts.Token);
        await abortedAck;
        await consumer.DisposeAsync();
        consumer = await CreateConsumer(client, topicName, subName, cts.Token);
        var messageIds = messageList.Select(message => message.MessageId).ToList();
        var redelivered = new List<MessageId>();
        for (var i = 0; i < 4; i++)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeToReceive, cts.Token);
            message.ShouldNotBeNull();
            redelivered.Add(message.MessageId);
        }

        //verify using committed transaction to ack message list
        transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(cts.Token);
        var committedAck = consumer.Acknowledge(messageList, transaction, cts.Token).AsTask();
        await transaction.Commit(cts.Token);
        await committedAck;
        await consumer.DisposeAsync();
        await using var lastConsumer = await CreateConsumer(client, topicName, subName, cts.Token);
        var noMoreMessage = await TransactionFixture.ReceiveOrNull(lastConsumer, _waitTimeToReceive, cts.Token);

        //Assert
        redelivered.ShouldAllBe(messageId => messageIds.Contains(messageId));
        noMoreMessage.ShouldBeNull();
    }

    // Upstream: TransactionTest#testPendingAckBatchMessageCommit
    [Fact]
    public async Task Acknowledge_GivenBatchMessageAckedInAbortedTransaction_ShouldAllowAckInNewTransaction()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("testPendingAckBatchMessageCommit");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topicName, "sub", _cts.Token, SubscriptionType.Shared);

        // send batch message, the size is 5
        await _fixture.ProduceBatchedMessages(topicName, 1, 5, _cts.Token);

        var txn1 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(10)).Create(_cts.Token);
        // ack the first message with transaction
        await consumer.Acknowledge((await consumer.Receive(_cts.Token)).MessageId, txn1, _cts.Token);
        var txn2 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(10)).Create(_cts.Token);
        // ack the second message with transaction
        var messageId = (await consumer.Receive(_cts.Token)).MessageId;
        await consumer.Acknowledge(messageId, txn2, _cts.Token);

        // commit the txn1
        await txn1.Commit(_cts.Token);
        // abort the txn2
        await txn2.Abort(_cts.Token);

        var txn3 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(10)).Create(_cts.Token);

        //Act
        // repeat ack the second message, can ack successful
        var act = async () => await consumer.Acknowledge(messageId, txn3, _cts.Token);

        //Assert
        await Should.NotThrowAsync(act);
    }

    // Upstream: PendingAckPersistentTest#testTransactionConflictExceptionWhenAckBatchMessage
    [Fact]
    public async Task Acknowledge_GivenBatchIndexAckedInCommittedTransaction_ShouldThrowTransactionConflictException()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("test");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("my-subscription-batch")
            .SubscriptionType(SubscriptionType.Exclusive)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceBatchedMessages(topicName, 1, 3, _cts.Token);
        var txn = await client.NewTransaction().Timeout(TimeSpan.FromDays(5)).Create(_cts.Token);
        _ = await consumer.Receive(_cts.Token);
        var message2 = await consumer.Receive(_cts.Token);
        message2.MessageId.BatchIndex.ShouldBe(1);
        await consumer.Acknowledge(message2.MessageId, txn, _cts.Token);
        var txn2 = await client.NewTransaction().Timeout(TimeSpan.FromDays(5)).Create(_cts.Token);
        await txn.Commit(_cts.Token);

        //Act
        var act = async () => await consumer.Acknowledge(message2.MessageId, txn2, _cts.Token);

        //Assert
        await Should.ThrowAsync<TransactionConflictException>(act);
    }

    // Upstream: ConsumerRedeliveryTest#testSelectiveRedeliveryAfterConflictingTransactionalBatchAck
    [Theory]
    [InlineData(SubscriptionType.Shared, false)]
    [InlineData(SubscriptionType.Shared, true)]
    [InlineData(SubscriptionType.KeyShared, false)]
    [InlineData(SubscriptionType.KeyShared, true)]
    public async Task RedeliverUnacknowledgedMessages_GivenConflictingTransactionalBatchAck_ShouldRedeliverUntouchedBatchIndexes(SubscriptionType subscriptionType, bool includePendingPosition)
    {
        //Arrange
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(60));
        var topicName = TransactionFixture.CreateTopicName("selective-redelivery");
        await _fixture.CreateTopic(topicName, cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.Int32)
            .Topic(topicName)
            .EnableBatching(true)
            .BatcherType(BatcherType.KeyBased)
            .BatchingMaxMessages(2)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.Int32)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .BatchIndexAckEnabled(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("sub")
            .SubscriptionType(subscriptionType)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, cts.Token);

        var receipts = new List<Task<MessageId>>
        {
            await SendWithKey(producer, "key", 0, cts.Token),
            await SendWithKey(producer, "key", 1, cts.Token)
        };
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(10), cts.Token);
        var first = await ReceiveMessage(consumer, cts.Token);
        var last = await ReceiveMessage(consumer, cts.Token);
        var unacked = new List<IMessage<int>> { last };

        if (includePendingPosition)
        {
            receipts = [await SendWithKey(producer, "key", 2, cts.Token), await SendWithKey(producer, "key", 3, cts.Token)];
            await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(10), cts.Token);
            unacked.Add(await ReceiveMessage(consumer, cts.Token));
            unacked.Add(await ReceiveMessage(consumer, cts.Token));
        }

        var txn = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(2)).Create(cts.Token);
        var conflicting = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(2)).Create(cts.Token);

        try
        {
            await consumer.Acknowledge(first.MessageId, txn, cts.Token);
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.Acknowledge(first.MessageId, conflicting, cts.Token));
            await txn.Commit(cts.Token);
            var requested = unacked.Select(message => message.MessageId).ToList();

            //Act
            await consumer.RedeliverUnacknowledgedMessages(requested, cts.Token);

            //Assert
            var redelivered = new List<MessageId>();
            for (var i = 0; i < unacked.Count; i++)
            {
                var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), cts.Token);
                message.ShouldNotBeNull("untouched batch indexes must remain selectively redeliverable");
                redelivered.Add(message.MessageId);
                await consumer.Acknowledge(message.MessageId, cts.Token);
            }

            redelivered.ShouldBe(requested, ignoreOrder: true);
        }
        finally
        {
            if (txn.CurrentState == TransactionState.Open)
                await txn.Abort(cts.Token);

            await conflicting.Abort(cts.Token);
        }
    }

    // Upstream: TransactionProduceTest#ackCommitTest
    // The producer of the upstream test has BatchingMaxMessages of 1, so every message is a batch of one. DotPulsar does not batch yet, and
    // its round robin router already changes the partition for every message. The pending acks are read from the admin API of the broker.
    // The transaction of the upstream test has a timeout of 5 seconds, which the test only meets because the broker is read in memory there.
    // Every pending ack is a request to the admin API here, so the transaction has the timeout of ackAbortTest, which is 30 seconds.
    [Fact]
    public async Task Commit_GivenTransactionalAcks_ShouldClearPendingAcksAndNotRedeliver()
    {
        //Arrange
        const string subscriptionName = "ackCommitTest";
        const int incomingMessageCnt = 10;
        var topicName = TransactionFixture.CreateTopicName("ack-commit");
        await _fixture.CreatePartitionedTopic(topicName, 3, _cts.Token);
        await using var client = CreateClient();
        var txn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(_cts.Token);
        await using var incomingProducer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < incomingMessageCnt; i++)
        {
            receipts.Add(await SendAsync(incomingProducer, Encoding.UTF8.GetBytes("Hello Txn.")));
        }

        await using var consumer = await CreateConsumer(client, topicName, subscriptionName, _cts.Token, SubscriptionType.Shared, SubscriptionInitialPosition.Earliest);

        var messageIds = new List<MessageId>();
        var acks = new List<Task>();
        for (var i = 0; i < incomingMessageCnt; i++)
        {
            var message = await consumer.Receive(_cts.Token);
            messageIds.Add(message.MessageId);
            acks.Add(consumer.Acknowledge(message.MessageId, txn, _cts.Token).AsTask());
        }

        // The pending messages count should be the incomingMessageCnt
        await Eventually(async () => (await GetPendingAckCount(subscriptionName, messageIds)).ShouldBe(incomingMessageCnt), TimeSpan.FromSeconds(10));

        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        var noMessage = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);

        // The pending messages count should be the incomingMessageCnt
        var pendingAckCountBeforeCommit = await GetPendingAckCount(subscriptionName, messageIds);

        //Act
        await txn.Commit(_cts.Token);

        //Assert
        noMessage.ShouldBeNull();
        pendingAckCountBeforeCommit.ShouldBe(incomingMessageCnt);

        // After commit, the pending messages count should be 0
        await Eventually(async () => (await GetPendingAckCount(subscriptionName, messageIds)).ShouldBe(0), TimeSpan.FromSeconds(10));

        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        for (var i = 0; i < incomingMessageCnt; i++)
        {
            (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldBeNull();
        }

        await Task.WhenAll(acks);
        await Task.WhenAll(receipts);
    }

    // Upstream: TransactionProduceTest#ackAbortTest
    // See the remarks of the test above.
    [Fact]
    public async Task Abort_GivenTransactionalAcks_ShouldClearPendingAcksAndRedeliver()
    {
        //Arrange
        const string subscriptionName = "ackAbortTest";
        const int incomingMessageCnt = 10;
        var topicName = TransactionFixture.CreateTopicName("ack-abort");
        await _fixture.CreatePartitionedTopic(topicName, 3, _cts.Token);
        await using var client = CreateClient();
        var txn = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(_cts.Token);
        await using var incomingProducer = client.NewProducer(Schema.ByteArray)
            .Topic(topicName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        for (var i = 0; i < incomingMessageCnt; i++)
        {
            await incomingProducer.Send(Encoding.UTF8.GetBytes("Hello Txn."), _cts.Token);
        }

        await using var consumer = await CreateConsumer(client, topicName, subscriptionName, _cts.Token, SubscriptionType.Shared, SubscriptionInitialPosition.Earliest);

        var messageIds = new List<MessageId>();
        var acks = new List<Task>();
        for (var i = 0; i < incomingMessageCnt; i++)
        {
            var message = await consumer.Receive(_cts.Token);
            messageIds.Add(message.MessageId);
            acks.Add(consumer.Acknowledge(message.MessageId, txn, _cts.Token).AsTask());
        }

        // The pending messages count should be the incomingMessageCnt
        await Eventually(async () => (await GetPendingAckCount(subscriptionName, messageIds)).ShouldBe(incomingMessageCnt), TimeSpan.FromSeconds(10));

        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        var noMessage = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);

        // The pending messages count should be the incomingMessageCnt
        var pendingAckCountBeforeAbort = await GetPendingAckCount(subscriptionName, messageIds);

        //Act
        await txn.Abort(_cts.Token);

        //Assert
        noMessage.ShouldBeNull();
        pendingAckCountBeforeAbort.ShouldBe(incomingMessageCnt);

        // After abort, the pending messages count should be 0
        await Eventually(async () => (await GetPendingAckCount(subscriptionName, messageIds)).ShouldBe(0), TimeSpan.FromSeconds(10));

        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        for (var i = 0; i < incomingMessageCnt; i++)
        {
            (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldNotBeNull();
        }

        await Task.WhenAll(acks);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTwoTransactionsAckingTheSameMessage_ShouldThrowTransactionConflictExceptionAndKeepTheConsumerUsable()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("conflict");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topicName).StateChangedHandler(_testOutputHelper.Log).Create();
        await using var consumer = await CreateConsumer(client, topicName, "conflict", _cts.Token, SubscriptionType.Shared);
        await producer.Send(Encoding.UTF8.GetBytes("1"), _cts.Token);
        await producer.Send(Encoding.UTF8.GetBytes("2"), _cts.Token);
        var first = await consumer.Receive(_cts.Token);
        var second = await consumer.Receive(_cts.Token);
        var txn1 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var txn2 = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        await consumer.Acknowledge(first.MessageId, txn1, _cts.Token);

        //Act
        var conflict = await Record.ExceptionAsync(async () => await consumer.Acknowledge(first.MessageId, txn2, _cts.Token));
        await consumer.Acknowledge(second.MessageId, txn1, _cts.Token);
        var commitOfFailedTransaction = await Record.ExceptionAsync(async () => await txn2.Commit(_cts.Token));
        await txn1.Commit(_cts.Token);

        //Assert
        conflict.ShouldBeOfType<TransactionConflictException>();
        consumer.State.IsFinalState().ShouldBeFalse();
        commitOfFailedTransaction.ShouldBeOfType<TransactionHasOperationFailedException>();
        txn1.CurrentState.ShouldBe(TransactionState.Committed);
        txn2.CurrentState.ShouldBe(TransactionState.Aborted);
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeToReceive, _cts.Token)).ShouldBeNull();
    }

    // Upstream: TransactionTest#testCreateTransactionSystemTopic (the acknowledgment of a message id that was never delivered)
    [Fact]
    public async Task Acknowledge_GivenMessageIdThatWasNeverDelivered_ShouldThrowTransactionConflictExceptionOrCompleteAndNotFaultTheConsumer()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("testCreateTransactionSystemTopic");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = await CreateConsumer(client, topicName, "test", _cts.Token, SubscriptionType.Shared);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(async () => await consumer.Acknowledge(new MessageId(10, 10, 10, -1), transaction, _cts.Token));

        //Assert
        if (exception is not null)
            exception.ShouldBeOfType<TransactionConflictException>();

        consumer.State.IsFinalState().ShouldBeFalse();
    }

    // Upstream: TransactionEndToEndTest#testTxnTimeOutInClient
    // This is the acknowledgment half of the upstream test. The send half needs the transactional producer.
    [Fact]
    public async Task Acknowledge_GivenTransactionTimedOutInClient_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("testTxnTimeOutInClient");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topicName)
            .ProducerName("testTxnTimeOut_producer")
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topicName)
            .ConsumerName("testTxnTimeOut_consumer")
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("testTxnTimeOut_sub")
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(1)).Create(_cts.Token);
        await producer.Send(string.Empty, _cts.Token);
        using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        waitCts.CancelAfter(TimeSpan.FromSeconds(10));
        while (transaction.CurrentState != TransactionState.TimedOut)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), waitCts.Token);
        }

        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeToReceive, _cts.Token);
        message.ShouldNotBeNull();

        //Act
        var single = await Record.ExceptionAsync(async () => await consumer.Acknowledge(message.MessageId, transaction, _cts.Token));
        var multiple = await Record.ExceptionAsync(async () => await consumer.Acknowledge([message.MessageId], transaction, _cts.Token));
        var cumulative = await Record.ExceptionAsync(async () => await consumer.AcknowledgeCumulative(message.MessageId, transaction, _cts.Token));

        //Assert
        single.ShouldBeOfType<InvalidTransactionStatusException>();
        multiple.ShouldBeOfType<InvalidTransactionStatusException>();
        cumulative.ShouldBeOfType<InvalidTransactionStatusException>();
        consumer.State.IsFinalState().ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenSharedSubscription_ShouldThrowConfigurationException()
    {
        //Arrange
        var topicName = TransactionFixture.CreateTopicName("cumulative-shared");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topicName).StateChangedHandler(_testOutputHelper.Log).Create();
        await using var consumer = await CreateConsumer(client, topicName, "cumulative", _cts.Token, SubscriptionType.Shared);
        await producer.Send(Encoding.UTF8.GetBytes("1"), _cts.Token);
        var message = await consumer.Receive(_cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(async () => await consumer.AcknowledgeCumulative(message.MessageId, transaction, _cts.Token));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>();
        exception.Message.ShouldBe("Cannot use cumulative acks on a non-exclusive/non-failover subscription");
        consumer.State.IsFinalState().ShouldBeFalse();
        await transaction.Commit(_cts.Token);
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenCommittedTransaction_ShouldNotDeliverTheAcknowledgedMessagesAgain()
    {
        //Arrange
        const string subscription = "cumulative";
        var topicName = TransactionFixture.CreateTopicName("cumulative-commit");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topicName).StateChangedHandler(_testOutputHelper.Log).Create();
        var consumer = await CreateConsumer(client, topicName, subscription, _cts.Token);
        for (var i = 0; i < 10; i++)
        {
            await producer.Send(Encoding.UTF8.GetBytes(i.ToString()), _cts.Token);
        }

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        for (var i = 0; i < 10; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i == 4)
                await consumer.AcknowledgeCumulative(message, transaction, _cts.Token);
        }

        //Act
        await transaction.Commit(_cts.Token);
        await consumer.DisposeAsync();
        await using var resubscribedConsumer = await CreateConsumer(client, topicName, subscription, _cts.Token);
        var received = new List<string>();
        while (await TransactionFixture.ReceiveOrNull(resubscribedConsumer, _waitTimeToReceive, _cts.Token) is { } message)
        {
            received.Add(Encoding.UTF8.GetString(message.Value()));
        }

        //Assert
        received.ShouldBe(["5", "6", "7", "8", "9"]);
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenBatchIndexInCommittedTransaction_ShouldDeliverOnlyTheLaterBatchIndexesAgain()
    {
        //Arrange
        const string subscription = "cumulative-batch";
        var topicName = TransactionFixture.CreateTopicName("cumulative-batch-commit");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        var consumer = await CreateConsumer(client, topicName, subscription, _cts.Token);
        await _fixture.ProduceBatchedMessages(topicName, 2, 10, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        for (var i = 0; i < 20; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            // The message with batch index 4 in the second batch
            if (i == 14)
                await consumer.AcknowledgeCumulative(message, transaction, _cts.Token);
        }

        //Act
        await transaction.Commit(_cts.Token);
        await consumer.DisposeAsync();
        await using var resubscribedConsumer = await CreateConsumer(client, topicName, subscription, _cts.Token);
        var received = new List<IMessage<byte[]>>();
        while (await TransactionFixture.ReceiveOrNull(resubscribedConsumer, _waitTimeToReceive, _cts.Token) is { } message)
        {
            received.Add(message);
        }

        //Assert
        received.Count.ShouldBe(5);
        received.Select(message => message.MessageId.BatchIndex).ShouldBe([5, 6, 7, 8, 9]);
        received.Select(message => message.MessageId.EntryId).Distinct().Count().ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenBatchIndexesInCommittedTransaction_ShouldDeliverOnlyTheOtherBatchIndexesAgain()
    {
        //Arrange
        const string subscription = "individual-batch";
        var topicName = TransactionFixture.CreateTopicName("individual-batch-commit");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        var consumer = await CreateConsumer(client, topicName, subscription, _cts.Token);
        await _fixture.ProduceBatchedMessages(topicName, 1, 10, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        for (var i = 0; i < 10; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i is 0 or 2)
                await consumer.Acknowledge(message, transaction, _cts.Token);
        }

        //Act
        await transaction.Commit(_cts.Token);
        await consumer.DisposeAsync();
        await using var resubscribedConsumer = await CreateConsumer(client, topicName, subscription, _cts.Token);
        var received = new List<IMessage<byte[]>>();
        while (await TransactionFixture.ReceiveOrNull(resubscribedConsumer, _waitTimeToReceive, _cts.Token) is { } message)
        {
            received.Add(message);
        }

        //Assert
        received.Select(message => message.MessageId.BatchIndex).ShouldBe([1, 3, 4, 5, 6, 7, 8, 9]);
    }

    // Additional coverage: the cumulative acknowledgment of a transaction clears the bits of the batch tracker of the consumer. When the
    // transaction is aborted and the batch is delivered again, the batch must not be acknowledged before every message is acknowledged again.
    [Fact]
    public async Task AcknowledgeCumulative_GivenAbortedTransactionAndRedeliveredBatch_ShouldAcknowledgeTheBatchOnlyAfterEveryMessageIsAcknowledgedAgain()
    {
        //Arrange
        const string subscription = "cumulative-batch-abort";
        var topicName = TransactionFixture.CreateTopicName("cumulative-batch-abort");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceBatchedMessages(topicName, 1, 10, _cts.Token);
        var delivered = await ReceiveMessages(consumer, 10);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        await consumer.AcknowledgeCumulative(delivered[4], transaction, _cts.Token);
        await transaction.Abort(_cts.Token);

        // All messages were received, so nothing is lost by asking the broker to deliver the unacknowledged messages again
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        var redelivered = await ReceiveMessages(consumer, 10);

        //Act
        // The indexes that the cumulative acknowledgment of the aborted transaction had cleared in the tracker are acknowledged last
        for (var i = 9; i >= 5; i--)
        {
            await consumer.Acknowledge(redelivered[i], _cts.Token);
        }

        var backlogWhenOnlyTheLaterIndexesAreAcknowledged = await GetBacklog(topicName, subscription);

        for (var i = 4; i >= 0; i--)
        {
            await consumer.Acknowledge(redelivered[i], _cts.Token);
        }

        //Assert
        redelivered.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, 10));
        backlogWhenOnlyTheLaterIndexesAreAcknowledged.ShouldNotBe(0);
        await WaitForBacklog(topicName, subscription, 0);
    }

    // Additional coverage: a batch that is delivered again after a partial acknowledgment of the consumer
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RedeliverUnacknowledgedMessages_GivenPartiallyAcknowledgedBatch_ShouldAcknowledgeTheBatchOnlyAfterEveryDeliveredMessageIsAcknowledged(bool batchIndexAckEnabled)
    {
        //Arrange
        var subscription = $"redelivery-{batchIndexAckEnabled}";
        var topicName = TransactionFixture.CreateTopicName("redelivery-partial-batch");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .BatchIndexAckEnabled(batchIndexAckEnabled)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceBatchedMessages(topicName, 1, 10, _cts.Token);
        var delivered = await ReceiveMessages(consumer, 10);
        for (var i = 0; i < 5; i++)
        {
            await consumer.Acknowledge(delivered[i], _cts.Token);
        }

        // The broker only knows about the acknowledgments of the batch indexes when they are sent
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        var redelivered = await ReceiveMessages(consumer, batchIndexAckEnabled ? 5 : 10);

        //Act
        // Every redelivered message except the one with the lowest batch index is acknowledged first
        foreach (var message in redelivered.Skip(1).Reverse())
        {
            await consumer.Acknowledge(message, _cts.Token);
        }

        var backlogWhenOneMessageIsMissing = await GetBacklog(topicName, subscription);
        await consumer.Acknowledge(redelivered[0], _cts.Token);

        //Assert
        redelivered.Select(message => message.MessageId.BatchIndex).ShouldBe(batchIndexAckEnabled ? Enumerable.Range(5, 5) : Enumerable.Range(0, 10));
        backlogWhenOneMessageIsMissing.ShouldNotBe(0);
        await WaitForBacklog(topicName, subscription, 0);
    }

    // Additional coverage: every partition of the topic is registered with the transaction, and the acknowledgments are routed to the consumer of the partition
    [Fact]
    public async Task Acknowledge_GivenMessagesOfPartitionedTopic_ShouldRegisterEveryPartitionWithTheTransaction()
    {
        //Arrange
        const string subscription = "partitions";
        var topicName = TransactionFixture.CreateTopicName("partitions");
        await _fixture.CreatePartitionedTopic(topicName, 3, _cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topicName).StateChangedHandler(_testOutputHelper.Log).Create();
        await using var consumer = await CreateConsumer(client, topicName, subscription, _cts.Token, SubscriptionType.Shared, SubscriptionInitialPosition.Earliest);
        for (var i = 0; i < 6; i++)
        {
            await producer.Send(Encoding.UTF8.GetBytes(i.ToString()), _cts.Token);
        }

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var messages = await ReceiveMessages(consumer, 6);

        //Act
        foreach (var message in messages)
        {
            await consumer.Acknowledge(message, transaction, _cts.Token);
        }

        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);
        await transaction.Commit(_cts.Token);

        //Assert
        var ackedPartitions = metadata.GetProperty("ackedPartitions").EnumerateObject().Select(partition => partition.Name).ToList();
        ackedPartitions.ShouldBe(Enumerable.Range(0, 3).Select(partition => $"{topicName}-partition-{partition}"), true);
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTimeToReceive, _cts.Token)).ShouldBeNull();
    }

    public void Dispose() => _cts.Dispose();

    private IPulsarClient CreateClient() => _fixture.CreateClient(_testOutputHelper.Log);

    private async Task<IConsumer<byte[]>> CreateConsumer(
        IPulsarClient client,
        string topicName,
        string subscription,
        CancellationToken cancellationToken,
        SubscriptionType subscriptionType = SubscriptionType.Exclusive,
        SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition.Latest)
    {
        var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topicName)
            .AckReceiptEnabled(true)
            .InitialPosition(initialPosition)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .SubscriptionType(subscriptionType)
            .Create();

        // The subscribe of the Java client returns when the consumer is connected, so the first receive does not wait for it
        await consumer.StateChangedTo(ConsumerState.Active, cancellationToken);
        return consumer;
    }

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

    // The Java producer.newMessage().key(key).value(value).sendAsync(): the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private static async ValueTask<Task<MessageId>> SendWithKey<TMessage>(IProducer<TMessage> producer, string key, TMessage value, CancellationToken cancellationToken)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);
        await producer.SendChannel.Send(new MessageMetadata { Key = key }, value, messageId =>
        {
            receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, cancellationToken);
        return receipt.Task;
    }

    private static async Task<IMessage<TMessage>> ReceiveMessage<TMessage>(IReceive<IMessage<TMessage>> receiver, CancellationToken cancellationToken)
    {
        var message = await TransactionFixture.ReceiveOrNull(receiver, TimeSpan.FromSeconds(10), cancellationToken);
        message.ShouldNotBeNull();
        return message;
    }

    private async Task<List<IMessage<byte[]>>> ReceiveMessages(IConsumer<byte[]> consumer, int count)
    {
        var messages = new List<IMessage<byte[]>>();

        for (var i = 0; i < count; i++)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeToReceive, _cts.Token);
            message.ShouldNotBeNull();
            messages.Add(message);
        }

        return messages;
    }

    // Java Awaitility.await().untilAsserted(...): at most 10 seconds by default, polling every 100 milliseconds
    private async Task Eventually(Func<Task> assertion, TimeSpan atMost)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await assertion();
                return;
            }
            catch (ShouldAssertException) when (stopwatch.Elapsed < atMost)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
            }
        }
    }

    // Java reads the individual ack positions of the pending ack handle of the broker. The admin API tells the state of every position.
    private async Task<int> GetPendingAckCount(string subscription, IEnumerable<MessageId> messageIds)
    {
        var count = 0;

        foreach (var messageId in messageIds)
        {
            var stats = await _fixture.GetPositionStatsInPendingAck(messageId.Topic, subscription, messageId.LedgerId, messageId.EntryId, null, _cts.Token);

            if (stats.GetProperty("state").GetString() == "PendingAck")
                count++;
        }

        return count;
    }

    private async Task<long> GetBacklog(string topicName, string subscription)
    {
        var stats = await _fixture.GetTopicStats(topicName, _cts.Token);
        return stats.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();
    }

    private async Task WaitForBacklog(string topicName, string subscription, long expected)
    {
        var backlog = long.MaxValue;

        for (var attempt = 0; attempt < 20 && backlog != expected; attempt++)
        {
            backlog = await GetBacklog(topicName, subscription);

            if (backlog != expected)
                await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
        }

        backlog.ShouldBe(expected);
    }
}
