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
public abstract class TransactionEndToEndTestsBase<TFixture> : IDisposable where TFixture : TransactionFixture
{
    protected const int TopicPartition = 3; // Upstream: TransactionEndToEndTest#TOPIC_PARTITION

    private static readonly TimeSpan _waitTimeForCanReceiveMsg = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan _waitTimeForCannotReceiveMsg = TimeSpan.FromSeconds(5);

    private readonly CancellationTokenSource _cts;
    private readonly ITestOutputHelper _testOutputHelper;

    protected TransactionEndToEndTestsBase(TFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        Fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    protected TFixture Fixture { get; }

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
    // Java replaces the connection of the producer with a mock that swallows the send command. Here the producer connects through a relay that swallows what the producer sends after it is connected,
    // and the transaction is created by another client that connects to the broker directly, so the requests of the transaction are answered.
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
        relay.SwallowClientToBroker();

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

        //Assert
        sendException.ShouldBeOfType<InvalidTransactionStatusException>();
        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTimeForCanReceiveMsg, _cts.Token);
        message.ShouldNotBeNull();
        // TODO(WP5): assert that consumer.Acknowledge(message.MessageId, transaction, ct) throws InvalidTransactionStatusException
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

    // Upstream: the broker checks that the mark delete position of the subscription is the last confirmed entry, or that the last confirmed entry is the next valid position after it,
    // which is the case when only the marker of the transaction is left
    private static bool IsMarkDeleteAtEnd(JsonElement internalStats, string subscription)
    {
        var lastConfirmedEntry = ParsePosition(internalStats.GetProperty("lastConfirmedEntry").GetString()!);
        var markDeletePosition = ParsePosition(internalStats.GetProperty("cursors").GetProperty(subscription).GetProperty("markDeletePosition").GetString()!);

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

    private static (long LedgerId, long EntryId) ParsePosition(string position)
    {
        var parts = position.Split(':');
        return (long.Parse(parts[0]), long.Parse(parts[1]));
    }
}
