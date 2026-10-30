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
using DotPulsar.Extensions;
using System.Collections.Concurrent;
using System.Text;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionConsumeTests : IDisposable
{
    private const int _messageCountBeforeTransaction = 10;
    private const int _transactionMessageCount = 10;
    private const int _messageCountAfterTransaction = 10;

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionConsumeTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TransactionConsumeTest#noSortedTest
    [Fact]
    public async Task Receive_GivenOpenTransactionBetweenNormalMessages_ShouldHoldBackLaterMessagesUntilCommit()
    {
        //Arrange
        var topic = CreateTopicName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await using var exclusive = client.NewConsumer(Schema.ByteArray).SubscriptionName("exclusive-test").SubscriptionType(SubscriptionType.Exclusive).Topic(topic).Create();
        await using var shared = client.NewConsumer(Schema.ByteArray).SubscriptionName("shared-test").SubscriptionType(SubscriptionType.Shared).Topic(topic).Create();
        await exclusive.StateChangedTo(ConsumerState.Active, _cts.Token);
        await shared.StateChangedTo(ConsumerState.Active, _cts.Token);
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);

        for (var i = 0; i < _messageCountBeforeTransaction; ++i)
            _ = await producer.Send(Encoding.UTF8.GetBytes("Normal - " + i), _cts.Token);

        for (var i = 0; i < _transactionMessageCount; ++i)
            _ = await producer.NewMessage().Transaction(transaction).Send(Encoding.UTF8.GetBytes("Txn - " + i), _cts.Token);

        for (var i = _messageCountBeforeTransaction; i < _messageCountBeforeTransaction + _messageCountAfterTransaction; ++i)
            _ = await producer.Send(Encoding.UTF8.GetBytes("Normal - " + i), _cts.Token);

        //Act
        for (var i = 0; i < _messageCountBeforeTransaction + _transactionMessageCount + _messageCountAfterTransaction; ++i)
        {
            if (i < _messageCountBeforeTransaction)
            {
                (await TransactionFixture.ReceiveOrNull(exclusive, TimeSpan.FromSeconds(2), _cts.Token)).ShouldNotBeNull();
                (await TransactionFixture.ReceiveOrNull(shared, TimeSpan.FromSeconds(2), _cts.Token)).ShouldNotBeNull();
            }
            else
            {
                (await TransactionFixture.ReceiveOrNull(exclusive, TimeSpan.FromMilliseconds(500), _cts.Token)).ShouldBeNull();
                (await TransactionFixture.ReceiveOrNull(shared, TimeSpan.FromMilliseconds(500), _cts.Token)).ShouldBeNull();
            }
        }

        await transaction.Commit(_cts.Token);

        //Assert
        for (var i = 0; i < _transactionMessageCount + _messageCountAfterTransaction; ++i)
        {
            (await TransactionFixture.ReceiveOrNull(exclusive, TimeSpan.FromSeconds(5), _cts.Token)).ShouldNotBeNull();
            (await TransactionFixture.ReceiveOrNull(shared, TimeSpan.FromSeconds(5), _cts.Token)).ShouldNotBeNull();
        }
    }

    // Upstream: TransactionConsumeTest#sortedTest
    [Fact]
    public async Task Receive_GivenCommittedTransactionBetweenNormalMessages_ShouldDeliverInSendOrder()
    {
        //Arrange
        var topic = CreateTopicName();
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await using var exclusive = client.NewConsumer(Schema.ByteArray).SubscriptionName("exclusive-test").SubscriptionType(SubscriptionType.Exclusive).Topic(topic).Create();
        await using var shared = client.NewConsumer(Schema.ByteArray).SubscriptionName("shared-test").SubscriptionType(SubscriptionType.Shared).Topic(topic).Create();
        await exclusive.StateChangedTo(ConsumerState.Active, _cts.Token);
        await shared.StateChangedTo(ConsumerState.Active, _cts.Token);
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        var sentMessages = new List<string>();

        for (var i = 0; i < _messageCountBeforeTransaction; ++i)
            sentMessages.Add(await SendMessage(producer, "Normal - " + i));

        for (var i = 0; i < _transactionMessageCount; ++i)
            sentMessages.Add(await SendMessage(producer, "Txn - " + i, transaction));

        //Act
        await transaction.Commit(_cts.Token);

        for (var i = _messageCountBeforeTransaction; i < _messageCountBeforeTransaction + _messageCountAfterTransaction; ++i)
            sentMessages.Add(await SendMessage(producer, "Normal - " + i));

        //Assert
        for (var i = 0; i < sentMessages.Count; ++i)
        {
            var exclusiveMessage = await TransactionFixture.ReceiveOrNull(exclusive, TimeSpan.FromSeconds(2), _cts.Token);
            exclusiveMessage.ShouldNotBeNull();
            Encoding.UTF8.GetString(exclusiveMessage.Value()).ShouldBe(sentMessages[i]);

            var sharedMessage = await TransactionFixture.ReceiveOrNull(shared, TimeSpan.FromSeconds(2), _cts.Token);
            sharedMessage.ShouldNotBeNull();
            Encoding.UTF8.GetString(sharedMessage.Value()).ShouldBe(sentMessages[i]);
        }
    }

    // Upstream: TransactionConsumeTest#testFirstTnxBufferSnapshotAndRecoveryConcurrently
    [Theory]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    public async Task Send_GivenConcurrentFirstTransactionalSendsFromManyClients_ShouldDeliverOnlyCommitted(bool doCommitTxn, int invocation)
    {
        //Arrange
        _testOutputHelper.Log($"Invocation {invocation}");
        const int producerCount = 10;
        var topic = CreateTopicName();
        var clients = new List<IPulsarClient>();
        var producers = new List<IProducer<string>>();

        try
        {
            await using var consumerClient = _fixture.CreateClient(_testOutputHelper.Log);
            await using var consumer = consumerClient.NewConsumer(Schema.String).SubscriptionName("s1").Topic(topic).Create();
            await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

            for (var i = 0; i < producerCount; ++i)
            {
                var client = _fixture.CreateClient(_testOutputHelper.Log);
                clients.Add(client);
                var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
                producers.Add(producer);
            }

            foreach (var producer in producers)
                await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

            var transactions = new List<ITransaction>();
            var sendResults = new ConcurrentBag<Task<MessageId>>();
            var sends = new List<Task>();

            for (var i = 0; i < producerCount; ++i)
                transactions.Add(await clients[i].NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token));

            //Act
            for (var i = 0; i < producerCount; ++i)
            {
                var index = i;
                sends.Add(Task.Run(() => sendResults.Add(producers[index].NewMessage().Transaction(transactions[index]).Send(index.ToString(), _cts.Token).AsTask()), _cts.Token));
            }

            await Task.WhenAll(sends);
            await WaitUntilTransactionBufferIsReady(topic);

            foreach (var sendResult in sendResults)
                _ = await sendResult.WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

            foreach (var transaction in transactions)
            {
                if (doCommitTxn)
                    await transaction.Commit(_cts.Token);
                else
                    await transaction.Abort(_cts.Token);
            }

            //Assert
            var received = new HashSet<string>(StringComparer.Ordinal);
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);

            while (message is not null)
            {
                _ = received.Add(message.Value());
                message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);
            }

            if (doCommitTxn)
            {
                for (var i = 0; i < producerCount; ++i)
                    received.ShouldContain(i.ToString());
            }
            else
                received.ShouldBeEmpty();
        }
        finally
        {
            foreach (var producer in producers)
                await producer.DisposeAsync();

            foreach (var client in clients)
                await client.DisposeAsync();
        }
    }

    // Upstream: TransactionConsumeTest#testAckChunkMessage
    // TODO(WP8-producer): Produce the message with IProducer (Schema.String, EnableChunking(true), 'a' x 6,000,000), and assert that the id that Send returns is the id
    // of a chunked message of which the first chunk is not the last chunk, as upstream does. Until then the perf tool of the broker image splits the message in 2 chunks.
    [Fact]
    public async Task Acknowledge_GivenChunkedMessageAndTransaction_ShouldLeaveNoUnackedMessagesAfterCommit()
    {
        //Arrange
        const string subscription = "testAckChunkMessage";
        var topic = CreateTopicName();
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.ByteArray).InitialPosition(SubscriptionInitialPosition.Earliest).SubscriptionName(subscription).SubscriptionType(SubscriptionType.Shared).Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await _fixture.ProduceChunkedMessages(topic, 1, 6_000 * 1_000, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token);
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(60), _cts.Token);
        message.ShouldNotBeNull();
        message.MessageId.FirstChunkMessageId.ShouldNotBeNull();
        message.MessageId.FirstChunkMessageId.ShouldNotBe(message.MessageId);

        // The perf tool can send a message or two more than it was asked to, and they are not part of the transaction
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token) is { } other)
        {
            await consumer.Acknowledge(other, _cts.Token);
        }

        //Act
        await consumer.Acknowledge(message.MessageId, transaction, _cts.Token);
        await transaction.Commit(_cts.Token);

        //Assert
        (await _fixture.GetUnackedMessages(topic, subscription, _cts.Token)).ShouldBe(0);
    }

    public void Dispose()
        => _cts.Dispose();

    private static string CreateTopicName()
        => $"persistent://{TransactionFixture.PublicTransactionNamespace}/txn-consume-test-{Guid.NewGuid():N}";

    private async Task<string> SendMessage(IProducer<byte[]> producer, string value, ITransaction? transaction = null)
    {
        var metadata = new MessageMetadata { Transaction = transaction };
        _ = await producer.Send(metadata, Encoding.UTF8.GetBytes(value), _cts.Token);
        return value;
    }

    private async Task WaitUntilTransactionBufferIsReady(string topic)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        while ((await _fixture.GetTransactionBufferStats(topic, false, false, _cts.Token)).GetProperty("state").GetString() != "Ready")
            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }
}
