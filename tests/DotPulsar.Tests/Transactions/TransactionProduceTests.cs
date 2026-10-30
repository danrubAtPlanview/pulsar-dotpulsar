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

using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using System.Net;
using System.Text;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionProduceTests : IDisposable
{
    private const int _numberOfPartitions = 3;

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionProduceTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TransactionProduceTest#produceAndCommitTest
    [Fact]
    public async Task Commit_GivenTransactionalMessagesOnPartitionedTopic_ShouldWriteCommitMarkerAndDeliverAllMessages()
        => await ProduceTest(true);

    // Upstream: TransactionProduceTest#produceAndAbortTest
    [Fact]
    public async Task Abort_GivenTransactionalMessagesOnPartitionedTopic_ShouldWriteAbortMarkerAndDeliverNoMessages()
        => await ProduceTest(false);

    // Upstream: TransactionProduceTest#testCommitFailure
    [Fact]
    public async Task Commit_GivenFailedTransactionalSend_ShouldThrowTransactionHasOperationFailedExceptionAndAbort()
    {
        //Arrange
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var transaction = await client.NewTransaction().Create(_cts.Token);
        var topic = TransactionFixture.CreateTopicName("test-commit-failure");
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var send = producer.NewMessage().Transaction(transaction).Send(new byte[1024 * 1024 * 10], _cts.Token).AsTask(); // The message is larger than the maximum message size of the broker

        //Act
        var exception = await Record.ExceptionAsync(async () => await transaction.Commit(_cts.Token));

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
        (await _fixture.GetTransactionMetadataStatusCode(transaction.Id, _cts.Token)).ShouldBe(HttpStatusCode.NotFound);
        var sendException = await Record.ExceptionAsync(async () => await send.WaitAsync(TimeSpan.FromSeconds(10), _cts.Token));
        sendException.ShouldNotBeNull();
        _testOutputHelper.Log($"The send failed with: {sendException}");
    }

    // Upstream: TransactionProduceTest#testAbortedTransactionDoesNotDiscardPlainMessageBatchedWithIt
    [Fact]
    public async Task Abort_GivenPlainMessageBatchedAfterTransactionalMessage_ShouldDeliverPlainMessage()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("txn-batch-isolation");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.String).SubscriptionName("txn-batch-isolation-sub").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(60)).Create(_cts.Token);
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topic)
            .EnableBatching(true)
            .BatchingMaxMessages(100)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5))
            .SendTimeout(TimeSpan.Zero)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        using var warmUpCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        warmUpCts.CancelAfter(TimeSpan.FromSeconds(30));
        _ = await producer.NewMessage().Transaction(transaction).Send("warm-up", warmUpCts.Token);
        var inTransaction = producer.NewMessage().Transaction(transaction).Send("in-txn", _cts.Token).AsTask();
        var plain = producer.Send("plain", _cts.Token).AsTask();

        //Act
        await producer.Flush(_cts.Token);
        await plain.WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        await transaction.Abort(_cts.Token);

        //Assert
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(30), _cts.Token);
        message.ShouldNotBeNull("the plain message was discarded by the aborted transaction");
        message.Value().ShouldBe("plain");
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
        _ = await Record.ExceptionAsync(async () => await inTransaction);
    }

    // Additional coverage
    // The unit test with the same name in ProducerTransactionTests counts the requests to the coordinator. This test asks the broker which partitions the transaction has, and ends the transaction:
    // the messages of a partition that was not registered would stay hidden after the commit.
    [Fact]
    public async Task Send_GivenPartitionedTopic_ShouldRegisterEachPartitionOnce()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("send-partitioned-topic");
        await _fixture.CreatePartitionedTopic(topic, _numberOfPartitions, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.String).SubscriptionName("sub").Topic(topic).InitialPosition(SubscriptionInitialPosition.Earliest).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String).Topic(topic).SendTimeout(TimeSpan.Zero).Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var messages = Enumerable.Range(0, _numberOfPartitions * 2).Select(i => $"message {i}").ToList();

        //Act
        foreach (var message in messages)
            _ = await producer.NewMessage().Transaction(transaction).Send(message, _cts.Token);

        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);
        await transaction.Commit(_cts.Token);
        var received = await TransactionFixture.DrainValues(consumer, TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        var producedPartitions = metadata.GetProperty("producedPartitions").EnumerateObject().Select(partition => partition.Name).ToList();
        producedPartitions.ShouldBe(Enumerable.Range(0, _numberOfPartitions).Select(partition => $"{topic}-partition-{partition}"), true);
        received.ShouldBe(messages, true);
    }

    public void Dispose()
        => _cts.Dispose();

    // Upstream: TransactionProduceTest#produceTest
    private async Task ProduceTest(bool endAction)
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName(endAction ? "produce-commit" : "produce-abort");
        await _fixture.CreatePartitionedTopic(topic, _numberOfPartitions, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);
        transaction.Id.MostSigBits.ShouldNotBe(ulong.MaxValue);
        transaction.Id.LeastSigBits.ShouldNotBe(ulong.MaxValue);
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("observer").Topic(topic).InitialPosition(SubscriptionInitialPosition.Earliest).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).SendTimeout(TimeSpan.Zero).Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var messageSet = new HashSet<string>(StringComparer.Ordinal);
        var sends = new List<Task<MessageId>>();

        for (var i = 0; i < _numberOfPartitions * 3; ++i)
        {
            var message = "Hello Txn - " + i;
            _ = messageSet.Add(message);
            sends.Add(producer.NewMessage().Transaction(transaction).Send(Encoding.UTF8.GetBytes(message), _cts.Token).AsTask());
        }

        foreach (var send in sends)
            (await send.WaitAsync(TimeSpan.FromSeconds(1), _cts.Token)).ShouldNotBeNull();

        // Upstream reads the entries of the managed ledgers, here the number of entries comes from the internal stats of every partition
        for (var partition = 0; partition < _numberOfPartitions; ++partition)
            (await GetNumberOfEntries(topic, partition)).ShouldBe(3);

        //Act
        if (endAction)
            await transaction.Commit(_cts.Token);
        else
            await transaction.Abort(_cts.Token);

        //Assert
        for (var partition = 0; partition < _numberOfPartitions; ++partition)
            (await GetNumberOfEntries(topic, partition)).ShouldBe(4); // The marker

        if (endAction)
        {
            for (var i = 0; i < _numberOfPartitions * 3; ++i)
            {
                var received = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);
                received.ShouldNotBeNull();
                messageSet.Remove(Encoding.UTF8.GetString(received.Value())).ShouldBeTrue();
            }

            messageSet.ShouldBeEmpty();
        }
        else
        {
            // Upstream reads the messages from the entries of the managed ledgers, which is not possible with the client
            (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token)).ShouldBeNull();
        }
    }

    private async Task<long> GetNumberOfEntries(string topic, int partition)
        => (await _fixture.GetInternalStats($"{topic}-partition-{partition}", _cts.Token)).GetProperty("numberOfEntries").GetInt64();
}
