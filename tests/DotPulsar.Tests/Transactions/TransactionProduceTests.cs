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
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).Create(); // TODO(WP6): .EnableBatching(true), as upstream relies on the default of the Java client
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
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).Create(); // TODO(WP7): .SendTimeout(TimeSpan.Zero), which is what the producer does without a send timeout
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
