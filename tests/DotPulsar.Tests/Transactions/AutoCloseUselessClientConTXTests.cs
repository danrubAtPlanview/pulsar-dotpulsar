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
using System.Text;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class AutoCloseUselessClientConTXTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public AutoCloseUselessClientConTXTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: AutoCloseUselessClientConTXTest#testConnectionAutoReleaseUnPartitionedTopicWithTransaction
    // The Java test has five brokers, makes the client connect to all of them and releases the connections that are not used, which the pool of the client reports
    // (at least five connections before and at most three after). DotPulsar has one broker here and does not report its connections, so these assertions are dropped.
    // DotPulsar closes a connection that has no producers and consumers when it has been inactive for the CloseInactiveConnectionsInterval, and the connection of the
    // transaction coordinator has none. So the producer and the consumer are disposed while the connection is inactive, and the transactions must still work afterwards.
    [Fact]
    public async Task Transactions_GivenInactiveConnectionsClosed_ShouldKeepWorking()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = _fixture.CreateConfiguredClient(builder => builder.CloseInactiveConnectionsInterval(TimeSpan.FromSeconds(1)), _testOutputHelper.Log);
        var consumer = CreateConsumer(client, topic);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var producer = CreateProducer(client, topic);
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        // Ensure transaction works
        await EnsureTransactionWorks(client, producer, consumer);

        //Act
        // The connection has nothing but the transaction coordinator to keep it, and then it is inactive for longer than the interval
        await consumer.DisposeAsync();
        await producer.DisposeAsync();
        await Task.Delay(TimeSpan.FromSeconds(3), _cts.Token);
        consumer = CreateConsumer(client, topic);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        producer = CreateProducer(client, topic);
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        //Assert
        // Ensure transaction works
        await EnsureTransactionWorks(client, producer, consumer);

        await consumer.DisposeAsync();
        await producer.DisposeAsync();
    }

    public void Dispose()
        => _cts.Dispose();

    private IConsumer<byte[]> CreateConsumer(IPulsarClient client, string topic)
        => client.NewConsumer(Schema.ByteArray)
        .Topic(topic)
        .SubscriptionName("my-subscription-x")
        .StateChangedHandler(_testOutputHelper.Log)
        .Create();

    private IProducer<byte[]> CreateProducer(IPulsarClient client, string topic)
        => client.NewProducer(Schema.ByteArray)
        .Topic(topic)
        .EnableBatching(true) // Upstream relies on the default of the Java client, which is batching
        .SendTimeout(TimeSpan.Zero)
        .StateChangedHandler(_testOutputHelper.Log)
        .Create();

    // Java AutoCloseUselessClientConSupports#ensureTransactionWorks
    private async Task EnsureTransactionWorks(IPulsarClient client, IProducer<byte[]> producer, IConsumer<byte[]> consumer)
    {
        var messageContentBefore = Guid.NewGuid().ToString();
        var messageContentTransaction = Guid.NewGuid().ToString();

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        // assert producer/consumer work OK
        _ = await producer.Send(Encoding.UTF8.GetBytes(messageContentBefore), _cts.Token);
        var message = await consumer.Receive(_cts.Token);
        Encoding.UTF8.GetString(message.Value()).ShouldBe(messageContentBefore);
        _ = await producer.NewMessage().Transaction(transaction).Send(Encoding.UTF8.GetBytes(messageContentTransaction), _cts.Token);
        var ack = consumer.Acknowledge(message.MessageId, transaction, _cts.Token).AsTask();
        await transaction.Commit(_cts.Token);
        await ack;
        var transactionMessage = await consumer.Receive(_cts.Token);
        Encoding.UTF8.GetString(transactionMessage.Value()).ShouldBe(messageContentTransaction);
        await consumer.Acknowledge(transactionMessage.MessageId, _cts.Token);
    }
}
