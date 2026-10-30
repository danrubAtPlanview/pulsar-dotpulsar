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
public sealed class TransactionStablePositionTests : IDisposable
{
    private static readonly TimeSpan _waitTime = TimeSpan.FromSeconds(2);

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionStablePositionTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TransactionStablePositionTest#commitTxnTest
    [Fact]
    public async Task Commit_GivenTransactionalMessageBetweenPlainMessages_ShouldBlockReadsUntilCommit()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("test-topic");
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).Create(); // TODO(WP6): .EnableBatching(false), which is what the producer does by default, and TODO(WP7): .SendTimeout(TimeSpan.Zero), which is what the producer does without a send timeout
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("test").SubscriptionType(SubscriptionType.Failover).InitialPosition(SubscriptionInitialPosition.Earliest).Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);

        await SendThreeMessages(producer, transaction);

        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token);
        message.ShouldNotBeNull();
        Encoding.UTF8.GetString(message.Value()).ShouldBe("test1");
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token)).ShouldBeNull();

        //Act
        await transaction.Commit(_cts.Token);

        //Assert
        await ReceiveAndAssert(consumer, "test2");
        await ReceiveAndAssert(consumer, "test3");
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token)).ShouldBeNull();
    }

    // Upstream: TransactionStablePositionTest#abortTxnTest
    [Fact]
    public async Task Abort_GivenTransactionalMessageBetweenPlainMessages_ShouldBlockReadsUntilAbortAndDropIt()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("test-topic");
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).Create(); // TODO(WP6): .EnableBatching(false), which is what the producer does by default, and TODO(WP7): .SendTimeout(TimeSpan.Zero), which is what the producer does without a send timeout
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("test").SubscriptionType(SubscriptionType.Failover).InitialPosition(SubscriptionInitialPosition.Earliest).Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(5)).Create(_cts.Token);

        await SendThreeMessages(producer, transaction);

        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token);
        message.ShouldNotBeNull();
        Encoding.UTF8.GetString(message.Value()).ShouldBe("test1");
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token)).ShouldBeNull();

        //Act
        await transaction.Abort(_cts.Token);

        //Assert
        await ReceiveAndAssert(consumer, "test3");
        (await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token)).ShouldBeNull();
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task SendThreeMessages(IProducer<byte[]> producer, ITransaction transaction)
    {
        _ = await producer.Send(Encoding.UTF8.GetBytes("test1"), _cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Send(Encoding.UTF8.GetBytes("test2"), _cts.Token);
        _ = await producer.Send(Encoding.UTF8.GetBytes("test3"), _cts.Token);
    }

    private async Task ReceiveAndAssert(IConsumer<byte[]> consumer, string expected)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, _waitTime, _cts.Token);
        message.ShouldNotBeNull();
        Encoding.UTF8.GetString(message.Value()).ShouldBe(expected);
    }
}
