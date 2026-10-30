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

using DotPulsar.Extensions;

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TopicTransactionBufferTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TopicTransactionBufferTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TopicTransactionBufferTest#testWriteSnapshotWhenFirstTxnMessageSend
    [Fact]
    public async Task Send_GivenFirstTransactionalMessage_ShouldMoveTransactionBufferFromNoSnapshotToReady()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testWriteSnapshotWhenFirstTxnMessageSend");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String).Topic(topic).Create(); // TODO(WP6): .EnableBatching(true), as upstream relies on the default of the Java client
        await using var consumer = client.NewConsumer(Schema.String).SubscriptionName("my-sub").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await WaitUntilState(topic, "NoSnapshot");

        //Act
        _ = await producer.Send("normal message", _cts.Token);

        //Assert
        (await GetState(topic)).ShouldBe("NoSnapshot");

        //Arrange
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token);

        //Act
        _ = await producer.NewMessage().Transaction(transaction).Send("transaction message", _cts.Token);

        //Assert
        (await GetState(topic)).ShouldBe("Ready");

        //Act
        using (var abortCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token))
        {
            abortCts.CancelAfter(TimeSpan.FromSeconds(5));
            await transaction.Abort(abortCts.Token);
        }

        _ = await producer.Send("normal message", _cts.Token);
        await _fixture.UnloadTopic(topic, _cts.Token);

        //Assert
        await WaitUntilState(topic, "Ready");
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);

        for (var i = 0; i < 2; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);
            message.ShouldNotBeNull();
            message.Value().ShouldBe("normal message");
        }

        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token)).ShouldBeNull();
    }

    // Upstream: TopicTransactionBufferTest#testMessagePublishInOrder
    [Fact]
    public async Task Commit_GivenTransactionalMessagesSentWhileBufferBecomesReady_ShouldDeliverInSendOrder()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testMessagePublishInOrder");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.Int32).Topic(topic).Create(); // TODO(WP6): .EnableBatching(true), as upstream relies on the default of the Java client
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        await using var consumer = client.NewConsumer(Schema.Int32).SubscriptionName("sub").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token);
        var sends = new List<Task<MessageId>>();

        // The first transactional message of a topic makes the transaction buffer take its first snapshot, so the messages sent while it does are held back until it is done
        for (var i = 0; i < 50; ++i)
            sends.Add(producer.NewMessage().Transaction(transaction).Send(i, _cts.Token).AsTask());

        for (var i = 50; i < 100; ++i)
            sends.Add(producer.NewMessage().Transaction(transaction).Send(i, _cts.Token).AsTask());

        //Act
        await transaction.Commit(_cts.Token);

        //Assert
        for (var i = 0; i < 100; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);
            message.ShouldNotBeNull();
            message.Value().ShouldBe(i);
        }

        await Task.WhenAll(sends);
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task<string?> GetState(string topic)
        => (await _fixture.TryGetTransactionBufferStats(topic, false, false, _cts.Token))?.GetProperty("state").GetString();

    private async Task WaitUntilState(string topic, string state)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        while (await GetState(topic) != state)
            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }
}
