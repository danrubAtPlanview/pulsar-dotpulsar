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
public sealed class TopicTransactionBufferRecoverTests : IDisposable
{
    private const string RecoverCommit = "recover-commit";
    private const string RecoverAbort = "recover-abort";
    private const string SubscriptionName = "test-recover";

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TopicTransactionBufferRecoverTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TopicTransactionBufferRecoverTest#recoverTest
    // Java waits until the transaction buffer of the topic reports that it is ready after the unload, which it asks the broker object for, and here the admin API tells the state.
    [Theory]
    [InlineData(RecoverAbort)]
    [InlineData(RecoverCommit)]
    public async Task Receive_GivenTopicUnloadedWithOpenTransactions_ShouldRecoverTransactionBuffer(string testTopic)
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName(testTopic);
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var transaction1 = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(_cts.Token);
        var transaction2 = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(_cts.Token);
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .SubscriptionName(SubscriptionName)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topic)
            .SendTimeout(TimeSpan.Zero)
            .EnableBatching(false)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        const int messageCount = 10;
        const string content = "Hello Txn - ";

        for (var i = 0; i < messageCount; i++)
        {
            var message = content + i;
            var transaction = i % 2 == 0 ? transaction1 : transaction2;
            _ = await producer.NewMessage().Transaction(transaction).Send(message, _cts.Token);
        }

        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldBeNull();

        await transaction1.Commit(_cts.Token);

        // only can receive message 1
        var received = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token);
        received.ShouldNotBeNull();
        await consumer.Acknowledge(received, _cts.Token);

        // can't receive message
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldBeNull();

        //Act
        await _fixture.UnloadTopic(topic, _cts.Token);
        await WaitUntilTransactionBufferIsReady(topic);

        //Assert
        if (testTopic == RecoverCommit)
        {
            await transaction2.Commit(_cts.Token);

            for (var i = messageCount; i > 1; i--)
            {
                received = await consumer.Receive(_cts.Token);
                await consumer.Acknowledge(received, _cts.Token);
            }
        }
        else
        {
            await transaction2.Abort(_cts.Token);

            for (var i = messageCount / 2; i > 1; i--)
            {
                received = await consumer.Receive(_cts.Token);
                await consumer.Acknowledge(received, _cts.Token);
            }
        }

        // can't receive message
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldBeNull();
    }

    public void Dispose()
        => _cts.Dispose();

    // The broker does not know the topic until a producer or a consumer connects to it again after the unload
    private async Task WaitUntilTransactionBufferIsReady(string topic)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        while ((await _fixture.TryGetTransactionBufferStats(topic, false, false, cts.Token))?.GetProperty("state").GetString() != "Ready")
            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }
}
