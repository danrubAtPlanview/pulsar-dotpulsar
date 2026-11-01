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
public sealed class TopicTransactionBufferPublishOrderTests : IDisposable
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TopicTransactionBufferPublishOrderTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TopicTransactionBufferPublishOrderTest#testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Send_GivenPlainMessageAfterTransactionalMessage_ShouldStoreInSendOrder(bool recovering)
    {
        // Without the fix the broker stores the plain message before the transactional message, while the transaction buffer takes its first snapshot (the image of the tests has the fix only if BrokerImage says so)
        if (!recovering)
            BrokerImage.SkipUnlessItHasFix(BrokerImage.PlainPublishBehindTransactionalPublish, "the plain message is kept behind the transactional messages");

        //Arrange
        var ns = $"{TransactionFixture.Tenant}/testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt-{Guid.NewGuid():N}";
        await _fixture.CreateNamespace(ns, 1, _cts.Token);
        await _fixture.SetNamespaceDeduplication(ns, false, _cts.Token);
        var topic = $"persistent://{ns}/test";
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(false).Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        if (recovering)
        {
            // A message in a committed transaction makes the transaction buffer of the topic write a snapshot, which it recovers from when the topic is loaded again
            var throwawayTransaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
            _ = await producer.NewMessage().Transaction(throwawayTransaction).Send("throwaway", _cts.Token);
            await throwawayTransaction.Commit(_cts.Token);
            await _fixture.UnloadTopic(topic, _cts.Token);
        }

        // The subscription is created after the throwaway message, so it does not receive it
        await using var consumer = client.NewConsumer(Schema.String).SubscriptionName("sub").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);

        //Act
        var transactionalSend = producer.NewMessage().Transaction(transaction).Send("txn", _cts.Token).AsTask();
        var plainSend = producer.Send("plain", _cts.Token).AsTask();
        var transactionalId = await transactionalSend.WaitAsync(_timeout, _cts.Token);
        var plainId = await plainSend.WaitAsync(_timeout, _cts.Token);
        await transaction.Commit(_cts.Token);

        //Assert
        (transactionalId < plainId).ShouldBeTrue($"The transactional message ({transactionalId}) must be stored before the plain message ({plainId})");
        var received = new List<string>();
        var message = await TransactionFixture.ReceiveOrNull(consumer, _timeout, _cts.Token);

        while (message is not null)
        {
            received.Add(message.Value());
            message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token);
        }

        received.ShouldBe(["txn", "plain"]);
    }

    public void Dispose()
        => _cts.Dispose();
}
