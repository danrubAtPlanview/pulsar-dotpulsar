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

[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionMarkerDeleteTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionMarkerDeleteTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TransactionMarkerDeleteTest#testMarkerDelete
    [Fact]
    public async Task Acknowledge_GivenTransactionMarkers_ShouldMoveMarkDeletePositionPastMarkers()
    {
        //Arrange
        const string subscription = "testMarkerDelete";
        var topic = TransactionFixture.CreateTopicName("testMarkerDelete");
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription)
            .AckReceiptEnabled(true)
            .SubscriptionType(SubscriptionType.Shared)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(true) // Upstream relies on the default of the Java client, which is batching
            .SendTimeout(TimeSpan.Zero)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        var transaction1 = await NewTransaction(client);
        var transaction2 = await NewTransaction(client);
        var transaction3 = await NewTransaction(client);
        var transaction4 = await NewTransaction(client);

        var messageId1 = await producer.NewMessage().Transaction(transaction1).Send([], _cts.Token);
        var messageId2 = await producer.NewMessage().Transaction(transaction2).Send([], _cts.Token);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();

        //Act
        await transaction1.Commit(_cts.Token);
        await consumer.Acknowledge(await consumer.Receive(_cts.Token), _cts.Token);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();

        //Assert
        // maxReadPosition move to msgId1, msgId2 have not be committed
        (await GetMarkDeletePosition(topic, subscription)).ShouldBe($"{messageId1.LedgerId}:{messageId1.EntryId}");

        //Act
        var messageId3 = await producer.NewMessage().Transaction(transaction3).Send([], _cts.Token);
        await transaction2.Commit(_cts.Token);
        await consumer.Acknowledge(await consumer.Receive(_cts.Token), _cts.Token);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();

        //Assert
        // maxReadPosition move to txn1 marker, so entryId is msgId2.getEntryId() + 1,
        // because send msgId2 before commit txn1
        (await GetMarkDeletePosition(topic, subscription)).ShouldBe($"{messageId2.LedgerId}:{messageId2.EntryId + 1}");

        //Act
        var messageId4 = await producer.NewMessage().Transaction(transaction4).Send([], _cts.Token);
        await transaction3.Commit(_cts.Token);
        await consumer.Acknowledge(await consumer.Receive(_cts.Token), _cts.Token);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();

        //Assert
        // maxReadPosition move to txn2 marker, because msgId4 have not be committed
        (await GetMarkDeletePosition(topic, subscription)).ShouldBe($"{messageId3.LedgerId}:{messageId3.EntryId + 1}");

        //Act
        await transaction4.Abort(_cts.Token);

        //Assert
        // maxReadPosition move to txn4 abort marker, so entryId is msgId4.getEntryId() + 2
        await Eventually(async () => (await GetMarkDeletePosition(topic, subscription)).ShouldBe($"{messageId4.LedgerId}:{messageId4.EntryId + 2}"), TimeSpan.FromSeconds(10));
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task<ITransaction> NewTransaction(IPulsarClient client)
        => await client.NewTransaction().Timeout(TimeSpan.FromSeconds(10)).Create(_cts.Token);

    private async Task<string> GetMarkDeletePosition(string topic, string subscription)
    {
        var stats = await _fixture.GetInternalStats(topic, _cts.Token);
        return stats.GetProperty("cursors").GetProperty(subscription).GetProperty("markDeletePosition").GetString()!;
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
}
