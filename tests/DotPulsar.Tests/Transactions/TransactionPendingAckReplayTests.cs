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

// The broker of the TransactionPendingAckReplayFixture has transactionPendingAckLogIndexMinLag=4002 and managedLedgerDefaultMarkDeleteRateLimit=10
[Collection("TransactionPendingAckReplay"), Trait("Category", "Integration")]
public sealed class TransactionPendingAckReplayTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionPendingAckReplayFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionPendingAckReplayTests(TransactionPendingAckReplayFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: PendingAckPersistentTest#cumulativePendingAckReplayTest
    // The Java test sets transactionPendingAckLogIndexMinLag and managedLedgerDefaultMarkDeleteRateLimit on the running broker, and this broker has them from the start.
    [Fact]
    public async Task AcknowledgeCumulative_GivenPendingAcksAndTopicUnload_ShouldReplayPendingAckState()
    {
        //Arrange
        const int messageCount = 1000;
        const string subscription = "cumulative-test";
        var topic = TransactionFixture.CreateTopicName("pending-ack-replay");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(true)
            .BatchingMaxMessages(200)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Failover)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var abortTransaction = await NewTransaction(client);

        var pendingAckMessageIds = new List<MessageId>();
        for (var i = 0; i < messageCount; i++)
        {
            _ = await producer.Send(Encoding.UTF8.GetBytes("Hello Pulsar!"), _cts.Token);
        }

        for (var i = 0; i < messageCount; i++)
        {
            var message = await consumer.Receive(_cts.Token);
            pendingAckMessageIds.Add(message.MessageId);
            await consumer.AcknowledgeCumulative(message.MessageId, abortTransaction, _cts.Token);
        }

        await _fixture.UnloadTopic(topic, consumer, _cts.Token);
        var transaction = await NewTransaction(client);

        for (var i = 0; i < pendingAckMessageIds.Count; i++)
        {
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.AcknowledgeCumulative(pendingAckMessageIds[i], transaction, _cts.Token));
        }

        var commitTransaction = await NewTransaction(client);
        await abortTransaction.Abort(_cts.Token);

        for (var i = 0; i < pendingAckMessageIds.Count; i++)
        {
            await consumer.AcknowledgeCumulative(pendingAckMessageIds[i], commitTransaction, _cts.Token);
        }

        await commitTransaction.Commit(_cts.Token);

        await _fixture.UnloadTopic(topic, consumer, _cts.Token);

        for (var i = 0; i < pendingAckMessageIds.Count; i++)
        {
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.AcknowledgeCumulative(pendingAckMessageIds[i], transaction, _cts.Token));
        }

        //Act
        // in order to check out the pending ack cursor is clear whether or not.
        await Eventually(async () =>
        {
            var (markDeletePosition, lastConfirmedEntry) = await _fixture.GetPendingAckLogPositions(topic, subscription, _cts.Token);

            //Assert
            markDeletePosition.CompareTo(lastConfirmedEntry).ShouldBe(0);
        }, TimeSpan.FromSeconds(10));
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task<ITransaction> NewTransaction(IPulsarClient client)
        => await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create(_cts.Token);

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
