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
using System.Text;

// The broker of the TransactionPendingAckLogIndexFixture has transactionPendingAckLogIndexMinLag=1 and managedLedgerDefaultMarkDeleteRateLimit=5
[Collection("TransactionPendingAckLogIndex"), Trait("Category", "Integration")]
public sealed class TransactionPendingAckLogIndexTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionPendingAckLogIndexFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public TransactionPendingAckLogIndexTests(TransactionPendingAckLogIndexFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: TransactionTest#testPendingAckMarkDeletePosition
    // The Java test sets transactionPendingAckLogIndexMinLag and managedLedgerDefaultMarkDeleteRateLimit on the running broker, and this broker has them from the start.
    [Fact]
    public async Task Acknowledge_GivenCommittedTransactionalAck_ShouldMarkDeletePendingAckLogUpToUncommittedEntries()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testPendingAckMarkDeletePosition");
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(true) // Upstream relies on the default of the Java client, which is batching
            .SendTimeout(TimeSpan.Zero)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName("sub")
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //pendingAck add message1 and commit mark, metadata add message1
        //PersistentMarkDeletedPosition have not updated
        _ = await producer.Send(Encoding.UTF8.GetBytes("test"), _cts.Token);
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        var message1 = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        message1.ShouldNotBeNull();
        var ack1 = consumer.Acknowledge(message1.MessageId, transaction, _cts.Token).AsTask();
        await transaction.Commit(_cts.Token);

        //PersistentMarkDeletedPosition of subscription have updated to message1,
        //check whether delete markDeletedPosition of pendingAck after append entry to pendingAck
        transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(5)).Create(_cts.Token);
        _ = await producer.Send(Encoding.UTF8.GetBytes("test"), _cts.Token);
        var message2 = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        message2.ShouldNotBeNull();

        //Act
        var ack2 = consumer.Acknowledge(message2.MessageId, transaction, _cts.Token).AsTask();

        //Assert
        await Eventually(async () =>
        {
            var (markDeletePosition, lastConfirmedEntry) = await _fixture.GetPendingAckLogPositions(topic, "sub", _cts.Token);
            markDeletePosition.LedgerId.ShouldBe(lastConfirmedEntry.LedgerId);
            //don`t contain commit mark and unCommitted message2
            markDeletePosition.EntryId.ShouldBe(lastConfirmedEntry.EntryId - 2);
        }, TimeSpan.FromSeconds(10));

        await ack1;
        await ack2;
    }

    public void Dispose()
        => _cts.Dispose();

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
