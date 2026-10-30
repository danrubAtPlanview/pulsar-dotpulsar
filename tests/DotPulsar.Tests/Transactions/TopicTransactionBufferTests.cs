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
        await using var producer = client.NewProducer(Schema.String).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
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
        await using var producer = client.NewProducer(Schema.Int32).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
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

    // Upstream: TopicTransactionBufferTest#testGetLastMessageIdsWithOngoingTransactions
    [Fact]
    public async Task GetLastMessageIds_GivenOngoingTransactions_ShouldNotAdvancePastOpenTransactions()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testGetLastMessageIdsWithOngoingTransactions");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("my-subscription").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Case1: send 3 original messages. |1:0|1:1|1:2|
        MessageId expectedLastMessageId = null!;
        for (var i = 0; i < 3; i++)
            expectedLastMessageId = await producer.Send([], _cts.Token);

        //Act
        var lastWithOnlyNormalMessages = await GetLastMessageId(consumer);

        //Assert
        lastWithOnlyNormalMessages.ShouldBe((expectedLastMessageId.LedgerId, expectedLastMessageId.EntryId));

        // Case2: send 2 ongoing transactional messages and 2 original messages.
        // |1:0|1:1|1:2|txn1:start->1:3|1:4|txn2:start->1:5.
        //Arrange
        var transaction1 = await client.NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token);
        var transaction2 = await client.NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token);

        // |1:0|1:1|1:2|txn1:1:3|
        _ = await producer.NewMessage().Transaction(transaction1).Send([], _cts.Token);

        // |1:0|1:1|1:2|txn1:1:3|1:4|
        var expectedLastMessageId1 = await producer.Send([], _cts.Token);

        // |1:0|1:1|1:2|txn1:1:3|1:4|txn2:1:5|
        _ = await producer.NewMessage().Transaction(transaction2).Send([], _cts.Token);

        //Act
        // Last message ID will not change when txn1 and txn2 do not end.
        var lastWithOpenTransactions = await GetLastMessageId(consumer);

        //Assert
        lastWithOpenTransactions.ShouldBe((expectedLastMessageId.LedgerId, expectedLastMessageId.EntryId));

        //Act
        // Last message ID will update to 1:4 when txn1 committed.
        // |1:0|1:1|1:2|txn1:1:3|1:4|txn2:1:5|tx1:commit->1:6|
        await Commit(transaction1);
        var lastAfterCommit = await GetLastMessageId(consumer);

        //Assert
        lastAfterCommit.ShouldBe((expectedLastMessageId1.LedgerId, expectedLastMessageId1.EntryId));

        //Act
        // Last message ID will still to 1:4 when txn2 aborted.
        // |1:0|1:1|1:2|txn1:1:3|1:4|txn2:1:5|tx1:commit->1:6|tx2:abort->1:7|
        await Abort(transaction2);
        var lastAfterAbort = await GetLastMessageId(consumer);

        //Assert
        lastAfterAbort.ShouldBe((expectedLastMessageId1.LedgerId, expectedLastMessageId1.EntryId));

        // Handle the case of the maxReadPosition < lastPosition, but it's an aborted transactional message.
        //Arrange
        var transaction3 = await client.NewTransaction().Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction3).Send([], _cts.Token);

        //Act
        var lastWithOpenTransaction3 = await GetLastMessageId(consumer);
        await Abort(transaction3);
        var lastAfterAbort3 = await GetLastMessageId(consumer);

        //Assert
        lastWithOpenTransaction3.ShouldBe((expectedLastMessageId1.LedgerId, expectedLastMessageId1.EntryId));
        lastAfterAbort3.ShouldBe((expectedLastMessageId1.LedgerId, expectedLastMessageId1.EntryId));
    }

    // Upstream: TopicTransactionBufferTest#testGetLastMessageIdsWithOpenTransactionAtLedgerHead
    // Upstream triggers the ledger switch with an unload of the topic and a wait until the managed ledger of the broker has opened its new ledger, which the internal stats of the topic tell here.
    [Fact]
    public async Task HasMessageAvailable_GivenOpenTransactionAtLedgerHead_ShouldReturnTrue()
    {
        //Arrange
        var topic = TransactionFixture.CreateTopicName("testGetLastMessageIdsWithOpenTransactionAtLedgerHead");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await using var consumer = client.NewConsumer(Schema.ByteArray).SubscriptionName("my-subscription").Topic(topic).Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        MessageId lastMessageId = null!;
        for (var i = 0; i < 3; i++)
            lastMessageId = await producer.Send(Encoding.UTF8.GetBytes(i.ToString()), _cts.Token);

        await _fixture.UnloadTopic(topic, _cts.Token);
        await _fixture.WaitForManagedLedgerState(topic, "LedgerOpened", _cts.Token);

        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromHours(5)).Create(_cts.Token);
        _ = await producer.NewMessage().Transaction(transaction).Send([], _cts.Token);
        await using var reader = client.NewReader(Schema.ByteArray).Topic(topic).StartMessageId(MessageId.Earliest).Create();
        await reader.StateChangedTo(ReaderState.Connected, _cts.Token);

        //Act
        var lastConfirmedEntry = (await _fixture.GetInternalStats(topic, _cts.Token)).GetProperty("lastConfirmedEntry").GetString();
        var hasMessageAvailable = await TransactionFixture.HasMessageAvailable(reader, MessageId.Earliest, null, _cts.Token);

        //Assert
        lastConfirmedEntry.ShouldNotStartWith($"{lastMessageId.LedgerId}:", Case.Sensitive, "the messages and the open transaction are in different ledgers");
        lastConfirmedEntry.ShouldEndWith(":0", Case.Sensitive, "the message of the open transaction is the first entry of its ledger");
        hasMessageAvailable.ShouldBeTrue();
    }

    public void Dispose()
        => _cts.Dispose();

    private async Task<(ulong LedgerId, ulong EntryId)> GetLastMessageId(IConsumer consumer)
    {
        var messageId = (await consumer.GetLastMessageIds(_cts.Token)).First();
        return (messageId.LedgerId, messageId.EntryId);
    }

    private async Task Commit(ITransaction transaction)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        await transaction.Commit(cts.Token);
    }

    private async Task Abort(ITransaction transaction)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        await transaction.Abort(cts.Token);
    }

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
