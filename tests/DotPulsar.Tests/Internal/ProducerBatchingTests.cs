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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Transactions;
using System.Collections.Concurrent;

[Trait("Category", "Unit")]
public sealed class ProducerBatchingTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Send_GivenBatchingEnabledAndTenMessages_ShouldReturnMessageIdsWithBatchIndexes()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection, maxMessages: 10);
        var messageIds = new ConcurrentDictionary<int, MessageId>();

        //Act
        for (var i = 0; i < 10; ++i)
        {
            var index = i;
            await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [(byte) i], messageId =>
            {
                messageIds[index] = messageId;
                return ValueTask.CompletedTask;
            }, Current.CancellationToken);
        }

        await WaitFor.Until(() => messageIds.Count == 10, "all callbacks are called");

        //Assert
        connection.Sent.ShouldHaveSingleItem().NumMessagesInBatch.ShouldBe(10);
        Enumerable.Range(0, 10).Select(i => messageIds[i].BatchIndex).ShouldBe(Enumerable.Range(0, 10));
        messageIds.Values.ShouldAllBe(messageId => messageId.EntryId == messageIds[0].EntryId && messageId.LedgerId == messageIds[0].LedgerId);
    }

    [Fact]
    public async Task Send_GivenBatchingEnabledAndOneMessage_ShouldReturnMessageIdWithoutBatchIndex()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = CreateProducer(connection, publishDelay: TimeSpan.FromMilliseconds(50));
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Act
        var messageId = await producer.Send(new DotPulsar.MessageMetadata(), [1], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.ShouldHaveSingleItem();
        sent.NumMessagesInBatch.ShouldBe(1);
        sent.NumMessages.ShouldBe(1);
        messageId.BatchIndex.ShouldBe(-1);
    }

    [Fact]
    public async Task Send_GivenBatchingDisabled_ShouldSendEveryMessageOnItsOwn()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = CreateProducer(connection, enableBatching: false);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Act
        await producer.Send(new DotPulsar.MessageMetadata(), [1], Current.CancellationToken);
        await producer.Send(new DotPulsar.MessageMetadata(), [2], Current.CancellationToken);

        //Assert
        connection.Sent.Count.ShouldBe(2);
        connection.Sent.ShouldAllBe(sent => sent.NumMessages == 1 && sent.NumMessagesInBatch == 1);
    }

    [Fact]
    public async Task SendChannel_GivenBatchingEnabled_ShouldCompleteAllCallbacks()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection);
        var messageIds = new List<MessageId>();

        //Act
        for (var i = 0; i < 7; ++i)
        {
            await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [(byte) i], messageId =>
            {
                lock (messageIds)
                    messageIds.Add(messageId);
                return ValueTask.CompletedTask;
            }, Current.CancellationToken);
        }

        await producer.SendChannel.Completion(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        await WaitFor.Until(() => { lock (messageIds) return messageIds.Count == 7; }, "all callbacks are called");

        //Assert
        connection.Sent.ShouldHaveSingleItem().NumMessagesInBatch.ShouldBe(7);
        lock (messageIds)
            messageIds.Select(messageId => messageId.BatchIndex).Order().ShouldBe(Enumerable.Range(0, 7));
    }

    [Fact]
    public async Task SendChannelCompletion_GivenLongPublishDelay_ShouldNotWaitForTheDelay()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [1], null, Current.CancellationToken);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [2], null, Current.CancellationToken);

        //Act
        await producer.SendChannel.Completion(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        connection.Sent.ShouldHaveSingleItem().NumMessagesInBatch.ShouldBe(2);
    }

    [Fact]
    public async Task Flush_GivenLongPublishDelay_ShouldSendImmediatelyAndAwaitLastMessage()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection);
        var sends = Enumerable.Range(0, 5).Select(i => producer.Send(new DotPulsar.MessageMetadata(), [(byte) i], Current.CancellationToken).AsTask()).ToArray();

        //Act
        await producer.Flush(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        sends.ShouldAllBe(send => send.IsCompletedSuccessfully);
        connection.Sent.ShouldHaveSingleItem().NumMessagesInBatch.ShouldBe(5);
    }

    [Fact]
    public async Task Flush_GivenManyRoundsOfSendsAndFlushes_ShouldNeverWaitForThePublishDelay()
    {
        //Arrange
        // The request to flush is made right after the dispatcher has seen that the queue is empty, and a message was enqueued after that. The request must not be lost.
        const int rounds = 3000;
        const int messagesPerRound = 150;
        var connection = new FakeProducerConnection();
        await using var producer = CreateProducer(connection, maxMessages: 1000);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Act
        for (var round = 0; round < rounds; ++round)
        {
            for (var i = 0; i < messagesPerRound; ++i)
                await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [(byte) i], null, Current.CancellationToken);

            await producer.Flush(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        }

        //Assert
        connection.Sent.Sum(sent => sent.NumMessagesInBatch).ShouldBe(rounds * messagesPerRound);
    }

    [Fact]
    public async Task Flush_GivenMessagesOnAPartitionedTopic_ShouldFlushEveryPartition()
    {
        //Arrange
        var connection = new FakeProducerConnection(3);
        await using var producer = CreateProducer(connection);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        var sends = Enumerable.Range(0, 6).Select(i => producer.Send(new DotPulsar.MessageMetadata(), [(byte) i], Current.CancellationToken).AsTask()).ToArray();

        //Act
        await producer.Flush(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        sends.ShouldAllBe(send => send.IsCompletedSuccessfully);
        connection.Sent.Count.ShouldBe(3);
        connection.Sent.ShouldAllBe(sent => sent.NumMessagesInBatch == 2);
    }

    [Fact]
    public async Task Flush_GivenNoMessages_ShouldComplete()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        var flush = producer.Flush(Current.CancellationToken);

        //Assert
        flush.IsCompletedSuccessfully.ShouldBeTrue();
        connection.Sent.ShouldBeEmpty();
    }

    [Fact]
    public async Task Flush_GivenBatchingDisabled_ShouldAwaitLastMessage()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        await using var producer = CreateProducer(connection, enableBatching: false);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [1], null, Current.CancellationToken);
        await WaitFor.Until(() => connection.Sent.Count == 1, "the message is written");

        //Act
        var flush = producer.Flush(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var isCompletedBeforeReceipt = flush.IsCompleted;
        connection.ReleaseResponses();
        await flush.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isCompletedBeforeReceipt.ShouldBeFalse();
    }

    [Fact]
    public async Task Flush_GivenLastMessageFails_ShouldThrowItsException()
    {
        //Arrange
        var connection = new FakeProducerConnection { Respond = message => FakeProducerConnection.CreateSendError(message, ServerError.NotAllowedError, "Not allowed") };
        await using var producer = await CreateConnectedProducer(connection);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [1], null, Current.CancellationToken);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [2], null, Current.CancellationToken);

        //Act
        var act = () => producer.Flush(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<NotAllowedException>(act);
    }

    [Fact]
    public async Task Flush_GivenCancellation_ShouldThrowOperationCanceledException()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        await using var producer = await CreateConnectedProducer(connection);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [1], null, Current.CancellationToken);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        var flush = producer.Flush(cts.Token).AsTask();
        await WaitFor.Until(() => connection.Sent.Count == 1, "the message is written");

        //Act
        await cts.CancelAsync();

        //Assert
        var exception = await Record.ExceptionAsync(() => flush.WaitAsync(_timeout, Current.CancellationToken));
        exception.ShouldBeAssignableTo<OperationCanceledException>();
    }

    [Fact]
    public async Task Flush_GivenDisposedProducer_ShouldThrowProducerDisposedException()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var producer = await CreateConnectedProducer(connection);
        await producer.DisposeAsync();

        //Act
        var act = () => producer.Flush(Current.CancellationToken).AsTask();

        //Assert
        await Should.ThrowAsync<ProducerDisposedException>(act);
    }

    [Fact]
    public async Task Send_GivenTransactionalSendsInOneBatch_ShouldMakeCommitWaitForEveryMessageOfTheBatch()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection, maxMessages: 3);
        for (var i = 0; i < 3; ++i)
            await producer.SendChannel.Send(new DotPulsar.MessageMetadata { Transaction = transaction }, [(byte) i], null, Current.CancellationToken);

        //Act
        var commit = transaction.Commit(Current.CancellationToken).AsTask();
        await WaitFor.Until(() => connection.Sent.Count == 1, "the batch is written");
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var isCompletedBeforeReceipt = commit.IsCompleted;
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        connection.ReleaseResponses();
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isCompletedBeforeReceipt.ShouldBeFalse();
        var sent = connection.Sent.ShouldHaveSingleItem();
        sent.NumMessagesInBatch.ShouldBe(3);
        sent.CommandIsTransactional.ShouldBeTrue();
        sent.MetadataIsTransactional.ShouldBeTrue();
        await tcClient.Received(1).Commit(transaction.Id, CancellationToken.None);
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    [Fact]
    public async Task Commit_GivenBatchOfTransactionalMessagesThatFailed_ShouldAbortTheTransaction()
    {
        //Arrange
        var connection = new FakeProducerConnection { Respond = message => FakeProducerConnection.CreateSendError(message, ServerError.NotAllowedError, "Transaction has been ended") };
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection, maxMessages: 3);
        var sends = Enumerable.Range(0, 3).Select(i => producer.Send(new DotPulsar.MessageMetadata { Transaction = transaction }, [(byte) i], Current.CancellationToken).AsTask()).ToArray();

        //Act
        foreach (var send in sends)
            await Should.ThrowAsync<NotAllowedException>(send.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        await Should.ThrowAsync<TransactionHasOperationFailedException>(transaction.Commit(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    [Fact]
    public async Task Send_GivenPlainAndTransactionalMessagesAndBatching_ShouldNotPutThemInOneEntry()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [1], null, Current.CancellationToken);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata { Transaction = transaction }, [2], null, Current.CancellationToken);
        await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [3], null, Current.CancellationToken);
        await producer.Flush(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent;
        sent.Count.ShouldBe(3);
        sent.Select(entry => entry.CommandIsTransactional).ShouldBe([false, true, false]);
        sent.Select(entry => entry.MetadataIsTransactional).ShouldBe([false, true, false]);
    }

    private static Producer<byte[]> CreateProducer(FakeProducerConnection connection, bool enableBatching = true, uint maxMessages = 1000, TimeSpan? publishDelay = null)
    {
        var options = new ProducerOptions<byte[]>(_topic, DotPulsar.Schema.ByteArray)
        {
            MaxPendingMessages = 500,
            EnableBatching = enableBatching,
            BatchingMaxMessages = maxMessages,
            BatchingMaxPublishDelay = publishDelay ?? TimeSpan.FromHours(1)
        };

        return new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, true);
    }

    private static async Task<Producer<byte[]>> CreateConnectedProducer(FakeProducerConnection connection, uint maxMessages = 1000)
    {
        var producer = CreateProducer(connection, maxMessages: maxMessages);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        return producer;
    }

    private static Transaction CreateTransaction(ITransactionCoordinatorClient tcClient)
        => new(tcClient, new TransactionId(0, 5), TimeSpan.FromMinutes(1));
}
