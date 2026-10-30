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
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class ProducerTransactionTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Send_GivenTransactionNotOpen_ShouldThrowInvalidTransactionStatusExceptionAndNotEnqueue()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await transaction.Abort(Current.CancellationToken);
        await using var producer = await CreateConnectedProducer(connection);
        var metadata = new MessageMetadata { Transaction = transaction };

        //Act
        var exception = await Record.ExceptionAsync(async () => await producer.Send(metadata, [1], Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>();
        connection.Sent.ShouldBeEmpty();
        metadata.SequenceId.ShouldBe(0UL);
        await tcClient.DidNotReceive().AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Send_GivenForeignITransaction_ShouldThrowArgumentException()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var foreignTransaction = Substitute.For<ITransaction>();
        foreignTransaction.Id.Returns(new TransactionId(0, 5));
        await using var producer = await CreateConnectedProducer(connection);
        var metadata = new MessageMetadata { Transaction = foreignTransaction };

        //Act
        var exception = await Record.ExceptionAsync(async () => await producer.Send(metadata, [1], Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ArgumentException>().ParamName.ShouldBe("transaction");
        connection.Sent.ShouldBeEmpty();
    }

    [Fact]
    public async Task Send_GivenFirstTransactionalSendThenPlainSend_ShouldWriteInCallOrder()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tcClient = CreateTcClient(() => new ValueTask(registration.Task));
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.SendChannel.Send(new MessageMetadata { Transaction = transaction }, [1], null, Current.CancellationToken);
        await producer.SendChannel.Send(new MessageMetadata(), [2], null, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var sentBeforeRegistration = connection.Sent.Count;
        registration.SetResult();
        await WaitFor.Until(() => connection.Sent.Count == 2, "both messages are written");

        //Assert
        sentBeforeRegistration.ShouldBe(0);
        var sent = connection.Sent;
        sent[0].CommandIsTransactional.ShouldBeTrue();
        sent[1].CommandIsTransactional.ShouldBeFalse();
        sent[0].SequenceId.ShouldBeLessThan(sent[1].SequenceId);
    }

    [Fact]
    public async Task Send_GivenRegistrationFails_ShouldFailOnlyThatMessageAndContinue()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var tcClient = CreateTcClient(() => ValueTask.FromException(new TransactionCoordinatorClientException("The registration failed")));
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        var failedSend = producer.Send(new MessageMetadata { Transaction = transaction }, [1], Current.CancellationToken).AsTask();
        var plainSend = producer.Send(new MessageMetadata(), [2], Current.CancellationToken).AsTask();
        var secondFailedSend = producer.Send(new MessageMetadata { Transaction = transaction }, [3], Current.CancellationToken).AsTask();
        var thirdPlainSend = producer.Send(new MessageMetadata(), [4], Current.CancellationToken).AsTask();

        //Assert
        await Should.ThrowAsync<TransactionCoordinatorClientException>(failedSend.WaitAsync(_timeout, Current.CancellationToken));
        await Should.ThrowAsync<TransactionCoordinatorClientException>(secondFailedSend.WaitAsync(_timeout, Current.CancellationToken));
        await plainSend.WaitAsync(_timeout, Current.CancellationToken);
        await thirdPlainSend.WaitAsync(_timeout, Current.CancellationToken);
        connection.Sent.Count.ShouldBe(2);
        connection.Sent.ShouldAllBe(x => !x.CommandIsTransactional);
        await Should.ThrowAsync<TransactionHasOperationFailedException>(transaction.Commit(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    [Fact]
    public async Task SendChannel_GivenTransactionalSend_ShouldRegisterOpBeforeReturning()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.SendChannel.Send(new MessageMetadata { Transaction = transaction }, [1], null, Current.CancellationToken);
        var commit = transaction.Commit(Current.CancellationToken).AsTask();
        await WaitFor.Until(() => connection.Sent.Count == 1, "the message is written");
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var isCompletedBeforeReceipt = commit.IsCompleted;
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        connection.ReleaseResponses();
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isCompletedBeforeReceipt.ShouldBeFalse();
        await tcClient.Received(1).Commit(transaction.Id, CancellationToken.None);
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    [Fact]
    public async Task SendChannel_GivenCommitStartedBeforeProducerIsConnected_ShouldFailTheCommitBecauseTheSendCanNoLongerBeRegistered()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var partitionsKnown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.NumberOfPartitionsGate = partitionsKnown.Task;
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = CreateProducer(connection);

        //Act
        var enqueue = producer.SendChannel.Send(new MessageMetadata { Transaction = transaction }, [1], null, Current.CancellationToken).AsTask();
        var commit = transaction.Commit(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var isCompletedBeforeConnected = commit.IsCompleted;
        partitionsKnown.SetResult();

        //Assert
        isCompletedBeforeConnected.ShouldBeFalse("the commit has to wait for the send, even though the send is not enqueued yet");
        await Should.ThrowAsync<InvalidTransactionStatusException>(enqueue.WaitAsync(_timeout, Current.CancellationToken));
        await Should.ThrowAsync<TransactionHasOperationFailedException>(commit.WaitAsync(_timeout, Current.CancellationToken));
        connection.Sent.ShouldBeEmpty();
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    [Fact]
    public async Task Send_GivenSamePartitionTwice_ShouldRegisterPartitionOnce()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.Send(new MessageMetadata { Transaction = transaction }, [1], Current.CancellationToken);
        await producer.Send(new MessageMetadata { Transaction = transaction }, [2], Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddPublishPartitionToTxn(transaction.Id, _topic, CancellationToken.None);
        connection.Sent.Count.ShouldBe(2);
    }

    [Fact]
    public async Task Send_GivenPartitionedTopic_ShouldRegisterEachPartitionOnce()
    {
        //Arrange
        var connection = new FakeProducerConnection(3);
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = CreateProducer(connection);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Act
        for (var i = 0; i < 6; ++i)
            await producer.Send(new MessageMetadata { Transaction = transaction }, [(byte) i], Current.CancellationToken);

        //Assert
        for (var i = 0; i < 3; ++i)
            await tcClient.Received(1).AddPublishPartitionToTxn(transaction.Id, $"{_topic}-partition-{i}", CancellationToken.None);
        await tcClient.Received(3).AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        connection.Sent.Count.ShouldBe(6);
    }

    [Fact]
    public async Task Send_GivenSendCancelledWhileQueueIsFull_ShouldFailTheOperationOfTheTransaction()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = CreateTransaction(tcClient);
        await using var producer = CreateProducer(connection, 1);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        await producer.SendChannel.Send(new MessageMetadata { Transaction = transaction }, [1], null, Current.CancellationToken);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);

        //Act
        var blockedEnqueue = producer.SendChannel.Send(new MessageMetadata { Transaction = transaction }, [2], null, cts.Token).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var isBlocked = !blockedEnqueue.IsCompleted;
        await cts.CancelAsync();
        var exception = await Record.ExceptionAsync(() => blockedEnqueue.WaitAsync(_timeout, Current.CancellationToken));
        await WaitFor.Until(() => connection.Sent.Count == 1, "the first message is written");
        connection.ReleaseResponses();

        //Assert
        isBlocked.ShouldBeTrue();
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        await Should.ThrowAsync<TransactionHasOperationFailedException>(transaction.Commit(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken));
    }

    [Fact]
    public async Task Send_GivenTransactionalMessageThenPlainMessage_ShouldNotLeakTransactionIdsIntoThePlainMessage()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = new Transaction(tcClient, new TransactionId(0, 0), TimeSpan.FromMinutes(1));
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.NewMessage().Transaction(transaction).Send([1], Current.CancellationToken);
        await producer.NewMessage().Send([2], Current.CancellationToken);
        await producer.NewMessage().Transaction(transaction).Send([3], Current.CancellationToken);

        //Assert
        var sent = connection.Sent;
        sent.Count.ShouldBe(3);
        sent[0].CommandIsTransactional.ShouldBeTrue();
        sent[0].MetadataIsTransactional.ShouldBeTrue();
        sent[1].CommandIsTransactional.ShouldBeFalse();
        sent[1].MetadataIsTransactional.ShouldBeFalse();
        sent[2].CommandIsTransactional.ShouldBeTrue();
        sent[2].MetadataIsTransactional.ShouldBeTrue();
    }

    [Fact]
    public async Task Send_GivenTransactionalMessage_ShouldSendTheIdsOfTheTransaction()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var transaction = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.NewMessage().Transaction(transaction).Send([1], Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.CommandTxnidMostBits.ShouldBe(0UL);
        sent.CommandTxnidLeastBits.ShouldBe(5UL);
        sent.MetadataTxnidMostBits.ShouldBe(0UL);
        sent.MetadataTxnidLeastBits.ShouldBe(5UL);
    }

    [Fact]
    public async Task CreateProducer_GivenClientWithTransactions_ShouldSetTxnEnabled()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var client = PulsarClientFactory.CreatePulsarClient(connection.Pool, new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"), Substitute.For<ITransactionCoordinatorClient>());

        //Act
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(_topic).Create();
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Assert
        var command = connection.CommandProducers.Single();
        command.HasTxnEnabled.ShouldBeTrue();
        command.TxnEnabled.ShouldBeTrue();
    }

    [Fact]
    public async Task CreateProducer_GivenClientWithoutTransactions_ShouldNotSetTxnEnabled()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var client = PulsarClientFactory.CreatePulsarClient(connection.Pool, new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));

        //Act
        await using var producer = client.NewProducer(Schema.ByteArray).Topic(_topic).Create();
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Assert
        connection.CommandProducers.Single().HasTxnEnabled.ShouldBeFalse();
    }

    private static Producer<byte[]> CreateProducer(FakeProducerConnection connection, uint maxPendingMessages = 500)
    {
        var options = new ProducerOptions<byte[]>(_topic, Schema.ByteArray) { MaxPendingMessages = maxPendingMessages };
        return new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, true);
    }

    private static async Task<Producer<byte[]>> CreateConnectedProducer(FakeProducerConnection connection)
    {
        var producer = CreateProducer(connection);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        return producer;
    }

    private static Transaction CreateTransaction(ITransactionCoordinatorClient tcClient)
        => new(tcClient, new TransactionId(0, 5), TimeSpan.FromMinutes(1));

    private static ITransactionCoordinatorClient CreateTcClient(Func<ValueTask> addPartition)
    {
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => addPartition());
        return tcClient;
    }
}
