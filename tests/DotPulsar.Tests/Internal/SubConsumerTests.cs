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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Chunking;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class SubConsumerTests
{
    private static readonly MessageId _messageId = new(1, 2, 0, -1, "persistent://public/default/test");
    private static readonly TransactionId _transactionId = new(0, 5);

    private readonly IRegisterEvent _eventRegister;
    private readonly IConsumerChannel<byte[]> _channel;

    public SubConsumerTests()
    {
        _eventRegister = Substitute.For<IRegisterEvent>();
        _channel = Substitute.For<IConsumerChannel<byte[]>>();
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenAckResponseWithoutError_ShouldComplete()
    {
        //Arrange
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(CreateAckResponse(null)));
        await using var uut = CreateSubConsumer(true);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenNoReceiptRequested_ShouldCompleteWithoutResponse()
    {
        //Arrange
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(null));
        await using var uut = CreateSubConsumer(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        await _channel.Received(1).Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenAckResponseWithTransactionConflictError_ShouldThrowTransactionConflictExceptionAndNotFaultConsumer()
    {
        //Arrange
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(CreateAckResponse(ServerError.TransactionConflict)));
        await using var uut = CreateSubConsumer(true);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<TransactionConflictException>();
        _eventRegister.DidNotReceive().Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenAckResponseWithConsumerNotFound_ShouldThrowConsumerNotFoundExceptionWithoutRetry()
    {
        //Arrange
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(CreateAckResponse(ServerError.ConsumerNotFound)));
        await using var uut = CreateSubConsumer(true);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ConsumerNotFoundException>();
        await _channel.Received(1).Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenConnectionLostWhileWaiting_ShouldRetryOnNewChannel()
    {
        //Arrange
        var calls = 0;
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1
            ? Task.FromException<BaseCommand?>(new TaskCanceledException())
            : Task.FromResult<BaseCommand?>(CreateAckResponse(null)));
        await using var uut = CreateSubConsumer(true);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        calls.ShouldBe(2);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenCallerCancellation_ShouldThrowOperationCanceledException()
    {
        //Arrange
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo => WaitForever(callInfo.Arg<CancellationToken>()));
        await using var uut = CreateSubConsumer(true);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, cts.Token));

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenNoReceiptWithinOperationTimeout_ShouldThrowOperationTimeoutExceptionAndNotFaultConsumer()
    {
        //Arrange
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo => WaitForever(callInfo.Arg<CancellationToken>()));
        await using var uut = CreateSubConsumer(true, TimeSpan.FromMilliseconds(100));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        _eventRegister.DidNotReceive().Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenPooledCommandReuse_ShouldResetTxnAndRequestIdFields()
    {
        //Arrange
        CommandAck? pooled = null;
        var observed = new List<(bool HasRequestId, bool HasTxnMostBits, bool HasTxnLeastBits, bool HasValidationError, int Properties)>();
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            var command = callInfo.Arg<CommandAck>();
            pooled = command;
            observed.Add((command.HasRequestId, command.HasTxnidMostBits, command.HasTxnidLeastBits, command.HasValidationError, command.Properties.Count));
            return Task.FromResult<BaseCommand?>(null);
        });
        await using var uut = CreateSubConsumer(false);
        await uut.Acknowledge(_messageId, Current.CancellationToken);
        pooled.ShouldNotBeNull();
        pooled.RequestId = 5;
        pooled.TxnidMostBits = 1;
        pooled.TxnidLeastBits = 2;
        pooled.ValidationError = CommandAck.Types.ValidationError.ChecksumMismatch;
        pooled.Properties.Add(new KeyLongValue { Key = "key", Value = 1 });

        //Act
        await uut.Acknowledge(_messageId, Current.CancellationToken);

        //Assert
        observed.Count.ShouldBe(2);
        observed[1].ShouldBe((false, false, false, false, 0));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenPooledCommandWithAckSetFromPreviousAck_ShouldResetAckSetAndBatchSize()
    {
        //Arrange
        CommandAck? pooled = null;
        var observed = new List<(int AckSetCount, bool HasBatchSize, bool HasFirstChunkMessageId)>();
        _channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            var command = callInfo.Arg<CommandAck>();
            pooled = command;
            var messageId = command.MessageId[0];
            observed.Add((messageId.AckSet.Count, messageId.HasBatchSize, messageId.FirstChunkMessageId is not null));
            return Task.FromResult<BaseCommand?>(null);
        });
        await using var uut = CreateSubConsumer(false);
        await uut.Acknowledge(_messageId, Current.CancellationToken);
        pooled.ShouldNotBeNull();
        pooled.MessageId[0].AckSet.AddRange([1L, 2L]);
        pooled.MessageId[0].BatchSize = 5;
        pooled.MessageId[0].FirstChunkMessageId = new MessageIdData { LedgerId = 1, EntryId = 1 };

        //Act
        await uut.Acknowledge(_messageId, Current.CancellationToken);

        //Assert
        observed.Count.ShouldBe(2);
        observed[1].ShouldBe((0, false, false));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenCompletedBatchFollowedByAnotherAck_ShouldNotModifyTheMessageIdOfTheBroker()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var brokerMessageId = BatchTestData.CreateEntryId(1, 2);
        var data = new ReadOnlySequence<byte>(BatchTestData.CreateBatchPayload(2));
        var first = batchHandler.Add(brokerMessageId, null, 0, BatchTestData.CreateMetadata(2), data, out _);
        var second = batchHandler.GetNext();
        first.ShouldNotBeNull();
        second.ShouldNotBeNull();
        var connection = CreateConnection(out var sent);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, false));
        await uut.Acknowledge(first.MessageId, Current.CancellationToken);
        await uut.Acknowledge(second.MessageId, Current.CancellationToken);

        //Act
        await uut.Acknowledge(new MessageId(9, 9, 0, -1), Current.CancellationToken);

        //Assert
        brokerMessageId.LedgerId.ShouldBe(1UL);
        brokerMessageId.EntryId.ShouldBe(2UL);
        sent.Count.ShouldBe(2);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[1].MessageId[0].EntryId.ShouldBe(9UL);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenPartialBatchAndBatchIndexAckEnabled_ShouldSendAckSetAndNotLeakItIntoTheNextAck()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 3);
        var connection = CreateConnection(out var sent);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, true));

        //Act
        await uut.Acknowledge(messages[1].MessageId, Current.CancellationToken);
        await uut.Acknowledge(new MessageId(9, 9, 0, -1), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(2);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b101);
        sent[1].MessageId[0].EntryId.ShouldBe(9UL);
        sent[1].MessageId[0].AckSet.Count.ShouldBe(0);
        sent[1].MessageId[0].HasBatchSize.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTwoIndexesOfSameBatchAndBatchIndexAckEnabled_ShouldSendOneEntryWithMergedAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, true));

        //Act
        await uut.Acknowledge([messages[0].MessageId, messages[4].MessageId], Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b01110);
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenPartialBatchAndBatchIndexAckDisabled_ShouldAcknowledgePreviousEntryCumulatively()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, false));

        //Act
        await uut.AcknowledgeCumulative(messages[2].MessageId, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId[0].EntryId.ShouldBe(1UL);
        sent[0].MessageId[0].BatchIndex.ShouldBe(-1);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenPartialBatchAndBatchIndexAckDisabledAndAckReceiptEnabled_ShouldCompleteWithoutSending()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateConnection(out var sent);
        await using var uut = CreateSubConsumer(true, channel: CreateChannel(connection, batchHandler, messageFactory, false, true));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(messages[2].MessageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        sent.ShouldBeEmpty();
        await connection.DidNotReceive().SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransaction_ShouldSendTransactionalAckWithTransactionIdAndIndividualAckType()
    {
        //Arrange
        var sent = new List<CommandAck>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].TxnidMostBits.ShouldBe(_transactionId.MostSigBits);
        sent[0].TxnidLeastBits.ShouldBe(_transactionId.LeastSigBits);
        sent[0].HasRequestId.ShouldBeFalse();
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].LedgerId.ShouldBe(_messageId.LedgerId);
        sent[0].MessageId[0].EntryId.ShouldBe(_messageId.EntryId);
        await _channel.DidNotReceive().Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndSeveralMessageIds_ShouldSendOneCommandWithAllMessageIds()
    {
        //Arrange
        var sent = new List<CommandAck>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge([new MessageId(1, 2, 0, -1), new MessageId(1, 3, 0, -1), new MessageId(1, 4, 0, -1)], transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].MessageId.Select(messageId => messageId.EntryId).ShouldBeSequence(2UL, 3UL, 4UL);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndNoMessageIds_ShouldNeitherSendNorRegisterAnything()
    {
        //Arrange
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge([], transaction, Current.CancellationToken);

        //Assert
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        await tcClient.DidNotReceive().AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenTransaction_ShouldSendTransactionalAckWithCumulativeAckType()
    {
        //Arrange
        var sent = new List<CommandAck>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command => sent.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.AcknowledgeCumulative(_messageId, transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].TxnidMostBits.ShouldBe(_transactionId.MostSigBits);
        sent[0].TxnidLeastBits.ShouldBe(_transactionId.LeastSigBits);
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task AcknowledgeCumulative_GivenTransactionAndNonExclusiveNonFailoverSubscription_ShouldThrowConfigurationExceptionAndNotSend(SubscriptionType subscriptionType)
    {
        //Arrange
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.AcknowledgeCumulative(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>();
        exception.Message.ShouldBe("Cannot use cumulative acks on a non-exclusive/non-failover subscription");
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        await tcClient.DidNotReceive().AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public async Task AcknowledgeCumulative_GivenTransactionAndExclusiveOrFailoverSubscription_ShouldSend(SubscriptionType subscriptionType)
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType);

        //Act
        await uut.AcknowledgeCumulative(_messageId, transaction, Current.CancellationToken);

        //Assert
        await _channel.Received(1).SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransaction_ShouldRegisterTheTopicAndSubscriptionWithTheTransactionOnce()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);
        await uut.Acknowledge(new MessageId(1, 3, 0, -1), transaction, Current.CancellationToken);
        await uut.AcknowledgeCumulative(new MessageId(1, 4, 0, -1), transaction, Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddSubscriptionToTxn(_transactionId, "persistent://public/default/test", "subscription", CancellationToken.None);
        await _channel.Received(3).SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransaction_ShouldRegisterAckOpSoThatCommitWaitsForTheReceipt()
    {
        //Arrange
        var receipt = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(receipt.Task);
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);
        var ackTask = uut.Acknowledge(_messageId, transaction, Current.CancellationToken).AsTask();
        var commitTask = transaction.Commit(Current.CancellationToken).AsTask();

        //Act
        var completedBeforeReceipt = await Task.WhenAny(commitTask, Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken)) == commitTask;
        receipt.SetResult(CreateAckResponse(null));
        await commitTask;
        await ackTask;

        //Assert
        completedBeforeReceipt.ShouldBeFalse();
        ackTask.IsCompletedSuccessfully.ShouldBeTrue();
        await tcClient.Received(1).Commit(_transactionId, CancellationToken.None);
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndAckResponseWithTransactionConflictError_ShouldThrowTransactionConflictExceptionAndNotFaultConsumer()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(ServerError.TransactionConflict));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<TransactionConflictException>();
        await _channel.Received(1).SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        _eventRegister.DidNotReceive().Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndAckResponseWithConsumerNotFound_ShouldThrowConsumerNotFoundExceptionWithoutRetry()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(ServerError.ConsumerNotFound));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ConsumerNotFoundException>();
        await _channel.Received(1).SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenTransactionalAckThatFailed_ShouldAbortAndThrowTransactionHasOperationFailedException()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(ServerError.TransactionConflict));
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);
        await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Act
        var exception = await Record.ExceptionAsync(async () => await transaction.Commit(Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        await tcClient.Received(1).Abort(_transactionId, CancellationToken.None);
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndRegistrationOfSubscriptionFails_ShouldFailWithTheSameExceptionAndNotSend()
    {
        //Arrange
        var transaction = CreateTransaction(out var tcClient);
        tcClient.AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask(Task.FromException(new TransactionNotFoundException("not found"))));
        await using var uut = CreateSubConsumer(false);

        //Act
        var first = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));
        var second = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        first.ShouldBeOfType<TransactionNotFoundException>();
        second.ShouldBeOfType<TransactionNotFoundException>();
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        _eventRegister.DidNotReceive().Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionThatIsNotOpen_ShouldThrowInvalidTransactionStatusExceptionAndNotSend()
    {
        //Arrange
        var transaction = CreateTransaction(out var tcClient);
        await transaction.Abort(Current.CancellationToken);
        await using var uut = CreateSubConsumer(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>();
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        await tcClient.DidNotReceive().AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenMessageIdsAndTransactionThatIsNotOpen_ShouldThrowInvalidTransactionStatusExceptionAndNotSend()
    {
        //Arrange
        var transaction = CreateTransaction(out _);
        await transaction.Abort(Current.CancellationToken);
        await using var uut = CreateSubConsumer(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge([_messageId], transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>();
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Java only checks that the transaction is open for the acknowledgment of a single message id, and it does not register that failure as an operation of the transaction
    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionThatIsCommitting_ShouldThrowInvalidTransactionStatusExceptionWithoutFailingTheCommit()
    {
        //Arrange
        var receipt = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(receipt.Task);
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);
        var firstAck = uut.Acknowledge(_messageId, transaction, Current.CancellationToken).AsTask();
        var commitTask = transaction.Commit(Current.CancellationToken).AsTask();

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(new MessageId(1, 3, 0, -1), transaction, Current.CancellationToken));
        receipt.SetResult(CreateAckResponse(null));
        await commitTask;

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>();
        await firstAck;
        await tcClient.Received(1).Commit(_transactionId, CancellationToken.None);
        transaction.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionThatWasNotCreatedByDotPulsar_ShouldThrowArgumentException()
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();
        await using var uut = CreateSubConsumer(false);

        //Act
        var single = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));
        var multiple = await Record.ExceptionAsync(async () => await uut.Acknowledge([_messageId], transaction, Current.CancellationToken));
        var cumulative = await Record.ExceptionAsync(async () => await uut.AcknowledgeCumulative(_messageId, transaction, Current.CancellationToken));

        //Assert
        single.ShouldBeOfType<ArgumentException>();
        multiple.ShouldBeOfType<ArgumentException>();
        cumulative.ShouldBeOfType<ArgumentException>();
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndBatchMessageId_ShouldSetBatchSizeAndKeepBatchIndexForTheChannel()
    {
        //Arrange
        var sent = new List<(int BatchIndex, int BatchSize)>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command => sent.Add((command.MessageId[0].BatchIndex, command.MessageId[0].BatchSize))), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge(MessageIdInternals.Create(1, 2, 0, 3, BatchTestData.Topic, 5), transaction, Current.CancellationToken);

        //Assert
        sent.ShouldBe([(3, 5)]);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionalAckFollowedByPlainAck_ShouldNotSendTransactionIdsOrRequestIdWithThePlainAck()
    {
        //Arrange
        var observed = new List<(bool HasTxnMostBits, bool HasTxnLeastBits, bool HasRequestId, bool HasValidationError)>();
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        _channel.Send(Arg.Do<CommandAck>(command => observed.Add((command.HasTxnidMostBits, command.HasTxnidLeastBits, command.HasRequestId, command.HasValidationError))), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BaseCommand?>(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);
        await uut.Acknowledge(_messageId, Current.CancellationToken);
        await uut.Acknowledge([_messageId], Current.CancellationToken);
        await uut.AcknowledgeCumulative(_messageId, Current.CancellationToken);

        //Assert
        observed.Count.ShouldBe(3);
        observed.ShouldAllBe(command => !command.HasTxnMostBits && !command.HasTxnLeastBits && !command.HasRequestId && !command.HasValidationError);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndConnectionLostWhileWaiting_ShouldRetryOnNewChannelWithTheSameCommand()
    {
        //Arrange
        var commands = new List<CommandAck>();
        var calls = 0;
        _channel.SendTransactional(Arg.Do<CommandAck>(commands.Add), Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1
            ? Task.FromException<BaseCommand>(new TaskCanceledException())
            : Task.FromResult(CreateAckResponse(null)));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        calls.ShouldBe(2);
        commands[1].ShouldBeSameAs(commands[0]);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndNoReceiptWithinOperationTimeout_ShouldThrowOperationTimeoutExceptionAndNotFaultConsumer()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo => WaitForeverForResponse(callInfo.Arg<CancellationToken>()));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false, TimeSpan.FromMilliseconds(100));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        _eventRegister.DidNotReceive().Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndCallerCancellation_ShouldThrowOperationCanceledExceptionAndFailTheOperation()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo => WaitForeverForResponse(callInfo.Arg<CancellationToken>()));
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, cts.Token));
        var commitException = await Record.ExceptionAsync(async () => await transaction.Commit(Current.CancellationToken));

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        commitException.ShouldBeOfType<TransactionHasOperationFailedException>();
        await tcClient.Received(1).Abort(_transactionId, CancellationToken.None);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndBrokerWithoutSupportForAckReceipts_ShouldThrowNotSupportedExceptionAndNotFaultConsumer()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(16);
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, false));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(messages[2].MessageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<NotSupportedException>();
        await connection.DidNotReceive().SendWithResponse(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        _eventRegister.DidNotReceive().Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndBatchMessage_ShouldNotBeSwallowedByTheBatchHandlerAndSendBatchSizeAndAckSet()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, false));

        //Act
        await uut.Acknowledge(messages[2].MessageId, transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].TxnidMostBits.ShouldBe(_transactionId.MostSigBits);
        sent[0].TxnidLeastBits.ShouldBe(_transactionId.LeastSigBits);
        sent[0].MessageId.Count.ShouldBe(1);
        sent[0].MessageId[0].EntryId.ShouldBe(2UL);
        sent[0].MessageId[0].HasBatchIndex.ShouldBeFalse();
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11011);
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenTransactionAndBatchMessage_ShouldSendAckSetOfTheLaterIndexes()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory, false));

        //Act
        await uut.AcknowledgeCumulative(messages[2].MessageId, transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11000);
    }

    // The messages of an aborted transaction are delivered again by the broker. Acknowledging every message of the new delivery must then acknowledge the whole batch.
    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenAbortedTransactionAndRedeliveryOfTheBatch_ShouldAcknowledgeEntryOnlyWhenAllIndexesAreAcknowledgedAgain()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var queue = new AsyncQueue<MessagePackage>();
        var connection = CreateTransactionalConnection(out _);
        var plainSent = new List<CommandAck>();
        connection.Send(Arg.Do<CommandAck>(ack => plainSent.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var channel = new ConsumerChannel<byte[]>(1, 100, queue, connection, batchHandler, messageFactory, [], BatchTestData.Topic, false, false, null, null, null, ChunkTestData.CreateAssembler(), null, null, ConsumerCryptoFailureAction.Fail);
        await using var uut = CreateSubConsumer(false, channel: channel);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var delivered = new List<DotPulsar.Abstractions.IMessage<byte[]>>();
        for (var i = 0; i < 5; ++i)
        {
            delivered.Add(await uut.Receive(Current.CancellationToken));
        }

        var aborted = CreateTransaction(out _);
        await uut.AcknowledgeCumulative(delivered[2].MessageId, aborted, Current.CancellationToken);
        await aborted.Abort(Current.CancellationToken);
        queue.Enqueue(BatchTestData.CreateBatchPackage(BatchTestData.CreateEntryId(1, 2), 5));
        var redelivered = new List<DotPulsar.Abstractions.IMessage<byte[]>>();
        for (var i = 0; i < 5; ++i)
        {
            redelivered.Add(await uut.Receive(Current.CancellationToken));
        }

        //Act
        var entryAcksAfterEachAck = new List<int>();
        foreach (var message in redelivered.Skip(3).Concat(redelivered.Take(3)))
        {
            await uut.Acknowledge(message.MessageId, Current.CancellationToken);
            entryAcksAfterEachAck.Add(plainSent.Count);
        }

        //Assert
        entryAcksAfterEachAck.ShouldBeSequence(0, 0, 0, 0, 1);
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages(Set)
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task RedeliverUnacknowledgedMessages_GivenNoMessageIds_ShouldNotSendAnyCommand(SubscriptionType subscriptionType)
    {
        //Arrange
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType);

        //Act
        await uut.RedeliverUnacknowledgedMessages(Array.Empty<MessageId>(), Current.CancellationToken);

        //Assert
        await _channel.DidNotReceive().Send(Arg.Any<CommandRedeliverUnacknowledgedMessages>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task RedeliverUnacknowledgedMessages_GivenMessageIdsOfTheSameEntry_ShouldSendTheEntryOnceWithoutBatchIndex(SubscriptionType subscriptionType)
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType);
        MessageId[] messageIds =
        [
            new(1, 2, 3, 0, "persistent://public/default/test"),
            new(1, 3, 3, -1, "persistent://public/default/test"),
            new(1, 2, 3, 4, "persistent://public/default/test"),
            new(1, 2, 3, -1, "persistent://public/default/test")
        ];

        //Act
        await uut.RedeliverUnacknowledgedMessages(messageIds, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Select(messageId => (messageId.LedgerId, messageId.EntryId, messageId.Partition)).ShouldBeSequence((1UL, 2UL, 3), (1UL, 3UL, 3));
        sent[0].MessageIds.ShouldAllBe(messageId => !messageId.HasBatchIndex);
        sent[0].HasConsumerEpoch.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenMoreThanOneThousandEntries_ShouldSendThemInCommandsOfOneThousand()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        await using var uut = CreateSubConsumer(false, subscriptionType: SubscriptionType.Shared);
        var messageIds = Enumerable.Range(0, 2500).Select(entryId => new MessageId(1, (ulong) entryId, 0, -1)).ToArray();

        //Act
        await uut.RedeliverUnacknowledgedMessages(messageIds, Current.CancellationToken);

        //Assert
        sent.Select(command => command.MessageIds.Count).ShouldBeSequence(1000, 1000, 500);
        sent.SelectMany(command => command.MessageIds).Select(messageId => messageId.EntryId).ShouldBeSequence(messageIds.Select(messageId => messageId.EntryId).ToArray());
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public async Task RedeliverUnacknowledgedMessages_GivenMessageIdsOnSubscriptionWithSingleActiveConsumer_ShouldRedeliverAllAndIncreaseEpoch(SubscriptionType subscriptionType)
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var consumerEpoch = new ConsumerEpoch();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType, consumerEpoch: consumerEpoch);

        //Act
        await uut.RedeliverUnacknowledgedMessages([_messageId], Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Count.ShouldBe(0);
        sent[0].ConsumerEpoch.ShouldBe(1UL);
        consumerEpoch.Value.ShouldBe(1);
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Exclusive, 2)]
    [InlineData(SubscriptionType.Failover, 2)]
    [InlineData(SubscriptionType.Shared, 0)]
    [InlineData(SubscriptionType.KeyShared, 0)]
    public async Task RedeliverUnacknowledgedMessages_GivenCalledTwiceWithoutMessageIds_ShouldIncreaseEpochOnlyForSubscriptionsWithSingleActiveConsumer(SubscriptionType subscriptionType, long expectedEpoch)
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var consumerEpoch = new ConsumerEpoch();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType, consumerEpoch: consumerEpoch);

        //Act
        await uut.RedeliverUnacknowledgedMessages(Current.CancellationToken);
        await uut.RedeliverUnacknowledgedMessages(Current.CancellationToken);

        //Assert
        consumerEpoch.Value.ShouldBe(expectedEpoch);
        sent.Count.ShouldBe(2);
        sent.ShouldAllBe(command => command.MessageIds.Count == 0 && command.HasConsumerEpoch);
        sent[1].ConsumerEpoch.ShouldBe((ulong) expectedEpoch);
    }

    // Additional coverage
    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenDisposedConsumer_ShouldThrowConsumerDisposedException()
    {
        //Arrange
        var uut = CreateSubConsumer(false);
        await uut.DisposeAsync();

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.RedeliverUnacknowledgedMessages(Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ConsumerDisposedException>();
    }

    private List<CommandRedeliverUnacknowledgedMessages> CaptureRedeliverCommands()
    {
        var commands = new List<CommandRedeliverUnacknowledgedMessages>();
        _channel.Send(Arg.Do<CommandRedeliverUnacknowledgedMessages>(command => commands.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return commands;
    }

    // Upstream: ConsumerImpl#trackMessage
    [Fact]
    public async Task Track_GivenMessage_ShouldTrackItsEntry()
    {
        //Arrange
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);

        //Act
        uut.Track(CreateMessage(new MessageId(1, 2, 0, 3, _messageId.Topic)));
        uut.Track(CreateMessage(new MessageId(1, 2, 0, 4, _messageId.Topic)));

        //Assert
        uut.UnAckedMessageCount.ShouldBe(1);
        tracker.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public async Task Track_GivenNoTracker_ShouldNotTrackAnything()
    {
        //Arrange
        await using var uut = CreateSubConsumer(false);

        //Act
        uut.Track(CreateMessage(_messageId));

        //Assert
        uut.UnAckedMessageCount.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#trackMessage (the redelivery count of the message is the one that the tracker uses for the backoff)
    [Fact]
    public async Task Track_GivenMessageWithRedeliveryCount_ShouldPassTheCountToTheTracker()
    {
        //Arrange
        var tracker = Substitute.For<IUnAckedMessageTracker>();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        var messageId = new MessageId(1, 2, 0, 3, _messageId.Topic);

        //Act
        uut.Track(CreateMessage(messageId, 4));

        //Assert
        tracker.Received(1).Add(messageId, 4u);
    }

    // Additional coverage
    [Fact]
    public async Task GetLocalRedeliveryCount_GivenTrackerWithoutBackoff_ShouldBeZero()
    {
        //Arrange
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        uut.Track(CreateMessage(_messageId));

        //Act
        var actual = uut.GetLocalRedeliveryCount(_messageId);

        //Assert
        actual.ShouldBe(0u);
    }

    // Additional coverage
    [Fact]
    public async Task GetLocalRedeliveryCount_GivenTrackerWithBackoffThatCountsRedeliveries_ShouldReturnTheNumberOfExpiries()
    {
        //Arrange
        var clock = new FakeMonotonicClock();
        using var tracker = new UnAckedMessageRedeliveryTracker(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), new MultiplierRedeliveryBackoff(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1)), true, (_, _) => default, () => clock.Now);
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        uut.Track(CreateMessage(_messageId));

        //Act
        _ = tracker.Tick();
        _ = tracker.Tick();
        var actual = uut.GetLocalRedeliveryCount(_messageId);

        //Assert
        actual.ShouldBe(1u);
        uut.UnAckedMessageCount.ShouldBe(1);
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse
    // The entry is removed when the acknowledgment is sent, before the receipt, and also when the transaction is aborted
    [Fact]
    public async Task Acknowledge_GivenTransactionAndTrackedMessage_ShouldUntrackItBeforeTheReceiptArrives()
    {
        //Arrange
        var receipt = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            sent.TrySetResult();
            return receipt.Task;
        });
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        uut.Track(CreateMessage(_messageId));
        var transaction = CreateTransaction(out _);

        //Act
        var ackTask = uut.Acknowledge(_messageId, transaction, Current.CancellationToken).AsTask();
        await sent.Task;

        //Assert
        ackTask.IsCompleted.ShouldBeFalse();
        tracker.Count.ShouldBe(0);
        receipt.SetResult(CreateAckResponse(ServerError.TransactionConflict));
        await Should.ThrowAsync<TransactionConflictException>(async () => await ackTask);
        tracker.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndBatchIndex_ShouldNotUntrackTheEntry()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        var batchMessageId = new MessageId(1, 2, 0, 3, _messageId.Topic);
        uut.Track(CreateMessage(batchMessageId));
        var transaction = CreateTransaction(out _);

        //Act
        await uut.Acknowledge(batchMessageId, transaction, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionAndMessageIdList_ShouldUntrackTheEntriesWithoutBatchIndex()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        uut.Track(CreateMessage(new MessageId(1, 1, 0, -1, _messageId.Topic)));
        uut.Track(CreateMessage(new MessageId(1, 2, 0, 1, _messageId.Topic)));
        uut.Track(CreateMessage(new MessageId(1, 3, 0, -1, _messageId.Topic)));
        var transaction = CreateTransaction(out _);

        //Act
        await uut.Acknowledge([new MessageId(1, 1, 0, -1, _messageId.Topic), new MessageId(1, 2, 0, 1, _messageId.Topic)], transaction, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(2);
        tracker.Remove(1, 2).ShouldBeTrue();
        tracker.Remove(1, 3).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenTransaction_ShouldUntrackAllEntriesUpToAndIncludingTheEntry()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        for (ulong entryId = 1; entryId <= 5; ++entryId)
        {
            uut.Track(CreateMessage(new MessageId(1, entryId, 0, -1, _messageId.Topic)));
        }

        var transaction = CreateTransaction(out _);

        //Act
        await uut.AcknowledgeCumulative(new MessageId(1, 3, 0, 2, _messageId.Topic), transaction, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(2);
        tracker.Remove(1, 4).ShouldBeTrue();
        tracker.Remove(1, 5).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task Acknowledge_GivenTransactionThatIsNotOpen_ShouldNotUntrackTheEntry()
    {
        //Arrange
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        uut.Track(CreateMessage(_messageId));
        var transaction = CreateTransaction(out _);
        await transaction.Abort(Current.CancellationToken);

        //Act
        _ = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        tracker.Count.ShouldBe(1);
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages()
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task RedeliverUnacknowledgedMessages_GivenNoMessageIds_ShouldClearTracker(SubscriptionType subscriptionType)
    {
        //Arrange
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType, tracker: tracker);
        uut.Track(CreateMessage(_messageId));

        //Act
        await uut.RedeliverUnacknowledgedMessages(Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(0);
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Exclusive, 0)]
    [InlineData(SubscriptionType.Failover, 0)]
    [InlineData(SubscriptionType.Shared, 1)]
    [InlineData(SubscriptionType.KeyShared, 1)]
    public async Task RedeliverUnacknowledgedMessages_GivenMessageIds_ShouldClearTrackerOnlyWhenAllAreRedelivered(SubscriptionType subscriptionType, int expectedTracked)
    {
        //Arrange
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType, tracker: tracker);
        uut.Track(CreateMessage(_messageId));

        //Act
        await uut.RedeliverUnacknowledgedMessages([_messageId], Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(expectedTracked);
    }

    // Upstream: ConsumerImpl#negativeAcknowledge
    [Fact]
    public async Task NegativeAcknowledge_GivenMessageId_ShouldUntrackTheEntryAndAddItToTheNegativeAcknowledgeTracker()
    {
        //Arrange
        using var tracker = CreateTracker();
        using var negativeAcknowledgeTracker = CreateNegativeAcknowledgeTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker, negativeAcknowledgeTracker: negativeAcknowledgeTracker);
        uut.Track(CreateMessage(new MessageId(1, 2, 0, 0, _messageId.Topic)));

        //Act
        await uut.NegativeAcknowledge(new MessageId(1, 2, 0, 1, _messageId.Topic), Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(0);
        negativeAcknowledgeTracker.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public async Task NegativeAcknowledge_GivenMessageIdsOfTheSameEntry_ShouldTrackTheEntryOnce()
    {
        //Arrange
        using var negativeAcknowledgeTracker = CreateNegativeAcknowledgeTracker();
        await using var uut = CreateSubConsumer(false, negativeAcknowledgeTracker: negativeAcknowledgeTracker);

        //Act
        await uut.NegativeAcknowledge(new MessageId(1, 2, 0, 0, _messageId.Topic), Current.CancellationToken);
        await uut.NegativeAcknowledge(new MessageId(1, 2, 0, 1, _messageId.Topic), Current.CancellationToken);
        await uut.NegativeAcknowledge(new MessageId(1, 2, 0, 2, _messageId.Topic), Current.CancellationToken);

        //Assert
        negativeAcknowledgeTracker.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public async Task NegativeAcknowledge_GivenDisposedConsumer_ShouldThrowConsumerDisposedException()
    {
        //Arrange
        var uut = CreateSubConsumer(false);
        await uut.DisposeAsync();

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.NegativeAcknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ConsumerDisposedException>();
    }

    // Additional coverage
    [Fact]
    public async Task NegativeAcknowledge_GivenDueEntry_ShouldRedeliverItThroughTheConsumer()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var timers = new FakeOneShotTimerFactory();
        var clock = new FakeMonotonicClock();
        var negativeAcknowledgeTracker = (NegativeAcknowledgeTracker?) null;
        await using var uut = CreateSubConsumer(false, subscriptionType: SubscriptionType.Shared, negativeAcknowledgeTrackerFactory: redeliver => negativeAcknowledgeTracker = new NegativeAcknowledgeTracker(TimeSpan.FromSeconds(1), redeliver, () => clock.Now, timers.Create));
        await uut.NegativeAcknowledge(new MessageId(1, 2, 0, 1, _messageId.Topic), Current.CancellationToken);

        //Act
        clock.Advance(TimeSpan.FromSeconds(2));
        timers.Timer.Fire();
        await WaitUntil(() => sent.Count == 1);

        //Assert
        negativeAcknowledgeTracker!.Count.ShouldBe(0);
        sent[0].MessageIds.Select(messageId => (messageId.LedgerId, messageId.EntryId)).ShouldBeSequence((1UL, 2UL));
    }

    // Upstream: ConsumerImpl#closeConsumerTasks
    [Fact]
    public async Task DisposeAsync_GivenTrackers_ShouldDisposeThem()
    {
        //Arrange
        var tracker = CreateTracker();
        var negativeAcknowledgeTracker = CreateNegativeAcknowledgeTracker();
        var uut = CreateSubConsumer(false, tracker: tracker, negativeAcknowledgeTracker: negativeAcknowledgeTracker);
        uut.Track(CreateMessage(_messageId));
        var negativelyAcknowledged = new MessageId(1, 3, 0, -1, _messageId.Topic);
        await uut.NegativeAcknowledge(negativelyAcknowledged, Current.CancellationToken);
        tracker.Count.ShouldBe(1);
        negativeAcknowledgeTracker.Count.ShouldBe(1);

        //Act
        await uut.DisposeAsync();

        //Assert
        tracker.Count.ShouldBe(0);
        negativeAcknowledgeTracker.Count.ShouldBe(0);
        negativeAcknowledgeTracker.Add(negativelyAcknowledged).ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task ChannelFaulted_GivenTracker_ShouldDisposeIt()
    {
        //Arrange
        using var tracker = CreateTracker();
        await using var uut = CreateSubConsumer(false, tracker: tracker);
        uut.Track(CreateMessage(_messageId));

        //Act
        await uut.ChannelFaulted(new Exception("fault"));

        //Assert
        tracker.Count.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages(Set), ConsumerImpl#processPossibleToDLQ
    [Theory]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task RedeliverUnacknowledgedMessages_GivenDeadLetterCandidate_ShouldDeadLetterTheEntryAndRedeliverTheOthers(SubscriptionType subscriptionType)
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var acknowledged = CaptureAcknowledgments();
        var (deadLetterHandler, producer) = CreateDeadLetterHandler();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType, deadLetterHandler: deadLetterHandler);
        deadLetterHandler.Register(1, 2, CreateMessage(new MessageId(1, 2, 0, -1, _messageId.Topic)));

        //Act
        await uut.RedeliverUnacknowledgedMessages([new MessageId(1, 2, 0, 3, _messageId.Topic), new MessageId(1, 3, 0, -1, _messageId.Topic)], Current.CancellationToken);

        //Assert
        await producer.Received(1).Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>());
        acknowledged.Count.ShouldBe(1);
        acknowledged[0].MessageId[0].EntryId.ShouldBe(2UL);
        acknowledged[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Select(messageId => messageId.EntryId).ShouldBeSequence(3UL);
    }

    // Additional coverage
    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenOnlyDeadLetterCandidates_ShouldNotSendRedeliverCommand()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        _ = CaptureAcknowledgments();
        var (deadLetterHandler, _) = CreateDeadLetterHandler();
        await using var uut = CreateSubConsumer(false, subscriptionType: SubscriptionType.Shared, deadLetterHandler: deadLetterHandler);
        deadLetterHandler.Register(1, 2, CreateMessage(new MessageId(1, 2, 0, -1, _messageId.Topic)));

        //Act
        await uut.RedeliverUnacknowledgedMessages([new MessageId(1, 2, 0, -1, _messageId.Topic)], Current.CancellationToken);

        //Assert
        sent.ShouldBeEmpty();
        deadLetterHandler.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenDeadLetterFailure_ShouldRedeliverTheEntry()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var (deadLetterHandler, producer) = CreateDeadLetterHandler();
        producer.Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>()).Returns<ValueTask<MessageId>>(_ => throw new InvalidOperationException("send failed"));
        await using var uut = CreateSubConsumer(false, subscriptionType: SubscriptionType.Shared, deadLetterHandler: deadLetterHandler);
        deadLetterHandler.Register(1, 2, CreateMessage(new MessageId(1, 2, 0, -1, _messageId.Topic)));

        //Act
        await uut.RedeliverUnacknowledgedMessages([new MessageId(1, 2, 0, -1, _messageId.Topic)], Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Select(messageId => messageId.EntryId).ShouldBeSequence(2UL);
        deadLetterHandler.Contains(1, 2).ShouldBeTrue();
    }

    // Additional coverage
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public async Task RedeliverUnacknowledgedMessages_GivenDeadLetterCandidateOnSubscriptionWithSingleActiveConsumer_ShouldRedeliverAllWithoutDeadLettering(SubscriptionType subscriptionType)
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var (deadLetterHandler, producer) = CreateDeadLetterHandler();
        await using var uut = CreateSubConsumer(false, subscriptionType: subscriptionType, deadLetterHandler: deadLetterHandler);
        deadLetterHandler.Register(1, 2, CreateMessage(new MessageId(1, 2, 0, -1, _messageId.Topic)));

        //Act
        await uut.RedeliverUnacknowledgedMessages([new MessageId(1, 2, 0, -1, _messageId.Topic)], Current.CancellationToken);

        //Assert
        await producer.DidNotReceive().Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>());
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenDeadLetterHandler_ShouldDisposeIt()
    {
        //Arrange
        var (deadLetterHandler, producer) = CreateDeadLetterHandler();
        var uut = CreateSubConsumer(false, deadLetterHandler: deadLetterHandler);
        deadLetterHandler.Register(1, 2, CreateMessage(new MessageId(1, 2, 0, -1, _messageId.Topic)));
        _ = await deadLetterHandler.TryDeadLetter(new MessageId(1, 3, 0, -1, _messageId.Topic), (_, _) => default, Current.CancellationToken);

        //Act
        await uut.DisposeAsync();

        //Assert
        deadLetterHandler.Count.ShouldBe(0);
        (await deadLetterHandler.TryDeadLetter(new MessageId(1, 2, 0, -1, _messageId.Topic), (_, _) => default, Current.CancellationToken)).ShouldBeFalse();
        await producer.DidNotReceive().Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>());
    }

    private List<CommandAck> CaptureAcknowledgments()
    {
        var commands = new List<CommandAck>();
        _channel.Send(Arg.Do<CommandAck>(command => commands.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(null));
        return commands;
    }

    private static UnAckedMessageTracker CreateTracker()
        => new(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1), (_, _) => default);

    private static NegativeAcknowledgeTracker CreateNegativeAcknowledgeTracker()
        => new(TimeSpan.FromMinutes(1), (_, _) => default);

    private static (DeadLetterHandler<byte[]> Handler, IProducer<ReadOnlySequence<byte>> Producer) CreateDeadLetterHandler()
    {
        var producer = Substitute.For<IProducer<ReadOnlySequence<byte>>>();
        producer.State.IsFinalState().Returns(false);
        producer.DisposeAsync().Returns(default(ValueTask));
        producer.Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<MessageId>(new MessageId(9, 9, 0, -1)));
        var schemaInfo = new SchemaInfo("schema", Array.Empty<byte>(), SchemaType.String, System.Collections.Immutable.ImmutableDictionary<string, string>.Empty);
        var handler = new DeadLetterHandler<byte[]>(new DeadLetterPolicy(3), _messageId.Topic, "subscription", "consumer", schemaInfo, (_, _) => producer);
        return (handler, producer);
    }

    private static IMessage<byte[]> CreateMessage(MessageId messageId, uint redeliveryCount = 0)
        => new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray).Create(messageId, redeliveryCount, new ReadOnlySequence<byte>([1, 2, 3]), new DotPulsar.Internal.PulsarApi.MessageMetadata { ProducerName = "producer", SequenceId = 1, PublishTime = 1 });

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }
    }

    private static (BatchHandler<byte[]> Handler, MessageFactory<byte[]> Factory) CreateBatchHandler()
    {
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        return (new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, null), messageFactory);
    }

    private static IConnection CreateConnection(out List<CommandAck> sent)
    {
        var acks = new List<CommandAck>();
        sent = acks;
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        connection.Send(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return connection;
    }

    private static ConsumerChannel<byte[]> CreateChannel(IConnection connection, BatchHandler<byte[]> batchHandler, MessageFactory<byte[]> messageFactory, bool batchIndexAckEnabled, bool ackReceiptEnabled = false)
        => new(1, 100, new AsyncQueue<MessagePackage>(), connection, batchHandler, messageFactory, [], BatchTestData.Topic, ackReceiptEnabled, batchIndexAckEnabled, null, null, null, ChunkTestData.CreateAssembler(), null, null, ConsumerCryptoFailureAction.Fail);

    private static Transaction CreateTransaction(out ITransactionCoordinatorClient tcClient)
    {
        tcClient = Substitute.For<ITransactionCoordinatorClient>();
        return new Transaction(tcClient, _transactionId, TimeSpan.FromMinutes(5));
    }

    private static IConnection CreateTransactionalConnection(out List<CommandAck> sent)
    {
        var acks = new List<CommandAck>();
        sent = acks;
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        connection.SendWithResponse(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        return connection;
    }

    private SubConsumer<byte[]> CreateSubConsumer(bool ackReceiptEnabled, TimeSpan? operationTimeout = null, IConsumerChannel<byte[]>? channel = null, SubscriptionType subscriptionType = SubscriptionType.Exclusive, ConsumerEpoch? consumerEpoch = null, IUnAckedMessageTracker? tracker = null, NegativeAcknowledgeTracker? negativeAcknowledgeTracker = null, DeadLetterHandler<byte[]>? deadLetterHandler = null, Func<Func<MessageId[], CancellationToken, ValueTask>, NegativeAcknowledgeTracker>? negativeAcknowledgeTrackerFactory = null, ChunkedMessageAssembler? chunkAssembler = null)
    {
        var correlationId = Guid.NewGuid();
        var executor = new Executor(correlationId, _eventRegister, new DefaultExceptionHandler());
        var state = Substitute.For<IState<ConsumerState>>();
        var factory = Substitute.For<IConsumerChannelFactory<byte[]>>();

        SubConsumer<byte[]>? subConsumer = null;
        subConsumer = new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            subscriptionType,
            "persistent://public/default/test",
            _eventRegister,
            channel ?? _channel,
            executor,
            state,
            factory,
            ackReceiptEnabled,
            operationTimeout ?? TimeSpan.FromSeconds(30),
            consumerEpoch ?? new ConsumerEpoch(),
            tracker,
            negativeAcknowledgeTracker ?? negativeAcknowledgeTrackerFactory?.Invoke((ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct)) ?? new NegativeAcknowledgeTracker(TimeSpan.FromMinutes(1), (_, _) => default),
            deadLetterHandler,
            chunkAssembler ?? ChunkTestData.CreateAssembler(),
            null);

        return subConsumer;
    }

    private static BaseCommand CreateAckResponse(ServerError? error)
    {
        var response = new CommandAckResponse { ConsumerId = 1, RequestId = 1 };

        if (error.HasValue)
        {
            response.Error = error.Value;
            response.Message = "broker message";
        }

        return new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = response };
    }

    private static async Task<BaseCommand?> WaitForever(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return null;
    }

    private static async Task<BaseCommand> WaitForeverForResponse(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return CreateAckResponse(null);
    }
}
