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
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Chunking;
using Microsoft.Extensions.ObjectPool;
using System.Reflection;

// The command of a transactional acknowledgment of one message id is pooled with the commands of the plain acknowledgments (followups/0007).
// It is given back when the acknowledgment has ended, and a command that comes from the pool must not carry anything of its former use.
[Collection("Allocations")]
[Trait("Category", "Unit")]
public sealed class SubConsumerTransactionalAckTests
{
    private const string Topic = "persistent://public/default/test";

    private static readonly MessageId _messageId = new(1, 2, 0, -1, Topic);
    private static readonly TransactionId _transactionId = new(0, 5);

    private readonly IRegisterEvent _eventRegister = Substitute.For<IRegisterEvent>();
    private readonly IConsumerChannel<byte[]> _channel = Substitute.For<IConsumerChannel<byte[]>>();

    public enum Failure
    {
        SendFails,
        ConnectionLost,
        ConflictResponse,
        ConsumerNotFoundResponse,
        NoReceiptWithinTimeout,
        CallerCancellation,
        BrokerWithoutAckReceipts,
        RegistrationFails
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndTransactionalAckAfterBatchMessage_ShouldNotCarryTheAckSetOrTheBatchSizeOfTheBatchMessage()
    {
        //Arrange
        var (batchHandler, messageFactory) = CreateBatchHandler();
        var messages = BatchTestData.AddBatch(batchHandler, 1, 2, 5);
        var connection = CreateTransactionalConnection(out var sent);
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false, channel: CreateChannel(connection, batchHandler, messageFactory));
        await uut.Acknowledge(messages[2].MessageId, transaction, Current.CancellationToken);

        //Act
        await uut.Acknowledge(new MessageId(9, 9, 0, -1, Topic), transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(2);
        sent[0].MessageId[0].BatchSize.ShouldBe(5);
        sent[0].MessageId[0].AckSet.ShouldBeAckSet(0b11011);
        sent[1].MessageId.Count.ShouldBe(1);
        sent[1].MessageId[0].LedgerId.ShouldBe(9UL);
        sent[1].MessageId[0].EntryId.ShouldBe(9UL);
        sent[1].MessageId[0].BatchIndex.ShouldBe(-1);
        sent[1].MessageId[0].HasBatchSize.ShouldBeFalse();
        sent[1].MessageId[0].AckSet.Count.ShouldBe(0);
        sent[1].MessageId[0].FirstChunkMessageId.ShouldBeNull();
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndMessageIdWithBatchSizeAfterOneWithout_ShouldSendTheBatchSizeOnlyWithTheSecond()
    {
        //Arrange
        var sent = new List<(int BatchIndex, bool HasBatchSize, int BatchSize)>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command => sent.Add((command.MessageId[0].BatchIndex, command.MessageId[0].HasBatchSize, command.MessageId[0].BatchSize))), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);
        await uut.Acknowledge(MessageIdInternals.Create(1, 3, 0, 3, Topic, 5), transaction, Current.CancellationToken);
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);

        //Assert
        sent.ShouldBe([(-1, false, 0), (3, true, 5), (-1, false, 0)]);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionalAcksOneAfterTheOther_ShouldReuseTheCommandAndItsMessageId()
    {
        //Arrange
        var commands = new List<CommandAck>();
        var messageIds = new List<MessageIdData>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command =>
        {
            commands.Add(command);
            messageIds.Add(command.MessageId[0]);
        }), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);
        await uut.Acknowledge(new MessageId(1, 3, 0, -1, Topic), transaction, Current.CancellationToken);
        await uut.AcknowledgeCumulative(new MessageId(1, 4, 0, -1, Topic), transaction, Current.CancellationToken);

        //Assert
        commands.Count.ShouldBe(3);
        commands[1].ShouldBeSameAs(commands[0]);
        commands[2].ShouldBeSameAs(commands[0]);
        messageIds[1].ShouldBeSameAs(messageIds[0]);
        messageIds[2].ShouldBeSameAs(messageIds[0]);
        commands[2].MessageId.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionalAcksOfDifferentTypesOneAfterTheOther_ShouldSendEveryAckWithItsOwnTypeAndMessageId()
    {
        //Arrange
        var sent = new List<(CommandAck.Types.AckType AckType, ulong EntryId, ulong TxnidMostBits, ulong TxnidLeastBits)>();
        _channel.SendTransactional(Arg.Do<CommandAck>(command => sent.Add((command.AckType, command.MessageId[0].EntryId, command.TxnidMostBits, command.TxnidLeastBits))), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);

        //Act
        await uut.AcknowledgeCumulative(new MessageId(1, 2, 0, -1, Topic), transaction, Current.CancellationToken);
        await uut.Acknowledge(new MessageId(1, 3, 0, -1, Topic), transaction, Current.CancellationToken);
        await uut.AcknowledgeCumulative(new MessageId(1, 4, 0, -1, Topic), transaction, Current.CancellationToken);

        //Assert
        sent.ShouldBe([
            (CommandAck.Types.AckType.Cumulative, 2UL, _transactionId.MostSigBits, _transactionId.LeastSigBits),
            (CommandAck.Types.AckType.Individual, 3UL, _transactionId.MostSigBits, _transactionId.LeastSigBits),
            (CommandAck.Types.AckType.Cumulative, 4UL, _transactionId.MostSigBits, _transactionId.LeastSigBits)
        ]);
    }

    // The connection assigns the request id of a command that asks for a receipt, and a command that is pooled keeps it
    [Fact]
    public async Task Acknowledge_GivenTransactionAfterAckThatAskedForReceipt_ShouldNotSendTheRequestIdOfTheAckBefore()
    {
        //Arrange
        var requestIds = new List<bool>();
        _channel.Send(Arg.Do<CommandAck>(command => command.RequestId = 77), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(CreateAckResponse(null)));
        _channel.SendTransactional(Arg.Do<CommandAck>(command => requestIds.Add(command.HasRequestId)), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(true);
        await uut.Acknowledge(_messageId, Current.CancellationToken);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);

        //Assert
        requestIds.ShouldBe([false]);
    }

    [Fact]
    public async Task Acknowledge_GivenPlainAckAfterTransactionalAck_ShouldNotCarryTheTransactionOrTheMessageIdOfTheTransactionalAck()
    {
        //Arrange
        var sent = new List<(bool HasTxnMostBits, bool HasTxnLeastBits, ulong EntryId, int AckSetCount, bool HasBatchSize)>();
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            // The channel acknowledges a message of a batch with the ack set of the batch
            var messageId = callInfo.Arg<CommandAck>().MessageId[0];
            messageId.AckSet.Add(3L);
            messageId.BatchSize = 5;
            return CreateAckResponse(null);
        });
        _channel.Send(Arg.Do<CommandAck>(command => sent.Add((command.HasTxnidMostBits, command.HasTxnidLeastBits, command.MessageId[0].EntryId, command.MessageId[0].AckSet.Count, command.MessageId[0].HasBatchSize))), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BaseCommand?>(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);
        await uut.Acknowledge(MessageIdInternals.Create(1, 2, 0, 3, Topic, 5), transaction, Current.CancellationToken);

        //Act
        await uut.Acknowledge(new MessageId(1, 3, 0, -1, Topic), Current.CancellationToken);

        //Assert
        sent.ShouldBe([(false, false, 3UL, 0, false)]);
    }

    [Theory]
    [InlineData(Failure.SendFails)]
    [InlineData(Failure.ConnectionLost)]
    [InlineData(Failure.ConflictResponse)]
    [InlineData(Failure.ConsumerNotFoundResponse)]
    [InlineData(Failure.NoReceiptWithinTimeout)]
    [InlineData(Failure.CallerCancellation)]
    [InlineData(Failure.BrokerWithoutAckReceipts)]
    [InlineData(Failure.RegistrationFails)]
    public async Task Acknowledge_GivenTransactionAndFailure_ShouldGiveTheCommandBackToThePool(Failure failure)
    {
        //Arrange
        var commands = new List<CommandAck>();
        var calls = 0;
        var failing = true;
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            commands.Add(callInfo.Arg<CommandAck>());

            if (!failing)
                return Task.FromResult(CreateAckResponse(null));

            return failure switch
            {
                Failure.SendFails => Task.FromException<BaseCommand>(new InvalidOperationException("failed")),
                Failure.ConnectionLost => ++calls == 1 ? Task.FromException<BaseCommand>(new TaskCanceledException()) : Task.FromException<BaseCommand>(new InvalidOperationException("failed")),
                Failure.ConflictResponse => Task.FromResult(CreateAckResponse(ServerError.TransactionConflict)),
                Failure.ConsumerNotFoundResponse => Task.FromResult(CreateAckResponse(ServerError.ConsumerNotFound)),
                Failure.NoReceiptWithinTimeout or Failure.CallerCancellation => WaitForeverForResponse(callInfo.Arg<CancellationToken>()),
                Failure.BrokerWithoutAckReceipts => Task.FromException<BaseCommand>(new NotSupportedException("protocol version")),
                _ => Task.FromResult(CreateAckResponse(null))
            };
        });
        var transaction = CreateTransaction(out var tcClient);

        if (failure == Failure.RegistrationFails)
            tcClient.AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new InvalidOperationException("registration failed")));

        await using var uut = CreateSubConsumer(false, failure == Failure.NoReceiptWithinTimeout ? TimeSpan.FromMilliseconds(100) : null);
        var pool = ReplacePool(uut);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);

        if (failure == Failure.CallerCancellation)
            cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, cts.Token));
        var getsAfterFailure = pool.Gets;
        var outstandingAfterFailure = pool.Outstanding;

        // The next acknowledgment gets the command back, and it is a clean one
        failing = false;
        await uut.Acknowledge(new MessageId(1, 3, 0, -1, Topic), CreateTransaction(out _), Current.CancellationToken);

        //Assert
        exception.ShouldNotBeNull();
        getsAfterFailure.ShouldBe(1);
        outstandingAfterFailure.ShouldBe(0);
        pool.Outstanding.ShouldBe(0);

        // A registration that fails ends the acknowledgment before the command is sent
        if (failure != Failure.RegistrationFails)
            commands[^1].ShouldBeSameAs(commands[0]);
    }

    // The acknowledgment is an operation of the transaction that begins and ends in the acknowledgment itself (and not in a continuation of its task)
    [Theory]
    [InlineData(null, true)]
    [InlineData(ServerError.TransactionConflict, false)]
    public async Task Commit_GivenTransactionalAckThatHasEnded_ShouldNotWaitForItAnymore(ServerError? error, bool expectCommitted)
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(error));
        var transaction = CreateTransaction(out var tcClient);
        await using var uut = CreateSubConsumer(false);
        await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        //Act
        var exception = await Record.ExceptionAsync(async () => await transaction.Commit(cts.Token));

        //Assert
        if (expectCommitted)
        {
            exception.ShouldBeNull();
            transaction.CurrentState.ShouldBe(TransactionState.Committed);
            await tcClient.Received(1).Commit(_transactionId, CancellationToken.None);
        }
        else
        {
            exception.ShouldBeOfType<TransactionHasOperationFailedException>();
            transaction.CurrentState.ShouldBe(TransactionState.Aborted);
            await tcClient.Received(1).Abort(_transactionId, CancellationToken.None);
        }
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndDisposedConsumer_ShouldThrowConsumerDisposedExceptionAndGiveTheCommandBackWithoutSending()
    {
        //Arrange
        var transaction = CreateTransaction(out _);
        var uut = CreateSubConsumer(false);
        var pool = ReplacePool(uut);
        await uut.DisposeAsync();

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ConsumerDisposedException>();
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        pool.Gets.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndFaultedConsumer_ShouldThrowTheFaultAndGiveTheCommandBackWithoutSending()
    {
        //Arrange
        var fault = new InvalidOperationException("the channel faulted");
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);
        var pool = ReplacePool(uut);
        await uut.ChannelFaulted(fault);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.AcknowledgeCumulative(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeSameAs(fault);
        await _channel.DidNotReceive().SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>());
        pool.Gets.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndConnectionLostWhileWaiting_ShouldRetryWithTheSameCommandAndGiveItBackOnce()
    {
        //Arrange
        var commands = new List<CommandAck>();
        var calls = 0;
        _channel.SendTransactional(Arg.Do<CommandAck>(commands.Add), Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1 ? Task.FromException<BaseCommand>(new TaskCanceledException()) : Task.FromResult(CreateAckResponse(null)));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);
        var pool = ReplacePool(uut);

        //Act
        await uut.Acknowledge(_messageId, transaction, Current.CancellationToken);

        //Assert
        commands.Count.ShouldBe(2);
        commands[1].ShouldBeSameAs(commands[0]);
        pool.Gets.ShouldBe(1);
        pool.Returns.ShouldBe(1);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndReceiptThatHasNotArrived_ShouldKeepTheCommandUntilTheAcknowledgmentHasEnded()
    {
        //Arrange
        var receipts = new List<TaskCompletionSource<BaseCommand>>();
        var sent = new List<(CommandAck Command, ulong EntryId)>();
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            var receipt = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
            var command = callInfo.Arg<CommandAck>();

            lock (receipts)
            {
                receipts.Add(receipt);
                sent.Add((command, command.MessageId[0].EntryId));
            }

            return receipt.Task;
        });
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);
        var pool = ReplacePool(uut);

        //Act
        var first = uut.Acknowledge(new MessageId(1, 2, 0, -1, Topic), transaction, Current.CancellationToken).AsTask();
        var second = uut.Acknowledge(new MessageId(1, 3, 0, -1, Topic), transaction, Current.CancellationToken).AsTask();
        await WaitUntil(() => { lock (receipts) { return receipts.Count == 2; } });
        var outstandingWhileWaiting = pool.Outstanding;
        var returnsWhileWaiting = pool.Returns;
        var entryIdsWhileWaiting = sent.Select(s => s.Command.MessageId[0].EntryId).ToList();
        receipts[0].SetResult(CreateAckResponse(null));
        await first;
        var outstandingAfterFirst = pool.Outstanding;
        receipts[1].SetResult(CreateAckResponse(null));
        await second;

        //Assert
        outstandingWhileWaiting.ShouldBe(2);
        returnsWhileWaiting.ShouldBe(0);
        sent[1].Command.ShouldNotBeSameAs(sent[0].Command);
        entryIdsWhileWaiting.ShouldBe([2UL, 3UL]);
        outstandingAfterFirst.ShouldBe(1);
        pool.Outstanding.ShouldBe(0);
    }

    // The acknowledgments of a list of message ids, which can have any size, do not use the pool
    [Fact]
    public async Task Acknowledge_GivenTransactionAndSeveralMessageIds_ShouldNotUseThePool()
    {
        //Arrange
        _channel.SendTransactional(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        var transaction = CreateTransaction(out _);
        await using var uut = CreateSubConsumer(false);
        var pool = ReplacePool(uut);

        //Act
        await uut.Acknowledge([new MessageId(1, 2, 0, -1, Topic), new MessageId(1, 3, 0, -1, Topic)], transaction, Current.CancellationToken);

        //Assert
        pool.Gets.ShouldBe(0);
        pool.Returns.ShouldBe(0);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionThatIsNotOpen_ShouldNotTakeACommandFromThePool()
    {
        //Arrange
        var transaction = CreateTransaction(out _);
        await transaction.Abort(Current.CancellationToken);
        await using var uut = CreateSubConsumer(false);
        var pool = ReplacePool(uut);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, transaction, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>();
        pool.Gets.ShouldBe(0);
    }

    // The steady state of a transactional acknowledgment: the registration with the transaction has been done, so nothing is left but the receipt and what bounds it.
    // The acknowledgment that waits for a receipt without a transaction is the fair comparison, as it has the same receipt and the same time limit.
    // The spec allows what the transaction adds to be as much as a continuation (64-112 B), and it was 536 B with a new command for every acknowledgment. It is 8 B now
    // (the closure of the executor of a transactional acknowledgment is a little bigger than the one of a plain acknowledgment), and every allocation that can come back
    // is bigger than the 32 B that this allows: the command (152 B), its message id (104 B), the array of its ack set (88 B), a result task (72 B) and a closure (48 B).
    [Fact]
    public async Task Acknowledge_GivenTransaction_ShouldAllocateAtMostOneContinuationMoreThanAcknowledgmentThatWaitsForReceipt()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var transaction = CreateTransaction(out _);
        await using var receiptConsumer = CreateSubConsumer(true, channel: CreateRealChannel(true));
        await using var transactionConsumer = CreateSubConsumer(false, channel: CreateRealChannel(false));
        await transactionConsumer.Acknowledge(_messageId, transaction, CancellationToken.None);

        //Act
        var receipt = await AllocationMeasurement.BytesPerOperation(async () => await receiptConsumer.Acknowledge(_messageId, CancellationToken.None));
        var transactional = await AllocationMeasurement.BytesPerOperation(async () => await transactionConsumer.Acknowledge(_messageId, transaction, CancellationToken.None), receipt + 32);

        //Assert
        (transactional - receipt).ShouldBeLessThanOrEqualTo(32);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndBatchMessage_ShouldAllocateAtMostOneContinuationMoreThanAcknowledgmentThatWaitsForReceipt()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var batchMessageId = MessageIdInternals.Create(1, 2, 0, 5, Topic, 64);
        var transaction = CreateTransaction(out _);
        await using var receiptConsumer = CreateSubConsumer(true, channel: CreateRealChannel(true));
        await using var transactionConsumer = CreateSubConsumer(false, channel: CreateRealChannel(false));
        await transactionConsumer.Acknowledge(batchMessageId, transaction, CancellationToken.None);

        //Act
        var receipt = await AllocationMeasurement.BytesPerOperation(async () => await receiptConsumer.Acknowledge(_messageId, CancellationToken.None));
        var transactional = await AllocationMeasurement.BytesPerOperation(async () => await transactionConsumer.Acknowledge(batchMessageId, transaction, CancellationToken.None), receipt + 32);

        //Assert
        (transactional - receipt).ShouldBeLessThanOrEqualTo(32);
    }

    [Fact]
    public async Task AcknowledgeCumulative_GivenTransaction_ShouldAllocateAtMostOneContinuationMoreThanAcknowledgmentThatWaitsForReceipt()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var transaction = CreateTransaction(out _);
        await using var receiptConsumer = CreateSubConsumer(true, channel: CreateRealChannel(true));
        await using var transactionConsumer = CreateSubConsumer(false, channel: CreateRealChannel(false));
        await transactionConsumer.AcknowledgeCumulative(_messageId, transaction, CancellationToken.None);

        //Act
        var receipt = await AllocationMeasurement.BytesPerOperation(async () => await receiptConsumer.AcknowledgeCumulative(_messageId, CancellationToken.None));
        var transactional = await AllocationMeasurement.BytesPerOperation(async () => await transactionConsumer.AcknowledgeCumulative(_messageId, transaction, CancellationToken.None), receipt + 32);

        //Assert
        (transactional - receipt).ShouldBeLessThanOrEqualTo(32);
    }

    // The receipt of a broker arrives on another thread, so the acknowledgment has to wait for it, and every async method that waits allocates its state machine.
    // What the transaction may add is the continuation that tells the transaction that the acknowledgment has ended (64-112 B, if it were needed). It is 0-30 B (it is not needed any more,
    // and the numbers of this test change by that much from run to run). It was 972 B with a command for every acknowledgment, a continuation, and an async method for the transaction.
    [Fact]
    public async Task Acknowledge_GivenTransactionAndReceiptOnThreadPool_ShouldAllocateAtMostOneContinuationMoreThanAcknowledgmentThatWaitsForReceipt()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var transaction = CreateTransaction(out _);
        await using var receiptConsumer = CreateSubConsumer(true, channel: CreateRealChannel(true, true));
        await using var transactionConsumer = CreateSubConsumer(false, channel: CreateRealChannel(false, true));
        await transactionConsumer.Acknowledge(_messageId, transaction, CancellationToken.None);

        //Act
        var difference = await SmallestDifferenceOnThreadPool(
            async () => await receiptConsumer.Acknowledge(_messageId, CancellationToken.None),
            async () => await transactionConsumer.Acknowledge(_messageId, transaction, CancellationToken.None));

        //Assert
        difference.ShouldBeLessThanOrEqualTo(64);
    }

    [Fact]
    public async Task Acknowledge_GivenTransactionAndBatchMessageAndReceiptOnThreadPool_ShouldAllocateAtMostOneContinuationMoreThanAcknowledgmentThatWaitsForReceipt()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var batchMessageId = MessageIdInternals.Create(1, 2, 0, 5, Topic, 64);
        var transaction = CreateTransaction(out _);
        await using var receiptConsumer = CreateSubConsumer(true, channel: CreateRealChannel(true, true));
        await using var transactionConsumer = CreateSubConsumer(false, channel: CreateRealChannel(false, true));
        await transactionConsumer.Acknowledge(batchMessageId, transaction, CancellationToken.None);

        //Act
        var difference = await SmallestDifferenceOnThreadPool(
            async () => await receiptConsumer.Acknowledge(_messageId, CancellationToken.None),
            async () => await transactionConsumer.Acknowledge(batchMessageId, transaction, CancellationToken.None));

        //Assert
        difference.ShouldBeLessThanOrEqualTo(64);
    }

    // The allocations of operations that wait for a receipt on the thread pool change by up to about 30 B from one measurement to the next (and by 100 B once in a while, more often when the
    // machine is busy), as the timing of the continuations does. A difference that comes from the code is there in every pair of measurements, so the smallest of up to ten pairs is the one that
    // counts. The pairs are only measured as long as the difference is too large.
    private static async Task<double> SmallestDifferenceOnThreadPool(Func<ValueTask> receiptOperation, Func<ValueTask> transactionOperation)
    {
        var smallest = double.MaxValue;

        for (var attempt = 0; attempt < 10 && smallest > 32; ++attempt)
        {
            var receipt = await AllocationMeasurement.BytesPerOperationOfAllThreads(receiptOperation);
            var transactional = await AllocationMeasurement.BytesPerOperationOfAllThreads(transactionOperation, receipt + 32);
            smallest = Math.Min(smallest, transactional - receipt);
        }

        return smallest;
    }

    private ConsumerChannel<byte[]> CreateRealChannel(bool ackReceiptEnabled, bool completeOnThreadPool = false)
    {
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        var batchHandler = new BatchHandler<byte[]>(true, messageFactory, Topic, null);
        return CreateChannel(new FakeAckConnection(completeOnThreadPool), batchHandler, messageFactory, ackReceiptEnabled);
    }

    private static CountingPool ReplacePool(SubConsumer<byte[]> subConsumer)
    {
        var field = typeof(SubConsumer<byte[]>).GetField("_commandAckPool", BindingFlags.Instance | BindingFlags.NonPublic) ?? throw new InvalidOperationException("SubConsumer._commandAckPool was not found");
        var pool = new CountingPool();
        field.SetValue(subConsumer, pool);
        return pool;
    }

    private static (BatchHandler<byte[]> Handler, MessageFactory<byte[]> Factory) CreateBatchHandler()
    {
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        return (new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, null), messageFactory);
    }

    private static ConsumerChannel<byte[]> CreateChannel(IConnection connection, BatchHandler<byte[]> batchHandler, MessageFactory<byte[]> messageFactory, bool ackReceiptEnabled = false)
        => new(1, 100, new AsyncQueue<MessagePackage>(), connection, batchHandler, messageFactory, [], Topic, ackReceiptEnabled, false, null, null, null, ChunkTestData.CreateAssembler(), null, null, ConsumerCryptoFailureAction.Fail);

    private static IConnection CreateTransactionalConnection(out List<CommandAck> sent)
    {
        var acks = new List<CommandAck>();
        sent = acks;
        var connection = Substitute.For<IConnection>();
        connection.ProtocolVersion.Returns(19);
        connection.SendWithResponse(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(CreateAckResponse(null));
        return connection;
    }

    private static Transaction CreateTransaction(out ITransactionCoordinatorClient tcClient)
    {
        tcClient = Substitute.For<ITransactionCoordinatorClient>();
        return new Transaction(tcClient, _transactionId, TimeSpan.FromMinutes(5));
    }

    private SubConsumer<byte[]> CreateSubConsumer(bool ackReceiptEnabled, TimeSpan? operationTimeout = null, IConsumerChannel<byte[]>? channel = null)
    {
        var correlationId = Guid.NewGuid();

        return new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            SubscriptionType.Exclusive,
            Topic,
            _eventRegister,
            channel ?? _channel,
            new Executor(correlationId, _eventRegister, new DefaultExceptionHandler()),
            Substitute.For<IState<ConsumerState>>(),
            Substitute.For<IConsumerChannelFactory<byte[]>>(),
            ackReceiptEnabled,
            operationTimeout ?? TimeSpan.FromSeconds(30),
            new ConsumerEpoch(),
            null,
            new NegativeAcknowledgeTracker(TimeSpan.FromMinutes(1), (_, _) => default),
            null,
            ChunkTestData.CreateAssembler(),
            null);
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

    private static async Task<BaseCommand> WaitForeverForResponse(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return CreateAckResponse(null);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }
    }

    // Counts what is taken from the pool and what is given back
    private sealed class CountingPool : ObjectPool<CommandAck>
    {
        private readonly ObjectPool<CommandAck> _pool = new DefaultObjectPool<CommandAck>(new DefaultPooledObjectPolicy<CommandAck>());
        private int _gets;
        private int _returns;

        public int Gets => Volatile.Read(ref _gets);
        public int Returns => Volatile.Read(ref _returns);
        public int Outstanding => Gets - Returns;

        public override CommandAck Get()
        {
            _ = Interlocked.Increment(ref _gets);
            return _pool.Get();
        }

        public override void Return(CommandAck obj)
        {
            _ = Interlocked.Increment(ref _returns);
            _pool.Return(obj);
        }
    }
}
