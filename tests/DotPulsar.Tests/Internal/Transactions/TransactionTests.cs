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

namespace DotPulsar.Tests.Internal.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class TransactionTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TransactionId _id = new(0, 5);

    // Additional coverage
    [Fact]
    public void Constructor_ShouldCreateOpenTransaction()
    {
        //Act
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());

        //Assert
        uut.Id.ShouldBe(_id);
        uut.CurrentState.ShouldBe(TransactionState.Open);
        uut.IsOpen.ShouldBeTrue();
        uut.State.IsFinalState().ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenSameTopicTwice_ShouldSendOneRequest()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);

        //Act
        await uut.RegisterProducedTopic("topic", Current.CancellationToken);
        await uut.RegisterProducedTopic("topic", Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddPublishPartitionToTxn(_id, "topic", CancellationToken.None);
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenDifferentTopics_ShouldSendOneRequestPerTopic()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);

        //Act
        await uut.RegisterProducedTopic("topic-partition-0", Current.CancellationToken);
        await uut.RegisterProducedTopic("topic-partition-1", Current.CancellationToken);
        await uut.RegisterProducedTopic("topic-partition-0", Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddPublishPartitionToTxn(_id, "topic-partition-0", CancellationToken.None);
        await tcClient.Received(1).AddPublishPartitionToTxn(_id, "topic-partition-1", CancellationToken.None);
        await tcClient.Received(2).AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenConcurrentCallers_ShouldSendOneRequest()
    {
        //Arrange
        var registration = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask(registration.Task));
        var uut = CreateTransaction(tcClient);
        var callers = Enumerable.Range(0, 20).Select(_ => Task.Run(() => uut.RegisterProducedTopic("topic", Current.CancellationToken).AsTask())).ToArray();

        //Act
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        registration.SetResult(null);
        await Task.WhenAll(callers).WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenCompletedRegistration_ShouldReturnCompletedValueTask()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        await uut.RegisterProducedTopic("topic", Current.CancellationToken);

        //Act
        var result = uut.RegisterProducedTopic("topic", Current.CancellationToken);

        //Assert
        result.IsCompletedSuccessfully.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenFailedRegistration_ShouldFailLaterCallsWithSameException()
    {
        //Arrange
        var exception = new TransactionCoordinatorClientException("failed");
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(exception));
        var uut = CreateTransaction(tcClient);

        //Act
        var first = await Record.ExceptionAsync(() => uut.RegisterProducedTopic("topic", Current.CancellationToken).AsTask());
        var second = await Record.ExceptionAsync(() => uut.RegisterProducedTopic("topic", Current.CancellationToken).AsTask());

        //Assert
        first.ShouldBeSameAs(exception);
        second.ShouldBeSameAs(exception);
        await tcClient.Received(1).AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenCancellationWhileRegistering_ShouldStopWaitingButKeepTheRegistration()
    {
        //Arrange
        var registration = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask(registration.Task));
        var uut = CreateTransaction(tcClient);
        using var cts = new CancellationTokenSource();
        var cancelled = uut.RegisterProducedTopic("topic", cts.Token).AsTask();

        //Act
        await cts.CancelAsync();
        var exception = await Record.ExceptionAsync(() => cancelled.WaitAsync(_timeout, Current.CancellationToken));
        var other = uut.RegisterProducedTopic("topic", Current.CancellationToken).AsTask();
        registration.SetResult(null);
        await other.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        await tcClient.Received(1).AddPublishPartitionToTxn(_id, "topic", CancellationToken.None);
    }

    // Additional coverage
    [Fact]
    public async Task RegisterProducedTopic_GivenStateNotOpen_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient, TimeSpan.FromMilliseconds(50));
        await uut.State.OnStateChangeTo(TransactionState.TimedOut, Current.CancellationToken);

        //Act
        var exception = Record.Exception(() => uut.RegisterProducedTopic("topic", Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[(0,5)] with unexpected state : TimedOut, expect Open state!");
        await tcClient.DidNotReceive().AddPublishPartitionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task RegisterAckedTopic_GivenSameTopicAndSubscriptionTwice_ShouldSendOneRequest()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);

        //Act
        await uut.RegisterAckedTopic("topic", "subscription", Current.CancellationToken);
        await uut.RegisterAckedTopic("topic", "subscription", Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddSubscriptionToTxn(_id, "topic", "subscription", CancellationToken.None);
        await tcClient.Received(1).AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task RegisterAckedTopic_GivenDifferentSubscription_ShouldSendAnotherRequest()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);

        //Act
        await uut.RegisterAckedTopic("topic", "subscription-1", Current.CancellationToken);
        await uut.RegisterAckedTopic("topic", "subscription-2", Current.CancellationToken);
        await uut.RegisterAckedTopic("other-topic", "subscription-1", Current.CancellationToken);

        //Assert
        await tcClient.Received(1).AddSubscriptionToTxn(_id, "topic", "subscription-1", CancellationToken.None);
        await tcClient.Received(1).AddSubscriptionToTxn(_id, "topic", "subscription-2", CancellationToken.None);
        await tcClient.Received(1).AddSubscriptionToTxn(_id, "other-topic", "subscription-1", CancellationToken.None);
    }

    // Additional coverage
    [Fact]
    public async Task RegisterAckedTopic_GivenFailedRegistration_ShouldFailLaterCallsWithSameException()
    {
        //Arrange
        var exception = new TransactionCoordinatorClientException("failed");
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(exception));
        var uut = CreateTransaction(tcClient);

        //Act
        var first = await Record.ExceptionAsync(() => uut.RegisterAckedTopic("topic", "subscription", Current.CancellationToken).AsTask());
        var second = await Record.ExceptionAsync(() => uut.RegisterAckedTopic("topic", "subscription", Current.CancellationToken).AsTask());

        //Assert
        first.ShouldBeSameAs(exception);
        second.ShouldBeSameAs(exception);
        await tcClient.Received(1).AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task RegisterAckedTopic_GivenStateNotOpen_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        await uut.Commit(Current.CancellationToken);

        //Act
        var exception = Record.Exception(() => uut.RegisterAckedTopic("topic", "subscription", Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[(0,5)] with unexpected state : Committed, expect Open state!");
        await tcClient.DidNotReceive().AddSubscriptionToTxn(Arg.Any<TransactionId>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public void EnsureOpen_GivenOpenTransaction_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());

        //Act
        var exception = Record.Exception(uut.EnsureOpen);

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task EnsureOpen_GivenAbortedTransaction_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        await uut.Abort(Current.CancellationToken);

        //Act
        var exception = Record.Exception(uut.EnsureOpen);

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[(0,5)] with unexpected state : Aborted, expect Open state!");
        uut.IsOpen.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void From_GivenTransaction_ShouldReturnTheTransaction()
    {
        //Arrange
        ITransaction uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());

        //Act
        var result = Transaction.From(uut);

        //Assert
        result.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void From_GivenTransactionNotCreatedByDotPulsar_ShouldThrowArgumentException()
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();

        //Act
        var exception = Record.Exception(() => Transaction.From(transaction));

        //Assert
        exception.ShouldBeOfType<ArgumentException>().Message.ShouldStartWith("The transaction must be created by DotPulsar");
        ((ArgumentException) exception).ParamName.ShouldBe("transaction");
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenPendingOps_ShouldWaitForOpsBeforeEndTxn()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        var send = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledge = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterSendOp(send.Task);
        uut.RegisterAckOp(acknowledge.Task);

        //Act
        var commit = uut.Commit(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var stateWhileWaiting = uut.CurrentState;
        send.SetResult(null);
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedWithOnePendingOp = commit.IsCompleted;
        acknowledge.SetResult(null);
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        stateWhileWaiting.ShouldBe(TransactionState.Committing);
        isCompletedWithOnePendingOp.ShouldBeFalse();
        uut.CurrentState.ShouldBe(TransactionState.Committed);
        await tcClient.Received(1).Commit(_id, CancellationToken.None);
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenNoOps_ShouldSetCommitted()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);

        //Act
        await uut.Commit(Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
        uut.State.IsFinalState().ShouldBeTrue();
        await tcClient.Received(1).Commit(_id, CancellationToken.None);
        await tcClient.DidNotReceive().Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenCompletedOps_ShouldSetCommitted()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.RegisterSendOp(Task.CompletedTask);
        uut.RegisterAckOp(Task.CompletedTask);

        //Act
        await uut.Commit(Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Upstream: TransactionImpl#commit (hasOpsFailed)
    [Fact]
    public async Task Commit_GivenFailedOp_ShouldAbortAndThrowTransactionHasOperationFailedException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.RegisterSendOp(Task.FromException(new TransactionConflictException("conflict")));
        uut.RegisterAckOp(Task.CompletedTask);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
        await tcClient.Received(1).Abort(_id, CancellationToken.None);
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenOpThatFailsWhileCommitIsWaiting_ShouldAbortAndThrowTransactionHasOperationFailedException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        var acknowledge = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterAckOp(acknowledge.Task);
        var commit = uut.Commit(Current.CancellationToken).AsTask();

        //Act
        acknowledge.SetException(new TransactionConflictException("conflict"));
        var exception = await Record.ExceptionAsync(() => commit.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenCancelledOp_ShouldBeTreatedAsFailedOp()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.RegisterSendOp(Task.FromCanceled(new CancellationToken(true)));

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenFailedOpAndAbortFails_ShouldStillThrowTransactionHasOperationFailedException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new TransactionNotFoundException("not found")));
        var uut = CreateTransaction(tcClient);
        uut.RegisterSendOp(Task.FromException(new TransactionConflictException("conflict")));

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        uut.CurrentState.ShouldBe(TransactionState.Error);
    }

    // Additional coverage
    [Theory]
    [InlineData(typeof(TransactionNotFoundException))]
    [InlineData(typeof(InvalidTransactionStatusException))]
    public async Task Commit_GivenTransactionNotFoundOrInvalidStatus_ShouldSetErrorState(Type exceptionType)
    {
        //Arrange
        var coordinatorException = (Exception) Activator.CreateInstance(exceptionType, "the message")!;
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(coordinatorException));
        var uut = CreateTransaction(tcClient);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeSameAs(coordinatorException);
        uut.CurrentState.ShouldBe(TransactionState.Error);
        uut.State.IsFinalState().ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenOtherError_ShouldKeepCommittingState()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new OperationTimeoutException("timeout")));
        var uut = CreateTransaction(tcClient);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        uut.CurrentState.ShouldBe(TransactionState.Committing);
    }

    // Upstream: TransactionClientCloseTest#testEndTransactionAfterClientClose
    [Fact]
    public async Task Commit_GivenTransactionCoordinatorClientNotReady_ShouldKeepCommittingState()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new TransactionCoordinatorClientStateException("closed")));
        var uut = CreateTransaction(tcClient);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<TransactionCoordinatorClientStateException>();
        uut.CurrentState.ShouldBe(TransactionState.Committing);
    }

    // Upstream: TransactionTest#testEndTxnWhenCommittingOrAborting
    [Fact]
    public async Task Commit_GivenCommittingState_ShouldSendEndTxnAgain()
    {
        //Arrange
        var calls = 0;
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1 ? ValueTask.FromException(new OperationTimeoutException("timeout")) : ValueTask.CompletedTask);
        var uut = CreateTransaction(tcClient);
        _ = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Act
        await uut.Commit(Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
        await tcClient.Received(2).Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenTwoConcurrentCommits_ShouldSendEndTxnTwice()
    {
        //Arrange
        var response = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(new ValueTask(response.Task));
        var uut = CreateTransaction(tcClient);

        //Act
        var first = uut.Commit(Current.CancellationToken).AsTask();
        var second = uut.Commit(Current.CancellationToken).AsTask();
        response.SetResult(null);
        await Task.WhenAll(first, second).WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
        await tcClient.Received(2).Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Theory]
    [InlineData(TransactionState.Committed)]
    [InlineData(TransactionState.Aborted)]
    [InlineData(TransactionState.Error)]
    [InlineData(TransactionState.TimedOut)]
    public async Task Commit_GivenFinalState_ShouldThrowInvalidTransactionStatusException(TransactionState state)
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = await CreateTransactionInState(tcClient, state);
        tcClient.ClearReceivedCalls();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe($"[0:5] with unexpected state: {state}, expect: [Open, Committing]");
        uut.CurrentState.ShouldBe(state);
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenAbortingState_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new OperationTimeoutException("timeout")));
        var uut = CreateTransaction(tcClient);
        _ = await Record.ExceptionAsync(() => uut.Abort(Current.CancellationToken).AsTask());

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[0:5] with unexpected state: Aborting, expect: [Open, Committing]");
        uut.CurrentState.ShouldBe(TransactionState.Aborting);
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenCallerCancels_ShouldNotChangeStateAndAllowRetry()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        var send = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterSendOp(send.Task);
        using var cts = new CancellationTokenSource();
        var cancelled = uut.Commit(cts.Token).AsTask();

        //Act
        await cts.CancelAsync();
        var exception = await Record.ExceptionAsync(() => cancelled.WaitAsync(_timeout, Current.CancellationToken));
        var stateAfterCancel = uut.CurrentState;
        send.SetResult(null);
        await uut.Commit(Current.CancellationToken);

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        stateAfterCancel.ShouldBe(TransactionState.Committing);
        uut.CurrentState.ShouldBe(TransactionState.Committed);
        await tcClient.Received(1).Commit(_id, CancellationToken.None);
    }

    // Upstream: TransactionTest#testCancelTxnTimeout
    [Fact]
    public async Task Commit_ShouldCancelTimer()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>(), TimeSpan.FromMilliseconds(200));

        //Act
        await uut.Commit(Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(600), Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task Abort_GivenOpenTransaction_ShouldSetAborted()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);

        //Act
        await uut.Abort(Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
        uut.State.IsFinalState().ShouldBeTrue();
        await tcClient.Received(1).Abort(_id, CancellationToken.None);
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Abort_GivenPendingOps_ShouldWaitForOpsBeforeEndTxn()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        var send = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterSendOp(send.Task);

        //Act
        var abort = uut.Abort(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var stateWhileWaiting = uut.CurrentState;
        await tcClient.DidNotReceive().Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        send.SetResult(null);
        await abort.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        stateWhileWaiting.ShouldBe(TransactionState.Aborting);
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Additional coverage
    [Fact]
    public async Task Abort_GivenFailedOp_ShouldIgnoreItAndAbort()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.RegisterSendOp(Task.FromException(new TransactionConflictException("conflict")));

        //Act
        await uut.Abort(Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
        await tcClient.Received(1).Abort(_id, CancellationToken.None);
    }

    // Additional coverage
    [Theory]
    [InlineData(typeof(TransactionNotFoundException))]
    [InlineData(typeof(InvalidTransactionStatusException))]
    public async Task Abort_GivenTransactionNotFoundOrInvalidStatus_ShouldSetErrorState(Type exceptionType)
    {
        //Arrange
        var coordinatorException = (Exception) Activator.CreateInstance(exceptionType, "the message")!;
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(coordinatorException));
        var uut = CreateTransaction(tcClient);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Abort(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeSameAs(coordinatorException);
        uut.CurrentState.ShouldBe(TransactionState.Error);
    }

    // Additional coverage
    [Fact]
    public async Task Abort_GivenOtherError_ShouldKeepAbortingStateAndAllowAbortingAgain()
    {
        //Arrange
        var calls = 0;
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1 ? ValueTask.FromException(new OperationTimeoutException("timeout")) : ValueTask.CompletedTask);
        var uut = CreateTransaction(tcClient);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Abort(Current.CancellationToken).AsTask());
        var stateAfterError = uut.CurrentState;
        await uut.Abort(Current.CancellationToken);

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        stateAfterError.ShouldBe(TransactionState.Aborting);
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Additional coverage
    [Theory]
    [InlineData(TransactionState.Committed)]
    [InlineData(TransactionState.Aborted)]
    [InlineData(TransactionState.Error)]
    [InlineData(TransactionState.TimedOut)]
    public async Task Abort_GivenFinalState_ShouldThrowInvalidTransactionStatusException(TransactionState state)
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = await CreateTransactionInState(tcClient, state);
        tcClient.ClearReceivedCalls();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Abort(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe($"[0:5] with unexpected state: {state}, expect: [Open, Aborting]");
        uut.CurrentState.ShouldBe(state);
        await tcClient.DidNotReceive().Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Abort_GivenCommittingState_ShouldThrowInvalidTransactionStatusException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new OperationTimeoutException("timeout")));
        var uut = CreateTransaction(tcClient);
        _ = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Abort(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[0:5] with unexpected state: Committing, expect: [Open, Aborting]");
    }

    // Additional coverage
    [Fact]
    public async Task Abort_ShouldCancelTimer()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>(), TimeSpan.FromMilliseconds(200));

        //Act
        await uut.Abort(Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(600), Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Upstream: TransactionImpl#run
    [Fact]
    public async Task Timeout_GivenElapsed_ShouldSetTimedOutAndRejectCommitAndAbort()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient, TimeSpan.FromMilliseconds(50));

        //Act
        var state = await uut.State.OnStateChangeTo(TransactionState.TimedOut, Current.CancellationToken);
        var commitException = await Record.ExceptionAsync(() => uut.Commit(Current.CancellationToken).AsTask());
        var abortException = await Record.ExceptionAsync(() => uut.Abort(Current.CancellationToken).AsTask());

        //Assert
        state.ShouldBe(TransactionState.TimedOut);
        uut.CurrentState.ShouldBe(TransactionState.TimedOut);
        uut.State.IsFinalState().ShouldBeTrue();
        commitException.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[0:5] with unexpected state: TimedOut, expect: [Open, Committing]");
        abortException.ShouldBeOfType<InvalidTransactionStatusException>().Message.ShouldBe("[0:5] with unexpected state: TimedOut, expect: [Open, Aborting]");
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        await tcClient.DidNotReceive().Abort(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Timeout_GivenCommitting_ShouldNotChangeState()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient, TimeSpan.FromMilliseconds(100));
        var send = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterSendOp(send.Task);
        var commit = uut.Commit(Current.CancellationToken).AsTask();

        //Act
        await Task.Delay(TimeSpan.FromMilliseconds(400), Current.CancellationToken);
        var stateAfterTimeout = uut.CurrentState;
        send.SetResult(null);
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        stateAfterTimeout.ShouldBe(TransactionState.Committing);
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenTimeoutLongerThanTheTimerSupports_ShouldNotThrow()
    {
        //Act
        var exception = Record.Exception(() => CreateTransaction(Substitute.For<ITransactionCoordinatorClient>(), TimeSpan.FromDays(365)));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task State_GivenCommit_ShouldNotifyOnStateChangeTo()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        var committing = uut.State.OnStateChangeTo(TransactionState.Committing, Current.CancellationToken).AsTask();
        var committed = uut.State.OnStateChangeTo(TransactionState.Committed, Current.CancellationToken).AsTask();

        //Act
        await uut.Commit(Current.CancellationToken);

        //Assert
        (await committing.WaitAsync(_timeout, Current.CancellationToken)).ShouldBe(TransactionState.Committing);
        (await committed.WaitAsync(_timeout, Current.CancellationToken)).ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task State_GivenAbort_ShouldNotifyOnStateChangeFromOpen()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        var changed = uut.State.OnStateChangeFrom(TransactionState.Open, Current.CancellationToken).AsTask();

        //Act
        await uut.Abort(Current.CancellationToken);

        //Assert
        var state = await changed.WaitAsync(_timeout, Current.CancellationToken);
        state.ShouldNotBe(TransactionState.Open);
    }

    // Upstream: TransactionTest#testAsyncSendOrAckForSingleFuture
    [Fact]
    public async Task RegisterSendOp_GivenConcurrentOps_ShouldCompleteWhenAllDone()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        var ops = Enumerable.Range(0, 30).Select(_ => new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();

        foreach (var op in ops)
            uut.RegisterSendOp(op.Task);

        var commit = uut.Commit(Current.CancellationToken).AsTask();

        //Act
        await Task.WhenAll(ops.Skip(1).Select(op => Task.Run(() => op.SetResult(null))));
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedWithOnePendingOp = commit.IsCompleted;
        ops[0].SetResult(null);
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isCompletedWithOnePendingOp.ShouldBeFalse();
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task RegisterAckOp_GivenOpsCompletedBeforeCommit_ShouldNotWait()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        var ops = Enumerable.Range(0, 10).Select(_ => new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();

        foreach (var op in ops)
            uut.RegisterAckOp(op.Task);

        foreach (var op in ops)
            op.SetResult(null);

        //Act
        await uut.Commit(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    // Additional coverage
    [Fact]
    public async Task RegisterSendOp_GivenOpsInSeveralRounds_ShouldWaitOnlyForTheOpsThatArePending()
    {
        //Arrange
        var uut = CreateTransaction(Substitute.For<ITransactionCoordinatorClient>());
        uut.RegisterSendOp(Task.CompletedTask);
        var second = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterSendOp(second.Task);
        second.SetResult(null);
        var third = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RegisterSendOp(third.Task);

        //Act
        var commit = uut.Commit(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedWithPendingOp = commit.IsCompleted;
        third.SetResult(null);
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isCompletedWithPendingOp.ShouldBeFalse();
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    private static Transaction CreateTransaction(ITransactionCoordinatorClient tcClient, TimeSpan? timeout = null)
        => new(tcClient, _id, timeout ?? TimeSpan.FromMinutes(1));

    private static async Task<Transaction> CreateTransactionInState(ITransactionCoordinatorClient tcClient, TransactionState state)
    {
        switch (state)
        {
            case TransactionState.Committed:
                var committed = CreateTransaction(tcClient);
                await committed.Commit(Current.CancellationToken);
                return committed;
            case TransactionState.Aborted:
                var aborted = CreateTransaction(tcClient);
                await aborted.Abort(Current.CancellationToken);
                return aborted;
            case TransactionState.Error:
                tcClient.Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new TransactionNotFoundException("not found")));
                var error = CreateTransaction(tcClient);
                _ = await Record.ExceptionAsync(() => error.Commit(Current.CancellationToken).AsTask());
                return error;
            case TransactionState.TimedOut:
                var timedOut = CreateTransaction(tcClient, TimeSpan.FromMilliseconds(20));
                await timedOut.State.OnStateChangeTo(TransactionState.TimedOut, Current.CancellationToken);
                return timedOut;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, null);
        }
    }
}
