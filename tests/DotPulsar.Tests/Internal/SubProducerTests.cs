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
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;
using System.Collections.Concurrent;

[Trait("Category", "Unit")]
public class SubProducerTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task EstablishNewChannel_WhenCalledThreeTimesInSuccession_ShouldNotThrowObjectDisposedException()
    {
        //Arrange
        var factory = Substitute.For<IProducerChannelFactory>();
        factory.Create(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Substitute.For<IProducerChannel>()));

        await using var sut = CreateSubProducer(factory);

        //Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            await sut.EstablishNewChannel(CancellationToken.None);
            await sut.EstablishNewChannel(CancellationToken.None);
            await sut.EstablishNewChannel(CancellationToken.None);
        });

        //Assert
        exception.ShouldNotBeOfType<ObjectDisposedException>();
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenRegistrationGateNotCompleted_ShouldNotWriteTheMessageOrTheMessagesBehindItBeforeTheGateCompletes()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var gatedOp = CreateSendOp(1, gate.Task);
        var plainOp = CreateSendOp(2);

        //Act
        await sut.Send(gatedOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var writtenBeforeGate = channel.Written;
        gate.SetResult();
        await Task.WhenAll(gatedOp.ReceiptTcs.Task, plainOp.ReceiptTcs.Task).WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        writtenBeforeGate.ShouldBeEmpty();
        channel.Written.ShouldBe([1UL, 2UL]);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenRegistrationGateAlreadyCompleted_ShouldWriteTheMessage()
    {
        //Arrange
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var sendOp = CreateSendOp(1, Task.CompletedTask);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        channel.Written.ShouldBe([1UL]);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenRegistrationGateFails_ShouldFailOnlyThatMessageWithoutReceiptAndContinueWithTheNext()
    {
        //Arrange
        var exception = new TransactionCoordinatorClientException("The registration failed");
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var failingOp = CreateSendOp(1, Task.FromException(exception));
        var plainOp = CreateSendOp(2);

        //Act
        await sut.Send(failingOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);

        //Assert
        var actual = await Should.ThrowAsync<TransactionCoordinatorClientException>(failingOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        actual.ShouldBeSameAs(exception);
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken); // The receipt of the next message is matched with the head of the queue, so it would fail if the failed message was still there
        channel.Written.ShouldBe([2UL]);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenRegistrationGateFailsLater_ShouldFailOnlyThatMessageAndContinueWithTheNext()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var failingOp = CreateSendOp(1, gate.Task);
        var plainOp = CreateSendOp(2);
        await sut.Send(failingOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);

        //Act
        gate.SetException(new TransactionCoordinatorClientException("The registration failed"));

        //Assert
        await Should.ThrowAsync<TransactionCoordinatorClientException>(failingOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        channel.Written.ShouldBe([2UL]);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenCallerCancelsWhileWaitingForRegistration_ShouldNotWaitForRegistrationAndContinueWithTheNext()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var cancelledOp = CreateSendOp(1, gate.Task);
        var plainOp = CreateSendOp(2);
        await sut.Send(cancelledOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);

        //Act
        _ = cancelledOp.ReceiptTcs.TrySetCanceled(Current.CancellationToken); // This is what Producer.Send does when the caller cancels

        //Assert
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        channel.Written.ShouldBe([2UL]);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenReceiptCompletedWhileWaitingForRegistration_ShouldNotWriteTheMessageAndContinueWithTheNext()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var expiredOp = CreateSendOp(1, gate.Task);
        var plainOp = CreateSendOp(2);
        await sut.Send(expiredOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);

        //Act
        _ = expiredOp.ReceiptTcs.TrySetException(new OperationTimeoutException("The message was not sent in time"));

        //Assert
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        channel.Written.ShouldBe([2UL]);
        await Should.ThrowAsync<OperationTimeoutException>(expiredOp.ReceiptTcs.Task);
    }

    // Additional coverage
    [Fact]
    public async Task EstablishNewChannel_GivenDispatcherWaitingForRegistration_ShouldNotWaitForRegistrationAndWriteTheMessageWhenItCompletes()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeChannel();
        await using var sut = CreateSubProducer(CreateFactory(channel));
        await sut.EstablishNewChannel(Current.CancellationToken);
        var sendOp = CreateSendOp(1, gate.Task);
        await sut.Send(sendOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);

        //Act
        await sut.EstablishNewChannel(Current.CancellationToken).WaitAsync(_timeout, Current.CancellationToken);
        gate.SetResult();
        await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        channel.Written.ShouldBe([1UL]);
    }

    // Additional coverage
    [Fact]
    public async Task ResponseProcessor_GivenNotAllowedSendError_ShouldFailOnlyThatMessage()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeChannel
        {
            Respond = sequenceId => sequenceId == 1
                ? CreateSendError(sequenceId, ServerError.NotAllowedError, "Transaction [(0,5)] has been ended")
                : CreateReceipt(sequenceId)
        };
        await using var sut = CreateSubProducer(CreateFactory(channel), registerEvent, true);
        await sut.EstablishNewChannel(Current.CancellationToken);
        var notAllowedOp = CreateSendOp(1);
        var plainOp = CreateSendOp(2);

        //Act
        await sut.Send(notAllowedOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);

        //Assert
        var exception = await Should.ThrowAsync<NotAllowedException>(notAllowedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        exception.Message.ShouldBe("Transaction [(0,5)] has been ended");
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        registerEvent.ReceivedCalls().Select(x => x.GetArguments()[0]).ShouldNotContain(x => x is ExecutorFaulted || x is SendReceiptWrongOrdering);
    }

    // Additional coverage
    [Fact]
    public async Task ResponseProcessor_GivenPersistenceSendError_ShouldFaultProducer()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeChannel { Respond = sequenceId => CreateSendError(sequenceId, ServerError.PersistenceError, "Could not write the message") };
        await using var sut = CreateSubProducer(CreateFactory(channel), registerEvent, true);
        await sut.EstablishNewChannel(Current.CancellationToken);
        var sendOp = CreateSendOp(1);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);

        //Assert
        await WaitFor.Until(() => registerEvent.ReceivedCalls().Select(x => x.GetArguments()[0]).Any(x => x is ExecutorFaulted { Exception: PersistenceException }), "the producer is faulted");
        sendOp.ReceiptTcs.Task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task ResponseProcessor_GivenNotAllowedSendErrorForMessageBehindTheHead_ShouldReconnectInsteadOfFailingTheWrongMessage()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeChannel
        {
            Respond = sequenceId => sequenceId == 2
                ? CreateSendError(sequenceId, ServerError.NotAllowedError, "Not allowed")
                : null
        };
        await using var sut = CreateSubProducer(CreateFactory(channel), registerEvent, true);
        await sut.EstablishNewChannel(Current.CancellationToken);
        var headOp = CreateSendOp(1);
        var notAllowedOp = CreateSendOp(2);

        //Act
        await sut.Send(headOp, Current.CancellationToken);
        await sut.Send(notAllowedOp, Current.CancellationToken);

        //Assert
        await WaitFor.Until(() => registerEvent.ReceivedCalls().Select(x => x.GetArguments()[0]).Any(x => x is SendReceiptWrongOrdering), "the producer reconnects");
        headOp.ReceiptTcs.Task.IsCompleted.ShouldBeFalse();
        notAllowedOp.ReceiptTcs.Task.IsCompleted.ShouldBeFalse();
    }

    // Upstream: BatchMessageTest#testSendOverSizeMessage, ProducerImpl#processOpSendMsg
    // Upstream: BatchMessageContainerImplTest#oversizedSingleMessageReleasesTheBatchBufferExactlyOnce, #testOversizedSingleMessageBatchReleasesItsPayloadExactlyOnce
    // A single message is not written to a pooled batch, so this tests the failure of the message, and SubProducerBatchOwnershipTests tests that no batch is rented for it when the producer batches.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Send_GivenMessageThatIsTooLarge_ShouldFailOnlyThatMessageWithTooLargeMessageExceptionAndContinueWithTheNext(bool failsAfterAwaiting)
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var exception = new TooLargeMessageException(10 * 1024 * 1024, 5 * 1024 * 1024);
        var channel = new FakeChannel { Fail = sequenceId => sequenceId == 1 ? (failsAfterAwaiting ? FailAfterYield(exception) : Task.FromException(exception)) : null };
        await using var sut = CreateSubProducer(CreateFactory(channel), registerEvent, true);
        await sut.EstablishNewChannel(Current.CancellationToken);
        var tooLargeOp = CreateSendOp(1);
        var plainOp = CreateSendOp(2);

        //Act
        await sut.Send(tooLargeOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);

        //Assert
        var actual = await Should.ThrowAsync<TooLargeMessageException>(tooLargeOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        actual.ShouldBeSameAs(exception);
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken); // The receipt of the next message is matched with the head of the queue, so it would fail if the message that is too large was still there
        channel.Written.ShouldBe([2UL]);
        registerEvent.ReceivedCalls().Select(x => x.GetArguments()[0]).ShouldNotContain(x => x is ExecutorFaulted || x is ChannelDisconnected);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenMessageThatIsTooLargeAndMaxPendingMessagesReached_ShouldReleaseTheQueueForTheNextMessage()
    {
        //Arrange
        var channel = new FakeChannel { Fail = sequenceId => sequenceId == 1 ? Task.FromException(new TooLargeMessageException(10, 5)) : null };
        await using var sut = CreateSubProducer(CreateFactory(channel), maxPendingMessages: 1);
        await sut.EstablishNewChannel(Current.CancellationToken);
        var tooLargeOp = CreateSendOp(1);
        var plainOp = CreateSendOp(2);

        //Act
        await sut.Send(tooLargeOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<TooLargeMessageException>(tooLargeOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
    }

    private static async Task FailAfterYield(Exception exception)
    {
        await Task.Yield();
        throw exception;
    }

    private static SendOp CreateSendOp(ulong sequenceId, Task? registrationGate = null)
        => new(new MessageMetadata { SequenceId = sequenceId }, ReadOnlySequence<byte>.Empty, new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously), registrationGate: registrationGate);

    private static BaseCommand CreateReceipt(ulong sequenceId)
        => new()
        {
            Type = BaseCommand.Types.Type.SendReceipt,
            SendReceipt = new CommandSendReceipt { SequenceId = sequenceId, MessageId = new MessageIdData { LedgerId = 1, EntryId = sequenceId } }
        };

    private static BaseCommand CreateSendError(ulong sequenceId, ServerError error, string message)
        => new()
        {
            Type = BaseCommand.Types.Type.SendError,
            SendError = new CommandSendError { SequenceId = sequenceId, Error = error, Message = message }
        };

    private static IProducerChannelFactory CreateFactory(IProducerChannel channel)
    {
        var factory = Substitute.For<IProducerChannelFactory>();
        factory.Create(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(channel));
        return factory;
    }

    private static SubProducer CreateSubProducer(IProducerChannelFactory factory, IRegisterEvent? registerEvent = null, bool useExecutorOfProducer = false, uint maxPendingMessages = 1000)
    {
        registerEvent ??= Substitute.For<IRegisterEvent>();
        var correlationId = Guid.NewGuid();

        return new(
            correlationId: correlationId,
            registerEvent: registerEvent,
            initialChannel: Substitute.For<IProducerChannel>(),
            executor: useExecutorOfProducer ? new Executor(correlationId, registerEvent, new DefaultExceptionHandler()) : new DirectExecutor(),
            state: Substitute.For<IState<ProducerState>>(),
            factory: factory,
            partition: 0,
            maxPendingMessages: maxPendingMessages,
            topic: "persistent://public/default/test",
            enableBatching: false,
            batcherType: BatcherType.Default,
            batchingMaxMessages: ProducerOptions<string>.DefaultBatchingMaxMessages,
            batchingMaxBytes: ProducerOptions<string>.DefaultBatchingMaxBytes,
            batchingMaxPublishDelay: ProducerOptions<string>.DefaultBatchingMaxPublishDelay,
            initialSequenceId: 1,
            sendTimeout: TimeSpan.Zero);
    }

    // A channel that writes nothing, but records the sequence ids of the messages it was asked to write and answers them
    private sealed class FakeChannel : IProducerChannel
    {
        private readonly ConcurrentQueue<ulong> _written = new();

        // Decides the answer to a message. By default it is a receipt. Returning null leaves the message unanswered.
        public Func<ulong, BaseCommand?>? Respond { get; set; }

        // Decides if writing a message fails. It returns the task that the write returns, or null if the write does not fail.
        public Func<ulong, Task?>? Fail { get; set; }

        public ulong[] Written => _written.ToArray();

        public int MaxMessageSize => 5 * 1024 * 1024;

        public Task Send(MessageMetadata metadata, ReadOnlySequence<byte> payload, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
        {
            var failure = Fail?.Invoke(metadata.SequenceId);
            if (failure is not null)
                return failure;

            _written.Enqueue(metadata.SequenceId);

            var response = Respond is null ? CreateReceipt(metadata.SequenceId) : Respond(metadata.SequenceId);
            if (response is not null)
                responseTcs.TrySetResult(response);

            return Task.CompletedTask;
        }

        public ChunkedSendState? PlanChunking(MessageMetadata metadata, ReadOnlySequence<byte> payload, int chunkMaxMessageSize)
            => throw new NotSupportedException();

        public Task SendChunk(ChunkedSendState state, int chunkId, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask ClosedByClient(CancellationToken cancellationToken) => new();

        public ValueTask DisposeAsync() => new();
    }

    private sealed class DirectExecutor : IExecute
    {
        public ValueTask Execute(Action action, CancellationToken cancellationToken = default)
        {
            action();
            return ValueTask.CompletedTask;
        }

        public async ValueTask Execute(Func<Task> func, CancellationToken cancellationToken = default)
            => await func().ConfigureAwait(false);

        public async ValueTask Execute(Func<ValueTask> func, CancellationToken cancellationToken = default)
            => await func().ConfigureAwait(false);

        public ValueTask<TResult> Execute<TResult>(Func<TResult> func, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(func());

        public async ValueTask<TResult> Execute<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken = default)
            => await func().ConfigureAwait(false);

        public async ValueTask<TResult> Execute<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken = default)
            => await func().ConfigureAwait(false);

        public ValueTask<bool> TryExecuteOnce(Action action, CancellationToken cancellationToken = default)
        {
            action();
            return ValueTask.FromResult(true);
        }

        public async ValueTask<bool> TryExecuteOnce(Func<Task> func, CancellationToken cancellationToken = default)
        {
            try
            {
                await func().ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        public async ValueTask<bool> TryExecuteOnce(Func<ValueTask> func, CancellationToken cancellationToken = default)
        {
            await func().ConfigureAwait(false);
            return true;
        }

        public ValueTask<ExecutionResult<TResult>> TryExecuteOnce<TResult>(Func<TResult> func, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new ExecutionResult<TResult>(true, func()));

        public async ValueTask<ExecutionResult<TResult>> TryExecuteOnce<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken = default)
            => new ExecutionResult<TResult>(true, await func().ConfigureAwait(false));

        public async ValueTask<ExecutionResult<TResult>> TryExecuteOnce<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken = default)
            => new ExecutionResult<TResult>(true, await func().ConfigureAwait(false));
    }
}
