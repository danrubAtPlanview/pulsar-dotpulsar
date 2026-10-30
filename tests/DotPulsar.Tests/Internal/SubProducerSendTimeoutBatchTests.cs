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
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;
using System.Text;

// The tests of what happens to the batch of a producer when the send timeout removes its messages. They look at the state of the batch, so they do not run in parallel with other tests (see SharedBatchPoolCollection).
// The send timeout is longer than in the other tests of the send timeout, because the dispatcher must have started to write the batch when the timeout fires.
[Collection("SharedBatchPool")]
[Trait("Category", "Unit")]
public sealed class SubProducerSendTimeoutBatchTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _sendTimeout = TimeSpan.FromMilliseconds(500);

    [Fact]
    public async Task SendTimeout_GivenTimeoutBeforeTheDispatcherWritesTheBatch_ShouldWriteTheBatchAndReturnBufferAfterTheWrite()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel { Respond = _ => null };
        GatedExecutor? gate = null;
        await using var sut = await CreateSubProducer(channel, registerEvent, executor => gate = new GatedExecutor(executor));
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3) };
        PreparedBatch? batch = null;
        gate!.Arm(() => batch = sendOps[0].Batch);

        foreach (var sendOp in sendOps)
            await sut.Send(sendOp, Current.CancellationToken);

        await gate.Entered.WaitAsync(_timeout, Current.CancellationToken);

        //Act
        foreach (var sendOp in sendOps)
            await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));

        var returnedBeforeTheWrite = batch!.IsReturned;
        gate.Release();
        await WaitFor.Until(() => batch.IsReturned, "the batch is returned when the write is done");

        //Assert
        returnedBeforeTheWrite.ShouldBeFalse("the dispatcher has taken the batch from the queue, and is going to write it");
        channel.Entries.ShouldHaveSingleItem().NumberOfMessages.ShouldBe(3);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is ChannelDisconnected);
    }

    private static SendOp CreateSendOp(ulong sequenceId)
        => new(
            new MessageMetadata { SequenceId = sequenceId },
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"payload-{sequenceId}")),
            new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously));

    private static async Task<SubProducer> CreateSubProducer(FakeProducerChannel channel, IRegisterEvent registerEvent, Func<IExecute, IExecute>? decorateExecutor = null)
    {
        var correlationId = Guid.NewGuid();
        var factory = Substitute.For<IProducerChannelFactory>();
        factory.Create(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult<IProducerChannel>(channel));
        IExecute executor = new Executor(correlationId, registerEvent, new DefaultExceptionHandler());

        var sut = new SubProducer(
            correlationId,
            registerEvent,
            Substitute.For<IProducerChannel>(),
            decorateExecutor is null ? executor : decorateExecutor(executor),
            Substitute.For<IState<ProducerState>>(),
            factory,
            0,
            1000,
            _topic,
            true,
            BatcherType.Default,
            3,
            128 * 1024,
            TimeSpan.FromHours(1),
            1,
            _sendTimeout);

        await sut.EstablishNewChannel(Current.CancellationToken);

        return sut;
    }

    // Holds the next write of the dispatcher until the test releases it. The send timeout can then fire after the dispatcher has taken a batch from the queue, and before it writes the batch.
    private sealed class GatedExecutor : IExecute
    {
        private readonly IExecute _inner;
        private readonly TaskCompletionSource _entered;
        private readonly TaskCompletionSource _released;
        private Action? _onEntered;

        public GatedExecutor(IExecute inner)
        {
            _inner = inner;
            _entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _onEntered = null;
        }

        public Task Entered => _entered.Task;

        // The next task that is executed waits. 'onEntered' is called by the thread that executes it, before it waits.
        public void Arm(Action onEntered)
            => Volatile.Write(ref _onEntered, onEntered);

        public void Release()
            => _ = _released.TrySetResult();

        public async ValueTask<bool> TryExecuteOnce(Func<Task> func, CancellationToken cancellationToken = default)
        {
            var onEntered = Interlocked.Exchange(ref _onEntered, null);

            if (onEntered is not null)
            {
                onEntered();
                _ = _entered.TrySetResult();
                await _released.Task.ConfigureAwait(false);
            }

            return await _inner.TryExecuteOnce(func, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask Execute(Action action, CancellationToken cancellationToken = default)
            => _inner.Execute(action, cancellationToken);

        public ValueTask Execute(Func<Task> func, CancellationToken cancellationToken = default)
            => _inner.Execute(func, cancellationToken);

        public ValueTask Execute(Func<ValueTask> func, CancellationToken cancellationToken = default)
            => _inner.Execute(func, cancellationToken);

        public ValueTask<TResult> Execute<TResult>(Func<TResult> func, CancellationToken cancellationToken = default)
            => _inner.Execute(func, cancellationToken);

        public ValueTask<TResult> Execute<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken = default)
            => _inner.Execute(func, cancellationToken);

        public ValueTask<TResult> Execute<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken = default)
            => _inner.Execute(func, cancellationToken);

        public ValueTask<bool> TryExecuteOnce(Action action, CancellationToken cancellationToken = default)
            => _inner.TryExecuteOnce(action, cancellationToken);

        public ValueTask<bool> TryExecuteOnce(Func<ValueTask> func, CancellationToken cancellationToken = default)
            => _inner.TryExecuteOnce(func, cancellationToken);

        public ValueTask<ExecutionResult<TResult>> TryExecuteOnce<TResult>(Func<TResult> func, CancellationToken cancellationToken = default)
            => _inner.TryExecuteOnce(func, cancellationToken);

        public ValueTask<ExecutionResult<TResult>> TryExecuteOnce<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken = default)
            => _inner.TryExecuteOnce(func, cancellationToken);

        public ValueTask<ExecutionResult<TResult>> TryExecuteOnce<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken = default)
            => _inner.TryExecuteOnce(func, cancellationToken);
    }
}
