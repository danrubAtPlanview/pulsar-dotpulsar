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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;

// The allocations and the pooled command of the acknowledgment path, which is the hot path of a consumer. See followups/0007.
[Trait("Category", "Unit")]
public sealed class SubConsumerAckPathTests
{
    private static readonly MessageId _messageId = new(1, 2, 0, -1, "persistent://public/default/test");

    private readonly IRegisterEvent _eventRegister = Substitute.For<IRegisterEvent>();

    // The executor needs a closure and a delegate for every acknowledgment, which is 104 B. That is what the acknowledgment allocated before transactions
    // were added (benchmark PlainAck on e8f2ba2). The result of the executor was a class, which added 32 B.
    [Fact]
    public async Task Acknowledge_GivenNoReceiptRequested_ShouldNotAllocateMoreThanTheClosureOfTheExecutor()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var channel = new FakeAckChannel();
        await using var uut = CreateSubConsumer(channel, false);

        //Act
        var allocated = await AllocationMeasurement.BytesPerOperation(async () => await uut.Acknowledge(_messageId, CancellationToken.None), 104);

        //Assert
        allocated.ShouldBeLessThanOrEqualTo(104);
        channel.Acknowledgments.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task AcknowledgeCumulative_GivenNoReceiptRequested_ShouldNotAllocateMoreThanTheClosureOfTheExecutor()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var channel = new FakeAckChannel();
        await using var uut = CreateSubConsumer(channel, false);

        //Act
        var allocated = await AllocationMeasurement.BytesPerOperation(async () => await uut.AcknowledgeCumulative(_messageId, CancellationToken.None), 104);

        //Assert
        allocated.ShouldBeLessThanOrEqualTo(104);
    }

    [Fact]
    public async Task Acknowledge_GivenNoReceiptRequestedAndConnectionLostWhileSending_ShouldRetryOnNewChannel()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var calls = 0;
        channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1 ? Task.FromException<BaseCommand?>(new TaskCanceledException()) : Task.FromResult<BaseCommand?>(null));
        await using var uut = CreateSubConsumer(channel, false);

        //Act
        var single = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));
        var callsAfterSingle = calls;
        calls = 0;
        var list = await Record.ExceptionAsync(async () => await uut.Acknowledge([_messageId, new MessageId(1, 3, 0, -1)], Current.CancellationToken));

        //Assert
        single.ShouldBeNull();
        callsAfterSingle.ShouldBe(2);
        list.ShouldBeNull();
        calls.ShouldBe(2);
    }

    [Fact]
    public async Task Acknowledge_GivenNoReceiptRequestedAndExceptionThatIsNotRetried_ShouldThrowTheExceptionAndRegisterExecutorFaulted()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var exception = new InvalidOperationException("failed");
        channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.FromException<BaseCommand?>(exception));
        await using var uut = CreateSubConsumer(channel, false);

        //Act
        var single = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));
        var list = await Record.ExceptionAsync(async () => await uut.Acknowledge([_messageId], Current.CancellationToken));

        //Assert
        single.ShouldBeSameAs(exception);
        list.ShouldBeSameAs(exception);
        _eventRegister.Received(2).Register(Arg.Is<IEvent>(e => e is ExecutorFaulted));
    }

    [Fact]
    public async Task Acknowledge_GivenNoReceiptRequestedAndChannelThatReturnsResponse_ShouldNotEvaluateIt()
    {
        //Arrange
        var channel = Substitute.For<IConsumerChannel<byte[]>>();
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse { ConsumerId = 1, Error = ServerError.TransactionConflict, Message = "conflict" } };
        channel.Send(Arg.Any<CommandAck>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(response));
        await using var uut = CreateSubConsumer(channel, false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Acknowledge(_messageId, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
    }

    private SubConsumer<byte[]> CreateSubConsumer(IConsumerChannel<byte[]> channel, bool ackReceiptEnabled, TimeSpan? operationTimeout = null)
    {
        var correlationId = Guid.NewGuid();

        return new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            SubscriptionType.Exclusive,
            "persistent://public/default/test",
            _eventRegister,
            channel,
            new Executor(correlationId, _eventRegister, new DefaultExceptionHandler()),
            Substitute.For<IState<ConsumerState>>(),
            Substitute.For<IConsumerChannelFactory<byte[]>>(),
            ackReceiptEnabled,
            operationTimeout ?? TimeSpan.FromSeconds(30),
            new ConsumerEpoch(),
            null,
            new NegativeAcknowledgeTracker(TimeSpan.FromMinutes(1), (_, _) => default),
            null,
            ChunkTestData.CreateAssembler());
    }

    // A channel that does not allocate, so that the allocations of the sub-consumer can be measured. A mocked channel records its calls, which allocates.
    private sealed class FakeAckChannel : IConsumerChannel<byte[]>
    {
        private static readonly Task<BaseCommand?> _noResponse = Task.FromResult<BaseCommand?>(null);

        public int Acknowledgments { get; private set; }

        public Task<BaseCommand?> Send(CommandAck command, CancellationToken cancellationToken)
        {
            ++Acknowledgments;
            return _noResponse;
        }

        public Task<BaseCommand> SendTransactional(CommandAck command, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task Send(CommandRedeliverUnacknowledgedMessages command, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task Send(CommandUnsubscribe command, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task Send(CommandSeek command, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<MessageId> Send(CommandGetLastMessageId command, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<IMessage<byte[]>> Receive(CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask ClosedByClient(CancellationToken cancellationToken) => default;
        public ValueTask DisposeAsync() => default;
    }
}
