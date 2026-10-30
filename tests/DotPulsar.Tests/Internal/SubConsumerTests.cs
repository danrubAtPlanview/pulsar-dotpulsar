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
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class SubConsumerTests
{
    private static readonly MessageId _messageId = new(1, 2, 0, -1, "persistent://public/default/test");

    private readonly IRegisterEvent _eventRegister;
    private readonly IConsumerChannel<byte[]> _channel;

    public SubConsumerTests()
    {
        _eventRegister = Substitute.For<IRegisterEvent>();
        _channel = Substitute.For<IConsumerChannel<byte[]>>();
    }

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

    private static (BatchHandler<byte[]> Handler, MessageFactory<byte[]> Factory) CreateBatchHandler()
    {
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        return (new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic), messageFactory);
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
        => new(1, 100, new AsyncQueue<MessagePackage>(), connection, batchHandler, messageFactory, [], BatchTestData.Topic, ackReceiptEnabled, batchIndexAckEnabled);

    private SubConsumer<byte[]> CreateSubConsumer(bool ackReceiptEnabled, TimeSpan? operationTimeout = null, IConsumerChannel<byte[]>? channel = null)
    {
        var correlationId = Guid.NewGuid();
        var executor = new Executor(correlationId, _eventRegister, new DefaultExceptionHandler());
        var state = Substitute.For<IState<ConsumerState>>();
        var factory = Substitute.For<IConsumerChannelFactory<byte[]>>();

        return new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            SubscriptionType.Exclusive,
            "persistent://public/default/test",
            _eventRegister,
            channel ?? _channel,
            executor,
            state,
            factory,
            ackReceiptEnabled,
            operationTimeout ?? TimeSpan.FromSeconds(30));
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
}
