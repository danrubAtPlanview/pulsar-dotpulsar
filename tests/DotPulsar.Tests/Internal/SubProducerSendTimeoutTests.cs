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
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;
using System.Text;

// The tests use short timeouts and wait for much longer than they need, because the timer of the sub producer is a Task.Delay
[Trait("Category", "Unit")]
public sealed class SubProducerSendTimeoutTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _sendTimeout = TimeSpan.FromMilliseconds(200);

    [Fact]
    public async Task SendTimeout_GivenNoReceipt_ShouldFailPendingMessageWithOperationTimeoutException()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel);
        var sendOp = CreateSendOp(1);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        channel.Entries.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task SendTimeout_GivenThreePendingMessages_ShouldFailAllWithTheirOwnSequenceIds()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(11), CreateSendOp(12), CreateSendOp(13) };

        //Act
        foreach (var sendOp in sendOps)
            await sut.Send(sendOp, Current.CancellationToken);

        //Assert
        var exceptions = new List<OperationTimeoutException>();
        foreach (var sendOp in sendOps)
            exceptions.Add(await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)));

        exceptions[0].Message.ShouldContain("sequence id 11");
        exceptions[1].Message.ShouldContain("sequence id 12");
        exceptions[2].Message.ShouldContain("sequence id 13");
    }

    [Fact]
    public async Task SendTimeout_GivenReceiptBeforeDeadline_ShouldNotFail()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, sendTimeout: TimeSpan.FromMilliseconds(500));
        var sendOp = CreateSendOp(1);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(1500), Current.CancellationToken);

        //Assert
        sendOp.ReceiptTcs.Task.IsCompletedSuccessfully.ShouldBeTrue();
    }

    [Fact]
    public async Task SendTimeout_GivenLateReceiptAfterTimeout_ShouldIgnoreItAndKeepProducerConnected()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, registerEvent);
        var timedOutOp = CreateSendOp(1);
        await sut.Send(timedOutOp, Current.CancellationToken);
        await Should.ThrowAsync<OperationTimeoutException>(timedOutOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        var lateEntry = channel.Entries.Single();
        var nextOp = CreateSendOp(2);
        await sut.Send(nextOp, Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 2, "the next message is written");

        //Act
        lateEntry.Response.SetResult(channel.CreateReceipt(lateEntry));
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var completedByTheLateReceipt = nextOp.ReceiptTcs.Task.IsCompleted;
        var nextEntry = channel.Entries[1];
        nextEntry.Response.SetResult(channel.CreateReceipt(nextEntry));
        await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        completedByTheLateReceipt.ShouldBeFalse("the receipt of the message that timed out is not the receipt of the next message");
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is SendReceiptWrongOrdering || argument is ChannelDisconnected);
    }

    [Fact]
    public async Task SendTimeout_GivenMessageEnqueuedAfterTimeout_ShouldNotBeFailedByThatTimeout()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, sendTimeout: TimeSpan.FromMilliseconds(600));
        var firstOp = CreateSendOp(1);
        await sut.Send(firstOp, Current.CancellationToken);
        await Should.ThrowAsync<OperationTimeoutException>(firstOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        var secondOp = CreateSendOp(2);

        //Act
        await sut.Send(secondOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(200), Current.CancellationToken);

        //Assert
        secondOp.ReceiptTcs.Task.IsCompleted.ShouldBeFalse("the second message has not waited for the timeout yet");
        await Should.ThrowAsync<OperationTimeoutException>(secondOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
    }

    [Fact]
    public async Task SendTimeout_GivenBatchingEnabled_ShouldAddBatchingMaxPublishDelayToDeadline()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, enableBatching: true, batchingMaxPublishDelay: TimeSpan.FromMilliseconds(800), sendTimeout: TimeSpan.FromMilliseconds(300));
        var sendOp = CreateSendOp(1);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(600), Current.CancellationToken);
        var failedBeforeTheBatchWasDue = sendOp.ReceiptTcs.Task.IsCompleted;
        await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));

        //Assert
        failedBeforeTheBatchWasDue.ShouldBeFalse("the message is waiting in a batch, and the batch is not due yet");
        stopwatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(950));
    }

    [Fact]
    public async Task SendTimeout_GivenBatchThatWaitedForItsDelay_ShouldWaitForTheSendTimeoutFromWhenTheBatchWasSent()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, enableBatching: true, batchingMaxPublishDelay: TimeSpan.FromMilliseconds(200), sendTimeout: TimeSpan.FromMilliseconds(300));
        var sendOp = CreateSendOp(1);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(400), Current.CancellationToken);
        var failedTooEarly = sendOp.ReceiptTcs.Task.IsCompleted;

        //Assert
        failedTooEarly.ShouldBeFalse("the message was sent when the batch was due, which is after 200 milliseconds, and not when it was created");
        await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
    }

    [Fact]
    public async Task SendTimeout_GivenFrozenBatch_ShouldFailAllMessagesOfTheBatchAndReturnBuffer()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, enableBatching: true, batchingMaxMessages: 3);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3) };

        //Act
        foreach (var sendOp in sendOps)
            await sut.Send(sendOp, Current.CancellationToken);

        //Assert
        foreach (var sendOp in sendOps)
            await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));

        channel.Entries.ShouldHaveSingleItem().NumberOfMessages.ShouldBe(3);
        channel.LastGivenMetadata!.HasNumMessagesInBatch.ShouldBeFalse("the metadata of the batch is reset when the batch is returned");
        sendOps.ShouldAllBe(sendOp => sendOp.Batch == null);
    }

    [Fact]
    public async Task SendTimeout_GivenTimeoutWhileDispatcherWaitsForRegistration_ShouldAbandonGroupAndContinue()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, enableBatching: true, batchingMaxPublishDelay: TimeSpan.FromMilliseconds(1));
        var gatedOp = CreateSendOp(1, gate.Task);
        var plainOp = CreateSendOp(2);
        await sut.Send(gatedOp, Current.CancellationToken);
        await sut.Send(plainOp, Current.CancellationToken);
        await Should.ThrowAsync<OperationTimeoutException>(gatedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        await Should.ThrowAsync<OperationTimeoutException>(plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        var laterOp = CreateSendOp(3);

        //Act
        gate.SetResult();
        await sut.Send(laterOp, Current.CancellationToken);
        await laterOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([3UL]);
    }

    [Fact]
    public async Task SendTimeout_GivenTimeoutWhileBatchIsBeingWritten_ShouldReturnBufferOnlyAfterTheWrite()
    {
        //Arrange
        var writeGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel { Respond = _ => null, WriteGate = writeGate.Task };
        await using var sut = await CreateSubProducer(channel, enableBatching: true, batchingMaxMessages: 3);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3) };
        foreach (var sendOp in sendOps)
            await sut.Send(sendOp, Current.CancellationToken);

        //Act
        foreach (var sendOp in sendOps)
            await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));

        var returnedWhileWriting = !channel.LastGivenMetadata!.HasNumMessagesInBatch;
        writeGate.SetResult();
        await WaitFor.Until(() => !channel.LastGivenMetadata.HasNumMessagesInBatch, "the batch is returned when the write is done");

        //Assert
        returnedWhileWriting.ShouldBeFalse("the frame of the batch is written from its buffer");
    }

    [Fact]
    public async Task SendTimeout_GivenZero_ShouldNotFailAnything()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, sendTimeout: TimeSpan.Zero);
        var sendOp = CreateSendOp(1);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromSeconds(1), Current.CancellationToken);

        //Assert
        sendOp.ReceiptTcs.Task.IsCompleted.ShouldBeFalse();
        sendOp.CreatedTimestamp.ShouldBe(0L);
    }

    [Fact]
    public async Task SendTimeout_GivenQueueFullAndEnqueuerBlocked_ShouldUnblockEnqueuerAfterTimeout()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, maxPendingMessages: 1);
        var firstOp = CreateSendOp(1);
        await sut.Send(firstOp, Current.CancellationToken);
        var secondOp = CreateSendOp(2);

        //Act
        var blockedSend = sut.Send(secondOp, Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isBlocked = !blockedSend.IsCompleted;
        await blockedSend.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isBlocked.ShouldBeTrue();
        await Should.ThrowAsync<OperationTimeoutException>(firstOp.ReceiptTcs.Task);
        await WaitFor.Until(() => channel.Entries.Count == 2, "the message that was blocked is written");
    }

    [Fact]
    public async Task SendTimeout_GivenDispose_ShouldStopTimerAndCancelPendingMessages()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        var sut = await CreateSubProducer(channel, sendTimeout: TimeSpan.FromSeconds(30));
        var sendOp = CreateSendOp(1);
        await sut.Send(sendOp, Current.CancellationToken);

        //Act
        await sut.DisposeAsync().AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        sendOp.ReceiptTcs.Task.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task WaitForSendQueueEmpty_GivenTimeoutRemovedAllMessages_ShouldComplete()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel);
        var sendOp = CreateSendOp(1);
        await sut.Send(sendOp, Current.CancellationToken);

        //Act
        await sut.WaitForSendQueueEmpty(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
    }

    [Fact]
    public async Task SendTimeout_GivenNoMessagesForLongerThanTheTimeout_ShouldNotAffectLaterMessages()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);

        //Act
        await Task.Delay(TimeSpan.FromMilliseconds(600), Current.CancellationToken);
        var sendOp = CreateSendOp(1);
        await sut.Send(sendOp, Current.CancellationToken);

        //Assert
        await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
    }

    private static SendOp CreateSendOp(ulong sequenceId, Task? registrationGate = null)
        => new(
            new MessageMetadata { SequenceId = sequenceId },
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"payload-{sequenceId}")),
            new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously),
            registrationGate: registrationGate);

    private static async Task<SubProducer> CreateSubProducer(
        FakeProducerChannel channel,
        IRegisterEvent? registerEvent = null,
        bool enableBatching = false,
        uint batchingMaxMessages = 1000,
        TimeSpan? batchingMaxPublishDelay = null,
        TimeSpan? sendTimeout = null,
        uint maxPendingMessages = 1000)
    {
        registerEvent ??= Substitute.For<IRegisterEvent>();
        var correlationId = Guid.NewGuid();
        var factory = Substitute.For<IProducerChannelFactory>();
        factory.Create(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult<IProducerChannel>(channel));

        var sut = new SubProducer(
            correlationId,
            registerEvent,
            Substitute.For<IProducerChannel>(),
            new Executor(correlationId, registerEvent, new DefaultExceptionHandler()),
            Substitute.For<IState<ProducerState>>(),
            factory,
            0,
            maxPendingMessages,
            _topic,
            enableBatching,
            BatcherType.Default,
            batchingMaxMessages,
            128 * 1024,
            batchingMaxPublishDelay ?? TimeSpan.FromHours(1),
            1,
            sendTimeout ?? _sendTimeout);

        await sut.EstablishNewChannel(Current.CancellationToken);

        return sut;
    }
}
