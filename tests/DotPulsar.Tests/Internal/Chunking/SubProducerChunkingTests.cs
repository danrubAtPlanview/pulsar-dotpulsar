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

namespace DotPulsar.Tests.Internal.Chunking;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;

// The part of the sub producer that sends a message in chunks and completes it when the last chunk is acknowledged, with a channel that records the chunks and answers them
[Trait("Category", "Unit")]
public sealed class SubProducerChunkingTests
{
    private const string _topic = "persistent://public/default/test";
    private const int _chunkSize = 100;
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _sendTimeout = TimeSpan.FromMilliseconds(200);

    // Upstream: ProducerImpl#sendAsync, #ackReceived
    [Fact]
    public async Task Send_GivenMessageThatIsSplit_ShouldWriteEveryChunkWithTheSameSequenceIdAndCompleteWithTheIdOfTheLastChunk()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, partition: 2);
        var payload = CreatePayload(250);
        var sendOp = CreateSendOp(5, payload);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(3);
        entries.Select(entry => entry.SequenceId).Distinct().ShouldBe([5UL]);
        entries.Select(entry => entry.Metadata.ChunkId).ShouldBe([0, 1, 2]);
        entries.Select(entry => entry.Metadata.Uuid).Distinct().ShouldBe(["fake-producer-5"]);
        entries.SelectMany(entry => entry.Payload).ToArray().ShouldBe(payload);
        messageId.EntryId.ShouldBe(3UL);
        messageId.Partition.ShouldBe(2);
        messageId.BatchIndex.ShouldBe(-1);
        messageId.FirstChunkMessageId.ShouldNotBeNull();
        messageId.FirstChunkMessageId.EntryId.ShouldBe(1UL);
        messageId.FirstChunkMessageId.Partition.ShouldBe(2);
        messageId.FirstChunkMessageId.Topic.ShouldBe(_topic);
        var chunkMessageIds = messageId.GetChunkMessageIds()!;
        chunkMessageIds.Select(id => id.EntryId).ShouldBe([1UL, 2UL, 3UL]);
        chunkMessageIds.ShouldAllBe(id => id.Partition == 2);
    }

    // Upstream: ProducerImpl#ackReceived (the callback is called only on the last chunk)
    [Fact]
    public async Task Send_GivenChunksThatAreAnsweredOneByOne_ShouldCompleteOnlyAfterTheReceiptOfTheLastChunk()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel);
        var sendOp = CreateSendOp(1, CreatePayload(250));
        await sut.Send(sendOp, Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 3, "all the chunks are written");
        var entries = channel.Entries;

        //Act
        Answer(entries[0], 10);
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var completedAfterFirst = sendOp.ReceiptTcs.Task.IsCompleted;
        Answer(entries[1], 11);
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var completedAfterSecond = sendOp.ReceiptTcs.Task.IsCompleted;
        Answer(entries[2], 12);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        completedAfterFirst.ShouldBeFalse();
        completedAfterSecond.ShouldBeFalse();
        messageId.EntryId.ShouldBe(12UL);
        messageId.FirstChunkMessageId!.EntryId.ShouldBe(10UL);
    }

    [Fact]
    public async Task Send_GivenMessageThatFitsInOneFrame_ShouldWriteOneEntryWithoutChunkFieldsAndCompleteWithAnIdThatIsNotChunked()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOp = CreateSendOp(1, CreatePayload(10));

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var entry = channel.Entries.Single();
        entry.Metadata.HasChunkId.ShouldBeFalse();
        entry.Metadata.HasNumChunksFromMsg.ShouldBeFalse();
        entry.Metadata.HasUuid.ShouldBeFalse();
        messageId.FirstChunkMessageId.ShouldBeNull();
        messageId.GetChunkMessageIds().ShouldBeNull();
        messageId.EntryId.ShouldBe(1UL);
    }

    [Fact]
    public async Task Send_GivenChunkingThatIsNotEnabled_ShouldNotPlanChunks()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, enableChunking: false);
        var sendOp = CreateSendOp(1, CreatePayload(250));

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        channel.PlanCount.ShouldBe(0);
        channel.Entries.Single().Payload.Length.ShouldBe(250);
    }

    // Upstream: MessageChunkingTest#sendSingleChunk (the chunk fields are set by the caller)
    [Fact]
    public async Task Send_GivenMessageWithChunkFieldsOfTheCaller_ShouldWriteItAsItIs()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var metadata = new MessageMetadata { SequenceId = 1, Uuid = "caller", ChunkId = 1, NumChunksFromMsg = 3, TotalChunkMsgSize = 100 };
        var sendOp = new SendOp(metadata, new ReadOnlySequence<byte>(CreatePayload(250)), new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously));

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var entry = channel.Entries.Single();
        entry.Metadata.Uuid.ShouldBe("caller");
        entry.Metadata.ChunkId.ShouldBe(1);
        entry.Payload.Length.ShouldBe(250);
        messageId.FirstChunkMessageId.ShouldBeNull();
    }

    // Upstream: ProducerImpl#sendAsync (the chunks are planned once, and the messages that are not acknowledged are sent again after a reconnect)
    [Fact]
    public async Task Send_GivenReconnectAfterTheReceiptsOfSomeChunks_ShouldWriteTheChunksThatAreNotAcknowledgedWithTheSamePlan()
    {
        //Arrange
        var first = new FakeProducerChannel { Respond = entry => entry.Metadata.ChunkId < 2 ? FakeProducerChannel.CreateReceipt(entry, (ulong) (entry.Metadata.ChunkId + 1)) : null };
        var second = new FakeProducerChannel();
        var factory = new ChannelFactory(first);
        await using var sut = await CreateSubProducer(first, factory: factory);
        var sendOp = CreateSendOp(1, CreatePayload(350));
        await sut.Send(sendOp, Current.CancellationToken);
        await WaitFor.Until(() => first.Entries.Count == 4 && sendOp.Chunking?.ReceiptedChunks == 2, "the first two chunks are acknowledged and the others are not");

        //Act
        factory.Channel = second;
        await sut.EstablishNewChannel(Current.CancellationToken);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        second.PlanCount.ShouldBe(0);
        second.Entries.Select(entry => entry.Metadata.ChunkId).ShouldBe([2, 3]);
        second.Entries.Select(entry => entry.Metadata.Uuid).Distinct().ShouldBe([first.Entries[0].Metadata.Uuid]);
        second.Entries.Select(entry => entry.Metadata.PublishTime).Distinct().ShouldBe([first.Entries[0].Metadata.PublishTime]);
        second.Entries.SelectMany(entry => entry.Payload).ToArray().ShouldBe(first.Entries.Skip(2).Take(2).SelectMany(entry => entry.Payload).ToArray());
        messageId.GetChunkMessageIds()!.Length.ShouldBe(4);
        messageId.FirstChunkMessageId!.EntryId.ShouldBe(1UL);
    }

    [Fact]
    public async Task Send_GivenReconnectBeforeAnyReceipt_ShouldWriteEveryChunkAgainWithoutPlanningAgain()
    {
        //Arrange
        var first = new FakeProducerChannel { Respond = _ => null };
        var second = new FakeProducerChannel();
        var factory = new ChannelFactory(first);
        await using var sut = await CreateSubProducer(first, factory: factory);
        var sendOp = CreateSendOp(1, CreatePayload(250));
        await sut.Send(sendOp, Current.CancellationToken);
        await WaitFor.Until(() => first.Entries.Count == 3, "all the chunks are written");

        //Act
        factory.Channel = second;
        await sut.EstablishNewChannel(Current.CancellationToken);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        first.PlanCount.ShouldBe(1);
        second.PlanCount.ShouldBe(0);
        second.Entries.Select(entry => entry.Metadata.ChunkId).ShouldBe([0, 1, 2]);
        messageId.GetChunkMessageIds()!.Length.ShouldBe(3);
    }

    // Upstream: ProducerImpl#run(Timeout)
    [Fact]
    public async Task SendTimeout_GivenTimeoutWhileAChunkIsBeingWritten_ShouldFailTheMessageAndNotWriteTheChunksThatAreLeft()
    {
        //Arrange
        var writeGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel { Respond = _ => null, WriteGate = writeGate.Task };
        await using var sut = await CreateSubProducer(channel, sendTimeout: _sendTimeout);
        var sendOp = CreateSendOp(1, CreatePayload(250));

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await Should.ThrowAsync<OperationTimeoutException>(sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        writeGate.SetResult();
        await WaitFor.Until(() => channel.Entries.Count == 1, "the chunk that was being written is recorded");
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken); // The chunks that are left must not be written

        //Assert
        channel.Entries.Count.ShouldBe(1);
        sendOp.IsRemoved.ShouldBeTrue();
    }

    [Fact]
    public async Task SendTimeout_GivenReceiptsOfTheChunksOfAMessageThatTimedOut_ShouldIgnoreThemAndCompleteTheNextMessageWithItsOwnId()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, sendTimeout: TimeSpan.FromSeconds(2));
        var chunkedOp = CreateSendOp(1, CreatePayload(250));
        await sut.Send(chunkedOp, Current.CancellationToken);
        await Should.ThrowAsync<OperationTimeoutException>(chunkedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        var nextOp = CreateSendOp(2, CreatePayload(10));
        await sut.Send(nextOp, Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 4, "the next message is written");
        var entries = channel.Entries;

        //Act
        foreach (var entry in entries.Take(3))
        {
            Answer(entry, 50);
        }

        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var completedBeforeItsReceipt = nextOp.ReceiptTcs.Task.IsCompleted;
        Answer(entries[3], 100);

        //Assert
        var messageId = await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        completedBeforeItsReceipt.ShouldBeFalse();
        messageId.EntryId.ShouldBe(100UL);
        messageId.FirstChunkMessageId.ShouldBeNull();
    }

    // Upstream: TypedMessageBuilderImpl#sendAsync (a send of a transaction is written when its topic is registered)
    [Fact]
    public async Task Send_GivenRegistrationGateThatIsNotCompleted_ShouldNeitherPlanNorWriteChunksBeforeItIs()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOp = CreateSendOp(1, CreatePayload(250), gate.Task);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var planCountBeforeGate = channel.PlanCount;
        var entriesBeforeGate = channel.Entries.Count;
        gate.SetResult();
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        planCountBeforeGate.ShouldBe(0);
        entriesBeforeGate.ShouldBe(0);
        channel.Entries.Count.ShouldBe(3);
        messageId.GetChunkMessageIds()!.Length.ShouldBe(3);
    }

    [Fact]
    public async Task Send_GivenRegistrationGateThatFails_ShouldFailTheMessageWithoutPlanningItAndContinueWithTheNext()
    {
        //Arrange
        var exception = new TransactionCoordinatorClientException("The registration failed");
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var failingOp = CreateSendOp(1, CreatePayload(250), Task.FromException(exception));
        var nextOp = CreateSendOp(2, CreatePayload(250));

        //Act
        await sut.Send(failingOp, Current.CancellationToken);
        await sut.Send(nextOp, Current.CancellationToken);

        //Assert
        (await Should.ThrowAsync<TransactionCoordinatorClientException>(failingOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken))).ShouldBeSameAs(exception);
        await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        channel.PlanCount.ShouldBe(1);
        channel.Entries.Select(entry => entry.SequenceId).Distinct().ShouldBe([2UL]);
    }

    // Upstream: ClientCnx#handleSendError, ProducerImpl#recoverNotAllowedError
    [Fact]
    public async Task Send_GivenNotAllowedSendErrorForAChunk_ShouldFailTheMessageAndContinueWithTheNext()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel
        {
            Respond = entry => entry.SequenceId == 1 && entry.Metadata.ChunkId == 1
                ? FakeProducerChannel.CreateSendError(entry.SequenceId, ServerError.NotAllowedError, "Not allowed")
                : FakeProducerChannel.CreateReceipt(entry, (ulong) (entry.Metadata.HasChunkId ? 10 + entry.Metadata.ChunkId : 100))
        };
        await using var sut = await CreateSubProducer(channel, registerEvent);
        var chunkedOp = CreateSendOp(1, CreatePayload(250));
        var nextOp = CreateSendOp(2, CreatePayload(10));

        //Act
        await sut.Send(chunkedOp, Current.CancellationToken);
        await sut.Send(nextOp, Current.CancellationToken);

        //Assert
        (await Should.ThrowAsync<NotAllowedException>(chunkedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken))).Message.ShouldBe("Not allowed");
        (await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)).EntryId.ShouldBe(100UL);
        registerEvent.ReceivedCalls().Select(x => x.GetArguments()[0]).ShouldNotContain(x => x is ExecutorFaulted || x is SendReceiptWrongOrdering);
    }

    // Upstream: ProducerImpl#sendAsync (InvalidMessageException)
    [Fact]
    public async Task Send_GivenPlanningThatFails_ShouldFailOnlyThatMessageAndNotFaultTheProducer()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var exception = new TooLargeMessageException(20, 10);
        var channel = new FakeProducerChannel { FailPlanning = metadata => metadata.SequenceId == 1 ? exception : null };
        await using var sut = await CreateSubProducer(channel, registerEvent);
        var failingOp = CreateSendOp(1, CreatePayload(250));
        var nextOp = CreateSendOp(2, CreatePayload(250));

        //Act
        await sut.Send(failingOp, Current.CancellationToken);
        await sut.Send(nextOp, Current.CancellationToken);

        //Assert
        (await Should.ThrowAsync<TooLargeMessageException>(failingOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken))).ShouldBeSameAs(exception);
        (await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)).GetChunkMessageIds()!.Length.ShouldBe(3);
        channel.Entries.Select(entry => entry.SequenceId).Distinct().ShouldBe([2UL]);
        registerEvent.ReceivedCalls().Select(x => x.GetArguments()[0]).ShouldNotContain(x => x is ExecutorFaulted || x is ChannelDisconnected);
    }

    // Upstream: MessageChunkingTest#testBlockIfQueueFullWhenChunking (a chunked message takes one place in the queue, so it never waits for room that only its own chunks can make)
    [Fact]
    public async Task Send_GivenMessageWithMoreChunksThanMaxPendingMessages_ShouldCompleteAllMessages()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, maxPendingMessages: 2);
        var sendOps = new[] { CreateSendOp(1, CreatePayload(1000)), CreateSendOp(2, CreatePayload(500)), CreateSendOp(3, CreatePayload(10)), CreateSendOp(4, CreatePayload(300)) };

        //Act
        foreach (var sendOp in sendOps)
        {
            await sut.Send(sendOp, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        }

        //Assert
        var messageIds = await Task.WhenAll(sendOps.Select(sendOp => sendOp.ReceiptTcs.Task)).WaitAsync(_timeout, Current.CancellationToken);
        messageIds.Select(messageId => messageId.GetChunkMessageIds()?.Length).ShouldBe([10, 5, null, 3]);
    }

    [Fact]
    public async Task Send_GivenChunkedMessagesAndPlainMessagesInTurn_ShouldCompleteEveryMessageWithItsOwnId()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1, CreatePayload(10)), CreateSendOp(2, CreatePayload(250)), CreateSendOp(3, CreatePayload(10)), CreateSendOp(4, CreatePayload(150)) };

        //Act
        foreach (var sendOp in sendOps)
        {
            await sut.Send(sendOp, Current.CancellationToken);
        }

        //Assert
        var messageIds = await Task.WhenAll(sendOps.Select(sendOp => sendOp.ReceiptTcs.Task)).WaitAsync(_timeout, Current.CancellationToken);
        messageIds[0].EntryId.ShouldBe(1UL);
        messageIds[0].FirstChunkMessageId.ShouldBeNull();
        messageIds[1].EntryId.ShouldBe(4UL);
        messageIds[1].FirstChunkMessageId!.EntryId.ShouldBe(2UL);
        messageIds[2].EntryId.ShouldBe(5UL);
        messageIds[2].FirstChunkMessageId.ShouldBeNull();
        messageIds[3].EntryId.ShouldBe(7UL);
        messageIds[3].FirstChunkMessageId!.EntryId.ShouldBe(6UL);
    }

    // Upstream: ProducerImpl#sendAsync (a message that was ended by the caller is not written, unless it is split in chunks, which are all written)
    [Fact]
    public async Task Send_GivenCallerThatEndsTheSendAfterTheMessageWasPlanned_ShouldStillWriteAllChunksAndKeepTheQueueInOrder()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel);
        var chunkedOp = CreateSendOp(1, CreatePayload(250));
        var nextOp = CreateSendOp(2, CreatePayload(10));
        await sut.Send(chunkedOp, Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 3, "all the chunks are written");

        //Act
        _ = chunkedOp.ReceiptTcs.TrySetCanceled();
        await sut.Send(nextOp, Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 4, "the next message is written");
        foreach (var entry in channel.Entries)
        {
            Answer(entry, 1);
        }

        //Assert
        (await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)).FirstChunkMessageId.ShouldBeNull();
        channel.Entries.Select(entry => entry.SequenceId).ShouldBe([1UL, 1UL, 1UL, 2UL]);
    }

    [Fact]
    public void MakeChunked_GivenOp_ShouldMakeItAGroupOfOneThatHasNoRegistrationGate()
    {
        //Arrange
        var gate = Task.CompletedTask;
        var sendOp = CreateSendOp(1, CreatePayload(10), gate);
        var state = ChunkedSendState.Plan(new MessageMetadata { SequenceId = 1, ProducerName = "p" }, new ReadOnlySequence<byte>(CreatePayload(250)), 1000, _chunkSize, 0);
        var countBefore = sendOp.BatchCount;
        var gateBefore = sendOp.RegistrationGate;

        //Act
        sendOp.MakeChunked(state);

        //Assert
        countBefore.ShouldBe(0);
        gateBefore.ShouldBeSameAs(gate);
        sendOp.BatchCount.ShouldBe(1);
        sendOp.Chunking.ShouldBeSameAs(state);
        sendOp.RegistrationGate.ShouldBeNull();
        sendOp.Batch.ShouldBeNull();
        sendOp.IsRemoved.ShouldBeFalse();
    }

    [Fact]
    public void LeaveGroup_GivenChunkedOp_ShouldForgetTheChunks()
    {
        //Arrange
        var sendOp = CreateSendOp(1, CreatePayload(10));
        sendOp.MakeChunked(ChunkedSendState.Plan(new MessageMetadata { SequenceId = 1, ProducerName = "p" }, new ReadOnlySequence<byte>(CreatePayload(250)), 1000, _chunkSize, 0));

        //Act
        sendOp.LeaveGroup();

        //Assert
        sendOp.Chunking.ShouldBeNull();
        sendOp.BatchCount.ShouldBe(0);
    }

    [Fact]
    public void MakeRemoved_GivenChunkedOp_ShouldMarkItRemoved()
    {
        //Arrange
        var sendOp = CreateSendOp(1, CreatePayload(10));
        sendOp.MakeChunked(ChunkedSendState.Plan(new MessageMetadata { SequenceId = 1, ProducerName = "p" }, new ReadOnlySequence<byte>(CreatePayload(250)), 1000, _chunkSize, 0));

        //Act
        sendOp.MakeRemoved();

        //Assert
        sendOp.IsRemoved.ShouldBeTrue();
        sendOp.Chunking.ShouldBeNull();
    }

    // Completes the response of an entry that the channel left unanswered, with a receipt of the entry id
    private static void Answer(SentEntry entry, ulong entryId)
        => entry.Response.TrySetResult(FakeProducerChannel.CreateReceipt(entry, entryId));

    private static byte[] CreatePayload(int length)
    {
        var payload = new byte[length];
        for (var i = 0; i < length; ++i)
        {
            payload[i] = (byte) (i % 251);
        }

        return payload;
    }

    private static SendOp CreateSendOp(ulong sequenceId, byte[] payload, Task? registrationGate = null)
        => new(new MessageMetadata { SequenceId = sequenceId }, new ReadOnlySequence<byte>(payload), new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously), registrationGate: registrationGate);

    private static async Task<SubProducer> CreateSubProducer(
        FakeProducerChannel channel,
        IRegisterEvent? registerEvent = null,
        ChannelFactory? factory = null,
        bool enableChunking = true,
        uint maxPendingMessages = 1000,
        int partition = -1,
        TimeSpan? sendTimeout = null)
    {
        registerEvent ??= Substitute.For<IRegisterEvent>();
        factory ??= new ChannelFactory(channel);
        var correlationId = Guid.NewGuid();
        var channelFactory = Substitute.For<IProducerChannelFactory>();
        channelFactory.Create(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(factory.Channel));

        var sut = new SubProducer(
            correlationId,
            registerEvent,
            Substitute.For<IProducerChannel>(),
            new Executor(correlationId, registerEvent, new DefaultExceptionHandler()),
            Substitute.For<IState<ProducerState>>(),
            channelFactory,
            partition,
            maxPendingMessages,
            _topic,
            false,
            BatcherType.Default,
            ProducerOptions<string>.DefaultBatchingMaxMessages,
            ProducerOptions<string>.DefaultBatchingMaxBytes,
            ProducerOptions<string>.DefaultBatchingMaxPublishDelay,
            1,
            sendTimeout ?? TimeSpan.Zero,
            enableChunking,
            _chunkSize);

        await sut.EstablishNewChannel(Current.CancellationToken);

        return sut;
    }

    // The channel that the factory creates, which a test replaces to make the sub producer reconnect to another channel
    private sealed class ChannelFactory
    {
        public ChannelFactory(IProducerChannel channel)
            => Channel = channel;

        public IProducerChannel Channel { get; set; }
    }
}
