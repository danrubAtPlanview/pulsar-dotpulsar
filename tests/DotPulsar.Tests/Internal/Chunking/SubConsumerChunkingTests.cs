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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class SubConsumerChunkingTests
{
    private const string Topic = "persistent://public/default/test";

    private readonly IConsumerChannel<byte[]> _channel;

    public SubConsumerChunkingTests()
        => _channel = Substitute.For<IConsumerChannel<byte[]>>();

    // Upstream: PersistentAcknowledgmentsGroupingTracker#flushAsync
    [Fact]
    public async Task Acknowledge_GivenChunkedMessageId_ShouldAcknowledgeEveryChunkIndividually()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();
        var chunkedMessageId = CreateChunkedMessageId(10, 11, 12);

        //Act
        await uut.Acknowledge(chunkedMessageId, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].MessageId.Select(id => (id.LedgerId, id.EntryId)).ShouldBeSequence((1UL, 10UL), (1UL, 11UL), (1UL, 12UL));
    }

    [Fact]
    public async Task Acknowledge_GivenChunkedMessageIdFollowedByAnotherMessageId_ShouldNotChangeTheIdsOfTheChunksAndOnlyAcknowledgeTheOtherId()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();
        var chunkedMessageId = CreateChunkedMessageId(10, 11, 12);
        await uut.Acknowledge(chunkedMessageId, Current.CancellationToken);

        //Act
        await uut.Acknowledge(new MessageId(2, 20, 0, -1, Topic), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(2);
        sent[1].MessageId.Select(id => (id.LedgerId, id.EntryId)).ShouldBeSequence((2UL, 20UL));
        chunkedMessageId.GetChunkMessageIds()!.Select(id => (id.LedgerId, id.EntryId)).ShouldBeSequence((1UL, 10UL), (1UL, 11UL), (1UL, 12UL));
    }

    [Fact]
    public async Task Acknowledge_GivenMessageIdThatIsNotChunkedAfterChunkedMessageId_ShouldAcknowledgeBothCorrectlyEveryTime()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();
        var plain = new MessageId(2, 20, 0, -1, Topic);

        //Act
        await uut.Acknowledge(plain, Current.CancellationToken);
        await uut.Acknowledge(CreateChunkedMessageId(10, 11), Current.CancellationToken);
        await uut.Acknowledge(plain, Current.CancellationToken);

        //Assert
        sent.Select(ack => ack.MessageId.Count).ShouldBeSequence(1, 2, 1);
    }

    // Upstream: PersistentAcknowledgmentsGroupingTracker#flushAsync (cumulative acknowledgment of a chunked message)
    [Fact]
    public async Task AcknowledgeCumulative_GivenChunkedMessageId_ShouldAcknowledgeTheLastChunkOnly()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();

        //Act
        await uut.AcknowledgeCumulative(CreateChunkedMessageId(10, 11, 12), Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId.Select(id => (id.LedgerId, id.EntryId)).ShouldBeSequence((1UL, 12UL));
    }

    [Fact]
    public async Task Acknowledge_GivenListWithChunkedAndOtherMessageIds_ShouldExpandOnlyTheChunkedOnes()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();
        MessageId[] messageIds = [new MessageId(1, 5, 0, -1, Topic), CreateChunkedMessageId(10, 11), new MessageId(1, 6, 0, -1, Topic)];

        //Act
        await uut.Acknowledge(messageIds, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(5UL, 10UL, 11UL, 6UL);
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse(MessageId, ...)
    [Fact]
    public async Task Acknowledge_GivenChunkedMessageIdAndTransaction_ShouldAcknowledgeEveryChunkInOneCommandWithTheTransaction()
    {
        //Arrange
        var sent = CaptureTransactionalAcknowledgments();
        var transaction = CreateTransaction();
        await using var uut = CreateUut();

        //Act
        await uut.Acknowledge(CreateChunkedMessageId(10, 11, 12), transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].TxnidMostBits.ShouldBe(0UL);
        sent[0].TxnidLeastBits.ShouldBe(5UL);
        sent[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
    }

    // C# deviation: the list overload of Java does not expand chunked messages
    [Fact]
    public async Task Acknowledge_GivenChunkedMessageIdsAndTransaction_ShouldAcknowledgeEveryChunkInOneCommandWithTheTransaction()
    {
        //Arrange
        var sent = CaptureTransactionalAcknowledgments();
        var transaction = CreateTransaction();
        await using var uut = CreateUut();

        //Act
        await uut.Acknowledge([CreateChunkedMessageId(10, 11), new MessageId(1, 20, 0, -1, Topic)], transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 20UL);
    }

    // Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse(MessageId, ...) (cumulative acknowledgments acknowledge the id itself)
    [Fact]
    public async Task AcknowledgeCumulative_GivenChunkedMessageIdAndTransaction_ShouldAcknowledgeTheLastChunkOnly()
    {
        //Arrange
        var sent = CaptureTransactionalAcknowledgments();
        var transaction = CreateTransaction();
        await using var uut = CreateUut();

        //Act
        await uut.AcknowledgeCumulative(CreateChunkedMessageId(10, 11, 12), transaction, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        sent[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(12UL);
    }

    [Fact]
    public async Task Acknowledge_GivenChunkedMessageIdAndTransactionAndTrackedMessage_ShouldNoLongerWaitForTheMessage()
    {
        //Arrange
        _ = CaptureTransactionalAcknowledgments();
        var transaction = CreateTransaction();
        using var tracker = new UnAckedMessageTracker(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1), (_, _) => default);
        await using var uut = CreateUut(tracker: tracker);
        var chunkedMessageId = CreateChunkedMessageId(10, 11, 12);
        uut.Track(CreateMessage(chunkedMessageId));
        tracker.Count.ShouldBe(1);

        //Act
        await uut.Acknowledge(chunkedMessageId, transaction, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(0);
    }

    // Upstream: UnAckedMessageTracker#addChunkedMessageIdsAndRemoveFromSequenceMap
    [Theory]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task RedeliverUnacknowledgedMessages_GivenChunkedMessageId_ShouldRedeliverEveryChunk(SubscriptionType subscriptionType)
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        await using var uut = CreateUut(subscriptionType);

        //Act
        await uut.RedeliverUnacknowledgedMessages([CreateChunkedMessageId(10, 11, 12)], Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Select(id => (id.LedgerId, id.EntryId)).ShouldBeSequence((1UL, 10UL), (1UL, 11UL), (1UL, 12UL));
        sent[0].MessageIds.ShouldAllBe(id => !id.HasBatchIndex);
    }

    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenChunkedMessageIdAndItsChunksAndOtherIds_ShouldSendEveryEntryOnce()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        await using var uut = CreateUut(SubscriptionType.Shared);
        MessageId[] messageIds =
        [
            new(1, 11, 0, -1, Topic),
            CreateChunkedMessageId(10, 11, 12),
            new(1, 13, 0, 2, Topic),
            CreateChunkedMessageId(10, 11, 12)
        ];

        //Act
        await uut.RedeliverUnacknowledgedMessages(messageIds, Current.CancellationToken);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].MessageIds.Select(id => id.EntryId).ShouldBeSequence(11UL, 10UL, 12UL, 13UL);
    }

    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenChunkedMessageIdWithMoreThanOneThousandChunks_ShouldSendTheChunksInCommandsOfOneThousand()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        await using var uut = CreateUut(SubscriptionType.Shared);
        var chunkedMessageId = CreateChunkedMessageId(Enumerable.Range(0, 1500).Select(entryId => (ulong) entryId).ToArray());

        //Act
        await uut.RedeliverUnacknowledgedMessages([chunkedMessageId], Current.CancellationToken);

        //Assert
        sent.Select(command => command.MessageIds.Count).ShouldBeSequence(1000, 500);
    }

    // Upstream: ConsumerImpl#redeliverUnacknowledgedMessages(Set), ConsumerImpl#processPossibleToDLQ
    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenDeadLetterCandidateThatIsChunked_ShouldDeadLetterItAcknowledgeEveryChunkAndRedeliverNone()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var acknowledged = CaptureAcknowledgments();
        var (deadLetterHandler, producer) = CreateDeadLetterHandler();
        await using var uut = CreateUut(SubscriptionType.Shared, deadLetterHandler: deadLetterHandler);
        var chunkedMessageId = CreateChunkedMessageId(10, 11, 12);
        deadLetterHandler.Register(1, 12, CreateMessage(chunkedMessageId));

        //Act
        await uut.RedeliverUnacknowledgedMessages([chunkedMessageId], Current.CancellationToken);

        //Assert
        await producer.Received(1).Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>());
        acknowledged.Count.ShouldBe(1);
        acknowledged[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        acknowledged[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
        sent.ShouldBeEmpty();
    }

    [Fact]
    public async Task RedeliverUnacknowledgedMessages_GivenChunkedMessageIdThatIsNotADeadLetterCandidate_ShouldRedeliverEveryChunk()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var (deadLetterHandler, producer) = CreateDeadLetterHandler();
        await using var uut = CreateUut(SubscriptionType.Shared, deadLetterHandler: deadLetterHandler);

        //Act
        await uut.RedeliverUnacknowledgedMessages([CreateChunkedMessageId(10, 11, 12)], Current.CancellationToken);

        //Assert
        await producer.DidNotReceive().Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>());
        sent.Single().MessageIds.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
    }

    // Upstream: NegativeAcksTracker#add (a negative acknowledgment of a chunked message redelivers every chunk)
    [Fact]
    public async Task NegativeAcknowledge_GivenChunkedMessageIdThatIsDue_ShouldRedeliverEveryChunk()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var timers = new FakeOneShotTimerFactory();
        var clock = new FakeMonotonicClock();
        await using var uut = CreateUut(SubscriptionType.Shared, negativeAcknowledgeTrackerFactory: redeliver => new NegativeAcknowledgeTracker(TimeSpan.FromSeconds(1), redeliver, () => clock.Now, timers.Create));
        await uut.NegativeAcknowledge(CreateChunkedMessageId(10, 11, 12), Current.CancellationToken);

        //Act
        clock.Advance(TimeSpan.FromSeconds(2));
        timers.Timer.Fire();
        await WaitUntil(() => sent.Count == 1);

        //Assert
        sent.Single().MessageIds.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
    }

    // Upstream: UnAckedMessageTracker (a message that is not acknowledged in time is redelivered with all its chunks)
    [Fact]
    public async Task Track_GivenChunkedMessageThatIsNotAcknowledgedInTime_ShouldRedeliverEveryChunk()
    {
        //Arrange
        var sent = CaptureRedeliverCommands();
        var timers = new FakeOneShotTimerFactory();
        SubConsumer<byte[]>? subConsumer = null;
        using var tracker = new UnAckedMessageTracker(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), (ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct), timers.Create);
        await using var uut = CreateUut(SubscriptionType.Shared, tracker: tracker);
        subConsumer = uut;
        tracker.Start();
        uut.Track(CreateMessage(CreateChunkedMessageId(10, 11, 12)));

        //Act
        for (var i = 0; i < 3; ++i)
        {
            timers.Timer.Fire();
        }

        await WaitUntil(() => sent.Count == 1);

        //Assert
        sent.Single().MessageIds.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId)
    [Fact]
    public async Task Seek_GivenChunkedMessageId_ShouldSeekToTheFirstChunk()
    {
        //Arrange
        var sent = CaptureSeekCommands();
        await using var uut = CreateUut();

        //Act
        await uut.Seek(CreateChunkedMessageId(10, 11, 12), Current.CancellationToken);

        //Assert
        sent.Single().MessageId.EntryId.ShouldBe(10UL);
    }

    [Fact]
    public async Task Seek_GivenMessageIdThatIsNotChunked_ShouldSeekToIt()
    {
        //Arrange
        var sent = CaptureSeekCommands();
        await using var uut = CreateUut();

        //Act
        await uut.Seek(new MessageId(1, 12, 0, -1, Topic), Current.CancellationToken);

        //Assert
        sent.Single().MessageId.EntryId.ShouldBe(12UL);
    }

    // Upstream: ConsumerImpl#removeExpireIncompleteChunkedMessages
    [Fact]
    public async Task AcknowledgeChunks_GivenChunkIds_ShouldAcknowledgeThemIndividually()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();
        MessageIdData[] chunkIds = [ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11)];

        //Act
        await uut.AcknowledgeChunks(chunkIds);

        //Assert
        sent.Count.ShouldBe(1);
        sent[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        sent[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL);
    }

    [Fact]
    public async Task AcknowledgeChunks_GivenDisposedConsumer_ShouldNotAcknowledgeAnything()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        var uut = CreateUut();
        await uut.DisposeAsync();

        //Act
        await uut.AcknowledgeChunks([ChunkTestData.CreateEntryId(1, 10)]);

        //Assert
        sent.ShouldBeEmpty();
    }

    [Fact]
    public async Task AcknowledgeChunks_GivenFaultedConsumer_ShouldNotAcknowledgeAnything()
    {
        //Arrange
        var sent = CaptureAcknowledgments();
        await using var uut = CreateUut();
        await uut.ChannelFaulted(new Exception("fault"));

        //Act
        await uut.AcknowledgeChunks([ChunkTestData.CreateEntryId(1, 10)]);

        //Assert
        sent.ShouldBeEmpty();
    }

    // Upstream: ConsumerImpl#closeConsumerTasks
    [Fact]
    public async Task DisposeAsync_ShouldDisposeTheChunkAssembler()
    {
        //Arrange
        var assembler = ChunkTestData.CreateAssembler();
        var uut = CreateUut(assembler: assembler);

        //Act
        await uut.DisposeAsync();

        //Assert
        FeedTwoChunks(assembler).ShouldBeNull();
    }

    [Fact]
    public async Task ChannelFaulted_ShouldDisposeTheChunkAssembler()
    {
        //Arrange
        var assembler = ChunkTestData.CreateAssembler();
        await using var uut = CreateUut(assembler: assembler);

        //Act
        await uut.ChannelFaulted(new Exception("fault"));

        //Assert
        FeedTwoChunks(assembler).ShouldBeNull();
    }

    [Fact]
    public async Task DisposeAsync_GivenAssemblerThatIsNotDisposed_ShouldAssembleChunks()
    {
        //Arrange
        var assembler = ChunkTestData.CreateAssembler();
        await using var uut = CreateUut(assembler: assembler);

        //Act
        var actual = FeedTwoChunks(assembler);

        //Assert
        actual.ShouldNotBeNull();
    }

    private static AssembledChunkedMessage? FeedTwoChunks(ChunkedMessageAssembler assembler)
    {
        List<MessageIdData> acks = [];
        List<MessageIdData> track = [];
        assembler.Process(ChunkTestData.CreateEntryId(1, 1), ChunkTestData.CreateChunkMetadata("a", 0, 2), new ReadOnlySequence<byte>([1]), acks, track);
        return assembler.Process(ChunkTestData.CreateEntryId(1, 2), ChunkTestData.CreateChunkMetadata("a", 1, 2), new ReadOnlySequence<byte>([2]), acks, track);
    }

    private SubConsumer<byte[]> CreateUut(
        SubscriptionType subscriptionType = SubscriptionType.Exclusive,
        UnAckedMessageTracker? tracker = null,
        DeadLetterHandler<byte[]>? deadLetterHandler = null,
        ChunkedMessageAssembler? assembler = null,
        Func<Func<MessageId[], CancellationToken, ValueTask>, NegativeAcknowledgeTracker>? negativeAcknowledgeTrackerFactory = null)
    {
        var correlationId = Guid.NewGuid();
        var eventRegister = Substitute.For<IRegisterEvent>();
        var executor = new Executor(correlationId, eventRegister, new DefaultExceptionHandler());

        SubConsumer<byte[]>? subConsumer = null;
        subConsumer = new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            subscriptionType,
            Topic,
            eventRegister,
            _channel,
            executor,
            Substitute.For<IState<ConsumerState>>(),
            Substitute.For<IConsumerChannelFactory<byte[]>>(),
            false,
            TimeSpan.FromSeconds(30),
            new ConsumerEpoch(),
            tracker,
            negativeAcknowledgeTrackerFactory?.Invoke((ids, ct) => subConsumer!.RedeliverUnacknowledgedMessages(ids, ct)) ?? new NegativeAcknowledgeTracker(TimeSpan.FromMinutes(1), (_, _) => default),
            deadLetterHandler,
            assembler ?? ChunkTestData.CreateAssembler());

        return subConsumer;
    }

    private static MessageId CreateChunkedMessageId(params ulong[] entryIds)
        => MessageIdInternals.CreateChunked(Topic, entryIds.Select(entryId => new MessageIdData { LedgerId = 1, EntryId = entryId, Partition = 0, BatchIndex = -1 }).ToArray());

    private static IMessage<byte[]> CreateMessage(MessageId messageId)
        => new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray).Create(messageId, 0, new ReadOnlySequence<byte>([1, 2, 3]), new MessageMetadata { ProducerName = "producer", SequenceId = 1, PublishTime = 1 });

    private static Transaction CreateTransaction()
        => new(Substitute.For<ITransactionCoordinatorClient>(), new TransactionId(0, 5), TimeSpan.FromMinutes(5));

    private List<CommandAck> CaptureAcknowledgments()
    {
        var commands = new List<CommandAck>();
        _channel.Send(Arg.Do<CommandAck>(command => commands.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.FromResult<BaseCommand?>(null));
        return commands;
    }

    private List<CommandAck> CaptureTransactionalAcknowledgments()
    {
        var commands = new List<CommandAck>();
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse { ConsumerId = 1, RequestId = 1 } };
        _channel.SendTransactional(Arg.Do<CommandAck>(command => commands.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(response);
        return commands;
    }

    private List<CommandRedeliverUnacknowledgedMessages> CaptureRedeliverCommands()
    {
        var commands = new List<CommandRedeliverUnacknowledgedMessages>();
        _channel.Send(Arg.Do<CommandRedeliverUnacknowledgedMessages>(command => commands.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return commands;
    }

    private List<CommandSeek> CaptureSeekCommands()
    {
        var commands = new List<CommandSeek>();
        _channel.Send(Arg.Do<CommandSeek>(command => commands.Add(command.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return commands;
    }

    private static (DeadLetterHandler<byte[]> Handler, IProducer<ReadOnlySequence<byte>> Producer) CreateDeadLetterHandler()
    {
        var producer = Substitute.For<IProducer<ReadOnlySequence<byte>>>();
        producer.State.IsFinalState().Returns(false);
        producer.DisposeAsync().Returns(default(ValueTask));
        producer.Send(Arg.Any<DotPulsar.MessageMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<MessageId>(new MessageId(9, 9, 0, -1)));
        var schemaInfo = new SchemaInfo("schema", Array.Empty<byte>(), SchemaType.String, System.Collections.Immutable.ImmutableDictionary<string, string>.Empty);
        var handler = new DeadLetterHandler<byte[]>(new DeadLetterPolicy(3), Topic, "subscription", "consumer", schemaInfo, (_, _) => producer);
        return (handler, producer);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }
    }
}
