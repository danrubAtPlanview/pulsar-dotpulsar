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
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Text;

[Trait("Category", "Unit")]
public sealed class ConsumerChannelChunkingTests
{
    private const ulong Ledger = 1;
    private const long Now = 5_000_000;

    // Upstream: ConsumerImpl#messageReceived
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunksInOrder_ShouldDeliverOneMessageWithTheAssembledPayloadAndTheIdsOfTheChunks()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 10));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 11));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 2, 3, 12));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        actual.MessageId.LedgerId.ShouldBe(Ledger);
        actual.MessageId.EntryId.ShouldBe(12UL);
        actual.MessageId.BatchIndex.ShouldBe(-1);
        actual.MessageId.Topic.ShouldBe(BatchTestData.Topic);
        actual.MessageId.FirstChunkMessageId.ShouldNotBeNull();
        actual.MessageId.FirstChunkMessageId.EntryId.ShouldBe(10UL);
        actual.MessageId.FirstChunkMessageId.BatchIndex.ShouldBe(-1);
        actual.MessageId.FirstChunkMessageId.Topic.ShouldBe(BatchTestData.Topic);
        actual.MessageId.GetChunkMessageIds()!.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
        uut.Acks.ShouldBeEmpty();
        uut.Assembler.PendingCount.ShouldBe(0);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkedMessage_ShouldDeliverTheMetadataOfTheLastChunk()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 10, customize: metadata => metadata.Properties.Add(new KeyValue { Key = "first", Value = "1" })));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 11, customize: metadata =>
        {
            metadata.Properties.Add(new KeyValue { Key = "last", Value = "2" });
            metadata.OrderingKey = Google.Protobuf.ByteString.CopyFrom([7, 8, 9]);
        }));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.Properties.ShouldContainKeyAndValue("last", "2");
        actual.Properties.ShouldNotContainKey("first");
        actual.OrderingKey.ShouldBe(new byte[] { 7, 8, 9 });
    }

    // Upstream: MessageChunkingSharedTest#testInterleavedChunks
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunksOfTwoMessagesInterleaved_ShouldDeliverTheMessagesInTheOrderTheyCompleted()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("A", 0, 2, 1));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("B", 0, 2, 2));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("B", 1, 2, 3));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("A", 1, 2, 4));

        //Act
        var first = await uut.Channel.Receive(Current.CancellationToken);
        var second = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(first).ShouldBe("chunk-B-0|chunk-B-1|");
        Text(second).ShouldBe("chunk-A-0|chunk-A-1|");
        uut.Acks.ShouldBeEmpty();
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk (duplicated chunk)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenDuplicatedChunkFromAnotherEntry_ShouldAcknowledgeTheDuplicateAndStillAssembleTheMessage()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 10));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 11));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 20));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 2, 3, 12));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        AcknowledgedEntries(uut).ShouldBeSequence(20UL);
        uut.Acks.ShouldAllBe(ack => ack.AckType == CommandAck.Types.AckType.Individual && ack.ConsumerId == 1);
    }

    // Upstream: MessageChunkingTest#testResendChunkMessages
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkThatIsRedeliveredFromTheSameEntry_ShouldNotAcknowledgeIt()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 10));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 11));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 11));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 2, 3, 12));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        uut.Acks.ShouldBeEmpty();
    }

    // Upstream: MessageChunkingTest#testResendChunkMessagesWithoutAckHole, ConsumerImpl#doProcessMessageChunk (corrupted first chunk)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenFirstChunkAgainFromAnotherEntry_ShouldAcknowledgeTheChunksThatWereReceivedBefore()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 10));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 20));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 21));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-a-0|chunk-a-1|");
        actual.MessageId.GetChunkMessageIds()!.Select(id => id.EntryId).ShouldBeSequence(20UL, 21UL);
        AcknowledgedEntries(uut).ShouldBeSequence(10UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkWithoutItsFirstChunkAndAckTimeout_ShouldTrackTheChunkAndNotAcknowledgeIt()
    {
        //Arrange
        using var tracker = CreateTracker();
        var uut = CreateUut(tracker: tracker);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 5, publishTime: Now));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 6)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(6UL);
        tracker.Count.ShouldBe(1);
        uut.Acks.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkWithoutItsFirstChunkThatIsOlderThanTheExpireTime_ShouldAcknowledgeTheChunkAndNotTrackIt()
    {
        //Arrange
        using var tracker = CreateTracker();
        var uut = CreateUut(tracker: tracker);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 5, publishTime: Now - 61_000));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 6)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(6UL);
        AcknowledgedEntries(uut).ShouldBeSequence(5UL);
        tracker.Count.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testMaxPendingChunkMessages
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenMoreChunkedMessagesThanMaxPendingAndAutoAckOldest_ShouldAcknowledgeTheChunksOfTheOldestMessage()
    {
        //Arrange
        using var tracker = CreateTracker();
        var uut = CreateUut(tracker: tracker, maxPendingChunkedMessage: 1, autoAckOldestChunkedMessageOnQueueFull: true);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 1, publishTime: Now));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 2, publishTime: Now));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("b", 0, 2, 3, publishTime: Now));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("b", 1, 2, 4, publishTime: Now));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-b-0|chunk-b-1|");
        AcknowledgedEntries(uut).ShouldBeSequence(1UL, 2UL);
        tracker.Count.ShouldBe(0);
    }

    // Upstream: MessageChunkingTest#testMaxPendingChunkMessages
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenMoreChunkedMessagesThanMaxPendingWithoutAutoAckOldest_ShouldTrackTheChunksOfTheOldestMessageInsteadOfAcknowledgingThem()
    {
        //Arrange
        using var tracker = CreateTracker();
        var uut = CreateUut(tracker: tracker, maxPendingChunkedMessage: 1, autoAckOldestChunkedMessageOnQueueFull: false);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 1, publishTime: Now));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 2, publishTime: Now));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("b", 0, 2, 3, publishTime: Now));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("b", 1, 2, 4, publishTime: Now));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-b-0|chunk-b-1|");
        uut.Acks.ShouldBeEmpty();
        tracker.Count.ShouldBe(2);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkThatIsDroppedAndNoAckTimeout_ShouldNotFail()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 5, publishTime: Now));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 6)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(6UL);
        uut.Acks.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenCompressedChunkedMessage_ShouldDecompressTheAssembledPayloadOnceUsingTheMetadataOfTheLastChunk()
    {
        //Arrange
        var decompressed = new List<(string Data, int Size)>();
        var uut = CreateUut(decompressor: CreateDecompressor((data, size) =>
        {
            decompressed.Add((Encoding.UTF8.GetString(data.ToArray()), size));
            return new ReadOnlySequence<byte>("the message"u8.ToArray());
        }));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 10, customize: UseCompression));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 11, customize: UseCompression));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("the message");
        decompressed.ShouldBeSequence(("chunk-a-0|chunk-a-1|", 99));
    }

    // Upstream: ConsumerImpl#messageReceived (decompression failure of an assembled chunked message)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkedMessageThatCanNotBeDecompressed_ShouldRejectTheLastChunkAndAcknowledgeTheOthers()
    {
        //Arrange
        var uut = CreateUut(decompressor: CreateDecompressor((_, _) => throw new InvalidOperationException("corrupted")));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 10, customize: UseCompression));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 11, customize: UseCompression));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 2, 3, 12, customize: UseCompression));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 13)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(13UL);
        var rejected = uut.Acks.Single(ack => ack.HasValidationError);
        rejected.ValidationError.ShouldBe(CommandAck.Types.ValidationError.DecompressionError);
        rejected.MessageId.Select(id => id.EntryId).ShouldBeSequence(12UL);
        var acknowledged = uut.Acks.Single(ack => !ack.HasValidationError);
        acknowledged.AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        acknowledged.MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL);
    }

    // The channel asks for more permits when it has counted as many entries as the last request gave. The first request is counted as half of its permits.
    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunks_ShouldCountEveryChunkEntryAsOnePermit()
    {
        //Arrange
        var uut = CreateUut(prefetchCount: 4);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 3, 1));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 3, 2));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 2, 3, 3));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 4)));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 5)));

        //Act
        _ = await uut.Channel.Receive(Current.CancellationToken);
        var flowsAfterThreeEntries = CountFlows(uut);
        _ = await uut.Channel.Receive(Current.CancellationToken);
        var flowsAfterFourEntries = CountFlows(uut);
        _ = await uut.Channel.Receive(Current.CancellationToken);
        var flowsAfterFiveEntries = CountFlows(uut);

        //Assert
        flowsAfterThreeEntries.ShouldBe(2);
        flowsAfterFourEntries.ShouldBe(2);
        flowsAfterFiveEntries.ShouldBe(3);
    }

    // Upstream: MessageChunkingTest#testOrphanedLastChunkDoesNotStallDispatch
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenOrphanedLastChunks_ShouldStillDeliverTheNextMessage()
    {
        //Arrange
        var uut = CreateUut(prefetchCount: 4);
        for (var i = 0; i < 12; ++i)
        {
            uut.Enqueue(ChunkTestData.CreateChunkPackage($"torn-{i}", 1, 2, (ulong) i + 1, publishTime: Now));
        }

        uut.Enqueue(ChunkTestData.CreateChunkPackage("live", 0, 2, 100));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("live", 1, 2, 101));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-live-0|chunk-live-1|");
        CountFlows(uut).ShouldBeGreaterThanOrEqualTo(3);
    }

    // Upstream: ConsumerImpl#seekAsync
    [Fact(Timeout = 30_000)]
    public async Task Send_GivenSeek_ShouldDropTheIncompleteChunkedMessages()
    {
        //Arrange
        var uut = CreateUut();
        uut.Connection.Send(Arg.Any<CommandSeek>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess() });
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 10, publishTime: Now));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 11)));
        _ = await uut.Channel.Receive(Current.CancellationToken);
        uut.Assembler.PendingCount.ShouldBe(1);

        //Act
        await uut.Channel.Send(new CommandSeek { MessagePublishTime = 1 }, Current.CancellationToken);

        //Assert
        uut.Assembler.PendingCount.ShouldBe(0);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Send_GivenIndividualAcknowledgmentOfAllChunksOfATrackedMessage_ShouldNoLongerWaitForTheMessage()
    {
        //Arrange
        using var tracker = CreateTracker();
        var uut = CreateUut(tracker: tracker);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 10));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 11));
        var message = await uut.Channel.Receive(Current.CancellationToken);
        tracker.Add(message.MessageId);
        var command = new CommandAck { AckType = CommandAck.Types.AckType.Individual };
        ChunkMessageIdExpander.TryExpand(message.MessageId.GetChunkMessageIds(), command.MessageId).ShouldBeTrue();

        //Act
        await uut.Channel.Send(command, Current.CancellationToken);

        //Assert
        tracker.Count.ShouldBe(0);
        uut.Acks.Single().MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL);
    }

    // Upstream: ConsumerImpl#messageReceived (possibleSendToDeadLetterTopicMessages)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenChunkedMessageThatExceededTheRedeliveryCount_ShouldRequestItsRedeliveryWithTheIdsOfTheChunks()
    {
        //Arrange
        var deadLetterHandler = BatchTestData.CreateDeadLetterHandler(1, out var exceeded);
        var uut = CreateUut(deadLetterHandler: deadLetterHandler);
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 0, 2, 10));
        uut.Enqueue(ChunkTestData.CreateChunkPackage("a", 1, 2, 11, redeliveryCount: 2));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 12)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);
        await WaitUntil(() => exceeded.Count == 1);

        //Assert
        actual.MessageId.EntryId.ShouldBe(12UL);
        exceeded.Single().EntryId.ShouldBe(11UL);
        exceeded.Single().GetChunkMessageIds()!.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL);
        deadLetterHandler.Contains(Ledger, 11).ShouldBeTrue();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenMessageThatIsNotChunked_ShouldDeliverAnIdWithoutChunks()
    {
        //Arrange
        var uut = CreateUut();
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 1)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.FirstChunkMessageId.ShouldBeNull();
        actual.MessageId.GetChunkMessageIds().ShouldBeNull();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenBatchThatClaimsToBeChunked_ShouldUnbatchItAndNotAssembleIt()
    {
        //Arrange
        var uut = CreateUut();
        var metadata = BatchTestData.CreateMetadata(2);
        metadata.NumChunksFromMsg = 3;
        metadata.Uuid = "a";
        uut.Enqueue(BatchTestData.CreatePackage(BatchTestData.CreateEntryId(Ledger, 1), metadata, BatchTestData.CreateBatchPayload(2)));

        //Act
        var first = await uut.Channel.Receive(Current.CancellationToken);
        var second = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        first.MessageId.BatchIndex.ShouldBe(0);
        second.MessageId.BatchIndex.ShouldBe(1);
        uut.Assembler.PendingCount.ShouldBe(0);
    }

    private static Context CreateUut(
        uint maxPendingChunkedMessage = 10,
        bool autoAckOldestChunkedMessageOnQueueFull = false,
        UnAckedMessageTracker? tracker = null,
        DeadLetterHandler<byte[]>? deadLetterHandler = null,
        IDecompressorFactory? decompressor = null,
        uint prefetchCount = 100)
    {
        var assembler = ChunkTestData.CreateAssembler(maxPendingChunkedMessage, autoAckOldestChunkedMessageOnQueueFull, TimeSpan.FromSeconds(60), () => Now);
        var acks = new List<CommandAck>();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var queue = new AsyncQueue<MessagePackage>();
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        var batchHandler = new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, deadLetterHandler);
        IDecompressorFactory[] decompressors = decompressor is null ? [] : [decompressor];
        var channel = new ConsumerChannel<byte[]>(1, prefetchCount, queue, connection, batchHandler, messageFactory, decompressors, BatchTestData.Topic, false, false, null, tracker, deadLetterHandler, assembler, null, null, ConsumerCryptoFailureAction.Fail);

        return new Context(channel, queue, connection, acks, assembler);
    }

    private static UnAckedMessageTracker CreateTracker()
        => new(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1), (_, _) => default);

    private static IDecompressorFactory CreateDecompressor(Func<ReadOnlySequence<byte>, int, ReadOnlySequence<byte>> decompress)
    {
        var decompressor = Substitute.For<IDecompress>();
        decompressor.Decompress(Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<int>()).Returns(callInfo => decompress(callInfo.Arg<ReadOnlySequence<byte>>(), callInfo.Arg<int>()));
        var factory = Substitute.For<IDecompressorFactory>();
        factory.CompressionType.Returns(CompressionType.Lz4);
        factory.Create().Returns(decompressor);
        return factory;
    }

    private static void UseCompression(MessageMetadata metadata)
    {
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 99;
    }

    private static string Text(IMessage<byte[]> message)
        => Encoding.UTF8.GetString(message.Data.ToArray());

    private static ulong[] AcknowledgedEntries(Context uut)
        => uut.Acks.Where(ack => !ack.HasValidationError).SelectMany(ack => ack.MessageId).Select(id => id.EntryId).ToArray();

    private static int CountFlows(Context uut)
        => uut.Connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow);

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }
    }

    private sealed class Context
    {
        private readonly AsyncQueue<MessagePackage> _queue;

        public Context(ConsumerChannel<byte[]> channel, AsyncQueue<MessagePackage> queue, IConnection connection, List<CommandAck> acks, ChunkedMessageAssembler assembler)
        {
            Channel = channel;
            _queue = queue;
            Connection = connection;
            Acks = acks;
            Assembler = assembler;
        }

        public ConsumerChannel<byte[]> Channel { get; }
        public IConnection Connection { get; }
        public List<CommandAck> Acks { get; }
        public ChunkedMessageAssembler Assembler { get; }

        public void Enqueue(MessagePackage messagePackage)
            => _queue.Enqueue(messagePackage);
    }
}
