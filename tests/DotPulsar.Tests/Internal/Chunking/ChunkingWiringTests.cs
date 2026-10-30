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
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Text;

// A consumer and a reader that are connected to a connection that a test controls, so the options reach the assembler of the sub-consumer and the sub-reader
[Trait("Category", "Unit")]
public sealed class ChunkingWiringTests
{
    private const string Topic = "persistent://public/default/test";

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Consumer_GivenChunksOfAMessage_ShouldDeliverItAndAcknowledgeEveryChunkWhenTheMessageIsAcknowledged()
    {
        //Arrange
        var broker = new FakeBroker();
        await using var uut = await broker.CreateConsumer(new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 3, 10));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 1, 3, 11));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 2, 3, 12));

        //Act
        var message = await uut.Receive(Current.CancellationToken);
        await uut.Acknowledge(message.MessageId, Current.CancellationToken);

        //Assert
        Encoding.UTF8.GetString(message.Data.ToArray()).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        message.MessageId.FirstChunkMessageId!.EntryId.ShouldBe(10UL);
        broker.Acks.Count.ShouldBe(1);
        broker.Acks[0].AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        broker.Acks[0].MessageId.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Consumer_GivenCumulativeAcknowledgmentOfAChunkedMessage_ShouldAcknowledgeTheLastChunkOnly()
    {
        //Arrange
        var broker = new FakeBroker();
        await using var uut = await broker.CreateConsumer(new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 2, 10));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 1, 2, 11));
        var message = await uut.Receive(Current.CancellationToken);

        //Act
        await uut.AcknowledgeCumulative(message.MessageId, Current.CancellationToken);

        //Assert
        broker.Acks.Single().AckType.ShouldBe(CommandAck.Types.AckType.Cumulative);
        broker.Acks.Single().MessageId.Select(id => id.EntryId).ShouldBeSequence(11UL);
    }

    // Upstream: ConsumerConfigurationData#maxPendingChunkedMessage, #autoAckOldestChunkedMessageOnQueueFull
    [Fact(Timeout = 30_000)]
    public async Task Consumer_GivenMaxPendingChunkedMessageAndAutoAckOldest_ShouldAcknowledgeTheChunksOfTheOldestMessageWhenTheQueueIsFull()
    {
        //Arrange
        var broker = new FakeBroker();
        var options = new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray)
        {
            MaxPendingChunkedMessage = 1,
            AutoAckOldestChunkedMessageOnQueueFull = true
        };
        await using var uut = await broker.CreateConsumer(options);
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 2, 1));
        broker.Deliver(ChunkTestData.CreateChunkPackage("b", 0, 2, 2));
        broker.Deliver(ChunkTestData.CreateChunkPackage("b", 1, 2, 3));

        //Act
        var message = await uut.Receive(Current.CancellationToken);

        //Assert
        Encoding.UTF8.GetString(message.Data.ToArray()).ShouldBe("chunk-b-0|chunk-b-1|");
        broker.Acks.SelectMany(ack => ack.MessageId).Select(id => id.EntryId).ShouldBeSequence(1UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Consumer_GivenMaxPendingChunkedMessageWithoutAutoAckOldest_ShouldNotAcknowledgeTheChunksOfTheOldestMessage()
    {
        //Arrange
        var broker = new FakeBroker();
        var options = new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray) { MaxPendingChunkedMessage = 1 };
        await using var uut = await broker.CreateConsumer(options);
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 2, 1));
        broker.Deliver(ChunkTestData.CreateChunkPackage("b", 0, 2, 2));
        broker.Deliver(ChunkTestData.CreateChunkPackage("b", 1, 2, 3));

        //Act
        _ = await uut.Receive(Current.CancellationToken);

        //Assert
        broker.Acks.ShouldBeEmpty();
    }

    // Upstream: ConsumerImpl#removeExpireIncompleteChunkedMessages
    [Fact(Timeout = 30_000)]
    public async Task Consumer_GivenExpireTimeOfIncompleteChunkedMessage_ShouldAcknowledgeTheChunksOfAnIncompleteMessageWhenItExpires()
    {
        //Arrange
        var broker = new FakeBroker();
        var options = new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray) { ExpireTimeOfIncompleteChunkedMessage = TimeSpan.FromMilliseconds(200) };
        await using var uut = await broker.CreateConsumer(options);

        //Act
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 3, 7));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 1, 3, 8));
        broker.Deliver(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 9)));
        var message = await uut.Receive(Current.CancellationToken);
        await WaitUntil(() => broker.Acks.Count > 0);

        //Assert
        message.MessageId.EntryId.ShouldBe(9UL);
        broker.Acks.Single().AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        broker.Acks.Single().MessageId.Select(id => id.EntryId).ShouldBeSequence(7UL, 8UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Consumer_GivenZeroExpireTimeOfIncompleteChunkedMessage_ShouldNeverAcknowledgeAnIncompleteMessage()
    {
        //Arrange
        var broker = new FakeBroker();
        var options = new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray) { ExpireTimeOfIncompleteChunkedMessage = TimeSpan.Zero };
        await using var uut = await broker.CreateConsumer(options);
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 3, 7));
        broker.Deliver(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 9)));

        //Act
        _ = await uut.Receive(Current.CancellationToken);
        await Task.Delay(500, Current.CancellationToken);

        //Assert
        broker.Acks.ShouldBeEmpty();
    }

    // Upstream: MessageChunkingTest#testReaderChunkingConfiguration
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenChunksOfAMessage_ShouldDeliverIt()
    {
        //Arrange
        var broker = new FakeBroker();
        await using var uut = await broker.CreateReader(new ReaderOptions<byte[]>(MessageId.Earliest, Topic, DotPulsar.Schema.ByteArray));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 3, 10));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 1, 3, 11));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 2, 3, 12));

        //Act
        var message = await uut.Receive(Current.CancellationToken);

        //Assert
        Encoding.UTF8.GetString(message.Data.ToArray()).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        message.MessageId.EntryId.ShouldBe(12UL);
        message.MessageId.FirstChunkMessageId!.EntryId.ShouldBe(10UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenMaxPendingChunkedMessage_ShouldDropTheOldestIncompleteMessage()
    {
        //Arrange
        var broker = new FakeBroker();
        var options = new ReaderOptions<byte[]>(MessageId.Earliest, Topic, DotPulsar.Schema.ByteArray) { MaxPendingChunkedMessage = 1 };
        await using var uut = await broker.CreateReader(options);
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 2, 1));
        broker.Deliver(ChunkTestData.CreateChunkPackage("b", 0, 2, 2));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 1, 2, 3));
        broker.Deliver(ChunkTestData.CreateChunkPackage("b", 1, 2, 4));

        //Act
        var message = await uut.Receive(Current.CancellationToken);

        //Assert
        Encoding.UTF8.GetString(message.Data.ToArray()).ShouldBe("chunk-b-0|chunk-b-1|");
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenZeroMaxPendingChunkedMessage_ShouldNotLimitTheIncompleteMessages()
    {
        //Arrange
        var broker = new FakeBroker();
        var options = new ReaderOptions<byte[]>(MessageId.Earliest, Topic, DotPulsar.Schema.ByteArray) { MaxPendingChunkedMessage = 0 };
        await using var uut = await broker.CreateReader(options);
        for (var i = 0; i < 12; ++i)
        {
            broker.Deliver(ChunkTestData.CreateChunkPackage($"m{i}", 0, 2, (ulong) i + 1));
        }

        broker.Deliver(ChunkTestData.CreateChunkPackage("m0", 1, 2, 100));

        //Act
        var message = await uut.Receive(Current.CancellationToken);

        //Assert
        Encoding.UTF8.GetString(message.Data.ToArray()).ShouldBe("chunk-m0-0|chunk-m0-1|");
    }

    // Upstream: MessageChunkingTest#testSeekChunkMessages (the reader starts at the first chunk of the message)
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenStartMessageIdThatIsChunked_ShouldSubscribeFromTheFirstChunk()
    {
        //Arrange
        var broker = new FakeBroker();
        var startMessageId = MessageIdInternals.CreateChunked(Topic, ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11), ChunkTestData.CreateEntryId(1, 12));

        //Act
        await using var uut = await broker.CreateReader(new ReaderOptions<byte[]>(startMessageId, Topic, DotPulsar.Schema.ByteArray));

        //Assert
        broker.Subscriptions.Single().StartMessageId.EntryId.ShouldBe(10UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenStartMessageIdThatIsNotChunked_ShouldSubscribeFromIt()
    {
        //Arrange
        var broker = new FakeBroker();

        //Act
        await using var uut = await broker.CreateReader(new ReaderOptions<byte[]>(new MessageId(1, 12, 0, -1, Topic), Topic, DotPulsar.Schema.ByteArray));

        //Assert
        broker.Subscriptions.Single().StartMessageId.EntryId.ShouldBe(12UL);
    }

    // Upstream: ConsumerImpl#seekAsync(MessageId)
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenSeekToAChunkedMessageId_ShouldSeekToTheFirstChunk()
    {
        //Arrange
        var broker = new FakeBroker();
        await using var uut = await broker.CreateReader(new ReaderOptions<byte[]>(MessageId.Earliest, Topic, DotPulsar.Schema.ByteArray));
        var chunkedMessageId = MessageIdInternals.CreateChunked(Topic, ChunkTestData.CreateEntryId(1, 10), ChunkTestData.CreateEntryId(1, 11));

        //Act
        await uut.Seek(chunkedMessageId, Current.CancellationToken);

        //Assert
        broker.Seeks.Single().MessageId.EntryId.ShouldBe(10UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Reader_GivenSeek_ShouldDropTheIncompleteChunkedMessages()
    {
        //Arrange
        var broker = new FakeBroker();
        await using var uut = await broker.CreateReader(new ReaderOptions<byte[]>(MessageId.Earliest, Topic, DotPulsar.Schema.ByteArray));
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 0, 2, 1));
        broker.Deliver(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 2)));
        _ = await uut.Receive(Current.CancellationToken);
        await uut.Seek(MessageId.Earliest, Current.CancellationToken);
        broker.Deliver(ChunkTestData.CreateChunkPackage("a", 1, 2, 3));
        broker.Deliver(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(1, 4)));

        //Act
        var message = await uut.Receive(Current.CancellationToken);

        //Assert
        message.MessageId.EntryId.ShouldBe(4UL);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, Current.CancellationToken);
        }
    }

    private sealed class FakeBroker
    {
        private readonly object _lock;
        private readonly List<CommandAck> _acks;
        private readonly List<CommandSubscribe> _subscriptions;
        private readonly List<CommandSeek> _seeks;
        private readonly IConnection _connection;
        private readonly IConnectionPool _connectionPool;
        private IChannel? _channel;

        public FakeBroker()
        {
            _lock = new();
            _acks = [];
            _subscriptions = [];
            _seeks = [];
            _connection = Substitute.For<IConnection>();

            _connection.Send(Arg.Any<CommandSubscribe>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
            {
                var channel = callInfo.Arg<IChannel>();

                lock (_lock)
                {
                    _subscriptions.Add(callInfo.Arg<CommandSubscribe>().Clone());
                    _channel = channel;
                }

                channel.Connected();
                return Task.FromResult(new SubscribeResponse(7));
            });

            _connection.Send(Arg.Do<CommandAck>(ack =>
            {
                lock (_lock)
                    _acks.Add(ack.Clone());
            }), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

            _connection.Send(Arg.Do<CommandSeek>(seek =>
            {
                lock (_lock)
                    _seeks.Add(seek.Clone());
            }), Arg.Any<CancellationToken>()).Returns(new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess() });

            _connection.Send(Arg.Any<CommandCloseConsumer>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand());

            _connectionPool = Substitute.For<IConnectionPool>();
            _connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(_connection));
            _connectionPool.GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(0));
        }

        public List<CommandAck> Acks
        {
            get
            {
                lock (_lock)
                    return [.. _acks];
            }
        }

        public List<CommandSubscribe> Subscriptions
        {
            get
            {
                lock (_lock)
                    return [.. _subscriptions];
            }
        }

        public List<CommandSeek> Seeks
        {
            get
            {
                lock (_lock)
                    return [.. _seeks];
            }
        }

        public async Task<IConsumer<byte[]>> CreateConsumer(ConsumerOptions<byte[]> options)
        {
            var consumer = new Consumer<byte[]>(new Uri("pulsar://localhost:6650"), options, _connectionPool, new DefaultExceptionHandler(), TimeSpan.FromSeconds(30));
            _ = await consumer.StateChangedTo(ConsumerState.Active, Current.CancellationToken);
            return consumer;
        }

        public async Task<IReader<byte[]>> CreateReader(ReaderOptions<byte[]> options)
        {
            var reader = new Reader<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), _connectionPool);
            _ = await reader.StateChangedTo(ReaderState.Connected, Current.CancellationToken);
            return reader;
        }

        public void Deliver(MessagePackage messagePackage)
        {
            IChannel? channel;

            lock (_lock)
                channel = _channel;

            channel!.Received(messagePackage);
        }
    }
}
