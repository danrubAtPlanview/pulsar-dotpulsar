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
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Transactions;

// A producer with chunking enabled, with the real channel of the producer and a connection that records what it is asked to write
[Trait("Category", "Unit")]
public sealed class ProducerChunkingTests
{
    private const string _topic = "persistent://public/default/test";
    private const int _maxMessageSize = 300;
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    // Upstream: ProducerImpl#sendAsync
    [Fact]
    public async Task Send_GivenMessageAboveMaxMessageSize_ShouldWriteChunksWithTheSameSequenceIdThatTheConnectionDoesNotCheck()
    {
        //Arrange
        var connection = CreateConnection();
        await using var producer = await CreateConnectedProducer(connection);
        var payload = CreatePayload(1000);

        //Act
        var messageId = await producer.Send(new MessageMetadata(), payload, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent;
        sent.Count.ShouldBeGreaterThan(3);
        sent.Select(message => message.SequenceId).Distinct().Count().ShouldBe(1);
        sent.Select(message => message.ChunkId).ShouldBe(Enumerable.Range(0, sent.Count).Select(chunkId => (int?) chunkId));
        sent.ShouldAllBe(message => message.IsChunk && message.SkipSizeCheck && message.NumChunksFromMsg == sent.Count && message.TotalChunkMsgSize == 1000);
        sent.Select(message => message.Uuid).Distinct().Count().ShouldBe(1);
        sent.SelectMany(message => message.Payload).ToArray().ShouldBe(payload);
        messageId.FirstChunkMessageId.ShouldNotBeNull();
        messageId.FirstChunkMessageId.ShouldNotBe(messageId);
        messageId.GetChunkMessageIds()!.Length.ShouldBe(sent.Count);
    }

    [Fact]
    public async Task Send_GivenChunkMaxMessageSize_ShouldWriteChunksOfThatSize()
    {
        //Arrange
        var connection = CreateConnection();
        await using var producer = await CreateConnectedProducer(connection, chunkMaxMessageSize: 100);

        //Act
        _ = await producer.Send(new MessageMetadata(), CreatePayload(1000), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        connection.Sent.Select(message => message.Payload.Length).ShouldBe(Enumerable.Repeat(100, 10));
    }

    [Fact]
    public async Task Send_GivenMessageThatFitsInOneFrame_ShouldWriteItWithoutChunkFields()
    {
        //Arrange
        var connection = CreateConnection();
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        var messageId = await producer.Send(new MessageMetadata(), CreatePayload(10), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.IsChunk.ShouldBeFalse();
        sent.ChunkId.ShouldBeNull();
        sent.Uuid.ShouldBeNull();
        messageId.FirstChunkMessageId.ShouldBeNull();
    }

    // Upstream: ProducerImpl#isChunkingEnabled, MessageChunkingTest#testLargeMessageOnNonPersistentTopicIsSentOnceWithoutChunking
    [Fact]
    public async Task Send_GivenNonPersistentTopic_ShouldWriteOneMessageThatTheConnectionChecks()
    {
        //Arrange
        var connection = CreateConnection();
        await using var producer = await CreateConnectedProducer(connection, "non-persistent://public/default/test", 100);
        var payload = CreatePayload(500);

        //Act
        var messageId = await producer.Send(new MessageMetadata(), payload, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.Payload.ShouldBe(payload);
        sent.IsChunk.ShouldBeFalse();
        sent.ChunkId.ShouldBeNull();
        sent.SkipSizeCheck.ShouldBeFalse();
        messageId.FirstChunkMessageId.ShouldBeNull();
    }

    [Fact]
    public async Task Send_GivenChunkingNotEnabled_ShouldWriteOneMessageThatTheConnectionChecks()
    {
        //Arrange
        var connection = CreateConnection();
        await using var producer = await CreateConnectedProducer(connection, enableChunking: false);
        var payload = CreatePayload(500);

        //Act
        _ = await producer.Send(new MessageMetadata(), payload, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.Payload.ShouldBe(payload);
        sent.SkipSizeCheck.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenMessageMetadata_ShouldNotChangeIt()
    {
        //Arrange
        var connection = CreateConnection();
        await using var producer = await CreateConnectedProducer(connection);
        var metadata = new MessageMetadata { Key = "key" };

        //Act
        _ = await producer.Send(metadata, CreatePayload(1000), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        var secondMessageId = await producer.Send(metadata, CreatePayload(1000), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        metadata.Key.ShouldBe("key");
        secondMessageId.FirstChunkMessageId.ShouldNotBeNull();
        connection.Sent.Select(message => message.Uuid).Distinct().Count().ShouldBe(2);
    }

    // Upstream: TransactionProduceTest, ProducerImpl#internalSendWithTxnAsync (every chunk carries the transaction, and the commit waits for the last chunk)
    [Fact]
    public async Task Send_GivenChunkedMessageOfTransaction_ShouldSetTheTransactionIdsOnEveryChunkAndCommitAfterTheLastReceipt()
    {
        //Arrange
        var connection = CreateConnection();
        connection.HoldResponses();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = new Transaction(tcClient, new TransactionId(0, 5), TimeSpan.FromMinutes(1));
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        await producer.SendChannel.Send(new MessageMetadata { Transaction = transaction }, CreatePayload(1000), null, Current.CancellationToken);
        var commit = transaction.Commit(Current.CancellationToken).AsTask();
        await WaitFor.Until(() => connection.Sent.Count > 3, "the chunks are written");
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var isCompletedBeforeReceipts = commit.IsCompleted;
        connection.ReleaseResponses();
        await commit.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        isCompletedBeforeReceipts.ShouldBeFalse();
        connection.Sent.ShouldAllBe(message => message.CommandTxnidMostBits == 0 && message.CommandTxnidLeastBits == 5 && message.CommandIsTransactional && message.MetadataIsTransactional);
        await tcClient.Received(1).Commit(transaction.Id, CancellationToken.None);
    }

    [Fact]
    public async Task Send_GivenChunkedMessageAndPartitionedTopic_ShouldCompleteWithTheIdsOfThePartition()
    {
        //Arrange
        var connection = CreateConnection(2);
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        var first = await producer.Send(new MessageMetadata(), CreatePayload(1000), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        var second = await producer.Send(new MessageMetadata(), CreatePayload(1000), Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        new[] { first.Partition, second.Partition }.Order().ShouldBe([0, 1]);
        first.FirstChunkMessageId!.Partition.ShouldBe(first.Partition);
        second.GetChunkMessageIds()!.ShouldAllBe(chunk => chunk.Partition == second.Partition);
    }

    private static FakeProducerConnection CreateConnection(uint numberOfPartitions = 0)
    {
        var connection = new FakeProducerConnection(numberOfPartitions);
        connection.Connection.MaxMessageSize.Returns(_maxMessageSize);
        return connection;
    }

    private static byte[] CreatePayload(int length)
    {
        var payload = new byte[length];
        for (var i = 0; i < length; ++i)
        {
            payload[i] = (byte) (i % 251);
        }

        return payload;
    }

    private static async Task<Producer<byte[]>> CreateConnectedProducer(FakeProducerConnection connection, string topic = _topic, int chunkMaxMessageSize = 0, bool enableChunking = true)
    {
        var options = new ProducerOptions<byte[]>(topic, Schema.ByteArray) { MaxPendingMessages = 500, EnableChunking = enableChunking, ChunkMaxMessageSize = chunkMaxMessageSize };
        var producer = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, true);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        return producer;
    }
}
