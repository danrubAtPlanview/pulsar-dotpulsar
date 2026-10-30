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

namespace DotPulsar.Tests.Internal.Crypto;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Batching;
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;
using System.Text;

// A producer with encryption keys, with the real channel of the producer and a connection that records what it is asked to write
[Trait("Category", "Unit")]
public sealed class ProducerEncryptionTests
{
    private const string _topic = "persistent://public/default/test";
    private const string _keyName = "rsa";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    // Upstream: ProducerImpl#connectionOpened (CommandProducer encrypted)
    [Fact]
    public async Task Connect_GivenEncryptionKeysAndCryptoKeyReader_ShouldSetEncryptedOnTheCommandProducer()
    {
        //Arrange
        var connection = new FakeProducerConnection();

        //Act
        await using var producer = await CreateConnectedProducer(connection);

        //Assert
        var command = connection.CommandProducers.Single();
        command.HasEncrypted.ShouldBeTrue();
        command.Encrypted.ShouldBeTrue();
    }

    [Fact]
    public async Task Connect_GivenNoEncryptionKeys_ShouldNotSetEncryptedOnTheCommandProducer()
    {
        //Arrange
        var connection = new FakeProducerConnection();

        //Act
        await using var producer = await CreateConnectedProducer(connection, new ProducerOptions<byte[]>(_topic, DotPulsar.Schema.ByteArray) { MaxPendingMessages = 500 });

        //Assert
        connection.CommandProducers.Single().HasEncrypted.ShouldBeFalse();
    }

    [Fact]
    public async Task Connect_GivenCryptoKeyReaderWithoutEncryptionKeys_ShouldNotSetEncryptedOrEncryptTheMessages()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var keyReader = Substitute.For<ICryptoKeyReader>();
        var options = new ProducerOptions<byte[]>(_topic, DotPulsar.Schema.ByteArray) { MaxPendingMessages = 500, CryptoKeyReader = keyReader };
        await using var producer = await CreateConnectedProducer(connection, options);

        //Act
        _ = await producer.Send(new DotPulsar.MessageMetadata(), [1, 2, 3], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        connection.CommandProducers.Single().HasEncrypted.ShouldBeFalse();
        connection.Sent.Single().Payload.ShouldBe([1, 2, 3]);
        connection.Sent.Single().Metadata.EncryptionKeys.ShouldBeEmpty();
        keyReader.ReceivedCalls().ShouldBeEmpty();
    }

    // Upstream: ProducerImpl#ProducerImpl, #addPublicKeyCipher, ProducerEncryptionKeyFailureTest#testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind
    [Fact]
    public async Task Create_GivenKeyThatCannotBeLoaded_ShouldThrowCryptoExceptionAndNeverConnectToTheBroker()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var options = CreateOptions(new InMemoryCryptoKeyReader().WithPublicKey(_keyName, CryptoTestKeys.Public("ClientRsa")), "missing-key");

        //Act
        var exception = Record.Exception(() => new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, true));
        await Task.Delay(TimeSpan.FromMilliseconds(200), Current.CancellationToken);

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key missing-key.");
        await connection.Pool.DidNotReceive().GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>());
        connection.CommandProducers.ShouldBeEmpty();
    }

    [Fact]
    public async Task Create_GivenKeyReaderThatThrows_ShouldThrowCryptoExceptionWithTheInnerException()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var failure = new IOException("The key store is not available");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPublicKey(_keyName, Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(_ => throw failure);

        //Act
        var exception = Record.Exception(() => new Producer<byte[]>(new Uri("pulsar://localhost:6650"), CreateOptions(keyReader), new DefaultExceptionHandler(), connection.Pool, null, true));

        //Assert
        var cryptoException = exception.ShouldBeOfType<CryptoException>();
        cryptoException.InnerException.ShouldBeSameAs(failure);
        cryptoException.Message.ShouldContain("The key store is not available");
        await connection.Pool.DidNotReceive().GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Create_GivenEncryptionKeys_ShouldReadThePublicKeysOnceAndNotWhenMessagesAreSent()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPublicKey(_keyName, Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Public("ClientRsa")));
        await using var producer = await CreateConnectedProducer(connection, CreateOptions(keyReader));

        //Act
        for (var i = 0; i < 3; ++i)
        {
            _ = await producer.Send(new DotPulsar.MessageMetadata(), [(byte) i], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        }

        //Assert
        keyReader.Received(1).GetPublicKey(_keyName, Arg.Any<IReadOnlyDictionary<string, string>>());
        connection.Sent.Count.ShouldBe(3);
    }

    // Upstream: ProducerImpl#sendAsync (the payload is encrypted after it has been compressed)
    [Fact]
    public async Task Send_GivenEncryptionKeys_ShouldWriteTheMessageEncryptedAndTheConsumerDecryptsIt()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection);
        var payload = Encoding.UTF8.GetBytes("a message that only the consumer can read");

        //Act
        _ = await producer.Send(new DotPulsar.MessageMetadata(), payload, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.Payload.Length.ShouldBe(payload.Length + MessageCrypto.TagLength);
        sent.Metadata.EncryptionKeys.Select(key => key.Key).ShouldBe([_keyName]);
        sent.Metadata.EncryptionParam.Length.ShouldBe(MessageCrypto.IvLength);
        Decrypt(sent).ShouldBe(payload);
    }

    [Fact]
    public async Task Send_GivenEncryptionKeysAndCompression_ShouldCompressBeforeItEncrypts()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var decompressorFactory = CompressionFactories.DecompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var options = CreateOptions(CreateKeyReader());
        await using var producer = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, compressorFactory, true);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        var payload = Enumerable.Repeat((byte) 'a', 2000).ToArray();

        //Act
        _ = await producer.Send(new DotPulsar.MessageMetadata(), payload, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.Metadata.Compression.ShouldBe(CompressionType.Zlib);
        sent.Metadata.UncompressedSize.ShouldBe(2000u);
        sent.Payload.Length.ShouldBeLessThan(payload.Length);
        using var decompressor = decompressorFactory.Create();
        decompressor.Decompress(new ReadOnlySequence<byte>(Decrypt(sent)), 2000).ToArray().ShouldBe(payload);
    }

    // Upstream: BatchMessageContainerImpl#buildAndEncryptBatchPayload
    [Fact]
    public async Task Send_GivenEncryptionKeysAndBatching_ShouldWriteOneBatchThatIsEncryptedAsAWhole()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var options = CreateOptions(CreateKeyReader());
        options.EnableBatching = true;
        options.BatchingMaxMessages = 3;
        options.BatchingMaxPublishDelay = TimeSpan.FromHours(1);
        await using var producer = await CreateConnectedProducer(connection, options);
        var messageIds = new List<MessageId>();

        //Act
        for (var i = 0; i < 3; ++i)
        {
            await producer.SendChannel.Send(new DotPulsar.MessageMetadata(), [(byte) i, (byte) i], messageId =>
            {
                lock (messageIds)
                {
                    messageIds.Add(messageId);
                }

                return ValueTask.CompletedTask;
            }, Current.CancellationToken);
        }

        await WaitFor.Until(() => messageIds.Count == 3, "all callbacks are called");

        //Assert
        var sent = connection.Sent.Single();
        sent.NumMessagesInBatch.ShouldBe(3);
        sent.NumMessages.ShouldBe(3);
        sent.Metadata.EncryptionKeys.Count.ShouldBe(1);
        sent.Metadata.EncryptionParam.Length.ShouldBe(MessageCrypto.IvLength);
        BatchPayloadReader.Read(Decrypt(sent), 3).Select(message => message.Payload).ShouldBe([[0, 0], [1, 1], [2, 2]]);
    }

    // Upstream: ProducerImpl#serializeAndSendMessage (every chunk is encrypted)
    [Fact]
    public async Task Send_GivenEncryptionKeysAndChunking_ShouldWriteChunksThatAreEncryptedOnTheirOwnAndFitTheMaxMessageSize()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.Connection.MaxMessageSize.Returns(1000);
        var options = CreateOptions(CreateKeyReader());
        options.EnableChunking = true;
        await using var producer = await CreateConnectedProducer(connection, options);
        var payload = Enumerable.Range(0, 3000).Select(i => (byte) (i % 251)).ToArray();

        //Act
        var messageId = await producer.Send(new DotPulsar.MessageMetadata(), payload, Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent;
        sent.Count.ShouldBeGreaterThan(3);
        sent.ShouldAllBe(message => message.IsChunk && message.SkipSizeCheck);
        sent.Select(message => message.Metadata.EncryptionParam.ToBase64()).Distinct().Count().ShouldBe(sent.Count, "every chunk has its own IV");
        sent.ShouldAllBe(message => message.TotalChunkMsgSize == 3000, "the total is the size of the payload before it is encrypted");
        sent.SelectMany(Decrypt).ToArray().ShouldBe(payload);
        sent.ShouldAllBe(message => GetSizeWithoutChunkFields(message) <= 1000);
        messageId.GetChunkMessageIds()!.Length.ShouldBe(sent.Count);
    }

    // Upstream: ProducerImpl#ProducerImpl (every partition has its own message crypto)
    [Fact]
    public async Task Connect_GivenPartitionedTopic_ShouldEncryptTheMessagesOfEveryPartitionWithItsOwnDataKey()
    {
        //Arrange
        var connection = new FakeProducerConnection(2);
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPublicKey(_keyName, Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Public("ClientRsa")));
        await using var producer = await CreateConnectedProducer(connection, CreateOptions(keyReader));

        //Act
        _ = await producer.Send(new DotPulsar.MessageMetadata(), [1], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        _ = await producer.Send(new DotPulsar.MessageMetadata(), [2], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        connection.CommandProducers.Count.ShouldBe(2);
        connection.CommandProducers.ShouldAllBe(command => command.Encrypted);
        keyReader.Received(2).GetPublicKey(_keyName, Arg.Any<IReadOnlyDictionary<string, string>>());
        var sent = connection.Sent;
        sent.Count.ShouldBe(2);
        sent.Select(message => Decrypt(message)[0]).Order().ShouldBe([(byte) 1, (byte) 2]);
        sent.Select(message => message.Metadata.EncryptionKeys[0].Value.ToBase64()).Distinct().Count().ShouldBe(2, "the data keys are wrapped separately");
    }

    [Fact]
    public async Task Connect_GivenPartitionedTopicAndKeyReaderThatFailsForTheSecondPartition_ShouldFaultTheProducer()
    {
        //Arrange
        var connection = new FakeProducerConnection(2);
        var calls = 0;
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPublicKey(_keyName, Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(_ => ++calls == 1 ? new EncryptionKeyInfo(CryptoTestKeys.Public("ClientRsa")) : null);

        //Act
        await using var producer = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), CreateOptions(keyReader), new DefaultExceptionHandler(), connection.Pool, null, true);
        await producer.State.OnStateChangeTo(ProducerState.Faulted, Current.CancellationToken);

        //Assert
        var exception = await Record.ExceptionAsync(async () => await producer.Send(new DotPulsar.MessageMetadata(), [1], Current.CancellationToken));
        exception.ShouldBeOfType<ProducerFaultedException>().InnerException.ShouldBeOfType<CryptoException>();
    }

    [Fact]
    public async Task Send_GivenNoEncryptionKeys_ShouldWriteTheMessageAsItIs()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        await using var producer = await CreateConnectedProducer(connection, new ProducerOptions<byte[]>(_topic, DotPulsar.Schema.ByteArray) { MaxPendingMessages = 500 });

        //Act
        _ = await producer.Send(new DotPulsar.MessageMetadata(), [1, 2, 3], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var sent = connection.Sent.Single();
        sent.Payload.ShouldBe([1, 2, 3]);
        sent.Metadata.EncryptionKeys.ShouldBeEmpty();
        sent.Metadata.HasEncryptionParam.ShouldBeFalse();
    }

    private static ICryptoKeyReader CreateKeyReader()
        => new InMemoryCryptoKeyReader().WithPublicKey(_keyName, CryptoTestKeys.Public("ClientRsa"));

    private static ProducerOptions<byte[]> CreateOptions(ICryptoKeyReader keyReader, string keyName = _keyName)
        => new(_topic, DotPulsar.Schema.ByteArray)
        {
            MaxPendingMessages = 500,
            CryptoKeyReader = keyReader,
            EncryptionKeys = [keyName]
        };

    private static async Task<Producer<byte[]>> CreateConnectedProducer(FakeProducerConnection connection, ProducerOptions<byte[]>? options = null)
    {
        options ??= CreateOptions(CreateKeyReader());
        var producer = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, true);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        return producer;
    }

    private static byte[] Decrypt(SentMessage sent)
    {
        using var consumer = new MessageCrypto(false);
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey(_keyName, CryptoTestKeys.Private("ClientRsa"));
        consumer.TryDecrypt(sent.Metadata, new ReadOnlySequence<byte>(sent.Payload), keyReader, out var decrypted).ShouldBeTrue("the consumer can decrypt what the producer encrypted");
        return decrypted.ToArray();
    }

    // The part of the size of the message that the chunk size is based on: the payload and the metadata without the fields that are added for the chunks
    private static int GetSizeWithoutChunkFields(SentMessage sent)
    {
        var metadata = sent.Metadata.Clone();
        metadata.ClearUuid();
        metadata.ClearNumChunksFromMsg();
        metadata.ClearTotalChunkMsgSize();
        metadata.ClearChunkId();
        return sent.Payload.Length + metadata.CalculateSize();
    }
}
