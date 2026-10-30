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

namespace DotPulsar.Tests.Internal.Batching;

using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;

// The batch of the container is sent by the ProducerChannel as it is, which also compresses the whole payload of the batch
[Trait("Category", "Unit")]
public sealed class BatchSendThroughProducerChannelTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;

    [Fact]
    public async Task Send_GivenBatchAndCompression_ShouldGiveTheConnectionTheBatchMetadataAndTheCompressedPayload()
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var decompressorFactory = CompressionFactories.DecompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        var connection = CreateConnection(sent);
        var uut = new ProducerChannel(5, "producer", connection, compressorFactory, null, null, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(20, (7, 13));
        var uncompressed = batch.Payload.ToArray();

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent.Count.ShouldBe(1);
        var metadata = sent[0].Metadata;
        metadata.ProducerName.ShouldBe("producer");
        metadata.NumMessagesInBatch.ShouldBe(20);
        metadata.SequenceId.ShouldBe(0ul);
        metadata.HighestSequenceId.ShouldBe(19ul);
        metadata.TxnidMostBits.ShouldBe(7ul);
        metadata.TxnidLeastBits.ShouldBe(13ul);
        metadata.Compression.ShouldBe(CompressionType.Zlib);
        metadata.UncompressedSize.ShouldBe((uint) uncompressed.Length);
        sent[0].CommandSequenceId.ShouldBe(0ul);

        using var decompressor = decompressorFactory.Create();
        var decompressed = decompressor.Decompress(new ReadOnlySequence<byte>(sent[0].Payload), uncompressed.Length).ToArray();
        decompressed.ShouldBe(uncompressed);
        BatchPayloadReader.Read(decompressed, 20).Count.ShouldBe(20);

        //Annihilate
        batch.Return();
    }

    [Fact]
    public async Task Send_GivenBatchThatIsSentTwice_ShouldSendTheSamePayloadAndMetadataBothTimes()
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        var uut = new ProducerChannel(5, "producer", CreateConnection(sent), compressorFactory, null, null, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(5, null);

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent.Count.ShouldBe(2);
        sent[1].Payload.ShouldBe(sent[0].Payload);
        sent[1].Metadata.Compression.ShouldBe(sent[0].Metadata.Compression);
        sent[1].Metadata.UncompressedSize.ShouldBe(sent[0].Metadata.UncompressedSize);
        sent[1].Metadata.NumMessagesInBatch.ShouldBe(5);
        sent[1].Metadata.SequenceId.ShouldBe(sent[0].Metadata.SequenceId);
        sent[1].Metadata.HighestSequenceId.ShouldBe(sent[0].Metadata.HighestSequenceId);

        //Annihilate
        batch.Return();
    }

    [Fact]
    public async Task Send_GivenBatchWithoutCompression_ShouldGiveTheConnectionTheUncompressedPayload()
    {
        //Arrange
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        var uut = new ProducerChannel(5, "producer", CreateConnection(sent), null, null, null, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(3, null);
        var expected = batch.Payload.ToArray();

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent[0].Payload.ShouldBe(expected);
        sent[0].Metadata.HasCompression.ShouldBeFalse();
        sent[0].Metadata.HasTxnidMostBits.ShouldBeFalse();

        //Annihilate
        batch.Return();
    }

    // Upstream: BatchMessageContainerImpl#buildAndEncryptBatchPayload, BatchMessageContainerImplTest#testToByteBufWithCompressionAndEncryption (the batch is compressed, and the result is encrypted)
    [Fact]
    public async Task Send_GivenBatchAndCompressionAndEncryption_ShouldCompressTheWholeBatchAndEncryptItOnce()
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var decompressorFactory = CompressionFactories.DecompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        using var messageCrypto = CreateMessageCrypto();
        var uut = new ProducerChannel(5, "producer", CreateConnection(sent), compressorFactory, null, messageCrypto, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(20, null);
        var uncompressed = batch.Payload.ToArray();

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        var actual = sent.ShouldHaveSingleItem();
        actual.Metadata.NumMessagesInBatch.ShouldBe(20);
        actual.Metadata.Compression.ShouldBe(CompressionType.Zlib);
        actual.Metadata.UncompressedSize.ShouldBe((uint) uncompressed.Length, "the size is that of the payload before it was compressed and encrypted");
        actual.Metadata.EncryptionKeys.Count.ShouldBe(1);
        actual.Metadata.EncryptionParam.Length.ShouldBe(MessageCrypto.IvLength);

        var compressed = Decrypt(actual.Metadata, actual.Payload);
        using var decompressor = decompressorFactory.Create();
        var decompressed = decompressor.Decompress(new ReadOnlySequence<byte>(compressed), uncompressed.Length).ToArray();
        decompressed.ShouldBe(uncompressed);
        BatchPayloadReader.Read(decompressed, 20).Count.ShouldBe(20);

        //Annihilate
        batch.Return();
    }

    // The batch is kept until the broker has acknowledged it, and it is sent again after a reconnect
    [Fact]
    public async Task Send_GivenBatchThatIsSentTwiceAndEncryption_ShouldEncryptItAgainWithAFreshIvAndLeaveNoEncryptionFieldsInTheBatch()
    {
        //Arrange
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        using var messageCrypto = CreateMessageCrypto();
        var uut = new ProducerChannel(5, "producer", CreateConnection(sent), null, null, messageCrypto, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(5, null);
        var expected = batch.Payload.ToArray();

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent.Count.ShouldBe(2);
        sent[1].Metadata.EncryptionParam.ToByteArray().ShouldNotBe(sent[0].Metadata.EncryptionParam.ToByteArray());
        sent[1].Payload.ShouldNotBe(sent[0].Payload);
        Decrypt(sent[0].Metadata, sent[0].Payload).ShouldBe(expected);
        Decrypt(sent[1].Metadata, sent[1].Payload).ShouldBe(expected);
        batch.Metadata.EncryptionKeys.ShouldBeEmpty();
        batch.Metadata.HasEncryptionParam.ShouldBeFalse();
        batch.Payload.ToArray().ShouldBe(expected, "the payload of the batch is not encrypted in place");

        //Annihilate
        batch.Return();
    }

    // Upstream: BatchMessageContainerImplTest#testRecoveryAfterBatchBuildFailure
    // The Java container builds the compressed and encrypted payload, and when that fails the messages stay in the container, and the next flush builds the batch again. A DotPulsar batch is compressed and
    // encrypted when it is written, so what is left after a failure is the batch itself, and the retry is to write it again. Encryption fails because no keys were added, and it works after they are.
    // The Java rows None and Zlib differ in who owns the payload after the compression. The compressed payload of DotPulsar is not pooled, and the batch is the only owner of its buffer in both rows.
    [Theory]
    [InlineData(CompressionType.None)]
    [InlineData(CompressionType.Zlib)]
    public async Task Send_GivenEncryptionFailsOnceAndTheBatchIsSentAgain_ShouldLeaveTheBatchAsItWasAndWriteItAsAValidFrame(CompressionType compressionType)
    {
        //Arrange
        var compressorFactory = compressionType == CompressionType.None ? null : CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == compressionType);
        var stream = CreateStreamOfProducer();
        await using var connection = Connect(stream);
        var producerId = await RegisterProducer(connection);
        using var messageCrypto = new MessageCrypto(true);
        var uut = new ProducerChannel(producerId, "producer", connection, compressorFactory, null, messageCrypto, ProducerCryptoFailureAction.Fail);
        var pool = new PreparedBatchPool(4);
        var batch = CreateBatch(pool, 2, 30);
        var uncompressed = batch.Payload.ToArray();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None));
        var payloadAfterFailure = batch.Payload.ToArray();
        var metadataAfterFailure = batch.Metadata.Clone();
        var sentAfterFailure = stream.SentFrames.Count(frame => IsSend(frame));
        messageCrypto.AddPublicKeyCipher(["rsa"], new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa")));
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        exception.ShouldBeOfType<CryptoException>();
        payloadAfterFailure.ShouldBe(uncompressed);
        metadataAfterFailure.Compression.ShouldBe(CompressionType.None, "the compression of the failed attempt is not left in the batch");
        metadataAfterFailure.UncompressedSize.ShouldBe(0u);
        metadataAfterFailure.EncryptionKeys.ShouldBeEmpty();
        sentAfterFailure.ShouldBe(0);
        var frame = ParseSendFrame(stream.SentFrames.Single(IsSend));
        frame.IsValid.ShouldBeTrue();
        frame.Command.Send.NumMessages.ShouldBe(2);
        frame.Metadata.Compression.ShouldBe(compressionType, "the compression branch must have run");
        var compressed = Decrypt(frame.Metadata, frame.Payload);
        var decompressed = compressionType == CompressionType.None ? compressed : Decompress(compressed, uncompressed.Length);
        decompressed.ShouldBe(uncompressed);
        batch.Payload.ToArray().ShouldBe(uncompressed);
        batch.IsReturned.ShouldBeFalse("the channel does not take the batch");

        //Annihilate
        batch.Return();
    }

    // Upstream: BatchMessageContainerImplTest#testNoCompressionBuildFailureProducesValidFrame
    // The Java test fails the build once in the encryption and once when the command is built, and the retry must write a frame whose total size is the number of bytes that follow it and that parses.
    // The batch is written by the connection, so the second failure is that the connection does not accept the size of the frame, and the retry is after the maximum message size was raised.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Send_GivenFailureOnceWithoutCompressionAndTheBatchIsSentAgain_ShouldWriteAValidFrame(bool failAtEncrypt)
    {
        //Arrange
        var stream = CreateStreamOfProducer();
        await using var connection = Connect(stream);
        var producerId = await RegisterProducer(connection);
        using var messageCrypto = failAtEncrypt ? new MessageCrypto(true) : null;
        var uut = new ProducerChannel(producerId, "producer", connection, null, null, messageCrypto, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(new PreparedBatchPool(4), 3, 30);
        var expected = batch.Payload.ToArray();

        if (!failAtEncrypt)
            connection.MaxMessageSize = 100;

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None));

        if (failAtEncrypt)
            messageCrypto!.AddPublicKeyCipher(["rsa"], new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa")));
        else
            connection.MaxMessageSize = MaxMessageSize;

        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        if (failAtEncrypt)
            exception.ShouldBeOfType<CryptoException>();
        else
            exception.ShouldBeOfType<TooLargeMessageException>();

        var frame = ParseSendFrame(stream.SentFrames.Single(IsSend));
        frame.TotalSizeIsTheNumberOfBytesThatFollowIt.ShouldBeTrue();
        frame.IsValid.ShouldBeTrue();
        frame.Command.Send.NumMessages.ShouldBe(3);
        frame.Metadata.NumMessagesInBatch.ShouldBe(3);
        var payload = failAtEncrypt ? Decrypt(frame.Metadata, frame.Payload) : frame.Payload;
        payload.ShouldBe(expected);
        batch.IsReturned.ShouldBeFalse("the channel does not take the batch");

        //Annihilate
        batch.Return();
    }

    // Upstream: BatchMessageContainerImplTest#oversizedBatchReleasesThePayloadExactlyOnce (the rows None and Zlib), #testOversizedBatchReleasesItsPayloadExactlyOnce, #testOversizedEncryptedBatchReleasesItsBuffersExactlyOnce
    // The payload is incompressible, so the batch is too large after the compression too, and the encryption makes the frame larger. Nothing is written, and the batch is left to its owner, which returns it
    // once: the pool gives it out once. The Java container owns the buffer of the batch until its payload is handed to the command, and in DotPulsar the channel never takes the batch.
    [Theory]
    [InlineData(CompressionType.None, false)]
    [InlineData(CompressionType.Zlib, false)]
    [InlineData(CompressionType.None, true)]
    public async Task Send_GivenBatchThatIsLargerThanMaxMessageSizeAfterCompressionAndEncryption_ShouldThrowTooLargeMessageExceptionAndLeaveTheBatchToItsOwner(CompressionType compressionType, bool encrypt)
    {
        //Arrange
        var compressorFactory = compressionType == CompressionType.None ? null : CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == compressionType);
        var stream = CreateStreamOfProducer();
        await using var connection = Connect(stream);
        connection.MaxMessageSize = 1000;
        var producerId = await RegisterProducer(connection);
        using var messageCrypto = encrypt ? CreateMessageCrypto() : null;
        var uut = new ProducerChannel(producerId, "producer", connection, compressorFactory, null, messageCrypto, ProducerCryptoFailureAction.Fail);
        var pool = new PreparedBatchPool(4);
        var batch = CreateBatch(pool, 2, 600);
        var expected = batch.Payload.ToArray();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None));
        var returnedByTheChannel = batch.IsReturned;
        var payloadAfterFailure = batch.Payload.ToArray();
        batch.Return();
        var first = pool.Get();
        var second = pool.Get();

        //Assert
        exception.ShouldBeOfType<TooLargeMessageException>();
        stream.SentFrames.ShouldNotContain(frame => IsSend(frame));
        returnedByTheChannel.ShouldBeFalse();
        payloadAfterFailure.ShouldBe(expected);
        batch.Metadata.EncryptionKeys.ShouldBeEmpty();
        first.ShouldBeSameAs(batch);
        second.ShouldNotBeSameAs(batch);
    }

    private static MessageCrypto CreateMessageCrypto()
    {
        var messageCrypto = new MessageCrypto(true);
        messageCrypto.AddPublicKeyCipher(["rsa"], new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa")));
        return messageCrypto;
    }

    private static byte[] Decrypt(MessageMetadata metadata, byte[] payload)
    {
        using var consumer = new MessageCrypto(false);
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey("rsa", CryptoTestKeys.Private("ClientRsa"));
        consumer.TryDecrypt(metadata, new ReadOnlySequence<byte>(payload), keyReader, out var decrypted).ShouldBeTrue("the consumer can decrypt what the producer encrypted");
        return decrypted.ToArray();
    }

    private static IConnection CreateConnection(List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)> sent)
    {
        var connection = Substitute.For<IConnection>();
        connection
            .When(c => c.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()))
            .Do(call =>
            {
                var package = call.Arg<SendPackage>();
                sent.Add((package.Metadata!.Clone(), package.Payload.ToArray(), package.Command!.SequenceId));
            });

        return connection;
    }

    private static PreparedBatch CreateBatch(int numberOfMessages, (ulong Most, ulong Least)? transaction)
    {
        var container = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            container.Add(TestBatchItem.Create((ulong) i, $"payload-{i}-{new string('x', 30)}", transaction: transaction), MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);

        return groups[0].Batch!;
    }

    // The payloads of the messages are random, so they can not be compressed
    private static PreparedBatch CreateBatch(PreparedBatchPool pool, int numberOfMessages, int payloadSize)
    {
        var container = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1), pool);
        var groups = new List<PreparedBatchGroup>();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            container.Add(TestBatchItem.Create((ulong) i, RandomNumberGenerator.GetBytes(payloadSize)), MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);

        return groups[0].Batch!;
    }

    private static byte[] Decompress(byte[] compressed, int uncompressedSize)
    {
        var decompressorFactory = CompressionFactories.DecompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        using var decompressor = decompressorFactory.Create();
        return decompressor.Decompress(new ReadOnlySequence<byte>(compressed), uncompressedSize).ToArray();
    }

    // The producer is registered with the connection, because a message is only sent for a producer that the connection knows
    private static async Task<ulong> RegisterProducer(Connection connection)
    {
        var channel = Substitute.For<IChannel>();
        channel.SenderLock().Returns(Substitute.For<IDisposable>());
        var command = new CommandProducer { Topic = "persistent://public/default/test" };
        _ = await connection.Send(command, channel, CancellationToken.None);
        return command.ProducerId;
    }

    private static FakePulsarStream CreateStreamOfProducer()
        => new(command => command.Type == BaseCommand.Types.Type.Producer
            ? new BaseCommand { Type = BaseCommand.Types.Type.ProducerSuccess, ProducerSuccess = new CommandProducerSuccess { RequestId = command.Producer.RequestId, ProducerName = "producer" } }
            : null);

    private static Connection Connect(FakePulsarStream stream)
    {
        var connection = Connection.Connect(stream, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        connection.MaxMessageSize = MaxMessageSize;
        return connection;
    }

    private static bool IsSend(byte[] frame)
    {
        var commandSize = (int) BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(4, 4));
        return BaseCommand.Parser.ParseFrom(frame, 8, commandSize).Type == BaseCommand.Types.Type.Send;
    }

    // A frame is the size of the frame, the size of the command, the command, the magic number, the checksum, the size of the metadata, the metadata and the payload
    private static SendFrame ParseSendFrame(byte[] frame)
    {
        var totalSize = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(0, 4));
        var commandSize = (int) BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(4, 4));
        var command = BaseCommand.Parser.ParseFrom(frame, 8, commandSize);
        var offset = 8 + commandSize;
        var hasMagicNumber = frame.AsSpan(offset, 2).SequenceEqual(Constants.MagicNumber);
        var checksum = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(offset + 2, 4));
        var checksummed = frame.AsMemory(offset + 6);
        var metadataSize = (int) BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(offset + 6, 4));
        var metadata = MessageMetadata.Parser.ParseFrom(frame, offset + 10, metadataSize);
        var payload = frame.AsSpan(offset + 10 + metadataSize).ToArray();
        var isValid = hasMagicNumber && command.Type == BaseCommand.Types.Type.Send && checksum == Crc32C.Calculate(new ReadOnlySequence<byte>(checksummed));

        return new SendFrame(totalSize == frame.Length - 4, isValid, command, metadata, payload);
    }

    private sealed record SendFrame(bool TotalSizeIsTheNumberOfBytesThatFollowIt, bool IsValid, BaseCommand Command, MessageMetadata Metadata, byte[] Payload);
}
