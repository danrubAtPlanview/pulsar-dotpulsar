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

using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

// The encryption of the messages, the batches and the chunks of ProducerChannel, with a connection that records what it is asked to send
[Trait("Category", "Unit")]
public sealed class ProducerChannelEncryptionTests
{
    private const string _producerName = "test-producer";
    private const string _keyName = "rsa";
    private const int _maxMessageSize = 1000;

    // Upstream: ProducerImpl#encryptMessage
    [Fact]
    public async Task Send_GivenMessageCrypto_ShouldWriteThePayloadEncryptedAndTheEncryptionFieldsInTheMetadata()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var payload = CreatePayload(100);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Payload.Length.ShouldBe(100 + MessageCrypto.TagLength);
        actual.Payload.ShouldNotBe(payload.ToArray());
        actual.Metadata.EncryptionKeys.Select(key => key.Key).ShouldBe([_keyName]);
        actual.Metadata.EncryptionKeys.Single().Value.Length.ShouldBe(256);
        actual.Metadata.EncryptionParam.Length.ShouldBe(MessageCrypto.IvLength);
        actual.Metadata.HasEncryptionAlgo.ShouldBeFalse("neither the Java client nor the C++ client sets it");
        actual.SkipSizeCheck.ShouldBeFalse();
        Decrypt(actual).ShouldBe(payload.ToArray());
    }

    [Fact]
    public async Task Send_GivenSeveralKeyNames_ShouldWriteTheKeysSortedByOrdinalOrder()
    {
        //Arrange
        using var crypto = CreateCrypto("b", "a", "B");
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, CreatePayload(10), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Single().Metadata.EncryptionKeys.Select(key => key.Key).ShouldBe(["B", "a", "b"]);
    }

    // Upstream: MessageCryptoBc#encrypt (a new IV for every message)
    [Fact]
    public async Task Send_GivenTheSameMetadataTwice_ShouldEncryptWithADifferentIvEachTime()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var metadata = new MessageMetadata { SequenceId = 1 };
        var payload = CreatePayload(100);

        //Act
        await uut.Send(metadata, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await uut.Send(metadata, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Count.ShouldBe(2);
        written[1].Metadata.EncryptionParam.ToByteArray().ShouldNotBe(written[0].Metadata.EncryptionParam.ToByteArray());
        written[1].Payload.ShouldNotBe(written[0].Payload);
        Decrypt(written[0]).ShouldBe(payload.ToArray());
        Decrypt(written[1]).ShouldBe(payload.ToArray());
    }

    // The metadata is pooled, and a message that is sent again after a reconnect is sent with the same metadata
    [Fact]
    public async Task Send_GivenMessageCrypto_ShouldNotLeaveTheEncryptionFieldsInTheMetadataAfterTheSend()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var uut = CreateChannel(new List<Written>(), crypto);
        var metadata = new MessageMetadata { SequenceId = 1 };

        //Act
        await uut.Send(metadata, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        metadata.EncryptionKeys.ShouldBeEmpty();
        metadata.HasEncryptionParam.ShouldBeFalse();
        metadata.HasEncryptionAlgo.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenTheConnectionFailsToSend_ShouldStillNotLeaveTheEncryptionFieldsInTheMetadata()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var uut = CreateChannel(new List<Written>(), crypto, failSend: new IOException("The connection is closed"));
        var metadata = new MessageMetadata { SequenceId = 1 };

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(metadata, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<IOException>();
        metadata.EncryptionKeys.ShouldBeEmpty();
        metadata.HasEncryptionParam.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenNoMessageCrypto_ShouldWriteThePayloadAsItIsWithoutEncryptionFields()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written, null);
        var payload = CreatePayload(100);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Payload.ShouldBe(payload.ToArray());
        actual.Metadata.EncryptionKeys.ShouldBeEmpty();
        actual.Metadata.HasEncryptionParam.ShouldBeFalse();
    }

    // Upstream: ProducerImpl#sendAsync (compress, then encrypt)
    [Fact]
    public async Task Send_GivenCompressionAndMessageCrypto_ShouldCompressThePayloadBeforeItEncryptsIt()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        byte[] compressed = [9, 8, 7, 6, 5];
        var factory = new CompressorFactory(CompressionType.Lz4, () => new Compressor(_ => new ReadOnlySequence<byte>(compressed)));
        var uut = CreateChannel(written, crypto, compressorFactory: factory);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Payload.Length.ShouldBe(compressed.Length + MessageCrypto.TagLength);
        actual.Metadata.Compression.ShouldBe(CompressionType.Lz4);
        actual.Metadata.UncompressedSize.ShouldBe(100u, "the sizes in the metadata are those of the payload before it was encrypted");
        Decrypt(actual).ShouldBe(compressed);
    }

    [Fact]
    public async Task Send_GivenCompressionAndMessageCrypto_ShouldResetTheCompressionAndTheEncryptionOfTheMetadata()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var factory = new CompressorFactory(CompressionType.Lz4, () => new Compressor(data => data));
        var uut = CreateChannel(new List<Written>(), crypto, compressorFactory: factory);
        var metadata = new MessageMetadata { SequenceId = 1 };

        //Act
        await uut.Send(metadata, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        metadata.Compression.ShouldBe(CompressionType.None);
        metadata.UncompressedSize.ShouldBe(0u);
        metadata.EncryptionKeys.ShouldBeEmpty();
        metadata.HasEncryptionParam.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenPayloadInSeveralSegments_ShouldEncryptAllOfIt()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var payload = CreateSegmentedPayload(CreatePayload(90).ToArray(), 7);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        payload.IsSingleSegment.ShouldBeFalse();
        var actual = written.Single();
        actual.Payload.Length.ShouldBe(90 + MessageCrypto.TagLength);
        Decrypt(actual).ShouldBe(payload.ToArray());
    }

    // Upstream: BatchMessageContainerImpl#buildAndEncryptBatchPayload (a batch is encrypted as a whole, its other metadata fields are not)
    [Fact]
    public async Task Send_GivenBatchMetadata_ShouldEncryptThePayloadOnceAndKeepTheBatchFieldsOfTheMetadataAndTheCommand()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var metadata = new MessageMetadata { SequenceId = 10, HighestSequenceId = 14, NumMessagesInBatch = 5, PartitionKey = "key" };
        var payload = CreatePayload(500);

        //Act
        await uut.Send(metadata, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Metadata.NumMessagesInBatch.ShouldBe(5);
        actual.Metadata.SequenceId.ShouldBe(10UL);
        actual.Metadata.HighestSequenceId.ShouldBe(14UL);
        actual.Metadata.PartitionKey.ShouldBe("key");
        actual.Command.NumMessages.ShouldBe(5);
        actual.Command.HighestSequenceId.ShouldBe(14UL);
        actual.Metadata.EncryptionKeys.Count.ShouldBe(1);
        Decrypt(actual).ShouldBe(payload.ToArray());
    }

    // Upstream: MessageChunkingTest#sendSingleChunk (the chunk fields are set by the caller)
    [Fact]
    public async Task Send_GivenChunkFieldsOfTheCaller_ShouldEncryptThePayloadAndKeepTheChunkFields()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var metadata = new MessageMetadata { SequenceId = 1, Uuid = "caller", ChunkId = 0, NumChunksFromMsg = 2, TotalChunkMsgSize = 100 };

        //Act
        await uut.Send(metadata, CreatePayload(10), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Payload.Length.ShouldBe(10 + MessageCrypto.TagLength);
        actual.Metadata.Uuid.ShouldBe("caller");
        actual.Metadata.TotalChunkMsgSize.ShouldBe(100);
        actual.Command.IsChunk.ShouldBeTrue();
        actual.SkipSizeCheck.ShouldBeTrue();
    }

    // Upstream: ProducerImpl#encryptMessage (FAIL)
    [Fact]
    public async Task Send_GivenEncryptionFailsAndFailAction_ShouldThrowTheCryptoExceptionAndNotWriteTheMessage()
    {
        //Arrange
        using var crypto = new MessageCrypto(true);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto, ProducerCryptoFailureAction.Fail);
        var metadata = new MessageMetadata { SequenceId = 1 };

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(metadata, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<CryptoException>();
        written.ShouldBeEmpty();
        metadata.EncryptionKeys.ShouldBeEmpty();
        metadata.HasEncryptionParam.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenEncryptionFailsAfterCompressionAndFailAction_ShouldResetTheCompression()
    {
        //Arrange
        using var crypto = new MessageCrypto(true);
        var factory = new CompressorFactory(CompressionType.Lz4, () => new Compressor(data => data));
        var uut = CreateChannel(new List<Written>(), crypto, ProducerCryptoFailureAction.Fail, compressorFactory: factory);
        var metadata = new MessageMetadata { SequenceId = 1 };

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(metadata, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<CryptoException>();
        metadata.Compression.ShouldBe(CompressionType.None);
        metadata.UncompressedSize.ShouldBe(0u);
    }

    // Upstream: ProducerImpl#encryptMessage (SEND)
    [Fact]
    public async Task Send_GivenEncryptionFailsAndSendAction_ShouldWriteThePayloadUnencryptedWithoutEncryptionFields()
    {
        //Arrange
        using var crypto = new MessageCrypto(true);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto, ProducerCryptoFailureAction.Send);
        var payload = CreatePayload(100);

        //Act
        await uut.Send(new MessageMetadata { SequenceId = 1 }, payload, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Payload.ShouldBe(payload.ToArray());
        actual.Metadata.EncryptionKeys.ShouldBeEmpty();
        actual.Metadata.HasEncryptionParam.ShouldBeFalse();
    }

    [Fact]
    public async Task Send_GivenTheMessageCryptoIsDisposedAndSendAction_ShouldNotSendTheMessageUnencrypted()
    {
        //Arrange
        var crypto = CreateCrypto(_keyName);
        crypto.Dispose();
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto, ProducerCryptoFailureAction.Send);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Send(new MessageMetadata { SequenceId = 1 }, CreatePayload(100), new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ObjectDisposedException>();
        written.ShouldBeEmpty();
    }

    // Upstream: ProducerImpl#sendAsync
    [Fact]
    public void PlanChunking_GivenMessageCrypto_ShouldSubtractTheBytesThatEncryptionAddsFromTheChunkSize()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var plain = CreateChannel(new List<Written>(), null);
        var encrypting = CreateChannel(new List<Written>(), crypto);
        var payload = CreatePayload(5000);

        //Act
        var plainState = plain.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;
        var encryptedState = encrypting.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;

        //Assert
        var overhead = crypto.GetMetadataOverhead();
        overhead.ShouldBeGreaterThan(MessageCrypto.TagLength + 256);
        encryptedState.ChunkSize.ShouldBe(plainState.ChunkSize - overhead);
        encryptedState.TotalChunks.ShouldBeGreaterThan(plainState.TotalChunks);
    }

    [Fact]
    public void PlanChunking_GivenMoreKeyNames_ShouldSubtractMoreFromTheChunkSize()
    {
        //Arrange
        using var one = CreateCrypto("one");
        using var two = CreateCrypto("one", "two");
        var payload = CreatePayload(5000);

        //Act
        var withOne = CreateChannel(new List<Written>(), one).PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;
        var withTwo = CreateChannel(new List<Written>(), two).PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;

        //Assert
        withTwo.ChunkSize.ShouldBe(withOne.ChunkSize - (two.GetMetadataOverhead() - one.GetMetadataOverhead()));
        withTwo.ChunkSize.ShouldBeLessThan(withOne.ChunkSize - 256);
    }

    [Fact]
    public void PlanChunking_GivenChunkMaxMessageSizeBelowWhatFits_ShouldUseItAsItIs()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var uut = CreateChannel(new List<Written>(), crypto);

        //Act
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 100)!;

        //Assert
        state.ChunkSize.ShouldBe(100, "Java does not reduce a chunk size that was set by the user");
        state.TotalChunks.ShouldBe(50);
    }

    [Fact]
    public void PlanChunking_GivenPayloadThatFitsInOneFrameOnlyWithoutEncryption_ShouldSplitIt()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var plain = CreateChannel(new List<Written>(), null);
        var encrypting = CreateChannel(new List<Written>(), crypto);
        var payload = CreatePayload(800);

        //Act
        var plainState = plain.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;
        var encryptedState = encrypting.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;

        //Assert
        plainState.TotalChunks.ShouldBe(1);
        encryptedState.TotalChunks.ShouldBe(2);
    }

    [Fact]
    public void PlanChunking_GivenMetadataAndEncryptionThatDoNotFitInTheMaxMessageSize_ShouldThrowTooLargeMessageException()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var uut = CreateChannel(new List<Written>(), crypto, maxMessageSize: 200);

        //Act
        var act = () => uut.PlanChunking(new MessageMetadata { SequenceId = 1 }, CreatePayload(100), 0);

        //Assert
        act.ShouldThrow<TooLargeMessageException>().MaxMessageSize.ShouldBe(200);
    }

    // Upstream: ProducerImpl#serializeAndSendMessage (every chunk is encrypted, the chunk fields are not)
    [Fact]
    public async Task SendChunk_GivenMessageCrypto_ShouldEncryptEveryChunkWithItsOwnIvAndKeepTheChunkFieldsPlain()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var payload = CreatePayload(5000);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;

        //Act
        for (var chunkId = 0; chunkId < state.TotalChunks; ++chunkId)
        {
            await uut.SendChunk(state, chunkId, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        }

        //Assert
        written.Count.ShouldBe(state.TotalChunks);
        written.Count.ShouldBeGreaterThan(1);
        written.Select(x => x.Metadata.EncryptionParam.ToBase64()).Distinct().Count().ShouldBe(written.Count, "every chunk has its own IV");
        written.ShouldAllBe(x => x.Metadata.EncryptionParam.Length == MessageCrypto.IvLength);
        written.ShouldAllBe(x => x.Metadata.EncryptionKeys.Count == 1 && x.Metadata.EncryptionKeys[0].Key == _keyName);
        written.Select(x => x.Metadata.ChunkId).ShouldBe(Enumerable.Range(0, written.Count));
        written.ShouldAllBe(x => x.Metadata.NumChunksFromMsg == state.TotalChunks && x.Metadata.TotalChunkMsgSize == 5000, "the total is the size of the payload before it is encrypted");
        written.ShouldAllBe(x => x.Command.IsChunk && x.SkipSizeCheck);
        written.SelectMany(Decrypt).ToArray().ShouldBe(payload.ToArray());
    }

    // The chunk size is reduced by the bytes that encryption adds, so that an encrypted chunk is as large as the maximum message size, and not larger
    [Fact]
    public async Task SendChunk_GivenMessageCrypto_ShouldFillTheMaxMessageSizeWithTheEncryptedChunkAndItsMetadata()
    {
        //Arrange
        using var crypto = CreateCrypto("one", "two");
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 0)!;

        //Act
        for (var chunkId = 0; chunkId < state.TotalChunks; ++chunkId)
        {
            await uut.SendChunk(state, chunkId, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        }

        //Assert
        var sizes = written.Select(GetSizeWithoutChunkFields).ToArray();
        sizes.Take(sizes.Length - 1).ShouldAllBe(size => size == _maxMessageSize);
        sizes[^1].ShouldBeLessThanOrEqualTo(_maxMessageSize);
    }

    [Fact]
    public async Task SendChunk_GivenMessageThatIsNotSplit_ShouldEncryptItAndItFitsTheMaxMessageSize()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var payload = CreatePayload(600);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;

        //Act
        await uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        state.TotalChunks.ShouldBe(1);
        var actual = written.Single();
        actual.Metadata.HasChunkId.ShouldBeFalse();
        actual.Command.IsChunk.ShouldBeFalse();
        actual.Payload.Length.ShouldBe(600 + MessageCrypto.TagLength);
        GetSizeWithoutChunkFields(actual).ShouldBeLessThanOrEqualTo(_maxMessageSize);
        Decrypt(actual).ShouldBe(payload.ToArray());
    }

    [Fact]
    public async Task SendChunk_GivenMessageCrypto_ShouldNotLeaveTheEncryptionFieldsInTheMetadataThatTheChunksShare()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var uut = CreateChannel(new List<Written>(), crypto);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 0)!;

        //Act & Assert
        for (var chunkId = 0; chunkId < state.TotalChunks; ++chunkId)
        {
            await uut.SendChunk(state, chunkId, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

            state.Metadata.EncryptionKeys.ShouldBeEmpty();
            state.Metadata.HasEncryptionParam.ShouldBeFalse();
        }
    }

    [Fact]
    public async Task SendChunk_GivenTheChunkIsSentAgain_ShouldEncryptItAgainWithAnotherIv()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto);
        var payload = CreatePayload(5000);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, payload, 0)!;

        //Act
        await uut.SendChunk(state, 1, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        await uut.SendChunk(state, 1, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Count.ShouldBe(2);
        written[1].Metadata.EncryptionParam.ToByteArray().ShouldNotBe(written[0].Metadata.EncryptionParam.ToByteArray());
        Decrypt(written[1]).ShouldBe(Decrypt(written[0]));
        Decrypt(written[0]).ShouldBe(state.GetChunk(1).ToArray());
    }

    [Fact]
    public async Task SendChunk_GivenNoMessageCrypto_ShouldWriteTheChunkWithoutEncryptionFields()
    {
        //Arrange
        var written = new List<Written>();
        var uut = CreateChannel(written, null);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 0)!;

        //Act
        await uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        written.Single().Payload.ShouldBe(state.GetChunk(0).ToArray());
        written.Single().Metadata.EncryptionKeys.ShouldBeEmpty();
        written.Single().Metadata.HasEncryptionParam.ShouldBeFalse();
    }

    // Upstream: ProducerImpl#encryptMessage (FAIL)
    [Fact]
    public async Task SendChunk_GivenEncryptionFailsAndFailAction_ShouldThrowTheCryptoExceptionAndNotWriteTheChunk()
    {
        //Arrange
        using var crypto = new MessageCrypto(true);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto, ProducerCryptoFailureAction.Fail);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 100)!;

        //Act
        var exception = await Record.ExceptionAsync(() => uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<CryptoException>();
        written.ShouldBeEmpty();
        state.Metadata.EncryptionKeys.ShouldBeEmpty();
    }

    // Upstream: ProducerImpl#encryptMessage (SEND)
    [Fact]
    public async Task SendChunk_GivenEncryptionFailsAndSendAction_ShouldWriteTheChunkUnencrypted()
    {
        //Arrange
        using var crypto = new MessageCrypto(true);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto, ProducerCryptoFailureAction.Send);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 100)!;

        //Act
        await uut.SendChunk(state, 3, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);

        //Assert
        var actual = written.Single();
        actual.Payload.ShouldBe(state.GetChunk(3).ToArray());
        actual.Metadata.ChunkId.ShouldBe(3);
        actual.Metadata.EncryptionKeys.ShouldBeEmpty();
        actual.Metadata.HasEncryptionParam.ShouldBeFalse();
    }

    // A message crypto that is disposed is not a failure to encrypt, so the Send action does not send the chunk unencrypted. The fields of the chunks before it are not left on the metadata that they share.
    [Fact]
    public async Task SendChunk_GivenTheMessageCryptoIsDisposedAfterAChunkAndSendAction_ShouldThrowAndNotLeaveTheEncryptionFieldsOfTheEarlierChunk()
    {
        //Arrange
        using var crypto = CreateCrypto(_keyName);
        var written = new List<Written>();
        var uut = CreateChannel(written, crypto, ProducerCryptoFailureAction.Send);
        var state = uut.PlanChunking(new MessageMetadata { SequenceId = 7 }, CreatePayload(5000), 100)!;
        await uut.SendChunk(state, 0, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken);
        crypto.Dispose();

        //Act
        var exception = await Record.ExceptionAsync(() => uut.SendChunk(state, 1, new TaskCompletionSource<BaseCommand>(), Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ObjectDisposedException>();
        written.Count.ShouldBe(1);
        written[0].Metadata.EncryptionKeys.Count.ShouldBe(1);
        state.Metadata.EncryptionKeys.ShouldBeEmpty();
        state.Metadata.HasEncryptionParam.ShouldBeFalse();
    }

    private static MessageCrypto CreateCrypto(params string[] keyNames)
    {
        var keyReader = new InMemoryCryptoKeyReader();

        foreach (var keyName in keyNames)
        {
            keyReader.WithPublicKey(keyName, CryptoTestKeys.Public("ClientRsa"));
        }

        var crypto = new MessageCrypto(true);
        crypto.AddPublicKeyCipher(keyNames, keyReader);
        return crypto;
    }

    private static byte[] Decrypt(Written written)
    {
        using var consumer = new MessageCrypto(false);
        var keyReader = new InMemoryCryptoKeyReader();

        foreach (var key in written.Metadata.EncryptionKeys)
        {
            keyReader.WithPrivateKey(key.Key, CryptoTestKeys.Private("ClientRsa"));
        }

        consumer.TryDecrypt(written.Metadata, new ReadOnlySequence<byte>(written.Payload), keyReader, out var decrypted).ShouldBeTrue("the consumer can decrypt what the producer encrypted");
        return decrypted.ToArray();
    }

    // The part of the size of the message that the chunk size is based on: the payload and the metadata without the fields that are added for the chunks
    private static int GetSizeWithoutChunkFields(Written written)
    {
        var metadata = written.Metadata.Clone();
        metadata.ClearUuid();
        metadata.ClearNumChunksFromMsg();
        metadata.ClearTotalChunkMsgSize();
        metadata.ClearChunkId();
        return written.Payload.Length + metadata.CalculateSize();
    }

    private static ReadOnlySequence<byte> CreatePayload(int length)
    {
        var payload = new byte[length];
        for (var i = 0; i < length; ++i)
        {
            payload[i] = (byte) (i % 251);
        }

        return new ReadOnlySequence<byte>(payload);
    }

    private static ReadOnlySequence<byte> CreateSegmentedPayload(byte[] payload, int segmentLength)
    {
        Segment? first = null;
        Segment? last = null;

        for (var start = 0; start < payload.Length; start += segmentLength)
        {
            var length = Math.Min(segmentLength, payload.Length - start);
            var segment = new Segment(payload.AsMemory(start, length), last);
            first ??= segment;
            last = segment;
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    private static ProducerChannel CreateChannel(
        List<Written> written,
        MessageCrypto? messageCrypto,
        ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.Fail,
        int maxMessageSize = _maxMessageSize,
        ICompressorFactory? compressorFactory = null,
        Exception? failSend = null)
    {
        var connection = Substitute.For<IConnection>();
        connection.MaxMessageSize.Returns(maxMessageSize);
        connection.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            // The package, the command and the metadata are reused, so what is checked is copied
            var package = callInfo.Arg<SendPackage>();
            written.Add(new Written(package.Command!.Clone(), package.Metadata!.Clone(), package.Payload.ToArray(), package.SkipSizeCheck));
            return failSend is null ? Task.CompletedTask : Task.FromException(failSend);
        });

        return new ProducerChannel(1, _producerName, connection, compressorFactory, null, messageCrypto, cryptoFailureAction);
    }

    private sealed record Written(CommandSend Command, MessageMetadata Metadata, byte[] Payload, bool SkipSizeCheck);

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, Segment? previous)
        {
            Memory = memory;

            if (previous is null)
                return;

            RunningIndex = previous.RunningIndex + previous.Memory.Length;
            previous.Next = this;
        }
    }
}
