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
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

[Trait("Category", "Unit")]
public sealed class MessageCryptoTests
{
    private static readonly byte[] _payload = Encoding.UTF8.GetBytes("Hello, end-to-end encryption!");

    private static void SkipWhenExplicitCurvesAreNotSupported()
        => Assert.SkipWhen(OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");

    private static InMemoryCryptoKeyReader CreateKeyReader(IReadOnlyDictionary<string, string>? keyMetadata = null)
        => new InMemoryCryptoKeyReader(keyMetadata)
            .WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa"))
            .WithPrivateKey("rsa", CryptoTestKeys.Private("ClientRsa"))
            .WithPublicKey("rsa-pkcs8", CryptoTestKeys.Public("ClientRsaPkcs8"))
            .WithPrivateKey("rsa-pkcs8", CryptoTestKeys.Private("ClientRsaPkcs8"))
            .WithPublicKey("ecdsa", CryptoTestKeys.Public("ClientEcdsa"))
            .WithPrivateKey("ecdsa", CryptoTestKeys.Private("ClientEcdsa"))
            .WithPublicKey("ecdsa-521", CryptoTestKeys.Public("CryptoEcdsa"))
            .WithPrivateKey("ecdsa-521", CryptoTestKeys.Private("CryptoEcdsa"))
            .WithPublicKey("a", CryptoTestKeys.Public("ClientRsa"))
            .WithPublicKey("B", CryptoTestKeys.Public("ClientRsa"))
            .WithPublicKey("_", CryptoTestKeys.Public("ClientRsa"));

    private static MessageCrypto CreateProducer(ICryptoKeyReader keyReader, params string[] keyNames)
    {
        var producer = new MessageCrypto(true);
        producer.AddPublicKeyCipher(keyNames, keyReader);
        return producer;
    }

    private static MessageMetadata CreateMetadata(string? producerName = "producer-1")
    {
        var metadata = new MessageMetadata { SequenceId = 1, PublishTime = 1 };

        if (producerName is not null)
            metadata.ProducerName = producerName;

        return metadata;
    }

    private static ReadOnlySequence<byte> Encrypt(MessageCrypto producer, MessageMetadata metadata, byte[] payload)
        => producer.Encrypt(metadata, new ReadOnlySequence<byte>(payload));

    private static byte[]? Decrypt(MessageCrypto consumer, ICryptoKeyReader keyReader, MessageMetadata metadata, ReadOnlySequence<byte> payload)
        => consumer.TryDecrypt(metadata, payload, keyReader, out var decrypted) ? decrypted.ToArray() : null;

    private static ReadOnlySequence<byte> Split(byte[] data, params int[] segmentLengths)
    {
        var builder = new SequenceBuilder<byte>();
        var offset = 0;

        foreach (var length in segmentLengths)
        {
            builder.Append(new ReadOnlyMemory<byte>(data, offset, length));
            offset += length;
        }

        builder.Append(new ReadOnlyMemory<byte>(data, offset, data.Length - offset));
        return builder.Build();
    }

    [Fact]
    public void Encrypt_GivenTwoKeyNames_ShouldWriteSortedEncryptionKeysAndTwelveByteIv()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "_", "a", "B", "a");
        var metadata = CreateMetadata();

        //Act
        var actual = Encrypt(uut, metadata, _payload);

        //Assert
        metadata.EncryptionKeys.Select(k => k.Key).ShouldBe(["B", "_", "a"]);
        metadata.EncryptionKeys.ShouldAllBe(k => k.Value.Length == 256);
        metadata.EncryptionParam.Length.ShouldBe(12);
        metadata.HasEncryptionAlgo.ShouldBeFalse();
        actual.Length.ShouldBe(_payload.Length + 16);
    }

    [Fact]
    public void Encrypt_GivenMetadataWithCompressionAndBatchFields_ShouldLeaveThemUntouched()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var metadata = CreateMetadata();
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 123;
        metadata.NumMessagesInBatch = 3;
        metadata.PartitionKey = "key";
        metadata.Properties.Add(new KeyValue { Key = "k", Value = "v" });

        //Act
        Encrypt(uut, metadata, _payload);

        //Assert
        metadata.Compression.ShouldBe(CompressionType.Lz4);
        metadata.UncompressedSize.ShouldBe(123u);
        metadata.NumMessagesInBatch.ShouldBe(3);
        metadata.PartitionKey.ShouldBe("key");
        metadata.Properties.Count.ShouldBe(1);
    }

    [Fact]
    public void Encrypt_GivenKeyInfoWithMetadata_ShouldWriteTheKeyMetadata()
    {
        //Arrange
        var keyReader = CreateKeyReader(new Dictionary<string, string> { ["version"] = "1.0", ["timestamp"] = "2026" });
        using var uut = CreateProducer(keyReader, "rsa");
        var metadata = CreateMetadata();

        //Act
        Encrypt(uut, metadata, _payload);

        //Assert
        metadata.EncryptionKeys[0].Metadata.ToDictionary(m => m.Key, m => m.Value).ShouldBe(new Dictionary<string, string> { ["version"] = "1.0", ["timestamp"] = "2026" });
    }

    [Fact]
    public void Encrypt_GivenKeyInfoWithoutMetadata_ShouldNotWriteKeyMetadata()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var metadata = CreateMetadata();

        //Act
        Encrypt(uut, metadata, _payload);

        //Assert
        metadata.EncryptionKeys[0].Metadata.Count.ShouldBe(0);
    }

    [Fact]
    public void Encrypt_GivenMetadataWithExistingEncryptionKeys_ShouldReplaceThem()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var metadata = CreateMetadata();
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "old", Value = Google.Protobuf.ByteString.CopyFromUtf8("old") });
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "older", Value = Google.Protobuf.ByteString.CopyFromUtf8("older") });

        //Act
        Encrypt(uut, metadata, _payload);

        //Assert
        metadata.EncryptionKeys.Select(k => k.Key).ShouldBe(["rsa"]);
    }

    [Fact]
    public void Encrypt_GivenSamePayloadTwice_ShouldUseDifferentIvAndCiphertext()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var firstMetadata = CreateMetadata();
        var secondMetadata = CreateMetadata();

        //Act
        var first = Encrypt(uut, firstMetadata, _payload);
        var second = Encrypt(uut, secondMetadata, _payload);

        //Assert
        firstMetadata.EncryptionParam.ShouldNotBe(secondMetadata.EncryptionParam);
        first.ToArray().ShouldNotBe(second.ToArray());
        firstMetadata.EncryptionKeys[0].Value.ShouldBe(secondMetadata.EncryptionKeys[0].Value);
    }

    [Fact]
    public void Encrypt_GivenPayload_ShouldProduceCiphertextFollowedByTagThatAesGcmAcceptsWithoutAad()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var metadata = CreateMetadata();
        using var privateKey = (RSA) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private("ClientRsa"));

        //Act
        var encrypted = Encrypt(uut, metadata, _payload).ToArray();

        //Assert
        var dataKey = privateKey.Decrypt(metadata.EncryptionKeys[0].Value.ToByteArray(), RSAEncryptionPadding.OaepSHA1);
        dataKey.Length.ShouldBe(32);
        using var aes = new AesGcm(dataKey, 16);
        var actual = new byte[_payload.Length];
        aes.Decrypt(metadata.EncryptionParam.ToByteArray(), encrypted.AsSpan(0, _payload.Length), encrypted.AsSpan(_payload.Length, 16), actual);
        actual.ShouldBe(_payload);
    }

    [Fact]
    public void Encrypt_GivenEmptyPayload_ShouldReturnTheTagOnly()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        using var consumer = new MessageCrypto(false);
        var metadata = CreateMetadata();

        //Act
        var actual = uut.Encrypt(metadata, ReadOnlySequence<byte>.Empty);

        //Assert
        actual.Length.ShouldBe(16);
        Decrypt(consumer, keyReader, metadata, actual).ShouldBe([]);
    }

    [Fact]
    public void Encrypt_GivenMultiSegmentSequence_ShouldProduceTheSameAsASingleSegment()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        using var consumer = new MessageCrypto(false);
        var data = Enumerable.Range(0, 5000).Select(i => (byte) i).ToArray();
        var metadata = CreateMetadata();

        //Act
        var actual = uut.Encrypt(metadata, Split(data, 1, 100, 2000));

        //Assert
        actual.Length.ShouldBe(5000 + 16);
        Decrypt(consumer, keyReader, metadata, actual).ShouldBe(data);
    }

    [Fact]
    public void Encrypt_GivenNoKeysAdded_ShouldThrowCryptoExceptionAndLeaveTheMetadataUntouched()
    {
        //Arrange
        using var uut = new MessageCrypto(true);
        var metadata = CreateMetadata();

        //Act
        var exception = Record.Exception(() => Encrypt(uut, metadata, _payload));

        //Assert
        exception.ShouldBeOfType<CryptoException>();
        metadata.EncryptionKeys.Count.ShouldBe(0);
        metadata.HasEncryptionParam.ShouldBeFalse();
    }

    [Fact]
    public void Encrypt_GivenDisposed_ShouldThrowObjectDisposedException()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        var uut = CreateProducer(keyReader, "rsa");
        uut.Dispose();

        //Act
        var exception = Record.Exception(() => Encrypt(uut, CreateMetadata(), _payload));

        //Assert
        exception.ShouldBeOfType<ObjectDisposedException>();
    }

    [Fact]
    public void Encrypt_GivenConcurrentCalls_ShouldProduceDecryptableMessagesWithUniqueIvs()
    {
        //Arrange
        const int threads = 8;
        const int messagesPerThread = 300;
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var results = new ConcurrentBag<(byte[] Payload, MessageMetadata Metadata, ReadOnlySequence<byte> Encrypted)>();

        //Act
        Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, thread =>
        {
            for (var i = 0; i < messagesPerThread; ++i)
            {
                var payload = Encoding.UTF8.GetBytes($"message {thread}-{i} " + new string('x', (thread * 31) + i));
                var metadata = CreateMetadata($"producer-{thread}");
                results.Add((payload, metadata, Encrypt(uut, metadata, payload)));
            }
        });

        //Assert
        results.Count.ShouldBe(threads * messagesPerThread);
        results.Select(r => r.Metadata.EncryptionParam.ToBase64()).Distinct().Count().ShouldBe(threads * messagesPerThread);

        using var consumer = new MessageCrypto(false);
        foreach (var result in results)
            Decrypt(consumer, keyReader, result.Metadata, result.Encrypted).ShouldBe(result.Payload);
    }

    [Fact]
    public async Task Encrypt_GivenConcurrentRotation_ShouldAlwaysMatchTheKeysWithTheCiphertext()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        using var cts = new CancellationTokenSource();
        var results = new ConcurrentBag<(MessageMetadata Metadata, ReadOnlySequence<byte> Encrypted)>();

        var encryptors = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!cts.IsCancellationRequested)
            {
                var metadata = CreateMetadata();
                results.Add((metadata, Encrypt(uut, metadata, _payload)));
            }
        })).ToArray();

        //Act
        await WaitUntil(() => !results.IsEmpty);

        for (var i = 0; i < 25; ++i)
        {
            uut.AddPublicKeyCipher(["rsa"], keyReader);
            await Task.Yield();
        }

        var countAfterRotations = results.Count;
        await WaitUntil(() => results.Count > countAfterRotations + 10);

        cts.Cancel();
        await Task.WhenAll(encryptors);

        //Assert
        results.Select(r => r.Metadata.EncryptionKeys[0].Value).Distinct().Count().ShouldBeGreaterThan(1);
        using var consumer = new MessageCrypto(false);
        foreach (var result in results)
            Decrypt(consumer, keyReader, result.Metadata, result.Encrypted).ShouldBe(_payload);
    }

    [Theory]
    [InlineData("rsa")]
    [InlineData("rsa", "rsa-pkcs8")]
    [InlineData("rsa", "ecdsa", "ecdsa-521")]
    [InlineData("ecdsa-521")]
    public void GetMetadataOverhead_GivenKeys_ShouldEqualTheGrowthOfTheSerializedMetadataAndThePayload(params string[] keyNames)
    {
        //Arrange
        if (keyNames.Any(k => k.StartsWith("ecdsa", StringComparison.Ordinal)))
            SkipWhenExplicitCurvesAreNotSupported();

        var keyReader = CreateKeyReader(new Dictionary<string, string> { ["version"] = "1.0" });
        using var uut = CreateProducer(keyReader, keyNames);
        var metadata = CreateMetadata();
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 100_000;
        var sizeBefore = metadata.CalculateSize();

        //Act
        var encrypted = Encrypt(uut, metadata, _payload);

        //Assert
        var actual = uut.GetMetadataOverhead();
        actual.ShouldBe((metadata.CalculateSize() - sizeBefore) + (int) (encrypted.Length - _payload.Length));
    }

    [Fact]
    public void GetMetadataOverhead_GivenNoKeysAdded_ShouldReturnZero()
    {
        //Arrange
        using var uut = new MessageCrypto(true);

        //Act
        var actual = uut.GetMetadataOverhead();

        //Assert
        actual.ShouldBe(0);
    }

    [Fact]
    public void AddPublicKeyCipher_GivenFailingSecondKey_ShouldKeepPreviousSnapshot()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        using var consumer = new MessageCrypto(false);
        var before = CreateMetadata();
        var encryptedBefore = Encrypt(uut, before, _payload);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa", "missing"], keyReader));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldContain("Failed to load public key missing.");
        var after = CreateMetadata();
        var encryptedAfter = Encrypt(uut, after, _payload);
        after.EncryptionKeys.Select(k => k.Key).ShouldBe(["rsa"]);
        after.EncryptionKeys[0].Value.ShouldBe(before.EncryptionKeys[0].Value);
        Decrypt(consumer, keyReader, before, encryptedBefore).ShouldBe(_payload);
        Decrypt(consumer, keyReader, after, encryptedAfter).ShouldBe(_payload);
    }

    [Fact]
    public void AddPublicKeyCipher_GivenFailingFirstCall_ShouldNotEnableEncryption()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["missing"], keyReader));

        //Assert
        exception.ShouldBeOfType<CryptoException>();
        uut.GetMetadataOverhead().ShouldBe(0);
        Should.Throw<CryptoException>(() => Encrypt(uut, CreateMetadata(), _payload));
    }

    [Fact]
    public void AddPublicKeyCipher_GivenNullKeyReader_ShouldThrowCryptoException()
    {
        //Arrange
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], null!));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldBe("Keyname or KeyReader is null");
    }

    [Fact]
    public void AddPublicKeyCipher_GivenNullKeyName_ShouldThrowCryptoException()
    {
        //Arrange
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa", null!], CreateKeyReader()));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldBe("Keyname or KeyReader is null");
    }

    [Fact]
    public void AddPublicKeyCipher_GivenNoKeyNames_ShouldThrowArgumentException()
    {
        //Arrange
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher([], CreateKeyReader()));

        //Assert
        exception.ShouldBeOfType<ArgumentException>();
    }

    [Fact]
    public void AddPublicKeyCipher_GivenKeyReaderReturningNull_ShouldThrowCryptoException()
    {
        //Arrange
        var keyReader = Substitute.For<ICryptoKeyReader>();
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], keyReader));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key rsa. ");
    }

    [Fact]
    public void AddPublicKeyCipher_GivenKeyReaderReturningEmptyKey_ShouldThrowCryptoException()
    {
        //Arrange
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey("rsa", []);
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], keyReader));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key rsa. ");
    }

    [Fact]
    public void AddPublicKeyCipher_GivenKeyReaderThatThrows_ShouldThrowCryptoExceptionWithInnerException()
    {
        //Arrange
        var inner = new InvalidOperationException("boom");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPublicKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(_ => throw inner);
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], keyReader));

        //Assert
        var cryptoException = exception.ShouldBeOfType<CryptoException>();
        cryptoException.Message.ShouldBe("Failed to load public key rsa. boom");
        cryptoException.InnerException.ShouldBe(inner);
    }

    [Fact]
    public void AddPublicKeyCipher_GivenInvalidPem_ShouldThrowCryptoException()
    {
        //Arrange
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey("rsa", Encoding.UTF8.GetBytes("not a key"));
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], keyReader));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldBe("Failed to load public key rsa. No PEM block was found");
    }

    [Fact]
    public void AddPublicKeyCipher_GivenPrivateKeyAsPublicKey_ShouldThrowCryptoException()
    {
        //Arrange
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Private("ClientRsa"));
        using var uut = new MessageCrypto(true);

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], keyReader));

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key rsa. ");
    }

    [Fact]
    public void AddPublicKeyCipher_GivenKeyReader_ShouldCallGetPublicKeyOncePerKeyNameWithEmptyMetadata()
    {
        //Arrange
        var inner = CreateKeyReader();
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPublicKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(call => inner.GetPublicKey(call.ArgAt<string>(0), call.ArgAt<IReadOnlyDictionary<string, string>>(1)));
        using var uut = new MessageCrypto(true);

        //Act
        uut.AddPublicKeyCipher(["rsa", "rsa-pkcs8", "rsa"], keyReader);

        //Assert
        keyReader.Received(1).GetPublicKey("rsa", Arg.Is<IReadOnlyDictionary<string, string>>(m => m.Count == 0));
        keyReader.Received(1).GetPublicKey("rsa-pkcs8", Arg.Is<IReadOnlyDictionary<string, string>>(m => m.Count == 0));
        keyReader.ReceivedCalls().Count().ShouldBe(2);
    }

    [Fact]
    public void AddPublicKeyCipher_GivenCalledTwice_ShouldRotateTheDataKeyAndKeepOldMessagesDecryptable()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        using var consumer = new MessageCrypto(false);
        using var privateKey = (RSA) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private("ClientRsa"));
        var firstMetadata = CreateMetadata();
        var first = Encrypt(uut, firstMetadata, _payload);

        //Act
        uut.AddPublicKeyCipher(["rsa"], keyReader);

        //Assert
        var secondMetadata = CreateMetadata();
        var second = Encrypt(uut, secondMetadata, _payload);
        var firstDataKey = privateKey.Decrypt(firstMetadata.EncryptionKeys[0].Value.ToByteArray(), RSAEncryptionPadding.OaepSHA1);
        var secondDataKey = privateKey.Decrypt(secondMetadata.EncryptionKeys[0].Value.ToByteArray(), RSAEncryptionPadding.OaepSHA1);
        firstDataKey.ShouldNotBe(secondDataKey);
        Decrypt(consumer, keyReader, firstMetadata, first).ShouldBe(_payload);
        Decrypt(consumer, keyReader, secondMetadata, second).ShouldBe(_payload);
    }

    [Fact]
    public async Task AddPublicKeyCipher_GivenRefreshInterval_ShouldRotateTheDataKeyPeriodically()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        var counting = Substitute.For<ICryptoKeyReader>();
        counting.GetPublicKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(call => keyReader.GetPublicKey(call.ArgAt<string>(0), call.ArgAt<IReadOnlyDictionary<string, string>>(1)));
        using var uut = new MessageCrypto(true, TimeSpan.FromMilliseconds(30));
        using var consumer = new MessageCrypto(false);
        var firstMetadata = CreateMetadata();

        //Act
        uut.AddPublicKeyCipher(["rsa"], counting);
        var first = Encrypt(uut, firstMetadata, _payload);
        await WaitUntil(() => counting.ReceivedCalls().Count() >= 4);

        //Assert
        var lastMetadata = CreateMetadata();
        var last = Encrypt(uut, lastMetadata, _payload);
        lastMetadata.EncryptionKeys[0].Value.ShouldNotBe(firstMetadata.EncryptionKeys[0].Value);
        Decrypt(consumer, keyReader, firstMetadata, first).ShouldBe(_payload);
        Decrypt(consumer, keyReader, lastMetadata, last).ShouldBe(_payload);
    }

    [Fact]
    public async Task AddPublicKeyCipher_GivenRefreshThatFails_ShouldKeepThePreviousDataKeyAndTryAgain()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        var calls = 0;
        var failing = Substitute.For<ICryptoKeyReader>();
        failing.GetPublicKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(call =>
        {
            if (Interlocked.Increment(ref calls) > 1)
                throw new InvalidOperationException("The key store is not available");

            return keyReader.GetPublicKey(call.ArgAt<string>(0), call.ArgAt<IReadOnlyDictionary<string, string>>(1));
        });
        using var uut = new MessageCrypto(true, TimeSpan.FromMilliseconds(30));
        using var consumer = new MessageCrypto(false);
        uut.AddPublicKeyCipher(["rsa"], failing);
        var firstMetadata = CreateMetadata();
        Encrypt(uut, firstMetadata, _payload);

        //Act
        await WaitUntil(() => Volatile.Read(ref calls) >= 4);

        //Assert
        var lastMetadata = CreateMetadata();
        var last = Encrypt(uut, lastMetadata, _payload);
        lastMetadata.EncryptionKeys[0].Value.ShouldBe(firstMetadata.EncryptionKeys[0].Value);
        Decrypt(consumer, keyReader, lastMetadata, last).ShouldBe(_payload);
    }

    [Fact]
    public async Task Dispose_GivenRefreshInterval_ShouldStopTheRefresh()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        var counting = Substitute.For<ICryptoKeyReader>();
        counting.GetPublicKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(call => keyReader.GetPublicKey(call.ArgAt<string>(0), call.ArgAt<IReadOnlyDictionary<string, string>>(1)));
        var uut = new MessageCrypto(true, TimeSpan.FromMilliseconds(20));
        uut.AddPublicKeyCipher(["rsa"], counting);
        await WaitUntil(() => counting.ReceivedCalls().Count() >= 3);

        //Act
        uut.Dispose();
        var callsAfterDispose = counting.ReceivedCalls().Count();
        await Task.Delay(200, Current.CancellationToken);

        //Assert
        counting.ReceivedCalls().Count().ShouldBe(callsAfterDispose);
    }

    [Fact]
    public void AddPublicKeyCipher_GivenConsumerMode_ShouldNotStartTheRefresh()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        var counting = Substitute.For<ICryptoKeyReader>();
        counting.GetPublicKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(call => keyReader.GetPublicKey(call.ArgAt<string>(0), call.ArgAt<IReadOnlyDictionary<string, string>>(1)));
        using var uut = new MessageCrypto(false, TimeSpan.FromMilliseconds(10));

        //Act
        uut.AddPublicKeyCipher(["rsa"], counting);
        Thread.Sleep(200);

        //Assert
        counting.ReceivedCalls().Count().ShouldBe(1);
    }

    [Theory]
    [InlineData("rsa")]
    [InlineData("rsa-pkcs8")]
    [InlineData("ecdsa")]
    [InlineData("ecdsa-521")]
    public void TryDecrypt_GivenEncryptedPayload_ShouldDecrypt(string keyName)
    {
        //Arrange
        if (keyName.StartsWith("ecdsa", StringComparison.Ordinal))
            SkipWhenExplicitCurvesAreNotSupported();

        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, keyName);
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out var decrypted);

        //Assert
        actual.ShouldBeTrue();
        decrypted.ToArray().ShouldBe(_payload);
    }

    [Fact]
    public void TryDecrypt_GivenMultipleKeys_ShouldDecryptWithAnyOfThem()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa", "rsa-pkcs8", "ecdsa", "ecdsa-521");
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        foreach (var keyName in new[] { "rsa", "rsa-pkcs8", "ecdsa", "ecdsa-521" })
        {
            var onlyKey = new InMemoryCryptoKeyReader().WithPrivateKey(keyName, CryptoTestKeys.Private(keyName switch
            {
                "rsa" => "ClientRsa",
                "rsa-pkcs8" => "ClientRsaPkcs8",
                "ecdsa" => "ClientEcdsa",
                _ => "CryptoEcdsa"
            }));
            using var uut = new MessageCrypto(false);

            //Act
            var actual = Decrypt(uut, onlyKey, metadata, encrypted);

            //Assert
            actual.ShouldBe(_payload);
        }
    }

    [Fact]
    public void TryDecrypt_GivenEncryptedPayload_ShouldUseLastKeyCacheWithoutCallingReaderTwice()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        using var uut = new MessageCrypto(false);
        var firstMetadata = CreateMetadata();
        var first = Encrypt(producer, firstMetadata, _payload);
        var secondMetadata = CreateMetadata();
        var second = Encrypt(producer, secondMetadata, _payload);

        //Act
        var actualFirst = Decrypt(uut, keyReader, firstMetadata, first);
        var actualSecond = Decrypt(uut, keyReader, secondMetadata, second);

        //Assert
        actualFirst.ShouldBe(_payload);
        actualSecond.ShouldBe(_payload);
        keyReader.Received(1).GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>());
    }

    [Fact]
    public void TryDecrypt_GivenMessagesFromTwoProducersWithTheSameDataKey_ShouldOnlyCallTheReaderOnce()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        using var uut = new MessageCrypto(false);
        var firstMetadata = CreateMetadata("producer-1");
        var first = Encrypt(producer, firstMetadata, _payload);
        var secondMetadata = CreateMetadata("producer-2");
        var second = Encrypt(producer, secondMetadata, _payload);
        var thirdMetadata = CreateMetadata(null);
        var third = Encrypt(producer, thirdMetadata, _payload);

        //Act
        var actualFirst = Decrypt(uut, keyReader, firstMetadata, first);
        var actualSecond = Decrypt(uut, keyReader, secondMetadata, second);
        var actualThird = Decrypt(uut, keyReader, thirdMetadata, third);
        var actualFirstAgain = Decrypt(uut, keyReader, firstMetadata, first);

        //Assert
        actualFirst.ShouldBe(_payload);
        actualSecond.ShouldBe(_payload);
        actualThird.ShouldBe(_payload);
        actualFirstAgain.ShouldBe(_payload);
        keyReader.Received(1).GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>());
    }

    [Fact]
    public void TryDecrypt_GivenRotatedDataKey_ShouldUnwrapTheNewKeyAndKeepUsingCachedKeysForOldMessages()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        using var uut = new MessageCrypto(false);
        var oldMetadata = CreateMetadata();
        var old = Encrypt(producer, oldMetadata, _payload);
        producer.AddPublicKeyCipher(["rsa"], producerKeyReader);
        var newMetadata = CreateMetadata();
        var @new = Encrypt(producer, newMetadata, _payload);

        //Act
        var actualOld = Decrypt(uut, keyReader, oldMetadata, old);
        var actualNew = Decrypt(uut, keyReader, newMetadata, @new);
        var actualOldAgain = Decrypt(uut, keyReader, oldMetadata, old);
        var actualNewAgain = Decrypt(uut, keyReader, newMetadata, @new);

        //Assert
        actualOld.ShouldBe(_payload);
        actualNew.ShouldBe(_payload);
        actualOldAgain.ShouldBe(_payload);
        actualNewAgain.ShouldBe(_payload);
        keyReader.Received(2).GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>());
    }

    [Fact]
    public void TryDecrypt_GivenKeyMetadata_ShouldPassTheMetadataToTheKeyReader()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader(new Dictionary<string, string> { ["version"] = "1.0" });
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = Decrypt(uut, keyReader, metadata, encrypted);

        //Assert
        actual.ShouldBe(_payload);
        keyReader.Received(1).GetPrivateKey("rsa", Arg.Is<IReadOnlyDictionary<string, string>>(m => m.Count == 1 && m["version"] == "1.0"));
    }

    [Fact]
    public void TryDecrypt_GivenWrongPrivateKey_ShouldReturnFalse()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey("rsa", CryptoTestKeys.Private("CryptoRsa"));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out var decrypted);

        //Assert
        actual.ShouldBeFalse();
        decrypted.IsEmpty.ShouldBeTrue();
    }

    // The upstream key 'private-key.client-mismatch-rsa.pem' is an EC key
    [Fact]
    public void TryDecrypt_GivenPrivateKeyOfAnotherType_ShouldReturnFalse()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey("rsa", Encoding.UTF8.GetBytes(CryptoTestKeys.ClientMismatchRsaPrivate));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenWrongPrivateKeyForEcKey_ShouldReturnFalse()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "ecdsa");
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey("ecdsa", CryptoTestKeys.Private("CryptoEcdsa"));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenKeyReaderWithoutTheKey_ShouldReturnFalse()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = new InMemoryCryptoKeyReader();
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenKeyReaderThatThrows_ShouldReturnFalse()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey(Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(_ => throw new InvalidOperationException("boom"));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenInvalidPrivateKey_ShouldReturnFalse()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey("rsa", Encoding.UTF8.GetBytes("not a key"));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenPublicKeyAsPrivateKey_ShouldReturnFalse()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey("rsa", CryptoTestKeys.Public("ClientRsa"));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenFirstKeyThatCannotBeUsed_ShouldTryTheNextKey()
    {
        //Arrange
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa", "rsa-pkcs8");
        var keyReader = new InMemoryCryptoKeyReader()
            .WithPrivateKey("rsa", CryptoTestKeys.Private("CryptoRsa"))
            .WithPrivateKey("rsa-pkcs8", CryptoTestKeys.Private("ClientRsaPkcs8"));
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = Decrypt(uut, keyReader, metadata, encrypted);

        //Assert
        actual.ShouldBe(_payload);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(10)]
    [InlineData(28)]
    [InlineData(29)]
    [InlineData(44)]
    public void TryDecrypt_GivenTamperedPayloadOrTag_ShouldReturnFalse(int index)
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload).ToArray();
        encrypted[index] ^= 1;

        //Act
        var actual = uut.TryDecrypt(metadata, new ReadOnlySequence<byte>(encrypted), keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenTamperedIv_ShouldReturnFalse()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);
        var iv = metadata.EncryptionParam.ToByteArray();
        iv[0] ^= 1;
        metadata.EncryptionParam = Google.Protobuf.ByteString.CopyFrom(iv);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenTamperedWrappedKey_ShouldReturnFalse()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);
        var wrapped = metadata.EncryptionKeys[0].Value.ToByteArray();
        wrapped[10] ^= 1;
        metadata.EncryptionKeys[0] = new EncryptionKeys { Key = "rsa", Value = Google.Protobuf.ByteString.CopyFrom(wrapped) };

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(15)]
    public void TryDecrypt_GivenPayloadShorterThanTheTag_ShouldReturnFalse(int length)
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, new ReadOnlySequence<byte>(new byte[length]), keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(11)]
    [InlineData(13)]
    public void TryDecrypt_GivenInvalidIvLength_ShouldReturnFalse(int length)
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);
        metadata.EncryptionParam = Google.Protobuf.ByteString.CopyFrom(new byte[length]);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenMetadataWithoutEncryptionKeys_ShouldReturnFalse()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);
        metadata.EncryptionKeys.Clear();

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, keyReader, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryDecrypt_GivenMultiSegmentSequence_ShouldDecrypt()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var data = Enumerable.Range(0, 5000).Select(i => (byte) (i * 7)).ToArray();
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, data).ToArray();

        //Act
        var actual = Decrypt(uut, keyReader, metadata, Split(encrypted, 3, 1000, 4000));

        //Assert
        actual.ShouldBe(data);
    }

    [Fact]
    public void TryDecrypt_GivenCachedKeyThatHasNotBeenUsedForMoreThanFourHours_ShouldCallTheReaderAgain()
    {
        //Arrange
        var now = TimeSpan.FromMinutes(1);
        var producerKeyReader = CreateKeyReader();
        using var producer = CreateProducer(producerKeyReader, "rsa");
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        using var uut = new MessageCrypto(false, clock: () => now);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var first = Decrypt(uut, keyReader, metadata, encrypted);
        now += TimeSpan.FromHours(3);
        var second = Decrypt(uut, keyReader, metadata, encrypted);
        now += TimeSpan.FromHours(3);
        var third = Decrypt(uut, keyReader, metadata, encrypted);
        now += TimeSpan.FromHours(4) + TimeSpan.FromMinutes(1);
        var fourth = Decrypt(uut, keyReader, metadata, encrypted);

        //Assert
        first.ShouldBe(_payload);
        second.ShouldBe(_payload);
        third.ShouldBe(_payload);
        fourth.ShouldBe(_payload);
        keyReader.Received(2).GetPrivateKey("rsa", Arg.Any<IReadOnlyDictionary<string, string>>());
    }

    [Fact]
    public void TryDecrypt_GivenDisposed_ShouldThrowObjectDisposedException()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);
        uut.Dispose();

        //Act
        var exception = Record.Exception(() => uut.TryDecrypt(metadata, encrypted, keyReader, out _));

        //Assert
        exception.ShouldBeOfType<ObjectDisposedException>();
    }

    [Fact]
    public void AddPublicKeyCipher_GivenDisposed_ShouldThrowObjectDisposedException()
    {
        //Arrange
        var uut = new MessageCrypto(true);
        uut.Dispose();

        //Act
        var exception = Record.Exception(() => uut.AddPublicKeyCipher(["rsa"], CreateKeyReader()));

        //Assert
        exception.ShouldBeOfType<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_GivenCalledTwice_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateProducer(CreateKeyReader(), "rsa");
        uut.Dispose();

        //Act
        var exception = Record.Exception(() => uut.Dispose());

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public void TryDecrypt_GivenNullKeyReaderAndAKeyThatIsNotCached_ShouldReturnFalse()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = Encrypt(producer, metadata, _payload);

        //Act
        var actual = uut.TryDecrypt(metadata, encrypted, null!, out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Encrypt_GivenSteadyState_ShouldOnlyAllocateTheOutputBufferAndTheIv()
    {
        //Arrange
        const int payloadLength = 4096;
        var keyReader = CreateKeyReader();
        using var uut = CreateProducer(keyReader, "rsa");
        var metadata = CreateMetadata();
        var payload = new ReadOnlySequence<byte>(new byte[payloadLength]);
        uut.Encrypt(metadata, payload); // Warm up

        //Act
        var actual = AllocationMeasurement.SmallestBytesOfCurrentThread(() => uut.Encrypt(metadata, payload), payloadLength + 16 + 100);

        //Assert (the output buffer, the IV and its ByteString)
        actual.ShouldBeLessThan(payloadLength + 16 + 100);
    }

    [Fact]
    public void TryDecrypt_GivenSteadyState_ShouldOnlyAllocateTheOutputBuffer()
    {
        //Arrange
        const int payloadLength = 4096;
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var metadata = CreateMetadata();
        var encrypted = producer.Encrypt(metadata, new ReadOnlySequence<byte>(new byte[payloadLength]));
        uut.TryDecrypt(metadata, encrypted, keyReader, out _); // Warm up (the key is unwrapped and cached)

        //Act
        var decrypted = false;
        var actual = AllocationMeasurement.SmallestBytesOfCurrentThread(() => decrypted = uut.TryDecrypt(metadata, encrypted, keyReader, out _), payloadLength + 100);

        //Assert
        decrypted.ShouldBeTrue();
        actual.ShouldBeLessThan(payloadLength + 100);
    }

    [Fact]
    public void TryDecrypt_GivenConcurrentCalls_ShouldDecryptAll()
    {
        //Arrange
        var keyReader = CreateKeyReader();
        using var producer = CreateProducer(keyReader, "rsa");
        using var uut = new MessageCrypto(false);
        var messages = Enumerable.Range(0, 200).Select(i =>
        {
            var payload = Encoding.UTF8.GetBytes($"message {i}");
            var metadata = CreateMetadata($"producer-{i % 4}");
            return (Payload: payload, Metadata: metadata, Encrypted: Encrypt(producer, metadata, payload));
        }).ToArray();
        var failures = new ConcurrentBag<int>();

        //Act
        Parallel.For(0, messages.Length * 5, i =>
        {
            var message = messages[i % messages.Length];
            var actual = Decrypt(uut, keyReader, message.Metadata, message.Encrypted);
            if (actual is null || !actual.AsSpan().SequenceEqual(message.Payload))
                failures.Add(i);
        });

        //Assert
        failures.ShouldBeEmpty();
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var timeout = TimeSpan.FromSeconds(30);
        var start = DateTime.UtcNow;

        while (!condition())
        {
            if (DateTime.UtcNow - start > timeout)
                throw new TimeoutException("The condition was not met");

            await Task.Delay(10, Current.CancellationToken);
        }
    }
}
