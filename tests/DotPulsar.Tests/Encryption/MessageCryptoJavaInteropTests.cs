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

namespace DotPulsar.Tests.Encryption;

using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Crypto;
using Google.Protobuf;
using System.Buffers;
using System.Security.Cryptography;

// These tests prove the byte-exact compatibility with the Java client (and BouncyCastle) in both directions without a broker
[Trait("Category", "Integration")]
public sealed class MessageCryptoJavaInteropTests : IClassFixture<JavaCryptoFixture>
{
    private readonly JavaCryptoFixture _fixture;

    public MessageCryptoJavaInteropTests(JavaCryptoFixture fixture)
        => _fixture = fixture;

    private static void SkipWhenExplicitCurvesAreNotSupported(string keySet)
        => Assert.SkipWhen(keySet.Contains("Ecdsa", StringComparison.Ordinal) && OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");

    private static MessageMetadata CreateMetadata()
        => new() { ProducerName = "producer", SequenceId = 1, PublishTime = 1 };

    [Theory]
    [InlineData("ClientRsa", 0)]
    [InlineData("ClientRsa", 1000)]
    [InlineData("ClientRsaPkcs8", 1000)]
    [InlineData("CryptoRsa", 1000)]
    [InlineData("ClientEcdsa", 0)]
    [InlineData("ClientEcdsa", 1000)]
    [InlineData("CryptoEcdsa", 0)]
    [InlineData("CryptoEcdsa", 1000)]
    public async Task TryDecrypt_GivenPayloadEncryptedByTheJavaClient_ShouldDecrypt(string keySet, int length)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported(keySet);
        var payload = RandomNumberGenerator.GetBytes(length);
        var encrypted = await _fixture.Encrypt(keySet, payload, Current.CancellationToken);
        var metadata = CreateMetadata();
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = keySet, Value = ByteString.CopyFrom(encrypted.WrappedKey) });
        metadata.EncryptionParam = ByteString.CopyFrom(encrypted.Iv);
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey(keySet, CryptoTestKeys.Private(keySet));
        using var uut = new MessageCrypto(false);

        //Act
        var actual = uut.TryDecrypt(metadata, new ReadOnlySequence<byte>(encrypted.Ciphertext), keyReader, out var decrypted);

        //Assert
        actual.ShouldBeTrue();
        decrypted.ToArray().ShouldBe(payload);
        encrypted.KeyMetadata.ShouldBe("version:1.0");
    }

    [Theory]
    [InlineData("ClientRsa", 0)]
    [InlineData("ClientRsa", 1000)]
    [InlineData("ClientRsaPkcs8", 1000)]
    [InlineData("CryptoRsa", 1000)]
    [InlineData("ClientEcdsa", 0)]
    [InlineData("ClientEcdsa", 1000)]
    [InlineData("CryptoEcdsa", 0)]
    [InlineData("CryptoEcdsa", 1000)]
    public async Task Encrypt_GivenPayload_ShouldBeDecryptedByTheJavaClient(string keySet, int length)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported(keySet);
        var payload = RandomNumberGenerator.GetBytes(length);
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey(keySet, CryptoTestKeys.Public(keySet));
        using var uut = new MessageCrypto(true);
        uut.AddPublicKeyCipher([keySet], keyReader);
        var metadata = CreateMetadata();

        //Act
        var encrypted = uut.Encrypt(metadata, new ReadOnlySequence<byte>(payload)).ToArray();
        var actual = await _fixture.Decrypt(keySet, metadata.EncryptionKeys[0].Value.ToByteArray(), metadata.EncryptionParam.ToByteArray(), encrypted, Current.CancellationToken);

        //Assert
        actual.ShouldBe(payload);
    }

    [Fact]
    public async Task Encrypt_GivenTwoKeyNames_ShouldBeDecryptedByTheJavaClientWithEitherKey()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported("Ecdsa");
        var payload = RandomNumberGenerator.GetBytes(500);
        var keyReader = new InMemoryCryptoKeyReader()
            .WithPublicKey("ClientRsa", CryptoTestKeys.Public("ClientRsa"))
            .WithPublicKey("ClientEcdsa", CryptoTestKeys.Public("ClientEcdsa"));
        using var uut = new MessageCrypto(true);
        uut.AddPublicKeyCipher(["ClientEcdsa", "ClientRsa"], keyReader);
        var metadata = CreateMetadata();
        var encrypted = uut.Encrypt(metadata, new ReadOnlySequence<byte>(payload)).ToArray();

        //Act
        var rsaKey = metadata.EncryptionKeys.Single(k => k.Key == "ClientRsa").Value.ToByteArray();
        var ecdsaKey = metadata.EncryptionKeys.Single(k => k.Key == "ClientEcdsa").Value.ToByteArray();
        var actualRsa = await _fixture.Decrypt("ClientRsa", rsaKey, metadata.EncryptionParam.ToByteArray(), encrypted, Current.CancellationToken);
        var actualEcdsa = await _fixture.Decrypt("ClientEcdsa", ecdsaKey, metadata.EncryptionParam.ToByteArray(), encrypted, Current.CancellationToken);

        //Assert
        metadata.EncryptionKeys.Select(k => k.Key).ShouldBe(["ClientEcdsa", "ClientRsa"]);
        actualRsa.ShouldBe(payload);
        actualEcdsa.ShouldBe(payload);
    }

    [Fact]
    public async Task Encrypt_GivenTamperedPayload_ShouldNotBeDecryptedByTheJavaClient()
    {
        //Arrange
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey("ClientRsa", CryptoTestKeys.Public("ClientRsa"));
        using var uut = new MessageCrypto(true);
        uut.AddPublicKeyCipher(["ClientRsa"], keyReader);
        var metadata = CreateMetadata();
        var encrypted = uut.Encrypt(metadata, new ReadOnlySequence<byte>(RandomNumberGenerator.GetBytes(100))).ToArray();
        encrypted[50] ^= 1;

        //Act
        var actual = await _fixture.Decrypt("ClientRsa", metadata.EncryptionKeys[0].Value.ToByteArray(), metadata.EncryptionParam.ToByteArray(), encrypted, Current.CancellationToken);

        //Assert
        actual.ShouldBeNull();
    }
}
