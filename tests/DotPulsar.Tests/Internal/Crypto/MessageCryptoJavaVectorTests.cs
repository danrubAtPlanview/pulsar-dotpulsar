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
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;

// Known-answer tests: the messages were encrypted by the Java client (see JavaCryptoVectors)
[Trait("Category", "Unit")]
public sealed class MessageCryptoJavaVectorTests
{
    // Additional coverage
    [Theory]
    [MemberData(nameof(JavaCryptoVectors.All), MemberType = typeof(JavaCryptoVectors))]
    public void TryDecrypt_GivenMessageEncryptedByTheJavaClient_ShouldReturnThePlaintext(string keySet, string wrappedKey, string iv, string ciphertext, string plaintext)
    {
        //Arrange
        Assert.SkipWhen(keySet.Contains("Ecdsa", StringComparison.Ordinal) && OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");
        var metadata = new MessageMetadata { ProducerName = "java-producer", SequenceId = 1, PublishTime = 1 };
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = keySet, Value = ByteString.CopyFrom(Convert.FromHexString(wrappedKey)) });
        metadata.EncryptionParam = ByteString.CopyFrom(Convert.FromHexString(iv));
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey(keySet, CryptoTestKeys.Private(keySet));
        using var uut = new MessageCrypto(false);

        //Act
        var actual = uut.TryDecrypt(metadata, new ReadOnlySequence<byte>(Convert.FromHexString(ciphertext)), keyReader, out var decrypted);

        //Assert
        actual.ShouldBeTrue();
        Convert.ToHexString(decrypted.ToArray()).ToLowerInvariant().ShouldBe(plaintext);
    }

    // Additional coverage
    [Fact]
    public void TryDecrypt_GivenAllMessagesOfTheJavaClientInOrder_ShouldOnlyCallTheKeyReaderOncePerDataKey()
    {
        //Arrange
        var messages = JavaCryptoVectors.All.Select(row => row.Data).Where(data => data.Item1 == "ClientRsa").ToArray();
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey("ClientRsa", Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        using var uut = new MessageCrypto(false);

        //Act
        foreach (var message in messages)
        {
            var metadata = new MessageMetadata { ProducerName = "java-producer", SequenceId = 1, PublishTime = 1 };
            metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "ClientRsa", Value = ByteString.CopyFrom(Convert.FromHexString(message.Item2)) });
            metadata.EncryptionParam = ByteString.CopyFrom(Convert.FromHexString(message.Item3));
            var actual = uut.TryDecrypt(metadata, new ReadOnlySequence<byte>(Convert.FromHexString(message.Item4)), keyReader, out var decrypted);

            //Assert
            actual.ShouldBeTrue();
            Convert.ToHexString(decrypted.ToArray()).ToLowerInvariant().ShouldBe(message.Item5);
        }

        messages.Length.ShouldBe(3);
        keyReader.Received(1).GetPrivateKey("ClientRsa", Arg.Any<IReadOnlyDictionary<string, string>>());
    }
}
