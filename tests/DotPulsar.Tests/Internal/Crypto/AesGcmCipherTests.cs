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

using DotPulsar.Internal.Crypto;
using System.Security.Cryptography;

[Trait("Category", "Unit")]
public sealed class AesGcmCipherTests
{
    private static byte[] Hex(string hex) => Convert.FromHexString(hex);

    // McGrew and Viega, "The Galois/Counter Mode of Operation (GCM)", test cases 13 and 14
    // Additional coverage
    [Fact]
    public void Encrypt_GivenNistTestCase13And14_ShouldMatch()
    {
        //Arrange
        using var uut = new AesGcmCipher(new byte[32]);
        var tag13 = new byte[16];
        var ciphertext14 = new byte[16];
        var tag14 = new byte[16];

        //Act
        var actual13 = uut.Encrypt(new byte[12], [], [], tag13);
        var actual14 = uut.Encrypt(new byte[12], new byte[16], ciphertext14, tag14);

        //Assert
        actual13.ShouldBeTrue();
        tag13.ShouldBe(Hex("530f8afbc74536b9a963b4f1c4cb738b"));
        actual14.ShouldBeTrue();
        ciphertext14.ShouldBe(Hex("cea7403d4d606b6e074ec5d3baf39d18"));
        tag14.ShouldBe(Hex("d0d1c8a799996bf0265b98b5d48ab919"));
    }

    // Additional coverage
    [Fact]
    public void Encrypt_GivenNistTestCase15_ShouldMatch()
    {
        //Arrange
        var key = Hex("feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308");
        var iv = Hex("cafebabefacedbaddecaf888");
        var plaintext = Hex("d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b391aafd255");
        using var uut = new AesGcmCipher(key);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[16];

        //Act
        var actual = uut.Encrypt(iv, plaintext, ciphertext, tag);

        //Assert
        actual.ShouldBeTrue();
        ciphertext.ShouldBe(Hex("522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662898015ad"));
        tag.ShouldBe(Hex("b094dac5d93471bdec1a502270e3cc6c"));
    }

    // Additional coverage
    [Fact]
    public void Decrypt_GivenNistTestCase14_ShouldReturnPlaintext()
    {
        //Arrange
        using var uut = new AesGcmCipher(new byte[32]);
        var plaintext = new byte[16];
        plaintext.AsSpan().Fill(0xFF);

        //Act
        var actual = uut.Decrypt(new byte[12], Hex("cea7403d4d606b6e074ec5d3baf39d18"), Hex("d0d1c8a799996bf0265b98b5d48ab919"), plaintext);

        //Assert
        actual.ShouldBeTrue();
        plaintext.ShouldBe(new byte[16]);
    }

    // Additional coverage
    [Fact]
    public void Decrypt_GivenTamperedTag_ShouldReturnFalse()
    {
        //Arrange
        using var uut = new AesGcmCipher(new byte[32]);
        var tag = Hex("d0d1c8a799996bf0265b98b5d48ab919");
        tag[15] ^= 1;

        //Act
        var actual = uut.Decrypt(new byte[12], Hex("cea7403d4d606b6e074ec5d3baf39d18"), tag, new byte[16]);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Theory]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    public void Encrypt_GivenSupportedKeyLength_ShouldRoundTrip(int keyLength)
    {
        //Arrange
        var key = new byte[keyLength];
        key.AsSpan().Fill(7);
        using var uut = new AesGcmCipher(key);
        var plaintext = new byte[] { 1, 2, 3, 4, 5 };
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[16];
        var actual = new byte[plaintext.Length];

        //Act
        uut.Encrypt(new byte[12], plaintext, ciphertext, tag);
        var decrypted = uut.Decrypt(new byte[12], ciphertext, tag, actual);

        //Assert
        decrypted.ShouldBeTrue();
        actual.ShouldBe(plaintext);
        uut.KeyLength.ShouldBe(keyLength);
    }

    // Additional coverage
    [Fact]
    public void Encrypt_GivenInvalidKeyLength_ShouldThrow()
    {
        //Act
        var exception = Record.Exception(() => new AesGcmCipher(new byte[17]));

        //Assert (the exception type depends on the platform)
        (exception is ArgumentException || exception is CryptographicException).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Encrypt_GivenDisposed_ShouldReturnFalse()
    {
        //Arrange
        var uut = new AesGcmCipher(new byte[32]);
        uut.Dispose();

        //Act
        var actual = uut.Encrypt(new byte[12], [], [], new byte[16]);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Decrypt_GivenDisposed_ShouldReturnFalse()
    {
        //Arrange
        var uut = new AesGcmCipher(new byte[32]);
        uut.Dispose();

        //Act
        var actual = uut.Decrypt(new byte[12], [], Hex("530f8afbc74536b9a963b4f1c4cb738b"), []);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Dispose_GivenCalledTwice_ShouldNotThrow()
    {
        //Arrange
        var uut = new AesGcmCipher(new byte[32]);
        uut.Dispose();

        //Act
        var exception = Record.Exception(() => uut.Dispose());

        //Assert
        exception.ShouldBeNull();
    }
}
