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
public sealed class RsaKeyWrapTests
{
    // Additional coverage
    [Theory]
    [InlineData("ClientRsa")]
    [InlineData("ClientRsaPkcs8")]
    [InlineData("CryptoRsa")]
    public void WrapUnwrap_GivenPkcs1AndPkcs8Keys_ShouldRoundTrip(string keySet)
    {
        //Arrange
        using var publicKey = (RSA) PemKeyLoader.LoadPublicKey(CryptoTestKeys.Public(keySet));
        using var privateKey = (RSA) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private(keySet));
        var dataKey = RandomNumberGenerator.GetBytes(32);

        //Act
        var wrapped = RsaKeyWrap.Wrap(publicKey, dataKey);
        var actual = RsaKeyWrap.Unwrap(privateKey, wrapped);

        //Assert
        wrapped.Length.ShouldBe(256);
        actual.ShouldBe(dataKey);
    }

    // Additional coverage
    [Fact]
    public void Wrap_GivenSameInputTwice_ShouldReturnDifferentOutput()
    {
        //Arrange
        using var publicKey = (RSA) PemKeyLoader.LoadPublicKey(CryptoTestKeys.Public("ClientRsa"));
        var dataKey = RandomNumberGenerator.GetBytes(32);

        //Act
        var first = RsaKeyWrap.Wrap(publicKey, dataKey);
        var second = RsaKeyWrap.Wrap(publicKey, dataKey);

        //Assert
        first.ShouldNotBe(second);
    }

    // Additional coverage
    [Fact]
    public void Wrap_GivenKey_ShouldUseOaepWithSha1()
    {
        //Arrange
        using var publicKey = (RSA) PemKeyLoader.LoadPublicKey(CryptoTestKeys.Public("ClientRsa"));
        using var privateKey = (RSA) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private("ClientRsa"));
        var dataKey = RandomNumberGenerator.GetBytes(32);

        //Act
        var wrapped = RsaKeyWrap.Wrap(publicKey, dataKey);

        //Assert
        privateKey.Decrypt(wrapped, RSAEncryptionPadding.OaepSHA1).ShouldBe(dataKey);
        Should.Throw<CryptographicException>(() => privateKey.Decrypt(wrapped, RSAEncryptionPadding.OaepSHA256));
    }

    // Additional coverage
    [Fact]
    public void Unwrap_GivenWrongPrivateKey_ShouldThrow()
    {
        //Arrange
        using var publicKey = (RSA) PemKeyLoader.LoadPublicKey(CryptoTestKeys.Public("ClientRsa"));
        using var otherPrivateKey = (RSA) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private("CryptoRsa"));
        var wrapped = RsaKeyWrap.Wrap(publicKey, RandomNumberGenerator.GetBytes(32));

        //Act
        var exception = Record.Exception(() => RsaKeyWrap.Unwrap(otherPrivateKey, wrapped));

        //Assert
        exception.ShouldBeAssignableTo<CryptographicException>();
    }

    // Additional coverage
    [Fact]
    public void Unwrap_GivenTamperedWrappedKey_ShouldThrow()
    {
        //Arrange
        using var publicKey = (RSA) PemKeyLoader.LoadPublicKey(CryptoTestKeys.Public("ClientRsa"));
        using var privateKey = (RSA) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private("ClientRsa"));
        var wrapped = RsaKeyWrap.Wrap(publicKey, RandomNumberGenerator.GetBytes(32));
        wrapped[100] ^= 1;

        //Act
        var exception = Record.Exception(() => RsaKeyWrap.Unwrap(privateKey, wrapped));

        //Assert
        exception.ShouldBeAssignableTo<CryptographicException>();
    }
}
