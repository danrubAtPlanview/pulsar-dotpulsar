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
using System.Numerics;
using System.Security.Cryptography;

[Trait("Category", "Unit")]
public sealed class EciesKeyWrapTests
{
    private static readonly byte[] _vectorSharedInput = Enumerable.Range(0, 32).Select(i => (byte) i).ToArray();
    private static readonly byte[] _vectorMessage = Enumerable.Range(0x40, 32).Select(i => (byte) i).ToArray();
    private static readonly byte[] _vectorCiphertext = Convert.FromHexString("af188fa5a2f3a0f2ac4ca11523b64643695d7e8b7736840d21a5af3c9f3e68b6");
    private static readonly byte[] _vectorTag = Convert.FromHexString("5a344411ae6820114d3588a57287eebf59139aab");

    private static ECDiffieHellman LoadPublicKey(string keySet)
        => (ECDiffieHellman) PemKeyLoader.LoadPublicKey(CryptoTestKeys.Public(keySet));

    private static ECDiffieHellman LoadPrivateKey(string keySet)
        => (ECDiffieHellman) PemKeyLoader.LoadPrivateKey(CryptoTestKeys.Private(keySet));

    private static byte[] CreateDataKey()
        => RandomNumberGenerator.GetBytes(32);

    private static void SkipWhenExplicitCurvesAreNotSupported()
        => Assert.SkipWhen(OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");

    [Fact]
    public void IesEncrypt_GivenVectorInput_ShouldReturnExpectedCiphertextAndTag()
    {
        //Act
        var actual = EciesKeyWrap.IesEncrypt(_vectorSharedInput, _vectorMessage);

        //Assert
        actual.Length.ShouldBe(32 + 20);
        actual.Take(32).ToArray().ShouldBe(_vectorCiphertext);
        actual.Skip(32).ToArray().ShouldBe(_vectorTag);
    }

    [Fact]
    public void TryIesDecrypt_GivenVectorOutput_ShouldReturnMessage()
    {
        //Arrange
        var data = _vectorCiphertext.Concat(_vectorTag).ToArray();

        //Act
        var actual = EciesKeyWrap.TryIesDecrypt(_vectorSharedInput, data, 0, data.Length, out var message);

        //Assert
        actual.ShouldBeTrue();
        message.ShouldBe(_vectorMessage);
    }

    [Fact]
    public void TryIesDecrypt_GivenOffset_ShouldIgnoreThePrefix()
    {
        //Arrange
        var data = new byte[] { 9, 9, 9 }.Concat(_vectorCiphertext).Concat(_vectorTag).ToArray();

        //Act
        var actual = EciesKeyWrap.TryIesDecrypt(_vectorSharedInput, data, 3, data.Length - 3, out var message);

        //Assert
        actual.ShouldBeTrue();
        message.ShouldBe(_vectorMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(51)]
    public void TryIesDecrypt_GivenTamperedByte_ShouldReturnFalse(int index)
    {
        //Arrange
        var data = _vectorCiphertext.Concat(_vectorTag).ToArray();
        data[index] ^= 1;

        //Act
        var actual = EciesKeyWrap.TryIesDecrypt(_vectorSharedInput, data, 0, data.Length, out var message);

        //Assert
        actual.ShouldBeFalse();
        message.ShouldBeNull();
    }

    [Fact]
    public void TryIesDecrypt_GivenTooShortInput_ShouldReturnFalse()
    {
        //Act
        var actual = EciesKeyWrap.TryIesDecrypt(_vectorSharedInput, new byte[19], 0, 19, out var message);

        //Assert
        actual.ShouldBeFalse();
        message.ShouldBeNull();
    }

    [Fact]
    public void TryIesDecrypt_GivenEmptyMessage_ShouldReturnEmptyMessage()
    {
        //Arrange
        var data = EciesKeyWrap.IesEncrypt(_vectorSharedInput, []);

        //Act
        var actual = EciesKeyWrap.TryIesDecrypt(_vectorSharedInput, data, 0, data.Length, out var message);

        //Assert
        data.Length.ShouldBe(20);
        actual.ShouldBeTrue();
        message.ShouldBe([]);
    }

    [Fact]
    public void WrapUnwrap_GivenSecp128r1ExplicitKey_ShouldRoundTrip()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var dataKey = CreateDataKey();

        //Act
        var wrapped = EciesKeyWrap.Wrap(publicKey, dataKey);
        var actual = EciesKeyWrap.Unwrap(privateKey, wrapped);

        //Assert
        wrapped.Length.ShouldBe(85);
        wrapped[0].ShouldBe((byte) 0x04);
        actual.ShouldBe(dataKey);
    }

    [Fact]
    public void WrapUnwrap_GivenSecp521r1ExplicitKey_ShouldRoundTrip()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("CryptoEcdsa");
        using var privateKey = LoadPrivateKey("CryptoEcdsa");
        var dataKey = CreateDataKey();

        //Act
        var wrapped = EciesKeyWrap.Wrap(publicKey, dataKey);
        var actual = EciesKeyWrap.Unwrap(privateKey, wrapped);

        //Assert
        wrapped.Length.ShouldBe(1 + 132 + 32 + 20);
        wrapped[0].ShouldBe((byte) 0x04);
        actual.ShouldBe(dataKey);
    }

    [Fact]
    public void WrapUnwrap_GivenNamedCurveKey_ShouldRoundTrip()
    {
        //Arrange
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        var dataKey = CreateDataKey();

        //Act
        var wrapped = EciesKeyWrap.Wrap(key, dataKey);
        var actual = EciesKeyWrap.Unwrap(key, wrapped);

        //Assert
        wrapped.Length.ShouldBe(1 + 64 + 32 + 20);
        actual.ShouldBe(dataKey);
    }

    [Fact]
    public void WrapUnwrap_GivenManyKeys_ShouldRoundTripIncludingCoordinatesWithLeadingZeros()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var sawLeadingZero = false;

        for (var i = 0; i < 1000; ++i)
        {
            var dataKey = CreateDataKey();

            //Act
            var wrapped = EciesKeyWrap.Wrap(publicKey, dataKey);
            var actual = EciesKeyWrap.Unwrap(privateKey, wrapped);

            //Assert
            wrapped.Length.ShouldBe(85);
            actual.ShouldBe(dataKey);
            sawLeadingZero |= wrapped[1] == 0 || wrapped[17] == 0;
        }

        sawLeadingZero.ShouldBeTrue();
    }

    [Fact]
    public void Wrap_GivenSameInputTwice_ShouldReturnDifferentOutput()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        var dataKey = CreateDataKey();

        //Act
        var first = EciesKeyWrap.Wrap(publicKey, dataKey);
        var second = EciesKeyWrap.Wrap(publicKey, dataKey);

        //Assert
        first.ShouldNotBe(second);
    }

    [Fact]
    public void Wrap_GivenKey_ShouldUseTheXCoordinateOfTheSharedPointAsDefinedByTheSpecification()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var curve = new ReferenceEc(privateKey.ExportExplicitParameters(false).Curve);
        var d = ReferenceEc.ToBigInteger(privateKey.ExportExplicitParameters(true).D!);
        var dataKey = CreateDataKey();

        //Act
        var wrapped = EciesKeyWrap.Wrap(publicKey, dataKey);

        //Assert
        var ephemeralPoint = new ReferenceEc.Point(
            ReferenceEc.ToBigInteger(wrapped[1..17]),
            ReferenceEc.ToBigInteger(wrapped[17..33]));
        var sharedPoint = curve.Multiply(d, ephemeralPoint);
        var sharedInput = wrapped[..33].Concat(curve.ToBytes(sharedPoint.X)).ToArray();
        var decrypted = EciesKeyWrap.TryIesDecrypt(sharedInput, wrapped, 33, wrapped.Length - 33, out var message);
        decrypted.ShouldBeTrue();
        message.ShouldBe(dataKey);
    }

    [Theory]
    [InlineData("ClientEcdsa")]
    [InlineData("CryptoEcdsa")]
    public void Unwrap_GivenWrappedKeyBuiltFromTheSpecification_ShouldReturnDataKey(string keySet)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey(keySet);
        using var privateKey = LoadPrivateKey(keySet);
        var publicParameters = publicKey.ExportExplicitParameters(false);
        var curve = new ReferenceEc(publicParameters.Curve);
        var recipient = new ReferenceEc.Point(ReferenceEc.ToBigInteger(publicParameters.Q.X!), ReferenceEc.ToBigInteger(publicParameters.Q.Y!));
        var ephemeralScalar = (ReferenceEc.ToBigInteger(RandomNumberGenerator.GetBytes(curve.FieldLength)) % (curve.Order - 1)) + 1;
        var ephemeralPoint = curve.Multiply(ephemeralScalar, curve.Generator);
        var sharedPoint = curve.Multiply(ephemeralScalar, recipient);
        var v = new byte[] { 0x04 }.Concat(curve.ToBytes(ephemeralPoint.X)).Concat(curve.ToBytes(ephemeralPoint.Y)).ToArray();
        var dataKey = CreateDataKey();
        var wrapped = v.Concat(EciesKeyWrap.IesEncrypt(v.Concat(curve.ToBytes(sharedPoint.X)).ToArray(), dataKey)).ToArray();

        //Act
        var actual = EciesKeyWrap.Unwrap(privateKey, wrapped);

        //Assert
        actual.ShouldBe(dataKey);
    }

    // IESEngine: Z = BigIntegers.asUnsignedByteArray(fieldSize, x), so a leading zero byte of the x-coordinate must be kept (about 1 in 256 shared secrets)
    [Fact]
    public void Wrap_GivenSharedSecretWithLeadingZeroByte_ShouldPadTheSharedSecretToTheFieldSize()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var curve = new ReferenceEc(privateKey.ExportExplicitParameters(false).Curve);
        var d = ReferenceEc.ToBigInteger(privateKey.ExportExplicitParameters(true).D!);
        var dataKey = CreateDataKey();
        var found = false;

        for (var i = 0; i < 5000 && !found; ++i)
        {
            //Act
            var wrapped = EciesKeyWrap.Wrap(publicKey, dataKey);

            //Assert
            var ephemeralPoint = new ReferenceEc.Point(ReferenceEc.ToBigInteger(wrapped[1..17]), ReferenceEc.ToBigInteger(wrapped[17..33]));
            var z = curve.ToBytes(curve.Multiply(d, ephemeralPoint).X);
            if (z[0] != 0)
                continue;

            found = true;
            var sharedInput = wrapped[..33].Concat(z).ToArray();
            EciesKeyWrap.TryIesDecrypt(sharedInput, wrapped, 33, wrapped.Length - 33, out var message).ShouldBeTrue();
            message.ShouldBe(dataKey);
        }

        found.ShouldBeTrue();
    }

    [Fact]
    public void Unwrap_GivenSharedSecretWithLeadingZeroByte_ShouldPadTheSharedSecretToTheFieldSize()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var publicParameters = publicKey.ExportExplicitParameters(false);
        var curve = new ReferenceEc(publicParameters.Curve);
        var recipient = new ReferenceEc.Point(ReferenceEc.ToBigInteger(publicParameters.Q.X!), ReferenceEc.ToBigInteger(publicParameters.Q.Y!));
        var dataKey = CreateDataKey();
        byte[]? wrapped = null;

        for (var i = 0; i < 5000 && wrapped is null; ++i)
        {
            var ephemeralScalar = (ReferenceEc.ToBigInteger(RandomNumberGenerator.GetBytes(curve.FieldLength)) % (curve.Order - 1)) + 1;
            var z = curve.ToBytes(curve.Multiply(ephemeralScalar, recipient).X);
            if (z[0] != 0)
                continue;

            var ephemeralPoint = curve.Multiply(ephemeralScalar, curve.Generator);
            var v = new byte[] { 0x04 }.Concat(curve.ToBytes(ephemeralPoint.X)).Concat(curve.ToBytes(ephemeralPoint.Y)).ToArray();
            wrapped = v.Concat(EciesKeyWrap.IesEncrypt(v.Concat(z).ToArray(), dataKey)).ToArray();
        }

        wrapped.ShouldNotBeNull();

        //Act
        var actual = EciesKeyWrap.Unwrap(privateKey, wrapped);

        //Assert
        actual.ShouldBe(dataKey);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(40)]
    [InlineData(84)]
    public void Unwrap_GivenTamperedByte_ShouldThrow(int index)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var wrapped = EciesKeyWrap.Wrap(publicKey, CreateDataKey());
        wrapped[index] ^= 1;

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(privateKey, wrapped));

        //Assert
        exception.ShouldBeAssignableTo<CryptographicException>();
    }

    [Fact]
    public void Unwrap_GivenTamperedMac_ShouldFail()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var wrapped = EciesKeyWrap.Wrap(publicKey, CreateDataKey());
        wrapped[^1] ^= 1;

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(privateKey, wrapped));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("MAC");
    }

    [Fact]
    public void Unwrap_GivenWrongPrivateKey_ShouldFail()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var otherPrivateKey = ECDiffieHellman.Create(publicKey.ExportParameters(false).Curve);
        var wrapped = EciesKeyWrap.Wrap(publicKey, CreateDataKey());

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(otherPrivateKey, wrapped));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("MAC");
    }

    [Theory]
    [InlineData(0x02)]
    [InlineData(0x03)]
    public void Unwrap_GivenCompressedEphemeralKey_ShouldThrow(int encoding)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var wrapped = EciesKeyWrap.Wrap(publicKey, CreateDataKey());
        wrapped[0] = (byte) encoding;

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(privateKey, wrapped));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("Compressed");
    }

    [Theory]
    [InlineData(0x00)]
    [InlineData(0x05)]
    [InlineData(0xFF)]
    public void Unwrap_GivenInvalidPointEncoding_ShouldThrow(int encoding)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var wrapped = EciesKeyWrap.Wrap(publicKey, CreateDataKey());
        wrapped[0] = (byte) encoding;

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(privateKey, wrapped));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("point encoding");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(33)]
    [InlineData(52)]
    [InlineData(84)]
    public void Unwrap_GivenTooShortInput_ShouldThrow(int length)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        using var publicKey = LoadPublicKey("ClientEcdsa");
        using var privateKey = LoadPrivateKey("ClientEcdsa");
        var wrapped = EciesKeyWrap.Wrap(publicKey, CreateDataKey())[..length];

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(privateKey, wrapped));

        //Assert
        exception.ShouldBeAssignableTo<CryptographicException>();
    }

    [Fact]
    public void Unwrap_GivenPointThatIsNotOnTheCurve_ShouldThrow()
    {
        //Arrange
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        var wrapped = EciesKeyWrap.Wrap(key, CreateDataKey());
        wrapped[64] ^= 1; // The last byte of Y

        //Act
        var exception = Record.Exception(() => EciesKeyWrap.Unwrap(key, wrapped));

        //Assert
        exception.ShouldBeAssignableTo<CryptographicException>();
    }

    [Fact]
    public void IsEcKey_GivenEcAndRsaKeys_ShouldReturnWhetherTheKeyIsEc()
    {
        //Arrange
        using var ec = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        using var rsa = RSA.Create(2048);

        //Act
        var actualEc = EciesKeyWrap.IsEcKey(ec);
        var actualRsa = EciesKeyWrap.IsEcKey(rsa);

        //Assert
        actualEc.ShouldBeTrue();
        actualRsa.ShouldBeFalse();
    }
}
