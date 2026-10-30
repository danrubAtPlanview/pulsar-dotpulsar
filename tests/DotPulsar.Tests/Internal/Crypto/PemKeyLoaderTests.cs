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
using System.Security.Cryptography.X509Certificates;
using System.Text;

[Trait("Category", "Unit")]
public sealed class PemKeyLoaderTests
{
    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static void SkipWhenExplicitCurvesAreNotSupported()
        => Assert.SkipWhen(OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");

    [Theory]
    [InlineData(CryptoTestKeys.ClientRsaPublic)]
    [InlineData(CryptoTestKeys.ClientRsaPkcs8Public)]
    [InlineData(CryptoTestKeys.CryptoRsaPublic)]
    public void LoadPublicKey_GivenRsaSpkiPublic_ShouldReturnRsa(string pem)
    {
        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(pem));

        //Assert
        var rsa = actual.ShouldBeAssignableTo<RSA>().ShouldNotBeNull();
        rsa.KeySize.ShouldBe(2048);
    }

    [Theory]
    [InlineData(CryptoTestKeys.ClientRsaPrivate)]
    [InlineData(CryptoTestKeys.ClientRsaPkcs8Private)]
    [InlineData(CryptoTestKeys.CryptoRsaPrivate)]
    public void LoadPrivateKey_GivenRsaPkcs1AndPkcs8Private_ShouldReturnRsa(string pem)
    {
        //Act
        using var actual = PemKeyLoader.LoadPrivateKey(Bytes(pem));

        //Assert
        var rsa = actual.ShouldBeAssignableTo<RSA>().ShouldNotBeNull();
        rsa.KeySize.ShouldBe(2048);
        rsa.ExportParameters(true).D.ShouldNotBeNull();
    }

    [Theory]
    [InlineData(CryptoTestKeys.ClientEcdsaPublic, 128)]
    [InlineData(CryptoTestKeys.CryptoEcdsaPublic, 521)]
    public void LoadPublicKey_GivenEcExplicitParametersPublic_ShouldReturnEcDh(string pem, int keySize)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(pem));

        //Assert
        var ec = actual.ShouldBeAssignableTo<ECDiffieHellman>().ShouldNotBeNull();
        ec.KeySize.ShouldBe(keySize);
    }

    [Theory]
    [InlineData(CryptoTestKeys.ClientEcdsaPrivate, 128)]
    [InlineData(CryptoTestKeys.CryptoEcdsaPrivate, 521)]
    public void LoadPrivateKey_GivenEcExplicitParametersPrivate_ShouldReturnEcDh(string pem, int keySize)
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();

        //Act
        using var actual = PemKeyLoader.LoadPrivateKey(Bytes(pem));

        //Assert
        var ec = actual.ShouldBeAssignableTo<ECDiffieHellman>().ShouldNotBeNull();
        ec.KeySize.ShouldBe(keySize);
        ec.ExportParameters(true).D.ShouldNotBeNull();
    }

    // The upstream file 'private-key.client-mismatch-rsa.pem' is a secp521r1 key (with explicit parameters) and not an RSA key. That is what makes it a mismatch for the RSA public keys.
    [Fact]
    public void LoadPrivateKey_GivenClientMismatchRsaPrivate_ShouldReturnEcDh()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();

        //Act
        using var actual = PemKeyLoader.LoadPrivateKey(Bytes(CryptoTestKeys.ClientMismatchRsaPrivate));

        //Assert
        actual.ShouldBeAssignableTo<ECDiffieHellman>().KeySize.ShouldBe(521);
    }

    [Fact]
    public void LoadPublicKey_GivenEcExplicitParametersPublicAndPrivate_ShouldReturnMatchingKeys()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();

        //Act
        using var publicKey = (ECDiffieHellman) PemKeyLoader.LoadPublicKey(Bytes(CryptoTestKeys.ClientEcdsaPublic));
        using var privateKey = (ECDiffieHellman) PemKeyLoader.LoadPrivateKey(Bytes(CryptoTestKeys.ClientEcdsaPrivate));

        //Assert
        var expected = privateKey.ExportParameters(false).Q;
        var actual = publicKey.ExportParameters(false).Q;
        actual.X.ShouldBe(expected.X);
        actual.Y.ShouldBe(expected.Y);
    }

    [Fact]
    public void LoadPrivateKey_GivenEcPrivateKeyWithLeadingEcParametersBlock_ShouldSkipTheBlock()
    {
        //Arrange
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        var pem = "-----BEGIN EC PARAMETERS-----\nBggqhkjOPQMBBw==\n-----END EC PARAMETERS-----\n" + key.ExportECPrivateKeyPem();

        //Act
        using var actual = PemKeyLoader.LoadPrivateKey(Bytes(pem));

        //Assert
        actual.ShouldBeAssignableTo<ECDiffieHellman>().ExportParameters(true).D.ShouldBe(key.ExportParameters(true).D);
    }

    [Fact]
    public void LoadPublicKey_GivenSpkiWithLeadingEcParametersBlock_ShouldSkipTheBlock()
    {
        //Arrange
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP384);
        var pem = "-----BEGIN EC PARAMETERS-----\nBgUrgQQAIg==\n-----END EC PARAMETERS-----\n" + key.ExportSubjectPublicKeyInfoPem();

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(pem));

        //Assert
        actual.ShouldBeAssignableTo<ECDiffieHellman>().ExportParameters(false).Q.X.ShouldBe(key.ExportParameters(false).Q.X);
    }

    [Fact]
    public void LoadPrivateKey_GivenPkcs8EcKey_ShouldReturnEcDh()
    {
        //Arrange
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);

        //Act
        using var actual = PemKeyLoader.LoadPrivateKey(Bytes(key.ExportPkcs8PrivateKeyPem()));

        //Assert
        actual.ShouldBeAssignableTo<ECDiffieHellman>().ExportParameters(true).D.ShouldBe(key.ExportParameters(true).D);
    }

    [Fact]
    public void LoadPublicKey_GivenRsaPublicKeyPem_ShouldReturnRsa()
    {
        //Arrange
        using var key = RSA.Create(2048);

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(key.ExportRSAPublicKeyPem()));

        //Assert
        actual.ShouldBeAssignableTo<RSA>().ExportParameters(false).Modulus.ShouldBe(key.ExportParameters(false).Modulus);
    }

    [Fact]
    public void LoadPublicKey_GivenRsaCertificate_ShouldReturnRsa()
    {
        //Arrange
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(certificate.ExportCertificatePem()));

        //Assert
        actual.ShouldBeAssignableTo<RSA>().ExportParameters(false).Modulus.ShouldBe(key.ExportParameters(false).Modulus);
    }

    [Fact]
    public void LoadPublicKey_GivenEcCertificate_ShouldReturnEcDh()
    {
        //Arrange
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=test", key, HashAlgorithmName.SHA256);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(certificate.ExportCertificatePem()));

        //Assert
        actual.ShouldBeAssignableTo<ECDiffieHellman>().ExportParameters(false).Q.X.ShouldBe(key.ExportParameters(false).Q.X);
    }

    [Fact]
    public void LoadPublicKey_GivenCertificateWithoutVersion_ShouldReturnRsa()
    {
        //Arrange
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var der = certificate.RawData;

        // Remove the version ([0] EXPLICIT INTEGER, A0 03 02 01 02) that follows the headers of the two enclosing sequences (4 bytes each) and fix their lengths
        const int versionOffset = 8;
        der[versionOffset].ShouldBe((byte) 0xA0);
        var withoutVersion = der.Take(versionOffset).Concat(der.Skip(versionOffset + 5)).ToArray();
        var pem = new string(PemEncoding.Write("CERTIFICATE", FixCertificateLengths(withoutVersion)));

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(pem));

        //Assert
        actual.ShouldBeAssignableTo<RSA>().ExportParameters(false).Modulus.ShouldBe(key.ExportParameters(false).Modulus);
    }

    [Fact]
    public void LoadPublicKey_GivenWindowsLineEndingsAndLeadingWhitespace_ShouldReturnRsa()
    {
        //Arrange
        var pem = "﻿  \r\n" + CryptoTestKeys.ClientRsaPublic.Replace("\n", "\r\n") + "\r\n";

        //Act
        using var actual = PemKeyLoader.LoadPublicKey(Bytes(pem));

        //Assert
        actual.ShouldBeAssignableTo<RSA>();
    }

    [Fact]
    public void LoadPublicKey_GivenPrivateKey_ShouldThrow()
    {
        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPublicKey(Bytes(CryptoTestKeys.ClientRsaPrivate)));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("RSA PRIVATE KEY");
    }

    [Fact]
    public void LoadPrivateKey_GivenPublicKey_ShouldThrow()
    {
        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPrivateKey(Bytes(CryptoTestKeys.ClientRsaPublic)));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("PUBLIC KEY");
    }

    [Theory]
    [InlineData("")]
    [InlineData("not a pem file")]
    [InlineData("-----BEGIN PUBLIC KEY-----")]
    [InlineData("-----BEGIN PUBLIC KEY-----\nMIIB\n")]
    [InlineData("-----BEGIN EC PARAMETERS-----\nBggqhkjOPQMBBw==\n-----END EC PARAMETERS-----\n")]
    public void LoadPublicKey_GivenNoPemBlock_ShouldThrow(string pem)
    {
        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPublicKey(Bytes(pem)));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("No PEM block");
    }

    [Fact]
    public void LoadPublicKey_GivenInvalidBase64_ShouldThrow()
    {
        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPublicKey(Bytes("-----BEGIN PUBLIC KEY-----\n@@@@\n-----END PUBLIC KEY-----\n")));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("base64");
    }

    [Fact]
    public void LoadPrivateKey_GivenEncryptedPem_ShouldThrow()
    {
        //Arrange
        var pem = "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,0123456789ABCDEF0123456789ABCDEF\n\nMIIB\n-----END RSA PRIVATE KEY-----\n";

        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPrivateKey(Bytes(pem)));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("encrypted");
    }

    [Theory]
    [InlineData("MIIB")]
    [InlineData("MAA=")]
    [InlineData("MAMCAQA=")]
    public void LoadPublicKey_GivenInvalidDer_ShouldThrow(string base64)
    {
        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPublicKey(Bytes($"-----BEGIN PUBLIC KEY-----\n{base64}\n-----END PUBLIC KEY-----\n")));

        //Assert
        exception.ShouldBeAssignableTo<CryptographicException>();
    }

    [Fact]
    public void LoadPublicKey_GivenUnsupportedAlgorithm_ShouldThrow()
    {
        //Arrange (an Ed25519 public key with the OID 1.3.101.112)
        var der = new byte[] { 0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00 }.Concat(new byte[32]).ToArray();
        var pem = new string(PemEncoding.Write("PUBLIC KEY", der));

        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPublicKey(Bytes(pem)));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("2B6570");
    }

    [Fact]
    public void LoadPrivateKey_GivenUnsupportedPemType_ShouldThrow()
    {
        //Arrange
        var pem = "-----BEGIN DSA PRIVATE KEY-----\nMIIB\n-----END DSA PRIVATE KEY-----\n";

        //Act
        var exception = Record.Exception(() => PemKeyLoader.LoadPrivateKey(Bytes(pem)));

        //Assert
        exception.ShouldBeOfType<CryptographicException>().Message.ShouldContain("DSA PRIVATE KEY");
    }

    // Updates the lengths of the Certificate sequence and of the TBSCertificate sequence (both use the long form with two length bytes for a 2048 bit key)
    private static byte[] FixCertificateLengths(byte[] der)
    {
        var fixedDer = (byte[]) der.Clone();

        // Certificate ::= SEQUENCE (30 82 LL LL)
        fixedDer[0].ShouldBe((byte) 0x30);
        fixedDer[1].ShouldBe((byte) 0x82);
        var certificateLength = fixedDer.Length - 4;
        fixedDer[2] = (byte) (certificateLength >> 8);
        fixedDer[3] = (byte) certificateLength;

        // TBSCertificate ::= SEQUENCE (30 82 LL LL) which was shortened by five bytes
        fixedDer[4].ShouldBe((byte) 0x30);
        fixedDer[5].ShouldBe((byte) 0x82);
        var tbsLength = ((fixedDer[6] << 8) | fixedDer[7]) - 5;
        fixedDer[6] = (byte) (tbsLength >> 8);
        fixedDer[7] = (byte) tbsLength;

        return fixedDer;
    }
}
