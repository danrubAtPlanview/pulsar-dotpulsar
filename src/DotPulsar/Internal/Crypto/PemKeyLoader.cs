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

namespace DotPulsar.Internal.Crypto;

using System.Security.Cryptography;
using System.Text;

// Upstream: apache/pulsar@f3846f4 pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java (loadPublicKey, loadPrivateKey)
public static class PemKeyLoader
{
    private const string BeginMarker = "-----BEGIN ";
    private const string EndMarker = "-----END ";
    private const string Dashes = "-----";

    private const byte IntegerTag = 0x02;
    private const byte ObjectIdentifierTag = 0x06;
    private const byte SequenceTag = 0x30;
    private const byte VersionTag = 0xA0;

    // 1.2.840.113549.1.1.1 (rsaEncryption)
    private static readonly byte[] _rsaOid = [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01];

    // 1.2.840.10045.2.1 (id-ecPublicKey)
    private static readonly byte[] _ecOid = [0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01];

    // Loads the first public key of the PEM data. An optional leading 'EC PARAMETERS' block is skipped.
    // The supported types are 'PUBLIC KEY', 'RSA PUBLIC KEY' and 'CERTIFICATE'. The result is an RSA or an ECDiffieHellman instance.
    public static AsymmetricAlgorithm LoadPublicKey(byte[] pem)
    {
#if NETSTANDARD2_0
        throw new PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later");
#else
        var text = Encoding.UTF8.GetString(pem);
        var position = 0;

        while (TryReadBlock(text, ref position, out var label, out var der))
        {
            switch (label)
            {
                case "EC PARAMETERS":
                    continue;
                case "PUBLIC KEY":
                    return ImportSubjectPublicKeyInfo(der);
                case "RSA PUBLIC KEY":
                    return CreateRsa(rsa => rsa.ImportRSAPublicKey(der, out _));
                case "CERTIFICATE":
                case "X509 CERTIFICATE":
                    return ImportSubjectPublicKeyInfo(ExtractSubjectPublicKeyInfo(der));
                default:
                    throw new CryptographicException($"Unsupported PEM type: {label}");
            }
        }

        throw new CryptographicException("No PEM block was found");
#endif
    }

    // Loads the first private key of the PEM data. An optional leading 'EC PARAMETERS' block is skipped.
    // The supported types are 'RSA PRIVATE KEY' (PKCS#1), 'EC PRIVATE KEY' (SEC1) and 'PRIVATE KEY' (PKCS#8). The result is an RSA or an ECDiffieHellman instance.
    public static AsymmetricAlgorithm LoadPrivateKey(byte[] pem)
    {
#if NETSTANDARD2_0
        throw new PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later");
#else
        var text = Encoding.UTF8.GetString(pem);
        var position = 0;

        while (TryReadBlock(text, ref position, out var label, out var der))
        {
            try
            {
                switch (label)
                {
                    case "EC PARAMETERS":
                        continue;
                    case "RSA PRIVATE KEY":
                        return CreateRsa(rsa => rsa.ImportRSAPrivateKey(der, out _));
                    case "EC PRIVATE KEY":
                        return CreateEc(ec => ec.ImportECPrivateKey(der, out _));
                    case "PRIVATE KEY":
                        return ImportPkcs8PrivateKey(der);
                    default:
                        throw new CryptographicException($"Unsupported PEM type: {label}");
                }
            }
            finally
            {
                Array.Clear(der, 0, der.Length);
            }
        }

        throw new CryptographicException("No PEM block was found");
#endif
    }

#if !NETSTANDARD2_0
    private static bool TryReadBlock(string text, ref int position, out string label, out byte[] der)
    {
        label = string.Empty;
        der = [];

        var begin = text.IndexOf(BeginMarker, position, StringComparison.Ordinal);
        if (begin < 0)
            return false;

        var labelStart = begin + BeginMarker.Length;
        var labelEnd = text.IndexOf(Dashes, labelStart, StringComparison.Ordinal);
        if (labelEnd < 0)
            return false;

        var blockLabel = text.Substring(labelStart, labelEnd - labelStart);
        var bodyStart = labelEnd + Dashes.Length;
        var endMarker = EndMarker + blockLabel + Dashes;
        var end = text.IndexOf(endMarker, bodyStart, StringComparison.Ordinal);
        if (end < 0)
            return false;

        try
        {
            der = Convert.FromBase64String(text.Substring(bodyStart, end - bodyStart));
        }
        catch (FormatException e)
        {
            throw new CryptographicException($"The PEM block '{blockLabel}' is not valid base64 (encrypted PEM is not supported)", e);
        }

        label = blockLabel;
        position = end + endMarker.Length;
        return true;
    }

    private static AsymmetricAlgorithm ImportSubjectPublicKeyInfo(byte[] der)
    {
        var algorithm = ReadAlgorithmOid(der, hasVersion: false);

        if (algorithm.AsSpan().SequenceEqual(_rsaOid))
            return CreateRsa(rsa => rsa.ImportSubjectPublicKeyInfo(der, out _));

        if (algorithm.AsSpan().SequenceEqual(_ecOid))
            return CreateEc(ec => ec.ImportSubjectPublicKeyInfo(der, out _));

        throw new CryptographicException($"Unsupported key algorithm: {OidToHex(algorithm)}");
    }

    private static AsymmetricAlgorithm ImportPkcs8PrivateKey(byte[] der)
    {
        var algorithm = ReadAlgorithmOid(der, hasVersion: true);

        if (algorithm.AsSpan().SequenceEqual(_rsaOid))
            return CreateRsa(rsa => rsa.ImportPkcs8PrivateKey(der, out _));

        if (algorithm.AsSpan().SequenceEqual(_ecOid))
            return CreateEc(ec => ec.ImportPkcs8PrivateKey(der, out _));

        throw new CryptographicException($"Unsupported key algorithm: {OidToHex(algorithm)}");
    }

    private static RSA CreateRsa(Action<RSA> import)
    {
        var rsa = RSA.Create();

        try
        {
            import(rsa);
            return rsa;
        }
        catch
        {
            rsa.Dispose();
            throw;
        }
    }

    private static ECDiffieHellman CreateEc(Action<ECDiffieHellman> import)
    {
        var ec = ECDiffieHellman.Create();

        try
        {
            import(ec);
            return ec;
        }
        catch
        {
            ec.Dispose();
            throw;
        }
    }

    // SubjectPublicKeyInfo ::= SEQUENCE { algorithm AlgorithmIdentifier, subjectPublicKey BIT STRING }
    // PrivateKeyInfo ::= SEQUENCE { version INTEGER, privateKeyAlgorithm AlgorithmIdentifier, privateKey OCTET STRING }
    // AlgorithmIdentifier ::= SEQUENCE { algorithm OBJECT IDENTIFIER, parameters ANY OPTIONAL }
    private static byte[] ReadAlgorithmOid(byte[] der, bool hasVersion)
    {
        ReadElement(der, 0, SequenceTag, out var contentOffset, out _);

        if (hasVersion)
            contentOffset = SkipElement(der, contentOffset, IntegerTag);

        ReadElement(der, contentOffset, SequenceTag, out var algorithmOffset, out _);
        ReadElement(der, algorithmOffset, ObjectIdentifierTag, out var oidOffset, out var oidLength);

        var oid = new byte[oidLength];
        Buffer.BlockCopy(der, oidOffset, oid, 0, oidLength);
        return oid;
    }

    // Certificate ::= SEQUENCE { tbsCertificate TBSCertificate, signatureAlgorithm AlgorithmIdentifier, signature BIT STRING }
    // TBSCertificate ::= SEQUENCE { version [0] EXPLICIT OPTIONAL, serialNumber, signature, issuer, validity, subject, subjectPublicKeyInfo, ... }
    private static byte[] ExtractSubjectPublicKeyInfo(byte[] certificate)
    {
        ReadElement(certificate, 0, SequenceTag, out var certificateOffset, out _);
        ReadElement(certificate, certificateOffset, SequenceTag, out var position, out _);

        if (position < certificate.Length && certificate[position] == VersionTag)
            position = SkipElement(certificate, position, VersionTag);

        position = SkipElement(certificate, position, IntegerTag); // serialNumber
        position = SkipElement(certificate, position, SequenceTag); // signature
        position = SkipElement(certificate, position, SequenceTag); // issuer
        position = SkipElement(certificate, position, SequenceTag); // validity
        position = SkipElement(certificate, position, SequenceTag); // subject

        var end = SkipElement(certificate, position, SequenceTag);
        var subjectPublicKeyInfo = new byte[end - position];
        Buffer.BlockCopy(certificate, position, subjectPublicKeyInfo, 0, subjectPublicKeyInfo.Length);
        return subjectPublicKeyInfo;
    }

    private static int SkipElement(byte[] data, int offset, byte tag)
    {
        ReadElement(data, offset, tag, out var contentOffset, out var contentLength);
        return contentOffset + contentLength;
    }

    private static void ReadElement(byte[] data, int offset, byte tag, out int contentOffset, out int contentLength)
    {
        if (offset < 0 || offset + 2 > data.Length || data[offset] != tag)
            throw new CryptographicException("The key is not valid DER");

        var firstLengthByte = data[offset + 1];

        if (firstLengthByte < 0x80)
        {
            contentLength = firstLengthByte;
            contentOffset = offset + 2;
        }
        else
        {
            var lengthBytes = firstLengthByte & 0x7F;

            if (lengthBytes == 0 || lengthBytes > 3 || offset + 2 + lengthBytes > data.Length)
                throw new CryptographicException("The key is not valid DER");

            contentLength = 0;

            for (var i = 0; i < lengthBytes; ++i)
                contentLength = (contentLength << 8) | data[offset + 2 + i];

            contentOffset = offset + 2 + lengthBytes;
        }

        if (contentOffset + contentLength > data.Length)
            throw new CryptographicException("The key is not valid DER");
    }

    private static string OidToHex(byte[] oid)
        => BitConverter.ToString(oid).Replace("-", string.Empty);
#endif
}
