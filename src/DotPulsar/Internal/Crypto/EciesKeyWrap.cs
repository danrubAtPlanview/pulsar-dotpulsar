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

// Upstream: apache/pulsar@f3846f4 pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java (ECIES with IESParameterSpec(null, null, 128))
// Upstream: bcgit/bc-java@d401e82 core/src/main/java/org/bouncycastle/crypto/engines/IESEngine.java (ECDHBasicAgreement, KDF2BytesGenerator(SHA1), HMac(SHA1), no block cipher)
public static class EciesKeyWrap
{
    // The MAC key length of IESParameterSpec(null, null, 128), in bytes
    public const int MacKeyLength = 16;

    // The length of HMAC-SHA1, in bytes
    public const int MacLength = 20;

    private const byte UncompressedPoint = 0x04;
    private const byte HybridPointEven = 0x06;
    private const byte HybridPointOdd = 0x07;
    private const byte CompressedPointEven = 0x02;
    private const byte CompressedPointOdd = 0x03;

    // IESEngine#getLengthTag: the encoding vector P2 is null, so the length tag is 8 zero bytes
    private static readonly byte[] _lengthTag = new byte[8];

    // The raw ECDH shared secret (the x-coordinate) is only available on .NET 8 and later
#if NET8_0_OR_GREATER
    public static bool IsSupported => true;
#else
    public static bool IsSupported => false;
#endif

    // The symmetric half. Returns C || T where C = message XOR K1 and T = HMAC-SHA1(K2, C || L2) with K2 || K1 = KDF2-SHA1(sharedInput, 16 + len(message)).
    public static byte[] IesEncrypt(byte[] sharedInput, byte[] message)
    {
        var key = Kdf2Sha1.Generate(sharedInput, MacKeyLength + message.Length);

        try
        {
            var output = new byte[message.Length + MacLength];

            for (var i = 0; i < message.Length; ++i)
                output[i] = (byte) (message[i] ^ key[MacKeyLength + i]);

            var mac = ComputeMac(key, output, 0, message.Length);
            Buffer.BlockCopy(mac, 0, output, message.Length, MacLength);

            return output;
        }
        finally
        {
            Array.Clear(key, 0, key.Length);
        }
    }

    // The input is C || T at the given offset. The MAC is verified (in constant time) before the message is decrypted.
    public static bool TryIesDecrypt(byte[] sharedInput, byte[] data, int offset, int count, out byte[]? message)
    {
        message = null;

        if (count < MacLength)
            return false;

        var messageLength = count - MacLength;
        var key = Kdf2Sha1.Generate(sharedInput, MacKeyLength + messageLength);

        try
        {
            var expectedMac = ComputeMac(key, data, offset, messageLength);

            if (!FixedTimeEquals(expectedMac, 0, data, offset + messageLength, MacLength))
                return false;

            var decrypted = new byte[messageLength];

            for (var i = 0; i < messageLength; ++i)
                decrypted[i] = (byte) (data[offset + i] ^ key[MacKeyLength + i]);

            message = decrypted;
            return true;
        }
        finally
        {
            Array.Clear(key, 0, key.Length);
        }
    }

    // ECDiffieHellman does not exist on netstandard2.0, so the keys are passed as AsymmetricAlgorithm. The key must be an ECDiffieHellman.
    public static bool IsEcKey(AsymmetricAlgorithm key)
    {
#if NETSTANDARD2_0
        return false;
#else
        return key is ECDiffieHellman;
#endif
    }

    // Returns V || C || T where V is the uncompressed ephemeral public key
    public static byte[] Wrap(AsymmetricAlgorithm recipientKey, byte[] dataKey)
    {
#if NET8_0_OR_GREATER
        var recipientPublicKey = (ECDiffieHellman) recipientKey;
        var recipient = recipientPublicKey.ExportParameters(false);
        var coordinateLength = recipient.Q.X!.Length;

        using var ephemeral = ECDiffieHellman.Create(recipient.Curve);
        var ephemeralPoint = ephemeral.ExportParameters(false).Q;

        var v = new byte[1 + (2 * coordinateLength)];
        v[0] = UncompressedPoint;
        CopyPadded(ephemeralPoint.X!, v, 1, coordinateLength);
        CopyPadded(ephemeralPoint.Y!, v, 1 + coordinateLength, coordinateLength);

        var sharedInput = Concatenate(v, ephemeral.DeriveRawSecretAgreement(recipientPublicKey.PublicKey), coordinateLength);

        try
        {
            var encrypted = IesEncrypt(sharedInput, dataKey);
            var output = new byte[v.Length + encrypted.Length];
            Buffer.BlockCopy(v, 0, output, 0, v.Length);
            Buffer.BlockCopy(encrypted, 0, output, v.Length, encrypted.Length);
            return output;
        }
        finally
        {
            Array.Clear(sharedInput, 0, sharedInput.Length);
        }
#else
        throw new PlatformNotSupportedException("ECIES requires .NET 8 or later");
#endif
    }

    public static byte[] Unwrap(AsymmetricAlgorithm recipientKey, byte[] wrappedKey)
    {
#if NET8_0_OR_GREATER
        var recipientPrivateKey = (ECDiffieHellman) recipientKey;
        var recipient = recipientPrivateKey.ExportParameters(false);
        var coordinateLength = recipient.Q.X!.Length;

        if (wrappedKey.Length == 0)
            throw new CryptographicException("The wrapped key is empty");

        int vLength;

        switch (wrappedKey[0])
        {
            case UncompressedPoint:
            case HybridPointEven:
            case HybridPointOdd:
                vLength = 1 + (2 * coordinateLength);
                break;
            case CompressedPointEven:
            case CompressedPointOdd:
                throw new CryptographicException("Compressed ephemeral public keys are not supported");
            default:
                throw new CryptographicException($"The ephemeral public key has an invalid point encoding type: 0x{wrappedKey[0]:X2}");
        }

        if (wrappedKey.Length < vLength + MacLength)
            throw new CryptographicException("The wrapped key is too short");

        var ephemeralKey = new ECParameters
        {
            Curve = recipient.Curve,
            Q = new ECPoint
            {
                X = Slice(wrappedKey, 1, coordinateLength),
                Y = Slice(wrappedKey, 1 + coordinateLength, coordinateLength)
            }
        };

        using var ephemeral = ImportPublicKey(ephemeralKey);

        var v = Slice(wrappedKey, 0, vLength);
        var sharedInput = Concatenate(v, recipientPrivateKey.DeriveRawSecretAgreement(ephemeral.PublicKey), coordinateLength);

        try
        {
            if (!TryIesDecrypt(sharedInput, wrappedKey, vLength, wrappedKey.Length - vLength, out var dataKey))
                throw new CryptographicException("The MAC of the wrapped key is invalid");

            return dataKey!;
        }
        finally
        {
            Array.Clear(sharedInput, 0, sharedInput.Length);
        }
#else
        throw new PlatformNotSupportedException("ECIES requires .NET 8 or later");
#endif
    }

    private static byte[] ComputeMac(byte[] key, byte[] data, int offset, int count)
    {
        var macKey = new byte[MacKeyLength];

        try
        {
            Buffer.BlockCopy(key, 0, macKey, 0, MacKeyLength);

            using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA1, macKey);
            hmac.AppendData(data, offset, count);
            hmac.AppendData(_lengthTag);
            return hmac.GetHashAndReset();
        }
        finally
        {
            Array.Clear(macKey, 0, macKey.Length);
        }
    }

    private static bool FixedTimeEquals(byte[] left, int leftOffset, byte[] right, int rightOffset, int count)
    {
#if NETSTANDARD2_0
        var difference = 0;

        for (var i = 0; i < count; ++i)
            difference |= left[leftOffset + i] ^ right[rightOffset + i];

        return difference == 0;
#else
        return CryptographicOperations.FixedTimeEquals(new ReadOnlySpan<byte>(left, leftOffset, count), new ReadOnlySpan<byte>(right, rightOffset, count));
#endif
    }

#if NET8_0_OR_GREATER
    // The platform validates the point (is it on the curve?) when the key is imported.
    // Windows throws a PlatformNotSupportedException if the point is not on a named curve. That is an invalid key and not a missing feature.
    private static ECDiffieHellman ImportPublicKey(ECParameters parameters)
    {
        try
        {
            return ECDiffieHellman.Create(parameters);
        }
        catch (PlatformNotSupportedException e)
        {
            throw new CryptographicException("The ephemeral public key is not valid", e);
        }
    }

    private static byte[] Slice(byte[] source, int offset, int count)
    {
        var slice = new byte[count];
        Buffer.BlockCopy(source, offset, slice, 0, count);
        return slice;
    }

    // BigIntegers.asUnsignedByteArray(fieldSize, z): the unsigned big-endian value, left-padded with zeros to the field size
    private static void CopyPadded(byte[] value, byte[] destination, int destinationOffset, int length)
    {
        if (value.Length > length)
            throw new CryptographicException("A coordinate is larger than the field size");

        Buffer.BlockCopy(value, 0, destination, destinationOffset + length - value.Length, value.Length);
    }

    // S = V || Z
    private static byte[] Concatenate(byte[] v, byte[] z, int coordinateLength)
    {
        try
        {
            var output = new byte[v.Length + coordinateLength];
            Buffer.BlockCopy(v, 0, output, 0, v.Length);
            CopyPadded(z, output, v.Length, coordinateLength);
            return output;
        }
        finally
        {
            Array.Clear(z, 0, z.Length);
        }
    }
#endif
}
