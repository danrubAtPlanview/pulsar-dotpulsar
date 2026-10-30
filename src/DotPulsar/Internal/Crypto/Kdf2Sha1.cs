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

using System.Buffers.Binary;
using System.Security.Cryptography;

// Upstream: bcgit/bc-java@d401e82 core/src/main/java/org/bouncycastle/crypto/generators/BaseKDFBytesGenerator.java (KDF2BytesGenerator with SHA-1, counter starts at 1, no IV)
public static class Kdf2Sha1
{
    private const int CounterLength = 4;
    private const uint CounterStart = 1;

    // Ti = SHA1(sharedSecret || I2OSP(i, 4)) for i = 1, 2, ... and the output is the first 'length' bytes of T1 || T2 || ...
    public static byte[] Generate(byte[] sharedSecret, int length)
    {
        var output = new byte[length];
        var counter = new byte[CounterLength];
        var offset = 0;

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);

        for (var i = CounterStart; offset < length; ++i)
        {
            BinaryPrimitives.WriteUInt32BigEndian(counter, i);
            hash.AppendData(sharedSecret);
            hash.AppendData(counter);

            var digest = hash.GetHashAndReset();
            var count = Math.Min(digest.Length, length - offset);
            Buffer.BlockCopy(digest, 0, output, offset, count);
            offset += count;
            Array.Clear(digest, 0, digest.Length);
        }

        return output;
    }
}
