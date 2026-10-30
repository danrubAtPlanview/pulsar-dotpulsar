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

using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;

// Upstream: apache/pulsar@f3846f4 pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java (encryptionKey and encryptedDataKeyMap)
public sealed class DataKeySnapshot : IDisposable
{
    // The tag and the length of the 'encryption_param' (the IV) field in the message metadata
    private const int EncryptionParamOverhead = 1 + 1;

    // The tag of the 'encryption_keys' field in the message metadata
    private const int EncryptionKeysTagLength = 1;

    // The key entries are shared by all the messages that are sent with this snapshot. Protobuf does not clone the messages that are
    // added to a repeated field and serializing a message does not mutate it, so they are never modified after the snapshot is created.
    public DataKeySnapshot(AesGcmCipher cipher, EncryptionKeys[] keys)
    {
        Cipher = cipher;
        Keys = keys;

        var overhead = AesGcmCipher.TagLength + EncryptionParamOverhead + AesGcmCipher.NonceLength;

        for (var i = 0; i < keys.Length; ++i)
        {
            var size = keys[i].CalculateSize();
            overhead += EncryptionKeysTagLength + CodedOutputStream.ComputeLengthSize(size) + size;
        }

        MetadataOverhead = overhead;
    }

    // The data key is only held by the cipher
    public AesGcmCipher Cipher { get; }

    // The wrapped data key for every key name, sorted by key name (ordinal)
    public IReadOnlyList<EncryptionKeys> Keys { get; }

    // The number of bytes that encrypting a message adds to the payload and to the metadata:
    // the GCM tag, the 'encryption_param' field (the IV) and the 'encryption_keys' fields
    public int MetadataOverhead { get; }

    public void Dispose()
        => Cipher.Dispose();
}
