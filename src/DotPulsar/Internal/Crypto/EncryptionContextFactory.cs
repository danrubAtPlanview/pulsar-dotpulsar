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
using Google.Protobuf.Collections;
using System.Collections.Immutable;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java (createEncryptionContext)
public static class EncryptionContextFactory
{
    // A duplicate key name keeps the last entry
    public static EncryptionContext Create(MessageMetadata metadata, bool isEncrypted)
    {
        var keys = new Dictionary<string, EncryptionKey>(metadata.EncryptionKeys.Count);

        for (var i = 0; i < metadata.EncryptionKeys.Count; ++i)
        {
            var encryptionKeys = metadata.EncryptionKeys[i];
            keys[encryptionKeys.Key] = new EncryptionKey(encryptionKeys.Value.ToByteArray(), ToDictionary(encryptionKeys.Metadata));
        }

        return new EncryptionContext(
            keys,
            metadata.EncryptionParam.ToByteArray(),
            metadata.HasEncryptionAlgo ? metadata.EncryptionAlgo : null,
            (DotPulsar.CompressionType) metadata.Compression,
            metadata.UncompressedSize,
            metadata.HasNumMessagesInBatch ? metadata.NumMessagesInBatch : null,
            isEncrypted);
    }

    private static IReadOnlyDictionary<string, string> ToDictionary(RepeatedField<KeyValue> keyValues)
    {
        if (keyValues.Count == 0)
            return ImmutableDictionary<string, string>.Empty;

        var dictionary = new Dictionary<string, string>(keyValues.Count);

        for (var i = 0; i < keyValues.Count; ++i)
            dictionary[keyValues[i].Key] = keyValues[i].Value;

        return dictionary;
    }
}
