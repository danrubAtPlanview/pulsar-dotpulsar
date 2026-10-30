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

namespace DotPulsar;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/common/api/EncryptionContext.java
/// <summary>
/// Encryption information of a received message.
/// </summary>
public sealed class EncryptionContext
{
    internal EncryptionContext(
        IReadOnlyDictionary<string, EncryptionKey> keys,
        byte[] param,
        string? algorithm,
        CompressionType compressionType,
        uint uncompressedMessageSize,
        int? batchSize,
        bool isEncrypted)
    {
        Keys = keys;
        Param = param;
        Algorithm = algorithm;
        CompressionType = compressionType;
        UncompressedMessageSize = uncompressedMessageSize;
        BatchSize = batchSize;
        IsEncrypted = isEncrypted;
    }

    /// <summary>
    /// The wrapped data keys by key name.
    /// </summary>
    public IReadOnlyDictionary<string, EncryptionKey> Keys { get; }

    /// <summary>
    /// The encryption parameter (the AES-GCM IV).
    /// </summary>
    public byte[] Param { get; }

    /// <summary>
    /// The encryption algorithm, if set by the producer.
    /// </summary>
    public string? Algorithm { get; }

    /// <summary>
    /// The compression type of the encrypted payload.
    /// </summary>
    public CompressionType CompressionType { get; }

    /// <summary>
    /// The uncompressed size of the payload.
    /// </summary>
    public uint UncompressedMessageSize { get; }

    /// <summary>
    /// The number of messages in the batch, if the message is a batch.
    /// </summary>
    public int? BatchSize { get; }

    /// <summary>
    /// Whether the payload is still encrypted (true) or was decrypted (false).
    /// </summary>
    public bool IsEncrypted { get; }
}
