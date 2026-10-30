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

namespace DotPulsar.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/CryptoKeyReader.java
/// <summary>
/// An abstraction for reading the keys used for end-to-end encryption.
/// </summary>
/// <remarks>
/// The methods are called when a producer is created, every 4 hours by the producer and when a consumer cannot decrypt a message with a cached key.
/// The implementation should not block.
/// </remarks>
public interface ICryptoKeyReader
{
    /// <summary>
    /// Get the public key (PEM) with the given name. Used by producers.
    /// </summary>
    /// <returns>
    /// The key or null if the key was not found.
    /// </returns>
    EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata);

    /// <summary>
    /// Get the private key (PEM) with the given name. Used by consumers and readers.
    /// </summary>
    /// <returns>
    /// The key or null if the key was not found.
    /// </returns>
    EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata);
}
