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

using System.Collections.Immutable;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/EncryptionKeyInfo.java
/// <summary>
/// An encryption key and its metadata (for example version or timestamp).
/// </summary>
public sealed class EncryptionKeyInfo
{
    /// <summary>
    /// Initializes a new instance using the specified key and optional metadata.
    /// </summary>
    public EncryptionKeyInfo(byte[] key, IReadOnlyDictionary<string, string>? metadata = null)
    {
        Key = key ?? throw new ArgumentNullException(nameof(key));
        Metadata = metadata ?? ImmutableDictionary<string, string>.Empty;
    }

    /// <summary>
    /// The key in PEM format.
    /// </summary>
    public byte[] Key { get; }

    /// <summary>
    /// The key's metadata. The default is an empty dictionary.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; }
}
