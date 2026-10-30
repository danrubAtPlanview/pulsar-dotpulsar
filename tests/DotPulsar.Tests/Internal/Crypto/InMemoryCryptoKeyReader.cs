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

using DotPulsar.Abstractions;

public sealed class InMemoryCryptoKeyReader : ICryptoKeyReader
{
    private readonly Dictionary<string, byte[]> _publicKeys;
    private readonly Dictionary<string, byte[]> _privateKeys;
    private readonly IReadOnlyDictionary<string, string>? _keyMetadata;

    public InMemoryCryptoKeyReader(IReadOnlyDictionary<string, string>? keyMetadata = null)
    {
        _publicKeys = [];
        _privateKeys = [];
        _keyMetadata = keyMetadata;
    }

    public InMemoryCryptoKeyReader WithPublicKey(string keyName, byte[] key)
    {
        _publicKeys[keyName] = key;
        return this;
    }

    public InMemoryCryptoKeyReader WithPrivateKey(string keyName, byte[] key)
    {
        _privateKeys[keyName] = key;
        return this;
    }

    public EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata)
        => _publicKeys.TryGetValue(keyName, out var key) ? new EncryptionKeyInfo(key, _keyMetadata) : null;

    public EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata)
        => _privateKeys.TryGetValue(keyName, out var key) ? new EncryptionKeyInfo(key, _keyMetadata) : null;
}
