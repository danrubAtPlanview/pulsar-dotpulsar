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

using DotPulsar.Abstractions;
using System.Collections.Immutable;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/DefaultCryptoKeyReader.java
/// <summary>
/// A crypto key reader that loads keys from 'file:' or 'data:application/x-pem-file;base64,' URIs.
/// </summary>
public sealed class DefaultCryptoKeyReader : ICryptoKeyReader
{
    private const string FileScheme = "file:";
    private const string DataScheme = "data:";
    private const string Base64Suffix = ";base64";
    private const string PemMediaType = "application/x-pem-file";

    private readonly string? _defaultPublicKey;
    private readonly string? _defaultPrivateKey;
    private readonly IReadOnlyDictionary<string, string> _publicKeys;
    private readonly IReadOnlyDictionary<string, string> _privateKeys;

    /// <summary>
    /// Initializes a new instance using a default public key URI, a default private key URI and optional per-key-name URIs.
    /// </summary>
    /// <remarks>
    /// The default URIs are used for the key names that are not in the per-key-name dictionaries.
    /// </remarks>
    public DefaultCryptoKeyReader(
        string? defaultPublicKey,
        string? defaultPrivateKey,
        IReadOnlyDictionary<string, string>? publicKeys = null,
        IReadOnlyDictionary<string, string>? privateKeys = null)
    {
        _defaultPublicKey = defaultPublicKey;
        _defaultPrivateKey = defaultPrivateKey;
        _publicKeys = Copy(publicKeys);
        _privateKeys = Copy(privateKeys);
    }

    /// <summary>
    /// Get the public key with the given name. Returns null if no URI is configured for the key name or if the key cannot be loaded.
    /// </summary>
    public EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata)
        => GetKey(_publicKeys, _defaultPublicKey, keyName);

    /// <summary>
    /// Get the private key with the given name. Returns null if no URI is configured for the key name or if the key cannot be loaded.
    /// </summary>
    public EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata)
        => GetKey(_privateKeys, _defaultPrivateKey, keyName);

    private static IReadOnlyDictionary<string, string> Copy(IReadOnlyDictionary<string, string>? keys)
    {
        if (keys is null || keys.Count == 0)
            return ImmutableDictionary<string, string>.Empty;

        var copy = new Dictionary<string, string>(keys.Count);

        foreach (var key in keys)
            copy[key.Key] = key.Value;

        return copy;
    }

    private static EncryptionKeyInfo? GetKey(IReadOnlyDictionary<string, string> keys, string? defaultKey, string keyName)
    {
        var uri = keys.TryGetValue(keyName, out var value) ? value : defaultKey;

        if (uri is null)
            return null;

        try
        {
            return new EncryptionKeyInfo(LoadKey(uri));
        }
        catch
        {
            return null;
        }
    }

    private static byte[] LoadKey(string uri)
    {
        if (uri.StartsWith(DataScheme, StringComparison.OrdinalIgnoreCase))
            return LoadDataKey(uri);

        if (uri.StartsWith(FileScheme, StringComparison.OrdinalIgnoreCase))
            return LoadFileKey(uri);

        throw new ArgumentException("Invalid key format");
    }

    private static byte[] LoadDataKey(string uri)
    {
        var comma = uri.IndexOf(',');
        if (comma < 0)
            throw new ArgumentException("Invalid key format");

        var header = uri.Substring(DataScheme.Length, comma - DataScheme.Length);

        var isBase64 = header.EndsWith(Base64Suffix, StringComparison.OrdinalIgnoreCase);
        var mediaType = isBase64 ? header.Substring(0, header.Length - Base64Suffix.Length) : header;

        if (!isBase64 || !string.Equals(mediaType, PemMediaType, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException($"Unsupported media type or encoding format: {mediaType}");

        return Convert.FromBase64String(uri.Substring(comma + 1));
    }

    private static byte[] LoadFileKey(string uri)
    {
        var remainder = uri.Substring(FileScheme.Length);

        // 'file:///path' is an absolute URI, anything else ('file:./path', 'file:/path') is a path
        var path = remainder.StartsWith("//", StringComparison.Ordinal)
            ? new Uri(uri).LocalPath
            : Path.Combine(Environment.CurrentDirectory, remainder);

        return File.ReadAllBytes(path);
    }
}
