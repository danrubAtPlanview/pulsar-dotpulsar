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

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;
using System.Collections.Immutable;
using System.Security.Cryptography;

// Upstream: apache/pulsar@f3846f4 pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java
public sealed class MessageCrypto : IDisposable
{
    public const int IvLength = AesGcmCipher.NonceLength;
    public const int TagLength = AesGcmCipher.TagLength;
    public const int DataKeyLength = 32;

    // The data key is rotated (and wrapped again) at this interval by a producer
    public static readonly TimeSpan DefaultKeyRefreshInterval = TimeSpan.FromHours(4);

    private const string NotSetProducerName = "__not_set__";

    private static readonly IReadOnlyDictionary<string, string> _noMetadata;
#if NETSTANDARD2_0
    private static readonly RandomNumberGenerator _random;
#endif

    private readonly bool _keyGenNeeded;
    private readonly TimeSpan _keyRefreshInterval;
    private readonly object _lock;
    private readonly DecryptionKeyCache _decryptionKeyCache;

    private DataKeySnapshot? _snapshot;
    private string[] _keyNames;
    private ICryptoKeyReader? _keyReader;
    private Timer? _timer;
    private int _isDisposed;

    static MessageCrypto()
    {
        _noMetadata = ImmutableDictionary<string, string>.Empty;
#if NETSTANDARD2_0
        _random = RandomNumberGenerator.Create();
#endif
    }

    // A producer uses 'keyGenNeeded: true'. It rotates the data key every 'keyRefreshInterval' (the default is 4 hours) after the keys have been added.
    // A consumer or a reader uses 'keyGenNeeded: false'. The 'keyRefreshInterval' and the 'clock' (monotonic, used to expire cached keys) are test seams.
    public MessageCrypto(bool keyGenNeeded, TimeSpan? keyRefreshInterval = null, Func<TimeSpan>? clock = null)
    {
        _keyGenNeeded = keyGenNeeded;
        _keyRefreshInterval = keyRefreshInterval ?? DefaultKeyRefreshInterval;
        _lock = new();
        _decryptionKeyCache = new DecryptionKeyCache(DecryptionKeyCache.DefaultExpireAfterAccess, DecryptionKeyCache.DefaultMaximumWeight, clock);
        _snapshot = null;
        _keyNames = [];
        _keyReader = null;
        _timer = null;
        _isDisposed = 0;
    }

    // The number of bytes that encrypting a message adds (the GCM tag and the metadata fields) as of the current data key.
    // It is zero until the public keys have been added.
    public int GetMetadataOverhead()
        => Volatile.Read(ref _snapshot)?.MetadataOverhead ?? 0;

    // Producer. Rotates the data key and wraps it for every key name. The new data key replaces the previous one when all the keys have been wrapped.
    // If a key cannot be read, loaded or used then a CryptoException is thrown and the previous data key (if any) is kept.
    public void AddPublicKeyCipher(IEnumerable<string> keyNames, ICryptoKeyReader keyReader)
    {
        ThrowIfDisposed();

        if (keyNames is null)
            throw new ArgumentNullException(nameof(keyNames));

        if (keyReader is null)
            throw new CryptoException("Keyname or KeyReader is null");

        var sortedKeyNames = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var keyName in keyNames)
        {
            if (keyName is null)
                throw new CryptoException("Keyname or KeyReader is null");

            sortedKeyNames.Add(keyName);
        }

        if (sortedKeyNames.Count == 0)
            throw new ArgumentException("At least one key name is required", nameof(keyNames));

        var names = new string[sortedKeyNames.Count];
        sortedKeyNames.CopyTo(names);

        lock (_lock)
        {
            ThrowIfDisposed();

            Publish(CreateSnapshot(names, keyReader));

            _keyNames = names;
            _keyReader = keyReader;

            if (_keyGenNeeded && _timer is null)
                _timer = new Timer(Refresh, null, _keyRefreshInterval, _keyRefreshInterval);
        }
    }

    // Producer. Encrypts the payload with AES-GCM and sets 'encryption_keys' and 'encryption_param' in the metadata.
    // The returned payload is the ciphertext followed by the 16 byte tag. The metadata is not modified if a CryptoException is thrown.
    public ReadOnlySequence<byte> Encrypt(MessageMetadata metadata, ReadOnlySequence<byte> payload)
    {
        ThrowIfDisposed();

        var length = checked((int) payload.Length);
        var output = new byte[length + TagLength];

        Span<byte> iv = stackalloc byte[IvLength];
        FillRandom(iv);

        byte[]? rented = null;
        DataKeySnapshot snapshot;

        try
        {
            ReadOnlySpan<byte> plaintext;

            if (payload.IsSingleSegment)
                plaintext = payload.First.Span;
            else
            {
                rented = ArrayPool<byte>.Shared.Rent(length);
                payload.CopyTo(rented);
                plaintext = new ReadOnlySpan<byte>(rented, 0, length);
            }

            while (true)
            {
                snapshot = Volatile.Read(ref _snapshot)!;

                if (snapshot is null)
                {
                    ThrowIfDisposed();
                    throw new CryptoException("No encryption keys have been added");
                }

                // The snapshot can only be disposed if it has been replaced (or if this instance has been disposed)
                if (snapshot.Cipher.Encrypt(iv, plaintext, output.AsSpan(0, length), output.AsSpan(length, TagLength)))
                    break;

                ThrowIfDisposed();
            }
        }
        catch (CryptographicException e)
        {
            throw new CryptoException($"Failed to encrypt the message. {e.Message}", e);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented, true);
        }

        metadata.EncryptionKeys.Clear();

        var keys = snapshot.Keys;
        for (var i = 0; i < keys.Count; ++i)
            metadata.EncryptionKeys.Add(keys[i]);

        metadata.EncryptionParam = ByteString.CopyFrom(iv);

        return new ReadOnlySequence<byte>(output);
    }

    // Consumer and reader. Decrypts the payload using the keys of the metadata. Returns false if the payload cannot be decrypted.
    // The last data key used for the producer is tried first, then the cached data keys and then the keys that the key reader returns.
    public bool TryDecrypt(MessageMetadata metadata, ReadOnlySequence<byte> payload, ICryptoKeyReader keyReader, out ReadOnlySequence<byte> decrypted)
    {
        ThrowIfDisposed();

        decrypted = default;

        var iv = metadata.EncryptionParam.Span;
        if (iv.Length != IvLength || payload.Length < TagLength)
            return false;

        var length = checked((int) payload.Length);
        var plaintextLength = length - TagLength;
        var output = new byte[plaintextLength];
        var producerName = metadata.HasProducerName ? metadata.ProducerName : NotSetProducerName;

        byte[]? rented = null;

        try
        {
            ReadOnlySpan<byte> data;

            if (payload.IsSingleSegment)
                data = payload.First.Span;
            else
            {
                rented = ArrayPool<byte>.Shared.Rent(length);
                payload.CopyTo(rented);
                data = new ReadOnlySpan<byte>(rented, 0, length);
            }

            if (TryDecryptData(metadata, producerName, keyReader, iv, data, output))
            {
                decrypted = new ReadOnlySequence<byte>(output);
                return true;
            }
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented, true);
        }

        ThrowIfDisposed();
        return false;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        lock (_lock)
        {
            _timer?.Dispose();
            _timer = null;
            _keyReader = null;

            Interlocked.Exchange(ref _snapshot, null)?.Dispose();
        }

        _decryptionKeyCache.Dispose();
    }

    private bool TryDecryptData(MessageMetadata metadata, string producerName, ICryptoKeyReader keyReader, ReadOnlySpan<byte> iv, ReadOnlySpan<byte> data, byte[] output)
    {
        var ciphertext = data.Slice(0, output.Length);
        var tag = data.Slice(output.Length, TagLength);

        // Pass 1: Try the last data key that was used for this producer (the fast path, no lookup of wrapped keys)
        if (_decryptionKeyCache.TryGetLast(producerName, out var last))
        {
            if (last.Decrypt(iv, ciphertext, tag, output))
                return true;

            _decryptionKeyCache.RemoveLast(producerName, last);
        }

        var keys = metadata.EncryptionKeys;

        // Pass 2: Try the cached data keys (no calls to the key reader)
        for (var i = 0; i < keys.Count; ++i)
        {
            if (_decryptionKeyCache.TryGet(keys[i].Value, out var cached) && cached.Decrypt(iv, ciphertext, tag, output))
            {
                _decryptionKeyCache.PutLast(producerName, cached);
                return true;
            }
        }

        // Pass 3: Unwrap the data keys using the key reader (the slow path)
        for (var i = 0; i < keys.Count; ++i)
        {
            var unwrapped = TryUnwrapDataKey(keys[i], keyReader);
            if (unwrapped is null)
                continue;

            _decryptionKeyCache.Put(keys[i].Value, unwrapped);

            if (unwrapped.Decrypt(iv, ciphertext, tag, output))
            {
                _decryptionKeyCache.PutLast(producerName, unwrapped);
                return true;
            }
        }

        return false;
    }

    // Any failure (the key reader throws, no key, an invalid key, a wrong key, an unsupported key type) means that the key cannot be used
    private static AesGcmCipher? TryUnwrapDataKey(EncryptionKeys encryptionKey, ICryptoKeyReader keyReader)
    {
        byte[]? dataKey = null;

        try
        {
            var keyInfo = keyReader.GetPrivateKey(encryptionKey.Key, ToDictionary(encryptionKey.Metadata));
            if (keyInfo is null || keyInfo.Key.Length == 0)
                return null;

            using var privateKey = PemKeyLoader.LoadPrivateKey(keyInfo.Key);

            if (privateKey is RSA rsa)
                dataKey = RsaKeyWrap.Unwrap(rsa, encryptionKey.Value.ToByteArray());
            else if (EciesKeyWrap.IsEcKey(privateKey) && EciesKeyWrap.IsSupported)
                dataKey = EciesKeyWrap.Unwrap(privateKey, encryptionKey.Value.ToByteArray());
            else
                return null;

            return new AesGcmCipher(dataKey);
        }
        catch
        {
            return null;
        }
        finally
        {
            if (dataKey is not null)
                Array.Clear(dataKey, 0, dataKey.Length);
        }
    }

    private void Refresh(object? state)
    {
        try
        {
            lock (_lock)
            {
                if (_isDisposed != 0 || _keyReader is null)
                    return;

                Publish(CreateSnapshot(_keyNames, _keyReader));
            }
        }
        catch
        {
            // Ignore: the previous data key is kept and the next refresh will try again
        }
    }

    private void Publish(DataKeySnapshot snapshot)
        => Interlocked.Exchange(ref _snapshot, snapshot)?.Dispose();

    // The new data key is only used when every key has been wrapped. The data key is only kept by the AES-GCM instance.
    private static DataKeySnapshot CreateSnapshot(string[] keyNames, ICryptoKeyReader keyReader)
    {
        var dataKey = new byte[DataKeyLength];

        try
        {
            FillRandom(dataKey);

            var cipher = new AesGcmCipher(dataKey);

            try
            {
                var keys = new EncryptionKeys[keyNames.Length];

                for (var i = 0; i < keyNames.Length; ++i)
                    keys[i] = Wrap(keyNames[i], dataKey, keyReader);

                return new DataKeySnapshot(cipher, keys);
            }
            catch
            {
                cipher.Dispose();
                throw;
            }
        }
        finally
        {
            Array.Clear(dataKey, 0, dataKey.Length);
        }
    }

    private static EncryptionKeys Wrap(string keyName, byte[] dataKey, ICryptoKeyReader keyReader)
    {
        EncryptionKeyInfo? keyInfo;

        try
        {
            keyInfo = keyReader.GetPublicKey(keyName, _noMetadata);
        }
        catch (Exception e)
        {
            throw new CryptoException($"Failed to load public key {keyName}. {e.Message}", e);
        }

        if (keyInfo is null || keyInfo.Key.Length == 0)
            throw new CryptoException($"Failed to load public key {keyName}. The key was not found");

        AsymmetricAlgorithm publicKey;

        try
        {
            publicKey = PemKeyLoader.LoadPublicKey(keyInfo.Key);
        }
        catch (PlatformNotSupportedException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new CryptoException($"Failed to load public key {keyName}. {e.Message}", e);
        }

        byte[] wrappedKey;

        using (publicKey)
        {
            try
            {
                if (publicKey is RSA rsa)
                    wrappedKey = RsaKeyWrap.Wrap(rsa, dataKey);
                else if (EciesKeyWrap.IsEcKey(publicKey) && EciesKeyWrap.IsSupported)
                    wrappedKey = EciesKeyWrap.Wrap(publicKey, dataKey);
                else if (EciesKeyWrap.IsEcKey(publicKey))
                    throw new CryptoException($"Unsupported key type EC for key {keyName} on this platform");
                else
                    throw new CryptoException($"Unsupported key type {publicKey.GetType().Name} for key {keyName}");
            }
            catch (CryptographicException e)
            {
                throw new CryptoException($"Failed to encrypt the data key with key {keyName}. {e.Message}", e);
            }
        }

        var encryptionKey = new EncryptionKeys { Key = keyName, Value = ByteString.CopyFrom(wrappedKey) };

        foreach (var metadata in keyInfo.Metadata)
            encryptionKey.Metadata.Add(new KeyValue { Key = metadata.Key, Value = metadata.Value ?? string.Empty });

        return encryptionKey;
    }

    private static Dictionary<string, string> ToDictionary(Google.Protobuf.Collections.RepeatedField<KeyValue> keyValues)
    {
        var dictionary = new Dictionary<string, string>(keyValues.Count);

        for (var i = 0; i < keyValues.Count; ++i)
            dictionary[keyValues[i].Key] = keyValues[i].Value;

        return dictionary;
    }

    private static void FillRandom(Span<byte> buffer)
    {
#if NETSTANDARD2_0
        var bytes = new byte[buffer.Length];
        _random.GetBytes(bytes);
        bytes.CopyTo(buffer);
#else
        RandomNumberGenerator.Fill(buffer);
#endif
    }

    private void ThrowIfDisposed()
    {
        if (_isDisposed != 0)
            throw new ObjectDisposedException(nameof(MessageCrypto));
    }
}
