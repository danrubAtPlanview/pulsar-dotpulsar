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

// Upstream: apache/pulsar@f3846f4 pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java (AES/GCM/NoPadding, 128 bit tag, no AAD)
public sealed class AesGcmCipher : IDisposable
{
    public const int NonceLength = 12;
    public const int TagLength = 16;

    private readonly object _lock;
#if !NETSTANDARD2_0
    private readonly AesGcm _aesGcm;
#endif
    private bool _isDisposed;

    // An AesGcm instance is not documented as thread-safe, so every use is guarded by a lock.
    // The key schedule is only computed once per instance, so an instance is created per data key and not per message.
    public AesGcmCipher(byte[] key)
    {
        _lock = new();
        _isDisposed = false;
        KeyLength = key.Length;

#if NETSTANDARD2_0
        throw new PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later");
#elif NET8_0_OR_GREATER
        _aesGcm = new AesGcm(key, TagLength);
#else
        _aesGcm = new AesGcm(key);
#endif
    }

    public int KeyLength { get; }

    // Returns false if the instance has been disposed. The caller is then expected to use a newer instance.
    public bool Encrypt(ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext, Span<byte> tag)
    {
#if NETSTANDARD2_0
        throw new PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later");
#else
        lock (_lock)
        {
            if (_isDisposed)
                return false;

            _aesGcm.Encrypt(nonce, plaintext, ciphertext, tag);
            return true;
        }
#endif
    }

    // Returns false if the tag does not match (or the instance has been disposed). Nothing is written to the plaintext in that case.
    public bool Decrypt(ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> tag, Span<byte> plaintext)
    {
#if NETSTANDARD2_0
        throw new PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later");
#else
        lock (_lock)
        {
            if (_isDisposed)
                return false;

            try
            {
                _aesGcm.Decrypt(nonce, ciphertext, tag, plaintext);
                return true;
            }
            catch (CryptographicException)
            {
                return false;
            }
        }
#endif
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
#if !NETSTANDARD2_0
            _aesGcm.Dispose();
#endif
        }
    }
}
