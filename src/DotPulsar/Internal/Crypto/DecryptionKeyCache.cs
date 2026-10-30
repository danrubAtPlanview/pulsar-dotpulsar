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

using Google.Protobuf;
using System.Collections.Concurrent;
using System.Diagnostics;

// Upstream: apache/pulsar@f3846f4 pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java (decryptionKeyCache, lastDecryptionKeyCache)
public sealed class DecryptionKeyCache : IDisposable
{
    public static readonly TimeSpan DefaultExpireAfterAccess = TimeSpan.FromHours(4);
    public const long DefaultMaximumWeight = 10 * 1024 * 1024;

    private readonly Cache<ByteString> _keys;
    private readonly Cache<string> _lastKeys;

    // The 'clock' (monotonic) is a test seam
    public DecryptionKeyCache(TimeSpan expireAfterAccess, long maximumWeight, Func<TimeSpan>? clock = null)
    {
        clock ??= CreateClock();
        _keys = new Cache<ByteString>(expireAfterAccess, maximumWeight, clock);
        _lastKeys = new Cache<string>(expireAfterAccess, maximumWeight, clock);
    }

    // The data keys by wrapped data key
    public bool TryGet(ByteString wrappedKey, out AesGcmCipher cipher)
        => _keys.TryGet(wrappedKey, out cipher);

    public void Put(ByteString wrappedKey, AesGcmCipher cipher)
        => _keys.Put(wrappedKey, cipher, wrappedKey.Length + cipher.KeyLength);

    // The last used data key by producer name
    public bool TryGetLast(string producerName, out AesGcmCipher cipher)
        => _lastKeys.TryGet(producerName, out cipher);

    public void PutLast(string producerName, AesGcmCipher cipher)
        => _lastKeys.Put(producerName, cipher, producerName.Length + cipher.KeyLength);

    // Only removes the entry if it is still the specified cipher
    public void RemoveLast(string producerName, AesGcmCipher cipher)
        => _lastKeys.Remove(producerName, cipher);

    public void Dispose()
    {
        _keys.Dispose();
        _lastKeys.Dispose();
    }

    private static Func<TimeSpan> CreateClock()
    {
        var timestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;
        return () => TimeSpan.FromTicks((long) (Stopwatch.GetTimestamp() * timestampToTicks));
    }

    private sealed class Entry
    {
        public Entry(AesGcmCipher cipher, long weight, long lastAccess)
        {
            Cipher = cipher;
            Weight = weight;
            LastAccess = lastAccess;
        }

        public AesGcmCipher Cipher { get; }
        public long Weight { get; }
        public long LastAccess;
    }

    // Expire after access and a maximum weight. The oldest accessed entries are evicted when the maximum weight is exceeded.
    // Reads are lock-free. Writes, removals and sweeps are serialized.
    private sealed class Cache<TKey> : IDisposable where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, Entry> _entries;
        private readonly object _lock;
        private readonly long _expireAfterAccess;
        private readonly long _maximumWeight;
        private readonly Func<TimeSpan> _clock;

        private long _weight;
        private long _nextSweep;

        public Cache(TimeSpan expireAfterAccess, long maximumWeight, Func<TimeSpan> clock)
        {
            _entries = new ConcurrentDictionary<TKey, Entry>();
            _lock = new();
            _expireAfterAccess = expireAfterAccess.Ticks;
            _maximumWeight = maximumWeight;
            _clock = clock;
            _weight = 0;
            _nextSweep = _clock().Ticks + (_expireAfterAccess / 10);
        }

        public bool TryGet(TKey key, out AesGcmCipher cipher)
        {
            if (_entries.TryGetValue(key, out var entry))
            {
                var now = _clock().Ticks;

                if (now - Volatile.Read(ref entry.LastAccess) <= _expireAfterAccess)
                {
                    Volatile.Write(ref entry.LastAccess, now);
                    cipher = entry.Cipher;
                    return true;
                }

                Remove(key, entry.Cipher);
            }

            cipher = null!;
            return false;
        }

        public void Put(TKey key, AesGcmCipher cipher, long weight)
        {
            lock (_lock)
            {
                var now = _clock().Ticks;

                if (_entries.TryGetValue(key, out var existing))
                    _weight -= existing.Weight;

                _entries[key] = new Entry(cipher, weight, now);
                _weight += weight;

                if (now >= _nextSweep)
                    Sweep(now);

                if (_weight > _maximumWeight)
                    Evict(key);
            }
        }

        public void Remove(TKey key, AesGcmCipher cipher)
        {
            lock (_lock)
            {
                if (_entries.TryGetValue(key, out var entry) && ReferenceEquals(entry.Cipher, cipher) && _entries.TryRemove(key, out _))
                    _weight -= entry.Weight;
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                foreach (var entry in _entries.Values)
                    entry.Cipher.Dispose();

                _entries.Clear();
                _weight = 0;
            }
        }

        private void Sweep(long now)
        {
            foreach (var pair in _entries)
            {
                if (now - Volatile.Read(ref pair.Value.LastAccess) > _expireAfterAccess && _entries.TryRemove(pair.Key, out var removed))
                    _weight -= removed.Weight;
            }

            _nextSweep = now + (_expireAfterAccess / 10);
        }

        private void Evict(TKey added)
        {
            var oldestFirst = _entries.ToArray();
            Array.Sort(oldestFirst, (x, y) => Volatile.Read(ref x.Value.LastAccess).CompareTo(Volatile.Read(ref y.Value.LastAccess)));

            foreach (var pair in oldestFirst)
            {
                if (_weight <= _maximumWeight)
                    break;

                if (EqualityComparer<TKey>.Default.Equals(pair.Key, added))
                    continue;

                if (_entries.TryRemove(pair.Key, out var removed))
                    _weight -= removed.Weight;
            }
        }
    }
}
