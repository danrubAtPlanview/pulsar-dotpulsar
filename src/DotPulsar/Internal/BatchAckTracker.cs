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

namespace DotPulsar.Internal;

using DotPulsar.Internal.PulsarApi;
using Google.Protobuf.Collections;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/MessageIdAdvUtils.java, apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageIdImpl.java, apache/pulsar-client-cpp@f71d7e9 lib/BatchMessageAcker.h
public sealed class BatchAckTracker
{
    private readonly ulong[] _words; // A set bit means that the message is not yet acknowledged
    private int _remaining;

    // Not thread safe. The BatchHandler serializes all access.
    public BatchAckTracker(MessageIdData entryId, int batchSize, RepeatedField<long>? brokerAckSet)
    {
        LedgerId = entryId.LedgerId;
        EntryId = entryId.EntryId;
        Partition = entryId.Partition;
        BatchSize = batchSize;
        _words = new ulong[AckSetWriter.WordCount(batchSize)];
        AckSetWriter.SetBits(_words, batchSize);
        _remaining = batchSize;

        if (brokerAckSet is not null)
            ApplyBrokerAckSet(brokerAckSet);
    }

    public ulong LedgerId { get; }
    public ulong EntryId { get; }
    public int Partition { get; }
    public int BatchSize { get; }

    public bool IsEmpty => _remaining == 0;

    public EntryKey Key => new(LedgerId, EntryId, Partition);

    // Clears the bit of the message. Returns whether all messages are acknowledged. An index outside the batch is ignored.
    public bool AckIndividual(int batchIndex)
    {
        if (batchIndex < 0 || batchIndex >= BatchSize)
            return IsEmpty;

        var index = batchIndex >> 6;
        var bit = 1UL << (batchIndex & 63);

        if ((_words[index] & bit) != 0)
        {
            _words[index] &= ~bit;
            --_remaining;
        }

        return IsEmpty;
    }

    // Clears the bits of the messages up to and including the batch index. Returns whether all messages are acknowledged.
    public bool AckCumulative(int batchIndex)
    {
        if (batchIndex < 0)
            return IsEmpty;

        var bitCount = Math.Min(batchIndex + 1, BatchSize);
        var fullWords = bitCount >> 6;

        for (var i = 0; i < fullWords; ++i)
        {
            _remaining -= PopCount(_words[i]);
            _words[i] = 0;
        }

        var remainder = bitCount & 63;

        if (remainder != 0)
        {
            var mask = (1UL << remainder) - 1;
            _remaining -= PopCount(_words[fullWords] & mask);
            _words[fullWords] &= ~mask;
        }

        return IsEmpty;
    }

    public bool IsAcked(int batchIndex)
        => batchIndex < 0 || batchIndex >= BatchSize || (_words[batchIndex >> 6] & (1UL << (batchIndex & 63))) == 0;

    public void CopyAckSetTo(RepeatedField<long> target)
        => AckSetWriter.WriteWords(target, _words);

    // An empty ack set means that nothing is acknowledged
    private void ApplyBrokerAckSet(RepeatedField<long> brokerAckSet)
    {
        if (brokerAckSet.Count == 0)
            return;

        for (var i = 0; i < _words.Length; ++i)
        {
            _words[i] &= i < brokerAckSet.Count ? unchecked((ulong) brokerAckSet[i]) : 0;
        }

        _remaining = CountBits();
    }

    private int CountBits()
    {
        var count = 0;

        for (var i = 0; i < _words.Length; ++i)
        {
            count += PopCount(_words[i]);
        }

        return count;
    }

    // BitOperations.PopCount is not available on all target frameworks
    private static int PopCount(ulong value)
    {
        value -= (value >> 1) & 0x5555555555555555UL;
        value = (value & 0x3333333333333333UL) + ((value >> 2) & 0x3333333333333333UL);
        value = (value + (value >> 4)) & 0x0F0F0F0F0F0F0F0FUL;
        return (int) ((value * 0x0101010101010101UL) >> 56);
    }
}
