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

using Google.Protobuf.Collections;
using System.Buffers;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageIdImpl.java
public static class AckSetWriter
{
    private const int MaxStackWords = 32;

    public static int WordCount(int batchSize)
        => (batchSize + 63) >> 6;

    // The bits [0, batchSize) are set, then the bit at batchIndex is cleared
    public static void WriteIndividual(RepeatedField<long> target, int batchSize, int batchIndex)
        => Write(target, batchSize, batchIndex, false);

    // The bits [0, batchSize) are set, then the bits [0, batchIndex] are cleared
    public static void WriteCumulative(RepeatedField<long> target, int batchSize, int batchIndex)
        => Write(target, batchSize, batchIndex, true);

    // Same layout as java.util.BitSet.toLongArray(), so trailing words that are zero are omitted
    public static void WriteWords(RepeatedField<long> target, ReadOnlySpan<ulong> words)
    {
        var length = words.Length;

        while (length > 0 && words[length - 1] == 0)
            --length;

        target.Clear();

        for (var i = 0; i < length; ++i)
        {
            target.Add(unchecked((long) words[i]));
        }
    }

    // Sets the bits [0, bitCount) and clears the rest
    public static void SetBits(Span<ulong> words, int bitCount)
    {
        var fullWords = bitCount >> 6;

        for (var i = 0; i < words.Length; ++i)
        {
            words[i] = i < fullWords ? ulong.MaxValue : 0;
        }

        var remainder = bitCount & 63;
        if (remainder != 0 && fullWords < words.Length)
            words[fullWords] = (1UL << remainder) - 1;
    }

    // Clears the bits [0, lastIndex]
    public static void ClearBitsUpTo(Span<ulong> words, int lastIndex)
    {
        if (lastIndex < 0)
            return;

        var bitCount = lastIndex + 1;
        var fullWords = Math.Min(bitCount >> 6, words.Length);

        for (var i = 0; i < fullWords; ++i)
        {
            words[i] = 0;
        }

        var remainder = bitCount & 63;
        if (remainder != 0 && fullWords < words.Length)
            words[fullWords] &= ~((1UL << remainder) - 1);
    }

    private static void Write(RepeatedField<long> target, int batchSize, int batchIndex, bool cumulative)
    {
        if (batchSize <= 0)
        {
            target.Clear();
            return;
        }

        var wordCount = WordCount(batchSize);
        ulong[]? rented = null;
        Span<ulong> words = wordCount <= MaxStackWords ? stackalloc ulong[wordCount] : (rented = ArrayPool<ulong>.Shared.Rent(wordCount)).AsSpan(0, wordCount);

        try
        {
            SetBits(words, batchSize);

            if (cumulative)
                ClearBitsUpTo(words, batchIndex);
            else if (batchIndex >= 0 && batchIndex < batchSize)
                words[batchIndex >> 6] &= ~(1UL << (batchIndex & 63));

            WriteWords(target, words);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<ulong>.Shared.Return(rented);
        }
    }
}
