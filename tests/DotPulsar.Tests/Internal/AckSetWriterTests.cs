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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Internal;
using Google.Protobuf.Collections;
using System.Collections;

[Trait("Category", "Unit")]
public sealed class AckSetWriterTests
{
    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(63, 1)]
    [InlineData(64, 1)]
    [InlineData(65, 2)]
    [InlineData(128, 2)]
    [InlineData(129, 3)]
    public void WordCount_GivenBatchSize_ShouldReturnNumberOfWords(int batchSize, int expected)
    {
        //Act
        var actual = AckSetWriter.WordCount(batchSize);

        //Assert
        actual.ShouldBe(expected);
    }

    [Fact]
    public void WriteIndividual_GivenBatchSize10Index3_ShouldWriteSingleWord0x3F7()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteIndividual(target, 10, 3);

        //Assert
        target.ShouldBeAckSet(0x3F7);
    }

    [Fact]
    public void WriteIndividual_GivenBatchSize64Index63_ShouldWriteOneWord()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteIndividual(target, 64, 63);

        //Assert
        target.ShouldBeAckSet(0x7FFFFFFFFFFFFFFF);
    }

    [Fact]
    public void WriteIndividual_GivenBatchSize65Index64_ShouldTrimTrailingZeroWord()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteIndividual(target, 65, 64);

        //Assert
        target.ShouldBeAckSet(-1L);
    }

    [Fact]
    public void WriteIndividual_GivenBatchSize130Index100_ShouldWriteThreeWords()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteIndividual(target, 130, 100);

        //Assert
        target.ShouldBeAckSet(-1L, unchecked((long) ~(1UL << 36)), 0b11L);
    }

    [Fact]
    public void WriteIndividual_GivenBatchSizeOfOneAndIndex0_ShouldWriteEmptyList()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteIndividual(target, 1, 0);

        //Assert
        target.Count.ShouldBe(0);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    [InlineData(1000)]
    public void WriteIndividual_GivenIndexOutsideBatch_ShouldWriteAllBits(int batchIndex)
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteIndividual(target, 10, batchIndex);

        //Assert
        target.ShouldBeAckSet(0x3FF);
    }

    [Fact]
    public void WriteIndividual_GivenBatchSizeZero_ShouldWriteEmptyList()
    {
        //Arrange
        var target = new RepeatedField<long> { 1, 2, 3 };

        //Act
        AckSetWriter.WriteIndividual(target, 0, 0);

        //Assert
        target.Count.ShouldBe(0);
    }

    [Fact]
    public void WriteCumulative_GivenBatchSize10Index4_ShouldClearBits0To4()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteCumulative(target, 10, 4);

        //Assert
        target.ShouldBeAckSet(0b11_1110_0000);
    }

    [Fact]
    public void WriteCumulative_GivenLastIndex_ShouldWriteEmptyList()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteCumulative(target, 10, 9);

        //Assert
        target.Count.ShouldBe(0);
    }

    [Fact]
    public void WriteCumulative_GivenBatchSize130Index70_ShouldClearTheFirst71Bits()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteCumulative(target, 130, 70);

        //Assert
        target.ShouldBeAckSet(0L, unchecked((long) ~((1UL << 7) - 1)), 0b11L);
    }

    [Fact]
    public void WriteCumulative_GivenIndex63_ShouldClearTheFirstWord()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteCumulative(target, 70, 63);

        //Assert
        target.ShouldBeAckSet(0L, 0b111111L);
    }

    [Fact]
    public void WriteCumulative_GivenIndexBeyondBatch_ShouldWriteEmptyList()
    {
        //Arrange
        var target = new RepeatedField<long>();

        //Act
        AckSetWriter.WriteCumulative(target, 70, 500);

        //Assert
        target.Count.ShouldBe(0);
    }

    [Fact]
    public void WriteIndividual_GivenTargetWithContent_ShouldReplaceTheContent()
    {
        //Arrange
        var target = new RepeatedField<long> { 1, 2, 3, 4 };

        //Act
        AckSetWriter.WriteIndividual(target, 10, 3);

        //Assert
        target.ShouldBeAckSet(0x3F7);
    }

    [Fact]
    public void WriteWords_GivenTrailingZeroWords_ShouldOmitThem()
    {
        //Arrange
        var target = new RepeatedField<long>();
        ulong[] words = [5, 0, 7, 0, 0];

        //Act
        AckSetWriter.WriteWords(target, words);

        //Assert
        target.ShouldBeAckSet(5, 0, 7);
    }

    [Fact]
    public void WriteWords_GivenOnlyZeroWords_ShouldWriteEmptyList()
    {
        //Arrange
        var target = new RepeatedField<long>();
        ulong[] words = [0, 0];

        //Act
        AckSetWriter.WriteWords(target, words);

        //Assert
        target.Count.ShouldBe(0);
    }

    [Theory]
    [InlineData(3000, 0)]
    [InlineData(3000, 1500)]
    [InlineData(3000, 2999)]
    [InlineData(2048, 2047)]
    [InlineData(2049, 2048)]
    public void WriteIndividual_GivenLargeBatch_ShouldMatchJavaBitSet(int batchSize, int batchIndex)
    {
        //Arrange
        var target = new RepeatedField<long>();
        var bits = new BitArray(batchSize, true);
        bits.Set(batchIndex, false);

        //Act
        AckSetWriter.WriteIndividual(target, batchSize, batchIndex);

        //Assert
        target.ShouldBeAckSet(ToLongArray(bits));
    }

    [Theory]
    [InlineData(3000, 0)]
    [InlineData(3000, 1500)]
    [InlineData(3000, 2998)]
    [InlineData(2049, 63)]
    [InlineData(2049, 64)]
    public void WriteCumulative_GivenLargeBatch_ShouldMatchJavaBitSet(int batchSize, int batchIndex)
    {
        //Arrange
        var target = new RepeatedField<long>();
        var bits = new BitArray(batchSize, true);
        for (var i = 0; i <= batchIndex; ++i)
        {
            bits.Set(i, false);
        }

        //Act
        AckSetWriter.WriteCumulative(target, batchSize, batchIndex);

        //Assert
        target.ShouldBeAckSet(ToLongArray(bits));
    }

    [Fact]
    public void WriteIndividual_AfterWarmup_ShouldNotAllocate()
    {
        //Arrange
        var target = new RepeatedField<long>();
        AckSetWriter.WriteIndividual(target, 1000, 500);
        AckSetWriter.WriteCumulative(target, 1000, 500);
        var before = GC.GetAllocatedBytesForCurrentThread();

        //Act
        for (var i = 0; i < 1000; ++i)
        {
            AckSetWriter.WriteIndividual(target, 1000, i);
            AckSetWriter.WriteCumulative(target, 1000, i);
        }

        //Assert
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        allocated.ShouldBe(0);
    }

    // The same layout as java.util.BitSet.toLongArray()
    private static long[] ToLongArray(BitArray bits)
    {
        var words = new long[(bits.Length + 63) / 64];

        for (var i = 0; i < bits.Length; ++i)
        {
            if (bits[i])
                words[i / 64] |= 1L << (i % 64);
        }

        var length = words.Length;
        while (length > 0 && words[length - 1] == 0)
            --length;

        return words.Take(length).ToArray();
    }
}
