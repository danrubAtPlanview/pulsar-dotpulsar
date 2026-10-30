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

namespace DotPulsar.Tests.Internal.Batching;

using DotPulsar.Internal.Batching;

[Trait("Category", "Unit")]
public sealed class PooledBufferWriterTests
{
    [Fact]
    public void WrittenMemory_GivenNothingWritten_ShouldBeEmpty()
    {
        //Arrange
        var uut = new PooledBufferWriter();

        //Act
        var actual = uut.WrittenMemory;

        //Assert
        actual.Length.ShouldBe(0);
        uut.WrittenSpan.Length.ShouldBe(0);
        uut.WrittenCount.ShouldBe(0);
        uut.Capacity.ShouldBe(0);
    }

    [Fact]
    public void GetSpan_GivenNothingWritten_ShouldReturnSpanWithAtLeastTheSizeHint()
    {
        //Arrange
        var uut = new PooledBufferWriter();

        //Act
        var actual = uut.GetSpan(5000);

        //Assert
        actual.Length.ShouldBeGreaterThanOrEqualTo(5000);

        //Annihilate
        uut.Reset();
    }

    [Fact]
    public void GetSpan_GivenSizeHintZero_ShouldReturnNonEmptySpan()
    {
        //Arrange
        var uut = new PooledBufferWriter();

        //Act
        var actual = uut.GetSpan();

        //Assert
        actual.Length.ShouldBeGreaterThan(0);

        //Annihilate
        uut.Reset();
    }

    [Fact]
    public void GetSpan_GivenSizeLargerThanTheFreeSpace_ShouldGrowAndKeepTheWrittenBytes()
    {
        //Arrange
        var uut = new PooledBufferWriter();
        uut.Initialize(16);
        var initialCapacity = uut.Capacity;
        var expected = new byte[initialCapacity - 3];
        new Random(1).NextBytes(expected);
        expected.CopyTo(uut.GetSpan(expected.Length));
        uut.Advance(expected.Length);

        //Act
        var span = uut.GetSpan(1000);
        span[0] = 42;
        uut.Advance(1);

        //Assert
        uut.Capacity.ShouldBeGreaterThan(initialCapacity);
        span.Length.ShouldBeGreaterThanOrEqualTo(1000);
        uut.WrittenCount.ShouldBe(expected.Length + 1);
        uut.WrittenSpan.Slice(0, expected.Length).ToArray().ShouldBe(expected);
        uut.WrittenSpan[expected.Length].ShouldBe((byte) 42);

        //Annihilate
        uut.Reset();
    }

    [Fact]
    public void GetMemory_GivenBytesWritten_ShouldReturnMemoryAfterTheWrittenBytes()
    {
        //Arrange
        var uut = new PooledBufferWriter();
        uut.GetSpan(4)[0] = 1;
        uut.Advance(1);

        //Act
        var memory = uut.GetMemory(10);
        memory.Span[0] = 2;
        uut.Advance(1);

        //Assert
        uut.WrittenMemory.ToArray().ShouldBe([1, 2]);

        //Annihilate
        uut.Reset();
    }

    [Fact]
    public void Advance_GivenCountBeyondTheArray_ShouldThrow()
    {
        //Arrange
        var uut = new PooledBufferWriter();
        uut.Initialize(16);

        //Act
        var exception = Record.Exception(() => uut.Advance(uut.Capacity + 1));

        //Assert
        exception.ShouldBeOfType<InvalidOperationException>();

        //Annihilate
        uut.Reset();
    }

    [Fact]
    public void Advance_GivenNegativeCount_ShouldThrow()
    {
        //Arrange
        var uut = new PooledBufferWriter();

        //Act
        var exception = Record.Exception(() => uut.Advance(-1));

        //Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Advance_GivenZeroAndNoArray_ShouldNotThrow()
    {
        //Arrange
        var uut = new PooledBufferWriter();

        //Act
        var exception = Record.Exception(() => uut.Advance(0));

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public void GetSpan_GivenNegativeSizeHint_ShouldThrow()
    {
        //Arrange
        var uut = new PooledBufferWriter();

        //Act
        var exception = Record.Exception(() => uut.GetSpan(-1));

        //Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Initialize_GivenAlreadyInitialized_ShouldKeepTheWrittenBytes()
    {
        //Arrange
        var uut = new PooledBufferWriter();
        uut.Initialize(64);
        uut.GetSpan(1)[0] = 7;
        uut.Advance(1);

        //Act
        uut.Initialize(1024);

        //Assert
        uut.WrittenCount.ShouldBe(1);
        uut.WrittenSpan[0].ShouldBe((byte) 7);

        //Annihilate
        uut.Reset();
    }

    [Fact]
    public void Reset_GivenWrittenBytes_ShouldReleaseTheArrayAndAllowReuse()
    {
        //Arrange
        var uut = new PooledBufferWriter();
        uut.GetSpan(100)[0] = 1;
        uut.Advance(1);

        //Act
        uut.Reset();
        uut.Reset();

        //Assert
        uut.WrittenCount.ShouldBe(0);
        uut.Capacity.ShouldBe(0);
        uut.WrittenMemory.Length.ShouldBe(0);

        uut.GetSpan(10)[0] = 5;
        uut.Advance(1);
        uut.WrittenMemory.ToArray().ShouldBe([5]);

        //Annihilate
        uut.Reset();
    }
}
