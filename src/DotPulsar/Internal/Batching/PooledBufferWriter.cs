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

namespace DotPulsar.Internal.Batching;

using System.Buffers;

public sealed class PooledBufferWriter : IBufferWriter<byte>
{
    private const int MinimumGrowth = 256;

    private byte[]? _array;
    private int _writtenCount;

    public PooledBufferWriter()
    {
        _array = null;
        _writtenCount = 0;
    }

    public int WrittenCount => _writtenCount;

    public ReadOnlyMemory<byte> WrittenMemory
        => _array is null ? ReadOnlyMemory<byte>.Empty : new ReadOnlyMemory<byte>(_array, 0, _writtenCount);

    public Span<byte> WrittenSpan
        => _array is null ? Span<byte>.Empty : new Span<byte>(_array, 0, _writtenCount);

    public int Capacity => _array?.Length ?? 0;

    // Rents the array that is to hold at least 'capacity' bytes. Does nothing if the writer already owns an array.
    public void Initialize(int capacity)
    {
        if (_array is not null)
            return;

        _array = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, MinimumGrowth));
        _writtenCount = 0;
    }

    public void Advance(int count)
    {
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, "The count cannot be negative");

        if (count == 0)
            return;

        if (_array is null || _writtenCount > _array.Length - count)
            throw new InvalidOperationException("Cannot advance past the end of the buffer");

        _writtenCount += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return new Memory<byte>(_array, _writtenCount, _array!.Length - _writtenCount);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return new Span<byte>(_array, _writtenCount, _array!.Length - _writtenCount);
    }

    // Returns the array to the pool. The writer can be used again afterwards.
    public void Reset()
    {
        var array = _array;
        _array = null;
        _writtenCount = 0;

        if (array is not null)
            ArrayPool<byte>.Shared.Return(array);
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 0)
            throw new ArgumentOutOfRangeException(nameof(sizeHint), sizeHint, "The size hint cannot be negative");

        if (sizeHint == 0)
            sizeHint = 1;

        if (_array is null)
        {
            _array = ArrayPool<byte>.Shared.Rent(Math.Max(sizeHint, MinimumGrowth));
            return;
        }

        var available = _array.Length - _writtenCount;
        if (sizeHint <= available)
            return;

        var needed = checked(_writtenCount + sizeHint);
        var newLength = Math.Max(needed, (int) Math.Min((long) _array.Length * 2, int.MaxValue));
        var newArray = ArrayPool<byte>.Shared.Rent(newLength);
        Buffer.BlockCopy(_array, 0, newArray, 0, _writtenCount);
        ArrayPool<byte>.Shared.Return(_array);
        _array = newArray;
    }
}
