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
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;
using System.Text;

public sealed class TestBatchItem : IBatchItem
{
    public TestBatchItem(MessageMetadata metadata, ReadOnlySequence<byte> data)
    {
        Metadata = metadata;
        Data = data;
    }

    public MessageMetadata Metadata { get; }
    public ReadOnlySequence<byte> Data { get; }

    public static TestBatchItem Create(
        ulong sequenceId,
        string? payload = null,
        string? key = null,
        byte[]? orderingKey = null,
        (ulong Most, ulong Least)? transaction = null)
        => Create(sequenceId, Encoding.UTF8.GetBytes(payload ?? $"payload-{sequenceId}"), key, orderingKey, transaction);

    public static TestBatchItem Create(
        ulong sequenceId,
        byte[] payload,
        string? key = null,
        byte[]? orderingKey = null,
        (ulong Most, ulong Least)? transaction = null)
    {
        var metadata = new MessageMetadata { SequenceId = sequenceId };

        if (key is not null)
            metadata.PartitionKey = key;

        if (orderingKey is not null)
            metadata.OrderingKey = ByteString.CopyFrom(orderingKey);

        if (transaction is not null)
        {
            metadata.TxnidMostBits = transaction.Value.Most;
            metadata.TxnidLeastBits = transaction.Value.Least;
        }

        return new TestBatchItem(metadata, new ReadOnlySequence<byte>(payload));
    }

    // A message with a payload of 'size' bytes
    public static TestBatchItem CreateWithSize(ulong sequenceId, int size)
        => Create(sequenceId, new byte[size]);

    public static TestBatchItem CreateWithKey(ulong sequenceId, int size, string key)
        => Create(sequenceId, new byte[size], key);

    // The payload split into segments of one byte
    public static TestBatchItem CreateSegmented(ulong sequenceId, byte[] payload)
    {
        if (payload.Length < 2)
            return Create(sequenceId, payload);

        var first = new Segment(payload.AsMemory(0, 1), null);
        var last = first;

        for (var i = 1; i < payload.Length; ++i)
        {
            last = last.Append(payload.AsMemory(i, 1));
        }

        return new TestBatchItem(new MessageMetadata { SequenceId = sequenceId }, new ReadOnlySequence<byte>(first, 0, last, 1));
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, Segment? previous)
        {
            Memory = memory;
            RunningIndex = previous is null ? 0 : previous.RunningIndex + previous.Memory.Length;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory, this);
            Next = next;
            return next;
        }
    }
}
