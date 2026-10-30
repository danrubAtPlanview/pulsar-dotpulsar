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
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using Google.Protobuf.Collections;
using System.Buffers;
using System.Buffers.Binary;

// Creates the entries that the broker delivers for a producer batch. Message i of a batch has the payload { i % 256 }.
public static class BatchTestData
{
    public const string Topic = "persistent://public/default/test";

    public static MessageMetadata CreateMetadata(int numberOfMessages)
        => new()
        {
            ProducerName = "test",
            NumMessagesInBatch = numberOfMessages,
            PublishTime = 1,
            SequenceId = 1
        };

    public static byte[] CreateBatchPayload(int numberOfMessages, params int[] compactedOut)
    {
        using var stream = new MemoryStream();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            var isCompactedOut = Array.IndexOf(compactedOut, i) >= 0;
            var singleMetadata = new SingleMessageMetadata { PayloadSize = isCompactedOut ? 0 : 1, CompactedOut = isCompactedOut }.ToByteArray();
            var size = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(size, (uint) singleMetadata.Length);
            stream.Write(size, 0, size.Length);
            stream.Write(singleMetadata, 0, singleMetadata.Length);

            if (!isCompactedOut)
                stream.WriteByte((byte) (i % 256));
        }

        return stream.ToArray();
    }

    public static MessageIdData CreateEntryId(ulong ledgerId, ulong entryId, int partition = 0)
        => new() { LedgerId = ledgerId, EntryId = entryId, Partition = partition, BatchIndex = -1 };

    // The id that the consumer is given for message 'batchIndex' of the entry
    public static MessageIdData CreateBatchedId(ulong ledgerId, ulong entryId, int batchIndex, int partition = 0)
        => new() { LedgerId = ledgerId, EntryId = entryId, Partition = partition, BatchIndex = batchIndex };

    public static MessageIdData ToMessageIdData(MessageId messageId)
        => new()
        {
            LedgerId = messageId.LedgerId,
            EntryId = messageId.EntryId,
            Partition = messageId.Partition,
            BatchIndex = messageId.BatchIndex
        };

    public static RepeatedField<long> CreateAckSet(params long[] words)
    {
        var ackSet = new RepeatedField<long>();
        ackSet.AddRange(words);
        return ackSet;
    }

    // The frame of a message as it is received from the broker
    public static MessagePackage CreatePackage(MessageIdData messageId, MessageMetadata metadata, byte[] payload, RepeatedField<long>? ackSet = null, long consumerEpoch = -1, uint redeliveryCount = 0)
    {
        var metadataBytes = metadata.ToByteArray();
        var frame = new byte[10 + metadataBytes.Length + payload.Length];
        frame[0] = 0x0e;
        frame[1] = 0x01;
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(6, 4), (uint) metadataBytes.Length);
        metadataBytes.CopyTo(frame.AsSpan(10));
        payload.CopyTo(frame.AsSpan(10 + metadataBytes.Length));
        var checksum = Crc32C.Calculate(new ReadOnlySequence<byte>(frame, 6, frame.Length - 6));
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(2, 4), checksum);

        return new MessagePackage(messageId, redeliveryCount, ackSet ?? new RepeatedField<long>(), new ReadOnlySequence<byte>(frame), consumerEpoch);
    }

    public static MessagePackage CreateBatchPackage(MessageIdData messageId, int numberOfMessages, RepeatedField<long>? ackSet = null, params int[] compactedOut)
        => CreatePackage(messageId, CreateMetadata(numberOfMessages), CreateBatchPayload(numberOfMessages, compactedOut), ackSet);

    public static MessagePackage CreateSinglePackage(MessageIdData messageId, byte payload = 42, long consumerEpoch = -1, uint redeliveryCount = 0)
        => CreatePackage(messageId, new MessageMetadata { ProducerName = "test", PublishTime = 1, SequenceId = 1 }, [payload], null, consumerEpoch, redeliveryCount);

    // Adds an entry to the handler and returns all messages that it delivers
    public static List<DotPulsar.Abstractions.IMessage<byte[]>> AddBatch(
        BatchHandler<byte[]> batchHandler,
        ulong ledgerId,
        ulong entryId,
        int numberOfMessages,
        RepeatedField<long>? ackSet = null,
        params int[] compactedOut)
        => AddBatch(batchHandler, ledgerId, entryId, numberOfMessages, out _, ackSet, compactedOut);

    public static List<DotPulsar.Abstractions.IMessage<byte[]>> AddBatch(
        BatchHandler<byte[]> batchHandler,
        ulong ledgerId,
        ulong entryId,
        int numberOfMessages,
        out int chargedButSkipped,
        RepeatedField<long>? ackSet = null,
        params int[] compactedOut)
    {
        var messages = new List<DotPulsar.Abstractions.IMessage<byte[]>>();
        var data = new ReadOnlySequence<byte>(CreateBatchPayload(numberOfMessages, compactedOut));
        var first = batchHandler.Add(CreateEntryId(ledgerId, entryId), ackSet, 0, CreateMetadata(numberOfMessages), data, out chargedButSkipped);

        if (first is null)
            return messages;

        messages.Add(first);

        while (batchHandler.GetNext() is { } next)
        {
            messages.Add(next);
        }

        return messages;
    }
}
