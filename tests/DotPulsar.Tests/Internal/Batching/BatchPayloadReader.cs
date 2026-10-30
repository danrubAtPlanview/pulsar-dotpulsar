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
using System.Buffers;
using System.Buffers.Binary;

// Reads a batch payload without using the code of the consumer
public static class BatchPayloadReader
{
    public static List<(SingleMessageMetadata Metadata, byte[] Payload)> Read(byte[] batchPayload, int numberOfMessages)
    {
        var messages = new List<(SingleMessageMetadata, byte[])>();
        var offset = 0;

        for (var i = 0; i < numberOfMessages; ++i)
        {
            var metadataSize = (int) BinaryPrimitives.ReadUInt32BigEndian(batchPayload.AsSpan(offset, 4));
            offset += 4;
            var metadata = SingleMessageMetadata.Parser.ParseFrom(batchPayload.AsSpan(offset, metadataSize));
            offset += metadataSize;
            var payload = batchPayload.AsSpan(offset, metadata.PayloadSize).ToArray();
            offset += metadata.PayloadSize;
            messages.Add((metadata, payload));
        }

        if (offset != batchPayload.Length)
            throw new ShouldAssertException($"The batch payload has {batchPayload.Length - offset} bytes after the last message");

        return messages;
    }

    public static List<(SingleMessageMetadata Metadata, byte[] Payload)> Read(PreparedBatch batch)
        => Read(batch.Payload.ToArray(), batch.Count);

    public static string ToHex(byte[] bytes)
        => Convert.ToHexString(bytes).ToLowerInvariant();

    public static byte[] FromHex(string hex)
        => Convert.FromHexString(hex);
}
