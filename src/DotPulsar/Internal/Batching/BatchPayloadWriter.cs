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

using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;
using System.Buffers.Binary;

// Upstream: apache/pulsar@f3846f4 pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java (serializeSingleMessageInBatchWithPayload)
public sealed class BatchPayloadWriter
{
    private readonly SingleMessageMetadata _singleMessageMetadata;

    public BatchPayloadWriter()
        => _singleMessageMetadata = new SingleMessageMetadata();

    // Appends [uint32 size of single message metadata][single message metadata][payload] to the output.
    // The single message metadata is reused, so this method must not be called concurrently.
    public void Write(IBufferWriter<byte> output, MessageMetadata metadata, ReadOnlySequence<byte> data)
    {
        var singleMessageMetadata = _singleMessageMetadata;

        singleMessageMetadata.Properties.Clear();
        singleMessageMetadata.ClearPartitionKey();
        singleMessageMetadata.ClearPartitionKeyB64Encoded();
        singleMessageMetadata.ClearOrderingKey();
        singleMessageMetadata.ClearEventTime();
        singleMessageMetadata.ClearSequenceId();
        singleMessageMetadata.ClearNullValue();
        singleMessageMetadata.ClearNullPartitionKey();

        if (metadata.HasPartitionKey)
        {
            singleMessageMetadata.PartitionKey = metadata.PartitionKey;
            singleMessageMetadata.PartitionKeyB64Encoded = metadata.PartitionKeyB64Encoded;
        }

        if (metadata.HasOrderingKey)
            singleMessageMetadata.OrderingKey = metadata.OrderingKey;

        var properties = metadata.Properties;
        for (var i = 0; i < properties.Count; ++i)
        {
            singleMessageMetadata.Properties.Add(properties[i]);
        }

        if (metadata.HasEventTime)
            singleMessageMetadata.EventTime = metadata.EventTime;

        if (metadata.HasSequenceId)
            singleMessageMetadata.SequenceId = metadata.SequenceId;

        if (metadata.HasNullValue)
            singleMessageMetadata.NullValue = metadata.NullValue;

        if (metadata.HasNullPartitionKey)
            singleMessageMetadata.NullPartitionKey = metadata.NullPartitionKey;

        singleMessageMetadata.PayloadSize = (int) data.Length;

        BinaryPrimitives.WriteUInt32BigEndian(output.GetSpan(4), (uint) singleMessageMetadata.CalculateSize());
        output.Advance(4);
        singleMessageMetadata.WriteTo(output);

        foreach (var segment in data)
        {
            var span = segment.Span;
            span.CopyTo(output.GetSpan(span.Length));
            output.Advance(span.Length);
        }

        // Do not hold on to the properties of the message
        singleMessageMetadata.Properties.Clear();
    }
}
