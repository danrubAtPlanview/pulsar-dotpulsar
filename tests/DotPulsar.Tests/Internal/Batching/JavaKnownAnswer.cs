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

using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Text;

// The bytes were produced by the Java client of Pulsar 4.0.9 (apachepulsar/pulsar:4.0.9, pulsar-common):
// Commands.serializeSingleMessageInBatchWithPayload for the three messages, and
// Commands.initBatchMessageMetadata followed by the setters that BatchMessageContainerImpl#createOpSendMsg calls for the metadata of the batch.
public static class JavaKnownAnswer
{
    public const string Payload = "000000280a080a026b31120276310a080a026b321202763212056b65792d6118092880d095ffbc31300040647061796c6f61642d300000000b18003a0301020340654801000000121208613256354c57493d180330014066500100ff7f";

    public const int PayloadSize = 93;

    // Sequence id 100, highest sequence id 102, transaction (7, 13), compression Zlib and uncompressed size 93
    public const string TransactionalCompressedMetadata = "0a0870726f6475636572106418e8d795ffbc312a07636c757374657232056b65792d613a0775732d656173743a0775732d776573744002485d58038201020908880100b0010db80107c00166";

    // Sequence id 100, highest sequence id 102
    public const string PlainMetadata = "0a0870726f6475636572106418e8d795ffbc312a07636c757374657232056b65792d613a0775732d656173743a0775732d7765737458038201020908880100c00166";

    // Sequence id 102, highest sequence id 102
    public const string KeyBasedMetadata = "0a0870726f6475636572106618e8d795ffbc312a07636c757374657232056b65792d613a0775732d656173743a0775732d7765737458038201020908880100c00166";

    public const string ProducerName = "producer";
    public const ulong PublishTime = 1700000001000;

    // The same first message as in CreateItems, followed by two messages with the same key
    public static TestBatchItem[] CreateItemsWithSameKey()
    {
        var items = CreateItems();

        return [items[0], TestBatchItem.Create(101, key: "key-a"), TestBatchItem.Create(102, key: "key-a")];
    }

    public static TestBatchItem[] CreateItems((ulong Most, ulong Least)? transaction = null)
    {
        var first = new MessageMetadata
        {
            SequenceId = 100,
            PartitionKey = "key-a",
            EventTime = 1700000000000,
            ReplicatedFrom = "cluster",
            SchemaVersion = ByteString.CopyFrom(9, 8)
        };
        first.Properties.Add(new KeyValue { Key = "k1", Value = "v1" });
        first.Properties.Add(new KeyValue { Key = "k2", Value = "v2" });
        first.ReplicateTo.Add("us-east");
        first.ReplicateTo.Add("us-west");

        var second = new MessageMetadata
        {
            SequenceId = 101,
            OrderingKey = ByteString.CopyFrom(1, 2, 3),
            NullValue = true
        };

        var third = new MessageMetadata
        {
            SequenceId = 102,
            PartitionKey = "a2V5LWI=",
            PartitionKeyB64Encoded = true,
            NullPartitionKey = true
        };

        var items = new[]
        {
            new TestBatchItem(first, new(Encoding.UTF8.GetBytes("payload-0"))),
            new TestBatchItem(second, new(Array.Empty<byte>())),
            new TestBatchItem(third, new(new byte[] { 0x00, 0xff, 0x7f }))
        };

        if (transaction is not null)
        {
            foreach (var item in items)
            {
                item.Metadata.TxnidMostBits = transaction.Value.Most;
                item.Metadata.TxnidLeastBits = transaction.Value.Least;
            }
        }

        return items;
    }
}
