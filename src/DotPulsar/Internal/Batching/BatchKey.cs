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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/BatchMessageKeyBasedContainer.java (getKey)
// The key that messages are grouped by: the ordering key if it is set, otherwise the partition key. Messages without a key share one key.
// C# deviation: Java uses the base64 encoded ordering key as a string, which allocates for every message. An ordering key is never equal to a partition key here.
public readonly struct BatchKey : IEquatable<BatchKey>
{
    private readonly ByteString? _orderingKey;
    private readonly string? _partitionKey;

    public BatchKey(MessageMetadata metadata)
    {
        if (metadata.HasOrderingKey)
        {
            _orderingKey = metadata.OrderingKey;
            _partitionKey = null;
        }
        else
        {
            _orderingKey = null;
            _partitionKey = metadata.HasPartitionKey ? metadata.PartitionKey : null;
        }
    }

    public bool Equals(BatchKey other)
    {
        if (_orderingKey is not null)
            return other._orderingKey is not null && _orderingKey.Equals(other._orderingKey);

        return other._orderingKey is null && string.Equals(_partitionKey, other._partitionKey, StringComparison.Ordinal);
    }

    public override bool Equals(object? obj)
        => obj is BatchKey other && Equals(other);

    public override int GetHashCode()
        => _orderingKey is not null ? _orderingKey.GetHashCode() : _partitionKey?.GetHashCode() ?? 0;
}
