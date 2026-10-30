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

// A message of a batch that fails when the batch is built from it, which is the only place where a batch of the container allocates and writes. The size of the message is read when it is added,
// so it can not fail then, and the container is only given the reason to fail after the message was added.
public sealed class FailingBatchItem : IBatchItem
{
    private readonly TestBatchItem _item;

    public FailingBatchItem(TestBatchItem item)
        => _item = item;

    public bool Fail { get; set; }

    public MessageMetadata Metadata => _item.Metadata;

    public ReadOnlySequence<byte> Data
        => Fail ? throw new OutOfMemoryException("The batch could not be allocated") : _item.Data;

    public static FailingBatchItem Create(ulong sequenceId, string? key = null)
        => new(TestBatchItem.Create(sequenceId, key: key));
}
