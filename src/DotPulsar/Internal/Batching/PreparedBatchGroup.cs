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

// The messages Items[Start] to Items[Start + Count - 1] of a container that are sent as one entry.
// Batch is null when Count is 1, because the message is then sent as it is, without any batch framing.
public readonly struct PreparedBatchGroup
{
    public PreparedBatchGroup(int start, int count, PreparedBatch? batch, ulong sequenceId, ulong highestSequenceId)
    {
        Start = start;
        Count = count;
        Batch = batch;
        SequenceId = sequenceId;
        HighestSequenceId = highestSequenceId;
    }

    public int Start { get; }
    public int Count { get; }
    public PreparedBatch? Batch { get; }

    // The sequence id the broker answers with in the send receipt
    public ulong SequenceId { get; }

    // The highest sequence id of the entry. Same as SequenceId when Count is 1 and for key based batching.
    public ulong HighestSequenceId { get; }
}
