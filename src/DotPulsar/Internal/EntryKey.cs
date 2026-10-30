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

namespace DotPulsar.Internal;

using DotPulsar.Internal.PulsarApi;

public readonly struct EntryKey : IEquatable<EntryKey>
{
    public EntryKey(ulong ledgerId, ulong entryId, int partition)
    {
        LedgerId = ledgerId;
        EntryId = entryId;
        Partition = partition;
    }

    public EntryKey(MessageIdData messageId)
        : this(messageId.LedgerId, messageId.EntryId, messageId.Partition)
    {
    }

    public ulong LedgerId { get; }
    public ulong EntryId { get; }
    public int Partition { get; }

    public bool Equals(EntryKey other)
        => LedgerId == other.LedgerId && EntryId == other.EntryId && Partition == other.Partition;

    public override bool Equals(object? obj)
        => obj is EntryKey other && Equals(other);

    public override int GetHashCode()
        => HashCode.Combine(LedgerId, EntryId, Partition);
}
