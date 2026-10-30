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

using DotPulsar.Internal.Batching;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;
using System.Buffers;

public sealed class SendOp : IBatchItem, IDisposable
{
    private static readonly object _single = new();
    private static readonly object _member = new();
    private static readonly object _removed = new();

    // There is one of these for every message, so the size of it matters (the allocations per message are a performance goal). The task that is awaited before the op is written and
    // the group of the op are therefore kept in one field, because the group is not made before the op is written. It is the registration gate while the op is not in a group (null if it has none),
    // '_single' for a group of one op, the batch for the first op of a group of more than one op, '_member' for the other ops of such a group, and '_removed' for an op that has been removed from the queue.
    private object? _gateOrGroup;

    public SendOp(
        MessageMetadata metadata,
        ReadOnlySequence<byte> data,
        TaskCompletionSource<MessageId> receiptTcs,
        Transaction? transaction = null,
        Task? registrationGate = null)
    {
        Metadata = metadata;
        Data = data;
        ReceiptTcs = receiptTcs;
        Transaction = transaction;
        _gateOrGroup = registrationGate;
        CreatedTimestamp = 0;
    }

    public MessageMetadata Metadata { get; }
    public ReadOnlySequence<byte> Data { get; }

    // Completed when the broker has acknowledged the message, when sending it failed, and when the caller or the send timeout has ended it. An op that is not in a group yet is not written if it has been completed.
    public TaskCompletionSource<MessageId> ReceiptTcs { get; }

    // The transaction the message is produced as part of. Null if the message is not transactional.
    public Transaction? Transaction { get; }

    // When the op was put in the send queue (Stopwatch ticks). 0 if it is not known, which is the case if there is no send timeout.
    public long CreatedTimestamp { get; set; }

    // The task that completes when the topic of a transactional message is registered with its transaction. Null when there is nothing to wait for before the message is written.
    public Task? RegistrationGate
        => _gateOrGroup as Task;

    // 0: not in a group yet. More than 0: the first op of a group of that many ops that is sent as one entry. -1: another op of such a group. -2: removed from the queue.
    // A group is frozen when it is made, and it is sent in the same way again after a reconnect.
    public int BatchCount
        => _gateOrGroup switch
        {
            null => 0,
            Task => 0,
            PreparedBatch batch => batch.Count,
            _ => ReferenceEquals(_gateOrGroup, _single) ? 1 : ReferenceEquals(_gateOrGroup, _member) ? -1 : -2
        };

    public bool IsRemoved
        => ReferenceEquals(_gateOrGroup, _removed);

    // The batch of a group of more than one op. It belongs to the first op of the group until the group is completed.
    public PreparedBatch? Batch
        => _gateOrGroup as PreparedBatch;

    public void MakeGroupOfOne()
        => _gateOrGroup = _single;

    public void MakeFirstOfGroup(PreparedBatch batch)
        => _gateOrGroup = batch;

    public void MakeMemberOfGroup()
        => _gateOrGroup = _member;

    public void LeaveGroup()
        => _gateOrGroup = null;

    public void MakeRemoved()
        => _gateOrGroup = _removed;

    public void Dispose()
    {
        _ = ReceiptTcs.TrySetCanceled();
        Batch?.Return();
        _gateOrGroup = null;
    }
}
