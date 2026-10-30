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
using DotPulsar.Internal.Transactions;
using System.Buffers;

public sealed class SendOp : IDisposable
{
    public SendOp(
        MessageMetadata metadata,
        ReadOnlySequence<byte> data,
        TaskCompletionSource<MessageId> receiptTcs,
        CancellationToken cancellationToken,
        Transaction? transaction = null,
        Task? registrationGate = null)
    {
        Metadata = metadata;
        Data = data;
        ReceiptTcs = receiptTcs;
        CancellationToken = cancellationToken;
        Transaction = transaction;
        RegistrationGate = registrationGate;
    }

    public MessageMetadata Metadata { get; }
    public ReadOnlySequence<byte> Data { get; }
    public TaskCompletionSource<MessageId> ReceiptTcs { get; }
    public CancellationToken CancellationToken { get; }

    // The transaction the message is produced as part of. Null if the message is not transactional.
    public Transaction? Transaction { get; }

    // The task that completes when the topic of a transactional message is registered with its transaction. Null when there is nothing to wait for before the message is written.
    public Task? RegistrationGate { get; }

    public void Dispose()
    {
        ReceiptTcs.TrySetCanceled();
    }
}
