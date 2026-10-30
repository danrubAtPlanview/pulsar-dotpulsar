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

namespace Transactions;

using DotPulsar;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Transactions;

// A transaction coordinator client that accepts everything at once
public sealed class FakeTransactionCoordinatorClient : ITransactionCoordinatorClient
{
    private ulong _nextLeastSigBits;

    public FakeTransactionCoordinatorClient()
    {
        _nextLeastSigBits = 0;
    }

    public TransactionCoordinatorClientState State => TransactionCoordinatorClientState.Ready;

    public ValueTask Start(CancellationToken cancellationToken) => default;

    public ValueTask EnsureStarted(CancellationToken cancellationToken) => default;

    public ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken)
        => new(new TransactionId(0, Interlocked.Increment(ref _nextLeastSigBits)));

    public ValueTask AddPublishPartitionToTxn(TransactionId transactionId, string partition, CancellationToken cancellationToken) => default;

    public ValueTask AddSubscriptionToTxn(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken) => default;

    public ValueTask Commit(TransactionId transactionId, CancellationToken cancellationToken) => default;

    public ValueTask Abort(TransactionId transactionId, CancellationToken cancellationToken) => default;

    public ValueTask DisposeAsync() => default;
}
