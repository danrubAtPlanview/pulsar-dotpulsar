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

namespace DotPulsar.Internal.Abstractions;

using DotPulsar.Internal.Transactions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TransactionCoordinatorClient.java
public interface ITransactionCoordinatorClient : IAsyncDisposable
{
    TransactionCoordinatorClientState State { get; }

    ValueTask Start(CancellationToken cancellationToken);
    ValueTask EnsureStarted(CancellationToken cancellationToken);
    ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken);
    ValueTask AddPublishPartitionToTxn(TransactionId transactionId, string partition, CancellationToken cancellationToken);
    ValueTask AddSubscriptionToTxn(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken);
    ValueTask Commit(TransactionId transactionId, CancellationToken cancellationToken);
    ValueTask Abort(TransactionId transactionId, CancellationToken cancellationToken);
}
