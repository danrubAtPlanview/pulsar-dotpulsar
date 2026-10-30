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

using DotPulsar.Abstractions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;

public interface ITransactionMetaStoreHandler : IStateHolder<TransactionMetaStoreHandlerState>, IAsyncDisposable
{
    TransactionMetaStoreHandlerState CurrentState { get; }

    Task Connected { get; }

    void Start();

    void ConnectionClosed();

    ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken);
    ValueTask AddPublishPartitionToTxn(TransactionId transactionId, string partition, CancellationToken cancellationToken);
    ValueTask AddSubscriptionToTxn(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken);
    ValueTask EndTxn(TransactionId transactionId, TxnAction action, CancellationToken cancellationToken);
}
