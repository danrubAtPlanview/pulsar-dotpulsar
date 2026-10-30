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

namespace DotPulsar.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/Transaction.java
/// <summary>
/// A transaction abstraction.
/// </summary>
public interface ITransaction : IStateHolder<TransactionState>
{
    /// <summary>
    /// The id of the transaction.
    /// </summary>
    TransactionId Id { get; }

    /// <summary>
    /// The current state of the transaction.
    /// </summary>
    TransactionState CurrentState { get; }

    /// <summary>
    /// Commit the transaction. Waits for all pending sends and acknowledgments of the transaction to complete before committing.
    /// </summary>
    /// <remarks>
    /// If any send or acknowledgment of the transaction has failed, then the transaction is aborted and a TransactionHasOperationFailedException is thrown.
    /// </remarks>
    ValueTask Commit(CancellationToken cancellationToken = default);

    /// <summary>
    /// Abort the transaction. Waits for all pending sends and acknowledgments of the transaction to complete before aborting.
    /// </summary>
    ValueTask Abort(CancellationToken cancellationToken = default);
}
