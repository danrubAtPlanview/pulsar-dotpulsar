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

namespace DotPulsar;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/Transaction.java (State)
/// <summary>
/// The possible states of a transaction.
/// </summary>
public enum TransactionState : byte
{
    /// <summary>
    /// The transaction is open. Messages can be produced and acknowledged within it.
    /// </summary>
    Open,

    /// <summary>
    /// A commit has been requested and is in progress.
    /// </summary>
    Committing,

    /// <summary>
    /// An abort has been requested and is in progress.
    /// </summary>
    Aborting,

    /// <summary>
    /// The transaction was committed. This is a final state.
    /// </summary>
    Committed,

    /// <summary>
    /// The transaction was aborted. This is a final state.
    /// </summary>
    Aborted,

    /// <summary>
    /// The transaction coordinator reported that the transaction is not found or has an invalid status. This is a final state.
    /// </summary>
    Error,

    /// <summary>
    /// The transaction timed out on the client side before it was committed or aborted. This is a final state.
    /// </summary>
    TimedOut
}
