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

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TxnID.java
/// <summary>
/// Unique identifier of a transaction.
/// </summary>
public readonly struct TransactionId : IEquatable<TransactionId>
{
    /// <summary>
    /// Initializes a new instance using the specified most and least significant bits.
    /// </summary>
    public TransactionId(ulong mostSigBits, ulong leastSigBits)
    {
        MostSigBits = mostSigBits;
        LeastSigBits = leastSigBits;
    }

    /// <summary>
    /// The most significant 64 bits of this transaction id. This is the id of the transaction coordinator owning the transaction.
    /// </summary>
    public ulong MostSigBits { get; }

    /// <summary>
    /// The least significant 64 bits of this transaction id.
    /// </summary>
    public ulong LeastSigBits { get; }

    public static bool operator ==(TransactionId x, TransactionId y) => x.Equals(y);
    public static bool operator !=(TransactionId x, TransactionId y) => !x.Equals(y);

    public override bool Equals(object? o) => o is TransactionId id && Equals(id);
    public bool Equals(TransactionId other) => MostSigBits == other.MostSigBits && LeastSigBits == other.LeastSigBits;
    public override int GetHashCode() => HashCode.Combine(MostSigBits, LeastSigBits);
    public override string ToString() => $"({MostSigBits},{LeastSigBits})";
}
