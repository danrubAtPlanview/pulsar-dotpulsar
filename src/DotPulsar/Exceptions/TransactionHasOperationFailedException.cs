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

namespace DotPulsar.Exceptions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/PulsarClientException.java (TransactionHasOperationFailedException)
/// <summary>
/// Commit not allowed because operations of the transaction have failed
/// </summary>
public sealed class TransactionHasOperationFailedException : DotPulsarException
{
    // The typo ("Now" instead of "Not") is kept verbatim from upstream
    public TransactionHasOperationFailedException() : base("Now allowed to commit the transaction due to failed operations of producing or acknowledgment") { }

    public TransactionHasOperationFailedException(string message) : base(message) { }

    public TransactionHasOperationFailedException(string message, Exception innerException) : base(message, innerException) { }
}
