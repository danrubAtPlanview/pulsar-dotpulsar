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

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TransactionCoordinatorClientException.java (MetaStoreHandlerNotExistsException)
/// <summary>
/// Transaction meta store handler not found
/// </summary>
public sealed class MetaStoreHandlerNotExistsException : DotPulsarException
{
    // Upstream has a format bug here (the message contains a literal '{}'), so the id is interpolated
    public MetaStoreHandlerNotExistsException(ulong tcId) : base($"Transaction meta store handler for transaction meta store {tcId} not exists.") { }

    public MetaStoreHandlerNotExistsException(string message) : base(message) { }

    public MetaStoreHandlerNotExistsException(string message, Exception innerException) : base(message, innerException) { }
}
