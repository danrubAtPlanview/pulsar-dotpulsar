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

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TcDiscovery.java
public interface ITcDiscovery : IAsyncDisposable
{
    IReadOnlyList<ITransactionMetaStoreHandler> Handlers { get; }

    ValueTask Start(CancellationToken cancellationToken);

    ITransactionMetaStoreHandler? HandlerForCoordinator(ulong tcId);

    ITransactionMetaStoreHandler? NextHandler();
}
