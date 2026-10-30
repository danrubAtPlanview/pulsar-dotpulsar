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

namespace DotPulsar.Internal.Transactions;

using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/AssignTopicTcDiscovery.java
public sealed class AssignTopicTcDiscovery : ITcDiscovery
{
    private readonly IConnectionPool _connectionPool;
    private readonly Func<ulong, string, ITransactionMetaStoreHandler> _handlerFactory;
    private ITransactionMetaStoreHandler[]? _handlers;
    private long _epoch;

    public AssignTopicTcDiscovery(IConnectionPool connectionPool, TimeSpan operationTimeout, Func<ulong, string, ITransactionMetaStoreHandler>? handlerFactory = null)
    {
        _connectionPool = connectionPool;
        _handlerFactory = handlerFactory ?? ((tcId, topic) => new TransactionMetaStoreHandler(tcId, topic, connectionPool, operationTimeout));
        _handlers = null;
        _epoch = 0;
    }

    public IReadOnlyList<ITransactionMetaStoreHandler> Handlers
        => Volatile.Read(ref _handlers) ?? [];

    // Upstream: AssignTopicTcDiscovery#start
    public async ValueTask Start(CancellationToken cancellationToken)
    {
        var partitions = await _connectionPool.GetNumberOfPartitions(Constants.TransactionCoordinatorAssignTopic, cancellationToken).ConfigureAwait(false);

        if (partitions == 0)
            throw new TransactionCoordinatorClientException("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized");

        var handlers = new ITransactionMetaStoreHandler[partitions];

        for (var i = 0; i < handlers.Length; ++i)
            handlers[i] = _handlerFactory((ulong) i, GetAssignTopicName(i));

        // The handlers are published before they are started, so that disposing the discovery also disposes the handlers of a failed start
        Volatile.Write(ref _handlers, handlers);

        foreach (var handler in handlers)
            handler.Start();

        await Task.WhenAll(handlers.Select(handler => handler.Connected)).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    // Upstream: AssignTopicTcDiscovery#handlerForCoordinator
    public ITransactionMetaStoreHandler? HandlerForCoordinator(ulong tcId)
    {
        var handlers = Volatile.Read(ref _handlers);
        return handlers is not null && tcId < (ulong) handlers.Length ? handlers[tcId] : null;
    }

    // Upstream: AssignTopicTcDiscovery#nextHandler
    public ITransactionMetaStoreHandler? NextHandler()
    {
        var handlers = Volatile.Read(ref _handlers);
        if (handlers is null || handlers.Length == 0)
            return null;

        var index = (int) ((ulong) Interlocked.Increment(ref _epoch) % (ulong) handlers.Length);
        return handlers[index];
    }

    // Upstream: AssignTopicTcDiscovery#close
    public async ValueTask DisposeAsync()
    {
        var handlers = Interlocked.Exchange(ref _handlers, null);
        if (handlers is null)
            return;

        foreach (var handler in handlers)
        {
            try
            {
                await handler.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // ignored
            }
        }
    }

    // Upstream: AssignTopicTcDiscovery#getTCAssignTopicName
    private static string GetAssignTopicName(int partition)
        => $"{Constants.TransactionCoordinatorAssignTopic}-partition-{partition}";
}
