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
using DotPulsar.Internal.PulsarApi;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionCoordinatorClientImpl.java
public sealed class TransactionCoordinatorClient : ITransactionCoordinatorClient
{
    private readonly Func<ITcDiscovery> _discoveryFactory;
    private readonly object _startLock;
    private ITcDiscovery? _discovery;
    private Task? _startTask;
    private int _state;

    public TransactionCoordinatorClient(IConnectionPool connectionPool, TimeSpan operationTimeout, Func<ITcDiscovery>? discoveryFactory = null)
    {
        _discoveryFactory = discoveryFactory ?? (() => new AssignTopicTcDiscovery(connectionPool, operationTimeout));
        _startLock = new object();
        _discovery = null;
        _startTask = null;
        _state = (int) TransactionCoordinatorClientState.None;
    }

    public TransactionCoordinatorClientState State
        => (TransactionCoordinatorClientState) Volatile.Read(ref _state);

    public IReadOnlyList<ITransactionMetaStoreHandler> Handlers
        => Volatile.Read(ref _discovery)?.Handlers ?? [];

    // Upstream: TransactionCoordinatorClientImpl#startAsync (C# deviation: a failed start resets the state to None so that it can be retried)
    public async ValueTask Start(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _state, (int) TransactionCoordinatorClientState.Starting, (int) TransactionCoordinatorClientState.None) != (int) TransactionCoordinatorClientState.None)
            throw new TransactionCoordinatorClientStateException($"Can not start while current state is {State}");

        var discovery = _discoveryFactory();
        Volatile.Write(ref _discovery, discovery);

        try
        {
            await discovery.Start(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await DisposeDiscovery(discovery).ConfigureAwait(false);
            _ = Interlocked.CompareExchange(ref _discovery, null, discovery);
            _ = Interlocked.CompareExchange(ref _state, (int) TransactionCoordinatorClientState.None, (int) TransactionCoordinatorClientState.Starting);
            throw;
        }

        // A close that happened while starting is not undone
        if (Interlocked.CompareExchange(ref _state, (int) TransactionCoordinatorClientState.Ready, (int) TransactionCoordinatorClientState.Starting) != (int) TransactionCoordinatorClientState.Starting)
            await DisposeDiscovery(discovery).ConfigureAwait(false);
    }

    // C# only: the start is lazy (ADR 0008), so every caller that needs a started client awaits the same start
    public async ValueTask EnsureStarted(CancellationToken cancellationToken)
    {
        var state = State;

        if (state == TransactionCoordinatorClientState.Ready)
            return;

        if (state is TransactionCoordinatorClientState.Closing or TransactionCoordinatorClientState.Closed)
            throw CreateNotReadyException();

        Task startTask;

        lock (_startLock)
        {
            // The start is not tied to the cancellation token of one caller, because other callers wait for the same start.
            // A start that failed has reset the state before its task is faulted, and a caller that comes in between must not wait for the start that failed.
            if (_startTask is null || _startTask.IsFaulted || State == TransactionCoordinatorClientState.None)
                _startTask = Start(CancellationToken.None).AsTask();

            startTask = _startTask;
        }

        try
        {
            await startTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch when (startTask.IsFaulted)
        {
            lock (_startLock)
            {
                if (ReferenceEquals(_startTask, startTask))
                    _startTask = null;
            }

            throw;
        }
    }

    // Upstream: TransactionCoordinatorClientImpl#newTransactionAsync
    public async ValueTask<TransactionId> NewTransaction(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var discovery = GetDiscovery();

        var handler = discovery.NextHandler();
        if (handler is null)
            throw State == TransactionCoordinatorClientState.Ready
                ? new TransactionCoordinatorClientException("No transaction coordinator is currently available")
                : CreateNotReadyException();

        return await handler.NewTransaction(timeout, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionCoordinatorClientImpl#addPublishPartitionToTxnAsync
    public async ValueTask AddPublishPartitionToTxn(TransactionId transactionId, string partition, CancellationToken cancellationToken)
    {
        var handler = GetHandler(transactionId);
        await handler.AddPublishPartitionToTxn(transactionId, partition, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionCoordinatorClientImpl#addSubscriptionToTxnAsync
    public async ValueTask AddSubscriptionToTxn(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken)
    {
        var handler = GetHandler(transactionId);
        await handler.AddSubscriptionToTxn(transactionId, topic, subscription, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionCoordinatorClientImpl#commitAsync
    public async ValueTask Commit(TransactionId transactionId, CancellationToken cancellationToken)
    {
        var handler = GetHandler(transactionId);
        await handler.EndTxn(transactionId, TxnAction.Commit, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionCoordinatorClientImpl#abortAsync
    public async ValueTask Abort(TransactionId transactionId, CancellationToken cancellationToken)
    {
        var handler = GetHandler(transactionId);
        await handler.EndTxn(transactionId, TxnAction.Abort, cancellationToken).ConfigureAwait(false);
    }

    // Upstream: TransactionCoordinatorClientImpl#closeAsync
    public async ValueTask DisposeAsync()
    {
        int previousState;

        do
        {
            previousState = Volatile.Read(ref _state);

            if (previousState is (int) TransactionCoordinatorClientState.Closing or (int) TransactionCoordinatorClientState.Closed)
                return;
        }
        while (Interlocked.CompareExchange(ref _state, (int) TransactionCoordinatorClientState.Closing, previousState) != previousState);

        // The discovery is kept, so a request that has read the 'Ready' state just before the close reaches the disposed discovery, which fails it
        var discovery = Volatile.Read(ref _discovery);
        if (discovery is not null)
            await DisposeDiscovery(discovery).ConfigureAwait(false);

        Volatile.Write(ref _state, (int) TransactionCoordinatorClientState.Closed);
    }

    private ITcDiscovery GetDiscovery()
    {
        var discovery = Volatile.Read(ref _discovery);

        if (State != TransactionCoordinatorClientState.Ready || discovery is null)
            throw CreateNotReadyException();

        return discovery;
    }

    private ITransactionMetaStoreHandler GetHandler(TransactionId transactionId)
    {
        var handler = GetDiscovery().HandlerForCoordinator(transactionId.MostSigBits);
        if (handler is null)
            throw State == TransactionCoordinatorClientState.Ready
                ? new MetaStoreHandlerNotExistsException(transactionId.MostSigBits)
                : CreateNotReadyException();

        return handler;
    }

    // Upstream: TransactionCoordinatorClientImpl#notReadyFailure
    private TransactionCoordinatorClientStateException CreateNotReadyException()
        => new($"Can not send a request while current state is {State}");

    private static async ValueTask DisposeDiscovery(ITcDiscovery discovery)
    {
        try
        {
            await discovery.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // ignored
        }
    }
}
