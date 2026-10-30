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

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.Transactions;

/// <summary>
/// Pulsar client for creating producers, consumers, readers and transactions.
/// </summary>
public sealed class PulsarClient : IPulsarClient
{
    private readonly object _lock = new();
    private readonly HashSet<IAsyncDisposable> _disposables;
    private readonly IConnectionPool _connectionPool;
    private readonly IHandleException _exceptionHandler;
    private readonly TimeSpan _operationTimeout;
    private readonly ITransactionCoordinatorClient? _transactionCoordinatorClient;
    private int _isDisposed;

    public Uri ServiceUrl { get; }

    /// <summary>
    /// The transaction coordinator client. It is null if transactions are not enabled, and it is started when the first transaction is created.
    /// </summary>
    public ITransactionCoordinatorClient? TransactionCoordinatorClient => _transactionCoordinatorClient;

    internal PulsarClient(
        IConnectionPool connectionPool,
        IHandleException exceptionHandler,
        Uri serviceUrl,
        TimeSpan operationTimeout,
        ITransactionCoordinatorClient? transactionCoordinatorClient)
    {
        _disposables = [];
        _connectionPool = connectionPool;
        _exceptionHandler = exceptionHandler;
        _operationTimeout = operationTimeout;
        _transactionCoordinatorClient = transactionCoordinatorClient;
        ServiceUrl = serviceUrl;
        _isDisposed = 0;
        DotPulsarMeter.ClientCreated();
    }

    /// <summary>
    /// Get a builder that can be used to configure and build a PulsarClient instance.
    /// </summary>
    public static IPulsarClientBuilder Builder()
        => new PulsarClientBuilder();

    /// <summary>
    /// Create a producer.
    /// </summary>
    public IProducer<TMessage> CreateProducer<TMessage>(ProducerOptions<TMessage> options)
    {
        ThrowIfDisposed();

        if (options.MaxPendingMessages == 0)
            throw new ConfigurationException("ProducerOptions.MaxPendingMessages must be greater than 0");

        if (options.BatchingMaxPublishDelay < TimeSpan.FromMilliseconds(1))
            throw new ConfigurationException("ProducerOptions.BatchingMaxPublishDelay must be at least 1 millisecond");

        ICompressorFactory? compressorFactory = null;
        if (options.CompressionType != CompressionType.None)
        {
            var compressionType = (Internal.PulsarApi.CompressionType) options.CompressionType;
            compressorFactory = CompressionFactories.CompressorFactories().SingleOrDefault(f => f.CompressionType == compressionType);

            if (compressorFactory is null)
                throw new CompressionException($"Support for {compressionType} compression was not found");
        }

        var producer = new Producer<TMessage>(ServiceUrl, options, _exceptionHandler, _connectionPool, compressorFactory, _transactionCoordinatorClient is not null);

        if (options.StateChangedHandler is not null)
            _ = StateMonitor.MonitorProducer(producer, options.StateChangedHandler);

        AddDisposable(producer);
        producer.StateChangedTo(ProducerState.Closed).AsTask().ContinueWith(_ => RemoveDisposable(producer));
        return producer;
    }

    /// <summary>
    /// Create a consumer.
    /// </summary>
    public IConsumer<TMessage> CreateConsumer<TMessage>(ConsumerOptions<TMessage> options)
    {
        ThrowIfDisposed();

        if (options.AckTimeout != TimeSpan.Zero && options.AckTimeout < TimeSpan.FromSeconds(1))
            throw new ConfigurationException("ConsumerOptions.AckTimeout must be TimeSpan.Zero (disabled) or at least 1 second");

        if (options.AckTimeoutTickTime < TimeSpan.FromMilliseconds(100))
            throw new ConfigurationException("ConsumerOptions.AckTimeoutTickTime must be at least 100 milliseconds");

        if (options.DeadLetterPolicy is not null && options.DeadLetterPolicy.MaxRedeliverCount == 0)
            throw new ConfigurationException("MaxRedeliverCount must be > 0.");

        if (options.NegativeAcknowledgeRedeliveryDelay < TimeSpan.Zero)
            throw new ConfigurationException("ConsumerOptions.NegativeAcknowledgeRedeliveryDelay may not be negative");

        if (options.ExpireTimeOfIncompleteChunkedMessage < TimeSpan.Zero)
            throw new ConfigurationException("ConsumerOptions.ExpireTimeOfIncompleteChunkedMessage may not be negative");

        var consumer = new Consumer<TMessage>(ServiceUrl, options, _connectionPool, _exceptionHandler, _operationTimeout);
        if (options.StateChangedHandler is not null)
            _ = StateMonitor.MonitorConsumer(consumer, options.StateChangedHandler);

        AddDisposable(consumer);
        consumer.StateChangedTo(ConsumerState.Closed).AsTask().ContinueWith(_ => RemoveDisposable(consumer));
        return consumer;
    }

    /// <summary>
    /// Create a reader.
    /// </summary>
    public IReader<TMessage> CreateReader<TMessage>(ReaderOptions<TMessage> options)
    {
        ThrowIfDisposed();

        if (options.ExpireTimeOfIncompleteChunkedMessage < TimeSpan.Zero)
            throw new ConfigurationException("ReaderOptions.ExpireTimeOfIncompleteChunkedMessage may not be negative");

        var reader = new Reader<TMessage>(ServiceUrl, options, _exceptionHandler, _connectionPool);
        if (options.StateChangedHandler is not null)
            _ = StateMonitor.MonitorReader(reader, options.StateChangedHandler);

        AddDisposable(reader);
        reader.StateChangedTo(ReaderState.Closed).AsTask().ContinueWith(_ => RemoveDisposable(reader));
        return reader;
    }

    // Upstream: TransactionBuilderImpl#build (C# deviation: the transaction coordinator client is started by the first transaction, because building the client does not connect to the broker)
    /// <summary>
    /// Create a transaction.
    /// </summary>
    public async ValueTask<ITransaction> CreateTransaction(TransactionOptions options, CancellationToken cancellationToken = default)
    {
        if (_transactionCoordinatorClient is null)
            throw new ConfigurationException("Transactions are not enabled");

        await _transactionCoordinatorClient.EnsureStarted(cancellationToken).ConfigureAwait(false);
        var id = await _transactionCoordinatorClient.NewTransaction(options.Timeout, cancellationToken).ConfigureAwait(false);
        return new Transaction(_transactionCoordinatorClient, id, options.Timeout);
    }

    /// <summary>
    /// Dispose the client and all its producers, consumers, readers and the transaction coordinator client.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        IEnumerable<IAsyncDisposable> disposables;

        lock (_lock)
        {
            disposables = _disposables.ToArray();
            _disposables.Clear();
        }

        foreach (var item in disposables)
            await item.DisposeAsync().ConfigureAwait(false);

        // Upstream: PulsarClientImpl#shutdown
        if (_transactionCoordinatorClient is not null)
        {
            try
            {
                await _transactionCoordinatorClient.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // ignored
            }
        }

        await _connectionPool.DisposeAsync().ConfigureAwait(false);

        DotPulsarMeter.ClientDisposed();
    }

    private void AddDisposable(IAsyncDisposable disposable)
    {
        lock (_lock)
        {
            _disposables.Add(disposable);
        }
    }

    internal void RemoveDisposable(IAsyncDisposable disposable)
    {
        lock (_lock)
        {
            _disposables.Remove(disposable);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_isDisposed != 0)
            throw new PulsarClientDisposedException();
    }
}
