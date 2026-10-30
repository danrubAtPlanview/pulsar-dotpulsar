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

namespace DotPulsar.Internal;

using DotPulsar.Abstractions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;

public sealed class ConsumerChannelFactory<TMessage> : IConsumerChannelFactory<TMessage>
{
    private readonly Guid _correlationId;
    private readonly IRegisterEvent _eventRegister;
    private readonly IConnectionPool _connectionPool;
    private readonly CommandSubscribe _subscribe;
    private readonly uint _messagePrefetchCount;
    private readonly BatchHandler<TMessage> _batchHandler;
    private readonly IMessageFactory<TMessage> _messageFactory;
    private readonly IEnumerable<IDecompressorFactory> _decompressorFactories;
    private readonly string _topic;
    private readonly bool _ackReceiptEnabled;
    private readonly bool _batchIndexAckEnabled;
    private readonly ConsumerEpoch? _consumerEpoch;
    private readonly UnAckedMessageTracker? _unAckedMessageTracker;
    private readonly DeadLetterHandler<TMessage>? _deadLetterHandler;
    private readonly ChunkedMessageAssembler _chunkAssembler;
    private readonly MessageCrypto? _messageCrypto;
    private readonly ICryptoKeyReader? _cryptoKeyReader;
    private readonly ConsumerCryptoFailureAction _cryptoFailureAction;

    public ConsumerChannelFactory(
        Guid correlationId,
        IRegisterEvent eventRegister,
        IConnectionPool connectionPool,
        CommandSubscribe subscribe,
        uint messagePrefetchCount,
        BatchHandler<TMessage> batchHandler,
        IMessageFactory<TMessage> messageFactory,
        IEnumerable<IDecompressorFactory> decompressorFactories,
        string topic,
        bool ackReceiptEnabled,
        bool batchIndexAckEnabled,
        ConsumerEpoch? consumerEpoch,
        UnAckedMessageTracker? unAckedMessageTracker,
        DeadLetterHandler<TMessage>? deadLetterHandler,
        ChunkedMessageAssembler chunkAssembler,
        MessageCrypto? messageCrypto,
        ICryptoKeyReader? cryptoKeyReader,
        ConsumerCryptoFailureAction cryptoFailureAction)
    {
        _correlationId = correlationId;
        _eventRegister = eventRegister;
        _connectionPool = connectionPool;
        _subscribe = subscribe;
        _messagePrefetchCount = messagePrefetchCount;
        _batchHandler = batchHandler;
        _messageFactory = messageFactory;
        _decompressorFactories = decompressorFactories;
        _topic = topic;
        _ackReceiptEnabled = ackReceiptEnabled;
        _batchIndexAckEnabled = batchIndexAckEnabled;
        _consumerEpoch = consumerEpoch;
        _unAckedMessageTracker = unAckedMessageTracker;
        _deadLetterHandler = deadLetterHandler;
        _chunkAssembler = chunkAssembler;
        _messageCrypto = messageCrypto;
        _cryptoKeyReader = cryptoKeyReader;
        _cryptoFailureAction = cryptoFailureAction;
    }

    public async Task<IConsumerChannel<TMessage>> Create(CancellationToken cancellationToken)
    {
        var connection = await _connectionPool.FindConnectionForTopic(_subscribe.Topic, cancellationToken).ConfigureAwait(false);
        var messageQueue = new AsyncQueue<MessagePackage>();
        var channel = new Channel(_correlationId, _eventRegister, messageQueue);

        // Upstream: ConsumerImpl#connectionOpened
        // The messages of the earlier connection are redelivered, so they are no longer candidates
        _deadLetterHandler?.Clear();

        var subscribeEpoch = _consumerEpoch?.Value ?? 0;

        if (_consumerEpoch is not null)
            _subscribe.ConsumerEpoch = (ulong) subscribeEpoch;

        var response = await connection.Send(_subscribe, channel, cancellationToken).ConfigureAwait(false);

        // Upstream: ConsumerImpl#connectionOpened
        // The epoch was increased while the consumer was subscribing, so the broker is told, before the first flow is sent
        if (_consumerEpoch is not null && _consumerEpoch.Value != subscribeEpoch)
        {
            var redeliver = new CommandRedeliverUnacknowledgedMessages { ConsumerId = response.ConsumerId, ConsumerEpoch = (ulong) _consumerEpoch.Value };
            await connection.Send(redeliver, cancellationToken).ConfigureAwait(false);
        }

        var filterEpoch = _subscribe.SubType is CommandSubscribe.Types.SubType.Exclusive or CommandSubscribe.Types.SubType.Failover ? _consumerEpoch : null;
        return new ConsumerChannel<TMessage>(response.ConsumerId, _messagePrefetchCount, messageQueue, connection, _batchHandler, _messageFactory, _decompressorFactories, _topic, _ackReceiptEnabled, _batchIndexAckEnabled, filterEpoch, _unAckedMessageTracker, _deadLetterHandler, _chunkAssembler, _messageCrypto, _cryptoKeyReader, _cryptoFailureAction);
    }
}
