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

using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;

public sealed class ProducerChannelFactory : IProducerChannelFactory
{
    private readonly Guid _correlationId;
    private readonly IRegisterEvent _eventRegister;
    private readonly IConnectionPool _connectionPool;
    private readonly CommandProducer _commandProducer;
    private readonly ICompressorFactory? _compressorFactory;
    private readonly Schema? _schema;
    private readonly MessageCrypto? _messageCrypto;
    private readonly ProducerCryptoFailureAction _cryptoFailureAction;
    private readonly bool _hasGeneratedName;
    private string? _generatedName;
    private ulong _epoch;
    private ulong? _topicEpoch;

    public ProducerChannelFactory(
        Guid correlationId,
        IRegisterEvent eventRegister,
        IConnectionPool connectionPool,
        string topic,
        string? producerName,
        ProducerAccessMode producerAccessMode,
        SchemaInfo schemaInfo,
        ICompressorFactory? compressorFactory,
        Dictionary<string, string>? properties,
        bool transactionsEnabled,
        string? initialSubscriptionName,
        MessageCrypto? messageCrypto,
        ProducerCryptoFailureAction cryptoFailureAction)
    {
        _correlationId = correlationId;
        _eventRegister = eventRegister;
        _connectionPool = connectionPool;
        _schema = schemaInfo.PulsarSchema;

        // Upstream: ProducerImpl#ProducerImpl (producerId = client.newProducerId())
        // The producer has the same id on every connection, because a broker only replaces the registration of a producer on a connection by a later one if the id is the same (Producer#isSuccessorTo).
        _commandProducer = new CommandProducer
        {
            ProducerAccessMode = producerAccessMode,
            ProducerId = ProducerIds.Next(),
            Topic = topic
        };

        // Upstream: ProducerImpl#ProducerImpl (userProvidedProducerName), Commands#newProducer
        // A producer without a name gets one from the broker, and sends it again when it connects again, so the broker sees the same producer and deduplicates what is sent again.
        // It says that the name is not the user's, also the first time, because older brokers (for example 3.1.0) only replace a registration if neither name is the user's (AbstractTopic#tryOverwriteOldProducer).
        // A name that the user gave is sent as it always was.
        if (producerName is not null)
            _commandProducer.ProducerName = producerName;
        else
        {
            _commandProducer.UserProvidedProducerName = false;
            _hasGeneratedName = true;
        }

        if (_schema.Type != Schema.Types.Type.None)
            _commandProducer.Schema = _schema;

        if (properties is not null)
            _commandProducer.Metadata.AddRange(properties.Select(x => new KeyValue { Key = x.Key, Value = x.Value }));

        // Upstream: ProducerImpl#connectionOpened (CommandProducer txn_enabled). It is only set when transactions are enabled, so that the bytes sent by clients without transactions do not change.
        if (transactionsEnabled)
            _commandProducer.TxnEnabled = true;

        if (!string.IsNullOrEmpty(initialSubscriptionName))
            _commandProducer.InitialSubscriptionName = initialSubscriptionName;

        // Upstream: ProducerImpl#connectionOpened (CommandProducer encrypted). The broker of a namespace that requires encryption rejects a producer that does not set it, and it rejects the messages that are not encrypted.
        // It is only set when the messages are encrypted, so that the bytes sent by clients without encryption do not change.
        if (messageCrypto is not null)
            _commandProducer.Encrypted = true;

        _compressorFactory = compressorFactory;
        _messageCrypto = messageCrypto;
        _cryptoFailureAction = cryptoFailureAction;
    }

    public async Task<IProducerChannel> Create(CancellationToken cancellationToken)
    {
        if (_topicEpoch.HasValue)
        {
            if (_commandProducer.ProducerAccessMode != ProducerAccessMode.Shared)
                _commandProducer.ProducerAccessMode = ProducerAccessMode.Exclusive;
            _commandProducer.TopicEpoch = _topicEpoch.Value;
        }
        else
            _commandProducer.ClearTopicEpoch();

        // Upstream: ConnectionHandler#switchClientCnx
        // The epoch grows with every attempt to connect, so the broker can tell a later registration of the producer from an earlier one.
        if (_hasGeneratedName)
        {
            _commandProducer.Epoch = _epoch++;

            if (_generatedName is not null)
                _commandProducer.ProducerName = _generatedName;
        }

        var connection = await _connectionPool.FindConnectionForTopic(_commandProducer.Topic, cancellationToken).ConfigureAwait(false);
        var channel = new Channel(_correlationId, _eventRegister, new AsyncQueue<MessagePackage>());
        var response = await connection.Send(_commandProducer, channel, cancellationToken).ConfigureAwait(false);
        _topicEpoch = response.TopicEpoch;

        // Upstream: ProducerImpl#connectionOpened (if (this.producerName == null) this.producerName = producerName)
        if (_hasGeneratedName)
            _generatedName ??= response.ProducerName;

        var schemaVersion = await GetSchemaVersion(connection, cancellationToken).ConfigureAwait(false);
        return new ProducerChannel(response.ProducerId, response.ProducerName, connection, _compressorFactory, schemaVersion, _messageCrypto, _cryptoFailureAction);
    }

    private async ValueTask<byte[]?> GetSchemaVersion(IConnection connection, CancellationToken cancellationToken)
    {
        if (_schema is null || _schema.Type == Schema.Types.Type.None)
            return null;

        var command = new CommandGetOrCreateSchema
        {
            Schema = _schema,
            Topic = _commandProducer.Topic
        };

        var response = await connection.Send(command, cancellationToken).ConfigureAwait(false);

        response.Expect(BaseCommand.Types.Type.GetOrCreateSchemaResponse);
        if (response.GetOrCreateSchemaResponse.HasErrorCode)
            response.GetOrCreateSchemaResponse.Throw();

        return response.GetOrCreateSchemaResponse.SchemaVersion.ToArray();
    }
}
