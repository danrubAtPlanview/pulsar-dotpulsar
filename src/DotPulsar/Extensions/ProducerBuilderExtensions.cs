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

namespace DotPulsar.Extensions;

using DotPulsar.Abstractions;
using DotPulsar.Internal;

/// <summary>
/// Extensions for IProducerBuilder.
/// </summary>
public static class ProducerBuilderExtensions
{
    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IProducerBuilder<TMessage> StateChangedHandler<TMessage>(
        this IProducerBuilder<TMessage> builder,
        Action<ProducerStateChanged> handler)
    {
        void forwarder(ProducerStateChanged producerStateChanged, CancellationToken _) => handler(producerStateChanged);
        builder.StateChangedHandler(new ActionStateChangedHandler<ProducerStateChanged>(forwarder, default));
        return builder;
    }

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IProducerBuilder<TMessage> StateChangedHandler<TMessage>(
        this IProducerBuilder<TMessage> builder,
        Action<ProducerStateChanged, CancellationToken> handler,
        CancellationToken cancellationToken = default)
    {
        builder.StateChangedHandler(new ActionStateChangedHandler<ProducerStateChanged>(handler, cancellationToken));
        return builder;
    }

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IProducerBuilder<TMessage> StateChangedHandler<TMessage>(
        this IProducerBuilder<TMessage> builder,
        Func<ProducerStateChanged, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        builder.StateChangedHandler(new FuncStateChangedHandler<ProducerStateChanged>(handler, cancellationToken));
        return builder;
    }

    /// <summary>
    /// Use the DefaultCryptoKeyReader with a public key URI that is always used to encrypt.
    /// </summary>
    public static IProducerBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(
        this IProducerBuilder<TMessage> builder,
        string publicKey)
        => builder.CryptoKeyReader(new DefaultCryptoKeyReader(publicKey, null));

    /// <summary>
    /// Use the DefaultCryptoKeyReader with public key URIs by key name.
    /// </summary>
    public static IProducerBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(
        this IProducerBuilder<TMessage> builder,
        IReadOnlyDictionary<string, string> publicKeys)
        => builder.CryptoKeyReader(new DefaultCryptoKeyReader(null, null, publicKeys));

    /// <summary>
    /// Whether to batch messages. The default is 'false'.
    /// </summary>
    public static IProducerBuilder<TMessage> EnableBatching<TMessage>(this IProducerBuilder<TMessage> builder, bool enableBatching)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(EnableBatching)).EnableBatching(enableBatching);

    /// <summary>
    /// Set the maximum number of messages in a batch. 0 means that there is no limit. The default is 1000.
    /// </summary>
    public static IProducerBuilder<TMessage> BatchingMaxMessages<TMessage>(this IProducerBuilder<TMessage> builder, uint batchingMaxMessages)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(BatchingMaxMessages)).BatchingMaxMessages(batchingMaxMessages);

    /// <summary>
    /// Set the maximum number of bytes (uncompressed) in a batch. 0 means that the maximum message size of the broker is used. The default is 131072 (128 KB).
    /// </summary>
    public static IProducerBuilder<TMessage> BatchingMaxBytes<TMessage>(this IProducerBuilder<TMessage> builder, uint batchingMaxBytes)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(BatchingMaxBytes)).BatchingMaxBytes(batchingMaxBytes);

    /// <summary>
    /// Set the maximum delay between the first message being added to a batch and the batch being sent. It must be at least 1 millisecond. The default is 1 millisecond.
    /// </summary>
    public static IProducerBuilder<TMessage> BatchingMaxPublishDelay<TMessage>(this IProducerBuilder<TMessage> builder, TimeSpan batchingMaxPublishDelay)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(BatchingMaxPublishDelay)).BatchingMaxPublishDelay(batchingMaxPublishDelay);

    /// <summary>
    /// Set the batcher type. The default is 'Default'.
    /// </summary>
    public static IProducerBuilder<TMessage> BatcherType<TMessage>(this IProducerBuilder<TMessage> builder, BatcherType batcherType)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(BatcherType)).BatcherType(batcherType);

    /// <summary>
    /// Set the send timeout. If the broker has not acknowledged the oldest message that is waiting within this time, every message that is waiting in the producer (of the partition) fails with an OperationTimeoutException. TimeSpan.Zero disables the timeout. The default is 'TimeSpan.Zero'.
    /// </summary>
    public static IProducerBuilder<TMessage> SendTimeout<TMessage>(this IProducerBuilder<TMessage> builder, TimeSpan sendTimeout)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(SendTimeout)).SendTimeout(sendTimeout);

    /// <summary>
    /// Whether to split messages that are larger than the maximum message size of the broker into chunks. It only applies to persistent topics, and it can not be combined with batching. The default is 'false'.
    /// </summary>
    public static IProducerBuilder<TMessage> EnableChunking<TMessage>(this IProducerBuilder<TMessage> builder, bool enableChunking)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(EnableChunking)).EnableChunking(enableChunking);

    /// <summary>
    /// Whether a send that the broker rejects with 'NotAllowedError', and a message that is larger than the maximum message size of the broker, fail only that message. The default is 'true'. With 'false' the producer faults, as it did before DotPulsar had this option.
    /// </summary>
    public static IProducerBuilder<TMessage> FailOnlyTheRejectedMessage<TMessage>(this IProducerBuilder<TMessage> builder, bool failOnlyTheRejectedMessage)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(FailOnlyTheRejectedMessage)).FailOnlyTheRejectedMessage(failOnlyTheRejectedMessage);

    /// <summary>
    /// Set the maximum number of bytes in the payload of a chunk. 0 or less means that the maximum message size of the broker is used. The default is 0.
    /// </summary>
    public static IProducerBuilder<TMessage> ChunkMaxMessageSize<TMessage>(this IProducerBuilder<TMessage> builder, int chunkMaxMessageSize)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(ChunkMaxMessageSize)).ChunkMaxMessageSize(chunkMaxMessageSize);

    /// <summary>
    /// Set the crypto key reader used to read the public keys that encrypt the data key. This is required if encryption keys are added.
    /// </summary>
    public static IProducerBuilder<TMessage> CryptoKeyReader<TMessage>(this IProducerBuilder<TMessage> builder, ICryptoKeyReader cryptoKeyReader)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(CryptoKeyReader)).CryptoKeyReader(cryptoKeyReader);

    /// <summary>
    /// Add the name of a public key that encrypts the data key. The messages are encrypted if at least one key name is added, and a crypto key reader is set. This is optional.
    /// </summary>
    /// <remarks>
    /// The name may not be null or blank. Adding the same name again does nothing.
    /// </remarks>
    public static IProducerBuilder<TMessage> EncryptionKey<TMessage>(this IProducerBuilder<TMessage> builder, string keyName)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(EncryptionKey)).EncryptionKey(keyName);

    /// <summary>
    /// Set the action to take when encryption fails. The default is 'Fail'.
    /// </summary>
    public static IProducerBuilder<TMessage> CryptoFailureAction<TMessage>(this IProducerBuilder<TMessage> builder, ProducerCryptoFailureAction cryptoFailureAction)
        => ExtendedApi.Require<ProducerBuilder<TMessage>>(builder, nameof(builder), nameof(CryptoFailureAction)).CryptoFailureAction(cryptoFailureAction);
}
