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
/// Extensions for IReaderBuilder.
/// </summary>
public static class ReaderBuilderExtensions
{
    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IReaderBuilder<TMessage> StateChangedHandler<TMessage>(
        this IReaderBuilder<TMessage> builder,
        Action<ReaderStateChanged> handler)
    {
        void forwarder(ReaderStateChanged readerStateChanged, CancellationToken _) => handler(readerStateChanged);
        builder.StateChangedHandler(new ActionStateChangedHandler<ReaderStateChanged>(forwarder, default));
        return builder;
    }

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IReaderBuilder<TMessage> StateChangedHandler<TMessage>(
        this IReaderBuilder<TMessage> builder,
        Action<ReaderStateChanged, CancellationToken> handler,
        CancellationToken cancellationToken = default)
    {
        builder.StateChangedHandler(new ActionStateChangedHandler<ReaderStateChanged>(handler, cancellationToken));
        return builder;
    }

    /// <summary>
    /// Register a state changed handler.
    /// </summary>
    public static IReaderBuilder<TMessage> StateChangedHandler<TMessage>(
        this IReaderBuilder<TMessage> builder,
        Func<ReaderStateChanged, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        builder.StateChangedHandler(new FuncStateChangedHandler<ReaderStateChanged>(handler, cancellationToken));
        return builder;
    }

    /// <summary>
    /// Use the DefaultCryptoKeyReader with a private key URI that is always used to decrypt.
    /// </summary>
    public static IReaderBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(
        this IReaderBuilder<TMessage> builder,
        string privateKey)
        => builder.CryptoKeyReader(new DefaultCryptoKeyReader(null, privateKey));

    /// <summary>
    /// Use the DefaultCryptoKeyReader with private key URIs by key name.
    /// </summary>
    public static IReaderBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(
        this IReaderBuilder<TMessage> builder,
        IReadOnlyDictionary<string, string> privateKeys)
        => builder.CryptoKeyReader(new DefaultCryptoKeyReader(null, null, null, privateKeys));

    /// <summary>
    /// Set the max number of chunked messages that are being assembled at the same time. When it is exceeded, the oldest is dropped. 0 means no limit. The default is 10.
    /// </summary>
    public static IReaderBuilder<TMessage> MaxPendingChunkedMessage<TMessage>(this IReaderBuilder<TMessage> builder, uint maxPendingChunkedMessage)
        => ExtendedApi.Require<ReaderBuilder<TMessage>>(builder, nameof(builder), nameof(MaxPendingChunkedMessage)).MaxPendingChunkedMessage(maxPendingChunkedMessage);

    /// <summary>
    /// Whether to acknowledge (instead of redeliver) the chunks of the oldest pending chunked message when 'MaxPendingChunkedMessage' is exceeded. The default is 'false'.
    /// </summary>
    public static IReaderBuilder<TMessage> AutoAckOldestChunkedMessageOnQueueFull<TMessage>(this IReaderBuilder<TMessage> builder, bool autoAckOldestChunkedMessageOnQueueFull)
        => ExtendedApi.Require<ReaderBuilder<TMessage>>(builder, nameof(builder), nameof(AutoAckOldestChunkedMessageOnQueueFull)).AutoAckOldestChunkedMessageOnQueueFull(autoAckOldestChunkedMessageOnQueueFull);

    /// <summary>
    /// Set the time after which an incomplete chunked message is dropped. TimeSpan.Zero disables expiry. The default is 1 minute.
    /// </summary>
    public static IReaderBuilder<TMessage> ExpireTimeOfIncompleteChunkedMessage<TMessage>(this IReaderBuilder<TMessage> builder, TimeSpan expireTimeOfIncompleteChunkedMessage)
        => ExtendedApi.Require<ReaderBuilder<TMessage>>(builder, nameof(builder), nameof(ExpireTimeOfIncompleteChunkedMessage)).ExpireTimeOfIncompleteChunkedMessage(expireTimeOfIncompleteChunkedMessage);

    /// <summary>
    /// Set the crypto key reader used to read the private keys that decrypt the messages. This is optional.
    /// </summary>
    public static IReaderBuilder<TMessage> CryptoKeyReader<TMessage>(this IReaderBuilder<TMessage> builder, ICryptoKeyReader cryptoKeyReader)
        => ExtendedApi.Require<ReaderBuilder<TMessage>>(builder, nameof(builder), nameof(CryptoKeyReader)).CryptoKeyReader(cryptoKeyReader);

    /// <summary>
    /// Set the action to take when a message cannot be decrypted. The default is 'Fail'.
    /// </summary>
    public static IReaderBuilder<TMessage> CryptoFailureAction<TMessage>(this IReaderBuilder<TMessage> builder, ConsumerCryptoFailureAction cryptoFailureAction)
        => ExtendedApi.Require<ReaderBuilder<TMessage>>(builder, nameof(builder), nameof(CryptoFailureAction)).CryptoFailureAction(cryptoFailureAction);
}
