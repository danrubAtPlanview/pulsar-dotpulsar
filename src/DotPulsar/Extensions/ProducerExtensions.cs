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
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal;

/// <summary>
/// Extensions for IProducer.
/// </summary>
public static class ProducerExtensions
{
    /// <summary>
    /// Get a builder that can be used to configure and build a Message.
    /// </summary>
    public static IMessageBuilder<TMessage> NewMessage<TMessage>(this IProducer<TMessage> producer)
        => new MessageBuilder<TMessage>(producer);

    /// <summary>
    /// Wait for the state to change to a specific state.
    /// </summary>
    /// <returns>
    /// The current state.
    /// </returns>
    /// <remarks>
    /// If the state change to a final state, then all awaiting tasks will complete.
    /// </remarks>
    public static async ValueTask<ProducerStateChanged> StateChangedTo(this IProducer producer, ProducerState state, CancellationToken cancellationToken = default)
    {
        var currentState = await producer.State.OnStateChangeTo(state, cancellationToken).ConfigureAwait(false);
        return new ProducerStateChanged(producer, currentState);
    }

    /// <summary>
    /// Wait for the state to change to a specific state with a delay.
    /// </summary>
    /// <returns>
    /// The current state.
    /// </returns>
    /// <remarks>
    /// If the state change to a final state, then all awaiting tasks will complete.
    /// </remarks>
    public static async ValueTask<ProducerStateChanged> StateChangedTo(this IProducer producer, ProducerState state, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        var currentState = await producer.State.OnStateChangeTo(state, delay, cancellationToken).ConfigureAwait(false);
        return new ProducerStateChanged(producer, currentState);
    }

    /// <summary>
    /// Wait for the state to change from a specific state.
    /// </summary>
    /// <returns>
    /// The current state.
    /// </returns>
    /// <remarks>
    /// If the state change to a final state, then all awaiting tasks will complete.
    /// </remarks>
    public static async ValueTask<ProducerStateChanged> StateChangedFrom(this IProducer producer, ProducerState state, CancellationToken cancellationToken = default)
    {
        var currentState = await producer.State.OnStateChangeFrom(state, cancellationToken).ConfigureAwait(false);
        return new ProducerStateChanged(producer, currentState);
    }

    /// <summary>
    /// Wait for the state to change from a specific state with a delay.
    /// </summary>
    /// <returns>
    /// The current state.
    /// </returns>
    /// <remarks>
    /// If the state change to a final state, then all awaiting tasks will complete.
    /// </remarks>
    public static async ValueTask<ProducerStateChanged> StateChangedFrom(this IProducer producer, ProducerState state, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        var currentState = await producer.State.OnStateChangeFrom(state, delay, cancellationToken).ConfigureAwait(false);
        return new ProducerStateChanged(producer, currentState);
    }

    /// <summary>
    /// Send all batched messages immediately and wait for the broker to acknowledge the last message that was sent before this call.
    /// </summary>
    /// <exception cref="Exception">The exception of the last message that was sent before this call, if sending it failed.</exception>
    public static ValueTask Flush(this IProducer producer, CancellationToken cancellationToken = default)
        => ExtendedApi.Require<IExtendedProducer>(producer, nameof(producer), nameof(Flush)).Flush(cancellationToken);
}
