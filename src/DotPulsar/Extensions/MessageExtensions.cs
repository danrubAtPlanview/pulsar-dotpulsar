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
using DotPulsar.Internal.Abstractions;

/// <summary>
/// Extensions for IMessage.
/// </summary>
public static class MessageExtensions
{
    /// <summary>
    /// The encryption context if the message was encrypted by the producer, otherwise null. It is also null for an IMessage that DotPulsar did not create.
    /// </summary>
    public static EncryptionContext? GetEncryptionContext(this IMessage message)
        => message is IExtendedMessage extendedMessage ? extendedMessage.EncryptionContext : null;

    /// <summary>
    /// Get the conversation id.
    /// </summary>
    public static string? GetConversationId(this IMessage message)
    {
        if (message.Properties.TryGetValue(Constants.ConversationId, out var conversationId))
            return conversationId;

        return null;
    }
}
