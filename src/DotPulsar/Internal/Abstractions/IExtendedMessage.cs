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

/// <summary>
/// The members that were added to IMessage after it was released. Adding them to IMessage would break every implementation of it outside of DotPulsar, so they are extension methods of IMessage
/// ('MessageExtensions') that read this interface, which every message that DotPulsar creates implements.
/// </summary>
public interface IExtendedMessage
{
    EncryptionContext? EncryptionContext { get; }
}
