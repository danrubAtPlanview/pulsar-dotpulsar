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

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/ConsumerCryptoFailureAction.java
/// <summary>
/// The action a consumer or reader takes when a message cannot be decrypted.
/// </summary>
public enum ConsumerCryptoFailureAction : byte
{
    /// <summary>
    /// Do not deliver the message and do not acknowledge it. This is the default.
    /// </summary>
    Fail = 0,

    /// <summary>
    /// Acknowledge the message (with validation error 'DecryptionError') and do not deliver it.
    /// </summary>
    Discard = 1,

    /// <summary>
    /// Deliver the encrypted (and possibly compressed) payload. 'IMessage.EncryptionContext' has the information needed to decrypt it.
    /// </summary>
    Consume = 2
}
