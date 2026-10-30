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

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/ProducerCryptoFailureAction.java
/// <summary>
/// The action a producer takes when encryption fails.
/// </summary>
public enum ProducerCryptoFailureAction : byte
{
    /// <summary>
    /// Fail the send. This is the default.
    /// </summary>
    Fail = 0,

    /// <summary>
    /// Ignore the failure and send the message unencrypted.
    /// </summary>
    Send = 1
}
