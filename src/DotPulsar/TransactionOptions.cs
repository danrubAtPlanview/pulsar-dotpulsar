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

/// <summary>
/// The transaction building options.
/// </summary>
public sealed class TransactionOptions
{
    /// <summary>
    /// The default transaction timeout.
    /// </summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Initializes a new instance using the default values.
    /// </summary>
    public TransactionOptions()
    {
        Timeout = DefaultTimeout;
    }

    /// <summary>
    /// Set the transaction timeout. The transaction will be aborted by the broker if it is not committed or aborted within this time. The default is 1 minute.
    /// </summary>
    public TimeSpan Timeout { get; set; }
}
