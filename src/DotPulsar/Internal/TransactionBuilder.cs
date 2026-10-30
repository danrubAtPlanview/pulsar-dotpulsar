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
using DotPulsar.Exceptions;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionBuilderImpl.java
public sealed class TransactionBuilder : ITransactionBuilder
{
    private readonly IPulsarClient _pulsarClient;
    private TimeSpan _timeout;

    public TransactionBuilder(IPulsarClient pulsarClient)
    {
        _pulsarClient = pulsarClient;
        _timeout = TransactionOptions.DefaultTimeout;
    }

    public ITransactionBuilder Timeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    // Upstream: TransactionBuilderImpl#build (C# deviation: a timeout that is not greater than zero is rejected, because the broker uses its default for a timeout of zero and a negative timeout would wrap around when it is converted to an unsigned number)
    public async ValueTask<ITransaction> Create(CancellationToken cancellationToken)
    {
        if (_timeout <= TimeSpan.Zero)
            throw new ConfigurationException("TransactionOptions.Timeout must be greater than zero");

        var options = new TransactionOptions { Timeout = _timeout };
        return await _pulsarClient.CreateTransaction(options, cancellationToken).ConfigureAwait(false);
    }
}
