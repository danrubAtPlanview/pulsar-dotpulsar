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

namespace DotPulsar.Tests;

using Testcontainers.Pulsar;

// A broker where the transaction coordinator allows two active transactions at a time (the Java test TransactionCoordinatorConfigTest sets the same limit)
public sealed class TransactionMaxActiveFixture : TransactionFixture
{
    public const int MaxActiveTransactions = 2;

    protected override PulsarBuilder ConfigureBroker(PulsarBuilder builder)
        => builder.WithEnvironment("PULSAR_PREFIX_maxActiveTransactionsPerCoordinator", MaxActiveTransactions.ToString());
}
