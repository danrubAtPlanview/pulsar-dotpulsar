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

// A broker that writes the index of the pending ack log of a subscription after every entry and that moves the mark delete position of the cursors often. The Java
// test TransactionTest#testPendingAckMarkDeletePosition sets the two settings on the running broker, which can not be done here because they are not dynamic.
public sealed class TransactionPendingAckLogIndexFixture : TransactionFixture
{
    protected override PulsarBuilder ConfigureBroker(PulsarBuilder builder)
        => builder
        .WithEnvironment("PULSAR_PREFIX_transactionPendingAckLogIndexMinLag", "1")
        .WithEnvironment("PULSAR_PREFIX_managedLedgerDefaultMarkDeleteRateLimit", "5");
}
