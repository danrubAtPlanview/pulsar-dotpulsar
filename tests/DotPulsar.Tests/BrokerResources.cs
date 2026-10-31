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

// Every collection of integration tests has its own Pulsar broker, because the collections need different broker settings, and xUnit runs the collections in
// parallel. The apachepulsar/pulsar image reserves a 2 GB heap for each broker (PULSAR_MEM is -Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g), so the brokers of all
// the collections together do not fit in a Docker VM of 16 GB. Two things keep the memory in check:
// - at most DOTPULSAR_TESTS_MAX_BROKERS (default 2) brokers run at the same time. A collection that has to wait for a broker slot starts when another collection is done
// - a broker gets DOTPULSAR_TESTS_BROKER_MEMORY (default -Xms512m -Xmx1g -XX:MaxDirectMemorySize=1g) as PULSAR_MEM
public static class BrokerResources
{
    private const string MaxBrokersVariable = "DOTPULSAR_TESTS_MAX_BROKERS";
    private const string BrokerMemoryVariable = "DOTPULSAR_TESTS_BROKER_MEMORY";
    private const string DefaultBrokerMemory = "-Xms512m -Xmx1g -XX:MaxDirectMemorySize=1g";
    private const int DefaultMaxBrokers = 2;
    private static readonly SemaphoreSlim _slots = new(GetMaxBrokers());

    // Waits for a broker slot. The wait is not limited, because a collection can only start when the collections in front of it are done, and the slot must be disposed when the broker is stopped.
    public static async Task<IDisposable> WaitForSlot(CancellationToken cancellationToken = default)
    {
        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new Slot();
    }

    public static PulsarBuilder WithBrokerMemory(this PulsarBuilder builder)
    {
        var memory = Environment.GetEnvironmentVariable(BrokerMemoryVariable);
        return builder.WithEnvironment("PULSAR_MEM", string.IsNullOrWhiteSpace(memory) ? DefaultBrokerMemory : memory);
    }

    private static int GetMaxBrokers()
    {
        var value = Environment.GetEnvironmentVariable(MaxBrokersVariable);
        return int.TryParse(value, out var maxBrokers) && maxBrokers > 0 ? maxBrokers : DefaultMaxBrokers;
    }

    private sealed class Slot : IDisposable
    {
        private int _isDisposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
                _slots.Release();
        }
    }
}
