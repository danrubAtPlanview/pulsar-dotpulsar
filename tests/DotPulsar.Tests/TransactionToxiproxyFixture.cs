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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Pulsar;
using Toxiproxy.Net;

// A broker with a transaction coordinator that the clients reach through Toxiproxy, so a test can cut the connection to it and restore it, as the tests of the
// IntegrationFixture do for a broker without a transaction coordinator. The admin API is reached directly.
public sealed class TransactionToxiproxyFixture : TransactionFixture
{
    private const int PulsarPort = 6650;
    private const int ToxiProxyControlPort = 8474;
    private const int ToxiProxyPort = 15124;
    private readonly INetwork _network;
    private readonly IContainer _toxiProxy;
    private Connection _toxiProxyConnection;
    private Proxy _toxiProxyPulsarProxy;

    public TransactionToxiproxyFixture()
    {
        _network = new NetworkBuilder()
            .WithName(Guid.NewGuid().ToString("D"))
            .Build();

        _toxiProxy = new ContainerBuilder("ghcr.io/shopify/toxiproxy:2.12.0")
            .WithPortBinding(ToxiProxyControlPort, true)
            .WithPortBinding(ToxiProxyPort, true)
            .WithHostname("toxiproxy")
            .WithNetwork(_network)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(strategy => strategy.ForPath("/version").ForPort(ToxiProxyControlPort)))
            .Build();

        _toxiProxyConnection = new Connection();
        _toxiProxyPulsarProxy = new Proxy();
    }

    protected override PulsarBuilder ConfigureBroker(PulsarBuilder builder)
        => builder
        .WithNetwork(_network)
        .WithHostname("pulsar");

    public override async ValueTask InitializeAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        await _network.CreateAsync(cts.Token);
        Current.SendDiagnosticMessage("Starting Toxiproxy");
        await _toxiProxy.StartAsync(cts.Token);

        await base.InitializeAsync();

        _toxiProxyConnection = new Connection(_toxiProxy.Hostname, _toxiProxy.GetMappedPublicPort(ToxiProxyControlPort));
        _toxiProxyPulsarProxy = new Proxy
        {
            Name = "localToPulsarCluster",
            Enabled = true,
            Listen = $"0.0.0.0:{ToxiProxyPort}",
            Upstream = $"pulsar:{PulsarPort}"
        };
        await _toxiProxyConnection.Client().AddAsync(_toxiProxyPulsarProxy);

        ServiceUrl = new Uri($"pulsar://{_toxiProxy.Hostname}:{_toxiProxy.GetMappedPublicPort(ToxiProxyPort)}");
        Current.SendDiagnosticMessage("Toxiproxy successfully mapped connections between host and the Pulsar Cluster. You can connect with: " + ServiceUrl);
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _toxiProxy.DisposeAsync();
        _toxiProxyConnection.Dispose();
        await _network.DisposeAsync();
    }

    public async Task<IAsyncDisposable> DisableThePulsarConnection()
    {
        _toxiProxyPulsarProxy.Enabled = false;
        await _toxiProxyPulsarProxy.UpdateAsync();
        return new Releaser(_toxiProxyPulsarProxy);
    }

    private sealed class Releaser : IAsyncDisposable
    {
        private readonly Proxy _proxy;

        public Releaser(Proxy proxy) => _proxy = proxy;

        public async ValueTask DisposeAsync()
        {
            _proxy.Enabled = true;
            await _proxy.UpdateAsync();
        }
    }
}
