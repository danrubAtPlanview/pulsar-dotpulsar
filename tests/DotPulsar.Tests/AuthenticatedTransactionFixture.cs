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

using DotPulsar.Abstractions;
using System.Net;
using System.Text;
using System.Text.Json;
using Testcontainers.Pulsar;

// A broker that checks the roles of its clients, like the one of the Java test AuthenticatedTransactionProducerConsumerTest. The roles are the subjects of the tokens:
// - 'admin' is a super user (the infrastructure of the fixture is a super user too)
// - 'client' owns the transactions of the tests, and may use the namespace 'tnx/ns1'
// - 'client2' is a role that may only look up the transaction coordinator and does not own any transaction
// Testcontainers.Pulsar already enables the authentication and the authorization with the token provider (see TransactionFixtureBase).
public sealed class AuthenticatedTransactionFixture : TransactionFixtureBase
{
    public const string AdminRole = "admin";
    public const string ClientRole = "client";
    public const string OtherClientRole = "client2";
    public const string SystemNamespace = "pulsar/system";

    // The key of the broker is created when the container starts, so the tokens are created in the container. The environment variable of the key is set by Testcontainers.Pulsar.
    private const string TokenSecretKey = "$PULSAR_PREFIX_tokenSecretKey";

    public AuthenticatedTransactionFixture() : base(true) { }

    // The settings of the roles are not the environment variable 'superUserRoles', because Testcontainers.Pulsar uses that one as the subject of the token of the broker.
    protected override PulsarBuilder ConfigureBroker(PulsarBuilder builder)
        => builder.WithEnvironment("PULSAR_PREFIX_superUserRoles", $"{SuperUser},{AdminRole}");

    // Upstream: AuthenticatedTransactionProducerConsumerTest#setup, #grantTxnLookupToRole
    protected override async Task OnClusterStarted(CancellationToken cancellationToken)
    {
        await GrantNamespacePermission(SystemNamespace, ClientRole, ["consume"], cancellationToken);
        await GrantNamespacePermission(Namespace, ClientRole, ["produce", "consume", "functions", "sources", "sinks", "packages"], cancellationToken);
        await GrantNamespacePermission(SystemNamespace, OtherClientRole, ["consume"], cancellationToken);
    }

    // Creates a token of one hour for the role. The authentication of a client is a token like this (Java: Jwts.builder().setSubject(subject).setExpiration(one hour))
    public async Task<string> CreateToken(string subject, CancellationToken cancellationToken)
    {
        var result = await ExecInPulsar($"bin/pulsar tokens create --secret-key {TokenSecretKey} --subject {subject} --expiry-time 1h", cancellationToken);

        if (result.ExitCode != 0)
            throw new Exception($"Could not create the token of '{subject}': {result.Stderr}");

        return result.Stdout.Trim();
    }

    // A client that authenticates as the role of the token
    public IPulsarClient CreateClientForRole(string token)
        => PulsarClient
        .Builder()
        .Authentication(AuthenticationFactory.Token(token))
        .EnableTransaction(true)
        .ServiceUrl(ServiceUrl)
        .Build();

    // A client that does not authenticate (Java: PulsarClient.builder().serviceUrl(url).enableTransaction(true).build())
    public IPulsarClient CreateClientWithoutAuthentication()
        => PulsarClient
        .Builder()
        .EnableTransaction(true)
        .ServiceUrl(ServiceUrl)
        .Build();

    // GET /admin/v3/transactions/coordinatorStats: the broker answers 404 while it has not loaded the transaction coordinator
    public async Task<bool> IsTransactionCoordinatorLoaded(int coordinatorId, CancellationToken cancellationToken)
        => await GetStatusCode($"/admin/v3/transactions/coordinatorStats?coordinatorId={coordinatorId}", cancellationToken) == HttpStatusCode.OK;

    // GET /admin/v2/namespaces/{namespace}/permissions: the actions of every role
    public async Task<JsonElement> GetNamespacePermissions(string @namespace, CancellationToken cancellationToken)
    {
        var path = $"/admin/v2/namespaces/{@namespace}/permissions";

        using var httpClient = CreateAdminClient();
        using var response = await httpClient.GetAsync(path, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"GET {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var json = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        return json.RootElement.Clone();
    }

    // POST /admin/v2/namespaces/{namespace}/permissions/{role}
    public async Task GrantNamespacePermission(string @namespace, string role, string[] actions, CancellationToken cancellationToken)
    {
        var path = $"/admin/v2/namespaces/{@namespace}/permissions/{Uri.EscapeDataString(role)}";
        var json = $"[{string.Join(",", actions.Select(action => $"\"{action}\""))}]";

        using var httpClient = CreateAdminClient();
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await httpClient.PostAsync(path, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"POST {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
    }
}
