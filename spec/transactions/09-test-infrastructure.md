# 09 — Test infrastructure

## Overview: all fixtures

| Fixture (file) | Collection | Broker config | Used by |
|---|---|---|---|
| `TransactionFixture` (`tests/DotPulsar.Tests/TransactionFixture.cs`) | `"Transaction"` | See §4.1 below: TC enabled, dedup on, batch-index ack **on**, the `TransactionTestBase` extras, tenant `tnx`, namespaces `tnx/ns1` (4 bundles) and `public/txn` (10 bundles) | almost every integration parity test |
| `TransactionWithoutBatchIndexAckFixture` | `"TransactionWithoutBatchIndexAck"` | as above, but `acknowledgmentAtBatchIndexLevelEnabled=false` | `TransactionEndToEndWithoutBatchIndexAckTests` ([08-tests/tests-e2e](08-tests/tests-e2e.md) §1) |
| `TransactionPendingAckLogIndexFixture` / `TransactionPendingAckReplayFixture` | `"TransactionPendingAckLogIndex"` / `"TransactionPendingAckReplay"` | `transactionPendingAckLogIndexMinLag` and the mark-delete rate limit, which are not dynamic: lag 1 / rate 5 and lag 4002 / rate 10 | [08-tests/tests-broker](08-tests/tests-broker.md) §2.2 (T22, A4) |
| `AuthenticatedTransactionFixture` | `"AuthenticatedTransaction"` | token auth with roles `admin`, `client`, `client2` and namespace grants | [08-tests/tests-broker](08-tests/tests-broker.md) §2.3 |
| `TransactionMaxActiveFixture` | `"TransactionMaxActive"` | `maxActiveTransactionsPerCoordinator=2`, 1 TC | `TransactionCoordinatorConfigTest#testMaxActiveTxn` ([08-tests/tests-client](08-tests/tests-client.md)) |
| existing `IntegrationFixture` | `"Integration"` | TC **disabled** | "broker without TC" tests |

Extra fixtures should derive from a common `abstract class TransactionFixtureBase` that holds the container setup
and helpers. Each subclass overrides only `ConfigureBroker(PulsarBuilder)`.

Every fixture adds one container start, roughly 30–60 s. CI runs all collections sequentially. Raise the xUnit
`maxParallelThreads` setting only if CI time becomes a problem.

## Helper catalogue (consolidated)

Every helper lives on `TransactionFixtureBase`. Its signature and implementation are specified in one of:

- §4.1 below: `CreateTopicName`, `CreateTopic`, `CreatePartitionedTopic`, `GetTopicStats`, `GetInternalStats`,
  `GetTransactionMetadata`, `GetStatusCode`, `ReceiveOrNull`, `CreateClient`, `ServiceUrl`, `AdminUrl`, `Authentication`.
- [08-tests/tests-e2e §5](08-tests/tests-e2e.md): `GetConsumerUnackedMessages`, `CreateSubscription`, `DeleteTopic`,
  `TransactionMetadataExists`, `GetTransactionTimeoutTotal`, `IsMarkDeleteAtEnd`.
- [08-tests/tests-broker §2.4-2.5](08-tests/tests-broker.md): the transaction-buffer / pending-ack admin endpoints,
  `HasMessageAvailable`, `Observe`, metrics, compaction triggers, and namespace/permission helpers.

If two drafts define the same helper with different signatures, use the more general one and update the call sites.

## Cross-cutting rules for integration tests

- **Per-test timeout:** the `_cts` is 3 minutes for transaction integration test classes (the repo default is
  1 minute). Long upstream tests need it ([08-tests/tests-e2e §7](08-tests/tests-e2e.md)).
- **`receive(5, SECONDS)` returning null** becomes `await fixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), ct)`.
  This preserves the upstream 5 s waits.
- **Unique names:** use `TransactionFixture.CreateTopicName("<java-local-name>")`. It keeps the Java topic name as a
  readable prefix and appends a GUID, because one container is shared by the whole collection.
- **Java producers batch by default; DotPulsar does not.** Ported tests call `.EnableBatching(true)` wherever Java
  relies on its default ([12 D1](12-risks-open-questions.md)).
- **Java `sendAsync` loops** become the ordered `SendAsync` helper over `ISendChannel`
  ([08-tests/tests-e2e §2.2](08-tests/tests-e2e.md)). Producer requirement R-OP-SYNC/R-ORDER
  ([producer-integration](05-components/producer-integration.md)) guarantees the semantics.
- **Broker image version:** the image is pinned to `apachepulsar/pulsar:4.0.9`, the same image as `IntegrationFixture`.
  Some upstream tests target fixes that exist only on Pulsar master
  ([08-tests/tests-broker](08-tests/tests-broker.md) OD-10).
  - If a test fails on 4.0.9 **because of a broker bug fixed upstream**, mark it
    `[Fact(Skip = "Requires broker fix <PR/commit> not in apachepulsar/pulsar:4.0.9")]`.
  - Do **not** bump the shared image as part of a feature WP.
  - Bumping the image is its own PR, which then removes those skips.

---

## Detailed specification: `TransactionFixture`

Scope: the xUnit collection fixture(s) used by every ported Java transaction integration test
(`tests/DotPulsar.Tests/Transactions/*Tests.cs`, `[Collection("Transaction")]`).
No repo files were modified. Sources were read at the tags cited below.

Pinned references used in this draft:
- DotPulsar HEAD e8f2ba2: `tests/DotPulsar.Tests/DotPulsar.Tests.csproj`, `IntegrationFixture.cs`, `IntegrationCollection.cs`.
- testcontainers-dotnet tag `4.15.0` (matches `DotPulsar.Tests.csproj:31` `Testcontainers.Pulsar` `Version="4.15.0"`):
  `src/Testcontainers.Pulsar/PulsarBuilder.cs`, `PulsarContainer.cs`.
  NOTE: the local NuGet cache only has `testcontainers.pulsar/4.16.0` (no 4.15.0 restored yet), so the
  source at tag 4.15.0 was used (cited as `tc:<file> (Lx-Ly)`).
- apache/pulsar tag `v4.0.9` (commit 4128e258d515333b143c43a637bc5630ce658abb) = the broker image the fixture
  runs (`apachepulsar/pulsar:4.0.9`, `IntegrationFixture.cs:57`). Cited as `p409:<path> (Lx)`.
  (`conf/standalone.conf`, `PulsarStandalone.java`, `docker/pulsar/scripts/apply-config-from-env.py`,
  `ServiceConfiguration.java`, `admin/v3/Transactions.java`, `admin/impl/TransactionsBase.java`.)
  `apply-config-from-env.py` and `PulsarStandalone.java` are byte-identical on `branch-4.0` head (f539c3f).
- Java reference SHA f3846f4811b6a7d98de40a3be2caff1fe288e026 cited as `java:`.

---

### 1. How Testcontainers.Pulsar 4.15.0 starts the container

1. `PulsarBuilder.Init()` (tc:PulsarBuilder.cs L123-133): binds 6650/8080, `WithFunctionsWorker(false)`,
   entrypoint `/bin/sh -c`, command = busy-wait until `/testcontainers.sh` exists, then runs it;
   startup callback `CopyStartupScriptAsync`.
2. `PulsarContainer.CopyStartupScriptAsync` (tc:PulsarContainer.cs L96-120) writes `/testcontainers.sh`:
   ```
   #!/bin/bash
   # only if AuthenticationEnabled == true  (L102-109):
   bin/pulsar tokens create-secret-key --output /tmp/secret.key
   export brokerClientAuthenticationParameters=token:$(bin/pulsar tokens create --secret-key $PULSAR_PREFIX_tokenSecretKey --subject $superUserRoles)
   export CLIENT_PREFIX_authParams=$brokerClientAuthenticationParameters
   bin/apply-config-from-env.py conf/standalone.conf
   bin/apply-config-from-env.py --prefix CLIENT_PREFIX_ conf/client.conf
   # always (L111-117):
   bin/pulsar standalone --no-functions-worker --no-stream-storage
   ```
   **Consequence (critical):** `apply-config-from-env.py` runs **only when `.WithAuthentication()` is used**
   (tc:PulsarContainer.cs L102). Without authentication every broker env var is silently ignored and
   `bin/pulsar standalone` runs with stock `conf/standalone.conf`. The transaction fixture MUST call
   `.WithAuthentication()` (it does, mirroring `IntegrationFixture.cs:58`).
3. `WithAuthentication()` (tc:PulsarBuilder.cs L83-87, env map L153-167) adds env:
   `authenticateOriginalAuthData=false`, `authenticationEnabled=true`, `authorizationEnabled=true`,
   `authenticationProviders=...AuthenticationProviderToken`, `brokerClientAuthenticationPlugin=...AuthenticationToken`,
   `CLIENT_PREFIX_authPlugin=...`, `PULSAR_PREFIX_authenticationRefreshCheckSeconds=5`,
   `PULSAR_PREFIX_tokenSecretKey=file:///tmp/secret.key`, `superUserRoles=test-user`.
4. Readiness (tc:PulsarBuilder.cs L100-120, L169-216): wait until `/tmp/secret.key` exists, then HTTP
   `GET /admin/v2/namespaces/public/default` on 8080 with a 1h token returns 2xx.

### apply-config-from-env.py semantics (p409:docker/pulsar/scripts/apply-config-from-env.py)
- Pass 1 (L71-78): for **every** env var whose name equals a key already present in the conf file
  (including commented `#key=value` lines, L62-65), the line is replaced with `key=value`.
- Pass 2 (L85-99): for every env var starting with the prefix (default `PULSAR_PREFIX_`, L31), the prefix
  is stripped; the key is **appended** if absent (L94-96) or **updated** if present (L97-99).
- So: plain env names work only for keys that exist in `standalone.conf`; `PULSAR_PREFIX_<key>` works for
  any key. Recommendation: **always use `PULSAR_PREFIX_`** for fixture overrides (uniform, cannot silently
  no-op because of a missing key).

### Relevant keys in p409:conf/standalone.conf and ServiceConfiguration defaults

| Key | standalone.conf v4.0.9 | ServiceConfiguration v4.0.9 default | Java test value | Fixture value |
|---|---|---|---|---|
| `transactionCoordinatorEnabled` | L1268 `=false` | L3580 `false` | `true` (java:pulsar-broker/.../transaction/TransactionTestBase.java L161) | `true` |
| `brokerDeduplicationEnabled` | L176 `=false` | L866 `false` | `true` (TransactionTestBase.java L162) | `true` |
| `acknowledgmentAtBatchIndexLevelEnabled` | **absent** (only mentioned inside a comment at L864, which the script does not parse as this key) | L433 **`false`** | Java master default is **`true`** (java:pulsar-broker-common/.../ServiceConfiguration.java L501); `TransactionEndToEndWithoutBatchIndexAckTest` sets `false` (L35) | `true` (main) / `false` (variant) — **must use `PULSAR_PREFIX_`** |
| `transactionBufferSnapshotMaxTransactionCount` | L1274 `=1000` | — | `2` (TransactionTestBase.java L163) | `2` |
| `transactionBufferSnapshotMinTimeInMillis` | L1279 `=5000` | — | `2000` (TransactionTestBase.java L164) | `2000` |
| `dispatcherRetryBackoffInitialTimeInMs` | L310 `=1` | — | `0` (TransactionTestBase.java L166) | `0` |
| `dispatcherRetryBackoffMaxTimeInMs` | L315 `=10` | — | `0` (TransactionTestBase.java L167) | `0` |
| `activeConsumerFailoverDelayTimeMillis` | L149 `=1000` | — | `0` (TransactionTestBase.java L149; MockedPulsarServiceBaseTest.java L596) | `0` |
| `systemTopicEnabled` | L569 `=true` | L1651 `true` | default | unchanged (needed by txn buffer snapshots) |
| `topicLevelPoliciesEnabled` | L580 `=true` | — | default | unchanged |
| `allowAutoTopicCreationType` | L1254 `=non-partitioned` | — | `NON_PARTITIONED` (L153) | unchanged |
| `brokerDeleteInactiveTopicsEnabled` | L130 `=true` | true | default true | see Open decisions (recommend `false`) |
| `superUserRoles` | L614 `=` | — | n/a | `test-user` (set by `WithAuthentication`) |
| `clusterName` | L89 `=standalone` | — | `test` | unchanged (`standalone`) |

**Important finding:** `acknowledgmentAtBatchIndexLevelEnabled` default differs between the Java reference
(master: `true`) and the 4.0.9 broker (`false`). Without an explicit `PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled=true`
the "main" fixture would silently behave like `TransactionEndToEndWithoutBatchIndexAckTest`.

### 2. Transaction coordinator bootstrap in standalone (p409:pulsar-broker/src/main/java/org/apache/pulsar/PulsarStandalone.java)

- L354 `broker.start()`; L359-360 create `public/default`; L362 create `pulsar/system` namespace (tenant `pulsar`);
  L363-371: **if `transactionCoordinatorEnabled`**, create partitioned topic metadata for
  `persistent://pulsar/system/transaction_coordinator_assign` with **1 partition** if absent
  (`SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN`, p409:pulsar-common/.../SystemTopicNames.java L67-68).
  => standalone has exactly one TC (`tc_id = 0`), topic `persistent://pulsar/system/transaction_coordinator_assign-partition-0`.
  (Java tests use 16 TCs, `TransactionEndToEndTest.java` L97/L102; accepted in spec decisions.)
- **Race:** Testcontainers' readiness probe (`/admin/v2/namespaces/public/default`) can succeed after L359-360
  but before L362-371 finished, and the TC metadata store itself is loaded lazily when the broker takes
  ownership of the bundle owning `transaction_coordinator_assign-partition-0` (or on the first
  `CommandTcClientConnectRequest`). Therefore the fixture must actively wait for TC readiness (see §4).
- Readiness probe choice: `GET /admin/v3/transactions/coordinatorStats?coordinatorId=0`
  (p409:.../admin/v3/Transactions.java L81 path; p409:.../admin/impl/TransactionsBase.java L105-118):
  calls `validateTopicOwnership(assign-partition-0)` (which makes the broker acquire ownership and hence
  load the TC), returns **404 "Transaction coordinator not found!"** while the store is not yet loaded, and
  returns `TransactionCoordinatorStats` JSON once loaded. Field `state` (pulsar-client-admin-api
  `TransactionCoordinatorStats.java` L30, v4.0.9) equals `"Ready"` when usable.
  Preceded by `GET /admin/v2/persistent/pulsar/system/transaction_coordinator_assign/partitions` until
  `{"partitions":1}`.

### 3. Authentication / authorization

- Token subject is `test-user` (tc:PulsarContainer.cs L51-60, `--subject PulsarBuilder.Username`; `Username = "test-user"`
  tc:PulsarBuilder.cs L18) and `superUserRoles=test-user` (tc:PulsarBuilder.cs L165; key present at
  p409 standalone.conf L614, so the plain env name is applied by pass 1). => the fixture's token **is a
  superuser**. The client can therefore `CommandLookupTopic` `persistent://pulsar/system/transaction_coordinator_assign-partition-0`,
  send `TcClientConnectRequest`/`NewTxn`/`EndTxn`, and call all `/admin/v3/transactions/*` endpoints
  (these require superuser/tenant-admin).
- `bin/pulsar-admin` inside the container is authenticated via `conf/client.conf` populated from
  `CLIENT_PREFIX_authPlugin` / `CLIENT_PREFIX_authParams` (tc:PulsarContainer.cs L106, L108), which is
  why `IntegrationFixture.CreateTopic` (`IntegrationFixture.cs:160-168`) works with auth enabled.
- Tenants created by the fixture must list `test-user` as admin role and `standalone` as allowed cluster.

### 4. Exact C# — new files

Placement: next to the existing fixture (`tests/DotPulsar.Tests/IntegrationFixture.cs`), namespace `DotPulsar.Tests`.
Style copied from `IntegrationFixture.cs` (Apache header, file-scoped namespace, usings inside namespace,
`Current.SendDiagnosticMessage` via `<Using Include="Xunit.TestContext" Static="true" />` at
`DotPulsar.Tests.csproj:15`, `/bin/bash -c bin/pulsar-admin ...` exec pattern from L160-185,
`JsonDocument` stats parsing from `Internal/ConsumerTests.cs:596-631`).
Differences vs IntegrationFixture: no Toxiproxy/network (no ported txn test needs connection fault
injection; see Open decisions), `ServiceUrl` = `GetBrokerAddress()` directly.

#### 4.1 `tests/DotPulsar.Tests/TransactionFixture.cs`

```csharp
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

using DotNet.Testcontainers.Containers;
using DotPulsar.Abstractions;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Testcontainers.Pulsar;

public class TransactionFixture : IAsyncLifetime
{
    public const string Tenant = "tnx";
    public const string Namespace = Tenant + "/ns1";
    public const string TransactionCoordinatorAssignTopic = "persistent://pulsar/system/transaction_coordinator_assign";
    private const string ClusterName = "standalone";
    private const string SuperUser = "test-user";
    private readonly CancellationTokenSource _cts;
    private readonly PulsarContainer _pulsarCluster;
    private string? _token;

    public TransactionFixture() : this(true) { }

    protected TransactionFixture(bool acknowledgmentAtBatchIndexLevelEnabled)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));

        _pulsarCluster = new PulsarBuilder("apachepulsar/pulsar:4.0.9")
            .WithAuthentication()
            .WithEnvironment("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
            .WithEnvironment("PULSAR_PREFIX_brokerDeduplicationEnabled", "true")
            .WithEnvironment("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", acknowledgmentAtBatchIndexLevelEnabled ? "true" : "false")
            .WithEnvironment("PULSAR_PREFIX_transactionBufferSnapshotMaxTransactionCount", "2")
            .WithEnvironment("PULSAR_PREFIX_transactionBufferSnapshotMinTimeInMillis", "2000")
            .WithEnvironment("PULSAR_PREFIX_dispatcherRetryBackoffInitialTimeInMs", "0")
            .WithEnvironment("PULSAR_PREFIX_dispatcherRetryBackoffMaxTimeInMs", "0")
            .WithEnvironment("PULSAR_PREFIX_activeConsumerFailoverDelayTimeMillis", "0")
            .WithEnvironment("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
            .Build();
    }

    public Uri ServiceUrl { get; private set; } = null!; // This will be set in `InitializeAsync`

    public Uri AdminUrl { get; private set; } = null!; // This will be set in `InitializeAsync`

    public AuthenticationHeaderValue AuthorizationHeader => new("Bearer", _token);

    public IAuthentication Authentication => AuthenticationFactory.Token(ct => ValueTask.FromResult(_token!));

    public HttpClient CreateAdminClient() => new()
    {
        BaseAddress = AdminUrl,
        DefaultRequestHeaders =
        {
            Authorization = AuthorizationHeader
        }
    };

    public IPulsarClient CreateClient(IHandleException? exceptionHandler = null, bool enableTransaction = true)
    {
        var builder = PulsarClient
            .Builder()
            .Authentication(Authentication)
            .EnableTransaction(enableTransaction)
            .ServiceUrl(ServiceUrl);

        if (exceptionHandler is not null)
            builder.ExceptionHandler(exceptionHandler);

        return builder.Build();
    }

    public async ValueTask DisposeAsync()
    {
        await _pulsarCluster.DisposeAsync();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    public async ValueTask InitializeAsync()
    {
        SubscribeToContainerEvents(_pulsarCluster, "Pulsar cluster");
        Current.SendDiagnosticMessage("Starting Pulsar Cluster with transactions enabled");
        await _pulsarCluster.StartAsync(_cts.Token);

        ServiceUrl = new Uri(_pulsarCluster.GetBrokerAddress());
        AdminUrl = new Uri(_pulsarCluster.GetServiceAddress());
        _token = (await _pulsarCluster.CreateAuthenticationTokenAsync(Timeout.InfiniteTimeSpan, _cts.Token)).TrimEnd();

        await WaitForTransactionCoordinator(_cts.Token);
        await CreateTenant(Tenant, _cts.Token);
        await CreateNamespace(Namespace, 4, _cts.Token);

        Current.SendDiagnosticMessage("The transaction coordinator is ready. You can connect with: " + ServiceUrl);
    }

    private async Task WaitForTransactionCoordinator(CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var partitionsResponse = await httpClient.GetAsync($"/admin/v2/{ToRestPath(TransactionCoordinatorAssignTopic)}/partitions", cancellationToken);
            if (partitionsResponse.IsSuccessStatusCode)
            {
                var partitions = await ReadJson(partitionsResponse, cancellationToken);
                if (partitions.GetProperty("partitions").GetInt32() > 0)
                {
                    using var statsResponse = await httpClient.GetAsync("/admin/v3/transactions/coordinatorStats?coordinatorId=0", cancellationToken);
                    if (statsResponse.IsSuccessStatusCode)
                    {
                        var stats = await ReadJson(statsResponse, cancellationToken);
                        if (stats.TryGetProperty("state", out var state) && state.GetString() == "Ready")
                            return;
                    }
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }
    }

    private void HandleClusterStateChange(string containerName, string state) => Current.SendDiagnosticMessage($"The {containerName} changed state to: {state}");

    private void SubscribeToContainerEvents(IContainer container, string containerName)
    {
        container.Created += (_, _) => HandleClusterStateChange(containerName, "Created");
        container.Creating += (_, _) => HandleClusterStateChange(containerName, "Creating");
        container.Started += (_, _) => HandleClusterStateChange(containerName, "Started");
        container.Starting += (_, _) => HandleClusterStateChange(containerName, "Starting");
        container.Stopped += (_, _) => HandleClusterStateChange(containerName, "Stopped");
        container.Stopping += (_, _) => HandleClusterStateChange(containerName, "Stopping");
    }

    // ---- Topic / namespace management (pulsar-admin exec, same pattern as IntegrationFixture.cs:160-185) ----

    public static string CreateTopicName(string localName) => $"persistent://{Namespace}/{localName}-{Guid.NewGuid():N}";

    public async Task<string> CreateTopic(CancellationToken cancellationToken)
    {
        var topic = $"persistent://{Namespace}/{Guid.NewGuid():N}";
        await CreateTopic(topic, cancellationToken);
        return topic;
    }

    public async Task CreateTopic(string topic, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics create {topic}", "Could not create the topic", cancellationToken);

    public async Task<string> CreatePartitionedTopic(int numberOfPartitions, CancellationToken cancellationToken)
    {
        var topic = $"persistent://{Namespace}/{Guid.NewGuid():N}";
        await CreatePartitionedTopic(topic, numberOfPartitions, cancellationToken);
        return topic;
    }

    public async Task CreatePartitionedTopic(string topic, int numberOfPartitions, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics create-partitioned-topic {topic} -p {numberOfPartitions}", "Could not create the partitioned topic", cancellationToken);

    public async Task DeletePartitionedTopic(string topic, bool force, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics delete-partitioned-topic {topic}{(force ? " -f" : string.Empty)}", "Could not delete the partitioned topic", cancellationToken);

    public async Task CreateTenant(string tenant, CancellationToken cancellationToken)
        => await RunAdminCommand($"tenants create {tenant} --allowed-clusters {ClusterName} --admin-roles {SuperUser}", "Could not create the tenant", cancellationToken);

    public async Task CreateNamespace(string ns, int numberOfBundles, CancellationToken cancellationToken)
        => await RunAdminCommand($"namespaces create {ns} --bundles {numberOfBundles}", "Could not create the namespace", cancellationToken);

    private async Task RunAdminCommand(string command, string errorMessage, CancellationToken cancellationToken)
    {
        var arguments = $"bin/pulsar-admin {command}";

        var result = await _pulsarCluster.ExecAsync(["/bin/bash", "-c", arguments], cancellationToken);

        if (result.ExitCode != 0)
            throw new Exception($"{errorMessage}: {result.Stderr}");
    }

    // ---- Topic stats (admin v2) ----

    public Task<JsonElement> GetTopicStats(string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/stats", cancellationToken);

    public Task<JsonElement> GetPartitionedTopicStats(string topic, bool perPartition, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/partitioned-stats?perPartition={(perPartition ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetInternalStats(string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/internalStats", cancellationToken);

    public async Task<long> GetUnackedMessages(string topic, string subscription, CancellationToken cancellationToken)
    {
        var stats = await GetTopicStats(topic, cancellationToken);
        return stats.GetProperty("subscriptions").GetProperty(subscription).GetProperty("unackedMessages").GetInt64();
    }

    // ---- Transaction admin (admin v3, p409:pulsar-broker/.../admin/v3/Transactions.java) ----

    public Task<JsonElement> ListCoordinators(CancellationToken cancellationToken)
        => GetJson("/admin/v3/transactions/coordinators", cancellationToken);

    public Task<JsonElement> GetCoordinatorStats(int? coordinatorId, CancellationToken cancellationToken)
        => GetJson(coordinatorId is null ? "/admin/v3/transactions/coordinatorStats" : $"/admin/v3/transactions/coordinatorStats?coordinatorId={coordinatorId}", cancellationToken);

    public Task<JsonElement> GetCoordinatorInternalStats(int coordinatorId, bool metadata, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/coordinatorInternalStats/{coordinatorId}?metadata={(metadata ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetTransactionMetadata(TransactionId transactionId, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionMetadata/{ToPath(transactionId)}", cancellationToken);

    public Task<JsonElement> GetTransactionInBufferStats(TransactionId transactionId, string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionInBufferStats/{ToRestPath(topic, false)}/{ToPath(transactionId)}", cancellationToken);

    public Task<JsonElement> GetTransactionInPendingAckStats(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionInPendingAckStats/{ToRestPath(topic, false)}/{Uri.EscapeDataString(subscription)}/{ToPath(transactionId)}", cancellationToken);

    public Task<JsonElement> GetTransactionBufferStats(string topic, bool lowWaterMarks, bool segmentStats, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionBufferStats/{ToRestPath(topic, false)}?lowWaterMarks={(lowWaterMarks ? "true" : "false")}&segmentStats={(segmentStats ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetTransactionBufferInternalStats(string topic, bool metadata, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionBufferInternalStats/{ToRestPath(topic, false)}?metadata={(metadata ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetPendingAckStats(string topic, string subscription, bool lowWaterMarks, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/pendingAckStats/{ToRestPath(topic, false)}/{Uri.EscapeDataString(subscription)}?lowWaterMarks={(lowWaterMarks ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetPendingAckInternalStats(string topic, string subscription, bool metadata, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/pendingAckInternalStats/{ToRestPath(topic, false)}/{Uri.EscapeDataString(subscription)}?metadata={(metadata ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetPositionStatsInPendingAck(string topic, string subscription, ulong ledgerId, ulong entryId, int? batchIndex, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/positionStatsInPendingAck/{ToRestPath(topic, false)}/{Uri.EscapeDataString(subscription)}/{ledgerId}/{entryId}{(batchIndex is null ? string.Empty : $"?batchIndex={batchIndex}")}", cancellationToken);

    public Task<JsonElement> GetSlowTransactions(TimeSpan timeout, int? coordinatorId, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/slowTransactions/{(long) timeout.TotalMilliseconds}{(coordinatorId is null ? string.Empty : $"?coordinatorId={coordinatorId}")}", cancellationToken);

    public async Task AbortTransaction(TransactionId transactionId, CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var content = new StringContent(string.Empty);
        using var response = await httpClient.PostAsync($"/admin/v3/transactions/abortTransaction/{ToPath(transactionId)}", content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"Could not abort the transaction {transactionId}: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
    }

    public async Task<HttpStatusCode> GetStatusCode(string path, CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var response = await httpClient.GetAsync(path, cancellationToken);
        return response.StatusCode;
    }

    private async Task<JsonElement> GetJson(string path, CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var response = await httpClient.GetAsync(path, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"GET {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");

        return await ReadJson(response, cancellationToken);
    }

    private static async Task<JsonElement> ReadJson(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var json = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        return json.RootElement.Clone();
    }

    // "persistent://tenant/ns/local" -> "persistent/tenant/ns/local" (admin v2) or "tenant/ns/local" (admin v3, TopicName.getRestPath(false))
    private static string ToRestPath(string topic, bool includeDomain = true)
    {
        var separator = topic.IndexOf("://", StringComparison.Ordinal);
        var domain = topic.Substring(0, separator);
        var parts = topic.Substring(separator + 3).Split('/');
        var path = $"{parts[0]}/{parts[1]}/{Uri.EscapeDataString(parts[2])}";
        return includeDomain ? $"{domain}/{path}" : path;
    }

    // Java formats TxnID bits as signed longs (TransactionsImpl: txnID.getMostSigBits() + "")
    private static string ToPath(TransactionId transactionId)
        => $"{unchecked((long) transactionId.MostSigBits)}/{unchecked((long) transactionId.LeastSigBits)}";

    // ---- Receive helper (Java consumer.receive(timeout) returning null) ----

    public static async Task<IMessage<TMessage>?> ReceiveOrNull<TMessage>(IReceive<IMessage<TMessage>> receiver, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            return await receiver.Receive(cts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }
}
```

Notes on the code:
- `IReceive<IMessage<TMessage>>` covers both `IConsumer<T>` (`src/DotPulsar/Abstractions/IConsumerOfT.cs:20`) and
  `IReader<T>` (`IReaderOfT.cs:20`). Cancelling a pending receive does not lose messages: single-topic path
  goes through `AsyncQueue.Dequeue` whose cancel removes only the pending waiter (`src/DotPulsar/Internal/AsyncQueue.cs` Dequeue/Cancel);
  multi-topic path keeps the in-flight sub-receive task in `_receiveTasks` (`src/DotPulsar/Internal/Consumer.cs:220-258`).
  Usage: `var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token); message.ShouldBeNull();`
- `CreateClient(...)` depends on the new `IPulsarClientBuilder.EnableTransaction(bool)` (spec decision).
  Test classes may instead keep their own private `CreateClient()` like `Internal/ConsumerTests.cs:585-591`
  (with `.ExceptionHandler(_testOutputHelper.Log)`); passing `_testOutputHelper.Log` through `exceptionHandler` keeps both styles possible.
- Admin v2 base path is `/admin/v2/persistent/...` (same as `Internal/ConsumerTests.cs:599`); admin v3 resources are
  registered under `/admin/v3` (p409:pulsar-broker/.../PulsarService.java L1189-1190) + `@Path("/transactions")`
  (p409:.../admin/v3/Transactions.java L61).
- `unackedMessages` is `SubscriptionStats.unackedMessages` in topic stats JSON (`subscriptions.<sub>.unackedMessages`);
  Java tests read it via `admin.topics().getStats(topic).getSubscriptions().get(sub).getUnackedMessages()`.
  For a partitioned topic call `GetTopicStats(topic + "-partition-N")` or `GetPartitionedTopicStats`.

#### Verified v3 transaction endpoints (Java SHA f3846f4 `Transactions.java` line / v4.0.9 line)

| Helper | HTTP | Path | java L | v4.0.9 L |
|---|---|---|---|---|
| ListCoordinators | GET | `/admin/v3/transactions/coordinators` | 71 | 69 |
| GetCoordinatorStats | GET | `/coordinatorStats[?coordinatorId=N]` | 86-99 | 81 |
| GetTransactionInBufferStats | GET | `/transactionInBufferStats/{tenant}/{namespace}/{topic}/{mostSigBits}/{leastSigBits}` | 105 | 97 |
| GetTransactionInPendingAckStats | GET | `/transactionInPendingAckStats/{tenant}/{namespace}/{topic}/{subName}/{mostSigBits}/{leastSigBits}` | 147 | 134 |
| GetTransactionBufferStats | GET | `/transactionBufferStats/{tenant}/{namespace}/{topic}?lowWaterMarks&segmentStats` | 190-211 | 172 |
| GetPendingAckStats | GET | `/pendingAckStats/{tenant}/{namespace}/{topic}/{subName}?lowWaterMarks` | 233-253 | 210 |
| GetTransactionMetadata | GET | `/transactionMetadata/{mostSigBits}/{leastSigBits}` | 275 | 246 |
| GetSlowTransactions | GET | `/slowTransactions/{timeoutMillis}[?coordinatorId=N]` (millis: TransactionsImpl.java L172 `timeUnit.toMillis(timeout)`) | 299-317 | 267 |
| GetCoordinatorInternalStats | GET | `/coordinatorInternalStats/{coordinatorId}?metadata` | 323-339 | 287 |
| GetPendingAckInternalStats | GET | `/pendingAckInternalStats/{tenant}/{namespace}/{topic}/{subName}?metadata` | 345-367 | 305 |
| GetTransactionBufferInternalStats | GET | `/transactionBufferInternalStats/{tenant}/{namespace}/{topic}?metadata` | 404-424 | 359 |
| (not wrapped) scale TCs | POST | `/transactionCoordinator/replicas` | 444 | 394 |
| GetPositionStatsInPendingAck | GET | `/positionStatsInPendingAck/{tenant}/{namespace}/{topic}/{subName}/{ledgerId}/{entryId}[?batchIndex]` | 468-491 | 418 |
| AbortTransaction | POST | `/abortTransaction/{mostSigBits}/{leastSigBits}` | 515 | 458 |

Topic segment = `TopicName.getRestPath(false)` = `tenant/namespace/encodedLocalName` (no domain;
java:pulsar-client-admin/.../TransactionsImpl.java L92, L108, L139, L156, L162, L221). Txn bits are
formatted as decimal Java `long` (TransactionsImpl.java L93-95, L124-125, L290-291).

#### 4.2 `tests/DotPulsar.Tests/TransactionCollection.cs`

```csharp
/*
 * (Apache header as above)
 */

namespace DotPulsar.Tests;

[CollectionDefinition("Transaction")]
public class TransactionCollection : ICollectionFixture<TransactionFixture> { }
```
(Exact shape of `IntegrationCollection.cs:15-18`.)

#### 4.3 Variant for `TransactionEndToEndWithoutBatchIndexAckTest`

`tests/DotPulsar.Tests/TransactionWithoutBatchIndexAckFixture.cs`
```csharp
/*
 * (Apache header as above)
 */

namespace DotPulsar.Tests;

public sealed class TransactionWithoutBatchIndexAckFixture : TransactionFixture
{
    public TransactionWithoutBatchIndexAckFixture() : base(false) { }
}
```

`tests/DotPulsar.Tests/TransactionWithoutBatchIndexAckCollection.cs`
```csharp
/*
 * (Apache header as above)
 */

namespace DotPulsar.Tests;

[CollectionDefinition("TransactionWithoutBatchIndexAck")]
public class TransactionWithoutBatchIndexAckCollection : ICollectionFixture<TransactionWithoutBatchIndexAckFixture> { }
```

Java `TransactionEndToEndWithoutBatchIndexAckTest extends TransactionEndToEndTest` (L31) and runs **all**
inherited tests with `acknowledgmentAtBatchIndexLevelEnabled=false` (L35). C# equivalent: make the ported
`TransactionEndToEndTests` an `abstract class TransactionEndToEndTestsBase<TFixture> where TFixture : TransactionFixture`
holding all test methods, with two concrete classes:
```csharp
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class TransactionEndToEndTests : TransactionEndToEndTestsBase<TransactionFixture> { ctor(TransactionFixture, ITestOutputHelper) }

[Collection("TransactionWithoutBatchIndexAck"), Trait("Category", "Integration")]
public sealed class TransactionEndToEndWithoutBatchIndexAckTests : TransactionEndToEndTestsBase<TransactionWithoutBatchIndexAckFixture> { ... }
```
(xUnit v3 discovers `[Fact]/[Theory]` on inherited methods of the concrete classes.)
Java overrides in the subclass: `txnIndividualAckTestBatchAndFailoverSub` flips the broker config back to
`true` at runtime (L42-44) — `acknowledgmentAtBatchIndexLevelEnabled` is not a dynamic config, the Java
test mutates the in-process `ServiceConfiguration`, which is impossible against a container. See Open decisions.
`testUnackMessageAfterAckAllMessages` (L49-51) and `testAckWithTransactionReduceUnAckMessageCount` (L56-57)
are re-declared only to change data providers / enable them; the base implementations run unchanged.

### 5. Things the fixture does NOT do (and why)
- No Toxiproxy: none of the transaction tests need network fault injection. `TransactionClientConnectTest`
  (java:pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionClientConnectTest.java) manipulates
  broker internals (TC unload / stop) — not reproducible via a container; flagged for the test-porting drafts.
- No multi-TC: standalone creates 1 partition (p409 PulsarStandalone.java L369-370). Tests asserting on 16 TCs
  need adaptation (owned by test drafts).
- Cluster name is `standalone` (p409 standalone.conf L89) vs Java `test` (TransactionTestBase.java L56);
  irrelevant unless a test asserts cluster names.

### Open decisions (recommendation)

1. **Env-var mechanism:** use `PULSAR_PREFIX_<key>` for every override (works whether or not the key is
   present; `acknowledgmentAtBatchIndexLevelEnabled` is absent from standalone.conf so a plain env name
   would be silently ignored). Must keep `.WithAuthentication()`, otherwise no env is applied at all
   (tc:PulsarContainer.cs L102). Recommendation: as coded.
2. **`acknowledgmentAtBatchIndexLevelEnabled` default:** set explicitly `true` in the main fixture to match
   Java master (ServiceConfiguration L501) — 4.0.9 default is `false` (L433). Recommendation: as coded.
3. **Extra Java test-base configs** (`transactionBufferSnapshotMaxTransactionCount=2`, `...MinTimeInMillis=2000`,
   dispatcher backoff 0, `activeConsumerFailoverDelayTimeMillis=0`): include, for behavioural fidelity with
   `TransactionTestBase.startBroker` (L146-167). Recommendation: include.
4. **`brokerDeleteInactiveTopicsEnabled=false`:** Java mocked brokers effectively never hit the 60s GC
   in short tests; a shared container living for the whole collection would delete idle admin-created topics
   after `brokerDeleteInactiveTopicsFrequencySeconds` (standalone.conf L130-133). Recommendation: set `false` (as coded).
5. **Tenant/namespace `tnx/ns1` (4 bundles)** created in `InitializeAsync` to mirror `TransactionTestBase.setUpBase`
   (L118-119, `TENANT="tnx"`, `NAMESPACE1="tnx/ns1"`, L70-71). Because one container is shared by the whole
   collection, Java fixed topic names (e.g. `NAMESPACE1 + "/txn-timeout"`) would collide across tests and
   across the `[Theory]` rows; recommendation: tests use `TransactionFixture.CreateTopicName("<java-local-name>")`
   which appends a GUID suffix, keeping the Java local name as a readable prefix.
6. **Variant `txnIndividualAckTestBatchAndFailoverSub` override** (Java L42-44, runtime flip to `true`):
   recommendation: do NOT port the override in the variant class (the identical scenario already runs in
   `TransactionEndToEndTests` under the `true` fixture); in the base class mark it so the variant skips it,
   e.g. make the method `public virtual` and override in `TransactionEndToEndWithoutBatchIndexAckTests` with
   a comment `// Upstream: TransactionEndToEndWithoutBatchIndexAckTest#txnIndividualAckTestBatchAndFailoverSub — requires broker config flip at runtime; covered by TransactionEndToEndTests` and `Skip`.
   (Alternative: third fixture; rejected — extra container for a duplicate run.)
7. **Fixture readiness gate**: poll `assign/partitions` then `coordinatorStats?coordinatorId=0` until `state=="Ready"`
   (bounded by the 10-minute `_cts`, same as `IntegrationFixture.cs:43`). Recommendation: as coded.

### Blockers / unverified
- Not run against Docker in this drafting step. Unverified assumption: if the broker acquires ownership of
  the `pulsar/system` bundle before PulsarStandalone L369 creates the assign-topic metadata, the TC may only
  load on the first `TcClientConnectRequest`/re-ownership; the `coordinatorStats` probe re-validates ownership
  each call (TransactionsBase.java L108) but if it stays 404 the implementer should add a fallback
  (e.g. `bin/pulsar-admin namespaces unload pulsar/system` once, then re-poll). Verify on first implementation run.
- Local NuGet cache has Testcontainers.Pulsar 4.16.0 only; csproj pins 4.15.0 (analysis done on 4.15.0 source).
  If the implementer bumps to 4.16.0, re-check `PulsarContainer.CopyStartupScriptAsync`.
- `TransactionId` member types (`ulong` vs `long`) not fixed by shared context; `ToPath` uses `unchecked((long) ...)`
  which compiles for both.
