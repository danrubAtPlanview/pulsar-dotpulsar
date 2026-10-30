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
using DotPulsar.Extensions;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Testcontainers.Pulsar;

// The container setup and the admin helpers shared by every fixture that needs a Pulsar broker with the transaction
// coordinator enabled. Derived fixtures override ConfigureBroker to add or change broker settings.
//
// Every broker setting is passed as PULSAR_PREFIX_{key}. Testcontainers.Pulsar only applies the environment variables
// to conf/standalone.conf when .WithAuthentication() is used, so that call must never be removed. Plain variable
// names only work for keys that already exist in standalone.conf, whereas the prefixed names work for every key
// (verified against apachepulsar/pulsar:4.0.9: the container log shows 'Adding config' / 'Updating config' for each).
public abstract class TransactionFixtureBase : IAsyncLifetime
{
    public const string Tenant = "tnx";
    public const string Namespace = Tenant + "/ns1";
    public const string PublicTransactionNamespace = "public/txn";
    public const string TransactionCoordinatorAssignTopic = "persistent://pulsar/system/transaction_coordinator_assign";
    private const string TransactionCoordinatorWarmUpTopic = "persistent://public/default/transaction-coordinator-warm-up";
    private const string ClusterName = "standalone";
    private const string SuperUser = "test-user";
    private const int MaxStartAttempts = 3;
    private static readonly TimeSpan _startAttemptTimeout = TimeSpan.FromMinutes(4);
    private readonly bool _acknowledgmentAtBatchIndexLevelEnabled;
    private readonly CancellationTokenSource _cts;
    private PulsarContainer _pulsarCluster;
    private string? _token;

    protected TransactionFixtureBase(bool acknowledgmentAtBatchIndexLevelEnabled)
    {
        _acknowledgmentAtBatchIndexLevelEnabled = acknowledgmentAtBatchIndexLevelEnabled;
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        _pulsarCluster = null!; // This will be set in `InitializeAsync`
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
        var builder = CreateClientBuilder(enableTransaction);

        if (exceptionHandler is not null)
            builder.ExceptionHandler(exceptionHandler);

        return builder.Build();
    }

    public IPulsarClient CreateClient(Action<ExceptionContext> exceptionHandler, bool enableTransaction = true)
        => CreateClientBuilder(enableTransaction).ExceptionHandler(exceptionHandler).Build();

    private IPulsarClientBuilder CreateClientBuilder(bool enableTransaction)
        => PulsarClient
        .Builder()
        .Authentication(Authentication)
        .EnableTransaction(enableTransaction)
        .ServiceUrl(ServiceUrl);

    // Add or change the broker settings. The common settings have already been applied.
    protected virtual PulsarBuilder ConfigureBroker(PulsarBuilder builder) => builder;

    public async ValueTask DisposeAsync()
    {
        if (_pulsarCluster is not null)
            await _pulsarCluster.DisposeAsync();

        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    // Under memory pressure the container sometimes fails to start ("Failed to create token") or hangs. A bounded attempt that is
    // repeated with a new container fails in minutes with a clear message instead of after the 10 minutes of the fixture.
    public async ValueTask InitializeAsync()
    {
        for (var attempt = 1; ; ++attempt)
        {
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            attemptCts.CancelAfter(_startAttemptTimeout);

            try
            {
                await StartCluster(attemptCts.Token);
                return;
            }
            catch (Exception exception)
            {
                var hasTimedOut = exception is OperationCanceledException && attemptCts.IsCancellationRequested && !_cts.IsCancellationRequested;
                var reason = hasTimedOut ? $"it was not ready within {_startAttemptTimeout}" : exception.Message;

                if (attempt == MaxStartAttempts || _cts.IsCancellationRequested)
                {
                    if (hasTimedOut)
                        throw new TimeoutException($"The Pulsar broker was not ready within {_startAttemptTimeout} in {attempt} attempts", exception);

                    throw;
                }

                Current.SendDiagnosticMessage($"Attempt {attempt} of {MaxStartAttempts} to start the Pulsar broker failed ({reason}). Trying again with a new container.");
                await DisposeCluster();
            }
        }
    }

    private async ValueTask DisposeCluster()
    {
        if (_pulsarCluster is null)
            return;

        try
        {
            await _pulsarCluster.DisposeAsync();
        }
        catch (Exception exception)
        {
            Current.SendDiagnosticMessage($"Could not dispose the Pulsar broker: {exception.Message}");
        }

        _pulsarCluster = null!;
    }

    private async Task StartCluster(CancellationToken cancellationToken)
    {
        var builder = new PulsarBuilder("apachepulsar/pulsar:4.0.9")
            .WithAuthentication()
            .WithEnvironment("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
            .WithEnvironment("PULSAR_PREFIX_brokerDeduplicationEnabled", "true")
            .WithEnvironment("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", _acknowledgmentAtBatchIndexLevelEnabled ? "true" : "false")
            .WithEnvironment("PULSAR_PREFIX_transactionBufferSnapshotMaxTransactionCount", "2")
            .WithEnvironment("PULSAR_PREFIX_transactionBufferSnapshotMinTimeInMillis", "2000")
            .WithEnvironment("PULSAR_PREFIX_dispatcherRetryBackoffInitialTimeInMs", "0")
            .WithEnvironment("PULSAR_PREFIX_dispatcherRetryBackoffMaxTimeInMs", "0")
            .WithEnvironment("PULSAR_PREFIX_activeConsumerFailoverDelayTimeMillis", "0")
            .WithEnvironment("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false");

        _pulsarCluster = ConfigureBroker(builder).Build();

        SubscribeToContainerEvents(_pulsarCluster, "Pulsar cluster");
        Current.SendDiagnosticMessage("Starting Pulsar Cluster with transactions enabled");
        await _pulsarCluster.StartAsync(cancellationToken);

        ServiceUrl = new Uri(_pulsarCluster.GetBrokerAddress());
        AdminUrl = new Uri(_pulsarCluster.GetServiceAddress());
        _token = (await _pulsarCluster.CreateAuthenticationTokenAsync(Timeout.InfiniteTimeSpan, cancellationToken)).TrimEnd();

        await WaitForTransactionCoordinator(cancellationToken);
        await CreateTenant(Tenant, cancellationToken);
        await CreateNamespace(Namespace, 4, cancellationToken);
        await CreateNamespace(PublicTransactionNamespace, 10, cancellationToken);

        Current.SendDiagnosticMessage("The transaction coordinator is ready. You can connect with: " + ServiceUrl);
    }

    // Verified against apachepulsar/pulsar:4.0.9: the standalone broker does not load the transaction coordinator until
    // the first CommandTcClientConnectRequest arrives. The namespace bundle ownership listener that should load it
    // fires before PulsarStandalone has created the transaction_coordinator_assign partitioned topic metadata, and
    // neither 'pulsar-admin namespaces unload pulsar/system' nor a topic lookup makes it fire again. Until the first
    // connect, GET /admin/v3/transactions/coordinatorStats?coordinatorId=0 answers 404 "Transaction coordinator not
    // found!" forever. A transactional Java client is therefore run once inside the container, and then the
    // coordinator reports the state "Ready".
    private async Task WaitForTransactionCoordinator(CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        var lastObservation = string.Empty;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (isReady, canBeConnected, observation) = await ProbeTransactionCoordinator(httpClient, cancellationToken);

            if (isReady)
                return;

            if (observation != lastObservation)
            {
                Current.SendDiagnosticMessage($"Waiting for the transaction coordinator: {observation}");
                lastObservation = observation;
            }

            if (canBeConnected)
                await ConnectTransactionCoordinator(cancellationToken);

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }
    }

    private static async Task<(bool IsReady, bool CanBeConnected, string Observation)> ProbeTransactionCoordinator(HttpClient httpClient, CancellationToken cancellationToken)
    {
        using var partitionsResponse = await httpClient.GetAsync($"/admin/v2/{ToRestPath(TransactionCoordinatorAssignTopic)}/partitions", cancellationToken);
        if (!partitionsResponse.IsSuccessStatusCode)
            return (false, false, $"The partition metadata request returned {(int) partitionsResponse.StatusCode}");

        var partitions = await ReadJson(partitionsResponse, cancellationToken);
        if (partitions.GetProperty("partitions").GetInt32() <= 0)
            return (false, false, "The transaction coordinator assign topic has no partitions yet");

        using var statsResponse = await httpClient.GetAsync("/admin/v3/transactions/coordinatorStats?coordinatorId=0", cancellationToken);
        if (statsResponse.StatusCode == HttpStatusCode.NotFound)
            return (false, true, "The transaction coordinator is not loaded yet");

        if (!statsResponse.IsSuccessStatusCode)
            return (false, false, $"The coordinator stats request returned {(int) statsResponse.StatusCode}");

        var stats = await ReadJson(statsResponse, cancellationToken);
        var state = stats.TryGetProperty("state", out var stateProperty) ? stateProperty.GetString() : null;

        return (state == "Ready", false, $"The transaction coordinator state is '{state}'");
    }

    private async Task ConnectTransactionCoordinator(CancellationToken cancellationToken)
    {
        Current.SendDiagnosticMessage("Connecting a transactional client to load the transaction coordinator");

        var arguments = $"bin/pulsar-perf produce {TransactionCoordinatorWarmUpTopic} -txn -abort -nmt 1 -r 1 -m 1 -s 10";

        var result = await _pulsarCluster.ExecAsync(["/bin/bash", "-c", arguments], cancellationToken);

        if (result.ExitCode != 0)
            Current.SendDiagnosticMessage($"Could not connect the transactional client: {result.Stderr}");
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

    // ---- Topic / namespace management (pulsar-admin exec, same pattern as IntegrationFixture) ----

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

    public async Task DeleteTopic(string topic, bool force, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics delete {topic}{(force ? " -f" : string.Empty)}", "Could not delete the topic", cancellationToken);

    // Java: admin.topics().createSubscription(topic, subscription, MessageId.earliest) or MessageId.latest
    public async Task CreateSubscription(string topic, string subscription, bool earliest, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics create-subscription {topic} -s {subscription} -m {(earliest ? "earliest" : "latest")}", "Could not create the subscription", cancellationToken);

    public async Task UnloadTopic(string topic, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics unload {topic}", "Could not unload the topic", cancellationToken);

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

    // ---- Batched messages ----

    // Produces batches of batchSize messages. A batch is closed by the number of messages, and the time window of a batch is so long that it is never closed by it.
    // The payloads are the numbers of the messages, so the tests identify the messages by their batch index.
    public async Task ProduceBatchedMessages(string topic, int numberOfBatches, int batchSize, CancellationToken cancellationToken)
    {
        await using var client = CreateClient(enableTransaction: false);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(true)
            .BatchingMaxMessages((uint) batchSize)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .Create();

        for (var i = 0; i < numberOfBatches * batchSize; i++)
        {
            await producer.SendChannel.Send(Encoding.UTF8.GetBytes(i.ToString()), null, cancellationToken);
        }

        await producer.Flush(cancellationToken);
    }

    // ---- Chunked messages ----

    // Produces messages that are split in chunks when they are larger than the maximum message size of the broker (5 MiB by default)
    public async Task ProduceChunkedMessages(string topic, int numberOfMessages, int messageSize, CancellationToken cancellationToken)
    {
        await using var client = CreateClient(enableTransaction: false);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableChunking(true)
            .Create();

        var message = new byte[messageSize];

        for (var i = 0; i < numberOfMessages; i++)
        {
            _ = await producer.Send(message, cancellationToken);
        }
    }

    // ---- Broker configuration and topic metadata (admin v2) ----

    public Task<JsonElement> GetRuntimeConfiguration(CancellationToken cancellationToken)
        => GetJson("/admin/v2/brokers/configuration/runtime", cancellationToken);

    public Task<JsonElement> GetPartitionedTopicMetadata(string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/partitions", cancellationToken);

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

    // ---- Namespace and topic policies (admin v2) ----

    public Task SetNamespaceDeduplication(string ns, bool enabled, CancellationToken cancellationToken)
        => PostJson($"/admin/v2/namespaces/{ns}/deduplication", enabled ? "true" : "false", cancellationToken);

    // The broker rejects a producer that does not encrypt its messages (it must set 'encrypted' when it is created), and the messages that are not encrypted
    public Task SetNamespaceEncryptionRequired(string ns, bool required, CancellationToken cancellationToken)
        => PostJson($"/admin/v2/namespaces/{ns}/encryptionRequired", required ? "true" : "false", cancellationToken);

    public Task SetTopicDelayedDeliveryPolicy(string topic, bool active, long tickTimeMillis, long maxDeliveryDelayInMillis, CancellationToken cancellationToken)
        => PostJson($"/admin/v2/{ToRestPath(topic)}/delayedDelivery", $"{{\"active\":{(active ? "true" : "false")},\"tickTime\":{tickTimeMillis},\"maxDeliveryDelayInMillis\":{maxDeliveryDelayInMillis}}}", cancellationToken);

    // ---- Transaction admin (admin v3) ----

    public Task<JsonElement> ListCoordinators(CancellationToken cancellationToken)
        => GetJson("/admin/v3/transactions/coordinators", cancellationToken);

    public Task<JsonElement> GetCoordinatorStats(int? coordinatorId, CancellationToken cancellationToken)
        => GetJson(coordinatorId is null ? "/admin/v3/transactions/coordinatorStats" : $"/admin/v3/transactions/coordinatorStats?coordinatorId={coordinatorId}", cancellationToken);

    public Task<JsonElement> GetCoordinatorInternalStats(int coordinatorId, bool metadata, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/coordinatorInternalStats/{coordinatorId}?metadata={(metadata ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetTransactionMetadata(long mostSigBits, long leastSigBits, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionMetadata/{ToPath(mostSigBits, leastSigBits)}", cancellationToken);

    public Task<JsonElement> GetTransactionMetadata(TransactionId transactionId, CancellationToken cancellationToken)
        => GetTransactionMetadata(ToSigned(transactionId.MostSigBits), ToSigned(transactionId.LeastSigBits), cancellationToken);

    // The broker removes a transaction from its metadata store when the transaction has been committed or aborted
    public Task<HttpStatusCode> GetTransactionMetadataStatusCode(TransactionId transactionId, CancellationToken cancellationToken)
        => GetStatusCode($"/admin/v3/transactions/transactionMetadata/{ToPath(ToSigned(transactionId.MostSigBits), ToSigned(transactionId.LeastSigBits))}", cancellationToken);

    // Java: TransactionMetadataStore#getTxnMeta fails with TransactionNotFoundException (the admin API answers 404) when the transaction has ended or timed out at the coordinator
    public async Task<bool> TransactionMetadataExists(TransactionId transactionId, CancellationToken cancellationToken)
    {
        var statusCode = await GetTransactionMetadataStatusCode(transactionId, cancellationToken);

        return statusCode switch
        {
            HttpStatusCode.OK => true,
            HttpStatusCode.NotFound => false,
            _ => throw new Exception($"The transaction metadata of {transactionId} was requested, and the broker answered {(int) statusCode}")
        };
    }

    // Java: the sum of getMetadataStoreStats().timeoutCount of the transaction metadata stores. The broker exposes it as pulsar_txn_timeout_total, with one sample for every coordinator
    public async Task<long> GetTransactionTimeoutTotal(CancellationToken cancellationToken)
    {
        const string metricName = "pulsar_txn_timeout_total";

        using var httpClient = CreateAdminClient();
        using var response = await httpClient.GetAsync("/metrics/", cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"GET /metrics/ failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");

        var total = 0d;

        foreach (var line in (await response.Content.ReadAsStringAsync(cancellationToken)).Split('\n'))
        {
            if (!line.StartsWith(metricName, StringComparison.Ordinal))
                continue;

            var endOfLabels = line.IndexOf('}');
            var value = (endOfLabels < 0 ? line.Substring(metricName.Length) : line.Substring(endOfLabels + 1)).Trim().Split(' ')[0];
            total += double.Parse(value, CultureInfo.InvariantCulture);
        }

        return (long) total;
    }

    public Task<JsonElement> GetTransactionInBufferStats(long mostSigBits, long leastSigBits, string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionInBufferStats/{ToRestPath(topic, false)}/{ToPath(mostSigBits, leastSigBits)}", cancellationToken);

    public Task<JsonElement> GetTransactionInBufferStats(TransactionId transactionId, string topic, CancellationToken cancellationToken)
        => GetTransactionInBufferStats(ToSigned(transactionId.MostSigBits), ToSigned(transactionId.LeastSigBits), topic, cancellationToken);

    public Task<JsonElement> GetTransactionInPendingAckStats(long mostSigBits, long leastSigBits, string topic, string subscription, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionInPendingAckStats/{ToRestPath(topic, false)}/{Uri.EscapeDataString(subscription)}/{ToPath(mostSigBits, leastSigBits)}", cancellationToken);

    public Task<JsonElement> GetTransactionInPendingAckStats(TransactionId transactionId, string topic, string subscription, CancellationToken cancellationToken)
        => GetTransactionInPendingAckStats(ToSigned(transactionId.MostSigBits), ToSigned(transactionId.LeastSigBits), topic, subscription, cancellationToken);

    public Task<JsonElement> GetTransactionBufferStats(string topic, bool lowWaterMarks, bool segmentStats, CancellationToken cancellationToken)
        => GetJson($"/admin/v3/transactions/transactionBufferStats/{ToRestPath(topic, false)}?lowWaterMarks={(lowWaterMarks ? "true" : "false")}&segmentStats={(segmentStats ? "true" : "false")}", cancellationToken);

    // The broker does not know a topic that is not loaded, for example after it was unloaded, until a producer or a consumer connects to it again
    public async Task<JsonElement?> TryGetTransactionBufferStats(string topic, bool lowWaterMarks, bool segmentStats, CancellationToken cancellationToken)
    {
        var path = $"/admin/v3/transactions/transactionBufferStats/{ToRestPath(topic, false)}?lowWaterMarks={(lowWaterMarks ? "true" : "false")}&segmentStats={(segmentStats ? "true" : "false")}";

        using var httpClient = CreateAdminClient();
        using var response = await httpClient.GetAsync(path, cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;

        if (!response.IsSuccessStatusCode)
            throw new Exception($"GET {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");

        return await ReadJson(response, cancellationToken);
    }

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

    public async Task AbortTransaction(long mostSigBits, long leastSigBits, CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var content = new StringContent(string.Empty, Encoding.UTF8, "application/json");
        using var response = await httpClient.PostAsync($"/admin/v3/transactions/abortTransaction/{ToPath(mostSigBits, leastSigBits)}", content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"Could not abort the transaction ({mostSigBits},{leastSigBits}): {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
    }

    public Task AbortTransaction(TransactionId transactionId, CancellationToken cancellationToken)
        => AbortTransaction(ToSigned(transactionId.MostSigBits), ToSigned(transactionId.LeastSigBits), cancellationToken);

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

    private async Task PostJson(string path, string json, CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await httpClient.PostAsync(path, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"POST {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
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

    // Java formats the transaction id bits as signed longs (TransactionsImpl: txnID.getMostSigBits() + "")
    private static string ToPath(long mostSigBits, long leastSigBits) => $"{mostSigBits}/{leastSigBits}";

    private static long ToSigned(ulong bits) => unchecked((long) bits);

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
