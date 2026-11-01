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
    protected const string SuperUser = "test-user";
    private const int MaxStartAttempts = 3;
    private static readonly TimeSpan _startAttemptTimeout = TimeSpan.FromMinutes(4);
    private readonly bool _acknowledgmentAtBatchIndexLevelEnabled;
    private readonly CancellationTokenSource _cts;
    private PulsarContainer _pulsarCluster;
    private IDisposable? _brokerSlot;
    private string? _token;

    protected TransactionFixtureBase(bool acknowledgmentAtBatchIndexLevelEnabled)
    {
        _acknowledgmentAtBatchIndexLevelEnabled = acknowledgmentAtBatchIndexLevelEnabled;
        _cts = new CancellationTokenSource(); // The time limit starts when the fixture has a broker slot
        _pulsarCluster = null!; // This will be set in `InitializeAsync`
    }

    public Uri ServiceUrl { get; protected set; } = null!; // This will be set in `InitializeAsync`

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

    // For the tests that need client settings, for example the operation timeout. The transactions are enabled.
    public IPulsarClient CreateConfiguredClient(Action<IPulsarClientBuilder> configure, Action<ExceptionContext>? exceptionHandler = null)
    {
        var builder = CreateClientBuilder(true);
        configure(builder);

        if (exceptionHandler is not null)
            builder.ExceptionHandler(exceptionHandler);

        return builder.Build();
    }

    private IPulsarClientBuilder CreateClientBuilder(bool enableTransaction)
        => PulsarClient
        .Builder()
        .Authentication(Authentication)
        .EnableTransaction(enableTransaction)
        .ServiceUrl(ServiceUrl);

    // Add or change the broker settings. The common settings have already been applied.
    protected virtual PulsarBuilder ConfigureBroker(PulsarBuilder builder) => builder;

    // Prepare the broker after the transaction coordinator is ready and the tenant and namespaces exist, for example with roles and permissions
    protected virtual Task OnClusterStarted(CancellationToken cancellationToken) => Task.CompletedTask;

    public virtual async ValueTask DisposeAsync()
    {
        try
        {
            if (_pulsarCluster is not null)
                await _pulsarCluster.DisposeAsync();
        }
        finally
        {
            _brokerSlot?.Dispose();
        }

        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    public virtual async ValueTask InitializeAsync()
    {
        _brokerSlot = await BrokerResources.WaitForSlot();
        _cts.CancelAfter(TimeSpan.FromMinutes(15));

        try
        {
            await StartClusterWithRetries();
        }
        catch
        {
            _brokerSlot.Dispose();
            throw;
        }
    }

    // Under memory pressure the container sometimes fails to start ("Failed to create token") or hangs. A bounded attempt that is
    // repeated with a new container fails in minutes with a clear message instead of after the 10 minutes of the fixture.
    private async Task StartClusterWithRetries()
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
        var builder = new PulsarBuilder(BrokerImage.Name)
            .WithAuthentication()
            .WithBrokerMemory()
            .WithEnvironment("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
            .WithEnvironment("PULSAR_PREFIX_brokerDeduplicationEnabled", "true")
            .WithEnvironment("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", _acknowledgmentAtBatchIndexLevelEnabled ? "true" : "false")
            .WithEnvironment("PULSAR_PREFIX_transactionBufferSnapshotMaxTransactionCount", "2")
            .WithEnvironment("PULSAR_PREFIX_transactionBufferSnapshotMinTimeInMillis", "2000")
            .WithEnvironment("PULSAR_PREFIX_dispatcherRetryBackoffInitialTimeInMs", "0")
            .WithEnvironment("PULSAR_PREFIX_dispatcherRetryBackoffMaxTimeInMs", "0")
            .WithEnvironment("PULSAR_PREFIX_activeConsumerFailoverDelayTimeMillis", "0")
            .WithEnvironment("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
            .WithEnvironment("PULSAR_PREFIX_forceDeleteNamespaceAllowed", "true");

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
        await OnClusterStarted(cancellationToken);

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

    public async Task DeleteSubscription(string topic, string subscription, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics unsubscribe {topic} --subscription {subscription}", "Could not delete the subscription", cancellationToken);

    public async Task UnloadTopic(string topic, CancellationToken cancellationToken)
        => await RunAdminCommand($"topics unload {topic}", "Could not unload the topic", cancellationToken);

    // Unloads the topic and returns when the consumer is connected to it again (Java: admin.topics().unload(topic) followed by Awaitility.await().until(consumer::isConnected)).
    // The consumer is observed from before the unload, because it has usually reconnected before the unload command returns, and the state it left would be missed.
    public async Task UnloadTopic(string topic, IConsumer consumer, CancellationToken cancellationToken)
    {
        var hasLeftActive = consumer.StateChangedFrom(ConsumerState.Active, cancellationToken).AsTask();
        await UnloadTopic(topic, cancellationToken);
        await hasLeftActive;
        await consumer.StateChangedTo(ConsumerState.Active, cancellationToken);
    }

    public async Task CreateTenant(string tenant, CancellationToken cancellationToken)
        => await RunAdminCommand($"tenants create {tenant} --allowed-clusters {ClusterName} --admin-roles {SuperUser}", "Could not create the tenant", cancellationToken);

    public async Task CreateNamespace(string ns, int numberOfBundles, CancellationToken cancellationToken)
        => await RunAdminCommand($"namespaces create {ns} --bundles {numberOfBundles}", "Could not create the namespace", cancellationToken);

    // The broker only deletes a namespace that has topics when it is told to force it, and when forceDeleteNamespaceAllowed is true
    public async Task DeleteNamespace(string ns, bool force, CancellationToken cancellationToken)
    {
        var path = $"/admin/v2/namespaces/{ns}?force={(force ? "true" : "false")}";

        using var httpClient = CreateAdminClient();
        using var response = await httpClient.DeleteAsync(path, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"DELETE {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
    }

    public async Task UnloadNamespace(string @namespace, CancellationToken cancellationToken)
    {
        var path = $"/admin/v2/namespaces/{@namespace}/unload";

        using var httpClient = CreateAdminClient();
        using var content = new StringContent(string.Empty, Encoding.UTF8, "application/json");
        using var response = await httpClient.PutAsync(path, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"PUT {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
    }

    // The broker does not list its system topics, like the pending ack topic of a subscription that has transactional acks
    public async Task<string[]> GetTopicList(string ns, CancellationToken cancellationToken)
    {
        var topics = await GetJson($"/admin/v2/persistent/{ns}", cancellationToken);
        return topics.EnumerateArray().Select(topic => topic.GetString()!).ToArray();
    }

    private async Task RunAdminCommand(string command, string errorMessage, CancellationToken cancellationToken)
    {
        var arguments = $"bin/pulsar-admin {command}";

        var result = await _pulsarCluster.ExecAsync(["/bin/bash", "-c", arguments], cancellationToken);

        if (result.ExitCode != 0)
            throw new Exception($"{errorMessage}: {result.Stderr}");
    }

    // Runs a command in the container of the broker. The container has the command line tools of Pulsar, which are the Java client.
    public async Task<ExecResult> ExecInPulsar(string bashCommand, CancellationToken cancellationToken)
        => await _pulsarCluster.ExecAsync(["/bin/bash", "-c", bashCommand], cancellationToken);

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

    // The topics of the namespace as the admin API lists them (Java: admin.namespaces().getTopics(namespace)). The listing leaves out the system topics, and
    // the transaction pending ack and buffer topics are system topics, unless includeSystemTopics is set.
    public async Task<IReadOnlyList<string>> GetNamespaceTopics(string ns, bool includeSystemTopics, CancellationToken cancellationToken)
    {
        var topics = await GetJson($"/admin/v2/namespaces/{ns}/topics?includeSystemTopic={(includeSystemTopics ? "true" : "false")}", cancellationToken);
        return topics.EnumerateArray().Select(topic => topic.GetString()!).ToList();
    }

    // ---- Topic stats (admin v2) ----

    public Task<JsonElement> GetTopicStats(string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/stats", cancellationToken);

    public Task<JsonElement> GetPartitionedTopicStats(string topic, bool perPartition, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/partitioned-stats?perPartition={(perPartition ? "true" : "false")}", cancellationToken);

    public Task<JsonElement> GetInternalStats(string topic, CancellationToken cancellationToken)
        => GetJson($"/admin/v2/{ToRestPath(topic)}/internalStats", cancellationToken);

    // The state of the managed ledger of a topic, for example LedgerOpened. The broker loads a topic that is not loaded when it is asked for its stats, so after an unload it is the state of the new managed ledger.
    public async Task WaitForManagedLedgerState(string topic, string state, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        while (true)
        {
            try
            {
                if ((await GetInternalStats(topic, cts.Token)).GetProperty("state").GetString() == state)
                    return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // The topic is not available while the broker loads it
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
        }
    }

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

    // Without the deduplication of the broker a message is not dropped when its sequence id is not larger than the one of the message before it.
    // The broker applies a topic policy when it has read it back from the system topic of the namespace, so this waits until it has.
    public async Task SetTopicDeduplication(string topic, bool enabled, CancellationToken cancellationToken)
    {
        var path = $"/admin/v2/{ToRestPath(topic)}/deduplicationEnabled";
        var json = enabled ? "true" : "false";
        await PostJson(path, json, cancellationToken);

        using var httpClient = CreateAdminClient();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        while (true)
        {
            using var response = await httpClient.GetAsync(path, cts.Token);

            if (response.IsSuccessStatusCode && await response.Content.ReadAsStringAsync(cts.Token) == json)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
        }
    }

    public Task SetTopicCompactionThreshold(string topic, long bytes, CancellationToken cancellationToken)
        => PostJson($"/admin/v2/{ToRestPath(topic)}/compactionThreshold", bytes.ToString(System.Globalization.CultureInfo.InvariantCulture), cancellationToken);

    // ---- Prometheus metrics ----

    // The metrics of the broker as text. The broker writes the metrics of the topics that are loaded in it, and it does not load a topic to do so.
    public async Task<string> GetPrometheusMetricsText(CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var response = await httpClient.GetAsync("/metrics/", cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"GET /metrics/ failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");

        return await response.Content.ReadAsStringAsync(cancellationToken);
    }

    // ---- Compaction (admin v2) ----

    public async Task TriggerCompaction(string topic, CancellationToken cancellationToken)
    {
        var path = $"/admin/v2/{ToRestPath(topic)}/compaction";

        using var httpClient = CreateAdminClient();
        using var content = new StringContent(string.Empty, Encoding.UTF8, "application/json");
        using var response = await httpClient.PutAsync(path, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"PUT {path} failed: {response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");
    }

    // NOT_RUN, RUNNING, SUCCESS or ERROR
    public async Task<string> GetCompactionStatus(string topic, CancellationToken cancellationToken)
        => (await GetJson($"/admin/v2/{ToRestPath(topic)}/compaction", cancellationToken)).GetProperty("status").GetString()!;

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

    // The mark delete position of the cursor of the pending ack log of the subscription and the last confirmed entry of the log, as (ledger id, entry id).
    // Java reads them from the cursor with reflection, or from the same admin stats (TransactionTest#testPendingAckMarkDeletePosition).
    public async Task<((long LedgerId, long EntryId) MarkDeletePosition, (long LedgerId, long EntryId) LastConfirmedEntry)> GetPendingAckLogPositions(string topic, string subscription, CancellationToken cancellationToken)
    {
        var stats = await GetPendingAckInternalStats(topic, subscription, false, cancellationToken);
        var log = stats.GetProperty("pendingAckLogStats").GetProperty("managedLedgerInternalStats");
        var markDeletePosition = log.GetProperty("cursors").GetProperty("__pending_ack_state").GetProperty("markDeletePosition").GetString()!;
        var lastConfirmedEntry = log.GetProperty("lastConfirmedEntry").GetString()!;

        return (ParsePosition(markDeletePosition), ParsePosition(lastConfirmedEntry));
    }

    // "ledgerId:entryId"
    private static (long LedgerId, long EntryId) ParsePosition(string position)
    {
        var parts = position.Split(':');
        return (long.Parse(parts[0], CultureInfo.InvariantCulture), long.Parse(parts[1], CultureInfo.InvariantCulture));
    }

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

    // The reason the broker gives for the failure of an admin request, which is the message of the PulsarAdminException of the Java client
    public async Task<(HttpStatusCode StatusCode, string? Reason)> GetErrorReason(HttpMethod method, string path, string? json, CancellationToken cancellationToken)
    {
        using var httpClient = CreateAdminClient();
        using var request = new HttpRequestMessage(method, path);

        if (json is not null)
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");

        using var response = await httpClient.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (response.IsSuccessStatusCode)
            throw new Exception($"{method} {path} did not fail: {response.StatusCode} {body}");

        using var document = JsonDocument.Parse(body);
        var reason = document.RootElement.ValueKind == JsonValueKind.Object && document.RootElement.TryGetProperty("reason", out var property) ? property.GetString() : null;

        return (response.StatusCode, reason);
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

    // ---- Prometheus metrics ----

    // The metrics of the broker. The samples are parsed like the Java tests parse them (PrometheusMetricsTest.parseMetrics): the samples without labels are left out.
    public async Task<IReadOnlyList<PrometheusSample>> GetPrometheusMetrics(CancellationToken cancellationToken)
        => ParsePrometheusMetrics(await GetPrometheusMetricsText(cancellationToken));

    public sealed record PrometheusSample(string Name, IReadOnlyDictionary<string, string> Labels, double Value);

    private static List<PrometheusSample> ParsePrometheusMetrics(string text)
    {
        var samples = new List<PrometheusSample>();

        foreach (var line in text.Split('\n'))
        {
            var open = line.IndexOf('{');
            var close = line.LastIndexOf('}');

            if (line.StartsWith('#') || open < 0 || close < open)
                continue;

            var fields = line.Substring(close + 1).Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var value = fields[0] switch
            {
                "+Inf" => double.PositiveInfinity,
                "-Inf" => double.NegativeInfinity,
                "NaN" => double.NaN,
                _ => double.Parse(fields[0], CultureInfo.InvariantCulture)
            };

            samples.Add(new PrometheusSample(line.Substring(0, open), ParsePrometheusLabels(line.Substring(open + 1, close - open - 1)), value));
        }

        return samples;
    }

    // name="value",name="value" where a value can contain commas and the escaped characters \\ \" and \n
    private static Dictionary<string, string> ParsePrometheusLabels(string text)
    {
        var labels = new Dictionary<string, string>();
        var position = 0;

        while (position < text.Length)
        {
            var equals = text.IndexOf('=', position);

            if (equals < 0)
                break;

            var name = text.Substring(position, equals - position).Trim(',', ' ');
            var value = new StringBuilder();
            position = equals + 2; // Skips the opening quote

            while (position < text.Length && text[position] != '"')
            {
                if (text[position] == '\\' && position + 1 < text.Length)
                {
                    ++position;
                    value.Append(text[position] == 'n' ? '\n' : text[position]);
                }
                else
                {
                    value.Append(text[position]);
                }

                ++position;
            }

            labels[name] = value.ToString();
            ++position; // Skips the closing quote
        }

        return labels;
    }

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

    // ---- Reading until nothing is left ----

    // Java: while (true) { message = receiver.receive(timeout); if (message == null) break; values.add(message.getValue()); }
    public static async Task<List<TMessage>> DrainValues<TMessage>(IReceive<IMessage<TMessage>> receiver, TimeSpan receiveTimeout, CancellationToken cancellationToken)
    {
        var values = new List<TMessage>();

        while (true)
        {
            var message = await ReceiveOrNull(receiver, receiveTimeout, cancellationToken);

            if (message is null)
                return values;

            values.Add(message.Value());
        }
    }

    // Java: ConsumerImpl#hasMessageAvailableAsync, for a reader that starts at startMessageId (earliest or a message id) and reads without inclusive.
    // The reader has a message to read when the last message id that the broker gives is after the last message that was read, or after the start when none was read.
    // The broker gives an entry id of -1 (ulong.MaxValue) when there is no message to read in its last ledger, even if a transaction is open there.
    public static async Task<bool> HasMessageAvailable(IReader reader, MessageId startMessageId, MessageId? lastRead, CancellationToken cancellationToken)
    {
        var last = (await reader.GetLastMessageIds(cancellationToken)).Single();

        if (last.EntryId == ulong.MaxValue)
            return false;

        var reference = lastRead ?? startMessageId;

        // Earliest is not before every other message id when compared as numbers
        if (reference == MessageId.Earliest)
            return true;

        return last.LedgerId > reference.LedgerId || (last.LedgerId == reference.LedgerId && last.EntryId > reference.EntryId);
    }

    // Java: while (reader.hasMessageAvailable()) { messages.add(reader.readNext()); }
    public static async Task<List<IMessage<TMessage>>> ReadAllAvailable<TMessage>(IReader<TMessage> reader, MessageId startMessageId, CancellationToken cancellationToken)
    {
        var messages = new List<IMessage<TMessage>>();

        while (await HasMessageAvailable(reader, startMessageId, messages.Count == 0 ? null : messages[^1].MessageId, cancellationToken))
        {
            // readNext of Java waits until there is a message. The broker has said that there is one, so it must come quickly.
            var message = await ReceiveOrNull(reader, TimeSpan.FromSeconds(30), cancellationToken);
            message.ShouldNotBeNull("the reader has a message available, but it did not receive it");
            messages.Add(message);
        }

        return messages;
    }
}
