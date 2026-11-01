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

namespace DotPulsar.Tests.Encryption;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using System.Text;

// A namespace that requires encryption: the broker rejects a producer that does not set 'encrypted' in the CommandProducer, and the messages that have no encryption keys (testEncryptionRequired of TransactionTest)
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class EncryptionRequiredTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public EncryptionRequiredTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Additional coverage
    [Fact]
    public async Task Send_GivenEncryptionRequiredNamespaceAndEncryptedProducer_ShouldAcceptTheMessageAndTheConsumerDecryptsIt()
    {
        //Arrange
        var topic = await CreateTopicInEncryptionRequiredNamespace();
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .EncryptionKey("my-app-key")
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Public("ClientRsa")))
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("subscription")
            .Topic(topic)
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Private("ClientRsa")))
            .Create();
        var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

        //Act
        _ = await producer.Send(payload, _cts.Token);
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        actual.ShouldNotBeNull();
        actual.Data.ToArray().ShouldBe(payload);
        actual.GetEncryptionContext().ShouldNotBeNull().Keys.Keys.ShouldBe(["my-app-key"]);
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenEncryptionRequiredNamespaceAndProducerWithoutEncryption_ShouldNotBeAcceptedByTheBroker()
    {
        //Arrange
        var topic = await CreateTopicInEncryptionRequiredNamespace();
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topic).Create();

        //Act
        var state = await producer.State.OnStateChangeFrom(ProducerState.Disconnected, _cts.Token);

        //Assert
        state.ShouldBe(ProducerState.Faulted);
        var exception = await Record.ExceptionAsync(async () => await producer.Send([1, 2, 3], _cts.Token));
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<DotPulsar.Exceptions.ProducerFaultedException>().InnerException.ShouldNotBeNull().Message.ShouldContain("ncryption");
    }

    private async Task<string> CreateTopicInEncryptionRequiredNamespace()
    {
        var ns = $"public/encryption-required-{Guid.NewGuid():N}";
        await Admin($"namespaces create {ns}");
        await Admin($"namespaces set-encryption-required -e {ns}");
        var topic = $"persistent://{ns}/test";
        await _fixture.CreateTopic(topic, _cts.Token);
        return topic;
    }

    private async Task Admin(string command)
    {
        var result = await _fixture.ExecInPulsar($"bin/pulsar-admin {command}", _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"'{command}' failed: {result.Stderr}");
    }

    private static string CreateDataUri(byte[] key)
        => "data:application/x-pem-file;base64," + Convert.ToBase64String(key);

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();
}
