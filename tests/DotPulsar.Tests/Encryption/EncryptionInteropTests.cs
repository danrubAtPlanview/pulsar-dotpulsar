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

// These tests use the 'pulsar-client' CLI (the Java client) of the apachepulsar/pulsar image together with a DotPulsar producer or consumer.
// The tests with a DotPulsar consumer run. TODO(WP9-producer): the tests with a DotPulsar producer need the encryption options of the producer builder and the encrypt step of the producer channel.
// The byte-exact compatibility with the Java client is also covered without a broker by MessageCryptoJavaInteropTests.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class EncryptionInteropTests : IDisposable
{
    private const string SkipReason = "Requires encryption wiring of the producer (TODO(WP9-producer))";

    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public EncryptionInteropTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    [Fact]
    public async Task Receive_GivenJavaProducedRsaEncryptedMessage_ShouldDecrypt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientRsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        await ProduceUsingJavaClient(topic, "client-rsa.pem", "ClientRsa", "java-rsa-0");
        var actual = await Receive(consumer);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("java-rsa-0");
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        actual.EncryptionContext.Keys.Keys.ShouldBe(["client-rsa.pem"]);
    }

    [Fact]
    public async Task Receive_GivenJavaProducedEcdsaEncryptedMessage_ShouldDecrypt()
    {
        //Arrange
        Assert.SkipWhen(OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientEcdsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        await ProduceUsingJavaClient(topic, "client-ecdsa.pem", "ClientEcdsa", "java-ecdsa-0");
        var actual = await Receive(consumer);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("java-ecdsa-0");
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        actual.EncryptionContext.Keys.Keys.ShouldBe(["client-ecdsa.pem"]);
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedRsaEncryptedMessage_ShouldBeDecryptedByJavaCli()
    {
        // A producer with the key client-rsa.pem sends "dotpulsar-rsa-0"
        // bin/pulsar-client consume {topic} -s interop -p Earliest -n 1 -ekv file:///pulsar/keys/private-key.client-rsa.pem exits with 0 and prints 'content:dotpulsar-rsa-0'
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedEcdsaEncryptedMessage_ShouldBeDecryptedByJavaCli()
    {
        // The same as the RSA test with client-ecdsa.pem
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedEncryptedLz4Message_ShouldBeDecryptedByJavaCli()
    {
        // The same as the RSA test with CompressionType.Lz4 (compress, then encrypt)
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedEncryptedMessageWithoutKey_ShouldNotBeDelivered()
    {
        // bin/pulsar-client consume with '-ca DISCARD' and '-n 1' (wrapped in a 10 second timeout) prints nothing and the message is acknowledged (msgBacklog is 0)
    }

    // The tool produces a message that is encrypted with the public key, and the consumer has the private key
    private async Task ProduceUsingJavaClient(string topic, string keyName, string keySet, string message)
    {
        var keyFile = $"/pulsar/keys/{keyName}.pub";
        await _fixture.CopyToPulsar(CryptoTestKeys.Public(keySet), keyFile.TrimStart('/'), _cts.Token);
        var result = await _fixture.ExecInPulsar($"bin/pulsar-client produce {topic} -m '{message}' -n 1 -ekn {keyName} -ekv file://{keyFile}", _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"Could not produce the encrypted message: {result.Stderr}");
    }

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topic, string privateKeySet)
        => client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .Topic(topic)
            .DefaultCryptoKeyReader("data:application/x-pem-file;base64," + Convert.ToBase64String(CryptoTestKeys.Private(privateKeySet)))
            .Create();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private async Task<IMessage<string>> Receive(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(60), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }
}
