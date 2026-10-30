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
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Tests.Internal.Crypto;
using System.Text.Json;

[Collection("Integration"), Trait("Category", "Integration")]
public sealed class ProducerEncryptionKeyFailureTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public ProducerEncryptionKeyFailureTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: ProducerEncryptionKeyFailureTest#testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind
    // The client does not tell how many producers it has, so that is not asserted. The producer is not created, and it is not added to the client.
    [Fact]
    public async Task Create_GivenKeyThatCannotBeLoaded_ShouldThrowCryptoExceptionAndNotRegisterPublisher()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();

        //Act
        var exception = Record.Exception(() => client.NewProducer(Schema.ByteArray).Topic(topic).EncryptionKey("missing-key").CryptoKeyReader(new MissingKeyReader()).Create());

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key missing-key.");
        await AssertNoPublisherFor(topic, TimeSpan.FromSeconds(3));
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionFailure (step 1: an invalid key name)
    [Fact]
    public async Task Create_GivenKeyNameThatTheKeyReaderCannotFind_ShouldThrowCryptoException()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey("client-rsa.pem", CryptoTestKeys.Public("ClientRsa"));

        //Act
        var exception = Record.Exception(() => client.NewProducer(Schema.ByteArray).Topic(topic).EncryptionKey("client-non-existant-rsa.pem").CryptoKeyReader(keyReader).Create());

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key client-non-existant-rsa.pem.");
        await AssertNoPublisherFor(topic, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Create_GivenKeyThatIsNotAPemKey_ShouldThrowCryptoExceptionAndNotRegisterPublisher()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        var keyReader = new InMemoryCryptoKeyReader().WithPublicKey("my-key", "this is not a key"u8.ToArray());

        //Act
        var exception = Record.Exception(() => client.NewProducer(Schema.ByteArray).Topic(topic).EncryptionKey("my-key").CryptoKeyReader(keyReader).Create());

        //Assert
        exception.ShouldBeOfType<CryptoException>().Message.ShouldStartWith("Failed to load public key my-key.");
        await AssertNoPublisherFor(topic, TimeSpan.FromSeconds(1));
    }

    private async Task AssertNoPublisherFor(string topic, TimeSpan duration)
    {
        // The connection is opened in the background, so the broker is given time to register the producer, which it must not
        var end = DateTime.UtcNow + duration;

        do
        {
            using var httpClient = _fixture.CreateAdminClient();
            using var response = await httpClient.GetAsync($"/admin/v2/persistent/{topic.Replace("persistent://", string.Empty)}/stats", _cts.Token);
            response.EnsureSuccessStatusCode();
            await using var stream = await response.Content.ReadAsStreamAsync(_cts.Token);
            using var json = await JsonDocument.ParseAsync(stream, cancellationToken: _cts.Token);
            json.RootElement.GetProperty("publishers").GetArrayLength().ShouldBe(0, "A producer whose creation failed must not stay registered on the broker");
            await Task.Delay(TimeSpan.FromMilliseconds(250), _cts.Token);
        }
        while (DateTime.UtcNow < end);
    }

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    // A key reader that cannot find any key, as a misconfigured deployment would
    private sealed class MissingKeyReader : ICryptoKeyReader
    {
        public EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata)
            => null;

        public EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata)
            => null;
    }
}
