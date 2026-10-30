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

using DotPulsar.Exceptions;

[Trait("Category", "Unit")]
public sealed class PulsarClientReaderOptionsTests
{
    private static readonly Uri _serviceUrl = new("pulsar://localhost:6650");

    // Additional coverage
    [Fact]
    public async Task CreateReader_GivenNegativeExpireTimeOfIncompleteChunkedMessage_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = new ReaderOptions<string>(MessageId.Earliest, "persistent://public/default/test", Schema.String)
        {
            ExpireTimeOfIncompleteChunkedMessage = TimeSpan.FromMilliseconds(-1)
        };

        //Act
        var exception = Record.Exception(() => client.CreateReader(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ReaderOptions.ExpireTimeOfIncompleteChunkedMessage may not be negative");
    }

    // Additional coverage
    [Theory]
    [InlineData(3)]
    [InlineData(-1)]
    [InlineData(255)]
    public async Task CreateReader_GivenCryptoFailureActionThatIsNotDefined_ShouldThrowConfigurationException(int cryptoFailureAction)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = new ReaderOptions<string>(MessageId.Earliest, "persistent://public/default/test", Schema.String)
        {
            CryptoFailureAction = (ConsumerCryptoFailureAction) cryptoFailureAction
        };

        //Act
        var exception = Record.Exception(() => client.CreateReader(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ReaderOptions.CryptoFailureAction is not a valid ConsumerCryptoFailureAction");
    }
}
