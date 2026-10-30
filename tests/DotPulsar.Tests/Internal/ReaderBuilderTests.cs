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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Abstractions;
using DotPulsar.Internal;

[Trait("Category", "Unit")]
public sealed class ReaderBuilderTests
{
    [Fact]
    public void Create_GivenNoChunkSettings_ShouldUseTheDefaults()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().MaxPendingChunkedMessage.ShouldBe(10U);
        options().AutoAckOldestChunkedMessageOnQueueFull.ShouldBeFalse();
        options().ExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMinutes(1));
        ReaderOptions<string>.DefaultMaxPendingChunkedMessage.ShouldBe(10U);
        ReaderOptions<string>.DefaultAutoAckOldestChunkedMessageOnQueueFull.ShouldBeFalse();
        ReaderOptions<string>.DefaultExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMinutes(1));
    }

    // Upstream: MessageChunkingTest#testReaderChunkingConfiguration (adapted: the built options are asserted instead of the consumer configuration)
    [Fact]
    public void Create_GivenChunkSettings_ShouldPassThemOnInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.MaxPendingChunkedMessage(12).AutoAckOldestChunkedMessageOnQueueFull(true).ExpireTimeOfIncompleteChunkedMessage(TimeSpan.FromMilliseconds(12)).Create();

        //Assert
        options().MaxPendingChunkedMessage.ShouldBe(12U);
        options().AutoAckOldestChunkedMessageOnQueueFull.ShouldBeTrue();
        options().ExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMilliseconds(12));
    }

    [Fact]
    public void Builder_GivenChunkSettings_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act & Assert
        uut.MaxPendingChunkedMessage(1).ShouldBeSameAs(uut);
        uut.AutoAckOldestChunkedMessageOnQueueFull(true).ShouldBeSameAs(uut);
        uut.ExpireTimeOfIncompleteChunkedMessage(TimeSpan.Zero).ShouldBeSameAs(uut);
    }

    [Fact]
    public void ReaderOptions_GivenNoChanges_ShouldHaveTheDefaultChunkSettings()
    {
        //Act
        var uut = new ReaderOptions<string>(MessageId.Earliest, "persistent://public/default/test", Schema.String);

        //Assert
        uut.MaxPendingChunkedMessage.ShouldBe(ReaderOptions<string>.DefaultMaxPendingChunkedMessage);
        uut.AutoAckOldestChunkedMessageOnQueueFull.ShouldBe(ReaderOptions<string>.DefaultAutoAckOldestChunkedMessageOnQueueFull);
        uut.ExpireTimeOfIncompleteChunkedMessage.ShouldBe(ReaderOptions<string>.DefaultExpireTimeOfIncompleteChunkedMessage);
    }

    private static (IReaderBuilder<string> Builder, Func<ReaderOptions<string>> Options) CreateBuilder()
    {
        ReaderOptions<string>? captured = null;
        var client = Substitute.For<IPulsarClient>();
        client.CreateReader(Arg.Do<ReaderOptions<string>>(options => captured = options)).Returns(Substitute.For<IReader<string>>());

        var builder = new ReaderBuilder<string>(client, Schema.String)
            .StartMessageId(MessageId.Earliest)
            .Topic("persistent://public/default/test");

        return (builder, () => captured ?? throw new InvalidOperationException("The reader was not created"));
    }
}
