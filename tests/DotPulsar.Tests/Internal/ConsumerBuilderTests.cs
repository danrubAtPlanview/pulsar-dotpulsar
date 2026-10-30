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
public sealed class ConsumerBuilderTests
{
    [Fact]
    public void Create_GivenNoBatchIndexAckEnabled_ShouldDisableIt()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().BatchIndexAckEnabled.ShouldBeFalse();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Create_GivenBatchIndexAckEnabled_ShouldPassItOnInTheOptions(bool batchIndexAckEnabled)
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.BatchIndexAckEnabled(batchIndexAckEnabled).Create();

        //Assert
        options().BatchIndexAckEnabled.ShouldBe(batchIndexAckEnabled);
    }

    [Fact]
    public void BatchIndexAckEnabled_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act
        var actual = uut.BatchIndexAckEnabled(true);

        //Assert
        actual.ShouldBeSameAs(uut);
    }

    [Fact]
    public void ConsumerOptions_GivenNoChanges_ShouldHaveTheDefaultBatchIndexAckEnabled()
    {
        //Act
        var uut = new ConsumerOptions<string>("subscription", "persistent://public/default/test", Schema.String);

        //Assert
        ConsumerOptions<string>.DefaultBatchIndexAckEnabled.ShouldBeFalse();
        uut.BatchIndexAckEnabled.ShouldBe(ConsumerOptions<string>.DefaultBatchIndexAckEnabled);
    }

    private static (IConsumerBuilder<string> Builder, Func<ConsumerOptions<string>> Options) CreateBuilder()
    {
        ConsumerOptions<string>? captured = null;
        var client = Substitute.For<IPulsarClient>();
        client.CreateConsumer(Arg.Do<ConsumerOptions<string>>(options => captured = options)).Returns(Substitute.For<IConsumer<string>>());

        var builder = new ConsumerBuilder<string>(client, Schema.String)
            .SubscriptionName("subscription")
            .Topic("persistent://public/default/test");

        return (builder, () => captured ?? throw new InvalidOperationException("The consumer was not created"));
    }
}
