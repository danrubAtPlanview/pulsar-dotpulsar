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

namespace DotPulsar.Tests.Internal.Crypto;

using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class SubConsumerEncryptionTests
{
    private const string Topic = "persistent://public/default/test";

    // Upstream: ConsumerImpl#closeAsync (msgCrypto)
    [Fact]
    public async Task DisposeAsync_GivenMessageCrypto_ShouldDisposeIt()
    {
        //Arrange
        var messageCrypto = new MessageCrypto(false);
        var uut = CreateUut(Substitute.For<IConsumerChannel<byte[]>>(), messageCrypto);

        //Act
        await uut.DisposeAsync();

        //Assert
        AssertDisposed(messageCrypto);
    }

    [Fact]
    public async Task ChannelFaulted_GivenMessageCrypto_ShouldDisposeIt()
    {
        //Arrange
        var messageCrypto = new MessageCrypto(false);
        await using var uut = CreateUut(Substitute.For<IConsumerChannel<byte[]>>(), messageCrypto);

        //Act
        await uut.ChannelFaulted(new InvalidOperationException("The channel faulted"));

        //Assert
        AssertDisposed(messageCrypto);
    }

    [Fact]
    public async Task DisposeAsync_GivenNoMessageCrypto_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateUut(Substitute.For<IConsumerChannel<byte[]>>(), null);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.DisposeAsync());

        //Assert
        exception.ShouldBeNull();
    }

    internal static void AssertDisposed(MessageCrypto messageCrypto)
        => Should.Throw<ObjectDisposedException>(() => messageCrypto.TryDecrypt(new MessageMetadata(), ReadOnlySequence<byte>.Empty, Substitute.For<ICryptoKeyReader>(), out _));

    private static SubConsumer<byte[]> CreateUut(IConsumerChannel<byte[]> channel, MessageCrypto? messageCrypto)
    {
        var correlationId = Guid.NewGuid();
        var eventRegister = Substitute.For<IRegisterEvent>();

        return new SubConsumer<byte[]>(
            correlationId,
            new Uri("pulsar://localhost:6650"),
            "subscription",
            SubscriptionType.Exclusive,
            Topic,
            eventRegister,
            channel,
            new Executor(correlationId, eventRegister, new DefaultExceptionHandler()),
            Substitute.For<IState<ConsumerState>>(),
            Substitute.For<IConsumerChannelFactory<byte[]>>(),
            false,
            TimeSpan.FromSeconds(30),
            new ConsumerEpoch(),
            null,
            new NegativeAcknowledgeTracker(TimeSpan.FromMinutes(1), (_, _) => default),
            null,
            ChunkTestData.CreateAssembler(),
            messageCrypto);
    }
}
