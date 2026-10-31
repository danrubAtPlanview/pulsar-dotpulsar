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

using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;

// A producer of a partitioned topic that is connected to a connection that a test controls, so the test decides when each partition is connected
[Trait("Category", "Unit")]
public sealed class ProducerStateTests
{
    private const string Topic = "persistent://public/default/test";
    private const int Partitions = 3;
    private static readonly TimeSpan _guard = TimeSpan.FromSeconds(10);

    // Additional coverage
    // A sub producer that has not reported yet is disconnected, so the producer is partially connected while a partition is not, and it is not waiting for the exclusive access
    [Fact(Timeout = 30_000)]
    public async Task State_GivenPartitionedTopicAndOnePartitionNotYetConnected_ShouldBePartiallyConnectedUntilEveryPartitionIsConnected()
    {
        //Arrange
        var blockedTopic = $"{Topic}-partition-1";
        var releaseBlockedTopic = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ulong nextProducerId = 0;
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandProducer>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo => Create(callInfo.Arg<CommandProducer>(), callInfo.Arg<IChannel>()));
        connection.Send(Arg.Any<CommandCloseProducer>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand());
        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        pool.GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(Partitions));

        async Task<ProducerResponse> Create(CommandProducer command, IChannel channel)
        {
            if (command.Topic == blockedTopic)
                await releaseBlockedTopic.Task;

            channel.Connected();
            var producerId = Interlocked.Increment(ref nextProducerId);
            return new ProducerResponse(producerId, $"producer-{producerId}", 0);
        }

        await using var uut = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), new ProducerOptions<byte[]>(Topic, DotPulsar.Schema.ByteArray), new DefaultExceptionHandler(), pool, null, false);

        //Act
        await uut.State.OnStateChangeTo(ProducerState.PartiallyConnected, Current.CancellationToken).AsTask().WaitAsync(_guard, Current.CancellationToken);
        releaseBlockedTopic.SetResult();
        await uut.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken).AsTask().WaitAsync(_guard, Current.CancellationToken);

        //Assert
        uut.State.IsFinalState().ShouldBeFalse();
    }
}
