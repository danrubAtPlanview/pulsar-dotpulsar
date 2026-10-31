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

using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;
using System.Collections.Concurrent;

// A consumer that is connected to a connection that a test controls, so the test decides when the partitions are subscribed and when the owner of a channel gets it
[Trait("Category", "Unit")]
public sealed class ConsumerStateTests
{
    private const string Topic = "persistent://public/default/test";
    private const int Partitions = 3;

    // Additional coverage
    // The broker answers the subscribe command, and the channel raises its event, before the consumer has its channel. The consumer is not active before that, so a Receive that follows the
    // state change finds the channel, and does not wait for the retry of the executor (3 seconds).
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenReceiveRightAfterTheConsumerIsActive_ShouldNotWaitForTheRetryInterval()
    {
        //Arrange
        IChannel? channel = null;
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandSubscribe>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo => Subscribe(callInfo.Arg<IChannel>()));
        connection.Send(Arg.Any<CommandCloseConsumer>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand());
        var connectionPool = Substitute.For<IConnectionPool>();
        connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        connectionPool.GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(0));

        async Task<SubscribeResponse> Subscribe(IChannel subscribedChannel)
        {
            channel = subscribedChannel;
            subscribedChannel.Connected();
            await Task.Delay(TimeSpan.FromMilliseconds(200)); // The consumer has no channel yet
            return new SubscribeResponse(7);
        }

        var exceptionHandler = new ExceptionHandlerPipeline(TimeSpan.FromSeconds(3), [new DefaultExceptionHandler()]); // The pipeline that the client builder makes, with its default retry interval
        await using var uut = new Consumer<byte[]>(new Uri("pulsar://localhost:6650"), new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray), connectionPool, exceptionHandler, TimeSpan.FromSeconds(30));

        //Act
        await uut.StateChangedTo(ConsumerState.Active, Current.CancellationToken);
        channel!.Received(ChunkTestData.CreateChunkPackage("a", 0, 1, 10));
        var message = await uut.Receive(Current.CancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(2), Current.CancellationToken);

        //Assert
        message.ShouldNotBeNull();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task State_GivenPartitionedTopicAndOnePartitionNotYetSubscribed_ShouldNotBeActiveUntilEveryPartitionIsSubscribed()
    {
        //Arrange
        var blockedTopic = $"{Topic}-partition-1";
        var releaseBlockedTopic = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribedTopics = new ConcurrentBag<string>();
        var allOthersSubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandSubscribe>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            var topic = callInfo.Arg<CommandSubscribe>().Topic;
            var channel = callInfo.Arg<IChannel>();
            return Subscribe(topic, channel);
        });
        connection.Send(Arg.Any<CommandCloseConsumer>(), Arg.Any<CancellationToken>()).Returns(new BaseCommand());
        var connectionPool = Substitute.For<IConnectionPool>();
        connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        connectionPool.GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(Partitions));

        async Task<SubscribeResponse> Subscribe(string topic, IChannel channel)
        {
            if (topic == blockedTopic)
                await releaseBlockedTopic.Task;

            subscribedTopics.Add(topic);
            if (!subscribedTopics.Contains(blockedTopic) && subscribedTopics.Count == Partitions - 1)
                allOthersSubscribed.TrySetResult();

            channel.Connected();
            return new SubscribeResponse(7);
        }

        await using var uut = new Consumer<byte[]>(new Uri("pulsar://localhost:6650"), new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray), connectionPool, new DefaultExceptionHandler(), TimeSpan.FromSeconds(30));
        await allOthersSubscribed.Task;

        //Act
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMilliseconds(500));
        var activeBeforeLastPartition = await Record.ExceptionAsync(async () => await uut.StateChangedTo(ConsumerState.Active, cts.Token));
        releaseBlockedTopic.SetResult();
        await uut.StateChangedTo(ConsumerState.Active, Current.CancellationToken);

        //Assert
        activeBeforeLastPartition.ShouldNotBeNull();
        activeBeforeLastPartition.ShouldBeAssignableTo<OperationCanceledException>();
        subscribedTopics.Count.ShouldBe(Partitions);
    }
}
