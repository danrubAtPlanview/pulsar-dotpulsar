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
using System.Diagnostics;

// A connection that is dead, but not detected yet, never answers the command that closes a producer, a consumer or a reader. The disposal waits for the answer for as long as the operation
// timeout, like the Java client does, and then goes on. Before, it waited without a limit, which was 91 seconds with the default keep-alive (followup 0014).
[Trait("Category", "Unit")]
public sealed class DisposeCloseTimeoutTests
{
    private const string Topic = "persistent://public/default/test";
    private static readonly Uri _serviceUrl = new("pulsar://localhost:6650");
    private static readonly TimeSpan _operationTimeout = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan _guard = TimeSpan.FromSeconds(10);

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task DisposeAsync_GivenConsumerWhoseCloseCommandIsNeverAnswered_ShouldCompleteAfterTheOperationTimeout()
    {
        //Arrange
        var pool = CreatePoolThatNeverAnswersTheCloseCommand();
        var uut = new Consumer<byte[]>(_serviceUrl, new ConsumerOptions<byte[]>("subscription", Topic, DotPulsar.Schema.ByteArray), pool, new DefaultExceptionHandler(), _operationTimeout);
        await uut.StateChangedTo(ConsumerState.Active, Current.CancellationToken);

        //Act
        var stopwatch = Stopwatch.StartNew();
        await uut.DisposeAsync().AsTask().WaitAsync(_guard, Current.CancellationToken);

        //Assert
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task DisposeAsync_GivenReaderWhoseCloseCommandIsNeverAnswered_ShouldCompleteAfterTheOperationTimeout()
    {
        //Arrange
        var pool = CreatePoolThatNeverAnswersTheCloseCommand();
        var uut = new Reader<byte[]>(_serviceUrl, new ReaderOptions<byte[]>(MessageId.Earliest, Topic, DotPulsar.Schema.ByteArray), new DefaultExceptionHandler(), pool, _operationTimeout);
        await uut.StateChangedTo(ReaderState.Connected, Current.CancellationToken);

        //Act
        var stopwatch = Stopwatch.StartNew();
        await uut.DisposeAsync().AsTask().WaitAsync(_guard, Current.CancellationToken);

        //Assert
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task DisposeAsync_GivenProducerWhoseCloseCommandIsNeverAnswered_ShouldCompleteAfterTheOperationTimeout()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.Connection.Send(Arg.Any<CommandCloseProducer>(), Arg.Any<CancellationToken>()).Returns(new TaskCompletionSource<BaseCommand>().Task);
        var uut = new Producer<byte[]>(_serviceUrl, new ProducerOptions<byte[]>(Topic, DotPulsar.Schema.ByteArray), new DefaultExceptionHandler(), connection.Pool, null, false, _operationTimeout);
        await uut.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);

        //Act
        var stopwatch = Stopwatch.StartNew();
        await uut.DisposeAsync().AsTask().WaitAsync(_guard, Current.CancellationToken);

        //Assert
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    // A connection that subscribes at once and that never answers a command that closes a consumer. Like the real connection, it does not stop waiting for the answer when the token is cancelled.
    private static IConnectionPool CreatePoolThatNeverAnswersTheCloseCommand()
    {
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Any<CommandSubscribe>(), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            callInfo.Arg<IChannel>().Connected();
            return Task.FromResult(new SubscribeResponse(7));
        });
        connection.Send(Arg.Any<CommandCloseConsumer>(), Arg.Any<CancellationToken>()).Returns(new TaskCompletionSource<BaseCommand>().Task);

        var pool = Substitute.For<IConnectionPool>();
        pool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));
        pool.GetNumberOfPartitions(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<uint>(0));
        return pool;
    }
}
