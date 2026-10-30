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
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Events;

[Trait("Category", "Unit")]
public sealed class ExecutorTests
{
    private readonly IRegisterEvent _eventRegister = Substitute.For<IRegisterEvent>();

    [Fact]
    public void ExecutionResult_ShouldBeAValueType()
        => typeof(ExecutionResult<string>).IsValueType.ShouldBeTrue();

    [Fact]
    public void ExecutionResult_GivenDefault_ShouldNotBeSuccessful()
    {
        //Act
        var uut = default(ExecutionResult<string>);

        //Assert
        uut.Success.ShouldBeFalse();
        uut.Result.ShouldBeNull();
    }

    [Fact]
    public async Task Execute_GivenFuncThatReturnsResult_ShouldReturnTheResult()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Rethrow);

        //Act
        var result = await uut.Execute(() => "result", Current.CancellationToken);

        //Assert
        result.ShouldBe("result");
    }

    [Fact]
    public async Task Execute_GivenFuncThatReturnsTaskOfResult_ShouldReturnTheResult()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Rethrow);

        //Act
        var result = await uut.Execute(() => Task.FromResult("result"), Current.CancellationToken);

        //Assert
        result.ShouldBe("result");
    }

    [Fact]
    public async Task Execute_GivenFuncThatReturnsValueTaskOfResult_ShouldReturnTheResult()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Rethrow);

        //Act
        var result = await uut.Execute(() => new ValueTask<string>("result"), Current.CancellationToken);

        //Assert
        result.ShouldBe("result");
    }

    [Fact]
    public async Task Execute_GivenFuncThatReturnsNull_ShouldReturnNull()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Rethrow);

        //Act
        var result = await uut.Execute(() => new ValueTask<string?>((string?) null), Current.CancellationToken);

        //Assert
        result.ShouldBeNull();
    }

    [Fact]
    public async Task Execute_GivenFuncThatFailsWithExceptionThatIsRetried_ShouldCallTheFuncAgainAndReturnTheResult()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Retry);
        var calls = 0;

        //Act
        var result = await uut.Execute(() => new ValueTask<string>(++calls == 1 ? throw new InvalidOperationException("first") : "second"), Current.CancellationToken);

        //Assert
        result.ShouldBe("second");
        calls.ShouldBe(2);
        _eventRegister.DidNotReceive().Register(Arg.Any<IEvent>());
    }

    [Fact]
    public async Task Execute_GivenTaskThatFailsWithExceptionThatIsRetried_ShouldCallTheFuncAgainAndReturnTheResult()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Retry);
        var calls = 0;

        //Act
        var result = await uut.Execute(() => ++calls == 1 ? Task.FromException<string>(new InvalidOperationException("first")) : Task.FromResult("second"), Current.CancellationToken);

        //Assert
        result.ShouldBe("second");
        calls.ShouldBe(2);
    }

    [Fact]
    public async Task Execute_GivenFuncWithoutResultThatFailsWithExceptionThatIsRetried_ShouldCallTheFuncAgain()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Retry);
        var calls = 0;

        //Act
        await uut.Execute(() => ++calls == 1 ? throw new InvalidOperationException("first") : default(ValueTask), Current.CancellationToken);

        //Assert
        calls.ShouldBe(2);
    }

    [Fact]
    public async Task Execute_GivenFuncThatFailsWithExceptionThatIsRethrown_ShouldThrowAndRegisterExecutorFaulted()
    {
        //Arrange
        var correlationId = Guid.NewGuid();
        var uut = CreateExecutor(FaultAction.Rethrow, correlationId);
        var exception = new InvalidOperationException("failed");
        var calls = 0;

        //Act
        var thrown = await Record.ExceptionAsync(async () => await uut.Execute(() =>
        {
            ++calls;
            return new ValueTask<string>(Task.FromException<string>(exception));
        }, Current.CancellationToken));

        //Assert
        thrown.ShouldBeSameAs(exception);
        calls.ShouldBe(1);
        _eventRegister.Received(1).Register(Arg.Is<IEvent>(e => e is ExecutorFaulted && ((ExecutorFaulted) e).CorrelationId == correlationId && ((ExecutorFaulted) e).Exception == exception));
    }

    [Fact]
    public async Task Execute_GivenFuncThatFailsAndCancellationWasRequested_ShouldRethrowWithoutAskingTheHandler()
    {
        //Arrange
        var handler = Substitute.For<IHandleException>();
        var uut = new Executor(Guid.NewGuid(), _eventRegister, handler);
        using var cts = new CancellationTokenSource();
        var exception = new InvalidOperationException("failed");

        //Act
        var thrown = await Record.ExceptionAsync(async () => await uut.Execute(() =>
        {
            cts.Cancel();
            return new ValueTask<string>(Task.FromException<string>(exception));
        }, cts.Token));

        //Assert
        thrown.ShouldBeSameAs(exception);
        await handler.DidNotReceive().OnException(Arg.Any<ExceptionContext>());
        _eventRegister.DidNotReceive().Register(Arg.Any<IEvent>());
    }

    [Fact]
    public async Task TryExecuteOnce_GivenFuncThatReturnsResult_ShouldReturnSuccessfulResult()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Retry);

        //Act
        var fromFunc = await uut.TryExecuteOnce(() => "result", Current.CancellationToken);
        var fromTask = await uut.TryExecuteOnce<string>(() => Task.FromResult("result"), Current.CancellationToken);
        var fromValueTask = await uut.TryExecuteOnce<string>(() => new ValueTask<string>("result"), Current.CancellationToken);

        //Assert
        foreach (var result in new[] { fromFunc, fromTask, fromValueTask })
        {
            result.Success.ShouldBeTrue();
            result.Result.ShouldBe("result");
        }
    }

    [Fact]
    public async Task TryExecuteOnce_GivenFuncThatFailsWithExceptionThatIsRetried_ShouldReturnUnsuccessfulResultWithoutRunningTheFuncAgain()
    {
        //Arrange
        var uut = CreateExecutor(FaultAction.Retry);
        var calls = 0;

        //Act
        Func<string> func = () => { ++calls; throw new InvalidOperationException("failed"); };
        Func<Task<string>> taskFunc = () => { ++calls; return Task.FromException<string>(new InvalidOperationException("failed")); };
        Func<ValueTask<string>> valueTaskFunc = () => { ++calls; throw new InvalidOperationException("failed"); };
        var fromFunc = await uut.TryExecuteOnce(func, Current.CancellationToken);
        var fromTask = await uut.TryExecuteOnce(taskFunc, Current.CancellationToken);
        var fromValueTask = await uut.TryExecuteOnce(valueTaskFunc, Current.CancellationToken);

        //Assert
        calls.ShouldBe(3);
        foreach (var result in new[] { fromFunc, fromTask, fromValueTask })
        {
            result.Success.ShouldBeFalse();
            result.Result.ShouldBeNull();
        }
    }

    // The executor does not allocate for a result, which the acknowledgment and the receive of every message use. It did, when the result was a class.
    [Fact]
    public async Task Execute_GivenValueTaskOfResultThatCompletesAtOnce_ShouldNotAllocate()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var uut = CreateExecutor(FaultAction.Retry);
        var func = static () => new ValueTask<string>("result");

        //Act
        var allocated = await AllocationMeasurement.BytesPerOperation(async () => _ = await uut.Execute(func, CancellationToken.None));

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public async Task Execute_GivenTaskOfResultThatCompletesAtOnce_ShouldNotAllocate()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var uut = CreateExecutor(FaultAction.Retry);
        var task = Task.FromResult("result");
        var func = () => task;

        //Act
        var allocated = await AllocationMeasurement.BytesPerOperation(async () => _ = await uut.Execute(func, CancellationToken.None));

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public async Task Execute_GivenFuncOfResultThatCompletesAtOnce_ShouldNotAllocate()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var uut = CreateExecutor(FaultAction.Retry);
        var func = static () => "result";

        //Act
        var allocated = await AllocationMeasurement.BytesPerOperation(async () => _ = await uut.Execute(func, CancellationToken.None));

        //Assert
        allocated.ShouldBe(0);
    }

    [Fact]
    public async Task Execute_GivenValueTaskWithoutResultThatCompletesAtOnce_ShouldNotAllocate()
    {
        //Arrange
        AllocationMeasurement.SkipWhenNotOptimized();
        var uut = CreateExecutor(FaultAction.Retry);
        var func = static () => default(ValueTask);

        //Act
        var allocated = await AllocationMeasurement.BytesPerOperation(async () => await uut.Execute(func, CancellationToken.None));

        //Assert
        allocated.ShouldBe(0);
    }

    private Executor CreateExecutor(FaultAction action, Guid? correlationId = null)
        => new(correlationId ?? Guid.NewGuid(), _eventRegister, new FixedExceptionHandler(action));

    private sealed class FixedExceptionHandler(FaultAction action) : IHandleException
    {
        public ValueTask OnException(ExceptionContext exceptionContext)
        {
            exceptionContext.Result = action;
            exceptionContext.ExceptionHandled = true;
            return default;
        }
    }
}
