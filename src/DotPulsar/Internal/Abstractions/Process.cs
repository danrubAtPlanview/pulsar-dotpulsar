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

namespace DotPulsar.Internal.Abstractions;

using DotPulsar.Internal.Events;

public abstract class Process : IProcess
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly object _stateLock = new object();
    private int _isReconnecting;
    private bool _isChannelEstablished;
    protected readonly AsyncQueue<Func<CancellationToken, Task>> ActionQueue;
    private Task? _actionProcessorTask;
    protected ChannelState ChannelState;
    protected ExecutorState ExecutorState;
    protected Exception? Exception;

    protected Process(Guid correlationId)
    {
        _cancellationTokenSource = new CancellationTokenSource();
        ActionQueue = new AsyncQueue<Func<CancellationToken, Task>>();
        ChannelState = ChannelState.Disconnected;
        ExecutorState = ExecutorState.Ok;
        _isChannelEstablished = false;
        CorrelationId = correlationId;
    }

    public Guid CorrelationId { get; }

    // True when the channel of the last connect has been handed to its owner, which is after the broker has answered the subscribe or the create command. The channel raises its events
    // when the broker answers, which is before that, so a state that tells the user that the channel can be used must wait for it. Only read when the state lock is held (CalculateState).
    protected bool IsChannelEstablished => _isChannelEstablished;

    public void Start()
    {
        _actionProcessorTask = ProcessActions(_cancellationTokenSource.Token);

        lock (_stateLock)
        {
            CalculateState();
        }
    }

    public virtual async ValueTask DisposeAsync()
    {
        _cancellationTokenSource.Cancel();
        if (_actionProcessorTask is not null)
            await _actionProcessorTask.ConfigureAwait(false);
    }

    public void Handle(IEvent e)
    {
        lock (_stateLock)
        {
            switch (e)
            {
                case ExecutorFaulted executorFaulted:
                    ExecutorState = ExecutorState.Faulted;
                    Exception = executorFaulted.Exception;
                    break;
                case ChannelActivated _:
                    ChannelState = ChannelState.Active;
                    break;
                case ChannelClosedByServer _:
                    ChannelState = ChannelState.ClosedByServer;
                    _isChannelEstablished = false;
                    break;
                case ChannelConnected _:
                    // The broker tells a failover consumer whether it is the active consumer before it answers the subscribe command, so the channel can be active or inactive when it is connected,
                    // and being connected does not change that. A disconnect has reset the state when the channel of an earlier connection was lost.
                    if (ChannelState is not (ChannelState.Active or ChannelState.Inactive))
                        ChannelState = ChannelState.Connected;
                    break;
                case ChannelDeactivated _:
                    ChannelState = ChannelState.Inactive;
                    break;
                case SendReceiptWrongOrdering _:
                case ChannelDisconnected _:
                    ChannelState = ChannelState.Disconnected;
                    _isChannelEstablished = false;
                    break;
                case ChannelReachedEndOfTopic _:
                    ChannelState = ChannelState.ReachedEndOfTopic;
                    break;
                case ChannelUnsubscribed _:
                    ChannelState = ChannelState.Unsubscribed;
                    break;
                case ProducerWaitingForExclusive _:
                    ChannelState = ChannelState.WaitingForExclusive;
                    break;
            }

            CalculateState();
        }
    }

    protected abstract void CalculateState();

    protected void ScheduleReconnect(IContainsChannel channel)
    {
        if (Interlocked.CompareExchange(ref _isReconnecting, 1, 0) != 0)
            return;

        ActionQueue.Enqueue(async cancellationToken =>
        {
            var isEstablished = false;

            try
            {
                await channel.CloseChannel(cancellationToken).ConfigureAwait(false);
                await channel.EstablishNewChannel(cancellationToken).ConfigureAwait(false);
                isEstablished = true;
            }
            finally
            {
                Interlocked.Exchange(ref _isReconnecting, 0);

                lock (_stateLock)
                {
                    // The events that the channel raised while it was being established have been waiting for this, and they are applied now
                    if (isEstablished)
                        _isChannelEstablished = true;

                    if (isEstablished || ChannelState is ChannelState.ClosedByServer or ChannelState.Disconnected)
                        CalculateState();
                }
            }
        });
    }

    private async Task ProcessActions(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var func = await ActionQueue.Dequeue(cancellationToken).ConfigureAwait(false);
                await func.Invoke(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Ignore
            }
        }
    }
}
