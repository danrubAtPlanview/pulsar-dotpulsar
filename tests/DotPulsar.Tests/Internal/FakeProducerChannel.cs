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

using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Batching;
using System.Buffers;
using System.Collections.Concurrent;

// A channel that writes nothing, but records the entries (a message, or a batch of messages) it was asked to write, and answers them
public sealed class FakeProducerChannel : IProducerChannel
{
    private readonly ConcurrentQueue<SentEntry> _entries = new();
    private long _nextEntryId;
    private int _planCount;

    // Decides the answer to an entry. By default it is a receipt. Returning null leaves the entry unanswered, so it can be answered by the test.
    public Func<SentEntry, BaseCommand?>? Respond { get; set; }

    // Decides if writing an entry fails
    public Func<SentEntry, Exception?>? Fail { get; set; }

    // The write of an entry is not completed before this task is. Null means that it is completed at once.
    public Task? WriteGate { get; set; }

    // With a write gate, the entry is answered when it is written (before the gate is open) and not after the gate, like a receipt that comes before the write of the connection has returned
    public bool RespondBeforeWriteCompletes { get; set; }

    // Decides if planning the chunks of a message fails
    public Func<MessageMetadata, Exception?>? FailPlanning { get; set; }

    public int MaxMessageSize { get; set; } = 5 * 1024 * 1024;

    // The number of times the chunks of a message were planned
    public int PlanCount => Volatile.Read(ref _planCount);

    public IReadOnlyList<SentEntry> Entries => _entries.ToArray();

    // The metadata object of the last entry as it was given to the channel. It belongs to a batch that is reused after the batch is completed.
    public MessageMetadata? LastGivenMetadata { get; private set; }

    public Task Send(MessageMetadata metadata, ReadOnlySequence<byte> payload, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
    {
        LastGivenMetadata = metadata;
        var entry = new SentEntry(metadata.Clone(), payload.ToArray(), responseTcs);

        var exception = Fail?.Invoke(entry);
        if (exception is not null)
            return Task.FromException(exception);

        _entries.Enqueue(entry);

        return Answer(entry, responseTcs, WriteGate);
    }

    public ChunkedSendState? PlanChunking(MessageMetadata metadata, ReadOnlySequence<byte> payload, int chunkMaxMessageSize)
    {
        _ = Interlocked.Increment(ref _planCount);

        var exception = FailPlanning?.Invoke(metadata);
        if (exception is not null)
            throw exception;

        if (metadata.HasNumChunksFromMsg)
            return null;

        var plannedMetadata = metadata.Clone();
        plannedMetadata.ProducerName = "fake-producer";
        return ChunkedSendState.Plan(plannedMetadata, payload, MaxMessageSize, chunkMaxMessageSize, 0);
    }

    // The entry is a copy of the chunk as it is given to the channel, because the metadata of the state is changed for the next chunk
    public Task SendChunk(ChunkedSendState state, int chunkId, TaskCompletionSource<BaseCommand> responseTcs, CancellationToken cancellationToken)
    {
        if (state.IsChunked)
            state.Metadata.ChunkId = chunkId;

        var entry = new SentEntry(state.Metadata.Clone(), state.GetChunk(chunkId).ToArray(), responseTcs);

        var exception = Fail?.Invoke(entry);
        if (exception is not null)
            return Task.FromException(exception);

        _entries.Enqueue(entry);

        return Answer(entry, responseTcs, WriteGate);
    }

    private Task Answer(SentEntry entry, TaskCompletionSource<BaseCommand> responseTcs, Task? writeGate)
    {
        if (writeGate is null)
            return Answer(entry, responseTcs);

        if (!RespondBeforeWriteCompletes)
            return AnswerAfterWrite(entry, responseTcs, writeGate);

        _ = Answer(entry, responseTcs);
        return writeGate;
    }

    private Task Answer(SentEntry entry, TaskCompletionSource<BaseCommand> responseTcs)
    {
        var response = Respond is null ? CreateReceipt(entry) : Respond(entry);
        if (response is not null)
            _ = responseTcs.TrySetResult(response);

        return Task.CompletedTask;
    }

    private async Task AnswerAfterWrite(SentEntry entry, TaskCompletionSource<BaseCommand> responseTcs, Task writeGate)
    {
        await writeGate.ConfigureAwait(false);
        await Answer(entry, responseTcs).ConfigureAwait(false);
    }

    public ValueTask ClosedByClient(CancellationToken cancellationToken)
        => new();

    public ValueTask DisposeAsync()
        => new();

    public BaseCommand CreateReceipt(SentEntry entry)
        => CreateReceipt(entry, (ulong) Interlocked.Increment(ref _nextEntryId));

    public static BaseCommand CreateReceipt(SentEntry entry, ulong entryId, int batchIndex = -1)
    {
        var receipt = new CommandSendReceipt { SequenceId = entry.SequenceId, MessageId = new MessageIdData { LedgerId = 1, EntryId = entryId } };

        if (batchIndex >= 0)
            receipt.MessageId.BatchIndex = batchIndex;

        return new BaseCommand { Type = BaseCommand.Types.Type.SendReceipt, SendReceipt = receipt };
    }

    public static BaseCommand CreateSendError(ulong sequenceId, ServerError error, string message)
        => new()
        {
            Type = BaseCommand.Types.Type.SendError,
            SendError = new CommandSendError { SequenceId = sequenceId, Error = error, Message = message }
        };
}

public sealed record SentEntry(MessageMetadata Metadata, byte[] Payload, TaskCompletionSource<BaseCommand> Response)
{
    public ulong SequenceId => Metadata.SequenceId;
    public ulong HighestSequenceId => Metadata.HasHighestSequenceId ? Metadata.HighestSequenceId : Metadata.SequenceId;
    public bool IsBatch => Metadata.HasNumMessagesInBatch;
    public int NumberOfMessages => Metadata.HasNumMessagesInBatch ? Metadata.NumMessagesInBatch : 1;

    // The sequence ids of the messages in the entry
    public ulong[] MessageSequenceIds
        => IsBatch
            ? BatchPayloadReader.Read(Payload, NumberOfMessages).Select(message => message.Metadata.SequenceId).ToArray()
            : [Metadata.SequenceId];
}
