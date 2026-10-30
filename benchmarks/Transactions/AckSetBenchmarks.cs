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

namespace Transactions;

using BenchmarkDotNet.Attributes;
using DotPulsar.Internal;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf.Collections;

/// <summary>
/// The ack set benchmarks build the ack set of a batch in the same way as the consumer does when it acknowledges a message of a batch, without a broker and without a connection.
/// The ack set goes into the RepeatedField of a MessageIdData that is reused, as the field of a pooled command is. A warm-up call is done in the setup, so the field has its array.
/// <para>
/// Threshold (documented in the spec 07-performance.md): building an ack set allocates 0 bytes.
/// BuildAckSet_64 and BuildAckSet_1000 write an individual and a cumulative ack set for every message index of a batch of 64 and of 1000 messages (AckSetWriter, which is what a
/// transactional batch index acknowledgment uses). Each operation is one such pair of ack sets.
/// BuildAckSet_Tracker_64 and BuildAckSet_Tracker_1000 acknowledge a message in the BatchAckTracker of a batch and copy its ack set (which is what a non-transactional batch index
/// acknowledgment uses).
/// </para>
/// <para>
/// Measured (Medium job, in-process, Release, .NET 10, Intel Core Ultra 9 185H), two runs. The threshold is met: every method allocates 0 B. The times change by about 10-20% between the runs.
/// <code>
/// BuildAckSet_64              22.3-22.8 ns    0 B
/// BuildAckSet_1000            65.0-78.7 ns    0 B
/// BuildAckSet_Tracker_64       5.1-5.7 ns     0 B
/// BuildAckSet_Tracker_1000    17.5-20.0 ns    0 B
/// </code>
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AckSetBenchmarks
{
    private readonly RepeatedField<long> _target = [];
    private BatchAckTracker? _tracker64;
    private BatchAckTracker? _tracker1000;

    [GlobalSetup]
    public void Setup()
    {
        var entryId = new MessageIdData { LedgerId = 1, EntryId = 1 };

        // Every second message is acknowledged, so the ack set of the tracker has bits to copy
        _tracker64 = new BatchAckTracker(entryId, 64, null);
        _tracker1000 = new BatchAckTracker(entryId, 1000, null);

        for (var i = 0; i < 64; i += 2)
            _tracker64.AckIndividual(i);

        for (var i = 0; i < 1000; i += 2)
            _tracker1000.AckIndividual(i);

        // The field keeps its array when it is cleared, so it only has to be as big as the biggest ack set
        AckSetWriter.WriteIndividual(_target, 1000, 0);
    }

    [Benchmark(OperationsPerInvoke = 64)]
    public void BuildAckSet_64()
        => BuildAckSets(64);

    [Benchmark(OperationsPerInvoke = 1000)]
    public void BuildAckSet_1000()
        => BuildAckSets(1000);

    [Benchmark(OperationsPerInvoke = 64)]
    public void BuildAckSet_Tracker_64()
        => CopyAckSets(_tracker64!, 64);

    [Benchmark(OperationsPerInvoke = 1000)]
    public void BuildAckSet_Tracker_1000()
        => CopyAckSets(_tracker1000!, 1000);

    private void BuildAckSets(int batchSize)
    {
        for (var i = 0; i < batchSize; ++i)
        {
            AckSetWriter.WriteIndividual(_target, batchSize, i);
            AckSetWriter.WriteCumulative(_target, batchSize, i);
        }
    }

    private void CopyAckSets(BatchAckTracker tracker, int batchSize)
    {
        for (var i = 0; i < batchSize; ++i)
        {
            // Acknowledging an index that is already acknowledged changes nothing, so the tracker keeps the same ack set
            _ = tracker.AckIndividual(i & ~1);
            tracker.CopyAckSetTo(_target);
        }
    }
}
