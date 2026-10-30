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

namespace DotPulsar.Internal.Batching;

using Microsoft.Extensions.ObjectPool;

public sealed class PreparedBatchPool
{
    public const int DefaultMaximumRetained = 64;

    private readonly ObjectPool<PreparedBatch> _pool;

    static PreparedBatchPool()
        => Shared = new PreparedBatchPool(DefaultMaximumRetained);

    public PreparedBatchPool(int maximumRetained)
        => _pool = new DefaultObjectPool<PreparedBatch>(new Policy(this), maximumRetained);

    public static PreparedBatchPool Shared { get; }

    public PreparedBatch Get()
    {
        var batch = _pool.Get();
        batch.MarkRented();
        return batch;
    }

    // Called by PreparedBatch.Return()
    internal void Return(PreparedBatch batch)
        => _pool.Return(batch);

    private sealed class Policy : PooledObjectPolicy<PreparedBatch>
    {
        private readonly PreparedBatchPool _owner;

        public Policy(PreparedBatchPool owner)
            => _owner = owner;

        public override PreparedBatch Create()
            => new(_owner);

        public override bool Return(PreparedBatch obj)
        {
            obj.Reset();
            return true;
        }
    }
}
