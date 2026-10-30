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

using System.Reflection;

// The batch size of a MessageId is internal, and the library has no InternalsVisibleTo
public static class MessageIdInternals
{
    private static readonly PropertyInfo _batchSize = typeof(MessageId).GetProperty("BatchSize", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("MessageId.BatchSize was not found");

    public static int GetBatchSize(this MessageId messageId)
        => (int) _batchSize.GetValue(messageId)!;

    public static MessageId Create(ulong ledgerId, ulong entryId, int partition, int batchIndex, string topic, int batchSize)
        => (MessageId) Activator.CreateInstance(
            typeof(MessageId),
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            [ledgerId, entryId, partition, batchIndex, topic, batchSize],
            null)!;
}
