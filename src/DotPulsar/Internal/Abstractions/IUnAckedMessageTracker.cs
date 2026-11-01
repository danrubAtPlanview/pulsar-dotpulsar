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

// What a sub-consumer, a consumer channel and the consumer need of the tracker that redelivers the messages that are not acknowledged in time
public interface IUnAckedMessageTracker : IDisposable
{
    // The number of entries that are waited for
    int Count { get; }

    // Start the timer. The first firing is one tick later. Does nothing if it is already started.
    void Start();

    // Track the entry of the message id, which is not tracked again if it is already tracked. The redelivery count is the one that the broker sent with the message.
    bool Add(MessageId messageId, uint redeliveryCount);

    bool Remove(MessageId messageId);

    bool Remove(ulong ledgerId, ulong entryId);

    // Remove every tracked entry up to and including the entry
    int RemoveMessagesTill(ulong ledgerId, ulong entryId);

    void Clear();
}
