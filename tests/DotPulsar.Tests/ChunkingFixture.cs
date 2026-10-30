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

namespace DotPulsar.Tests;

// A broker with a small maximum message size, so a message is split in chunks without being large. The Java tests set the maximum message size of the broker
// to 35, 50, 100 or 1024 bytes in the test itself, which can not be done on a running broker here, so these tests use one value that is small enough for all of them.
public sealed class ChunkingFixture : IntegrationFixture
{
    public const int MaxMessageSize = 100;

    public ChunkingFixture() : base(new Dictionary<string, string> { ["maxMessageSize"] = MaxMessageSize.ToString() }) { }
}
