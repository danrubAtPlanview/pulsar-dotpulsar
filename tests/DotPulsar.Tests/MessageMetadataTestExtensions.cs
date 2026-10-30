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

using System.Reflection;

public static class MessageMetadataTestExtensions
{
    private static readonly PropertyInfo _metadataProperty = typeof(MessageMetadata).GetProperty("Metadata", BindingFlags.Instance | BindingFlags.NonPublic)!;

    // The protocol metadata is internal, and the library does not expose its internals to the tests
    public static DotPulsar.Internal.PulsarApi.MessageMetadata ProtocolMetadata(this MessageMetadata metadata)
        => (DotPulsar.Internal.PulsarApi.MessageMetadata) _metadataProperty.GetValue(metadata)!;
}
