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

namespace DotPulsar.Internal;

/// <summary>
/// Finds what an extension method needs that is not a member of the public interface it extends. Members that are added to an interface after it was released would break every implementation of the interface
/// outside of DotPulsar, so they are extension methods that call a capability interface (IExtendedConsumer, IExtendedProducer, IExtendedPulsarClient) or the builder that DotPulsar creates.
/// </summary>
internal static class ExtendedApi
{
    public static T Require<T>(object? instance, string parameterName, string member) where T : class
    {
        if (instance is null)
            throw new ArgumentNullException(parameterName);

        if (instance is T supported)
            return supported;

        throw new NotSupportedException($"'{member}' is not supported by '{instance.GetType()}'. It is only supported by the implementations that DotPulsar creates.");
    }
}
