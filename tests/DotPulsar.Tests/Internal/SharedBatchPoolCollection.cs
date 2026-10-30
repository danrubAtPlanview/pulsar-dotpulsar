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

// The tests that look at the state of a batch of a producer. The producers rent their batches from one pool that is shared by the whole process, so a batch that has been returned is rented again by
// any test that runs in parallel and sends a batch. These tests do not run in parallel with other tests.
[CollectionDefinition("SharedBatchPool", DisableParallelization = true)]
public sealed class SharedBatchPoolCollection;
