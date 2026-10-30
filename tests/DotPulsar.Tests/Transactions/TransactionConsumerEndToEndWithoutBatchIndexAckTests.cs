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

namespace DotPulsar.Tests.Transactions;

// Upstream: TransactionEndToEndWithoutBatchIndexAckTest
// The broker has acknowledgmentAtBatchIndexLevelEnabled=false
[Collection("TransactionWithoutBatchIndexAck"), Trait("Category", "Integration")]
public sealed class TransactionConsumerEndToEndWithoutBatchIndexAckTests : TransactionConsumerEndToEndTestsBase<TransactionWithoutBatchIndexAckFixture>
{
    public TransactionConsumerEndToEndWithoutBatchIndexAckTests(TransactionWithoutBatchIndexAckFixture fixture, ITestOutputHelper outputHelper) : base(fixture, outputHelper) { }

    protected override string SkipReasonForUnackMessageCountTests => "Disabled upstream in TransactionEndToEndWithoutBatchIndexAckTest (enabled = false)";

    protected override string SkipReasonForBatchAndFailoverIndividualAck => "Upstream turns acknowledgmentAtBatchIndexLevelEnabled on at runtime for this test, and the same scenario runs in TransactionConsumerEndToEndTests";
}
