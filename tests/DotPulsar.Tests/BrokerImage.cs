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

// The Pulsar image that the integration tests run against, and the broker fixes that it has.
// - DOTPULSAR_TESTS_PULSAR_IMAGE replaces the default image, for example to run the tests against another version of the broker.
// - DOTPULSAR_TESTS_BROKER_FIXES lists the numbers of the apache/pulsar pull requests that the image has and that the default image has not (comma separated, for example 27128,27082).
//   A test that needs such a fix is skipped unless its number is in the list, so the tests can be run against an image that has the fix as soon as there is one.
public static class BrokerImage
{
    private const string ImageVariable = "DOTPULSAR_TESTS_PULSAR_IMAGE";
    private const string FixesVariable = "DOTPULSAR_TESTS_BROKER_FIXES";
    private const string DefaultName = "apachepulsar/pulsar:4.0.9";

    // The broker answers an ack for a consumer that it no longer has with ConsumerNotFound
    public const int AckResponseForRemovedConsumer = 27128;

    // The broker keeps a plain message behind the transactional messages that the transaction buffer holds back
    public const int PlainPublishBehindTransactionalPublish = 27082;

    public static string Name => GetName(Environment.GetEnvironmentVariable(ImageVariable));

    public static bool HasFix(int pullRequest) => HasFix(Environment.GetEnvironmentVariable(FixesVariable), pullRequest);

    public static string GetName(string? configured)
        => string.IsNullOrWhiteSpace(configured) ? DefaultName : configured.Trim();

    public static bool HasFix(string? fixes, int pullRequest)
    {
        if (string.IsNullOrWhiteSpace(fixes))
            return false;

        return fixes
            .Split([',', ';', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(fix => fix.TrimStart('#'))
            .Any(fix => int.TryParse(fix, out var number) && number == pullRequest);
    }

    // Skips the test that calls it, unless the image has the fix
    public static void SkipUnlessItHasFix(int pullRequest, string description)
        => Assert.SkipWhen(!HasFix(pullRequest), $"Requires the broker fix apache/pulsar#{pullRequest} ({description}), which is not in {DefaultName}. Run against an image that has it with {FixesVariable}={pullRequest}");
}
