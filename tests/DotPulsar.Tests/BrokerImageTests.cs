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

// The functions that read the variables are tested with the value of the variable as a parameter, because a test must not change the environment of the process that the integration tests read
[Trait("Category", "Unit")]
public sealed class BrokerImageTests
{
    // Additional coverage (followup 0002)
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void GetName_GivenNoImage_ShouldReturnTheDefaultImage(string? configured)
        => BrokerImage.GetName(configured).ShouldBe("apachepulsar/pulsar:4.0.9");

    // Additional coverage (followup 0002)
    [Fact]
    public void GetName_GivenImage_ShouldReturnTheImage()
        => BrokerImage.GetName(" apachepulsar/pulsar:4.0.15 ").ShouldBe("apachepulsar/pulsar:4.0.15");

    // Additional coverage (followup 0002)
    [Theory]
    [InlineData(null, 27128, false)]
    [InlineData("", 27128, false)]
    [InlineData("27128", 27128, true)]
    [InlineData("27128", 27082, false)]
    [InlineData("27128,27082", 27082, true)]
    [InlineData("27128, 27082", 27082, true)]
    [InlineData("#27128;#27082", 27082, true)]
    [InlineData("27128 27082", 27128, true)]
    [InlineData("271280", 27128, false)]
    [InlineData("27128x,abc", 27128, false)]
    public void HasFix_GivenFixes_ShouldFindTheNumber(string? fixes, int pullRequest, bool expected)
        => BrokerImage.HasFix(fixes, pullRequest).ShouldBe(expected);
}
