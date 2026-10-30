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

using DotPulsar.Extensions;
using System.Text;
using System.Text.Json;

[Collection("AuthenticatedTransaction"), Trait("Category", "Integration")]
public sealed class AuthenticatedTransactionFixtureTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly AuthenticatedTransactionFixture _fixture;

    public AuthenticatedTransactionFixtureTests(AuthenticatedTransactionFixture fixture)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
    }

    // Additional coverage
    [Fact]
    public async Task AuthenticatedTransactionFixture_ShouldApplyTheBrokerConfiguration()
    {
        //Act
        var configuration = await _fixture.GetRuntimeConfiguration(_cts.Token);

        //Assert
        configuration.GetProperty("authenticationEnabled").GetString().ShouldBe("true");
        configuration.GetProperty("authorizationEnabled").GetString().ShouldBe("true");
        configuration.GetProperty("transactionCoordinatorEnabled").GetString().ShouldBe("true");
        configuration.GetProperty("superUserRoles").GetString()!.Trim('[', ']').Split(',', StringSplitOptions.TrimEntries).ShouldBe(["test-user", "admin"], ignoreOrder: true);
    }

    // Additional coverage
    [Fact]
    public async Task AuthenticatedTransactionFixture_ShouldGrantThePermissionsOfTheRoles()
    {
        //Act
        var systemPermissions = await _fixture.GetNamespacePermissions(AuthenticatedTransactionFixture.SystemNamespace, _cts.Token);
        var permissions = await _fixture.GetNamespacePermissions(TransactionFixtureBase.Namespace, _cts.Token);

        //Assert
        GetActions(systemPermissions, "client").ShouldBe(["consume"]);
        GetActions(systemPermissions, "client2").ShouldBe(["consume"]);
        GetActions(permissions, "client").ShouldBe(["produce", "consume", "functions", "sources", "sinks", "packages"], ignoreOrder: true);
        permissions.TryGetProperty("client2", out _).ShouldBeFalse();
    }

    // Additional coverage: the tests use the unload of the system namespace instead of the call of the broker that removes the transaction coordinator in the Java tests
    [Fact]
    public async Task UnloadNamespace_GivenSystemNamespace_ShouldRemoveTheTransactionCoordinatorUntilAClientConnects()
    {
        //Arrange
        await using var client = _fixture.CreateClient();
        var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var isLoadedBefore = await _fixture.IsTransactionCoordinatorLoaded(0, _cts.Token);

        //Act
        await _fixture.UnloadNamespace(AuthenticatedTransactionFixture.SystemNamespace, _cts.Token);
        var isLoadedAfterUnload = await _fixture.IsTransactionCoordinatorLoaded(0, _cts.Token);
        await using var otherClient = _fixture.CreateClient();
        var otherTransaction = await otherClient.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token);
        var stats = await _fixture.GetCoordinatorStats(0, _cts.Token);

        //Assert
        isLoadedBefore.ShouldBeTrue();
        isLoadedAfterUnload.ShouldBeFalse();
        stats.GetProperty("state").GetString().ShouldBe("Ready");

        await transaction.Abort(_cts.Token);
        await otherTransaction.Abort(_cts.Token);
    }

    // Additional coverage
    [Theory]
    [InlineData("client")]
    [InlineData("client2")]
    [InlineData("admin")]
    public async Task CreateToken_GivenSubject_ShouldCreateTokenOfOneHourForTheSubject(string subject)
    {
        //Act
        var token = await _fixture.CreateToken(subject, _cts.Token);

        //Assert
        var payload = JsonDocument.Parse(DecodeBase64Url(token.Split('.')[1])).RootElement;
        payload.GetProperty("sub").GetString().ShouldBe(subject);
        (payload.GetProperty("exp").GetInt64() - DateTimeOffset.UtcNow.ToUnixTimeSeconds()).ShouldBeInRange(3_000, 3_600);
    }

    private static List<string> GetActions(JsonElement permissions, string role)
        => permissions.GetProperty(role).EnumerateArray().Select(action => action.GetString()!).ToList();

    private static string DecodeBase64Url(string value)
    {
        var base64 = value.Replace('-', '+').Replace('_', '/');
        return Encoding.UTF8.GetString(Convert.FromBase64String(base64.PadRight(base64.Length + (4 - base64.Length % 4) % 4, '=')));
    }

    public void Dispose() => _cts.Dispose();
}
