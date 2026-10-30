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

using DotPulsar.Tests.Internal.Crypto;
using System.Text;

[Trait("Category", "Unit")]
public sealed class DefaultCryptoKeyReaderTests : IDisposable
{
    // The (LF) data URIs of the upstream test. The first is the content of crypto_rsa_public.key and the second is the content of crypto_rsa_private.key.
    private const string RsaPublicDataUri =
        "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQU5CZ2txaGtpRzl3MEJBU"
        + "UVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF6elRUenNTc1pGWWxXeWJack1OdwphRGpncWluSU5vNXlOa0h1UkJQZzJyNTZCRWFIb1U1e"
        + "StjY0RoeXhCR0NLUFprVGNRYXN2WWdXSjNzSFJLQWxOCmRaTkc4R3QzazJTcmZEcnJ0ajFLTDNHNk5XUkE4VHF5Umt4eGw1dnBBT"
        + "WM2OVVqWDlIUHdTemxtckM3WlhtMWUKU3dZVFY3Kzdxcy82OUpMQm5yTUpjc2wrSXlYVWFoaFJuOHcyRmtzOUpXcmlOS2kxUFNnQ"
        + "1BqTWpnS0JGN3lhRQpBVEowR01TTWM4RnZYV3dGSnNXQldRa1V3Z3FsRXhSMU1EaVZWQnR3OVF0SkIyOUlOaTBORHMyUGViNjFEd"
        + "DQ5Ck5abE4va2xKQ1hJVXRCU0lxZzlvK2lSS1Z3WExIbklNMFdIVm5tUm4yTUswbmYwMy9Ed0NJVm5iNWVsVG9aNzIKOHdJREFRQ"
        + "UIKLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg==";

    private const string RsaPrivateDataUri =
        "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBe"
        + "npUVHpzU3NaRllsV3liWnJNTndhRGpncWluSU5vNXlOa0h1UkJQZzJyNTZCRWFICm9VNXkrY2NEaHl4QkdDS1Baa1RjUWFzdllnV"
        + "0ozc0hSS0FsTmRaTkc4R3QzazJTcmZEcnJ0ajFLTDNHNk5XUkEKOFRxeVJreHhsNXZwQU1jNjlValg5SFB3U3psbXJDN1pYbTFlU"
        + "3dZVFY3Kzdxcy82OUpMQm5yTUpjc2wrSXlYVQphaGhSbjh3MkZrczlKV3JpTktpMVBTZ0NQak1qZ0tCRjd5YUVBVEowR01TTWM4R"
        + "nZYV3dGSnNXQldRa1V3Z3FsCkV4UjFNRGlWVkJ0dzlRdEpCMjlJTmkwTkRzMlBlYjYxRHQ0OU5abE4va2xKQ1hJVXRCU0lxZzlvK"
        + "2lSS1Z3WEwKSG5JTTBXSFZubVJuMk1LMG5mMDMvRHdDSVZuYjVlbFRvWjcyOHdJREFRQUJBb0lCQUhXRnZmaVJua0dPaHNPTApab"
        + "npSb01qTU1janh4OGdCeFErM0YxL3ZjbUkvRk0rbC9UbGxXRnNKSUp3allveEExZHFvaGRDTk9tTzdSblpjCnNiZW1oeE4veEFXS"
        + "3ZwaVB5Wis5ZjRHdWc0d2pVZjBFYnIwamtJZkV4Y3k2dGs0bHNlLzdMOWxMaE9mMWw2RmoKTlJDVXNaMlZ4WlRJZjdXakh2Qm02S"
        + "UNOaFhkZmdjL1RPWC9INEJCTXh5UWtrbXZTN3lRSFBtbmVrVnBDandYaQpSZ2RQT3BCU0hVQXN1TGMzY2RPN0R6U2xYQnkrUjNVQ"
        + "jViQzk3ZWZTVHd4bU1kY0dVTlFoMTdDdXcrb3UyT0xKCmwvV3lNQkpnS1AwenA4TUkyWUNQMHRvRTFWVjBGV2lzaU5VZHl3Mm1tZ"
        + "HNLQlBDdFpXNEpmL2F2UkxqQ3B5ODMKZ3llSGk0a0NnWUVBN2ZhYzh1L1dvVWNSeGZ3YmV4eFZOSWI1anBWZ1EyMlhYVXZjT0JBM"
        + "zE0NUhGSDRrRDlRcwpPbE9NNDhpRVgxR3ErRk9iK1RrVmEzeWVFYnlFSFZtTnhtN1pxREVsR2xQbkhIZ1dKZlZvNGx0ZW1rTlE4Y"
        + "1FJCkNpRGhVSDdEOWlHZDRUckxxK3U4Slkvb3kwZHBKeWFKL0dzTlB3alZ6TWlBOWtEdUkyS0tScGNDZ1lFQTN1bHAKc1p5ODJFa"
        + "WJUK1lZTnNoZkF5elBjUWMrQ1ZkY3BTM3lFVU1jTXJyR1QySUhiQWhsZHpBV3ZuaTdQRjFDdllvQgplb1BTR2lFR01Bd0FmYVVJN"
        + "HBzWnZBVFpUZitHV0tpemxIODIwbHc0dFkyTlcydFlGd0RjWjZFUEtkcTlaQ096CkxmeXcyTmhMcWkyRnBGeUFwY1hsQTkyVVVJM"
        + "EZjWENDdEFLSjJnVUNnWUVBc0k1bWVyVktpTlRETWlOUWZISlUKSWFuM3BUdmRkWW50WVhKMGpVQXpQb0s0NkZLRERSOStSVFJTZ"
        + "DNzQ0Evc0RJRVpnbG5RdEdWZ1hxODgwTXRhTQpJMnVCb0pIK0ZsK2tQUEk0ZEtkMXoyUzlkelYwN0R4blBxU1FwL20yQ1h0OXVXd"
        + "TNTL0tXNFVPNkZJRUNXdUwwClJFMWxRWnliak5wREhQS2wvYWtTTVRjQ2dZQnFDODBXakNSakdKZWF1VEpIemFjMTBYbVdvZ1ZuV"
        + "0VKZzZxekEKZlpiS280UjRlNEJnYXRZcWo1d2lYVGxtREZBVjc3T29YMUh5MEVjclVHcGpXOElRWEEwd0gzWnAzdWhCQVhEOQpja"
        + "y9ZWDdzeTAvYXR5VEdOTUFHcTR6cGRoUXlZdVVzaTA1WW1jeS83ODlBaVUwZDRsZDdQcWZoSEllKzIrZm1VClBhanJLUUtCZ1FDa"
        + "nBqMFkrRS9IaTBTSlVLTlZQbDN1K1NOMWxMY1IxL2dNakpic1lGR3VhMU1zTFhCTlhUU2wKUWlZSGlhZFQ3QmhQRWtGZFc3dStiR"
        + "ndzMmFkbVcxOUJvVWIrd2d0WlQvdDduVHlvUzRMYWc0dnlhek5QWnpkUQp4NlhQcndaaW1kMFhERGl0R0xqY0xmOTkxRUZzWFNxU"
        + "HpuRERmWHRKMzErb1U2T2JuSFNJdEE9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=";

    private static readonly IReadOnlyDictionary<string, string> _noMetadata = new Dictionary<string, string>();

    private readonly string _directory;

    public DefaultCryptoKeyReaderTests()
    {
        _directory = Path.Combine(Environment.CurrentDirectory, $"crypto-keys-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
        => Directory.Delete(_directory, true);

    private string WriteKey(string name, string content)
    {
        var path = Path.Combine(_directory, name);
        File.WriteAllText(path, content + "\n", new UTF8Encoding(false));
        return path;
    }

    private string RelativeUri(string path)
        => "file:./" + Path.GetRelativePath(Environment.CurrentDirectory, path).Replace('\\', '/');

    private static byte[] Bytes(string content)
        => new UTF8Encoding(false).GetBytes(content + "\n");

    // Upstream: DefaultCryptoKeyReaderTest#testBuild
    [Fact]
    public void GetKeys_GivenDefaultsAndKeyNameUris_ShouldUseTheKeyNameUriBeforeTheDefault()
    {
        //Arrange
        var defaultPublic = WriteKey("default-public.key", "default-public");
        var defaultPrivate = WriteKey("default-private.key", "default-private");
        var public1 = WriteKey("public1.key", "public1");
        var public2 = WriteKey("public2.key", "public2");
        var private3 = WriteKey("private3.key", "private3");
        var uut = new DefaultCryptoKeyReader(
            "file:" + defaultPublic,
            "file:" + defaultPrivate,
            new Dictionary<string, string> { ["key1"] = "file:" + public1, ["key2"] = "file:" + public2 },
            new Dictionary<string, string> { ["key3"] = "file:" + private3 });

        //Act
        var actualPublic1 = uut.GetPublicKey("key1", _noMetadata);
        var actualPublic2 = uut.GetPublicKey("key2", _noMetadata);
        var actualPublic3 = uut.GetPublicKey("key3", _noMetadata);
        var actualPrivate1 = uut.GetPrivateKey("key1", _noMetadata);
        var actualPrivate3 = uut.GetPrivateKey("key3", _noMetadata);

        //Assert
        actualPublic1!.Key.ShouldBe(Bytes("public1"));
        actualPublic2!.Key.ShouldBe(Bytes("public2"));
        actualPublic3!.Key.ShouldBe(Bytes("default-public"));
        actualPrivate1!.Key.ShouldBe(Bytes("default-private"));
        actualPrivate3!.Key.ShouldBe(Bytes("private3"));
    }

    [Fact]
    public void GetKeys_GivenDictionariesThatAreChangedAfterwards_ShouldNotSeeTheChanges()
    {
        //Arrange
        var path = WriteKey("public1.key", "public1");
        var publicKeys = new Dictionary<string, string> { ["key1"] = "file:" + path };
        var uut = new DefaultCryptoKeyReader(null, null, publicKeys);

        //Act
        publicKeys["key1"] = "file:invalid";
        publicKeys["key2"] = "file:" + path;

        //Assert
        uut.GetPublicKey("key1", _noMetadata)!.Key.ShouldBe(Bytes("public1"));
        uut.GetPublicKey("key2", _noMetadata).ShouldBeNull();
    }

    // Upstream: DefaultCryptoKeyReaderTest#testGetKeys
    [Fact]
    public void GetKeys_GivenNoUris_ShouldReturnNull()
    {
        //Arrange
        var uut = new DefaultCryptoKeyReader(null, null);

        //Act
        var actualPublic = uut.GetPublicKey("key0", _noMetadata);
        var actualPrivate = uut.GetPrivateKey("key0", _noMetadata);

        //Assert
        actualPublic.ShouldBeNull();
        actualPrivate.ShouldBeNull();
    }

    // Upstream: DefaultCryptoKeyReaderTest#testGetKeys
    [Fact]
    public void GetKeys_GivenFileAndDataUris_ShouldReturnTheKeysOrNull()
    {
        //Arrange
        var ecdsaPublic = WriteKey("crypto_ecdsa_public.key", CryptoTestKeys.CryptoEcdsaPublic);
        var ecdsaPrivate = WriteKey("crypto_ecdsa_private.key", CryptoTestKeys.CryptoEcdsaPrivate);
        var uut = new DefaultCryptoKeyReader(
            RelativeUri(ecdsaPublic),
            RelativeUri(ecdsaPrivate),
            new Dictionary<string, string>
            {
                ["key1"] = RsaPublicDataUri,
                ["key2"] = "file:invalid",
                ["key3"] = "data:invalid"
            },
            new Dictionary<string, string>
            {
                ["key1"] = RsaPrivateDataUri,
                ["key2"] = "file:invalid",
                ["key3"] = "data:invalid"
            });

        //Act
        var actualPublic0 = uut.GetPublicKey("key0", _noMetadata);
        var actualPrivate0 = uut.GetPrivateKey("key0", _noMetadata);
        var actualPublic1 = uut.GetPublicKey("key1", _noMetadata);
        var actualPrivate1 = uut.GetPrivateKey("key1", _noMetadata);

        //Assert
        actualPublic0!.Key.ShouldBe(Bytes(CryptoTestKeys.CryptoEcdsaPublic));
        actualPrivate0!.Key.ShouldBe(Bytes(CryptoTestKeys.CryptoEcdsaPrivate));
        actualPublic1!.Key.ShouldBe(Bytes(CryptoTestKeys.CryptoRsaPublic));
        actualPrivate1!.Key.ShouldBe(Bytes(CryptoTestKeys.CryptoRsaPrivate));
        uut.GetPublicKey("key2", _noMetadata).ShouldBeNull();
        uut.GetPrivateKey("key2", _noMetadata).ShouldBeNull();
        uut.GetPublicKey("key3", _noMetadata).ShouldBeNull();
        uut.GetPrivateKey("key3", _noMetadata).ShouldBeNull();
    }

    [Fact]
    public void GetPublicKey_GivenKeyInfo_ShouldHaveEmptyMetadata()
    {
        //Arrange
        var uut = new DefaultCryptoKeyReader(RsaPublicDataUri, null);

        //Act
        var actual = uut.GetPublicKey("key", _noMetadata);

        //Assert
        actual!.Metadata.ShouldBeEmpty();
    }

    [Fact]
    public void GetPublicKey_GivenAbsoluteFileUriWithTwoSlashes_ShouldReturnTheKey()
    {
        //Arrange
        var path = WriteKey("public.key", "public");
        var uut = new DefaultCryptoKeyReader(new Uri(path).AbsoluteUri, null);

        //Act
        var actual = uut.GetPublicKey("key", _noMetadata);

        //Assert
        actual!.Key.ShouldBe(Bytes("public"));
    }

    [Fact]
    public void GetPublicKey_GivenAbsoluteFilePathAfterTheScheme_ShouldReturnTheKey()
    {
        //Arrange
        var path = WriteKey("public.key", "public");
        var uut = new DefaultCryptoKeyReader("file:" + path, null);

        //Act
        var actual = uut.GetPublicKey("key", _noMetadata);

        //Assert
        actual!.Key.ShouldBe(Bytes("public"));
    }

    [Fact]
    public void GetPublicKey_GivenUppercaseScheme_ShouldReturnTheKey()
    {
        //Arrange
        var path = WriteKey("public.key", "public");
        var uut = new DefaultCryptoKeyReader("FILE:" + path, null);

        //Act
        var actual = uut.GetPublicKey("key", _noMetadata);

        //Assert
        actual!.Key.ShouldBe(Bytes("public"));
    }

    [Theory]
    [InlineData("file:")]
    [InlineData("file:does-not-exist.pem")]
    [InlineData("file:///does-not-exist.pem")]
    [InlineData("data:")]
    [InlineData("data:invalid")]
    [InlineData("data:,AAAA")]
    [InlineData("data:text/plain;base64,AAAA")]
    [InlineData("data:application/x-pem-file,AAAA")]
    [InlineData("data:application/x-pem-file;charset=utf-8;base64,AAAA")]
    [InlineData("data:application/x-pem-file;base64,@@@@")]
    [InlineData("http://localhost/key.pem")]
    [InlineData("https://localhost/key.pem")]
    [InlineData("ftp://localhost/key.pem")]
    [InlineData("key.pem")]
    [InlineData("")]
    public void GetPublicKey_GivenUriThatCannotBeLoaded_ShouldReturnNull(string uri)
    {
        //Arrange
        var uut = new DefaultCryptoKeyReader(uri, uri, new Dictionary<string, string> { ["key"] = uri }, new Dictionary<string, string> { ["key"] = uri });

        //Act
        var actualPublic = uut.GetPublicKey("key", _noMetadata);
        var actualPrivate = uut.GetPrivateKey("key", _noMetadata);
        var actualDefault = uut.GetPublicKey("other", _noMetadata);

        //Assert
        actualPublic.ShouldBeNull();
        actualPrivate.ShouldBeNull();
        actualDefault.ShouldBeNull();
    }

    [Fact]
    public void GetPublicKey_GivenDataUriWithPemMediaTypeInUpperCase_ShouldReturnTheKey()
    {
        //Arrange
        var uri = "data:APPLICATION/X-PEM-FILE;BASE64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("key"));
        var uut = new DefaultCryptoKeyReader(uri, null);

        //Act
        var actual = uut.GetPublicKey("key", _noMetadata);

        //Assert
        actual!.Key.ShouldBe(Encoding.UTF8.GetBytes("key"));
    }

    [Fact]
    public void GetKeys_GivenKeyNameThatIsInTheWrongDictionary_ShouldUseTheDefault()
    {
        //Arrange
        var uut = new DefaultCryptoKeyReader(
            "data:application/x-pem-file;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("default-public")),
            "data:application/x-pem-file;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("default-private")),
            privateKeys: new Dictionary<string, string> { ["key"] = "data:application/x-pem-file;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("private")) });

        //Act
        var actualPublic = uut.GetPublicKey("key", _noMetadata);
        var actualPrivate = uut.GetPrivateKey("key", _noMetadata);

        //Assert
        Encoding.UTF8.GetString(actualPublic!.Key).ShouldBe("default-public");
        Encoding.UTF8.GetString(actualPrivate!.Key).ShouldBe("private");
    }
}
