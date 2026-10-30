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

namespace DotPulsar.Tests.Encryption;

// These tests use the 'pulsar-client' CLI (the Java client) of the apachepulsar/pulsar image together with a DotPulsar producer or consumer.
// They need the encryption options of the producer, consumer and reader builders and the encrypt and decrypt steps of the channels (WP9).
// The byte-exact compatibility with the Java client is already covered without a broker by MessageCryptoJavaInteropTests.
[Trait("Category", "Integration")]
public sealed class EncryptionInteropTests
{
    private const string SkipReason = "Requires encryption wiring (WP9)";

    [Fact(Skip = SkipReason)]
    public void Receive_GivenJavaProducedRsaEncryptedMessage_ShouldDecrypt()
    {
        // bin/pulsar-client produce {topic} -m "java-rsa-0" -n 1 -ekn client-rsa.pem -ekv file:///pulsar/keys/public-key.client-rsa.pem
        // A consumer (Earliest) with DefaultCryptoKeyReader(null, private key) receives "java-rsa-0" and IsEncrypted is false
    }

    [Fact(Skip = SkipReason)]
    public void Receive_GivenJavaProducedEcdsaEncryptedMessage_ShouldDecrypt()
    {
        // The same as the RSA test with client-ecdsa.pem
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedRsaEncryptedMessage_ShouldBeDecryptedByJavaCli()
    {
        // A producer with the key client-rsa.pem sends "dotpulsar-rsa-0"
        // bin/pulsar-client consume {topic} -s interop -p Earliest -n 1 -ekv file:///pulsar/keys/private-key.client-rsa.pem exits with 0 and prints 'content:dotpulsar-rsa-0'
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedEcdsaEncryptedMessage_ShouldBeDecryptedByJavaCli()
    {
        // The same as the RSA test with client-ecdsa.pem
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedEncryptedLz4Message_ShouldBeDecryptedByJavaCli()
    {
        // The same as the RSA test with CompressionType.Lz4 (compress, then encrypt)
    }

    [Fact(Skip = SkipReason)]
    public void Consume_GivenDotPulsarProducedEncryptedMessageWithoutKey_ShouldNotBeDelivered()
    {
        // bin/pulsar-client consume with '-ca DISCARD' and '-n 1' (wrapped in a 10 second timeout) prints nothing and the message is acknowledged (msgBacklog is 0)
    }
}
