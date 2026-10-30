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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotPulsar.Tests.Internal.Crypto;
using System.Text;

// Runs the Java client crypto (org.apache.pulsar.client.impl.crypto.MessageCryptoBc with BouncyCastle) in the apachepulsar/pulsar image.
// The broker is not started. The image has a JDK and the jars of the client.
public sealed class JavaCryptoFixture : IAsyncLifetime
{
    private const string Interop = """
        import java.nio.ByteBuffer;
        import java.nio.file.Files;
        import java.nio.file.Paths;
        import java.util.HashMap;
        import java.util.Map;
        import java.util.Set;
        import java.util.TreeSet;
        import org.apache.pulsar.client.api.CryptoKeyReader;
        import org.apache.pulsar.client.api.EncryptionKeyInfo;
        import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
        import org.apache.pulsar.common.api.proto.EncryptionKeys;
        import org.apache.pulsar.common.api.proto.KeyValue;
        import org.apache.pulsar.common.api.proto.MessageMetadata;

        public class Interop {

            static String hex(byte[] b) {
                StringBuilder sb = new StringBuilder();
                for (byte x : b) {
                    sb.append(String.format("%02x", x));
                }
                return sb.toString();
            }

            static byte[] unhex(String s) {
                byte[] r = new byte[s.length() / 2];
                for (int i = 0; i < r.length; i++) {
                    r[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
                }
                return r;
            }

            // encrypt <publicKeyFile> <keyName> <payloadHex>
            // decrypt <privateKeyFile> <keyName> <wrappedKeyHex> <ivHex> <ciphertextHex>
            public static void main(String[] a) throws Exception {
                String mode = a[0];
                byte[] keyBytes = Files.readAllBytes(Paths.get(a[1]));
                String keyName = a[2];

                CryptoKeyReader reader = new CryptoKeyReader() {
                    @Override
                    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
                        Map<String, String> m = new HashMap<>();
                        m.put("version", "1.0");
                        return new EncryptionKeyInfo(keyBytes, m);
                    }

                    @Override
                    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
                        return new EncryptionKeyInfo(keyBytes, new HashMap<>());
                    }
                };

                if (mode.equals("encrypt")) {
                    byte[] payload = unhex(a[3]);
                    MessageCryptoBc crypto = new MessageCryptoBc("interop", true);
                    Set<String> names = new TreeSet<>();
                    names.add(keyName);
                    crypto.addPublicKeyCipher(names, reader);
                    MessageMetadata md = new MessageMetadata();
                    ByteBuffer out = ByteBuffer.allocate(crypto.getMaxOutputSize(payload.length));
                    crypto.encrypt(names, reader, () -> md, ByteBuffer.wrap(payload), out);
                    byte[] enc = new byte[out.remaining()];
                    out.get(enc);
                    EncryptionKeys k = md.getEncryptionKeyAt(0);
                    System.out.println("key=" + k.getKey());
                    System.out.println("wrapped=" + hex(k.getValue()));
                    for (KeyValue kv : k.getMetadatasList()) {
                        System.out.println("meta=" + kv.getKey() + ":" + kv.getValue());
                    }
                    System.out.println("iv=" + hex(md.getEncryptionParam()));
                    System.out.println("ciphertext=" + hex(enc));
                } else if (mode.equals("decrypt")) {
                    byte[] wrapped = unhex(a[3]);
                    byte[] iv = unhex(a[4]);
                    byte[] ciphertext = unhex(a[5]);
                    MessageCryptoBc crypto = new MessageCryptoBc("interop", false);
                    MessageMetadata md = new MessageMetadata();
                    md.addEncryptionKey().setKey(keyName).setValue(wrapped);
                    md.setEncryptionParam(iv);
                    ByteBuffer out = ByteBuffer.allocate(crypto.getMaxOutputSize(ciphertext.length));
                    boolean ok = crypto.decrypt(() -> md, ByteBuffer.wrap(ciphertext), out, reader);
                    if (ok) {
                        byte[] dec = new byte[out.remaining()];
                        out.get(dec);
                        System.out.println("plaintext=" + hex(dec));
                    } else {
                        System.out.println("FAIL");
                    }
                }
            }
        }
        """;

    private static readonly string[] _keySets = ["ClientRsa", "ClientRsaPkcs8", "CryptoRsa", "ClientEcdsa", "CryptoEcdsa"];

    private readonly CancellationTokenSource _cts;
    private readonly IContainer _container;

    public JavaCryptoFixture()
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

        var builder = new ContainerBuilder("apachepulsar/pulsar:4.0.9")
            .WithEntrypoint("tail", "-f", "/dev/null")
            .WithResourceMapping(Encoding.UTF8.GetBytes(Interop), "/work/Interop.java");

        foreach (var keySet in _keySets)
        {
            builder = builder
                .WithResourceMapping(CryptoTestKeys.Public(keySet), $"/work/keys/{keySet}.pub")
                .WithResourceMapping(CryptoTestKeys.Private(keySet), $"/work/keys/{keySet}.key");
        }

        _container = builder.Build();
    }

    public async ValueTask InitializeAsync()
        => await _container.StartAsync(_cts.Token).ConfigureAwait(false);

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync().ConfigureAwait(false);
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    // The Java client encrypts the payload with a new data key that is wrapped with the public key
    public async Task<JavaEncrypted> Encrypt(string keySet, byte[] payload, CancellationToken cancellationToken)
    {
        var output = await Run(["encrypt", $"/work/keys/{keySet}.pub", keySet, Convert.ToHexString(payload)], cancellationToken).ConfigureAwait(false);

        return new JavaEncrypted(
            Convert.FromHexString(output["wrapped"]),
            Convert.FromHexString(output["iv"]),
            Convert.FromHexString(output["ciphertext"]),
            output.TryGetValue("meta", out var meta) ? meta : null);
    }

    // The Java client decrypts the payload. Returns null if the Java client cannot decrypt it.
    public async Task<byte[]?> Decrypt(string keySet, byte[] wrappedKey, byte[] iv, byte[] ciphertext, CancellationToken cancellationToken)
    {
        var output = await Run(["decrypt", $"/work/keys/{keySet}.key", keySet, Convert.ToHexString(wrappedKey), Convert.ToHexString(iv), Convert.ToHexString(ciphertext)], cancellationToken).ConfigureAwait(false);
        return output.TryGetValue("plaintext", out var plaintext) ? Convert.FromHexString(plaintext) : null;
    }

    private async Task<Dictionary<string, string>> Run(string[] arguments, CancellationToken cancellationToken)
    {
        string[] command = ["java", "-cp", "/pulsar/lib/*", "/work/Interop.java", .. arguments];
        var result = await _container.ExecAsync(command, cancellationToken).ConfigureAwait(false);

        if (result.ExitCode != 0)
            throw new InvalidOperationException($"The Java client failed with exit code {result.ExitCode}: {result.Stderr}");

        var output = new Dictionary<string, string>();

        foreach (var line in result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = line.Trim();
            var separator = trimmed.IndexOf('=');

            if (separator > 0)
                output[trimmed.Substring(0, separator)] = trimmed.Substring(separator + 1);
            else
                output[trimmed] = string.Empty;
        }

        return output;
    }

    public sealed record JavaEncrypted(byte[] WrappedKey, byte[] Iv, byte[] Ciphertext, string? KeyMetadata);
}
