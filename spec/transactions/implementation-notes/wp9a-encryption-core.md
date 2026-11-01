# WP9a as built: encryption core (not wired)

New files only; no NuGet package was added (BCL only; the explicit-curve test keys import on Windows). Findings and platform matrix: `followups/0003`.

## Signatures (namespace `DotPulsar.Internal.Crypto`; `MessageMetadata` below is `DotPulsar.Internal.PulsarApi.MessageMetadata`)

```csharp
public sealed class MessageCrypto : IDisposable
{
    public MessageCrypto(bool keyGenNeeded, TimeSpan? keyRefreshInterval = null, Func<TimeSpan>? clock = null);
    public void AddPublicKeyCipher(IEnumerable<string> keyNames, ICryptoKeyReader keyReader);
    public ReadOnlySequence<byte> Encrypt(MessageMetadata metadata, ReadOnlySequence<byte> payload);
    public bool TryDecrypt(MessageMetadata metadata, ReadOnlySequence<byte> payload, ICryptoKeyReader keyReader, out ReadOnlySequence<byte> decrypted);
    public int GetMetadataOverhead();
    public void Dispose();
}
public static class EncryptionContextFactory { public static EncryptionContext Create(MessageMetadata metadata, bool isEncrypted); }
```

## Producer wiring

- In `PulsarClient.CreateProducer`, when both keys and a reader are set: `new MessageCrypto(true).AddPublicKeyCipher(options.EncryptionKeys, options.CryptoKeyReader)`, synchronously.
  It throws `CryptoException`, or `PlatformNotSupportedException` on netstandard2.0 / where AES-GCM is unavailable.
- One `MessageCrypto` per `Producer`, disposed in `DisposeAsync`. The 4-hour refresh timer starts itself.
- In `ProducerChannel.Send`, after compression and chunking: `sendPackage.Payload = crypto.Encrypt(metadata, sendPackage.Payload)` and set `resetEncryption = true`.
  - `Encrypt` throws only `CryptoException` (or `ObjectDisposedException` after dispose) and leaves the metadata untouched on failure.
  - With the `Send` failure action, catch and send plaintext. With `Fail`, fail only that send.
- In the `finally`: `metadata.EncryptionKeys.Clear()` and `metadata.ClearEncryptionParam()`. The `EncryptionKeys` elements are shared across sends: never mutate them.
- Each call draws a fresh IV; the output is a fresh `byte[]`, so resends are safe.
- Chunk size is `maxMessageSize - metadata.CalculateSize() - crypto.GetMetadataOverhead()`. The overhead is the 16-byte tag plus the exact serialized `encryption_keys` fields plus 14 bytes for
  `encryption_param`. It returns 0 until keys are added.

## Consumer / reader wiring

- `new MessageCrypto(false)` per sub-consumer, with the `ICryptoKeyReader`.
- After `ExtractData` and before decompression, when `metadata.EncryptionKeys.Count > 0`, call `TryDecrypt`. It never throws except `ObjectDisposedException`.

| Outcome | Action |
|---|---|
| `true` | `data = decrypted`; `EncryptionContextFactory.Create(metadata, false)` |
| `false` with `Consume` | deliver the raw payload with `EncryptionContextFactory.Create(metadata, true)` |
| `false` with `Discard` | `RejectPackage(…, DecryptionError)` |
| `false` with `Fail` | neither ack nor deliver |

On netstandard2.0, `TryDecrypt` always returns false.

## Open items

- Interaction with chunking (WP8): an undecryptable chunk that is assembled still encrypted exceeds `total_chunk_msg_size` by 16 bytes per chunk and hits the assembler's overflow guard.
  Java would throw in the same case; WP9 must decide how to treat it (see `wp8a-chunking-core.md`).
- Six skipped interop tests (`EncryptionInteropTests`, `Skip = "Requires encryption wiring (WP9)"`), the §9.2 broker integration tests, the builder validation test and the
  `ConsumerChannelTests` undecryptable-message variants are left for the wiring step.
