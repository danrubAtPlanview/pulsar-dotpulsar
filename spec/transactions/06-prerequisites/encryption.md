# WP-PREREQ-ENCRYPTION — End-to-end message encryption

Status: draft. Target file in the repo: `spec/transactions/06-prerequisites/encryption.md`.

## 1. Why this is needed

| Portable upstream test | What it needs |
|---|---|
| `TransactionTest#testEncryptionRequired` (java L1793-1814) | Namespace has `encryptionRequired=true`. Producer has `addEncryptionKey("my-app-key")` and `defaultCryptoKeyReader("file:./src/test/resources/certificate/public-key.client-rsa.pem")`. A transactional send must succeed, then `txn.commit()`. The broker rejects a `CommandProducer` without `encrypted=true` and rejects messages without `encryption_keys` in that namespace. RSA key only. |
| `TransactionTest#testDelayedDeliveryExceedsMaxDelay` (java L2046-2081) | Same producer configuration (RSA, `"my-app-key"`, `defaultCryptoKeyReader("file:...public-key.client-rsa.pem")`). The send must fail with `NotAllowedException`, message `"Exceeds max allowed delivery delay of 5000 milliseconds"`. Encryption must not change the outcome. |

The feature's own upstream tests (section 9) are ported as well. They cover ECDSA/ECIES, the consumer failure
actions, chunking with encryption and `DefaultCryptoKeyReader` URIs.

**Scope.** Producer encryption, consumer decryption and **reader decryption**. Java `ReaderBuilder` also has
`cryptoKeyReader`/`cryptoFailureAction`. DotPulsar's `Reader` uses the same `ConsumerChannel`, so supporting it costs one
option each. Not in scope: the Java `MessageCrypto` plug-in interface (`messageCrypto(...)` builder method),
`decryptFailListener`, `MessagePayloadProcessor`, and the websocket proxy.

## 2. Upstream mapping

All Java paths are relative to `apache/pulsar@f3846f4811b6a7d98de40a3be2caff1fe288e026`. All C++ paths are relative to
`apache/pulsar-client-cpp@f71d7e99019d58b1d11858f05ff0dc83b718627b`. `BC` means
`bcgit/bc-java@d401e82d3f425cb7639cc38d14b85e1a880c32d4`; it is used only to pin down the ECIES byte format.

| Concern | Java | C++ | DotPulsar target |
|---|---|---|---|
| Constants: `RSA_TRANS="RSA/NONE/OAEPWithSHA1AndMGF1Padding"`, `AESGCM="AES/GCM/NoPadding"`, `ECIES`, `tagLen=128 bits` | `pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java#<fields>` (L86-93, L112) | `lib/MessageCrypto.cc#MessageCrypto` (L31-46: `dataKeyLen_ 32`, `tagLen_ 16`, `ivLen_ 12`) | `Internal/Crypto/MessageCrypto.cs` constants |
| `IV_LEN = 12` | `pulsar-client-api/.../api/MessageCrypto.java#IV_LEN` (L35) | `lib/MessageCrypto.cc` L35 | `MessageCrypto.IvLength` |
| Secure random, data key generation (AES-256) | `MessageCryptoBc` static init L124-159, `THREAD_LOCAL_KEY_GENERATOR` L190-206, `generateEncryptionKey` L226-228 | `RAND_bytes` L44, L202 | `RandomNumberGenerator` |
| Constructor and decryption caches (4h expire-after-access, 10 MB weight) | `MessageCryptoBc#MessageCryptoBc` L230-257 | `removeExpiredDataKey` L162-176 (4h) | `MessageCrypto` ctor, `DecryptionKeyCache` |
| PEM public key load (SPKI, X.509 cert, optional leading `EC PARAMETERS`) | `MessageCryptoBc#loadPublicKey` L259-301 | `loadPublicKey` L50-67 (`PEM_read_bio_PUBKEY`) | `Internal/Crypto/PemKeyLoader.cs` |
| PEM private key load (PKCS#1 `RSA PRIVATE KEY`, SEC1 `EC PRIVATE KEY`, PKCS#8, optional `EC PARAMETERS`) | `MessageCryptoBc#loadPrivateKey` L303-349 | `loadPrivateKey` L69-86 | `PemKeyLoader` |
| Rotate data key and wrap it for every key name | `MessageCryptoBc#addPublicKeyCipher(Set,CryptoKeyReader)` L361-369, `#addPublicKeyCipher(String,...)` L371-418 | `addPublicKeyCipher` L197-270 | `MessageCrypto.AddPublicKeyCipher` |
| ECIES parameter spec `IESParameterSpec(null, null, 128)` | `MessageCryptoBc#createIESParameterSpec` L421-425 | — (C++ is RSA only, `EVP_PKEY_encrypt` with `RSA_PKCS1_OAEP_PADDING`, L115-139) | `Internal/Crypto/EciesKeyWrap.cs` |
| ECIES engine | BC `prov/.../asymmetric/ec/IESCipher.java#ECIES` (L524-538: `IESEngine(ECDHBasicAgreement, KDF2BytesGenerator(SHA1), HMac(SHA1))`), `IESCipher` L463-468 (uncompressed ephemeral key), `core/.../engines/IESEngine.java#encryptBlock` L178-271, `#decryptBlock` L273-381, `#processBlock` L384-451, `#getLengthTag` L454-462, `core/.../generators/BaseKDFBytesGenerator.java#generateBytes` L79-129 | — | `EciesKeyWrap` |
| RSA OAEP | `RSA_TRANS` via BC | `rsaEncrypt`/`rsaDecrypt` L88-139 (OpenSSL OAEP default = SHA-1, MGF1-SHA-1, empty label) | `RSA.Encrypt/Decrypt(…, RSAEncryptionPadding.OaepSHA1)` |
| Encrypt payload, write `encryption_keys` + `encryption_param` | `MessageCryptoBc#encrypt` L454-524 | `MessageCrypto::encrypt` L280-395 | `MessageCrypto.Encrypt` |
| Unwrap data key | `MessageCryptoBc#tryDecryptDataKey` L526-579 | `decryptDataKey` L397-452 | `MessageCrypto.TryUnwrapDataKey` |
| Decrypt payload | `MessageCryptoBc#decryptData` L581-611 | `decryptData` L454-518 | `MessageCrypto.TryDecryptData` |
| 3-pass decrypt (last key per producer, cached keys, key reader) | `MessageCryptoBc#decrypt` L629-673, `SecretKeyCacheKey` L676-700 | `getKeyAndDecryptData` L520-551, `decrypt` L553-575 (MD5 digest of the wrapped key as the cache key) | `MessageCrypto.TryDecrypt` |
| Max output size | `MessageCryptoBc#getMaxOutputSize` L613-616 (`len + max(len, 512)`) | `len + EVP_MAX_BLOCK_LENGTH + 16` L340 | not needed: exact sizes |
| Key reader API | `pulsar-client-api/.../api/CryptoKeyReader.java` L31-57 | `include/pulsar/CryptoKeyReader.h` | `Abstractions/ICryptoKeyReader.cs` |
| Key info | `pulsar-client-api/.../api/EncryptionKeyInfo.java` L31-61 | `include/pulsar/EncryptionKeyInfo.h` | `EncryptionKeyInfo.cs` |
| Failure actions | `.../api/ProducerCryptoFailureAction.java` L29-39 (FAIL, SEND), `.../api/ConsumerCryptoFailureAction.java` L30-51 (FAIL, DISCARD, CONSUME) | `include/pulsar/{Producer,Consumer}CryptoFailureAction.h` | `ProducerCryptoFailureAction.cs`, `ConsumerCryptoFailureAction.cs` |
| Encryption context on received messages | `pulsar-client-api/.../common/api/EncryptionContext.java` L34-57; `ConsumerImpl#createEncryptionContext` L3264-3291 | `include/pulsar/EncryptionContext.h`; `ConsumerImpl.cc` L717-755 | `EncryptionContext.cs`, `EncryptionKey.cs`, `IMessage.EncryptionContext` |
| Default key reader (`file:`/`data:` URIs) | `pulsar-client/.../impl/DefaultCryptoKeyReader.java` L33-108 | `lib/CryptoKeyReader.cc` (file paths) | `DefaultCryptoKeyReader.cs` |
| Builder methods | `ProducerBuilder.java` L380-449; `ProducerBuilderImpl` L237-270 (blank key → `IllegalArgumentException`); `ConsumerBuilder.java` L336-389; `ConsumerBuilderImpl` L156-162 (default FAIL), L367-393 | `ProducerConfiguration.h`, `ConsumerConfiguration.h` | `IProducerBuilder`, `IConsumerBuilder`, `IReaderBuilder` + options |
| Encryption-enabled predicate | `ProducerConfigurationData#isEncryptionEnabled` L266-268 (keys non-empty **and** reader non-null); `cryptoFailureAction` default FAIL L179; `encryptionKeys` is a `TreeSet` L219 | `ProducerConfiguration::isEncryptionEnabled` | `ProducerOptions` |
| Producer: create crypto, initial wrap, 4h refresh | `ProducerImpl` ctor L262-280, L332-334; `#scheduleKeyGeneratorTask` L3232-3238 (delay 0, period 4h); `#addPublicKeyCipher` L3240-3257 (initial failure → creation fails with `CryptoException` and the producer is closed) | `ProducerImpl.cc` L71 (`4*60*60*1000` ms), L95-101, L240-252 | `PulsarClient.CreateProducer`, `Producer` |
| `CommandProducer.encrypted` | `ProducerImpl` L2230 → `Commands#newProducer` L978-990 (`setEncrypted`) | `Commands.cc` | `ProducerChannelFactory` |
| Producer: compress → chunk → encrypt each chunk | `ProducerImpl#sendAsync` L589-669 (compress whole payload, compute chunks), L892 (`encryptMessageOrReleaseSource` per chunk), `#encryptMessage` L1077-1112 (SEND action → send plaintext) | `ProducerImpl.cc` L611-638 (encrypt each chunk slice), `encryptMessage` L991-1000 | `ProducerChannel.Send` |
| Batch: whole compressed batch payload encrypted once | `BatchMessageContainerImpl#buildAndEncryptBatchPayload` L244-261 | `BatchMessageContainerBase` | batch container ([producer-batching.md]) calls the same encrypt step via `ProducerChannel.Send` |
| Consumer: create crypto when a reader is set | `ConsumerImpl` ctor L387-406 | `ConsumerImpl.cc` L160-162 | `ConsumerChannel` |
| Consumer: decrypt before decompress; chunk decrypt per chunk, decompress after reassembly | `ConsumerImpl#messageReceived` L1578-1608 | `ConsumerImpl.cc` L717-745 | `ConsumerChannel.Receive` |
| Consumer: decrypt or failure action | `ConsumerImpl#decryptPayloadIfNeeded` L2309-2334, `#handleCryptoFailure` L2336-2387, `#discardMessage` L2455-2460 | `ConsumerImpl::decryptMessageIfNeeded` L1005-1049 | `ConsumerChannel` + `RejectPackage` |
| CLI flags (interop) | `pulsar-client-tools/.../cli/CmdProduce.java` L140-147 (`-ekn/--encryption-key-name`, `-ekv/--encryption-key-value`), `CmdConsume.java` L125-132 (`-ekv`, `-ca/--crypto-failure-action`), L104 (`-n`) | — | interop tests |

## 3. Wire format (normative, byte-exact)

### 3.1 Payload encryption (both RSA and EC)

- Data key: 32 random bytes (AES-256). Java uses `KeyGenerator.init(256)` (L202). If the JCE policy limits AES to 128
  bits, Java falls back to 128 (L196-200). DotPulsar always generates 256-bit keys and must **accept** 16, 24 or 32-byte
  unwrapped data keys when decrypting.
- IV: 12 random bytes. A new IV is drawn per `Encrypt` call (L499-500), so every chunk, every batch and every resend
  gets one.
- Cipher: AES-GCM, 128-bit tag, no AAD.
- Output payload is `ciphertext || tag`. Its length is `plaintextLength + 16`. Java writes this as one buffer
  (`cipher.doFinal`). C++ writes it the same way (L360-383).
- `MessageMetadata.encryption_param` = the 12-byte IV (L504).
- `MessageMetadata.encryption_keys`: the list is cleared first (L466). Then add one `EncryptionKeys` per configured key
  name, in **ordinal-sorted key-name order** (Java `TreeSet`). Each entry has `key` = key name and `value` = wrapped data
  key. `metadata` = the `EncryptionKeyInfo.Metadata` entries returned by the key reader's `GetPublicKey`, and only when
  non-empty (L475-489).
- `MessageMetadata.encryption_algo` is **not set**. Neither Java nor C++ writes it.
- `compression` and `uncompressed_size` stay as set by the compression step. They describe the plaintext **before**
  encryption. `num_messages_in_batch`, chunk fields, the partition key and properties stay **plaintext**. Only the
  payload is encrypted.

### 3.2 Data key wrap: RSA

`RSA/NONE/OAEPWithSHA1AndMGF1Padding` is OAEP with SHA-1, MGF1-SHA-1 and an empty label. It is identical to .NET
`RSAEncryptionPadding.OaepSHA1` and to the OpenSSL default (C++ L121). `value` = `RSA.Encrypt(dataKey, OaepSHA1)`. Its
length is the modulus size, e.g. 256 bytes for RSA-2048.

### 3.3 Data key wrap: EC (BC "ECIES" with `IESParameterSpec(null, null, 128)`)

This applies when the public key algorithm is EC (Java `"ECDSA"` algorithm name). Let `n = (fieldSizeBits + 7) / 8`.
It is 16 for the secp128r1 test key and 66 for secp521r1.

Encrypt (wrap the 32-byte data key `M` for recipient public point `Q`):
1. Generate an ephemeral key pair `(d_e, Q_e)` on the recipient's curve.
2. `V = 0x04 || X(Q_e) || Y(Q_e)`, where X and Y are each left-padded to `n` bytes (uncompressed point; BC
   `IESCipher` L463-468, `usePointCompression=false`).
3. `Z = x-coordinate of (d_e · Q)`, as an unsigned big-endian integer left-padded to exactly `n` bytes
   (`ECDHBasicAgreement` + `BigIntegers.asUnsignedByteArray(fieldSize, z)`, IESEngine L425-427).
4. `S = V || Z` (IESEngine L430-435; V is non-empty).
5. `K = KDF2-SHA1(S, 48)`: `K = T1 || T2 || T3[0..8)` where `Ti = SHA1(S || I2OSP(i, 4))` for i = 1, 2, 3, the counter
   is big-endian and starts at 1, and the derivation IV is null (BaseKDFBytesGenerator L98-126; `KDF2BytesGenerator` =
   counterStart 1). The length is `macKeySize/8 + len(M)` = 16 + 32 = 48.
6. `K2 = K[0..16)` is the MAC key. `K1 = K[16..48)` is the keystream (IESEngine L202-203; the "K2 first" layout applies
   because V is non-empty, in all BC versions in use).
7. `C = M XOR K1`.
8. `L2 = 8 zero bytes` (`getLengthTag(null)`, L454-462, because the encoding vector `P2` is null).
9. `T = HMAC-SHA1(key = K2, data = C || L2)`. T is 20 bytes. `P2` is null, so it is not fed.
10. `value = V || C || T`. Its length is `(1 + 2n) + 32 + 20`, i.e. 85 bytes for secp128r1.

Decrypt: read the first byte of `value`. `0x04` (and BC's `0x06`/`0x07`) means `|V| = 1 + 2n`. `0x02`/`0x03` means
compressed, `|V| = 1 + n`; DotPulsar does not support it and fails. `0x00` is invalid. Require
`len(value) >= |V| + 20`. Compute `Z = x(d · Q_e)` and `S = V || Z`, then
`K = KDF2-SHA1(S, 16 + (len - |V| - 20))`. Recompute T over `C || L2` and compare in constant time
(`CryptographicOperations.FixedTimeEquals`). Then `M = C XOR K1`.

Test vector for steps 5-9 (computed independently with Python `hashlib`/`hmac`, to be asserted by a unit test of the
symmetric half, `Ies.Encrypt(S, M)`):

```
S  = 000102...1f  (32 bytes 0x00..0x1f)
M  = 404142...5f  (32 bytes 0x40..0x5f)
K  = a98ffb7caef3bd518fb7bc1b6cc89dbdef59cde6e6b6e6b5e405eb5e6ffb080c390c2cd82363d25a79fcf567c36336e9
C  = af188fa5a2f3a0f2ac4ca11523b64643695d7e8b7736840d21a5af3c9f3e68b6
T  = 5a344411ae6820114d3588a57287eebf59139aab
```

### 3.4 Producer frame changes

- `CommandProducer.encrypted = true` when encryption is enabled (Java L2230; broker `encryptionRequired` check).
- Order on the producer (Java L589-669, L892; C++ L611-638): **compress whole payload → split into chunks → encrypt
  each chunk separately**. Each chunk gets its own IV and `encryption_keys` entry. Batches: **build batch → compress →
  encrypt once**. `uncompressed_size` and `total_chunk_msg_size` refer to the compressed-but-unencrypted sizes (Java sets
  `total_chunk_msg_size = compressedPayloadSize`, L848).
- Chunk sizing: Java computes `payloadChunkSize = maxMessageSize - metadata.getSerializedSize()` **before** encryption
  metadata is added (L657). DotPulsar's `Connection.Send` throws `TooLargeMessageException` if the frame exceeds
  `MaxMessageSize` (Connection.cs L252-253), so when encryption is enabled the chunk size must also subtract
  `EncryptionOverhead` = `16 (tag) + Σ over keys (serialized EncryptionKeys entry size) + 2 + 12 (encryption_param tag/len + IV)`.
  Compute it from the current `DataKeySnapshot` (section 5.2). The chunking spec ([chunking.md]) owns the formula and
  calls `MessageCrypto.GetMetadataOverhead()`.

### 3.5 Consumer order

Validate the checksum, parse the metadata, then **decrypt** (if `encryption_keys` count > 0). Then:
- non-chunked message: decompress, then batch-parse or deliver.
- chunk: append the **decrypted, still-compressed** chunk; decompress the assembled payload once complete (Java L1591,
  L1665; C++ L740-745).

## 4. Public API (new and changed)

All new public types follow the repo style: Apache header, file-scoped namespace, usings inside the namespace, XML docs.

### 4.1 `src/DotPulsar/Abstractions/ICryptoKeyReader.cs`

```csharp
namespace DotPulsar.Abstractions;

/// <summary>
/// An abstraction for reading the keys used for end-to-end encryption.
/// </summary>
/// <remarks>
/// The methods are called when a producer is created, every 4 hours by the producer and when a consumer cannot decrypt a message with a cached key.
/// The implementation should not block.
/// </remarks>
public interface ICryptoKeyReader
{
    /// <summary>
    /// Get the public key (PEM) with the given name. Used by producers.
    /// </summary>
    EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata);

    /// <summary>
    /// Get the private key (PEM) with the given name. Used by consumers and readers.
    /// </summary>
    EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata);
}
```

- The interface is synchronous, like Java's `CryptoKeyReader`. This lets `Create()` fail synchronously; see §5.4.
- `GetPublicKey` is called with an empty dictionary, where Java passes `null` (L377). `GetPrivateKey` is called with the
  `EncryptionKeys.metadata` of the message.
- A `null` return, or a `Key` of length 0, means the key was not found.

### 4.2 `src/DotPulsar/EncryptionKeyInfo.cs`

```csharp
/// <summary>
/// An encryption key and its metadata (for example version or timestamp).
/// </summary>
public sealed class EncryptionKeyInfo
{
    /// <summary>
    /// Initializes a new instance using the specified key and optional metadata.
    /// </summary>
    public EncryptionKeyInfo(byte[] key, IReadOnlyDictionary<string, string>? metadata = null) { ... }

    /// <summary>
    /// The key in PEM format.
    /// </summary>
    public byte[] Key { get; }

    /// <summary>
    /// The key's metadata. The default is an empty dictionary.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; }
}
```

### 4.3 Enums (`src/DotPulsar/`)

```csharp
/// <summary>
/// The action a producer takes when encryption fails.
/// </summary>
public enum ProducerCryptoFailureAction : byte
{
    /// <summary>
    /// Fail the send. This is the default.
    /// </summary>
    Fail,

    /// <summary>
    /// Ignore the failure and send the message unencrypted.
    /// </summary>
    Send
}

/// <summary>
/// The action a consumer or reader takes when a message cannot be decrypted.
/// </summary>
public enum ConsumerCryptoFailureAction : byte
{
    /// <summary>
    /// Do not deliver the message and do not acknowledge it. This is the default.
    /// </summary>
    Fail,

    /// <summary>
    /// Acknowledge the message (with validation error 'DecryptionError') and do not deliver it.
    /// </summary>
    Discard,

    /// <summary>
    /// Deliver the encrypted (and possibly compressed) payload. 'IMessage.EncryptionContext' has the information needed to decrypt it.
    /// </summary>
    Consume
}
```

### 4.4 `src/DotPulsar/EncryptionContext.cs` and `src/DotPulsar/EncryptionKey.cs`

```csharp
/// <summary>
/// Encryption information of a received message.
/// </summary>
public sealed class EncryptionContext
{
    /// <summary>
    /// The wrapped data keys by key name.
    /// </summary>
    public IReadOnlyDictionary<string, EncryptionKey> Keys { get; }

    /// <summary>
    /// The encryption parameter (the AES-GCM IV).
    /// </summary>
    public byte[] Param { get; }

    /// <summary>
    /// The encryption algorithm, if set by the producer.
    /// </summary>
    public string? Algorithm { get; }

    /// <summary>
    /// The compression type of the encrypted payload.
    /// </summary>
    public CompressionType CompressionType { get; }

    /// <summary>
    /// The uncompressed size of the payload.
    /// </summary>
    public uint UncompressedMessageSize { get; }

    /// <summary>
    /// The number of messages in the batch, if the message is a batch.
    /// </summary>
    public int? BatchSize { get; }

    /// <summary>
    /// Whether the payload is still encrypted (true) or was decrypted (false).
    /// </summary>
    public bool IsEncrypted { get; }
}

/// <summary>
/// A wrapped data key and its metadata.
/// </summary>
public sealed class EncryptionKey
{
    /// <summary>
    /// The wrapped (encrypted) data key.
    /// </summary>
    public byte[] KeyValue { get; }

    /// <summary>
    /// The key's metadata.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; }
}
```

Constructors are `internal`, matching the existing `Message<T>` precedent. `Algorithm` is `null` unless
`metadata.HasEncryptionAlgo`.

### 4.5 `IMessage` (breaking for custom implementations; consistent with the decision that interfaces gain members)

```csharp
/// <summary>
/// The encryption context if the message was encrypted by the producer, otherwise null.
/// </summary>
EncryptionContext? EncryptionContext { get; }
```

Java sets the context whenever `encryption_keys` is non-empty (L3268), whether or not decryption succeeded.
`IsEncrypted` tells the two cases apart.

### 4.6 `src/DotPulsar/DefaultCryptoKeyReader.cs`

```csharp
/// <summary>
/// A crypto key reader that loads keys from 'file:' or 'data:application/x-pem-file;base64,' URIs.
/// </summary>
public sealed class DefaultCryptoKeyReader : ICryptoKeyReader
{
    /// <summary>
    /// Initializes a new instance using a default public key URI, a default private key URI and optional per-key-name URIs.
    /// </summary>
    public DefaultCryptoKeyReader(
        string? defaultPublicKey,
        string? defaultPrivateKey,
        IReadOnlyDictionary<string, string>? publicKeys = null,
        IReadOnlyDictionary<string, string>? privateKeys = null) { ... }
}
```

Lookup is `keys.TryGetValue(keyName, out uri) ? uri : default`, mirroring Java `getOrDefault` (L57, L75). URI rules
replace Java `new URL(..)` (L90-106):
- `data:` must be exactly `data:application/x-pem-file;base64,<b64>`. Otherwise it fails with "Unsupported media type
  or encoding format: <media type>".
- `file:` followed by `//` uses `new Uri(uri).LocalPath`. Otherwise the remainder after `file:` is a path relative to
  `Environment.CurrentDirectory` (needed for Java's `file:./src/...` form).
- Any other scheme gives `ArgumentException("Invalid key format")`.

On failure Java logs and returns an `EncryptionKeyInfo` with a null key. DotPulsar returns `null`, which then fails as
"key not found" (CryptoException on the producer, decryption failure on the consumer).

### 4.7 Options and builders

`ProducerOptions<TMessage>`:

```csharp
/// <summary>
/// The default crypto failure action.
/// </summary>
public static readonly ProducerCryptoFailureAction DefaultCryptoFailureAction = ProducerCryptoFailureAction.Fail;

/// <summary>
/// Set the crypto key reader used to read the public keys. This is optional.
/// </summary>
public ICryptoKeyReader? CryptoKeyReader { get; set; }

/// <summary>
/// Set the names of the public keys used to encrypt the data key. Encryption is enabled when at least one key name and a crypto key reader are set.
/// </summary>
public IEnumerable<string> EncryptionKeys { get; set; }   // backed by SortedSet<string>(StringComparer.Ordinal), same pattern as ConsumerOptions.Topics

/// <summary>
/// Set the action to take when encryption fails. The default is 'Fail'.
/// </summary>
public ProducerCryptoFailureAction CryptoFailureAction { get; set; }
```

`IProducerBuilder<TMessage>` (+ `ProducerBuilder` fields/`Create()` mapping):

```csharp
/// <summary>
/// Set the crypto key reader used to read the public keys. This is optional.
/// </summary>
IProducerBuilder<TMessage> CryptoKeyReader(ICryptoKeyReader cryptoKeyReader);

/// <summary>
/// Add the name of a public key used to encrypt the data key. This is optional.
/// </summary>
IProducerBuilder<TMessage> EncryptionKey(string keyName);

/// <summary>
/// Set the action to take when encryption fails. The default is 'Fail'.
/// </summary>
IProducerBuilder<TMessage> CryptoFailureAction(ProducerCryptoFailureAction cryptoFailureAction);
```

`EncryptionKey` throws `ArgumentException("Encryption key cannot be blank", nameof(keyName))` for a null or whitespace
name (Java `checkArgument`, ProducerBuilderImpl L262).

`ConsumerOptions<TMessage>` and `ReaderOptions<TMessage>`: `CryptoKeyReader` (`ICryptoKeyReader?`, "used to read the
private keys") and `CryptoFailureAction` (`ConsumerCryptoFailureAction`, default `Fail`, static
`DefaultCryptoFailureAction`). `IConsumerBuilder<TMessage>` and `IReaderBuilder<TMessage>` get
`CryptoKeyReader(ICryptoKeyReader)` and `CryptoFailureAction(ConsumerCryptoFailureAction)` with the same docs.

Extension methods (`ProducerBuilderExtensions`, `ConsumerBuilderExtensions`, `ReaderBuilderExtensions`), mirroring Java
`defaultCryptoKeyReader`:

```csharp
/// <summary>
/// Use the DefaultCryptoKeyReader with a public key URI that is always used to encrypt.
/// </summary>
public static IProducerBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(this IProducerBuilder<TMessage> builder, string publicKey)
    => builder.CryptoKeyReader(new DefaultCryptoKeyReader(publicKey, null));

/// <summary>
/// Use the DefaultCryptoKeyReader with public key URIs by key name.
/// </summary>
public static IProducerBuilder<TMessage> DefaultCryptoKeyReader<TMessage>(this IProducerBuilder<TMessage> builder, IReadOnlyDictionary<string, string> publicKeys)
    => builder.CryptoKeyReader(new DefaultCryptoKeyReader(null, null, publicKeys));
```

The consumer and reader versions are the same with `privateKey`/`privateKeys`.

### 4.8 Exceptions (`src/DotPulsar/Exceptions/`)

| C# | Java | Thrown when |
|---|---|---|
| `CryptoException : DotPulsarException` | `PulsarClientException.CryptoException` | A public key cannot be read, loaded or used to wrap the data key, at `Create()` or at refresh; a send fails to encrypt with action `Fail` (it faults that send's task only, not the producer); messages: `"Failed to load public key {keyName}. {inner}"`, `"Unsupported key type {alg} for key {keyName}"`, `"Keyname or KeyReader is null"` (verbatim Java texts, L373, L384, L400) |

`ProducerFaultedException` is **not** used for crypto failures.

## 5. Internal design

New folder `src/DotPulsar/Internal/Crypto/` (namespace `DotPulsar.Internal.Crypto`; per conventions §2, more than 3
files). Types are `public sealed`.

| File | Responsibility |
|---|---|
| `MessageCrypto.cs` | Producer side: data key snapshot, wrap, encrypt, 4h refresh. Consumer side: caches and 3-pass decrypt. One class, two modes: `keyGenNeeded` like Java. |
| `DataKeySnapshot.cs` | Immutable: `byte[] DataKey`, the AES-GCM instance, `IReadOnlyList<(string Name, ByteString WrappedKey, KeyValue[] Metadata)>` sorted ordinally |
| `PemKeyLoader.cs` | PEM text → `AsymmetricAlgorithm` (`RSA` or `ECDiffieHellman`), per TFM |
| `RsaKeyWrap.cs` | OAEP-SHA1 wrap/unwrap |
| `EciesKeyWrap.cs` | §3.3; `Ies` symmetric half exposed as `public static` for the vector test |
| `Kdf2Sha1.cs` | KDF2 with SHA-1 (span based, no allocations except the output) |
| `AesGcmCipher.cs` | TFM-specific AES-GCM wrapper (see §6) |
| `DecryptionKeyCache.cs` | `ConcurrentDictionary<ByteString, CachedDataKey>` + last key per producer name; expire-after-access 4h; weight limit 10 MiB (`wrappedKey.Length + dataKey.Length`), oldest-access eviction |

### 5.1 Producer data key lifecycle

- `PulsarClient.CreateProducer` (PulsarClient.cs L59-84), after the compression check: if `options.EncryptionKeys` is
  non-empty and `options.CryptoKeyReader` is not null, create `new MessageCrypto(keyGenNeeded: true)` and call
  `AddPublicKeyCipher(options.EncryptionKeys, options.CryptoKeyReader)` **synchronously**. A `CryptoException`
  propagates out of `Create()`. No `Producer` is constructed, so nothing is registered with the client or the broker.
  This is the parity point for `ProducerEncryptionKeyFailureTest`.
- `AddPublicKeyCipher` builds a **new** `DataKeySnapshot`: a fresh 32-byte key, then each key name is loaded and
  wrapped. Only when every key succeeds is it published with `Volatile.Write(ref _snapshot, newSnapshot)`. Upstream
  differs: Java rotates `encryptionKey` first and then overwrites map entries one by one (L363-367, L417), so a failure
  midway leaves a data key that does not match the wrapped keys. The DotPulsar behaviour is a strict improvement with
  no wire difference.
- Refresh: `MessageCrypto` owns a `Timer` with dueTime 4h and period 4h, started only in producer mode. Each tick calls
  `AddPublicKeyCipher`; failures are swallowed and the old snapshot stays (Java `catchingAndLoggingThrowables`).
  Disposal stops the timer. `Producer.DisposeAsync` disposes `MessageCrypto` and the snapshot's AES-GCM instance.
- Java re-loads a key that is missing from the map during `encrypt` (L469-473). That cannot happen in DotPulsar: the
  key set is fixed at `Create()` and every snapshot contains all keys.
- One `MessageCrypto` per `Producer`, shared by all sub-producers (partitions). Java has one per partition
  `ProducerImpl`; sharing is wire-invisible and saves RSA/EC operations.

### 5.2 Producer encrypt step

`MessageCrypto.Encrypt(MessageMetadata metadata, ReadOnlySequence<byte> payload) → ReadOnlySequence<byte>`:
1. `snapshot = Volatile.Read(ref _snapshot)`.
2. `metadata.EncryptionKeys.Clear()`. Then add an `EncryptionKeys { Key, Value = wrapped (ByteString, reused) }` per
   snapshot entry, plus its `Metadata` `KeyValue`s. These are cached `EncryptionKeys` objects held by the snapshot.
   Protobuf messages added to a `RepeatedField` are not cloned, and serialization does not mutate them, so they can be
   shared across sends.
3. Fill a 12-byte IV with `RandomNumberGenerator.Fill` (ns2.1+) or `_rng.GetBytes(iv)` (ns2.0). Set
   `metadata.EncryptionParam = ByteString.CopyFrom(iv)`.
4. Allocate `output = new byte[payload.Length + 16]` and encrypt into `output[..len]`, with the tag in
   `output[len..]`. If `payload` is multi-segment, copy it into an `ArrayPool<byte>.Shared` rental first and return the
   rental afterwards.
5. Return `new ReadOnlySequence<byte>(output)`.

`ProducerChannel.Send` (ProducerChannel.cs L63-111) after the compression block (L85-94):

```text
if (_messageCrypto is not null)
{
    try { sendPackage.Payload = _messageCrypto.Encrypt(metadata, sendPackage.Payload); resetEncryption = true; }
    catch (Exception e) when (_cryptoFailureAction == ProducerCryptoFailureAction.Send) { /* send unencrypted */ }
    // with Fail: let the CryptoException propagate → responseTcs/send faults (see below)
}
```

The `finally` (L98-110) must also reset `metadata.EncryptionKeys.Clear()` and `metadata.ClearEncryptionParam()` when
`resetEncryption`. This is required because of `MessageMetadataObjectPool` reuse and resends after reconnect (each
resend re-encrypts with a fresh IV, which is valid). With action `Fail`, an encryption exception must fail **only that
SendOp**: it completes `sendOp.ReceiptTcs` with the `CryptoException` and removes it from the queue. It must not fault
the channel or `SubProducer`. Edit point: `SubProducer.MessageDispatcher` (SubProducer.cs L107-149). Wrap
`channel.Send` so that a `CryptoException` (and only that type) runs
`sendOp.ReceiptTcs.TrySetException(e); _sendQueue.RemoveCurrentItem(); continue;`. `TryExecuteOnce` must not see it.
Java behaves the same way: the callback fails, the producer stays usable.

`ProducerChannelFactory` (L47-51): add `Encrypted = true` to `CommandProducer` when the factory receives
`isEncrypted: true`. That is a new ctor parameter passed from `Producer.CreateSubProducer` (Producer.cs L163). The
`ProducerChannel` ctor gets `MessageCrypto? messageCrypto` and `ProducerCryptoFailureAction cryptoFailureAction`.

Chunking ([chunking.md]) splits **before** this step, so each chunk passes through `ProducerChannel.Send` with its own
metadata copy (or reset of chunk fields) and is encrypted individually, as in Java. Batching ([producer-batching.md])
hands the built batch payload to `ProducerChannel.Send`, so batches are encrypted once.

### 5.3 Consumer decrypt step

`ConsumerChannelFactory` (L20-62) and the `ConsumerChannel` ctor (L37-71) gain `MessageCrypto? messageCrypto`
(non-null iff `CryptoKeyReader` is set; `keyGenNeeded: false`), `ICryptoKeyReader? cryptoKeyReader` and
`ConsumerCryptoFailureAction cryptoFailureAction`. Wire these in `Consumer.CreateSubConsumer` (Consumer.cs L461-466)
and in `Reader` (Reader.cs L289-291). There is one `MessageCrypto` per sub-consumer, as Java has per `ConsumerImpl`.

In `ConsumerChannel.Receive`, after `ExtractData` (L98-100) and **before** the decompression block (L102-117):

```text
EncryptionContext? encryptionContext = null;
var isUndecryptable = false;
if (metadata.EncryptionKeys.Count > 0)
{
    if (_messageCrypto is not null && _messageCrypto.TryDecrypt(metadata, data, _cryptoKeyReader!, out var decrypted))
    {
        data = decrypted;
        encryptionContext = EncryptionContextFactory.Create(metadata, isEncrypted: false);
    }
    else
    {
        switch (_cryptoFailureAction)
        {
            case ConsumerCryptoFailureAction.Consume:
                isUndecryptable = true;
                encryptionContext = EncryptionContextFactory.Create(metadata, isEncrypted: true);
                break;
            case ConsumerCryptoFailureAction.Discard:
                await RejectPackage(messagePackage, CommandAck.Types.ValidationError.DecryptionError, cancellationToken).ConfigureAwait(false);
                continue;
            default: // Fail
                _unAckedMessageTracker?.Add(new MessageId(id.LedgerId, id.EntryId, id.Partition, -1, _topic)); // see ack-timeout.md
                continue;
        }
    }
}
```

- If `isUndecryptable`, skip decompression and batch parsing. Return
  `_messageFactory.Create(messageId.ToMessageId(_topic), redeliveryCount, data, metadata, encryptionContext)` as a
  single non-batch message (Java L1591, L1608). For a chunk with `Consume`, follow Java: the raw encrypted chunks go
  through chunk assembly and the assembled bytes are delivered with `IsEncrypted = true` (Java L1608-1611; documented
  as unusable by the application, like a batch).
- `RejectPackage` (L253-264) already sends an individual ack with a validation error. That is exactly Java
  `discardMessage` (L2455-2460).
- The `Fail` path neither acks nor delivers. The broker redelivers on reconnect, or via ack timeout when enabled (Java
  L2378-2381, C++ L1022-1025). The `_sendWhenZero` accounting stays as for `RejectPackage` (no undo). It mirrors
  Java's behaviour of not returning permits for FAIL.
- `MessageFactory.Create` (MessageFactory.cs L50-56) and both private `Create` overloads get
  `EncryptionContext? encryptionContext` and pass it to the `Message<TValue>` ctor (new last-but-one parameter before
  `schema`). `BatchHandler.Add` (BatchHandler.cs L41-71) takes the context and passes the **same instance** to each
  message in the batch.
- `EncryptionContextFactory.Create(MessageMetadata, bool)` is internal. It builds `Keys` (key name →
  `EncryptionKey(value.ToByteArray(), metadata dict)`; a duplicate key name keeps the last, as Java `Collectors.toMap`
  would throw — DotPulsar must not throw), `Param = EncryptionParam.ToByteArray()`, `Algorithm`,
  `CompressionType = (CompressionType) metadata.Compression`, `UncompressedMessageSize = metadata.UncompressedSize`,
  and `BatchSize = metadata.HasNumMessagesInBatch ? metadata.NumMessagesInBatch : null`.

### 5.4 `MessageCrypto.TryDecrypt` (mirrors Java L629-673 exactly)

`producerName = metadata.HasProducerName ? metadata.ProducerName : "__not_set__"`.
1. Pass 1: the last key used for `producerName`. Try AES-GCM with it. On failure, remove that entry.
2. Pass 2: for each `EncryptionKeys e` in order, try the key cached under `e.Value` (a `ByteString`, compared by
   content, used directly as the dictionary key). On success, set it as the last key for the producer.
3. Pass 3: for each `e` in order, call `reader.GetPrivateKey(e.Key, metadataDict(e.Metadata))`, load the PEM and unwrap
   by key type (RSA → OAEP-SHA1; EC → §3.3). Each `e` is wrapped in try/catch: any exception or `null` means skip to the
   next. On success, cache by `e.Value`, try AES-GCM and on success set the last key.
4. Otherwise return false.

AES-GCM decrypt: require `data.Length >= 16`. The ciphertext is `data[..^16]` and the tag is `data[^16..]`. The output
is an exact-size `byte[data.Length - 16]`. A tag mismatch (`CryptographicException`, or
`AuthenticationTagMismatchException` on net8+) returns false.

## 6. .NET implementation per target framework

| Primitive | net8.0 / net9.0 / net10.0 | netstandard2.1 | netstandard2.0 |
|---|---|---|---|
| AES-256-GCM | `System.Security.Cryptography.AesGcm`; use `new AesGcm(key, 16)` (the 1-arg ctor is obsolete `SYSLIB0053` on net8+) | `new AesGcm(key)` (available since netstandard2.1) | **not available** |
| RSA OAEP-SHA1 | `RSA.Encrypt/Decrypt(…, RSAEncryptionPadding.OaepSHA1)` | same | same (`RSA.Create()` + `ImportParameters`) |
| PEM parsing | `AsymmetricAlgorithm.ImportFromPem` (ignores unknown labels such as `EC PARAMETERS`); `X509Certificate2.CreateFromPem` for `CERTIFICATE` | manual: find the first supported `-----BEGIN <label>-----` block, base64-decode it, then dispatch: `PUBLIC KEY` → `ImportSubjectPublicKeyInfo`; `RSA PUBLIC KEY` → `ImportRSAPublicKey`; `RSA PRIVATE KEY` → `ImportRSAPrivateKey`; `PRIVATE KEY` → `ImportPkcs8PrivateKey`; `EC PRIVATE KEY` → `ImportECPrivateKey`; `CERTIFICATE` → `new X509Certificate2(der).GetRSAPublicKey()/GetECDiffieHellmanPublicKey()` | none of the import APIs exist |
| Key-type detection | Try RSA, then ECDiffieHellman, by label: `RSA *` → RSA. For `PUBLIC KEY`/`PRIVATE KEY`, read the SPKI/PKCS#8 algorithm OID (`1.2.840.113549.1.1.1` RSA, `1.2.840.10045.2.1` EC) | same | — |
| ECDH raw shared secret (x-coordinate) | `ECDiffieHellman.DeriveRawSecretAgreement(ECDiffieHellmanPublicKey)` (**.NET 8+ only**); left-pad to `n` | **not available** (only hashed derivations) | — |
| Ephemeral key on the recipient curve | `ECDiffieHellman.Create(recipient.ExportParameters(false).Curve)` | — | — |
| Random | `RandomNumberGenerator.Fill(span)` | `RandomNumberGenerator.Fill(span)` | `RandomNumberGenerator.Create().GetBytes(byte[])` |
| Constant-time compare | `CryptographicOperations.FixedTimeEquals` | same | manual loop (OR of XORs) |

**Test key facts (checked with `openssl ec -text`):** `certificate/private-key.client-ecdsa.pem` and
`public-key.client-ecdsa.pem` are **secp128r1 with explicit curve parameters** (field 128-bit, `EC PARAMETERS` block +
`EC PRIVATE KEY`). `crypto_ecdsa_*.key` are **secp521r1 with explicit parameters**. `*-rsa.pem` are RSA-2048 (PKCS#1
private, SPKI public), **with two exceptions that the WP9a core found** (followup 0003 item 1 to 3):
`private-key.client-mismatch-rsa.pem` is **an EC key** (secp521r1 with explicit parameters) despite its name, and
`public-key.client-mismatch-rsa.pem` is **byte-identical** to `public-key.client-rsa.pem`, so neither is a second RSA key;
a test that needs a key that does not match must generate one. `private-key.client-rsa-pkcs8.pem` is PKCS#8, and
`public-key.client-rsa-pkcs8.pem` is **not** the PKCS#8 form of `public-key.client-rsa.pem` but a **different** RSA key:
treat the two key pairs as independent. .NET supports explicit prime curves for
ECDiffieHellman on Windows (CNG) and Linux (OpenSSL), **not on macOS**. Whether CNG accepts a 128-bit explicit curve
must be verified by `PemKeyLoaderTests` (acceptance criterion A6); it works on Windows 11 and in the Linux container.

Resulting support matrix (recommended Option A, §11 D1):

| | net8+ | netstandard2.1 (e.g. .NET Core 3.1-7) | netstandard2.0 (.NET Framework) |
|---|---|---|---|
| RSA keys | yes | yes | no |
| EC (ECIES) keys | yes (Windows/Linux) | no → `CryptoException("Unsupported key type EC for key {name} on this platform")` | no |
| Consumer without decryption (`Consume`/`Discard`/`Fail` actions, `EncryptionContext`) | yes | yes | yes |

On netstandard2.0, setting `EncryptionKeys` + `CryptoKeyReader` makes `Create()` throw
`PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later")`. A consumer with a
`CryptoKeyReader` behaves as if decryption failed (it applies its failure action), and creation logs nothing (the repo
has no logging).

## 7. Zero-allocation / performance notes

- No allocation per message for metadata: cached `EncryptionKeys` protobuf objects live in the snapshot; only the IV
  `ByteString` (12 bytes) and the output buffer are allocated.
- The output buffer must outlive the send (the receipt and resends), so it is a plain `byte[]`, not pooled. Multi-segment
  inputs use an `ArrayPool` scratch buffer that is returned in `finally`.
- AES-GCM instance: one per `DataKeySnapshot` (producer) and one per cached data key (consumer). `AesGcm` instances are
  **not** documented as thread-safe, so guard each use with `lock (instance)`. Contention is negligible because each
  sub-producer dispatcher and each `ConsumerChannel.Receive` is already serialized. Do not create an `AesGcm` per message
  (key schedule cost).
- Asymmetric operations happen only at create, every 4h (producer), and on cache misses (consumer). Never per message
  in steady state, thanks to Pass 1 (last key per producer).
- Decrypt allocates exactly one `byte[plaintextLength]`. The existing decompression path allocates anyway.
- Avoid LINQ in `Encrypt`/`TryDecrypt`.

## 8. Exact DotPulsar edit points (summary)

| File | Change |
|---|---|
| `Abstractions/ICryptoKeyReader.cs` | new |
| `EncryptionKeyInfo.cs`, `EncryptionContext.cs`, `EncryptionKey.cs`, `ProducerCryptoFailureAction.cs`, `ConsumerCryptoFailureAction.cs`, `DefaultCryptoKeyReader.cs` | new |
| `Exceptions/CryptoException.cs` | new |
| `Internal/Crypto/*` | new (§5 table) |
| `Abstractions/IMessage.cs` | + `EncryptionContext` |
| `Internal/Message.cs` | ctor param + property |
| `Internal/MessageFactory.cs` L50-101 | pass `EncryptionContext?` |
| `Internal/Abstractions/IMessageFactory.cs` | signature |
| `Internal/BatchHandler.cs` L41-71 | pass context to each message |
| `Internal/ConsumerChannel.cs` L37-71, L98-135 | ctor params; decrypt step §5.3 |
| `Internal/ConsumerChannelFactory.cs` L32-61 | pass through |
| `Internal/Consumer.cs` L461-466; `Internal/Reader.cs` L289-291 | build `MessageCrypto` if reader set |
| `ProducerOptions.cs`, `ConsumerOptions.cs`, `ReaderOptions.cs` | new options + defaults |
| `Abstractions/IProducerBuilder.cs`, `IConsumerBuilder.cs`, `IReaderBuilder.cs`; `Internal/ProducerBuilder.cs`, `ConsumerBuilder.cs`, `ReaderBuilder.cs` | builder methods + `Create()` mapping |
| `Extensions/ProducerBuilderExtensions.cs`, `ConsumerBuilderExtensions.cs`, `ReaderBuilderExtensions.cs` | `DefaultCryptoKeyReader` overloads |
| `PulsarClient.cs` L59-84 | validation + `MessageCrypto` creation + initial wrap |
| `Internal/Producer.cs` L50-86, L156-172, L174-188 | ctor param, pass to factory and channel, dispose |
| `Internal/ProducerChannelFactory.cs` L31-82 | `Encrypted = true`; pass crypto to channel |
| `Internal/ProducerChannel.cs` L32-111 | encrypt after compress; reset in `finally` |
| `Internal/SubProducer.cs` L107-149 | per-SendOp `CryptoException` handling |
| `CHANGELOG.md` | "Added: end-to-end encryption (ICryptoKeyReader, DefaultCryptoKeyReader, EncryptionKey/CryptoKeyReader/CryptoFailureAction on producer, consumer and reader builders, IMessage.EncryptionContext, CryptoException)"; "Changed: IMessage has a new member" |

## 9. Tests

Test resources: copy `pulsar-broker/src/test/resources/certificate/{public,private}-key.client-{rsa,ecdsa,rsa-pkcs8,mismatch-rsa}.pem`
and `pulsar-client/src/test/resources/crypto_{rsa,ecdsa}_{public,private}.key` (Apache-2.0, same license) to
`tests/DotPulsar.Tests/Resources/Crypto/` with `CopyToOutputDirectory=PreserveNewest`. Java tests use
`file:./src/test/resources/certificate/...`. In C#, use the equivalent `file:./Resources/Crypto/...`, which resolves
against the test output directory (the xUnit current directory). The `data:` URIs in `testDefaultCryptoKeyReader` are
copied verbatim.

Classification: **P** = portable (port with identical intent and values), **A** = adapted (language or
infrastructure change, stated), **N** = not portable (reason).

### 9.1 Unit tests (`tests/DotPulsar.Tests/Internal/Crypto/`, `[Trait("Category", "Unit")]`)

There is no upstream `MessageCryptoBcTest` in the tree. These tests are DotPulsar-specific and pin the wire format.

| C# class#method | Asserts |
|---|---|
| `Kdf2Sha1Tests.Generate_GivenVectorInput_ShouldReturnExpectedBytes` | §3.3 vector `K` |
| `EciesKeyWrapTests.IesEncrypt_GivenVectorInput_ShouldReturnExpectedCiphertextAndTag` | §3.3 vector `C`, `T` |
| `EciesKeyWrapTests.WrapUnwrap_GivenSecp128r1ExplicitKey_ShouldRoundTrip` | test key pair from resources; output length 85 |
| `EciesKeyWrapTests.WrapUnwrap_GivenSecp521r1ExplicitKey_ShouldRoundTrip` | `crypto_ecdsa_*.key`; length 1+132+32+20 = 185 |
| `EciesKeyWrapTests.Unwrap_GivenTamperedMac_ShouldFail` | constant-time MAC failure |
| `RsaKeyWrapTests.WrapUnwrap_GivenPkcs1AndPkcs8Keys_ShouldRoundTrip` | 256-byte wrapped key |
| `PemKeyLoaderTests.Load_Given{RsaPkcs1Private,RsaPkcs8Private,RsaSpkiPublic,EcExplicitParamsPrivate,EcExplicitParamsPublic}_ShouldReturn{Rsa,EcDh}` | `[Theory]` over resource files |
| `AesGcmCipherTests.Encrypt_GivenNistTestCase13And14_ShouldMatch` | McGrew-Viega GCM test cases 13 (K=0^256, IV=0^96, P=∅ → T=`530f8afbc74536b9a963b4f1c4cb738b`) and 14 (P=0^128 → C=`cea7403d4d606b6e074ec5d3baf39d18`, T=`d0d1c8a799996bf0265b98b5d48ab919`) |
| `MessageCryptoTests.Encrypt_GivenTwoKeyNames_ShouldWriteSortedEncryptionKeysAndTwelveByteIv` | metadata fields, no `encryption_algo`, payload length +16 |
| `MessageCryptoTests.TryDecrypt_GivenEncryptedPayload_ShouldUseLastKeyCacheWithoutCallingReaderTwice` | NSubstitute reader `Received(1)` across 2 messages |
| `MessageCryptoTests.TryDecrypt_GivenWrongPrivateKey_ShouldReturnFalse` | |
| `MessageCryptoTests.AddPublicKeyCipher_GivenFailingSecondKey_ShouldKeepPreviousSnapshot` | §5.1 atomicity |
| `MessageCryptoTests.TryDecrypt_GivenMultiSegmentSequence_ShouldDecrypt` | multi-segment input |
| `DefaultCryptoKeyReaderTests` | Port of `pulsar-client/src/test/java/.../DefaultCryptoKeyReaderTest.java` (fetch at the pinned SHA; **P** for the file/data/default/map cases, **A**: Java `null` key ⇒ C# `null` return) |
| `ProducerBuilderTests.EncryptionKey_GivenBlankName_ShouldThrowArgumentException` | |
| `ConsumerChannelTests.Receive_GivenUndecryptableMessageAndDiscard_ShouldSendAckWithDecryptionError` (+ `Fail`, `Consume` variants) | NSubstitute `IConnection` |

### 9.2 Integration tests

Location: `tests/DotPulsar.Tests/Encryption/` (`[Collection("Integration"), Trait("Category", "Integration")]`,
existing `IntegrationFixture`). Transaction tests stay with their transaction classes.

| Upstream | Class | C# | Cls | Notes |
|---|---|---|---|---|
| `TransactionTest#testEncryptionRequired` L1793-1814 | `TransactionTests` | `Send_GivenEncryptionRequiredNamespaceAndEncryptedTransactionalMessage_ShouldCommit` | A | Namespace `tnx/testEncryptionRequired`, `setEncryptionRequiredStatus(true)` via `pulsar-admin namespaces set-encryption-required -e tnx/testEncryptionRequired` (fixture exec). Key name `"my-app-key"`, `DefaultCryptoKeyReader("file:./Resources/Crypto/public-key.client-rsa.pem")`, send timeout 5s, txn timeout 5s |
| `TransactionTest#testDelayedDeliveryExceedsMaxDelay` L2046-2081 | `TransactionTests` | `Send_GivenEncryptedTransactionalMessageExceedingMaxDeliveryDelay_ShouldThrowNotAllowedException` | A | owned by the transaction test spec; listed for the encryption dependency |
| `SimpleProducerConsumerTest#testRSAEncryption` L2774-2868 | `SimpleProducerConsumerTests` | `Receive_GivenRsaEncryptedMessagesFromThreeProducers_ShouldDecryptInOrder` | P | 3 producers (`client-rsa.pem` ×2, `client-rsa-pkcs8.pem`), 10 msgs each, `"my-message-{i}"`. The normal consumer (other topic, as upstream) gets `null` within `RECEIVE_TIMEOUT_MEDIUM_MILLIS` (=1500 ms in `ProducerConsumerBase`; verify). Every message has `EncryptionContext` |
| `#testECDSAEncryption` L2687-2772 | same | `Receive_GivenEcdsaEncryptedLz4Messages_ShouldDecrypt` | P (net8+, skip on macOS) | LZ4 (test project already references K4os); peek via admin (`pulsar-admin topics peek-messages`) must succeed |
| `#testCryptoWithChunking` L2871-2903 | same | `Receive_GivenEcdsaEncryptedChunkedMessage_ShouldReassembleAndDecrypt` | A | Upstream sets the broker `maxMessageSize=1000`. The shared fixture cannot do that, so set the producer chunk max size to 1000 (`ChunkMaxMessageSize`, chunking.md) and send 5100 random bytes. Assert equal data and `EncryptionContext.Keys.Count == 1` |
| `#testDefaultCryptoKeyReader` L2905-3042 | same | `Receive_GivenDefaultCryptoKeyReaderWithFileAndDataUris_ShouldDecrypt` | P | 4 consumers (`sub1..sub4`), 4 producers, 10 msgs each, exact data URIs |
| `#testEncryptionFailure` L3192-3330 | same | `Receive_GivenEncryptionFailureActions_ShouldFailConsumeOrDiscard` | A | Step 1: `Should.Throw<CryptoException>(() => builder.Create())` (Java: create() throws). Step 3: `ReceiveOrNull(5s)` is null. Steps 4-6 as upstream, with `IsEncrypted` asserts. `acknowledgmentGroupTime(0)` has no equivalent (DotPulsar acks immediately) |
| `#testEncryptionConsumerWithoutCryptoReader` L3332-3465 | same | `Receive_GivenConsumeActionWithoutCryptoKeyReader_ShouldAllowManualDecryption` | A | Uses `TopicsPattern` as upstream; manual decryption via `new MessageCrypto(keyGenNeeded: false).TryDecrypt(...)` plus the LZ4 decompressor (Java uses `MessageCryptoBc` directly); key metadata `version=1.0` |
| `#testEncryptionFailureWithBatchPublish` L5264-5335 | same | — | depends | **A** once producer batching + ack timeout land (2000 msgs, batch max 5, ECDSA, consumer `Discard` + ack timeout 1s, receiver queue 100; assert cursor read position via admin internal-stats). Otherwise blocked |
| `#testE2EEncryptionWithCompression` L5441-5487 | same | `Receive_GivenMismatchedKeyAndConsumeAction_ShouldDeliverEncryptedCompressedPayload` | P | `Schema.String`, LZ4, `compressionMinMsgBodySize(1)` (DotPulsar compresses always: no-op), 10 msgs, `IsEncrypted == true` |
| `#testRedeliveryOfFailedMessages` L3044-3190 | — | — | N | `groups = "quarantine"` upstream (flaky); skip |
| `ProducerEncryptionKeyFailureTest#testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind` L64-82 | `ProducerEncryptionKeyFailureTests` | `Create_GivenKeyThatCannotBeLoaded_ShouldThrowCryptoExceptionAndNotRegisterPublisher` | A | `Should.Throw<CryptoException>`. Then for 3s, poll the admin REST `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/stats` → `publishers` is empty. Client-side producer count: no public API, so assert the `PulsarClient` has not added a disposable (skip that assert, documented) |
| `cpp tests/EncryptionTest.cc#testDecryptionSuccess/Failure` L132-145 | `EncryptionInteropTests` | — | N (C++-only) | covered by the CLI interop tests below |
| `websocket/*ClientSideEncryption*` | — | — | N | websocket proxy out of scope |

### 9.3 Interop tests (`tests/DotPulsar.Tests/Encryption/EncryptionInteropTests.cs`, Integration)

These use the `pulsar-client` CLI inside the `apachepulsar/pulsar:4.0.9` container, which is the Java client. Add
`IntegrationFixture.ExecInPulsar(string bashCommand, CancellationToken)` (returns stdout/exit code) and
`CopyToPulsar(byte[] content, string path, CancellationToken)`. Both wrap the existing `_pulsarCluster.ExecAsync` and
`CopyAsync`, as used by `CreateTopic`/`AddSchemaToExistingTopic` (IntegrationFixture.cs L160-199). Copy the PEM files to
`/pulsar/keys/`. CLI flags are verified against `CmdProduce.java` L140-147 and `CmdConsume.java` L104-132 (the same
flags exist in 4.0.x).

| C# | Steps |
|---|---|
| `Receive_GivenJavaProducedRsaEncryptedMessage_ShouldDecrypt` | `bin/pulsar-client produce {topic} -m "java-rsa-0" -n 1 -ekn client-rsa.pem -ekv file:///pulsar/keys/public-key.client-rsa.pem`. DotPulsar consumer (Earliest) with `DefaultCryptoKeyReader(privateKey: "file:<local path>")` receives `"java-rsa-0"`, `IsEncrypted == false` |
| `Receive_GivenJavaProducedEcdsaEncryptedMessage_ShouldDecrypt` | same with `client-ecdsa.pem` (net8+, skip on macOS) |
| `Consume_GivenDotPulsarProducedRsaEncryptedMessage_ShouldBeDecryptedByJavaCli` | DotPulsar producer (key `client-rsa.pem`, public PEM) sends `"dotpulsar-rsa-0"`. Then `bin/pulsar-client consume {topic} -s interop -p Earliest -n 1 -ekv file:///pulsar/keys/private-key.client-rsa.pem` exits 0 and stdout contains `content:dotpulsar-rsa-0` |
| `Consume_GivenDotPulsarProducedEcdsaEncryptedMessage_ShouldBeDecryptedByJavaCli` | same with ECDSA (proves the §3.3 byte format against real BouncyCastle) |
| `Consume_GivenDotPulsarProducedEncryptedLz4Message_ShouldBeDecryptedByJavaCli` | producer `CompressionType.Lz4` (proves compress→encrypt order) |
| `Consume_GivenDotPulsarProducedEncryptedMessageWithoutKey_ShouldNotBeDelivered` | CLI consume with `-ca DISCARD`, `-n 1` and a 10s `timeout` wrapper. Nothing is printed and the message is acked: check `pulsar-admin topics stats` `msgBacklog == 0` |

The ordering of producer and consumer in the CLI tests: subscribe first (`-p Earliest` makes order irrelevant). The CLI
prints `----- got message -----` followed by the content line (`AbstractCmdConsume.MESSAGE_BOUNDARY`, L52). Assert
with `ShouldContain`.

## 10. Acceptance criteria

- A1: All unit tests in §9.1 pass on net8.0, net9.0 and net10.0. RSA/AES tests also pass on a netstandard2.1 test host
  if one is configured (the test project is net8+ only; ns2.1 code is compile-checked).
- A2: `TransactionTests.Send_GivenEncryptionRequiredNamespaceAndEncryptedTransactionalMessage_ShouldCommit` passes.
  Without encryption configured, the same send fails (manual negative check, not a test).
- A3: All four directions of the §9.3 interop tests pass. This proves byte-exact RSA, ECIES and AES-GCM compatibility
  with Java/BC.
- A4: No change to the wire for producers and consumers without encryption options (`CommandProducer.encrypted` stays
  unset; metadata untouched). Existing test suite green.
- A5: `MessageMetadataObjectPool` objects returned after an encrypted send have no `EncryptionKeys` and no
  `EncryptionParam`. Unit test: send twice through `ProducerChannel` with a stub `IConnection` and inspect the metadata
  after `Send` returns.
- A6: `PemKeyLoaderTests` for the explicit-parameter EC keys pass on Windows and Linux CI. If secp128r1 explicit import
  fails on either OS, switch to Option B (§11 D1) for EC only and record the result.
- A7: Public API docs present; `CHANGELOG.md` updated; no new warnings (`GenerateDocumentationFile`).
- A8: Steady-state encrypt/decrypt allocates only the output buffer and the IV `ByteString` (verify with a
  BenchmarkDotNet run in `benchmarks/`, optional).

## 11. Open decisions (recommendation)

- **D1 — Crypto backend.**
  *Option A (recommended):* pure BCL, no new dependency. AES-GCM and RSA on netstandard2.1+, ECIES on net8+ via
  `DeriveRawSecretAgreement`, nothing on netstandard2.0 (`PlatformNotSupportedException` at `Create()`). This keeps the
  repo's zero-dependency stance; compression libraries are optional and reflection-loaded. Every portable test runs on
  net8+, and it is the cheapest to review.
  *Option B:* add `BouncyCastle.Cryptography` (MIT, netstandard2.0 + net6) as a hard dependency on all TFMs. Use BC for
  PEM, RSA-OAEP and ECIES (`IesEngine`, exact parity by construction), and BC `GcmBlockCipher` on netstandard2.0. Full
  matrix including .NET Framework and macOS explicit curves; about 6 MB of dependency.
  *Option C:* a separate optional package `DotPulsar.Encryption.BouncyCastle` that plugs in through an internal
  `IMessageCryptoProvider` discovered by reflection (the `CompressionFactories` precedent). Most flexible, most work.
  Choose A now; B or C can be added later behind the same `MessageCrypto` internal seam.
- **D2 — Keys without a reader.** Java silently sends **unencrypted** when `addEncryptionKey` is used without a
  `cryptoKeyReader` (`isEncryptionEnabled` L266-268). Recommendation: keep parity (no exception) but document it in the
  `EncryptionKey` XML remarks. Alternative: throw `ConfigurationException("A CryptoKeyReader must be set when encryption keys are added")`,
  which is safer but deviates. Keep parity because the upstream tests rely on neither behaviour.
- **D3 — `ICryptoKeyReader` sync vs async.** Recommend sync (parity, allows a synchronous `CryptoException` from
  `Create()`). An async variant would need `Create()` to stay non-throwing and the failure to surface as
  `ProducerState.Faulted`, which breaks the `ProducerEncryptionKeyFailureTest` port shape.
- **D4 — `EncryptionKey` builder name.** `EncryptionKey(string)` (DotPulsar `ProducerProperty(key, value)` style) vs
  `AddEncryptionKey(string)` (Java). Recommend `EncryptionKey`.
- **D5 — Reader support.** Recommend including it (one option pair, same channel code). Java supports it.
- **D6 — `encryption_algo`.** Recommend never writing it (parity with Java and C++) and exposing it read-only via
  `EncryptionContext.Algorithm`.
