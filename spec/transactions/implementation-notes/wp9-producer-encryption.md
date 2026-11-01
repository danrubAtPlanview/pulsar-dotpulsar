# WP9 producer as built: encryption in the producer (merged)

Commits on `feature/transactions` (cherry-picked from the agent branch, clean merge): `Add the encryption options to the producer`, `Encrypt the messages of the producer`, `Add unit tests for encrypting producer messages`, `Do not retry a CryptoException`, `Add integration tests of producer encryption`, `Add changelog entries for encrypting messages`, `Add a benchmark of sending encrypted messages`, `Check that the interop messages are encrypted`, then `Document end-to-end encryption` (README and `docs/api/index.md`).
`Do not retry a CryptoException` is the one change outside the producer: `RetriableErrors` had a TODO to add `CryptoException` when encryption landed, and Java lists it as non-retriable.
After the merge: Debug unit 2116 passed, 11 skipped; full solution Release build 0 errors, the 4 pre-existing warnings; no `TODO(WP...)` marker is left in `src/` or `tests/`.

On the agent's base: Integration collection 215 passed, 26 skipped, 0 failed (20 m); `Transaction` collection 107 passed, 2 skipped, 1 failed (`Receive_GivenMessagesInPendingAckStateAndSmallPrefetch_ShouldNotGetConsumerStuck`, the known load flake; its class alone: 26 passed); `EncryptionInteropTests` 11, `ConsumerEncryptionTests` 25, `ProducerEncryptionKeyFailureTests` 3, `EncryptionRequiredTests` 2, `TransactionTests` 13, each 3 runs, all passed. Not run: the `TransactionWithoutBatchIndexAck` collection, netstandard2.0 at runtime, macOS and Linux. Release unit (without `MessageProcessorTests`) 2123 passed.

## Public API added

- `ProducerOptions<T>`: static `DefaultCryptoFailureAction` (`Fail`), `CryptoKeyReader` (`ICryptoKeyReader?`), `EncryptionKeys` (`IEnumerable<string>`, ordinal-sorted set), `CryptoFailureAction` (`ProducerCryptoFailureAction`).
- `IProducerBuilder<T>`: `CryptoKeyReader(ICryptoKeyReader)`, `EncryptionKey(string)` (a blank name throws `ArgumentException`), `CryptoFailureAction(ProducerCryptoFailureAction)`.
- `ProducerBuilderExtensions.DefaultCryptoKeyReader(string publicKey)` and `(IReadOnlyDictionary<string, string> publicKeys)`.
- Validation in `ProducerBuilder.Create` and `PulsarClient.CreateProducer`: encryption keys without a key reader throw `ConfigurationException` (**deviation from spec decision D2**, which recommended Java parity: the Java client sends the messages unencrypted). A key reader without keys is allowed. In `CreateProducer` only, a blank key name in the options or an undefined `CryptoFailureAction` also throws `ConfigurationException`.
- Internal: `ProducerChannel` and `ProducerChannelFactory` got required trailing `MessageCrypto?` and `ProducerCryptoFailureAction` parameters; `SubProducer` an optional trailing `MessageCrypto?`.

## Wiring

- `Producer`'s constructor creates the `MessageCrypto(true)` and wraps the data key synchronously: a key that cannot be used makes `Create()` throw `CryptoException` (`PlatformNotSupportedException` on netstandard2.0) before anything connects.
- One `MessageCrypto` per sub-producer (**deviation from spec section 5.1**, which shares one per `Producer`), disposed in the sub-producer's `InternalDispose`; the first sub-producer takes the one made at `Create()`, other partitions create theirs in `Monitor`. The key reader is therefore called once per partition and again on every 4-hour refresh.
- `CommandProducer.Encrypted = true` when a crypto exists, otherwise unset, so the plain wire is unchanged.
- `ProducerChannel.Send` encrypts after compression (a batch is one payload with one IV). `SendChunk` encrypts every chunk with its own IV; `PlanChunking` subtracts `GetMetadataOverhead()` from the chunk size. `EncryptionKeys` and `EncryptionParam` of the pooled metadata are cleared in `finally` whenever they were set, also on failure paths.
- Failure handling is in `SubProducer.AwaitSend` and `SendChunks` (the spec said `MessageDispatcher`; the initial wrap is in the `Producer` constructor, not in `PulsarClient`; the factory derives `Encrypted` from the crypto instance). With `Fail` only that group fails with the exception and the producer is not faulted. With `Send` only a `CryptoException` falls back to plaintext; a disposed crypto (`ObjectDisposedException`) never does.
- The dead letter producer is unchanged and still publishes plain text (parity with Java, followup 0010 item 1 stays open); a unit test pins it.

## Tests

About 65 new unit tests, and the integration classes above. Interop in both directions: a .NET producer consumed by the Java CLI (RSA, ECDSA, LZ4, a batch, a chunked message, a no-key `-ca DISCARD` case) and Java perf output (LZ4+ECDSA, a batch, a 12 MB chunked message) consumed by .NET. `testEncryptionRequired` is ported as `Send_GivenEncryptionRequiredNamespaceAndEncryptedTransactionalMessage_ShouldCommit` (the spec said `..._ShouldSucceed`; the flag is set through the admin REST endpoint). Not ported: the batch-container crypto tests A3.5, A3.7 and A3.23 (encryption lives in `ProducerChannel`; the batch plus encryption tests are in `BatchSendThroughProducerChannelTests`); the client-side producer count assert of `testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind` (no API). `testEncryptionFailureWithBatchPublish` uses RSA and has no cursor-position check.
42 mutations of the wiring were run against the unit suite and 2 against the integration tests: 37 of the 42 unit mutations were killed; both integration mutations were killed after one test was strengthened (removing chunk encryption was not caught by the Java CLI test, because Java also reads unencrypted messages; `8d08e60` reads the encryption context back). Survivors are equivalent or unobservable: M09 (Send action returns `true`, only resets fields that were never set), M29 (dispose on a failed wrap), M24 (`Producer` passes the failure action to the factory; a real `MessageCrypto` cannot be made to fail), M25 and M30 (`Producer` hands the crypto to `SubProducer` and disposes the leftover initial crypto; not observable through the public surface, `SubProducer` disposal is tested directly).

## Allocations and performance (Release, in-process, bytes per op, direct / thread-pool)

| Benchmark | Base | With encryption wiring |
|---|---|---|
| PlainSend | 2950, 2953 / 3005, 3006 | 2957, 2954 / 3003, 3005 (unchanged within noise; no `SendOp` field added) |

`benchmarks/Transactions/EncryptionBenchmarks.cs` (fake connection): plain 100 B 2953 B and 3.6 us; encrypted 100 B 3182 B (+229 B: the output buffer plus the 12-byte IV `ByteString`) and 4.6 us; 10 chunks 11251 B and 8.6 us; 10 chunks encrypted 29943 B and 19.5 us (dominated by copying 15 KB through the encrypt step).

## Follow-ups

See [0013](../followups/0013-wp9-producer-findings.md).
