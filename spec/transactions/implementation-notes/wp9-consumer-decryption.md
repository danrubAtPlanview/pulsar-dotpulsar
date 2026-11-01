# WP9 consumer as built: decrypting messages in the consumer and the reader (merged)

Commits on `feature/transactions`: `Add crypto options and encryption context`, `Decrypt messages in consumer and reader`, `Add unit tests for decrypting consumer messages`, `Reword the remark of the reader crypto action`, `Add integration tests for decrypting messages`, `Add changelog entries for decrypting messages`, `Test that undecryptable batches are not over counted`.
The merge conflicted only in `IntegrationFixture.cs` (kept `ExecInPulsar` and `CopyToPulsar` together with the batching helper). Three existing test call sites (`SubConsumerAckPathTests`, `SubConsumerTransactionalAckTests` x2) needed the new trailing constructor arguments; the fix was folded into the decrypt commit.
After the merge: Debug unit 1948 passed, 11 skipped (allocation tests skip in Debug); Release unit 1955 passed, 3 runs (`MessageProcessorTests` excluded).
The merged state has **not** been run against a broker as a whole. On the agent's own base: Integration collection 150 passed, 31 skipped, 0 failed; Transaction collection 86 passed, 1 failed (`Receive_GivenMessagesInPendingAckStateAndSmallPrefetch_ShouldNotGetConsumerStuck`, 3 s receive timeout under a full-collection run, passes alone), 15 skipped.

## Public API added

The core already had `ICryptoKeyReader`, `EncryptionKeyInfo`, `DefaultCryptoKeyReader`, `EncryptionContext`, `EncryptionKey` and the two failure-action enums. Added:

- `ConsumerOptions` / `ReaderOptions`: `CryptoKeyReader`, `CryptoFailureAction` (default `Fail`), static `DefaultCryptoFailureAction`.
- `IConsumerBuilder` / `IReaderBuilder`: `CryptoKeyReader(...)`, `CryptoFailureAction(...)`; extension methods `DefaultCryptoKeyReader(string privateKey)` and `DefaultCryptoKeyReader(IReadOnlyDictionary<string, string> privateKeys)` for both builders.
- `IMessage.EncryptionContext` (breaking for custom `IMessage` implementations). **Superseded by decision 0009 / WP12:** it is the extension method `GetEncryptionContext()` now, and `IMessage` is unchanged.
- `PulsarClient.CreateConsumer` / `CreateReader` throw `ConfigurationException` for an undefined `CryptoFailureAction` (Java has no check).

## Wiring

- `Consumer.cs` / `Reader.cs` create one `MessageCrypto(false)` per sub-consumer / sub-reader when a key reader is set; `SubConsumer` / `SubReader` dispose it on dispose and on channel fault.
- `ConsumerChannel.Receive` decrypts at the former `TODO(WP9)` marker with the Consume / Discard / Fail table of the core note. `Fail` adds the entry to the ack-timeout tracker. The context is built after decrypt and chunk assembly, so a chunked message carries the context of the last chunk.
- Added beyond the core note: an entry that is not unpacked (Fail, Discard, a Consume'd batch) counts as received once per message of the batch, otherwise a discarding consumer stalls when the broker permits run out (killed by `Receive_GivenEncryptedBatchesAndDiscard_...`, which is `testEncryptionFailureWithBatchPublish` upstream).
- Constructor changes: `ConsumerChannel`, `ConsumerChannelFactory`, `SubConsumer`, `SubReader` got new trailing **required** parameters (`MessageCrypto?`, plus `ICryptoKeyReader?` and `ConsumerCryptoFailureAction` on the channel). `IMessageFactory.Create` and `BatchHandler.Add` got a trailing optional `EncryptionContext? encryptionContext = null`.

## Deviations from the spec

1. **Encrypted chunk that exceeds `total_chunk_msg_size` (the open item of the spec).** An undecryptable chunk is never assembled: each encrypted chunk has its own IV and is 16 bytes larger than its share of `total_chunk_msg_size`, so the assembler would drop it. Java does not deliver this case either (its fixed-size buffer overflows and throws). Implemented: with `Consume` a chunk is treated like `Fail` (not delivered, not acked, tracked for ack timeout); `Discard` rejects each chunk; `Fail` tracks each. The assembler is untouched, `ConsumerCryptoFailureAction.Consume` carries a doc remark. This overrides `06-prerequisites/chunking.md` section 5.3, which assumed Java delivers the assembled encrypted bytes.
2. An undecryptable batch with `Consume` is delivered as one message, not unpacked (Java fails to parse it).
3. **The default `Fail` is a behaviour change.** DotPulsar used to ignore the encryption metadata and deliver the raw payload; the CHANGELOG has a Changed line that says to use `Consume` for the old behaviour.
4. With `Fail` a reader cannot redeliver, so the message is skipped (documented on `ReaderOptions`).

## Tests

- Unit: `Internal/Crypto/ConsumerChannelEncryptionTests`, `SubConsumerEncryptionTests`, `SubReaderEncryptionTests` (plus `Receive_GivenUndecryptableBatchOfFiveMessages_ShouldNotCountMoreThanTheMessagesOfTheBatchAsReceived`).
- Integration: `Encryption/ConsumerEncryptionTests` (collection `Integration`), `EncryptionInteropTests` rewritten with a fixture; the two Java-produced consumer interop tests (RSA, ECDSA) are un-skipped and pass, as does the Java-produced chunked and encrypted one.
- About 50 unit-level and 10 integration-level mutations were killed. Not observable: whether `Consumer.cs` / `Reader.cs` hand the `MessageCrypto` to the sub-consumer (only the dispose path depends on it).
- Allocations (Release, 2000 messages, agent harness): non-encrypted receive unchanged (736.6 B queued, 1288.6 B waiting); an encrypted RSA message costs 1976.6 B.

## Still skipped

- Four `Consume_GivenDotPulsarProduced...` tests and `testEncryptionRequired`: they need the encrypting producer. Marked `TODO(WP9-producer)` (the WP9 producer agent un-skips them inside the new fixture structure of `EncryptionInteropTests.cs`).
- `TransactionTests.cs:289` `TODO(WP9)` (transactional send of an encrypted message), for the WP9 producer.
- Not run: netstandard2.0 runtime (compile-only, no warnings on all five TFMs), macOS EC keys, the `TransactionWithoutBatchIndexAck` collection.

## Notes for the WP9 producer agent

- `IntegrationFixture.ExecInPulsar` / `CopyToPulsar` exist (keep one copy). Constructors that changed are listed above; any new test that builds a `ConsumerChannel`, `ConsumerChannelFactory`, `SubConsumer` or `SubReader` needs the new arguments (`ChunkTestData.CreateAssembler()`, then `null`, plus `null, null, ConsumerCryptoFailureAction.Fail` for the channel).
- Follow-ups: [0010](../followups/0010-wp9-consumer-findings.md).
