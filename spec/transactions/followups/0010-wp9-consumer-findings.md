# 0010 - Findings from WP9 (consumer decryption)

Status: items 5 (the collection) and 6 closed in WP10; items 1 to 4 and the platform checks of item 5 are open for the reviewer.

## 1. Dead-lettered messages are published decrypted

A message that goes to the dead letter topic is published with the decrypted payload and without encryption metadata. Java does the same, so this is parity, but it is a confidentiality surprise: an encrypted topic feeds a plain dead letter topic.
Decide whether the dead letter producer should re-encrypt (needs the producer encryption of WP9 producer plus key names on `DeadLetterPolicy`) or whether to document it. Low to medium severity.

## 2. `EncryptionContext` costs about 1.2 KB per message

The context is built per message: a 256 B copy of the wrapped key and a dictionary. Caching the `Keys` collection per wrapped key (the keys repeat for a batch and for a producer's data key rotation period) would cut most of it. Only encrypted messages pay it. Low severity.

## 3. Default `Fail` changes behaviour for existing users

Before, an encrypted message was delivered as raw ciphertext (the metadata was ignored). Now a consumer without a key reader that reads an encrypted message does not receive it (it is not acked and tracked for the ack timeout, as in Java).
The CHANGELOG says so. Consider whether the first release should log a warning when a message is held back because there is no key reader.

## 4. A reader skips messages it cannot decrypt (`Fail`)

A reader has no redelivery, so with `Fail` the message is skipped silently. Documented on `ReaderOptions`. Consider a diagnostic event.

## 5. Not verified

- netstandard2.0 runtime (compile-only), macOS EC keys (see also 0003).
- The `TransactionWithoutBatchIndexAck` collection after the consumer changes.
- The broker reports `RedeliveryCount` 0 on Exclusive subscriptions, so the redelivery test uses Shared.

## 6. Unit flakes seen on the agent's Release runs

`TransactionCoordinatorClientTests.EnsureStarted_GivenCancelledCallerAndFailedStart_ShouldRetryOnNextCall` (2 in 8 runs) and `TransactionMetaStoreHandlerTests.DisposeAsync_GivenCallerWaitingForPermit_...` (2 in 5 runs) failed or hung. Tracked in [0009](0009-flaky-unit-tests.md); also the open question of 0008 item 2.

## Status at the end of WP10

Item 6 is closed (the unit flakes are followup 0009). Item 5 is verified for the `TransactionWithoutBatchIndexAck` collection, which passed in every full run of the integration collections after WP9 (31 passed and 11 skipped by design); the platform checks of item 5 (netstandard2.0 is compile only, macOS EC keys) remain. Items 1 to 4 are design decisions for the reviewer: dead letter publishes decrypted payloads like Java, the cost of `EncryptionContext`, the changed default `Fail`, the reader that skips.

Decisions (no code change, except the documentation): item 1, the dead letter producer does not encrypt, like Java; this is now stated in the XML documentation of `DeadLetterPolicy` (users have to protect the dead letter topic as the topic of the messages). Item 2, the cost of `EncryptionContext` (1.2 KB per encrypted message) is accepted; only encrypted messages pay it. Item 3, the changed default `Fail` is documented in the CHANGELOG and accepted. Item 4, a reader that skips a message that it cannot decrypt is documented on `ReaderOptions` and accepted; the library has no logging to add a warning to.
