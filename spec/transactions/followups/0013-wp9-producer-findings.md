# 0013 - Findings from WP9 (producer encryption)

Status: open, needs review after parity.

## 1. The producer failure actions are almost unreachable

The data key is wrapped at creation, so `MessageCrypto.Encrypt` can only throw `CryptoException` on an AES-GCM error. `ProducerCryptoFailureAction.Send` therefore has almost no way to take effect. Kept for parity with Java.

## 2. A failed 4-hour key refresh is swallowed silently

The repo has no logging, so a refresh of the data key that fails leaves no diagnostic. Consider a state change or an event when a logging facility exists.

## 3. A key error on the second or later partition faults the producer

For a partitioned topic, the first sub-producer takes the crypto made at `Create()`; the others create theirs in `Monitor`. A key that cannot be used on partition 2 or later faults the producer (state `Faulted`, `ProducerFaultedException` on send) instead of throwing from `Create()`. Fix by wrapping the key once, up front, for every partition (a shared wrapped data key is not possible because each `MessageCrypto` owns its data key), or by validating the public keys in `Create()`.

## 4. Chunking and encryption

A chunk that fails to encrypt in the middle of a message leaves the earlier chunks on the broker (matches Java; the consumer drops them on expiry). A user-set `ChunkMaxMessageSize` is not reduced by the encryption overhead (matches Java). A batch close to the maximum message size can still exceed it after encryption and then fails with `TooLargeMessageException`.

## 5. The dead letter topic still receives decrypted payloads

Unchanged, see followup 0010 item 1.

## 6. Keys without a key reader throw

Decision D2 of the spec recommended Java parity (send unencrypted without a reader). As built, `ConfigurationException`, because silently sending clear text when the user asked for encryption is a security hazard. Revisit only if maintainers want Java parity.

## 7. Process note

The wp9-producer agent overwrote another agent's `scratchpad/mutate.sh` by accident (replaced by an error stub; its own files are in `scratchpad/wp9p/`). No source file was affected.

Decisions (no code change): item 1 is Java parity and kept. Item 2: the library has no logging facility, so nothing can be written when a refresh fails, accepted. Item 3: a key that the key reader cannot give for the second or a later partition faults the producer (state `Faulted`); the first partition fails in `Create()`. Accepted: it needs the key reader to fail for one partition and not for the others. Item 4 is Java parity. Item 5 is followup 0010 item 1 (documented in `DeadLetterPolicy`). Item 6 is a deliberate hardening (`ConfigurationException` where Java sends clear text), listed in `deviations-summary.md`. Item 7 is a note about the work process.
