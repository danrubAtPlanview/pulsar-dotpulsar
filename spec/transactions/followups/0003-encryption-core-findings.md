# Follow-up 0003 — Findings from the standalone encryption core (WP9a)

| | |
|---|---|
| **Status** | Item 4 (deferred tests) done in WP10. Items 1 to 3 (spec corrections, platform checks on macOS, behaviour notes) are open for review. |
| **Found by** | WP9a (crypto core implemented and tested before the channel wiring) |
| **Severity** | Low. None of it blocks a work package. |

The crypto core (`Internal/Crypto/*`, public key reader and context types) is merged without wiring. It adds no NuGet
package: the BCL-only option (Option A in `06-prerequisites/encryption.md`) works because Windows imports the upstream test
keys. These are the points that the spec text gets wrong, or that need a second look before release.

## 1. Corrections to `06-prerequisites/encryption.md` and the test-key notes

| # | Spec statement | What was found | Action |
|---|---|---|---|
| 1 | `*-rsa.pem` test keys are RSA-2048 | `private-key.client-mismatch-rsa.pem` is **an EC key** (secp521r1 with explicit parameters) despite its name | Fix the key table in §9.1; the "mismatch" test must be written knowing it is EC |
| 2 | `client-mismatch` public key differs from `client` | `public-key.client-mismatch-rsa.pem` is **byte-identical** to `public-key.client-rsa.pem` | Do not rely on it as a different key; generate a real mismatching RSA key if a test needs one |
| 3 | `public-key.client-rsa-pkcs8.pem` is the PKCS#8 form of the same key | It is a **different** RSA key from `public-key.client-rsa.pem` | Treat the two as independent key pairs in tests |
| 4 | Ports of `MessageCryptoBcTest` / `MessageCryptoTest` | Neither class exists at the pinned SHA `f3846f4` | Reword §9.1 to say the tests are DotPulsar-specific, with known-answer vectors produced by the Java `MessageCryptoBc` |

## 2. Platform support matrix (to publish in the XML docs and README when WP9 is wired)

| Runtime | RSA key wrap | EC key wrap | Notes |
|---|---|---|---|
| net8.0, net9.0, net10.0 on Windows (CNG) | works | works | explicit-parameter secp128r1 and secp521r1 import fine (R12 is **not** a problem on Windows 11) |
| Linux (OpenSSL), via the `dotnet/sdk:10.0` container | works | works | same unit tests pass |
| macOS | not verified | not verified | EC tests use `Assert.SkipWhen(OperatingSystem.IsMacOS(), …)`. **Needs a run on a Mac before release.** |
| netstandard2.1 hosts older than net8 (net6, net7) | works | `CryptoException("Unsupported key type EC … on this platform")` | |
| .NET Core 3.1 | works | EC key import fails ("ASN1 corrupted data") | out of support |
| netstandard2.0 / .NET Framework 4.8 | not available | not available | `AddPublicKeyCipher` throws `PlatformNotSupportedException("End-to-end encryption requires netstandard2.1 or later")`; `TryDecrypt` returns false |

## 3. Behaviour worth reviewing

- An EC point that is not on a named curve makes Windows throw `PlatformNotSupportedException` rather than `CryptographicException`.
  `EciesKeyWrap.Unwrap` converts it so callers see a single failure type. Confirm the same on Linux and macOS.
- The 4-hour data-key refresh timer starts inside `MessageCrypto`. Producer disposal must dispose it (wiring step).
- `EncryptionContextFactory` is public only so the wiring code can build `EncryptionContext`, whose constructors are internal. Revisit whether
  `EncryptionContext` should get a public or internal factory when the public API is reviewed.
- Interop with Java was verified in both directions with the Java `MessageCryptoBc` running inside the broker image (RSA PKCS#1 and PKCS#8,
  secp128r1, secp521r1, tamper detection): 18 passed, 6 skipped. The six skipped tests need the encryption wiring and must be un-skipped by WP9.

## 4. Deferred tests (owned by the wiring step)

- `EncryptionInteropTests`: the six `[Fact(Skip = "Requires encryption wiring (WP9)")]` tests.
- Broker integration tests from §9.2, `ProducerBuilderTests.EncryptionKey_GivenBlankName_ShouldThrowArgumentException` and the
  `ConsumerChannelTests` undecryptable-message variants, which need edits to existing files.
- `TransactionTest#testEncryptionRequired` (the reason this feature exists for the transaction parity matrix).

## Status at the end of WP10

Verified at the end of WP10: item 4 (the deferred tests) is done. No test is skipped for the encryption wiring any more, `ProducerBuilderTests.EncryptionKey_GivenBlankKeyName_ShouldThrowArgumentException` exists, and `TransactionTest#testEncryptionRequired` is ported (`TransactionTests.Send_GivenEncryptionRequiredNamespaceAndEncryptedTransactionalMessage_ShouldCommit`, `done: true` in `upstream-map.yaml`). Items 1 to 3 stay review notes: the key table of the spec, the platform matrix (the runtime requirements are in the CHANGELOG, macOS was not run) and the behaviour notes.

Items 1 to 3 (the facts about the test keys) are applied to `06-prerequisites/encryption.md` (section with the test key facts). What stays open is the run of the EC tests on macOS, which needs a Mac.

## Closing of the macOS run

Decided by the requester on 2026-11-01: the run of the EC encryption tests on macOS is an external item that is owned by the maintainers (it needs a Mac; the tests are skipped on macOS by `Assert.SkipWhen(OperatingSystem.IsMacOS(), ...)`, and they pass on Windows and in the Linux container). It is listed in the pull request description.
