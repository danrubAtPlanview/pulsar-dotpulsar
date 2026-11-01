# Component: `TransactionId`

| | |
|---|---|
| C# | `src/DotPulsar/TransactionId.cs`: `public readonly struct TransactionId : IEquatable<TransactionId>` |
| Upstream | `java:pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TxnID.java` (L44 `mostSigBits`, L51 `leastSigBits`, L62-63 `toString`, L77-85 `equals`) |
| WP | WP3 |

## Definition

The full code is in [03-public-api §2.1](../03-public-api.md#21-srcdotpulsartransactionidcs).

## Behaviour

| Member | Behaviour | Upstream |
|---|---|---|
| ctor `(ulong mostSigBits, ulong leastSigBits)` | Stores both values. Note the parameter order (most, least); Java `TxnID(most, least)` uses the same order. | `TxnID` (Lombok ctor) |
| `ToString()` | `"(" + MostSigBits + "," + LeastSigBits + ")"`, invariant culture. Use `$"({MostSigBits},{LeastSigBits})"`; formatting a `ulong` does not depend on culture. | `toString` L63 |
| `Equals`/`==`/`!=` | Both values equal. | L77-85 |
| `GetHashCode` | `HashCode.Combine(MostSigBits, LeastSigBits)`. Java caches its hash, but a 16-byte struct needs no cache. | L62 |

**Wire mapping.** Proto `uint64` maps to `ulong` with no sign conversion. Java's `long` is signed. Real TC ids and
sequence numbers are below 2^63, so `ToString` gives the same text in both languages.

**Construction from responses.** The response carries both halves as separate fields, so build the id as
`new TransactionId(response.TxnidMostBits, response.TxnidLeastBits)`. Keep the order straight: Java's
`TransactionImpl` constructor takes `(least, most)`, but this struct takes `(most, least)`.

## Tests (`tests/DotPulsar.Tests/TransactionIdTests.cs`, Unit)

| Test | Assert |
|---|---|
| `ToString_GivenMostAndLeast_ShouldFormatLikeUpstream` | `new TransactionId(1, 5).ToString() == "(1,5)"` |
| `Equals_GivenSameBits_ShouldBeEqual` / `Equals_GivenDifferentBits_ShouldNotBeEqual` | `==`, `!=`, `Equals(object)`, `GetHashCode` all consistent |
| `Default_ShouldBeZeroZero` | `default(TransactionId).ToString() == "(0,0)"` |
