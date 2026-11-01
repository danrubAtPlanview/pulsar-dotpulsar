# 11 — Upstream traceability and sync procedure

The ported code must stay maintainable as upstream evolves. Traceability works at three levels: comments in code, a
machine-readable map, and a repeatable sync procedure.

## 1. In-code references

### 1.1 Type level (mandatory for every new or ported type)

```csharp
// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionImpl.java
public sealed class Transaction : ITransaction
```

- The comment goes on the line directly above the type declaration (above the XML `<summary>` if there is one,
  i.e. right after the `using` block and blank line).
- Format: `// Upstream: <owner>/<repo>@<7-char sha> <path>`. Several sources are comma-separated, e.g.
  `// Upstream: apache/pulsar@f3846f4 .../BatchMessageContainerImpl.java, apache/pulsar-client-cpp@f71d7e9 lib/BatchMessageContainer.cc`.
- For **existing** DotPulsar types that receive ported logic (`Producer`, `SubConsumer`, `ConsumerChannel`, …), do not
  add a type-level comment. Use member-level comments instead.

### 1.2 Member level (for non-trivial ported methods or blocks)

```csharp
// Upstream: TransactionImpl#commit
public async ValueTask Commit(CancellationToken cancellationToken)
```

```csharp
// Upstream: ConsumerImpl#doTransactionAcknowledgeForResponse (batch ack_set branch)
if (messageId.BatchIndex >= 0) { ... }
```

- Use `Class#method`, with no path (the type-level comment or the map gives it) and **no line numbers** (they drift).
- Parenthesised qualifiers are allowed: `(v4 branch only)`, `(C# deviation: async semaphore)`.
- A deviation from upstream semantics **must** be marked `(C# deviation: <reason>)`, and the same deviation must
  appear in `upstream-map.yaml` `deviations` and in [12-risks-open-questions.md](12-risks-open-questions.md).
- Tests: `// Upstream: <JavaTestClass>#<method>` above the `[Fact]`/`[Theory]`.

These are the **only** comments added for traceability. Do not add narrative comments explaining the port; this
matches the repo's sparse comment density.

## 2. `upstream-map.yaml`

The file [`upstream-map.yaml`](upstream-map.yaml) is the authoritative map. Schema:

```yaml
upstream:
  java: { repo: apache/pulsar, sha: f3846f4811b6a7d98de40a3be2caff1fe288e026 }
  cpp:  { repo: apache/pulsar-client-cpp, sha: f71d7e99019d58b1d11858f05ff0dc83b718627b }
components:
  - csharp: src/DotPulsar/Internal/Transactions/Transaction.cs        # file
    type: DotPulsar.Internal.Transactions.Transaction
    wp: WP3
    status: planned            # planned | ported | adapted | deviated | excluded
    sources:
      - repo: java
        path: pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionImpl.java
        lines: 1-280
    members:
      - csharp: Commit
        upstream: TransactionImpl#commit
        lines: 189-217
    deviations: []
tests:
  - upstream: TransactionEndToEndTest#produceCommitTest
    repo: java
    path: pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TransactionEndToEndTest.java
    lines: 313-368
    csharp: tests/DotPulsar.Tests/Transactions/TransactionEndToEndTests.cs#Commit_GivenTwoTransactions_ShouldDeliverAllMessagesAfterCommit
    status: planned           # planned | ported | partial | unit | excluded
    reason: ""
```

Implementers update `status` from `planned` to `ported`/`adapted`/`deviated` in the same PR that lands the code.
`lines` always refer to the pinned SHA.

## 3. Sync procedure (run when upstream moves; suggested cadence: each DotPulsar minor release)

1. **Find upstream changes to mapped paths:**
   ```bash
   OLD=f3846f4811b6a7d98de40a3be2caff1fe288e026
   NEW=$(gh api repos/apache/pulsar/commits/master --jq .sha)
   gh api "repos/apache/pulsar/compare/$OLD...$NEW" --jq '.files[].filename' > changed.txt
   # paths from the map:
   grep -E '^\s+path:' spec/transactions/upstream-map.yaml | awk '{print $2}' | sort -u > mapped.txt
   grep -Fxf mapped.txt changed.txt
   ```
   Do the same for `apache/pulsar-client-cpp` with its SHA and `main`.
2. **Read each relevant diff:**
   `gh api "repos/apache/pulsar/compare/$OLD...$NEW" --jq '.files[] | select(.filename=="<path>") | .patch'`.
3. **Classify each change:**
   - `behaviour`: port it.
   - `refactor` / `logging`: ignore, update the line numbers.
   - `v5-only` / `broker-only`: ignore.
   - `test-added` / `test-changed`: add or adjust the C# test, keeping the intent rule from [08](08-test-parity.md).
4. **Grep the C# side** for `// Upstream: <Class>#<method>` to find the code that corresponds to a changed method.
5. **Update the map:** bump `upstream.java.sha`, refresh `lines`, and add new rows. Update the short SHA in
   type-level comments only for files whose ported logic changed. Unchanged files may keep the old SHA; the map is
   authoritative.
6. **Record each sync** as a CHANGELOG entry under `### Changed` when behaviour changed, e.g.
   `- Aligned transaction handling with Apache Pulsar <version>`.

### 3.1 Watching C++ for real transactions

When `apache/pulsar-client-cpp` implements transactions (issue #366 closed, or `lib/st/Transaction.cc` no longer
returning `ResultOperationNotSupported`), open a tracking issue to cross-check DotPulsar against the C++ design. C++
is closer to DotPulsar's performance model. Its tests would be added as a second parity source, with a `cpp` row in
the `tests` section of the map.

## 4. Proto sync

`src/DotPulsar/Internal/PulsarApi.proto` is a copy of upstream `pulsar-common/src/main/proto/PulsarApi.proto`, with
`option csharp_namespace` added. When syncing:

- Port new messages and fields only when they are needed.
- Prefer upstream names. When upstream renames a field, rename it in DotPulsar too, keeping the same field number, so that wire compatibility is preserved. Example: `CommandNewTxn.txn_ttl_seconds` became `txn_ttl_millis`.
- After any rename, check the generated C# property names at every call site.
