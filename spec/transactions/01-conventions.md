# 01 — Conventions (mandatory)

New code must be indistinguishable from existing DotPulsar code. These rules come from `.editorconfig`,
`DotPulsar.sln.DotSettings` (the "DotPulsar: Full Cleanup" profile), `CONTRIBUTING.md`, `docs/dev/coding-style.md`,
and the code itself at baseline `e8f2ba2`. When in doubt, copy the nearest existing analogue (named below).

## 1. File layout

Every `.cs` file starts with the license header below. Copy it byte-for-byte; never alter it, as CONTRIBUTING forbids
changing license headers. After the header comes one blank line, then the file-scoped namespace, one blank line,
the `using` directives **inside** the namespace, one blank line, then the type.

```csharp
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

namespace DotPulsar.Internal.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionImpl.java
public sealed class Transaction : ITransaction
{
    ...
}
```

- `csharp_style_namespace_declarations = file_scoped:error`.
- `csharp_using_directive_placement = inside_namespace:error`.
- `dotnet_sort_system_directives_first = false`: usings are sorted alphabetically as one list, so `DotPulsar.*` comes
  before `Google.*`, which comes before `System.*`.
- `ImplicitUsings` is enabled. Do not add `using System;`, `System.Threading`, `System.Threading.Tasks`,
  `System.Collections.Generic`, `System.Linq`, `System.IO` or `System.Net.Http`.
- The `// Upstream:` comment goes directly above the type declaration; see [11-upstream-sync.md](11-upstream-sync.md).
  It is the only comment added for traceability. Method-level references use `// Upstream: Class#method`
  on the line above the method, and only for non-trivial ported methods.
- 4-space indentation, Allman braces (`csharp_new_line_before_open_brace = all`), final newline, no trailing whitespace.
- One top-level type per file. The file name equals the type name.

## 2. Folder and namespace placement

| Kind | Folder | Namespace | Precedent |
|---|---|---|---|
| Public value types, enums, options | `src/DotPulsar/` | `DotPulsar` | `MessageId.cs`, `ProducerState.cs`, `ConsumerOptions.cs` |
| Public interfaces | `src/DotPulsar/Abstractions/` | `DotPulsar.Abstractions` | `IProducer.cs` |
| Public exceptions | `src/DotPulsar/Exceptions/` | `DotPulsar.Exceptions` | `TransactionConflictException.cs` |
| Public extension methods | `src/DotPulsar/Extensions/` | `DotPulsar.Extensions` | `ConsumerExtensions.cs` |
| Internal transaction implementation | `src/DotPulsar/Internal/Transactions/` (**new**) | `DotPulsar.Internal.Transactions` | `Internal/Compression/` |
| Internal interfaces | `src/DotPulsar/Internal/Abstractions/` | `DotPulsar.Internal.Abstractions` | `ICompressorFactory.cs` |
| Internal events | `src/DotPulsar/Internal/Events/` | `DotPulsar.Internal.Events` | `ProducerCreated.cs` |
| Internal exceptions | `src/DotPulsar/Internal/Exceptions/` | `DotPulsar.Internal.Exceptions` | `ChannelNotReadyException.cs` |
| Prerequisite-feature internals (batching, trackers, crypto) | `src/DotPulsar/Internal/` (flat), or a new subfolder when there are more than 3 files (`Internal/Batching/`, `Internal/Crypto/`) | matching | `Internal/Compression/` |

**Visibility.** Internal implementation types are declared `public sealed class` in the `DotPulsar.Internal.*`
namespaces. The repo has no `InternalsVisibleTo`, so tests use these types directly. Use `internal` only where the
repo already does so for the analogous member, e.g. `PulsarClient`'s constructor or `MessageMetadata.Metadata`.

## 3. Type and member style

- Concrete classes are `sealed`; base classes are `abstract`. Value types are `readonly struct`, e.g. `TransactionId`.
- Private fields: `_camelCase`, `readonly` wherever possible, and **all** assigned in the constructor, including
  defaults (`_isDisposed = 0;`, `_disposables = [];`).
- Do not use primary constructors: `IDE0290` is silenced and the code does not use them.
- Use `var` everywhere.
- Expression-bodied properties, indexers and accessors are preferred. For one-line methods, ReSharper's cleanup
  profile prefers expression bodies with `=>` on the **next** line:

  ```csharp
  public ValueTask Abort(CancellationToken cancellationToken)
      => _transaction.Abort(cancellationToken);
  ```
- Braces are optional for single-statement `if`s (`if (x) return;` is common).
- Write casts with a space: `(uint) value` (`csharp_space_after_cast = true`).
- Parenthesise mixed binary operators for clarity.
- Locks: `private readonly object _lock = new();` with `lock (_lock) { ... }`. Prefer Interlocked/CAS for state
  that Java guards with `Atomic*FieldUpdater`.
- Disposal guard pattern (copy from `Producer.cs` / `Connection.cs`):

  ```csharp
  private int _isDisposed;

  public async ValueTask DisposeAsync()
  {
      if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
          return;
      ...
  }

  private void ThrowIfDisposed()
  {
      if (_isDisposed != 0)
          throw new TransactionCoordinatorClientDisposedException();
  }
  ```
- Collection expressions (`[]`) are fine. Avoid LINQ on hot paths; LINQ in setup or dispose code is fine.
- Never use `async void`. Never block (`.Result`/`.Wait()`) except where existing code already does so for a
  completed task (`responseTask.Result` after `WhenAny`).

## 4. Async rules

- Public API returns `ValueTask` / `ValueTask<T>`. Connection-level internals return `Task` / `Task<BaseCommand>`,
  following `IConnection`.
- **Every** `await` uses `.ConfigureAwait(false)`, including `await foreach (... .ConfigureAwait(false))` and
  `await using`.
- `CancellationToken cancellationToken` is always the last parameter. It has `= default` on public interfaces and
  extension methods; internal implementations usually omit the default.
- `TaskCompletionSource` is always created with `TaskCreationOptions.RunContinuationsAsynchronously` (see `Awaiter`).
- Target-specific code:

  ```csharp
  #if NET6_0_OR_GREATER
          return ValueTask.CompletedTask;
  #else
          return new ValueTask();
  #endif
  ```
  Use `#if NETSTANDARD2_0` / `#if NETSTANDARD2_0 || NETSTANDARD2_1` / `#if NET6_0_OR_GREATER` exactly as the
  existing code does. The targets are `netstandard2.0; netstandard2.1; net8.0; net9.0; net10.0`.

## 5. XML documentation (public API only)

Internal types have **no** XML docs. Public members always have a `<summary>`, usually one sentence. State the
default in the summary ("The default is 'false'.", "The default is 60 seconds.") and say whether a value is required
("This is required." / "This is optional."). Use `<remarks>` / `<returns>` only where the existing analogue does.
Templates:

```csharp
/// <summary>
/// Set the transaction timeout. The default is 60 seconds.
/// </summary>
ITransactionBuilder Timeout(TimeSpan timeout);
```

```csharp
/// <summary>
/// The default transaction timeout.
/// </summary>
public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);
```

Exception summaries are short noun phrases, e.g. `/// Ack with transaction conflict`.

## 6. Exceptions

All public exceptions are `public sealed class XException : DotPulsarException`, with exactly these two constructors
(copy `Exceptions/TransactionConflictException.cs`):

```csharp
/// <summary>
/// Transaction not found
/// </summary>
public sealed class TransactionNotFoundException : DotPulsarException
{
    public TransactionNotFoundException(string message) : base(message) { }

    public TransactionNotFoundException(string message, Exception innerException) : base(message, innerException) { }
}
```

Server errors are mapped to exceptions only in `Internal/Extensions/CommandExtensions.cs` (`Throw(ServerError, string)`).
The retry/rethrow policy is set only in `Internal/DefaultExceptionHandler.cs`.

## 7. Options and builders

- Options classes are `public sealed`. The constructor takes the required arguments. There is a
  `public static readonly DefaultX` field per optional setting, and `{ get; set; }` properties documented with
  their defaults.
- Fluent builders live in `Internal/` (`ProducerBuilder<T>`, `ConsumerBuilder<T>`, `PulsarClientBuilder`). Each has a
  private field per setting, sets the defaults in its constructor, and every setter `return this;`. Validation happens
  in `Create()`/`Build()` and throws `ConfigurationException("<Options>.<Property> may not be ...")`.
- Add each new builder method to the interface (`Abstractions/I*Builder.cs`), to the implementation, and to the
  options class if there is one. The original members are alphabetical, but every later addition was **appended at
  the end** (`MessageRouter`, `MaxPendingMessages`, `ProducerProperty` in `IProducerBuilder`; `AllowOutOfOrderDeliver`
  in `IConsumerBuilder`; `CloseInactiveConnectionsInterval` in `IPulsarClientBuilder`). Do the same: append new
  members at the end, in the order of the work packages that introduce them.

## 8. Tests

- Single project: `tests/DotPulsar.Tests` (net10.0). Libraries: xUnit v3, NSubstitute, Shouldly, AutoFixture
  (`[AutoData]`, `[InlineAutoData]` wrappers in the test project), Testcontainers.Pulsar, ToxiproxyNetCore.
  The global usings already include `NSubstitute`, `Shouldly`, `Xunit` and `static Xunit.TestContext`.
- Unit tests: `[Trait("Category", "Unit")]` on the class, and the file path mirrors `src`
  (`tests/DotPulsar.Tests/Internal/Transactions/TransactionTests.cs`). Use `Current.CancellationToken`.
- Integration tests: `[Collection("Transaction"), Trait("Category", "Integration")]`, with fixture injection through
  the constructor. Use a per-class `CancellationTokenSource _cts` and dispose it in `Dispose()`.
  Copy the shape of `tests/DotPulsar.Tests/Internal/ProducerTests.cs`.
- Test method names: `Method_GivenCondition_ShouldOutcome` (e.g. `Commit_GivenOpenTransaction_ShouldMakeMessagesVisible`).
- Body: `//Arrange`, `//Act`, `//Assert` comments, with no space after `//`; this is the dominant style. Name the
  system under test `uut` in unit tests.
- Assertions: Shouldly (`x.ShouldBe(y)`, `await Should.ThrowAsync<T>(...)`, `x.ShouldBeNull()`).
- Mocks: `Substitute.For<IConnection>()`; check calls with `.Received(1).Send(Arg.Is<CommandAck>(...), Arg.Any<CancellationToken>())`.
- Ported upstream tests carry `// Upstream: <JavaClass>#<javaMethod>` above the `[Fact]`/`[Theory]` attribute.
  A Java `@DataProvider` becomes a `[Theory]` with one `[InlineData]` per provider row, in the same order and with
  the same values.
- Unique resources: topics come from the fixture (`CreateTopic`, `CreatePartitionedTopic`), and subscriptions use
  `$"subscription-{Guid.NewGuid():N}"`. When upstream uses a literal subscription name such as `"test"`, keep the
  literal: each test uses its own unique topic, so a literal subscription name cannot collide.
- Log state changes and exceptions through `TestOutputHelperExtensions.Log` (`.StateChangedHandler(_testOutputHelper.Log)`,
  `.ExceptionHandler(_testOutputHelper.Log)`).

## 9. Benchmarks

The project lives at `benchmarks/<Name>/<Name>.csproj` and targets net10.0, using BenchmarkDotNet. Copy
`benchmarks/Compression` (its `Program.cs` uses `BenchmarkSwitcher`). Use `[MemoryDiagnoser]` on every benchmark class.

## 10. Commits, PRs, CHANGELOG, docs

- Commit message: a summary of at most 50 characters, a blank line, then a body wrapped at 72 characters, then
  `Fixes #<issue>` when it applies. The maintainer squash-merges PRs with `(#NNN)` appended to the subject.
- PR description follows `.github/pull_request_template.md`: `Fixes #...`, then the sections `# Description`,
  `# Regression` and `# Testing`.
- CONTRIBUTING asks for an issue before a large PR. The implementation plan ([10](10-implementation-plan.md)) uses one
  tracking issue plus one PR per work package.
- `CHANGELOG.md` follows Keep-a-Changelog 1.1.0. Add an `## [Unreleased]` section at the top if none exists, with
  `### Added` / `### Changed` / `### Fixed` / `### Removed` subsections. Entries are short sentences without a final
  period, for example:
  - `- Support for transactions (produce and acknowledge) using IPulsarClient.NewTransaction()`
  - `- Updated the protocol version from 14 to 19`
- `README.md` "Supported features" checklist and `docs/api/index.md` are updated in WP-DOCS.
- Do not hand-edit generated proto code. `PulsarApi.proto` is compiled by `Grpc.Tools` at build time.
