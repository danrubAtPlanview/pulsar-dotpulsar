# Follow-up 0001 — `MessageProcessorTests` hangs when the test project is run in Release

| | |
|---|---|
| **Status** | **Fixed in WP10**, in its own commit (`Do not spin in the MessageProcessor tests`), so that it can be taken out of the branch and sent as a separate change. |
| **Found by** | WP1, while running the unit suite with `-c Release` |
| **Related to transactions?** | No. The failure exists on untouched `master`. It is recorded here because the transaction work packages add many tests and benchmarks that people will run in Release (`benchmarks/Transactions` is Release-only), and a hung test host blocks the whole run. |
| **Severity** | Low for CI (CI runs Debug). Medium for contributors who run `dotnet test -c Release`. |

## 1. Symptom

`dotnet test tests/DotPulsar.Tests/DotPulsar.Tests.csproj -c Release --filter "Category=Unit"` never finishes. After roughly
320–390 tests have passed, the test host stops making progress at one test of `MessageProcessorTests`:

```
The test running when the crash occurred:
DotPulsar.Tests.Internal.MessageProcessorTests.Process_GivenNoShutdownGracePeriod_ShouldNotLetTaskComplete
```

With `--blame-hang-timeout 30s` the run is aborted ("The specified inactivity time of 30 seconds has elapsed"). Without
a hang timeout the run blocks forever, and the CPU stays busy because the test spins.

In Debug (`dotnet test` with no `-c`, which is what `.github/workflows/ci-unit.yaml` does) every test passes.

## 2. Evidence

| Check | Result |
|---|---|
| Untouched `master` (`e8f2ba2`), `-c Release`, `--filter "FullyQualifiedName~MessageProcessorTests"`, 3 runs | **Hangs in 3 of 3 runs.** Two of the four rows pass and the host then stalls. |
| Same commit, same command, with `_taskHasStarted` and `_taskHasCompleted` declared `volatile` (scratch worktree only) | **4 of 4 pass in 3 of 3 runs.** |
| `feature/transactions` tip, `-c Release`, all unit tests **except** `MessageProcessorTests` | **392 of 392 pass.** No other test is affected. |
| `feature/transactions` tip, `-c Release`, `MessageProcessorTests` | Hangs, as on `master`. |
| Debug, whole unit suite, both `master` and the branch | All pass. |

Reproduction recipe (needs the `--source` flag because of the private feed in the user-level NuGet config):

```bash
dotnet restore tests/DotPulsar.Tests/DotPulsar.Tests.csproj --source https://api.nuget.org/v3/index.json
dotnet build   tests/DotPulsar.Tests/DotPulsar.Tests.csproj -c Release --no-restore
dotnet test    tests/DotPulsar.Tests/DotPulsar.Tests.csproj -c Release --no-build \
               --filter "FullyQualifiedName~MessageProcessorTests" --blame-hang-timeout 30s
```

## 3. Root cause

`tests/DotPulsar.Tests/Internal/MessageProcessorTests.cs` synchronises two threads with plain fields and an empty spin loop:

```csharp
private bool _taskHasStarted;            // written by the processing task, read by the test thread
private bool _taskHasCompleted;

var processTask = uut.Process(_cts.Token).AsTask();
while (!_taskHasStarted) { }             // L68 (…ShouldNotLetTaskComplete) and L90 (…ShouldLetTaskComplete)
```

`_taskHasStarted` is neither `volatile` nor read through any memory barrier. In an optimised (Release) build the JIT is
allowed to hoist the field read out of the empty loop, so the loop becomes `if (!_taskHasStarted) while (true) { }`. The
write made on the thread-pool thread is never observed, and the test thread spins forever. In Debug the JIT does not
optimise, so the read stays in the loop and the test passes by luck.

The same pattern is in both tests that use `_taskHasStarted` (L68 and L90). The second one is exposed to the same hoisting,
and only happens not to hang first because the host stalls at the earlier row.

Secondary smell, not a cause of the hang: in `Process_GivenNoShutdownGracePeriod_ShouldNotLetTaskComplete` the test awaits
`processTask` before it calls `_semaphore.Release()`, whereas the grace-period variant releases first. The ordering is
intentional (the first test asserts the task is *not* allowed to finish), but it is the kind of thing a signal-based rewrite
should make explicit.

## 4. Proposed fix (to review after parity)

Smallest change, enough to remove the hang (verified on the baseline):

```csharp
private volatile bool _taskHasStarted;
private volatile bool _taskHasCompleted;
```

Preferred change, which also removes the busy loop and the CPU burn: replace the flag and the spin with a signal.

```csharp
private readonly TaskCompletionSource _taskHasStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
// ProcessMessage:  _taskHasStarted.TrySetResult();
// Act:             await _taskHasStarted.Task.WaitAsync(Current.CancellationToken);   // net10.0 test project, WaitAsync exists
```

Either way, apply it to **both** tests (L68 and L90) and re-run the verification checklist below. `_taskHasCompleted` is read only after
`await processTask`, so it is safe through the await's barrier, but making it `volatile` costs nothing.

## 5. Why it was not fixed now

- It is a pre-existing defect in a test unrelated to transactions. Changing it inside a transaction work package would add
  unrelated changes to a PR that maintainers will review for the transaction feature.
- It was requested to document it and review it after parity, so that the review can decide whether to send it upstream as a
  separate small PR (recommended) or fold it into WP10.
- Working around it locally is easy: `--filter "FullyQualifiedName!~MessageProcessorTests"`, or run Debug.

## 6. Verification checklist for whoever picks this up

1. Reproduce on the target commit: `-c Release`, `MessageProcessorTests` only, with `--blame-hang-timeout 30s`; expect the hang.
2. Apply the fix to both spin loops.
3. Run the same command at least 20 times in Release (`for i in $(seq 20); do dotnet test … || break; done`); expect 4 of 4 every time.
4. Run the whole unit suite in Release and in Debug; expect no hang and no regression.
5. Run the transaction suites in Release as well (`--filter "FullyQualifiedName~DotPulsar.Tests.Transactions"`, needs Docker)
   and record any other Release-only behaviour here. Release-mode results for the **new** transaction tests were not checked
   during WP1; the integration tests in later work packages were written and run in Debug.
6. If the maintainers prefer, add `-c Release` to `.github/workflows/ci-unit.yaml` after the fix so the problem cannot return.

## Fix (WP10)

Both tests wait for a `TaskCompletionSource` (`_taskHasStarted`) instead of spinning on a field, and `_taskHasCompleted` is `volatile`. Verification on the final tree: `MessageProcessorTests` alone in Release (`--blame-hang-timeout 30s`) passed 3 of 3 runs (4 tests, about 1 s), and the whole unit suite runs in Release with nothing excluded: 2160 passed on 3 of 3 runs. The filter `FullyQualifiedName!~MessageProcessorTests` is no longer needed.
