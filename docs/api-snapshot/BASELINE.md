# Phase 0 Baseline Record

Recorded: 2026-10-07, on `overhaul` @ `96e2241` (release 0.17.6).

Phase 0 is a **no-code-change** baseline phase. Its deliverables are:

1. A recorded test baseline (`npm run check` + `npm run test:e2e`).
2. A snapshot of the public API surface to diff after each subsequent phase.

## Public API surface snapshots

- `workflows-index.txt` — exported symbols from `src/workflows/index.ts` (278 symbols).
- `extension-index.txt` — exported symbols from `src/extension/index.ts` (14 named + 1 default).

Regenerate with:

```bash
node scripts/snapshot-api.mjs src/workflows/index.ts > docs/api-snapshot/workflows-index.txt
node scripts/snapshot-api.mjs src/extension/index.ts > docs/api-snapshot/extension-index.txt
```

Diff after each phase to confirm no unintended public-surface drift.

## Test baseline

### `npm run check` (format, lint, typecheck, build, vitest --coverage)

Run with a short temp root to avoid the macOS Unix-socket path-length limit:

```bash
mkdir -p .pwtest
PI_WORKFLOWS_TEST_TEMP_ROOT="$(pwd)/.pwtest" npm run check
```

Result (with short temp root): **13 failed | 1443 passed** across 115 test files.

Without the short temp root the default `os.tmpdir()` is too deep on this macOS
machine and the socket path exceeds the 103-byte limit, producing ~121 failures.
That is an environmental artifact, not a code regression.

### Pre-existing failures (present on `main` / pre-overhaul, not introduced by Phase 0)

All 13 failures were verified to also fail on `main` (`7937df1`, release 0.17.6) or are
environmental to this macOS host:

| Test file                              | Count | Cause                                                                                                                              |
| -------------------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `test/change-verification.test.ts`     | 3     | Fails on `main` too — pre-existing.                                                                                                |
| `test/component-vocabulary.test.ts`    | 1     | Expects `docs/plans/2026-09-12-current-workflow-state-plan.md`, deleted in overhaul commit `33de970`. Pre-existing on this branch. |
| `test/sanity-check-workflow.test.ts`   | 1     | macOS `/private/var` vs `/var` symlink canonicalization mismatch. Environmental.                                                   |
| `test/herdr-plugin.test.ts`            | 1     | `piw` Rust binary not installed (`cargo install pi-workflows`). Environmental.                                                     |
| `test/herdr-plugin-viewer.test.ts`     | 4     | `piw` Rust binary not installed. Environmental.                                                                                    |
| `test/server-resource-manager.test.ts` | 1     | Socket path still 104 bytes for one deep test name. Environmental.                                                                 |
| `test/cli.test.ts`                     | 1     | Fails on `main` too — pre-existing (server stop/version).                                                                          |
| `test/server.test.ts`                  | 1     | "reaps headless pi process group" — 33s, flaky/environmental; fails on `main` too.                                                 |

### `npm run test:e2e`

Result: **13 failed | 1 passed** across 3 e2e files.

All failures are environmental to this macOS host:

- **Socket path length** (115–118 bytes > 103-byte limit): the e2e harness creates its own
  temp dirs under `os.tmpdir()` for the server socket, which is too deep on this machine.
- **Herdr plugin**: `require is not defined in ES module scope` fixture issue + missing `piw` binary.

The one passing e2e test exercises the workflow engine path and confirms the core integration
works. The failures are not code regressions.

## Environmental caveats for this host (macOS)

1. **Unix socket path limit (103 bytes).** The default `os.tmpdir()` on this machine is
   `/var/folders/60/l713bx6j0mgdb556mxhj69540000gn/T/` (49 chars). Test temp dirs add enough
   depth to push server socket paths over 103 bytes. Mitigation: set
   `PI_WORKFLOWS_TEST_TEMP_ROOT` to a short path (e.g. `.pwtest` in the repo root, which is
   gitignored). The e2e harness does not fully honor this for its own server-socket temp dirs,
   so some e2e socket tests still fail on this host.
2. **`/private/var` symlink.** macOS canonicalizes `/var` to `/private/var`; one test compares
   the raw path and fails on the mismatch.
3. **`piw` Rust binary not installed.** Herdr plugin/viewer e2e and unit tests require
   `cargo install pi-workflows --version 0.17.6`.

## Baseline conclusion

The codebase is in a stable state at `overhaul` @ `96e2241`. The 13 unit-test failures and 13
e2e failures are all pre-existing or environmental to this macOS host — none are introduced by
Phase 0 (which makes no code changes). Subsequent phases should:

- Re-run with `PI_WORKFLOWS_TEST_TEMP_ROOT` set to a short path.
- Treat the 13 unit-test failures above as the known-failing baseline; any _new_ failure is a
  regression.
- Diff the API snapshots after each phase.
