# Deletion plan: what can go right now, and what falls out later

Companion to `PLAN.md`. "Right now" means: delete these and `npm run typecheck && npm run build
&& npm run test` stays green with **no other changes**. Every entry below was verified against the
current tree (import graph over `src/`, plus reference greps across `src`, `test`, `scripts`,
`package.json`, tsconfig, and vitest configs).

## Delete now — zero references

| Target                                                                                                          | Size    | Evidence                                                                                                                                                                                                                                  |
| --------------------------------------------------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/client/activity.ts`                                                                                        | 7 ln    | Two constants (`ORIGIN_ACTIVITY_REFRESH_MS`, `ORIGIN_ACTIVITY_LEASE_MS`) imported by nothing in `src/` or `test/`                                                                                                                         |
| `src/state/index.ts`                                                                                            | 44 ln   | Barrel file; zero importers anywhere, including same-dir `./index.js` imports                                                                                                                                                             |
| `schemas/` (10 JSON files)                                                                                      | —       | No reference in `src`, `test`, `scripts`, `package.json`, tsconfig, or vitest configs. Only listed in `package.json` `files`, which tolerates a missing dir                                                                               |
| `scripts/generate-viewer-benchmark.ts`                                                                          | —       | Referenced only by docs prose (`docs/DEVELOPMENT.md`); in no npm script                                                                                                                                                                   |
| `scripts/snapshot-api.mjs` + `docs/api-snapshot/` (`BASELINE.md`, `extension-index.txt`, `workflows-index.txt`) | 4 files | The API-snapshot baseline ritual; cross-referenced only by each other and docs. In no npm script                                                                                                                                          |
| ~~`scripts/live-e2e.d.mts`~~                                                                                    | —       | **Not deletable** — it is the type declaration for `scripts/live-e2e.mjs`, which `test/live-e2e-script.test.ts` imports; deleting it breaks typecheck. The original "zero references" check missed declaration-file resolution. Restored. |
| `check-baseline.log`, `e2e-baseline.log` (repo root)                                                            | —       | Mentioned only in `PLAN.md`                                                                                                                                                                                                               |

## Delete now — clean pairs (file + its only references)

| Target                                                                                                                                                                                 | Size                           | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/extension/session-delivery.ts` **and** `test/session-delivery.test.ts`                                                                                                            | 175 ln + test                  | Dead in production code: zero `src/` importers (not even `extension/index.ts`). Only its own test references it. Delete both together; suite stays green. `PLAN.md` lists it under "trim to what the worker path needs" — it needs deletion, not trimming                                                                                                                                                                                     |
| `skills/{autodoc,autoimplement,autoplan,monitor,sanity-check}` **and** `test/bundled-skills.test.ts`, plus the one `skills/` guard assertion in `test/component-vocabulary.test.ts:90` | 5 skill dirs + 1 test + 1 line | No runtime code references the dir (`pi-agent-group.ts:1003` is a `skills: []` config field, not a path). Only `bundled-skills.test.ts` reads it and `component-vocabulary.test.ts:90` asserts its presence — that test is a repo-wide retired-term linter whose other assertions stay, so delete line 90, not the file. Product note: these skills launch built-ins that Phase 3 removes anyway; taking them now is consistent, just earlier |

**Total immediately deletable:** ~226 lines of `src/`, 5 skill dirs, `schemas/`, 2 scripts,
1 test file + 2 assertion lines, the api-snapshot docs, and 2 baseline logs. No changes to any
other file required.

**Known pre-existing failures (not caused by these deletions — verified identical on the clean
tree):** `test/component-vocabulary.test.ts` fails 3 cases because untracked `.pwtest/` Playwright
artifacts in the working tree match its retired-`host`-term sweep, and `docs/plans/` no longer
exists (the dated-plan guard was removed above as part of this batch). The full suite also has
~17 failing files on the clean tree; most are Phase 3/5 casualties per `PLAN.md`.

## Second batch — server-subsystem tests (pulled forward from Phase 3)

Deleted on this branch by explicit decision: 15 test files (~11,000 lines) that test
`src/server/` and server-only code (`queue.ts`). The server code itself stays until Phase 3;
`test/e2e/` keeps exercising the full stack through the real pi runtime in the meantime.

- `server.test.ts` (4,069), `server-view.test.ts` (3,156), `run-queue.test.ts` (862),
  `server-protocol-state.test.ts` (407), `server-lock.test.ts` (386),
  `server-resource-manager.test.ts` (298), `server-scheduler.test.ts` (275),
  `workflow-recovery.test.ts` (275), `rpc-executor-flow.test.ts` (253),
  `server-restart.test.ts` (238), `resource-manager-pull-request.test.ts` (195),
  `server-resource-lifecycle.test.ts` (178), `workflow-runner-content.test.ts` (177),
  `rpc-executor.test.ts` (150), `workflow-runner-entry.test.ts` (116)

Kept despite the name: `temp-workflow-servers.test.ts` — it exports `stopTempWorkflowServers`,
imported by `test/global-setup.ts`; deleting it would break every test. Also kept for now:
client/viewer/herdr/channels/resource-manager tests (Phase 3) and `test/e2e/` (the integration
net until the worker path exists; replaced in Phase 5).

Companion minimal fix: `.pwtest` added to `SKIP_DIRECTORY` in `test/component-vocabulary.test.ts`
— untracked Playwright artifacts in the working tree tripped its retired-term sweep (pre-existing
local failure, now robust against them).

## Third batch — builtins and client tests (pulled forward from Phase 3)

Deleted by the same explicit decision: 20 test files (~11,500 lines) that test `src/builtins/`
and `src/client/`, both removed in Phase 3.

**Builtins (16 files, ~9,300 lines):** `builtin-autoimplement` (2,379), `pi-agent-group` (1,455),
`change-verification` (1,150), `monitor-workflow` (674), `autoimplement-plan-discovery` (393),
`workspace-preparation` (424), `monitor-human-approval` (475), `monitor-repair` (417),
`builtin-autodoc` (339), `sanity-check-workflow` (313), `builtin-autoplan` (301),
`autoimplement-command-batches` (256), `plan-approval` (241), `plan-change` (183),
`catalog` (162, covers `workflows/catalog.ts` which goes in Phase 4), `builtin-plain-summary` (143).

**Client (4 files, ~2,200 lines):** `client.test.ts` (1,405), `viewer-client.test.ts` (299,
viewer+client hybrid — everything it touches dies in Phase 3), `cli.test.ts` (232, the piw CLI),
`client-protocol.test.ts` (231).

**Skipped on purpose — they test surviving code or the e2e net, so they are not simple
deletions:**
- `loader.test.ts` (290) — tests the surviving `workflows/loader.ts`; builtins are only fixtures.
- `session-run-adapter.test.ts` (210) + `workflow-message-coordinator.test.ts` (988) — test
  surviving extension code but import `src/client/` (the coordinator test also imports
  `state/workflow-messages`, removed in Phase 4). They must be rewired to the worker adapter in
  Phase 2/3, not deleted.
- `e2e/package-resources.e2e.test.ts` (228) — part of the e2e net; goes with the client in
  Phase 3.

Test-file count after this batch: 118 → 81.

## Not deletable yet — what keeps each alive, and when it falls out

| Target                                           | Kept alive by                                                                                                                                                                                                                                                   | Falls out in                                                                                                    |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `src/viewer/render.ts` (362 ln)                  | Zero `src/` importers, but 3 test files import specific functions: `test/engine-more.test.ts` (`renderRunListLines`, `statusLabel`), `test/review-fixes.test.ts` (`renderRunDetailLines`), `test/render.test.ts` (several). Needs test surgery, not a clean cut | Phase 3, with `src/viewer/`                                                                                     |
| `protocol/` (`client.v1.schema.json` + fixtures) | 3 test files: `test/client-protocol.test.ts`, `test/client-boundary.test.ts`, and `test/server-view.test.ts` (the last tests server code that stays until Phase 3 and needs `fixtures/run-view-controls-v1.json`)                                               | Phase 3, with `src/client/` + `src/server/`                                                                     |
| `fixtures/layout/`                               | `test/layout-fixtures.test.ts`, `test/graph.test.ts`, `test/helpers/layout-fixtures.ts`, `test/helpers/random-workflows.ts`, plus the npm `fixtures` script (`scripts/export-layout-fixtures.mjs`)                                                              | Phase 3, with `src/render/graph*.ts` (trivial pair for the generator: script + `package.json` `fixtures` entry) |
| `fixtures/session-events/`                       | `test/session-reducer.test.ts`, which also covers the still-live `src/workflows/session-reducer.ts` store path                                                                                                                                                  | Phase 4, with `session-reducer.ts` + the `session_entries` table                                                |
| `examples/`                                      | 16 test files load example workflows at runtime (incl. `sqlite-lifecycle`, `run-queue`, `store`, `extension-args`, `examples`, `server-protocol-state`, `run-fencing`, `human-decision-store`)                                                                  | Phase 5, with the test reduction (keep only the generic DSL examples per `PLAN.md`)                             |
| ~~`skills/` (5 built-in skills)~~                | Deleted in the "delete now" batch: no runtime reference; only `test/bundled-skills.test.ts` + one guard assertion in `test/component-vocabulary.test.ts` kept it alive                                                                                          | Done (pair), or could have waited for Phase 3 with `src/builtins/`                                              |
| `assets/cover.svg`                               | `README.md` image                                                                                                                                                                                                                                               | With the README rewrite (Phase 5)                                                                               |

## Verification command

After the "delete now" batch:

```bash
npm run typecheck && npm run build && npm run test
```

(No coverage gate — see `PLAN.md` Phase 0.)
