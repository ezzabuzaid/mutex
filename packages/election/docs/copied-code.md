# Code copied into this package

This package keeps a few small files that other packages also have. Each one is listed here with its source and what changed, so that a fix in one copy goes into each copy, and the commit names all of them.

| Copy                               | Source                                            | What changed                                                                            | Backlog |
| ---------------------------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------- | ------- |
| `src/testing/wait-until.ts`        | `packages/mutex/src/testing/wait-until.ts`        | Nothing.                                                                                | #2509   |
| `src/testing/scratch-directory.ts` | `packages/mutex/src/testing/scratch-directory.ts` | The folder name starts with `election-test-`.                                           | #2509   |
| `src/testing/worker-process.ts`    | `packages/mutex/src/testing/worker-process.ts`    | No `host` and no `nodeOptions` option. The message check is inline, with no `isRecord`. | #2509   |

The test helpers are in three packages now: the mutex, the single flight and this one. The maintainer chose a third copy for them, and backlog #2509 records it. The claim of `SqliteElection` is a `FileLock` of `@zukhruf/fs`, so this package keeps no copy of the SQLite result-code checks.

## The election is in one place

This package is the election of the mutex and the single flight, made into one campaign with a subclass for each backend. Both packages use it since 2026-10-10, and their copies are deleted. Thus the election has no copy now.

What changed from the copies that the mutex and the single flight had:

- The campaign is the abstract class `LeaderElection`. The SQLite claim is its subclass `SqliteElection`.
- The claim file and the epoch file are options (`claimFile`, `epochFile`). The mutex uses `leader.lock` and `leader.epoch`. The single flight uses `flight.lock` and `flight.epoch`.
- `Leadership` is `Term`. A term is a lease of `@zukhruf/lease`: its `signal` aborts with `LeaseLostError` when the backend takes the claim away. A second `resign` waits for the first.
- `campaign` takes a `signal`, and a claim won after it aborted is given up.
- The claim is a `FileLock` of `@zukhruf/fs`. Its journal is in memory, so a leader that dies leaves no `<claimFile>-journal` file. The locks are the same as with the default journal.
