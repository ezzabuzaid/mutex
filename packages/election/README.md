# @zukhruf/election

Candidates campaign for one claim, and the candidate that wins it leads for one term. Each term has an epoch that is higher than the epoch of each earlier term, so a newer leader always outranks an older one. A term is a lease on leadership. It ends when the leader resigns or its process dies. With a backend that can take the claim away, a term can also be lost while the leader still runs, and the signal of the term tells its leader so.

The words in these documents have one meaning each. See the glossary in [CONTEXT.md](./CONTEXT.md).

## Use it

This project is an experiment.

```sh
npm install @zukhruf/election
```

## Elect a leader of one host

`SqliteElection` elects one leader among the processes of one host that use the same directory. The claim is the `FileLock` of `@zukhruf/fs` on `claimFile`: an exclusive SQLite transaction. The operating system holds that lock until the leader's process dies, so a dead leader frees the claim at once, and a living leader never loses it.

```ts
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { SqliteElection } from '@zukhruf/election';

const directory = await mkdtemp(join(tmpdir(), 'jobs-'));
const election = new SqliteElection({
  directory,
  claimFile: 'jobs.lock',
  epochFile: 'jobs.epoch',
});

const first = await election.campaign();
console.log('first term:', first?.epoch);
console.log('a second candidate wins:', await election.campaign());

await first?.resign();
await using second = await election.campaign({ timeout: 1000 });
console.log('next term:', second?.epoch);
await rm(directory, { recursive: true, force: true });
```

Output:

```
first term: 1n
a second candidate wins: undefined
next term: 2n
```

- `campaign({ timeout, signal })` resolves with a `Term`, or with `undefined` when another candidate still leads after `timeout` milliseconds. Without a `timeout`, it tries once. When `signal` aborts, it rejects with `signal.reason`, and a claim that it won meanwhile is given up.
- `claimFile` and `epochFile` are files in `directory`. Candidates with the same directory and the same file names are one group. Never delete `claimFile` while candidates run: a new file lets a second leader win.
- A term that resigns clean writes a third file, `<claimFile>.clean`, beside `claimFile`. It holds the epoch of that term. The next term reads it and removes it.
- The directory must be on a local file system. On a network file system, `campaign` rejects with `NetworkDirectoryError`.
- `pollInterval` is the time between two tries, in milliseconds. It defaults to 10. A try never blocks the process.

## The term

A term is a lease on leadership. `Term` implements the `Lease` interface of [`@zukhruf/lease`](../lease/README.md), so a function that takes a `Lease` also takes a term. See the glossary of [leases](../lease/CONTEXT.md).

- `term.epoch` is the number of the term. Each epoch is below 2^31.
- `term.resign()` ends the term and gives the claim up. A second call waits for the first, and its options do nothing. `await using` resigns at the end of its scope.
- `term.resign({ clean: true })` records a clean shutdown for the next term, and then ends the term. Use it only when the leader leaves nothing for the next term to recover, for example no work in progress. A lost term records nothing, also with `clean`. When the record fails, the claim is freed, and `resign` rejects with that error.
- `term.afterCleanShutdown` is `true` only when the term just before this one resigned clean. Then the new leader knows that it has nothing to recover. It is `false` after a crash, after a lost term, after a resign without `clean`, and for the first term.
- `term.signal` aborts with `LeaseLostError` when the term is lost while its leader still runs. The `subject` of the error is `'leadership'`, and its `cause` is the reason of the backend. The signal does not abort when the leader resigns.

A leader that acts for the group must stop when `term.signal` aborts, because another candidate can lead then. `SqliteElection` never loses a living term, so its signal never aborts. A backend that can take the claim away can lose one.

A term has no fencing token. The signal warns the leader, but it does not protect a resource: a leader can write after another candidate won. To protect a resource, make fencing tokens from the epoch with `EpochTokenSource` of [`@zukhruf/fencing`](../fencing/README.md). Then a resource can refuse the writes of an older leader:

```ts
import { EpochTokenSource } from '@zukhruf/fencing';

const tokens = new EpochTokenSource(term.epoch);
const token = await tokens.next('orders');
```

[ADR 0002](./docs/adr/0002-a-term-is-a-lease-on-leadership-with-no-fencing.md) tells why.

## Write a backend

`LeaderElection` is an abstract class. It runs the campaign, and it owns the term. A backend is a subclass that implements six steps for its claim:

| Step                                | What it does                                                                                                                                          |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `open(signal)`                      | Prepares one attempt, for example a connection. The campaign calls `tryClaim` on it again and again.                                                  |
| `tryClaim(claim, signal)`           | Tries the claim once. Resolves with `{ epoch, afterCleanShutdown }` for the term it won, or with `undefined` while another candidate holds the claim. |
| `watch(claim, lose)`                | Watches a won claim for as long as its term lasts. Calls `lose` when the backend takes the claim away. Returns a `Disposable` that stops the watch.   |
| `recordCleanShutdown(claim, epoch)` | Records that the term of `epoch` resigned clean. The campaign calls it before `release`, while the term still holds the claim.                        |
| `release(claim)`                    | Gives a won claim up when the leader resigns.                                                                                                         |
| `close(claim)`                      | Frees the resources of an attempt, at each end.                                                                                                       |

A backend must keep five rules:

1. `tryClaim` gives the epoch in the same step that wins the claim. Each epoch is higher than each earlier one, and below 2^31.
2. `tryClaim` gives `afterCleanShutdown: true` only when the term just before this one recorded a clean shutdown. It reads each record one time only. Thus a crash after a clean term never reads as clean.
3. `watch` calls `lose` before the backend can give the claim to another candidate. For a claim that the backend keeps only for a time, renew it well before that time ends, and count the time on a monotonic clock. Then an old leader stops before a new one starts.
4. The campaign never calls `recordCleanShutdown` or `release` after a loss, because the claim can be another candidate's then.
5. The campaign calls `close` after each end: a won term, a lost term, a failed attempt, or an attempt that never won.

This backend elects among the candidates of one process. Its claim is an entry in a map:

```ts
import { LeaderElection } from '@zukhruf/election';

const holders = new Map<string, bigint>();
const cleanShutdowns = new Set<string>();
let lastEpoch = 0n;

class MapElection extends LeaderElection<string> {
  readonly #name: string;

  constructor(name: string) {
    super(10);
    this.#name = name;
  }

  protected async open() {
    return this.#name;
  }

  protected async tryClaim(name: string) {
    if (holders.has(name)) return undefined;
    // Only the holder records, and each claim removes the record. Thus a
    // record is always from the term just before this one.
    const afterCleanShutdown = cleanShutdowns.delete(name);
    lastEpoch += 1n;
    holders.set(name, lastEpoch);
    return { epoch: lastEpoch, afterCleanShutdown };
  }

  // Nothing takes an entry of the map away, so there is nothing to watch.
  protected watch() {
    return { [Symbol.dispose]() {} };
  }

  protected async recordCleanShutdown(name: string) {
    cleanShutdowns.add(name);
  }

  protected async release(name: string) {
    holders.delete(name);
  }

  protected async close() {}
}

const term = await new MapElection('jobs').campaign();
console.log('leader:', term?.epoch);
console.log('rival:', await new MapElection('jobs').campaign());
await term?.resign({ clean: true });
const next = await new MapElection('jobs').campaign();
console.log('rival after the resign:', next?.epoch);
console.log('after a clean shutdown:', next?.afterCleanShutdown);
```

Output:

```
leader: 1n
rival: undefined
rival after the resign: 2n
after a clean shutdown: true
```

The socket lock store of `@zukhruf/mutex` uses `SqliteElection`, with the claim file `leader.lock` and the epoch file `leader.epoch`. Its leader serves the other candidates over a socket in the directory, so it works on one host only, also with another backend. Its server stops when `term.signal` aborts. `@zukhruf/single-flight` uses `SqliteElection` the same way, with the claim file `flight.lock` and the epoch file `flight.epoch`, so the two packages never join one election in a shared directory.

## Errors

| Error                   | When                                                                                                                                                                                                              |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NetworkDirectoryError` | The directory of a `SqliteElection` is on a network file system. It is the class of `@zukhruf/fs`.                                                                                                                |
| `LeaseLostError`        | The reason of `term.signal` when the backend took the claim away. Its `subject` is `'leadership'`, and its `cause` is the backend's reason. It is the class of `@zukhruf/lease`, and this package exports it too. |
| `signal.reason`         | A campaign rejects with it when its signal aborts.                                                                                                                                                                |

## Documentation

- [How SqliteElection elects a leader](./docs/concepts/leader-election.md)
- [Recipe: Run a job in only one process](./docs/recipes/singleton-job-with-leader-election.md)
- [ADR 0001: Leader election is a package, and each backend is a subclass of one campaign](./docs/adr/0001-leader-election-is-a-package-and-each-backend-is-a-subclass.md)
- [ADR 0002: A term is a lease on leadership, with no fencing](./docs/adr/0002-a-term-is-a-lease-on-leadership-with-no-fencing.md)
- [ADR 0003: A term that shuts down cleanly tells its successor](./docs/adr/0003-a-term-that-shuts-down-cleanly-tells-its-successor.md)
- [Code copied into this package](./docs/copied-code.md)

## Development

This package is part of the [zukhruf](../../README.md) workspace. Run the commands from the workspace root.

```sh
npm install
npx nx run election:test        # builds, then runs the tests in src/
npx nx run election:typecheck   # formats, lints, then type checks
npx nx run election:build       # compiles src/ to dist/
```

```
src/
  leader-election.ts     LeaderElection: the campaign, and the steps of a backend
  term.ts                Term: the epoch, the lease on leadership, and resign
  sqlite/                SqliteElection: the backend of one host
  testing/               the helpers that start candidate processes in the tests
```
