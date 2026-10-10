# How SqliteElection elects a leader

`SqliteElection` elects one leader among the processes of one host that use the same directory. The socket lock store of `@zukhruf/mutex` uses it to select its coordinator. This page tells what an election must do, how `SqliteElection` does it, and the rules for its files.

## What an election must do

1. **Make one claim win.** Many candidates try at the same time. Exactly one must win.
2. **Find a leader that stopped.** Then another candidate can win.
3. **Never have two leaders.** Two leaders act for the group at the same time, for example two coordinators that grant the same key.

## How SqliteElection does it

- **The claim.** A candidate starts an exclusive SQLite transaction on `<directory>/<claimFile>`. Only one connection can have that transaction. The leader keeps it open for its full term.
- **A leader that stops.** The operating system kernel keeps the SQLite file lock. When the leader process stops, the kernel removes the lock, also after `SIGKILL`. Then the next campaign wins.
- **The epoch.** The winner reads `<directory>/<epochFile>`, adds 1, and writes it back. Only the winner can do this, so the epoch always increases. The write is on the disk before the campaign resolves, so a power loss cannot give the same epoch two times.
- **A clean shutdown.** A term that resigns clean writes its epoch to `<directory>/<claimFile>.clean` before it gives the claim up. The next winner reads that note and removes it. The note tells of a clean shutdown only when its epoch is the epoch just before the new one. The write is atomic but not synced to the disk: a lost note only makes the next term recover when it did not have to.
- **A campaign does not block.** A candidate tries once, waits `pollInterval`, and tries again until `timeout`. The process continues other work between the tries. (A SQLite busy timeout would stop the event loop of each candidate.)

```ts
import { SqliteElection } from '@zukhruf/election';

const election = new SqliteElection({
  directory: '/var/lib/my-app/election',
  claimFile: 'jobs.lock',
  epochFile: 'jobs.epoch',
});
await using term = await election.campaign({ timeout: 1000 });
if (term) {
  console.log(`I am the leader, epoch ${term.epoch}`);
}
```

## Rules

- **Do not delete the claim file** while candidates run. A new file has no lock on it, so a second candidate wins and two leaders exist.
- **Give each group its own claim file.** Two groups that use the same `claimFile` in one directory are one election: one leader for both. `SocketStore` of `@zukhruf/mutex` uses `leader.lock` and `leader.epoch`. Do not use these names in a directory of a `SocketStore`.
- **Stop when the term ends.** A leader that serves others stops serving before it resigns, and when `term.signal` aborts. `SqliteElection` never loses a living term, but another backend can.

## Evidence

All results are from tests on macOS with Node 26:

- **One try is not enough.** With one try and no wait, 9 of 20 races had no leader. With tries again, 60 of 60 races had exactly one leader.
- **A deleted claim file gives two leaders.** After the claim file was deleted, a second candidate won while the first leader was alive.
- **A stopped leader is replaced fast.** After `SIGKILL` of the leader, the next candidate became the leader in approximately 10 ms.

The tests in `src/sqlite/sqlite-election.test.ts` check that exactly one of four processes wins, that a new leader has a higher epoch, that a losing campaign does not block its process, that the epoch is on the disk before the campaign resolves, and that only the term just after a clean resign reads a clean shutdown.
