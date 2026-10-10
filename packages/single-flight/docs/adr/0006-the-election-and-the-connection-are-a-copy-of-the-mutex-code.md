# The election and the connection are a copy of the mutex code until a third package needs them

Superseded on 2026-10-10 for the election by [`@zukhruf/election`](../../../election/docs/adr/0001-leader-election-is-a-package-and-each-backend-is-a-subclass.md). The single flight uses `SqliteElection` with the files `flight.lock` and `flight.epoch`, and its copy of the election is deleted. The decision below stays true for the connection: it is a copy of the mutex code until a third package needs it.

A single flight needs what the socket lock store of `@zukhruf/mutex` already has: an election in a directory, a socket to the winner, a handshake, and a supervisor that connects again after a loss. This package is the second one that needs that code. This project follows the Rule of Three, as Fowler gives it in _Refactoring_: "The first time you do something, you just do it. The second time you do something similar, you wince at the duplication, but you do the duplicate thing anyway. The third time you do something similar, you refactor." Thus this package keeps its own copy, with its own file names (`flight.lock`, `flight.epoch`, `flight.sock`) and a hello that names its protocol. The ledger [copied-from-mutex.md](../copied-from-mutex.md) lists each copied file, what changed in it, and why. Two backlog items wait for the third package: #2496 for the election and the connection (`@zukhruf/coordinator`), and #2509 for the test helpers that start processes (`@zukhruf/testing`). Then the three copies give the boundary of each shared package.

## Considered Options

- **Use the classes of `@zukhruf/mutex`.** The mutex classes stay for locks only. Its public `LeaderElection` has fixed file names: a single flight and a socket lock store in one directory would share one election, and the loser would never find its server.
- **Extract a shared package now.** With two copies, its boundary is a guess. Sandi Metz: "duplication is far cheaper than the wrong abstraction".
- **Copy now, record the copy, and extract when a third package needs the code.** This option was selected.

## Consequences

- A fix in one copy goes into the other copy too, and the commit names both files.
- The wire of the mutex does not change, so old and new mutex processes never split into two leaders. Pin tests in the mutex guard its file names and bytes.
- A single flight and a socket lock store can use one directory, and they never meet.
- The file helpers and the check that refuses a network directory were copies too. They moved to `@zukhruf/fs` when it was extracted (its ADR 0001), so they are no longer copies.
