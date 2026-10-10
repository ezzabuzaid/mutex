# Code copied from the mutex

A single flight elects a coordinator among the processes that use one directory. Each caller then talks to that coordinator over a socket. The mutex's socket lock store already does both. This package keeps its own copy of that code, and does not share it with the mutex yet.

The reason is the Rule of Three. It counts places, not packages. Code that a second place needs is copied, and it is extracted only when a third place needs it. The boundary of a shared package then comes from three working copies, not from a guess. The mutex's wire also stays as it is, so old and new mutex processes cannot split into two leaders. When a third consumer needs this code, the shared parts move to `@zukhruf/coordinator` (backlog #2496). Until then, a fix in one copy is made in the other copy too, and the commit names both files. This note is the input for that step.

This note keeps three kinds of records. The first sections list the files that this package copied, and what differs in each. [Knowledge shared with the mutex](#knowledge-shared-with-the-mutex) lists the rules that the two packages each write in their own code. [In two places inside the single flight](#in-two-places-inside-the-single-flight) lists the code that this package repeats in itself, as the [mutex note](../../mutex/docs/copied-code.md) does for the mutex.

Paths on the left are in `packages/mutex/src`. Paths on the right are in `packages/single-flight/src`.

On 2026-10-10, the mutex changed several of these originals. The maintainer chose to change the mutex first, and this package later. Backlog #2541 lists each change and the mutex commit that this package copied. The same day, this package copied each change (commits `dab7a50` to `a402b02`). Thus the copies match the mutex again, except for the differences below.

## Moved to `@zukhruf/fs`

The file helpers and the check that refuses a network directory are no longer copies. Both packages use `@zukhruf/fs` (its ADR 0001): `durableWrite`, `isErrno`, `patiently`, `assertLocalDirectory`, and one `NetworkDirectoryError` class that this package gives from its entry point.

## Moved to `@zukhruf/election`

The election is no longer a copy. `@zukhruf/election` holds it, made into one campaign with a subclass for each backend ([its ledger](../../election/docs/copied-code.md)). The mutex uses it since 2026-10-10, and this package too, since the same day (backlog #2530). The copy `election/` of this package and its SQLite helper `shared/sqlite/is-busy.ts` are deleted. The claim on `flight.lock` is now a `FileLock` of `@zukhruf/fs`. Its journal is in memory, so a coordinator that stops leaves no `flight.lock-journal` file. A published version claims `flight.lock` with an exclusive SQLite transaction, and that transaction and the file lock exclude each other (`flight-protocol.test.ts` pins both directions).

## Copied without a change

| Mutex                                         | Single flight                         |
| --------------------------------------------- | ------------------------------------- |
| `shared/is-record.ts`                         | `shared/is-record.ts`                 |
| `lock-stores/socket/leave-errors-to-close.ts` | `connection/leave-errors-to-close.ts` |
| `lock-stores/socket/json-line.ts`             | `connection/json-line.ts`             |
| `lock-stores/remote/connection.ts`            | `connection/connection.ts`            |
| `lock-stores/remote/connection-supervisor.ts` | `connection/connection-supervisor.ts` |

The supervisor's `Unavailable` state cannot occur in this package: `ElectingConnector.connect` never gives `undefined`, and `FlightClient` has no `unavailable` listener. In the mutex, the IPC and thread connectors use it. The state stays, so the two copies stay the same until the extraction (backlog #2520).

## Copied with changes

**`lock-stores/socket/socket-election.ts` → `connection/flight-election.ts`.**
The claim file is `flight.lock`, not `leader.lock`. The epoch file is `flight.epoch`, not `leader.epoch`. The function is `flightElection`, not `socketElection`.
Why: a single flight and a socket lock store can use one directory. With the same file names, they would share one election. The winner would serve only one of them, and the other one would never find a server.

**`lock-stores/remote/connector.ts` → `connection/connector.ts`.**
The lock aliases `ClientConnection` and `ClientConnector` become `FlightConnection` and `FlightConnector`. The comment on `connect` does not name keys.

**`lock-stores/socket/socket-connection.ts` → `connection/socket-connection.ts`.**
Only the path of its import of `connection.ts` differs.

**`lock-stores/socket/local-directory-connector.ts` → `connection/local-directory-connector.ts`.**
Only its types and comments change.

**`lock-stores/socket/handshake.ts` → `connection/handshake.ts`.**

- The hello names the protocol: `{"op":"hello","protocol":"single-flight","version":1}`. The refusal names it too.
- The welcome is `{"op":"welcome"}`. It lists no requests, because this protocol has no request that came after version 1.
- A process that opens with something other than a hello is closed at once. The mutex keeps such a connection open, for its processes from before the handshake. No such process of this package exists.
- `FIRST_LINE_LIMIT` stays 1024 bytes.

Why: a hello that names its protocol cannot be read as a hello of another protocol.

**`lock-stores/socket/protocol-version-error.ts` → `connection/protocol-version-error.ts`.**
The messages name the single flight's coordinator, not the socket store's leader.

**`lock-stores/socket/electing-connector.ts` → `connection/electing-connector.ts`.**

- It has no `connected` callback. In the mutex, that callback only emits the `follower` role event. This package has no role event. Thus `#follow` needs no stack that destroys the socket when the callback throws.
- It does not wrap the connection in `AdvertisedOpsConnection`, because the welcome lists no requests.
- `#lead` is `#coordinate`. The comments do not name a process from before the handshake, because no such process of this package exists.

**`lock-stores/socket/socket-store.ts` (`socketPathFor`, `SOCKET_PATH_LIMIT`) → `connection/socket-path.ts`.**
The socket file is `flight.sock`. The Windows pipe is `\\.\pipe\single-flight-<hash>`. The 103-byte limit stays.
Note: on macOS 27 with Node.js 26, a probe listened and connected on socket paths of 104 to 111 bytes. The mutex's comment says that macOS stops at 104 bytes. The extraction must check that premise before it keeps the limit.

**`lock-stores/socket/lock-server.ts` → `coordinator/flight-server.ts`.**

- It serves a `FlightCoordinator`, not a `LockCoordinator`. `EpochTokenSource` comes from `@zukhruf/fencing`.
- `close` ends each connection with `destroySoon`, not `destroy`.

Why: a coordinator process can also lead a flight. When it lands the flight and stops at once, `destroy` can drop a `landed` answer that is still on its way to a joiner. `destroySoon` sends what was written first, and then closes.

As in the mutex, the server closes itself when its term is lost. A second `close` then does not wait for the connections of the first `close` (`c142572`, after the mutex's `5ac2a20`). Thus the disposal of a single flight can end before an old connection sent a `landed` answer. The election of a single flight never takes a term from a coordinator that runs, so this cannot occur with `SqliteElection`.

**`lock-stores/remote/protocol.ts` (`RequestEnvelope`, `isRequestEnvelope`) → `protocol/flight-protocol.ts`.**
They are copied as they are. The rest of that file is the lock protocol, and this package has its own. As the mutex's `isLockRequest` does, `isFlightRequest` checks only the fields of each request, because the session already ran `isRequestEnvelope` ([ADR 0008](./adr/0008-a-connection-closes-only-when-its-framing-breaks.md)). `isFlightResponse` checks the whole answer, as the mutex's `isLockResponse` does.

## Tests copied

Three test files test the copies above. Each one is a copy of a mutex test file, so a fix to a test goes into both files too.

**`lock-stores/socket/socket-connection.test.ts` → `connection/socket-connection.test.ts`.**
Only the import paths and the prefix of the Windows pipe differ.

**`lock-stores/socket/electing-connector.test.ts` → `connection/electing-connector.test.ts`.**
The tests use `flightElection` and `flight.sock`, and give no `connected` callback. The names say coordinator, not leader.

**`lock-stores/socket/lock-server.test.ts` → `coordinator/flight-server.test.ts`.**
A single flight has no role event. Thus the test reads `flight.epoch` to see that the next process coordinates, and it checks that a new lease token carries a higher epoch.

## Test helpers copied

The tests start real processes, so this package also copies three helpers from `packages/mutex/src/testing`. They are not part of the package: the build leaves `src/testing` out.

**`testing/wait-until.ts` → `testing/wait-until.ts`.**
Copied without a change.

**`testing/scratch-directory.ts` → `testing/scratch-directory.ts`.**
The prefix of the directory is `flights-`, not `mutex-test-`.

**`testing/worker-process.ts` → `testing/worker-process.ts`.**
`startWorker` has no options: no `host` (it joins a child to the mutex's IPC coordinator) and no `nodeOptions`. The worker has no `exit` and no `find`. These tests use none of them.

These helpers do not belong in `@zukhruf/coordinator`. A third package that copies them is the time to move them to `@zukhruf/testing` (backlog #2509).

## Not copied

- `lock-stores/socket/advertised-ops-connection.ts`: the welcome lists no requests yet.
- `lock-stores/remote/envelope.ts` and `queries.ts`: this package uses no IPC channel and no look requests.
- `lock-stores/remote/lock-coordinator.ts` and `remote-lock-client.ts`. `FlightCoordinator`, its sessions and `FlightClient` are this package's own code: one request leads or joins, and an outcome goes to every joiner. But they repeat rules of the mutex code. The next section lists them.

## Knowledge shared with the mutex

The rules below are not file copies. Each package writes them in its own code, so the files differ, but each rule is the same knowledge. While both places exist, a fix to one goes into the other, and the commit names both files. The extraction of backlog #2496 waits for a third consumer, and it compares these rules too.

**The rule for a lost lease.**

- Places: `#fly` of `SingleFlight` (`single-flight.ts`), and `#run` of `Mutex` (`mutex/mutex.ts`).
- Same: the work gets only the token and the signal of the lease. When the work throws after the loss, the call rejects with a new `LeaseLostError`, and the error of the work is its `cause`. When that error is the reason of the signal, the call rejects with it as it is. When the work gives a value after the loss, the call rejects with the reason of the signal.
- Differs: the single flight lands each failure before it decides, so its joiners get the failure. It lands no value after the loss, because its joiners were told that the flight is interrupted. `#fly` gives its result as a value and never rejects, so a caller that stopped waiting leaves no rejection behind. ([ADR 0007](./adr/0007-a-work-error-after-a-lost-lease-rejects-with-leaselosterror.md) tells why.)
- A third place moves this rule to `@zukhruf/lease`.

**The wiring of an electing client.**

- Places: the constructor and `[Symbol.asyncDispose]` of `SingleFlight` (`single-flight.ts`), and of `SocketStore` (`lock-stores/socket/socket-store.ts`).
- Same: the client talks through a `ConnectionSupervisor`, a `LocalDirectoryConnector` and an `ElectingConnector`. `serve` starts the server through an `AsyncDisposableStack`, so a failure before the server fully started closes it. The server gets a grace window only when the epoch of its term is higher than 1. The servers of this process are in one stack. The disposal closes the client first, and then the servers.
- Differs: `SocketStore` emits its role events: `leader` in `serve`, and `follower` from `connected`. Its poll interval is an option. The single flight has no role event, and its poll interval is a constant of 10 milliseconds.

**The grace window and the reassertion with a newer token.**

- Places: the constructor, `GraceWindow` and the reassertion of `Coordinating` in `FlightCoordinator` (`coordinator/flight-coordinator.ts`), and the constructor, `GraceWindow` and the reassertion of `Granting` in `LockCoordinator` (`lock-stores/remote/lock-coordinator.ts`).
- Same: a `Latch` and a timer that does not keep the process alive. The next phase is current before the latch opens, so a request that the latch lets through uses the next phase. A window of zero starts in the next phase. During the window, for two claims on one key, the newer token wins (`isNewerThan`), and the older claim is lost. After the window, each reassertion is refused.
- Differs: the coordinator of the single flight keeps a `Flight` for each claim, and it keeps how each claimed flight ended, for a rejoin that comes after the end. A reassertion wakes the rejoins of its key. The lock coordinator gets a lease from its `MemoryStore` for each claim, and it calls the `lose` callback of the older claim. A lock request waits for the window with its signal. A run waits without one, and the coordinator then ignores a run that was withdrawn (`Party.waiting`). The lock coordinator's `graceWindow` is optional, with a default of 0. The flight coordinator's `graceWindow` is necessary.

**How a session answers.**

- Places: `Session`, `Serving` and `Ended` in `coordinator/session.ts`, and the same three classes in `lock-stores/remote/lock-coordinator.ts`.
- Same:
  - A message with no `op` or no `id` cannot be answered, so the session ignores it ([ADR 0008](./adr/0008-a-connection-closes-only-when-its-framing-breaks.md), mutex ADR 0017).
  - The session answers a request that this version does not know with `unsupported` and the id of the request. The connection stays open.
  - The session ignores a second request with an id that it already has (`#open` and `#register`).
  - A send that fails is ignored: the client is gone, and `close` comes next.
  - A request whose handling fails closes the connection.
  - On `close`, the session goes to `Ended` first, so it answers nothing on a connection that is gone.
- Differs: the mutex's `Ended` gives back each lease that arrives after the end. The single flight's `Ended` answers nothing, and the session withdraws each of its parties. The single flight closes the connection through its phase, and only for a run or a rejoin that fails. Backlog #2544 is about the close after a failed request on the mutex's shared channels. The single flight uses only sockets, so #2544 does not apply to it.

**The failover rules of a client.**

- Places: `FlightClient` (`client/flight-client.ts`), and `RemoteLockClient` (`lock-stores/remote/remote-lock-client.ts`).
- Same:
  - `#lose` forgets the held entry first, and then tells the lease that it is lost. Thus a new connection does not reassert it.
  - `#fail`: no coordinator heard the reassertions, so each held entry is lost, and each request that waits rejects with the error.
  - `#resume` sets `ref` or `unref` first. Then it reasserts each held entry, and then it sends each request that waits for a connection.
  - `#interrupt`: a request that went out on the lost connection goes again on the next one.
  - A message that is not an answer of the protocol answers nothing that the client waits for, so the client ignores it. Before `a12029c`, the single flight closed the connection for such a message, and the client connected again.
- Differs: the single flight sends each landing again after its reassertion. A run that joined a flight goes again as a rejoin of that flight. A try of the mutex is one attempt, so it has a `stranded` state. The single flight has no `unavailable` listener. Its `#lose` also wakes `close`.

**The map of the requests that wait for an answer.**

- Places: `#take` of `FlightClient` (`client/flight-client.ts`), and `#take` of `RemoteLockClient` and of `Queries` in the mutex. This is three places. The [mutex note](../../mutex/docs/copied-code.md) records it.
- The maintainer chose to keep the three places (backlog #2538). A fix to one goes into each, and the commit names each file.

**A token on the wire.**

- Places: `isToken` in `protocol/flight-protocol.ts`, and in `lock-stores/remote/protocol.ts`.
- Same: a string that `FencingToken.parse` accepts.
- Differs: the single flight's `isToken` is a type guard. The mutex's `isToken` gives a `boolean`. The single flight also checks the `flight` of a run and of a `joined` answer with it.

## In two places inside the single flight

The code below is in two places inside this package. While both places exist, a fix to one place goes into the other place too, and the commit names both places. A third place is the time to extract the code.

The paths are in `packages/single-flight/src`.

**A joiner told that its flight ended.**

- Places: `land` and `interrupt` of `Flight` (`coordinator/flight.ts`), and the answer to a rejoin from an ending in `GraceWindow.rejoin` (`coordinator/flight-coordinator.ts`).
- Same: a joiner of a flight that landed gets `{ op: 'landed', outcome }`. A joiner of a flight that was interrupted gets `{ op: 'interrupted' }`.
- Differs: `Flight` tells each joiner when the flight ends, and on a landing it then tells the leader `ack`. `GraceWindow` tells a rejoin that comes after its reasserted flight ended during the window.

**A landing that still waits.**

- Places: `close` and `#updateRef` of `FlightClient` (`client/flight-client.ts`).
- Same: `[...this.#held.values()].some(({ landing }) => landing)`: a flight that this process leads landed, and no coordinator acknowledged the landing yet.
- Differs: `close` waits while it is true. `#updateRef` keeps the process alive while it is true.

**The message of a closed single flight.**

- Places: `run` and `close` of `FlightClient` (`client/flight-client.ts`).
- Same: the text `'This single flight is closed.'`.
- Differs: `run` throws it for a call after `close`. `close` rejects each run that waits for its answer with it.

**A rejoin joins only its own flight.**

- Places: `rejoin` of `Coordinating` and of `GraceWindow` (`coordinator/flight-coordinator.ts`).
- Same: when the flight of the key in progress is the flight that the rejoin names (`Flight.is`), the party joins it.
- Differs: `Coordinating` answers `interrupted` for each other case. `GraceWindow` then looks for how that flight ended, and else waits for a reassertion of the key or for the end of the window.

**The registration of a flight.**

- Places: `run` of `Coordinating` and `reassert` of `GraceWindow` (`coordinator/flight-coordinator.ts`).
- Same: `new Flight(key, token, party)`, and the coordinator keeps it as the flight of its key.
- Differs: a run tells its party that it leads (`Party.lead`). A reassertion only resumes its party (`Party.resume`), because a leader from before a failover already knows that it leads.

**The wake sets.**

- Places: `#released` of `FlightClient` (`close` and `#release` in `client/flight-client.ts`), and `#wakers` of `GraceWindow` (`#reassertionOf` and `reassert` in `coordinator/flight-coordinator.ts`).
- Same: a set of functions that resolve a wait. The waker takes the set, clears it, and calls each function.
- Differs: the client has one set, and each release wakes `close`, which checks its condition again. The coordinator has one set for each key, and a woken rejoin looks at the flights again.

**The read of one JSON line.**

- Places: the `line` listener in the constructor of `SocketConnection` (`connection/socket-connection.ts`), and `parse` in `connection/handshake.ts`.
- Same: `JSON.parse` of one line of the socket's newline-delimited JSON. The write side is in one place already: `jsonLine` (`connection/json-line.ts`).
- Differs: a line that is not JSON. `SocketConnection` closes the connection, because the framing is broken ([ADR 0008](./adr/0008-a-connection-closes-only-when-its-framing-breaks.md)). `parse` gives `undefined`, and the handshake reads that as no `hello` or no answer.
- Both files are copies of the mutex files, and the mutex note records the same two places. Thus this code goes with the files to `@zukhruf/coordinator` (backlog #2496).

**Not counted.**
`Party.lead` sends the token as text, and `Flight.id` is the token as text too. The `lead` answer carries the token of the lease, and `Flight.id` is the name of the flight on the wire. They change for different reasons, so they are not one piece of knowledge (`261b8b4`).

## What the extraction can share

- **As is:** the connection, the connection supervisor, and the socket connection.
- **With parameters:**
  - the election: done in `@zukhruf/election`, where the names of its two files are options. The mutex and this package use it;
  - the socket path: the file name and the pipe prefix;
  - the handshake: the protocol name, the version, and what the welcome lists;
  - the electing connector: the `connected` callback, and what wraps the connection. It checks no message: the class that reads the protocol checks it;
  - the server: what serves each connection, and how `close` ends each connection;
  - the protocol error: the subject of its message.
- **To compare:** the rules in [Knowledge shared with the mutex](#knowledge-shared-with-the-mutex). The third consumer shows which of them are the same in kind.
- **Not shared:** each coordinator's rules, and each client's requests.
