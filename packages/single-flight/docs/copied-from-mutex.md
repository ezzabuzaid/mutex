# Code copied from the mutex

A single flight elects a coordinator among the processes that use one directory. Each caller then talks to that coordinator over a socket. The mutex's socket lock store already does both. This package keeps its own copy of that code, and does not share it with the mutex yet.

The reason is the Rule of Three. Code that a second place needs is copied, and it is extracted only when a third place needs it. The boundary of a shared package then comes from three working copies, not from a guess. The mutex's wire also stays as it is, so old and new mutex processes cannot split into two leaders. When a third consumer needs this code, the shared parts move to `@zukhruf/coordinator` (backlog #2496). Until then, a fix in one copy is made in the other copy too, and the commit names both files. This note is the input for that step.

Paths on the left are in `packages/mutex/src`. Paths on the right are in `packages/single-flight/src`.

On 2026-10-10, the mutex changed several of these originals. The maintainer chose to change the mutex first, and this package later. So for these changes, the copies here do not follow yet. Backlog #2541 lists each change and the mutex commit to copy. Until then, the rows below say what differs now.

## Moved to `@zukhruf/fs`

The file helpers and the check that refuses a network directory are no longer copies. Both packages use `@zukhruf/fs` (its ADR 0001): `durableWrite`, `isErrno`, `patiently`, `assertLocalDirectory`, and one `NetworkDirectoryError` class that this package gives from its entry point.

## Moved to `@zukhruf/election`

The election is no longer a copy. `@zukhruf/election` holds it, made into one campaign with a subclass for each backend ([its ledger](../../election/docs/copied-code.md)). The mutex uses it since 2026-10-10, and this package too, since the same day (backlog #2530). The copy `election/` of this package and its SQLite helper `shared/sqlite/is-busy.ts` are deleted. The claim on `flight.lock` is now a `FileLock` of `@zukhruf/fs`. Its journal is in memory, so a coordinator that stops leaves no `flight.lock-journal` file. A published version claims `flight.lock` with an exclusive SQLite transaction, and that transaction and the file lock exclude each other (`flight-protocol.test.ts` pins both directions).

## Copied without a change

| Mutex                 | Single flight         |
| --------------------- | --------------------- |
| `shared/is-record.ts` | `shared/is-record.ts` |

## Copied with changes

**`lock-stores/remote/connection.ts` → `connection/connection.ts`.**
The copy has a second type parameter, `Incoming`: its connection gives each message as checked. The mutex removed that parameter in 7f80fa7. A mutex connection gives each message as `unknown`, and the class that reads the protocol checks it (the mutex's ADR 0017). Backlog #2541.

**`lock-stores/remote/connection-supervisor.ts` → `connection/connection-supervisor.ts`.**
Only the type parameter `Incoming` differs, as in `connection.ts`. Backlog #2541.

**`lock-stores/socket/socket-election.ts` → `connection/flight-election.ts`.**
The claim file is `flight.lock`, not `leader.lock`. The epoch file is `flight.epoch`, not `leader.epoch`. The function is `flightElection`, not `socketElection`.
Why: a single flight and a socket lock store can use one directory. With the same file names, they would share one election. The winner would serve only one of them, and the other one would never find a server.

**`lock-stores/remote/connector.ts` → `connection/connector.ts`.**
The lock aliases `ClientConnection` and `ClientConnector` become `FlightConnection` and `FlightConnector`. The comment on `connect` does not name keys. The copy also keeps the type parameter `Incoming`, which the mutex removed in 7f80fa7 (backlog #2541).

**`lock-stores/socket/socket-connection.ts` → `connection/socket-connection.ts`.**
Differs since 2026-10-10 (backlog #2541):

- The copy takes an `isIncoming` check, and it closes the connection for a JSON line that fails the check. The mutex takes no check, and closes only for a line that is not JSON (7f80fa7, the mutex's ADR 0017). The maintainer chose that a JSON line that is not a message is ignored.
- The copy builds each line with `JSON.stringify` and a newline. The mutex uses `jsonLine` (27f9f3c).
- The copy wraps each write in a promise, after its own check that the socket is writable. The mutex binds `socket.write` once with `promisify`, and the callback reports a write to a closed socket (46a6cc6).

**`lock-stores/socket/local-directory-connector.ts` → `connection/local-directory-connector.ts`.**
Only its types and comments change.

**`lock-stores/socket/handshake.ts` → `connection/handshake.ts`.**

- The hello names the protocol: `{"op":"hello","protocol":"single-flight","version":1}`. The refusal names it too.
- The welcome is `{"op":"welcome"}`. It lists no requests, because this protocol has no request that came after version 1.
- A process that opens with something other than a hello is closed at once. The mutex keeps such a connection open, for its processes from before the handshake. No such process of this package exists.
- `FIRST_LINE_LIMIT` stays 1024 bytes.

Why: a hello that names its protocol cannot be read as a hello of another protocol.

Differs since 2026-10-10: the copy builds each line with `JSON.stringify` and a newline. The mutex uses `jsonLine` (27f9f3c). Backlog #2541.

**`lock-stores/socket/protocol-version-error.ts` → `connection/protocol-version-error.ts`.**
The messages name the single flight's coordinator, not the socket store's leader.

**`lock-stores/socket/electing-connector.ts` → `connection/electing-connector.ts`.**

- It has no `connected` callback. In the mutex, that callback only emits the `follower` role event. This package has no role event.
- It does not wrap the connection in `AdvertisedOpsConnection`, because the welcome lists no requests.
- The connection checks each message as a `FlightResponse`.

Differs since 2026-10-10 (backlog #2541):

- The copy waits between tries with `delay` alone. When the signal aborts there, the connect rejects with an `AbortError`, not with the reason of the signal, so the copy breaks the contract of `Connector`. The mutex wraps the wait in `untilAborted` (9609c2d).
- The copy gives up a term that it cannot serve, and destroys a socket that it cannot greet, in `catch` blocks. The mutex does both with disposable stacks (86b91bd).
- The copy reaches a socket with a promise that it builds itself. The mutex waits for `once(socket, 'connect')` (5ac2a20).
- The mutex's socket connection takes no check (7f80fa7). The copy gives `isFlightResponse` to its socket connection.

**`lock-stores/socket/socket-store.ts` (`socketPathFor`, `SOCKET_PATH_LIMIT`) → `connection/socket-path.ts`.**
The socket file is `flight.sock`. The Windows pipe is `\\.\pipe\single-flight-<hash>`. The 103-byte limit stays.
Note: on macOS 27 with Node.js 26, a probe listened and connected on socket paths of 104 to 111 bytes. The mutex's comment says that macOS stops at 104 bytes. The extraction must check that premise before it keeps the limit.

**`lock-stores/socket/lock-server.ts` → `coordinator/flight-server.ts`.**

- It serves a `FlightCoordinator`, not a `LockCoordinator`. `EpochTokenSource` comes from `@zukhruf/fencing`.
- `close` ends each connection with `destroySoon`, not `destroy`.

Why: a coordinator process can also lead a flight. When it lands the flight and stops at once, `destroy` can drop a `landed` answer that is still on its way to a joiner. `destroySoon` sends what was written first, and then closes.

Differs since 2026-10-10 (backlog #2541):

- The copy deletes an old socket file with `unlink` and an `ENOENT` check, waits for `listen` with a promise that it builds itself, and closes with `server.close` in a promise. The mutex uses `rm` with `force`, `once(server, 'listening')`, and `server[Symbol.asyncDispose]()` (5ac2a20).
- The copy gives `isRequestEnvelope` to each socket connection. The mutex gives no check: `LockCoordinator` checks each message (7f80fa7).

**`lock-stores/remote/protocol.ts` (`RequestEnvelope`, `isRequestEnvelope`) → `protocol/flight-protocol.ts`.**
They are copied as they are. The rest of that file is the lock protocol, and this package has its own.
Differs since 2026-10-10 (backlog #2541): the mutex's `isLockRequest` checks only the fields of each request, because `LockCoordinator` already ran `isRequestEnvelope` (7f80fa7). `isFlightRequest` here checks the record and the `id` again, after the socket connection ran `isRequestEnvelope`.

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
- `lock-stores/remote/lock-coordinator.ts` and `remote-lock-client.ts`. `FlightCoordinator`, its sessions and `FlightClient` follow their shape, with phases, a grace window, reassertion and re-sent requests. But their rules are this package's: one request leads or joins, and an outcome goes to every joiner. One part is the same: the map of the requests that wait for an answer, and its `#take`, are in `FlightClient` and in two classes of the mutex. The maintainer chose to keep the three places (backlog #2538, the [mutex note](../../mutex/docs/copied-code.md)). A fix to one goes into each, and the commit names each file.

## What the extraction can share

- **As is, after backlog #2541:** the connection, the connection supervisor, and the socket connection.
- **With parameters:**
  - the election: done in `@zukhruf/election`, where the names of its two files are options. The mutex and this package use it;
  - the socket path: the file name and the pipe prefix;
  - the handshake: the protocol name, the version, and what the welcome lists;
  - the electing connector: the `connected` callback, and what wraps the connection. After backlog #2541, it checks no message: the class that reads the protocol checks it;
  - the server: what serves each connection;
  - the protocol error: the subject of its message.
- **Not shared:** each coordinator's rules, and each client's requests.
