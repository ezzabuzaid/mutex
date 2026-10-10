# Single flight

Callers of one key share the flight in progress. A caller that comes while a flight runs does not start a second flight. It joins the flight and gets its outcome. All the processes that use one directory share their flights. One of these processes is the coordinator, and each call goes through it. Lease has the meaning that [the lease glossary](../lease/CONTEXT.md) gives it. Fencing token has the meaning that [the fencing glossary](../fencing/CONTEXT.md) gives it. Clean shutdown has the meaning that [the election glossary](../election/CONTEXT.md) gives it.

## Language

### Flights

**Flight**:
One run of the work for a key. A key has at most one flight in progress in a directory.
_Avoid_: Job, request, call

**Leader**:
The caller whose call runs the work of the flight. Its process can be any process of the directory. The leader is not the coordinator.
_Avoid_: Owner, primary

**Join**:
A caller waits for the flight in progress instead of starting one. The caller then gets the outcome of that flight.
_Avoid_: Dedupe, attach, subscribe

**Joiner**:
A caller that joined a flight. The coordinator told it which flight it joined, and its `onJoin` ran.
_Avoid_: Follower, waiter

**Landing**:
The leader ends its flight with a value or an error. The coordinator pushes that outcome to each joiner.
_Avoid_: Release, commit

**Outcome**:
How a flight ended: landed with a value, landed with an error, or interrupted.
_Avoid_: Result, response

**Interrupted**:
The outcome of a flight that ended without a landing: the process of its leader stopped, or its leader lost the lease of the flight. A joiner of an interrupted flight never runs the work again.
_Avoid_: Aborted, crashed

**Codec**:
The pair `encode` and `decode`. It turns the value of a flight into text and back. The value goes to the joiners as that text.
_Avoid_: Serializer, parse

**Cancel**:
A caller stops its own wait with a signal. The call rejects with the reason of the signal. The flight continues for the other callers, also when the caller of the leader cancels.
_Avoid_: Abort (the signal aborts; the caller cancels), give up

### The coordinator

**Directory**:
The local folder where the processes that share flights meet. All the processes that use one directory are one group. A key is shared only in its group.
_Avoid_: Records, lock directory

**Coordinator**:
The process of a directory that won the election. It answers each call: lead or join, in one step. It pushes each landing to the joiners.
_Avoid_: Leader (a leader runs a flight), server, master

**Term**:
The time that one coordinator holds its election. A term ends when the process of the coordinator stops or disposes its single flight. A term can also be lost while the coordinator still runs, when the election takes it away. Then the coordinator stops serving at once, and the other processes elect a new coordinator. The election of a single flight never takes a term from a coordinator that runs.
_Avoid_: Session, lease

**Epoch**:
The number of a term. Each term has a higher epoch than all the terms before it. The epoch is the high 32 bits of each lease token of the term.
_Avoid_: Generation, version

**Grace window**:
The first milliseconds of a term that follows another term. The coordinator starts no flight in it, so the leaders can reassert the flights in progress first. The first term of a directory has no grace window. A term after a clean shutdown has none too: the coordinator before it shuts down clean only when no flight is in progress and its own grace window is over.
_Avoid_: Timeout, delay

**Reassert**:
A leader tells a new coordinator about the flight that it leads, after the coordinator before it stopped. A reassert after the grace window is refused, and the leader loses its flight.
_Avoid_: Reclaim, re-register

**Rejoin**:
A joiner that lost its connection asks the new coordinator for the flight that it joined, by the token of that flight. A rejoin never leads. A call that got no answer joined nothing, so it is not a rejoin: it goes again as a new call.
_Avoid_: Resend, retry
