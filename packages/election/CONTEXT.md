# Election

Candidates campaign for one claim. The candidate that wins it is the leader for one term. Each term has an epoch that is higher than the epoch of each earlier term. A term is a lease on leadership, as `@zukhruf/lease` defines a lease. A backend decides what the claim is: for example, a SQLite lock on a local file, or a row in a database that the leader must renew.

## Language

**Candidate**:
A process, or a part of a process, that campaigns to lead.
_Avoid_: Node, member, peer

**Campaign**:
The tries of one candidate to win the claim, until it wins or its time is up.
_Avoid_: Vote, poll

**Claim**:
The one thing that only one candidate can hold at a time. The backend decides what it is.
_Avoid_: Lock (a claim can be a row in a database), lease (the term is the lease, not the claim), token

**Leader**:
The candidate that holds the claim. A group has at most one leader at a time.
_Avoid_: Master, primary, coordinator (a coordinator is what some packages build on a leader)

**Term**:
The time that one leader holds the claim. A term is a lease on leadership: its signal aborts when the backend takes the claim away. See the glossary of [@zukhruf/lease](../lease/CONTEXT.md). A term ends when the leader resigns, when its process dies, or when the backend takes the claim away. A term has no fencing token.
_Avoid_: Session, tenure

**Epoch**:
The number of a term. Each term has a higher epoch than all the terms before it, and the epoch is below 2^31. A fencing token can carry it: `EpochTokenSource` of `@zukhruf/fencing` makes tokens from it, so a newer leader always outranks an older one.
_Avoid_: Generation, version, revision

**Resign**:
The leader ends its term on purpose and gives the claim up.
_Avoid_: Release (the backend releases the claim when the leader resigns), step down

**Clean shutdown**:
A resign in which the leader says that it leaves nothing for the next term to recover, for example no work in progress. The backend records it while the term still holds the claim. Only the next term reads it, as `afterCleanShutdown`. A crash, a lost term, and a resign without `clean` are not a clean shutdown.
_Avoid_: Graceful shutdown, handover

**Lost term**:
A term that ended while its leader still ran: the backend took the claim away, for example because the leader did not renew it in time. A lost term is a lost lease of `@zukhruf/lease`: the signal of the term aborts with `LeaseLostError`, and its subject is `'leadership'`.
_Avoid_: Expired, revoked

**Backend**:
A subclass of `LeaderElection` that implements one kind of claim. `SqliteElection` is the backend of this package.
_Avoid_: Driver, adapter, provider
