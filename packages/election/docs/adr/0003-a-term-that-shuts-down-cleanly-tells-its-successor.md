# A term that shuts down cleanly tells its successor

The coordinators of `@zukhruf/single-flight` and of the socket lock store of `@zukhruf/mutex` are leaders of this package. When a new term starts after an earlier term, the coordinator waits a grace window. In that time, the candidates of the earlier term can tell it about their work in progress. When the earlier leader resigned with no work in progress, there is nothing to tell, but the coordinator waits all the same. Each process that runs alone thus starts about 500 ms late (backlog #2549). Only the leader knows that it has no work in progress. Only the election knows which term comes just after which. Thus the election keeps the record: a leader resigns with `term.resign({ clean: true })`, and the next term reads `term.afterCleanShutdown`.

Other systems do the same. The control file of PostgreSQL records the state of the cluster: "shut down" after a clean stop, "in production" while it runs. At startup, PostgreSQL does crash recovery only when the state is not "shut down". The controlled shutdown of Kafka also lets a broker tell the controller that it stops, so that the cluster has less to recover.

`SqliteElection` writes the epoch of the term to a note, `<claimFile>.clean`, before it gives the claim up. The next winner reads the epoch file and the note, and then removes the note. `afterCleanShutdown` is `true` only when the note holds the epoch just before the new one. A missing note, a damaged note, or a note with another epoch reads as `false`. The note is an atomic replace with `atomicWrite` of `@zukhruf/fs`, not a `durableWrite`: a note that a power loss removes only makes the next term recover when it did not have to. The epoch file still has the epoch only, as decimal text.

## Considered Options

- **Each coordinator keeps its own note.** The single flight and the mutex would each have a copy of the same rules: which term the note is about, and that each claim reads it once. The coordinator does not know the chain of terms, the election does.
- **A mark in the epoch file, for example `7 clean`.** Published versions read the epoch file with `BigInt`, which throws on any text after the number. A published candidate in the same directory would then fail each campaign.
- **A note beside the claim file, about one epoch, that each claim reads once.** This option was selected. The epoch makes a note from an older term useless, and the removal keeps a note from a term before a reset of the epoch file.

## Consequences

- A backend implements a sixth step, `recordCleanShutdown(claim, epoch)`, and `tryClaim` resolves with `{ epoch, afterCleanShutdown }`. The campaign calls `recordCleanShutdown` before `release`, while the term still holds the claim, and never after a loss. When the record fails, `resign` rejects with that error, and the claim is freed as after a failed release.
- `resign()` without options records nothing, so the next term recovers as before. `await using` resigns this way.
- Published versions (0.3.18 and older) never write or read the note. A published leader after a new one ignores the note, and it waits its grace window. A new leader after a published one finds no note, or a note with an older epoch, so it reads `false` and waits too.
- A directory of `SqliteElection` has one more file: `<claimFile>.clean`, for the time between a clean resign and the next claim.
