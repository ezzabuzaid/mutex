# The next coordinator skips the grace window after a clean shutdown

When a coordinator stops, the next coordinator starts its term with a grace window. In the grace window, the leaders of the last term reassert their flights, and the joiners rejoin them ([ADR 0005](./0005-a-joiner-rejoins-its-flight-by-its-token.md)). Before this decision, each term after the first term of a directory had a grace window, also when the coordinator before it disposed with no flight in progress. Then there was nothing to reassert, but the next coordinator waited all the same. Backlog #2549 measured it: three processes that run alone, one after the other, on one directory, each with one call and a disposal. The first call of the first term took 15 ms, and the first call of each later term took about 512 ms.

Only the coordinator knows that it has no flight in progress. Only the election knows which term comes just after which. Thus the coordinator tells the election, and the election keeps the record: `term.resign({ clean: true })` records a clean shutdown, and the next term reads `term.afterCleanShutdown` ([election ADR 0003](../../../election/docs/adr/0003-a-term-that-shuts-down-cleanly-tells-its-successor.md)). PostgreSQL does the same with its control file: after a clean stop, the next start does no crash recovery. Here, the grace window is the recovery.

A coordinator resigns clean only when it knows that it leaves nothing to recover:

- No flight is in progress in its coordinator.
- Its own grace window is over, or it had none. In its grace window, a leader of the term before it can still reassert a flight that this coordinator does not know yet.
- Its term is not lost. The election records nothing for a lost term.

The server reads this in the step that ends its connections. It cannot read it later: each connection that ends withdraws its flights, so after that there is never a flight in progress. A run can get its token in the same moment. Its `lead` answer goes out after the connection ended, so it never reaches its process. That process got no answer, so it sends the run again to the next coordinator as a new call, and the run leads or joins there. Thus no process knows of a flight that the read did not see.

## Considered Options

- **Each term after the first has a grace window.** This was the behavior before. Each process that runs alone starts about 500 ms late.
- **The coordinator keeps its own note in the directory.** The election ADR 0003 tells why the election keeps the note: it knows the chain of terms.
- **The server reads after the connections closed.** Then it reads no flight in progress each time, also when a leader in another process still runs its work. The next coordinator would refuse the reassert of that leader, and the leader would lose its lease.
- **The server reads when its term starts to end, with the grace window and the flights in progress.** This option was selected.

## Consequences

- A process that runs alone, after a process that disposed with nothing in progress, leads its first call at once. The #2549 probe now gives 15, 13 and 18 ms for the first three terms.
- A coordinator that disposes while a flight of another process is in progress leaves a grace window to the next coordinator. The leader reasserts its flight there, and its joiners get its value.
- After a stop without a disposal, for example a crash, the next coordinator keeps its grace window.
- A coordinator that disposes in its own grace window leaves a grace window to the next coordinator.
- A directory of a single flight has one more file, `flight.lock.clean`, between a clean shutdown and the next term.
- Published versions (0.3.18 and older) never record a clean shutdown and never read one. In a directory that they share with this version, the term after one of them, and each term of one of them, keeps its grace window.
- The socket lock store of `@zukhruf/mutex` still has a grace window in each term after the first. Its own step follows (#2549).
