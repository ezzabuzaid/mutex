# Leader election is a package, and each backend is a subclass of one campaign

The socket lock store of `@zukhruf/mutex` and `@zukhruf/single-flight` each had a copy of the same leader election. The copies were the same except for their file names. The mutex also published its copy as the entry point `@zukhruf/mutex/leader-election` ([mutex ADR 0002](../../../mutex/docs/adr/0002-election-is-not-part-of-the-mutex.md)). Thus the election became this package, for both packages to use. The election had to be open to other backends too, so that an election across hosts is only a new subclass. Thus `LeaderElection` is an abstract class with the Template Method pattern: the base runs the campaign and owns the term, and a backend implements five steps for its claim. `SqliteElection` is the first backend. A backend can take a claim away while its leader still runs, so the term has a signal that aborts on a loss. That signal is now a lease of `@zukhruf/lease` ([ADR 0002](./0002-a-term-is-a-lease-on-leadership-with-no-fencing.md)).

## Considered Options

- **Keep the two copies until a third package needs the code.** This is the Rule of Three, and [single-flight ADR 0006](../../../single-flight/docs/adr/0006-the-election-and-the-connection-are-a-copy-of-the-mutex-code.md) selected it. The maintainer extracted the election at the second copy, because its boundary was already clear: the claim, the term, the epoch and the campaign. The connection to the leader stays a copy in each package (backlog #2496).
- **An interface for backends, with a separate campaign.** Each backend would repeat the campaign: the tries, the deadline, the abort, and the cleanup after a failure. Those rules are the same for each backend, so they belong to one base class.
- **One abstract class with the campaign, and abstract steps.** The repository already uses this pattern for its file lock stores (`FileLockStore`). [Apache Curator](https://curator.apache.org/) and [client-go](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection) also keep the election in one place and put the backend behind a narrow interface. This option was selected.

## Consequences

- The mutex and the single flight use this package since 2026-10-10. The entry point `@zukhruf/mutex/leader-election` is removed, and `Term` replaced the `Leadership` of each copy. The copies are deleted, so the election is in one place ([copied code](../copied-code.md)).
- A backend keeps four rules ([README](../../README.md#write-a-backend)). The base cannot enforce the timing rule for a claim that the backend keeps only for a time: `lose` must come before the backend can give the claim to another candidate.
- A leader that acts for its group stops when `term.signal` aborts. The servers of the socket lock store and of the single flight do this, although `SqliteElection` never loses a living term.
- Those two servers meet their candidates over a socket in the directory, so they work on one host only, whatever the backend.
