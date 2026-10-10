# zukhruf

زخرف. A workspace of small Node.js libraries.

## Packages

| Package                                              | What it does                                                                                                                                                                                                                                                                                                                                                                                                     |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`@zukhruf/mutex`](./packages/mutex)                 | A mutex with interchangeable lock stores, from one object to every process on a host, with fencing tokens on every lease.                                                                                                                                                                                                                                                                                        |
| [`@zukhruf/eslint`](./packages/eslint)               | Shared ESLint rules and configs. Each check is its own named rule, so a repo's config cannot silently erase it. This workspace lints itself with it.                                                                                                                                                                                                                                                             |
| [`@zukhruf/testing`](./packages/testing)             | Disposable fixtures for integration tests: Docker containers and database servers, SQLite and DuckDB databases, HTTP servers and streams. A supervisor removes what a killed test run left behind.                                                                                                                                                                                                               |
| [`@zukhruf/single-flight`](./packages/single-flight) | Callers of one key share the run in progress, in one process or across the processes of a host that use one directory. A second caller joins the run and gets its outcome, not a "busy" error. One process of the directory is elected the coordinator, and it pushes each outcome. Its election comes from `@zukhruf/election`, its lease from `@zukhruf/lease`, and its fencing token from `@zukhruf/fencing`. |
| [`@zukhruf/async`](./packages/async)                 | Helpers for code that waits for promises. `untilAborted` cancels a wait when its signal aborts, also when other code stops the abort event. `Latch` holds callers until something happened, and never rejects.                                                                                                                                                                                                   |
| [`@zukhruf/fs`](./packages/fs)                       | File writes that a reader never sees half done: replace a file in one step, with or without a sync to the disk, or create it only when it is absent. A lock on a file that the kernel frees when its holder dies. Also refuses a directory on a network file system, and retries the short refusals of Windows.                                                                                                  |
| [`@zukhruf/election`](./packages/election)           | Elects one leader among candidates, with a term that ends when the leader resigns or dies, and an epoch that grows with each term. `SqliteElection` elects among the processes of one host; other backends are subclasses of one campaign.                                                                                                                                                                       |
| [`@zukhruf/lease`](./packages/lease)                 | A lease and its loss: the holder gets a signal that aborts once, with `LeaseLostError`, when another holder may have the right. An issuer gives out leases with `LeaseController`. A lease lasts for a session, not for a time.                                                                                                                                                                                  |
| [`@zukhruf/fencing`](./packages/fencing)             | Fencing tokens: an integer that grows with each grant, so a resource refuses the writes of a holder that lost its lease. Token sources that count in memory, in files, or by the epoch of a leader, and the fenced lease that carries a token.                                                                                                                                                                   |

## Apps

| App                                         | What it does                                                                                    |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [`reservation-app`](./apps/reservation-app) | An HTTP app that sells the last item once, with a fenced stock table. It uses `@zukhruf/mutex`. |

Each package has its own glossary. [CONTEXT-MAP.md](./CONTEXT-MAP.md) lists them.

## Development

```sh
npm install
npx nx run-many -t test        # builds, then runs every test
npx nx run-many -t typecheck   # formats, lints, then type checks
npx nx run <project>:test      # one project, e.g. mutex
```

## Use a package's source from another repo

Each export of a package has a `@zukhruf/source` condition first. This condition points at the source in `src/`. The other conditions point at the build in `dist/`. A repo that sets no condition gets `dist/`.

To change a package and a repo that uses it together, link the package. Then tell Node.js and TypeScript in the other repo to use the condition:

```sh
npm link                                            # in packages/<package>
npm link @zukhruf/<package>                         # in the other repo
NODE_OPTIONS=--conditions=@zukhruf/source node …    # in the other repo
```

In the `tsconfig.json` of the other repo:

```json
{
  "compilerOptions": {
    "customConditions": ["@zukhruf/source"],
    "allowImportingTsExtensions": true
  }
}
```

Node.js then runs the `.ts` files of the package, with no build. It can strip their types, because the link goes to a folder outside `node_modules`.

TypeScript then checks the source of the package with the options of the other repo. The source imports its own files with the `.ts` extension, so the other repo needs `allowImportingTsExtensions`. If the other repo emits JavaScript, use `rewriteRelativeImportExtensions` instead.

Set the condition in Node.js only while the package is linked. The published package has no `src/`, so Node.js stops with `ERR_MODULE_NOT_FOUND`. TypeScript does not stop: when the source file is not there, it uses the next condition, `types`.

The name follows the `@<scope>/source` convention of Nx and of the "live types" setup in TypeScript monorepos. The scope keeps the name unique. A repo that asks for `@zukhruf/source` gets the source of zukhruf packages only, and not of other packages that define a plain `source` condition. The name is not `development`, because bundlers such as Vite set `development` for every consumer.

## Release

Releases need no command. The packages that Nx tags `npm:public`, because their package.json is not private, are released together, as their conventional commits on `main` ask: a `feat`, `fix` or `refactor` bumps the patch version while the major version is 0, and `chore`, `docs`, `test` and `ci` release nothing.

When CI is green for a push to `main`, `.github/workflows/release.yml` runs `nx release version`, which versions the packages, commits `chore(release): publish <version>`, tags `release/<version>` and pushes both, and then `nx release publish`, which publishes to npm. Nothing reaches npm unless `main` has its release commit and tag, and a version already on npm is skipped. A push with nothing to release changes nothing. The release runs only for the commit CI tested; when `main` has moved on, the newer commit's CI run releases it.

To see what the next release would be:

```sh
npx nx release version --dry-run
```
