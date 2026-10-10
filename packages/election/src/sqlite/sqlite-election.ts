import { mkdir, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';

import {
  FileLock,
  assertLocalDirectory,
  atomicWrite,
  durableWrite,
  isErrno,
} from '@zukhruf/fs';

import { LeaderElection } from '../leader-election.ts';
import type { Term } from '../term.ts';

export interface SqliteElectionOptions {
  /** The local folder where the candidates meet. */
  directory: string;
  /**
   * The file in `directory` whose file lock is the claim.
   * Never delete it while candidates run: a new file would let a second
   * leader win. A term that resigns clean writes `<claimFile>.clean` beside
   * it, and the next term reads and removes it.
   */
  claimFile: string;
  /** The file in `directory` that records the epoch of the last term, as decimal text. */
  epochFile: string;
  /** Milliseconds between two tries while another process leads. Defaults to 10. */
  pollInterval?: number | undefined;
}

/**
 * Elects one leader among the processes of one host that share `directory`.
 * The claim is the file lock of `claimFile`, held for the whole term. The
 * kernel keeps that lock until the leader's process dies, so a living leader
 * never loses its term, and a dead one frees it at once.
 */
export class SqliteElection extends LeaderElection<FileLock> {
  readonly #directory: string;
  readonly #claimPath: string;
  readonly #epochPath: string;
  readonly #cleanShutdownPath: string;

  constructor({
    directory,
    claimFile,
    epochFile,
    pollInterval = 10,
  }: SqliteElectionOptions) {
    super(pollInterval);
    this.#directory = directory;
    this.#claimPath = join(directory, claimFile);
    this.#epochPath = join(directory, epochFile);
    this.#cleanShutdownPath = `${this.#claimPath}.clean`;
  }

  protected async open(): Promise<FileLock> {
    await assertLocalDirectory(this.#directory);
    await mkdir(this.#directory, { recursive: true });
    return FileLock.open(this.#claimPath);
  }

  protected async tryClaim(
    claim: FileLock,
  ): Promise<Pick<Term, 'epoch' | 'afterCleanShutdown'> | undefined> {
    if (!claim.tryLock()) return undefined;
    // Safe without further locking: only the holder of the claim gets here.
    const previous = await readEpoch(this.#epochPath);
    // Only a note that names the epoch just before this one tells of a clean
    // shutdown: an older note is from a term that ended long ago.
    const afterCleanShutdown =
      (await readCleanShutdown(this.#cleanShutdownPath)) ===
      previous.toString();
    const epoch = previous + 1n;
    await durableWrite(this.#epochPath, epoch.toString());
    // Read once: if the epochs ever start again, an old note could name one of them.
    await rm(this.#cleanShutdownPath, { force: true });
    return { epoch, afterCleanShutdown };
  }

  /** The kernel holds the claim for as long as the process lives, so there is nothing to watch. */
  protected watch(): Disposable {
    return { [Symbol.dispose]() {} };
  }

  /**
   * A lost note costs the next term only its recovery, so the note needs no
   * sync to the disk: an atomic replace is enough, and a reader never sees a
   * part of it.
   */
  protected async recordCleanShutdown(
    _claim: FileLock,
    epoch: bigint,
  ): Promise<void> {
    await atomicWrite(this.#cleanShutdownPath, epoch.toString());
  }

  protected async release(claim: FileLock): Promise<void> {
    claim.unlock();
  }

  protected async close(claim: FileLock): Promise<void> {
    claim[Symbol.dispose]();
  }
}

/** The epoch that the note names, as its text, or `undefined` when there is no note. */
async function readCleanShutdown(path: string): Promise<string | undefined> {
  try {
    return await readFile(path, 'utf8');
  } catch (error) {
    if (isErrno(error, 'ENOENT')) return undefined;
    throw error;
  }
}

async function readEpoch(path: string): Promise<bigint> {
  try {
    return BigInt(await readFile(path, 'utf8'));
  } catch (error) {
    if (isErrno(error, 'ENOENT')) return 0n;
    throw error;
  }
}
