import { Latch } from '@zukhruf/async';
import { type Lease, LeaseController } from '@zukhruf/lease';

/** What a term needs from its election to end: the backend's steps for the claim it won. */
export interface TermSteps {
  /** Starts watching the claim; `lose` reports that the backend took it away. */
  watch(lose: (reason: Error) => void): Disposable;
  /**
   * Records that the term resigned clean, while it still holds the claim.
   * Called only for a term that was not lost, before `release`.
   */
  recordCleanShutdown(): Promise<void>;
  /** Gives the claim up. Called only for a term that was not lost. */
  release(): Promise<void>;
  /** Frees the claim's resources. Called at each end. */
  close(): Promise<void>;
}

export interface ResignOptions {
  /**
   * The leader says that it leaves nothing for its successor to recover,
   * for example no work in progress. The next term then reads
   * `afterCleanShutdown` as true. A lost term never says it.
   */
  clean: boolean;
}

/** How a term ended, as data: a latch never rejects. */
type Ending = { failed: false } | { failed: true; error: unknown };

/**
 * One term of the elected leader: a lease on leadership. It ends when the
 * leader resigns, when its process dies, or when the backend takes the claim
 * away. Only the last one can happen while the leader still runs, and then
 * `signal` aborts with `LeaseLostError`.
 */
export class Term implements Lease, AsyncDisposable {
  /** Higher than the epoch of each earlier term, so a newer leader can always outrank an older one. */
  readonly epoch: bigint;
  /**
   * True only when the term just before this one resigned clean: its leader
   * left nothing to recover. False after a crash, a lost term, a resign that
   * did not say it was clean, and for the first term.
   */
  readonly afterCleanShutdown: boolean;
  readonly #steps: TermSteps;
  readonly #lease = new LeaseController('leadership');
  /** Aborts once the term starts to end, by resign or by loss. */
  readonly #ending = new AbortController();
  readonly #ended = new Latch<Ending>();
  readonly #watching = new DisposableStack();

  constructor(
    { epoch, afterCleanShutdown }: Pick<Term, 'epoch' | 'afterCleanShutdown'>,
    steps: TermSteps,
  ) {
    this.epoch = epoch;
    this.afterCleanShutdown = afterCleanShutdown;
    this.#steps = steps;
    this.#watching.use(steps.watch((reason) => this.#lose(reason)));
  }

  /** Aborts with `LeaseLostError` when the term ends while its leader still runs. */
  get signal(): AbortSignal {
    return this.#lease.signal;
  }

  /**
   * Ends the term and gives the claim up. With `clean`, it first records a
   * clean shutdown for the next term. A second call waits for the first, and
   * its options do nothing. After a loss, it only waits until the claim's
   * resources are free, records nothing, and never rejects.
   */
  async resign(options?: ResignOptions): Promise<void> {
    if (!this.#ending.signal.aborted) {
      this.#ending.abort();
      this.#lease.end();
      this.#ended.open(await this.#giveUp(options?.clean === true));
    }
    const ending = await this.#ended.wait();
    if (ending.failed) throw ending.error;
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.resign();
  }

  #lose(reason: Error) {
    // After a resign began, the lease ignores the loss: the leader gave the
    // claim up on purpose, so it is not told.
    this.#lease.lose(reason);
    // The claim is freed once, by the resign or by the first loss.
    if (this.#ending.signal.aborted) return;
    this.#ending.abort();
    // Freed one step later: a watch that reports a loss before it returns is
    // held by then, so it stops before its claim is freed.
    void Promise.resolve()
      .then(() => this.#freeLost())
      .then((ending) => this.#ended.open(ending));
  }

  async #giveUp(clean: boolean): Promise<Ending> {
    try {
      this.#watching.dispose();
      // Before the release: until then no other candidate can hold the claim.
      if (clean) await this.#steps.recordCleanShutdown();
      await this.#steps.release();
    } catch (error) {
      await this.#steps.close().catch(() => {});
      return { failed: true, error };
    }
    try {
      await this.#steps.close();
      return { failed: false };
    } catch (error) {
      return { failed: true, error };
    }
  }

  /**
   * A lost claim is no longer this term's to give up: a release could undo
   * the next leader's claim. Only its resources are freed, and a loss has no
   * caller to report a failure to.
   */
  async #freeLost(): Promise<Ending> {
    try {
      this.#watching.dispose();
    } finally {
      await this.#steps.close().catch(() => {});
    }
    return { failed: false };
  }
}
