import { setTimeout as delay } from 'node:timers/promises';

import { untilAborted } from '@zukhruf/async';

import { Term } from './term.ts';

export interface CampaignOptions {
  /** Milliseconds to keep trying before conceding to the current leader. Defaults to one attempt. */
  timeout?: number | undefined;
  /** Stops the campaign, which then rejects with `signal.reason`. A term won after it aborted is given up. */
  signal?: AbortSignal | undefined;
}

/**
 * Elects one leader among candidates. The campaign is the same for each
 * backend: open an attempt, try the claim until it is won or the time is
 * up, watch the won claim, and hand it to a term. Each backend is a
 * subclass that implements the steps for its claim.
 *
 * A backend keeps five rules:
 * - `tryClaim` returns the epoch of the term it won, in the same step, and
 *   each epoch is higher than every earlier one and below 2^31.
 * - `tryClaim` reports `afterCleanShutdown` only when the term just before
 *   it recorded a clean shutdown with `recordCleanShutdown`, and it reports
 *   each record once.
 * - `watch` calls `lose` before the backend can grant the claim to another
 *   candidate, so an old leader stops before a new one starts.
 * - `release` is never called after a loss: the claim may be another
 *   candidate's by then.
 * - `close` frees the attempt's resources, after each end.
 */
export abstract class LeaderElection<Claim> {
  readonly #pollInterval: number;

  protected constructor(pollInterval: number) {
    this.#pollInterval = pollInterval;
  }

  /** Resolves with the new term, or `undefined` when another candidate still leads after `timeout`. */
  async campaign({ timeout = 0, signal }: CampaignOptions = {}): Promise<
    Term | undefined
  > {
    signal?.throwIfAborted();
    const deadline = performance.now() + timeout;
    for (;;) {
      const outcome = await this.#attempt(deadline, signal);
      // A claim lost before its term began is no term: try again with a new attempt.
      if (outcome !== 'lost') return outcome;
    }
  }

  /** Prepares one attempt: the resources that `tryClaim` uses again and again. */
  protected abstract open(signal: AbortSignal | undefined): Promise<Claim>;

  /**
   * Tries the claim once. Resolves with the epoch of the term it won and
   * whether the term before it resigned clean, or `undefined` while another
   * candidate holds it.
   */
  protected abstract tryClaim(
    claim: Claim,
    signal: AbortSignal | undefined,
  ): Promise<Pick<Term, 'epoch' | 'afterCleanShutdown'> | undefined>;

  /** Watches a won claim for as long as its term lasts, and calls `lose` when the backend takes it away. */
  protected abstract watch(
    claim: Claim,
    lose: (reason: Error) => void,
  ): Disposable;

  /**
   * Records that the term of `epoch` resigned clean, so that the next
   * `tryClaim` reports it. Called while the claim is held, before `release`,
   * and never after a loss.
   */
  protected abstract recordCleanShutdown(
    claim: Claim,
    epoch: bigint,
  ): Promise<void>;

  /** Gives a won claim up. Never called after a loss. */
  protected abstract release(claim: Claim): Promise<void>;

  /** Frees the resources of an attempt, whether it won, lost or failed. */
  protected abstract close(claim: Claim): Promise<void>;

  async #attempt(
    deadline: number,
    signal: AbortSignal | undefined,
  ): Promise<Term | 'lost' | undefined> {
    const claim = await this.open(signal);
    let won: Pick<Term, 'epoch' | 'afterCleanShutdown'> | undefined;
    try {
      for (;;) {
        won = await this.tryClaim(claim, signal);
        if (won !== undefined) break;
        if (performance.now() >= deadline) break;
        await untilAborted(
          delay(this.#pollInterval, undefined, { signal }),
          signal,
        );
      }
    } catch (error) {
      await this.close(claim).catch(() => {});
      throw error;
    }
    if (won === undefined) {
      await this.close(claim);
      return undefined;
    }
    return this.#begin(claim, won, signal);
  }

  /** Hands a won claim to its term, or gives it up when the caller left meanwhile. */
  async #begin(
    claim: Claim,
    won: Pick<Term, 'epoch' | 'afterCleanShutdown'>,
    signal: AbortSignal | undefined,
  ): Promise<Term | 'lost'> {
    let term: Term;
    try {
      signal?.throwIfAborted();
      term = new Term(won, {
        watch: (lose) => this.watch(claim, lose),
        recordCleanShutdown: () => this.recordCleanShutdown(claim, won.epoch),
        release: () => this.release(claim),
        close: () => this.close(claim),
      });
    } catch (error) {
      await this.release(claim).catch(() => {});
      await this.close(claim).catch(() => {});
      throw error;
    }
    return term.signal.aborted ? 'lost' : term;
  }
}
