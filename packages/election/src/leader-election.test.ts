import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

// A backend is written against the package's public entry, as an outside author would.
import { LeaderElection, LeaseLostError } from './index.ts';
import { waitUntil } from './testing/wait-until.ts';

interface Script {
  /** What each try answers, in order: an epoch, `busy`, or an error. The last answer repeats. */
  tries?: Array<bigint | 'busy' | Error>;
  /** Runs inside each try, before it answers. */
  duringTry?: () => void;
  /** The attempt whose watch reports a loss before it returns. */
  lostAtOnce?: number;
  /** Aborting it reports a loss to the term that is watched then. */
  lose?: AbortSignal;
  /** Collects each watch's `lose`, so a test can call one late. */
  losers?: Array<(reason: Error) => void>;
  /** Thrown by each watch: the backend could not start to watch its claim. */
  watchFails?: Error;
  recordFails?: Error;
  /** Runs inside `release`, before it answers. */
  duringRelease?: () => Promise<void>;
  /** Runs inside `close`, before it answers. */
  duringClose?: () => Promise<void>;
  releaseFails?: Error;
  closeFails?: Error;
}

/**
 * A backend whose claim is the number of its attempt. It records each step it
 * is asked for, so a test reads what the campaign and the term did.
 */
class ScriptedElection extends LeaderElection<number> {
  readonly steps: string[] = [];
  readonly #script: Script;
  #attempts = 0;
  #tries = 0;

  constructor(script: Script, pollInterval = 1) {
    super(pollInterval);
    this.#script = script;
  }

  protected async open(): Promise<number> {
    this.#attempts += 1;
    this.steps.push(`open ${this.#attempts}`);
    return this.#attempts;
  }

  protected async tryClaim(
    claim: number,
  ): Promise<{ epoch: bigint; afterCleanShutdown: boolean } | undefined> {
    this.steps.push(`try ${claim}`);
    const answers = this.#script.tries ?? [1n];
    const answer = answers[Math.min(this.#tries, answers.length - 1)];
    this.#tries += 1;
    this.#script.duringTry?.();
    if (answer instanceof Error) throw answer;
    if (answer === 'busy' || answer === undefined) return undefined;
    return { epoch: answer, afterCleanShutdown: false };
  }

  protected watch(claim: number, lose: (reason: Error) => void): Disposable {
    this.steps.push(`watch ${claim}`);
    if (this.#script.watchFails) throw this.#script.watchFails;
    this.#script.losers?.push(lose);
    const expire = () =>
      lose(new Error(`The lease of attempt ${claim} expired`));
    this.#script.lose?.addEventListener('abort', expire, { once: true });
    if (this.#script.lostAtOnce === claim) expire();
    return {
      [Symbol.dispose]: () => {
        this.#script.lose?.removeEventListener('abort', expire);
        this.steps.push(`unwatch ${claim}`);
      },
    };
  }

  protected async recordCleanShutdown(
    claim: number,
    epoch: bigint,
  ): Promise<void> {
    this.steps.push(`record ${claim} at ${epoch}`);
    if (this.#script.recordFails) throw this.#script.recordFails;
  }

  protected async release(claim: number): Promise<void> {
    this.steps.push(`release ${claim}`);
    await this.#script.duringRelease?.();
    if (this.#script.releaseFails) throw this.#script.releaseFails;
  }

  protected async close(claim: number): Promise<void> {
    this.steps.push(`close ${claim}`);
    await this.#script.duringClose?.();
    if (this.#script.closeFails) throw this.#script.closeFails;
  }
}

/** Settles like `running`, but never rejects, so the rejection handler is attached in the step that starts it. */
const settle = <T>(running: Promise<T>) =>
  running.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );

/** Lets every queued step run, so a step that does not wait happens now. */
const drain = () => new Promise((resolve) => setImmediate(resolve));

describe('A term that its backend takes away', () => {
  test(
    'a lost term aborts its signal with LeaseLostError, stops watching, and frees its claim without giving it up',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const lease = new AbortController();
      const election = new ScriptedElection({ lose: lease.signal });
      const term = await election.campaign();
      assert.ok(term);

      // Act: the backend reports that the lease expired.
      lease.abort();
      await term.resign();

      // Assert
      assert.ok(term.signal.aborted);
      assert.ok(term.signal.reason instanceof LeaseLostError);
      assert.equal(term.signal.reason.subject, 'leadership');
      assert.match(
        String(term.signal.reason.cause),
        /lease of attempt 1 expired/,
      );
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'close 1',
      ]);
    },
  );

  test(
    'a resign after a loss resolves, gives nothing up, and frees the claim once, also when freeing it fails',
    { timeout: 10_000 },
    async () => {
      // Arrange: freeing the claim fails, as a dead connection to a backend can.
      const lease = new AbortController();
      const election = new ScriptedElection({
        lose: lease.signal,
        closeFails: new Error('The connection to the backend is gone'),
      });
      const term = await election.campaign();
      assert.ok(term);
      lease.abort();

      // Act
      const first = await settle(term.resign());
      const second = await settle(term.resign());

      // Assert
      assert.deepEqual(first, { value: undefined });
      assert.deepEqual(second, { value: undefined });
      assert.deepEqual(
        election.steps.filter((step) => /release|close/.test(step)),
        ['close 1'],
      );
    },
  );

  test(
    'a loss that comes while the term resigns changes nothing: the signal stays quiet, and the claim is given up and freed once',
    { timeout: 10_000 },
    async () => {
      // Arrange: the backend reports a loss while the release is on its way.
      const losers: Array<(reason: Error) => void> = [];
      const election = new ScriptedElection({
        losers,
        duringRelease: async () => losers[0]?.(new Error('A late expiry')),
      });
      const term = await election.campaign();
      assert.ok(term);

      // Act
      await term.resign();

      // Assert
      assert.equal(term.signal.aborted, false);
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'release 1',
        'close 1',
      ]);
    },
  );

  test(
    'a loss before the term begins ends that attempt without giving it up, and the campaign wins with a new attempt that a late loss cannot touch',
    { timeout: 10_000 },
    async () => {
      // Arrange: the first won claim is lost while it starts to be watched.
      const losers: Array<(reason: Error) => void> = [];
      const election = new ScriptedElection({ lostAtOnce: 1, losers });

      // Act
      const term = await election.campaign();
      assert.ok(term);
      // The first attempt's backend reports its loss again, after its attempt ended.
      losers[0]?.(new Error('A late report'));
      await term.resign();

      // Assert: each attempt in its own order; the two attempts may overlap.
      const stepsOf = (attempt: number) =>
        election.steps.filter((step) => step.endsWith(` ${attempt}`));
      assert.equal(term.signal.aborted, false);
      assert.deepEqual(stepsOf(1), [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'close 1',
      ]);
      assert.deepEqual(stepsOf(2), [
        'open 2',
        'try 2',
        'watch 2',
        'unwatch 2',
        'release 2',
        'close 2',
      ]);
    },
  );
});

describe('A term that its leader resigns', () => {
  test(
    'resign stops watching, then gives the claim up, then frees it, once; a second caller waits for the first, and the signal never aborts',
    { timeout: 10_000 },
    async () => {
      // Arrange: the release and the close answer only when the test lets them,
      // so the second resign comes while they run.
      const releasing = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const closing = Promise.withResolvers<void>();
      const close = Promise.withResolvers<void>();
      const election = new ScriptedElection({
        duringRelease: async () => {
          releasing.resolve();
          await release.promise;
        },
        duringClose: async () => {
          closing.resolve();
          await close.promise;
        },
      });
      const term = await election.campaign();
      assert.ok(term);

      // Act
      const events: string[] = [];
      const first = settle(term.resign()).then((result) => {
        events.push('first settled');
        return result;
      });
      await releasing.promise;
      const second = settle(term.resign()).then((result) => {
        events.push('second settled');
        return result;
      });
      await drain();
      const stepsWhileReleasing = [...election.steps];
      events.push('release answers');
      release.resolve();
      await closing.promise;
      await drain();
      events.push('close answers');
      close.resolve();
      const results = await Promise.all([first, second]);

      // Assert
      assert.deepEqual(
        events.slice(0, 2),
        ['release answers', 'close answers'],
        `A resign settled before the claim was free: ${events.join(', ')}`,
      );
      assert.ok(
        !stepsWhileReleasing.includes('close 1'),
        'The claim was freed before the release answered',
      );
      assert.deepEqual(results, [{ value: undefined }, { value: undefined }]);
      assert.equal(term.signal.aborted, false);
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'release 1',
        'close 1',
      ]);
    },
  );

  test(
    'a resign whose release fails still frees the claim, and each caller rejects with that failure',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const failure = new Error('The backend refused the release');
      const election = new ScriptedElection({ releaseFails: failure });
      const term = await election.campaign();
      assert.ok(term);

      // Act
      const first = await settle(term.resign());
      const second = await settle(term.resign());

      // Assert
      assert.deepEqual(first, { error: failure });
      assert.deepEqual(second, { error: failure });
      assert.deepEqual(election.steps.slice(-2), ['release 1', 'close 1']);
    },
  );
});

describe('A term that its leader resigns clean', () => {
  test(
    'a clean resign records the shutdown with the term’s epoch after it stops watching and before it gives the claim up',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const election = new ScriptedElection({ tries: [7n] });
      const term = await election.campaign();
      assert.ok(term);

      // Act
      await term.resign({ clean: true });

      // Assert
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'record 1 at 7',
        'release 1',
        'close 1',
      ]);
    },
  );

  test(
    'a resign with clean set to false records nothing, as a resign without options',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const election = new ScriptedElection({});
      const term = await election.campaign();
      assert.ok(term);

      // Act
      await term.resign({ clean: false });

      // Assert
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'release 1',
        'close 1',
      ]);
    },
  );

  test(
    'a clean resign whose record fails still frees the claim, and each caller rejects with that failure',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const failure = new Error('The backend could not record the shutdown');
      const election = new ScriptedElection({ recordFails: failure });
      const term = await election.campaign();
      assert.ok(term);

      // Act
      const first = await settle(term.resign({ clean: true }));
      const second = await settle(term.resign());

      // Assert
      assert.deepEqual(first, { error: failure });
      assert.deepEqual(second, { error: failure });
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'unwatch 1',
        'record 1 at 1',
        'close 1',
      ]);
    },
  );
});

describe('A campaign', () => {
  test(
    'a campaign that finds the claim held until its deadline resolves undefined and frees its attempt; a timeout of 0 tries once',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const election = new ScriptedElection({ tries: ['busy'] });

      // Act
      const once = await election.campaign();
      const steps = [...election.steps];

      // Assert
      assert.equal(once, undefined);
      assert.deepEqual(steps, ['open 1', 'try 1', 'close 1']);
    },
  );

  test(
    'a campaign tries again every poll interval of its backend until it wins, and its term carries the exact epoch that the backend gave',
    { timeout: 10_000 },
    async () => {
      // Arrange: the backend's epoch is large and not one more than the last, as an etcd revision is.
      const election = new ScriptedElection(
        { tries: ['busy', 'busy', 1_048_573n] },
        40,
      );
      const started = performance.now();

      // Act
      const term = await election.campaign({ timeout: 1000 });
      const took = performance.now() - started;

      // Assert
      assert.ok(term);
      assert.equal(term.epoch, 1_048_573n);
      assert.ok(
        took >= 70,
        `Two waits of 40 ms took only ${Math.round(took)} ms`,
      );
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'try 1',
        'try 1',
        'watch 1',
      ]);
      await term.resign();
    },
  );

  test(
    'two campaigns of one election each get an attempt of their own, and each attempt is freed once',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const election = new ScriptedElection({ tries: ['busy'] });

      // Act
      const results = await Promise.all([
        election.campaign({ timeout: 20 }),
        election.campaign({ timeout: 20 }),
      ]);

      // Assert
      assert.deepEqual(results, [undefined, undefined]);
      assert.deepEqual(
        election.steps.filter((step) => /open|close/.test(step)).sort(),
        ['close 1', 'close 2', 'open 1', 'open 2'],
      );
    },
  );

  test(
    'a step that throws frees the attempt, and the campaign rejects with that step’s own error even when freeing fails too',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const failure = new Error('The backend is unreachable');
      const election = new ScriptedElection({
        tries: [failure],
        closeFails: new Error('Freeing failed too'),
      });

      // Act
      const result = await settle(election.campaign());

      // Assert
      assert.deepEqual(result, { error: failure });
      assert.deepEqual(election.steps, ['open 1', 'try 1', 'close 1']);
    },
  );

  test(
    'a watch that fails to start gives the won claim up and frees it, and the campaign rejects with the watch’s error, also when giving up and freeing fail',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const failure = new Error('The lease renewal could not start');
      const election = new ScriptedElection({
        watchFails: failure,
        releaseFails: new Error('The release failed'),
        closeFails: new Error('The close failed'),
      });

      // Act
      const result = await settle(election.campaign());

      // Assert
      assert.deepEqual(result, { error: failure });
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'watch 1',
        'release 1',
        'close 1',
      ]);
    },
  );
});

describe('A campaign that its caller stops', () => {
  test(
    'a signal that aborted before the call rejects with its reason, and opens nothing',
    { timeout: 10_000 },
    async () => {
      // Arrange
      const reason = new Error('Stopped before the campaign');
      const election = new ScriptedElection({});

      // Act
      const result = await settle(
        election.campaign({ signal: AbortSignal.abort(reason) }),
      );

      // Assert
      assert.deepEqual(result, { error: reason });
      assert.deepEqual(election.steps, []);
    },
  );

  test(
    'a signal that aborts while the campaign waits for its next try rejects with its reason, and frees the attempt',
    { timeout: 10_000 },
    async (t) => {
      // Arrange
      const stop = new AbortController();
      const reason = new Error('Stopped while waiting');
      // A wait longer than this test may run: only an abort seen during the wait ends it in time.
      const election = new ScriptedElection({ tries: ['busy'] }, 60_000);

      // Act: the first try is busy, so the campaign waits for its next one.
      const campaigning = settle(
        election.campaign({ timeout: 120_000, signal: stop.signal }),
      );
      await waitUntil(
        t,
        () => election.steps.includes('try 1'),
        'The campaign must try the claim before it waits',
      );
      stop.abort(reason);
      const result = await campaigning;

      // Assert
      assert.deepEqual(result, { error: reason });
      assert.deepEqual(election.steps, ['open 1', 'try 1', 'close 1']);
    },
  );

  test(
    'a claim won after the caller left is given up and freed, and the campaign rejects with the signal’s reason, also when giving up and freeing fail',
    { timeout: 10_000 },
    async () => {
      // Arrange: the caller leaves while the claim is being won, and the release and the close fail.
      const stop = new AbortController();
      const reason = new Error('Stopped while the claim was won');
      const election = new ScriptedElection({
        duringTry: () => stop.abort(reason),
        releaseFails: new Error('The release failed'),
        closeFails: new Error('The close failed'),
      });

      // Act
      const result = await settle(election.campaign({ signal: stop.signal }));

      // Assert
      assert.deepEqual(result, { error: reason });
      assert.deepEqual(election.steps, [
        'open 1',
        'try 1',
        'release 1',
        'close 1',
      ]);
    },
  );
});
