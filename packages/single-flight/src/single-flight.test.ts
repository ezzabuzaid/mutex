import assert from 'node:assert/strict';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import fsPromises from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { join } from 'node:path';
import { describe, mock, test } from 'node:test';
import { Worker } from 'node:worker_threads';

import { SqliteElection } from '@zukhruf/election';
import { Mutex, SocketStore } from '@zukhruf/mutex';

import {
  type Codec,
  FlightFailedError,
  NetworkDirectoryError,
  SingleFlight,
} from './index.ts';
import { scratchDirectory } from './testing/scratch-directory.ts';
import { waitUntil } from './testing/wait-until.ts';

/** Values that are text already. */
const asText: Codec<string> = {
  encode: (value) => value,
  decode: (text) => text,
};

/** Settles like `running`, but never rejects, so the rejection handler is attached in the step that starts the call. */
const settle = <T>(running: Promise<T>) =>
  running.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );

/** Work that reports when it starts, and ends when the test says so. */
function controlledWork<T>() {
  const started = Promise.withResolvers<void>();
  const ending = Promise.withResolvers<T>();
  let runs = 0;
  return {
    started: started.promise,
    get runs() {
      return runs;
    },
    finish: ending.resolve,
    fail: ending.reject,
    work: async () => {
      runs++;
      started.resolve();
      return ending.promise;
    },
  };
}

/** Work that a joiner passes: it must never run. */
const neverRuns = async (): Promise<string> => {
  throw new Error('A joiner ran the work');
};

describe('A single flight when the work fails', () => {
  test(
    'the leader rejects with its own error, and every joiner, also one of the leader instance, with FlightFailedError that describes it',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = settle(leading.run('sync', flight.work));
      await flight.started;
      const sameJoined = Promise.withResolvers<void>();
      const otherJoined = Promise.withResolvers<void>();
      const same = settle(
        leading.run('sync', neverRuns, { onJoin: sameJoined.resolve }),
      );
      const elsewhere = settle(
        other.run('sync', neverRuns, { onJoin: otherJoined.resolve }),
      );
      await sameJoined.promise;
      await otherJoined.promise;
      const failure = Object.assign(new Error('The disk is full'), {
        code: 'ENOSPC',
      });

      // Act
      flight.fail(failure);
      const results = await Promise.all([leader, same, elsewhere]);

      // Assert
      const [led, ...joined] = results;
      assert.ok('error' in led);
      assert.equal(led.error, failure);
      for (const result of joined) {
        assert.ok('error' in result);
        assert.ok(
          result.error instanceof FlightFailedError,
          String(result.error),
        );
        assert.equal(result.error.key, 'sync');
        assert.deepEqual(result.error.failure, {
          name: 'Error',
          message: 'The disk is full',
          code: 'ENOSPC',
        });
      }
    },
  );

  test(
    'a thrown value that is not an error reaches a joiner as its text',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = settle(leading.run('sync', flight.work));
      await flight.started;
      const joined = Promise.withResolvers<void>();
      const joiner = settle(
        other.run('sync', neverRuns, { onJoin: joined.resolve }),
      );
      await joined.promise;

      // Act
      flight.fail('a plain text');
      const [led, join] = await Promise.all([leader, joiner]);

      // Assert
      assert.deepEqual(led, { error: 'a plain text' });
      assert.ok('error' in join && join.error instanceof FlightFailedError);
      assert.deepEqual(join.error.failure, {
        name: 'Error',
        message: 'a plain text',
      });
    },
  );

  test(
    'a value that the codec cannot encode fails the flight: the leader rejects with that error, and a joiner gets FlightFailedError',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      const failing: Codec<string> = {
        encode: () => {
          throw new TypeError('This value cannot be encoded');
        },
        decode: (text) => text,
      };
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: failing,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: failing,
      });
      const flight = controlledWork<string>();
      const leader = settle(leading.run('sync', flight.work));
      await flight.started;
      const joined = Promise.withResolvers<void>();
      const joiner = settle(
        other.run('sync', neverRuns, { onJoin: joined.resolve }),
      );
      await joined.promise;

      // Act
      flight.finish('value');
      const [led, join] = await Promise.all([leader, joiner]);

      // Assert
      assert.ok('error' in led && led.error instanceof TypeError);
      assert.ok('error' in join && join.error instanceof FlightFailedError);
      assert.equal(join.error.failure.name, 'TypeError');
    },
  );

  test(
    'a codec that cannot decode fails only the caller that uses it: the leader and the other joiners get the value',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      const failing: Codec<string> = {
        encode: (value) => value,
        decode: () => {
          throw new SyntaxError('This text cannot be decoded');
        },
      };
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using decoding = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using refusing = new SingleFlight({
        directory: directory.path,
        codec: failing,
      });
      const flight = controlledWork<string>();
      const leader = settle(leading.run('sync', flight.work));
      await flight.started;
      const firstJoined = Promise.withResolvers<void>();
      const secondJoined = Promise.withResolvers<void>();
      const decoded = settle(
        decoding.run('sync', neverRuns, { onJoin: firstJoined.resolve }),
      );
      const refused = settle(
        refusing.run('sync', neverRuns, { onJoin: secondJoined.resolve }),
      );
      await firstJoined.promise;
      await secondJoined.promise;

      // Act
      flight.finish('report');
      const results = await Promise.all([leader, decoded, refused]);

      // Assert
      assert.deepEqual(results[0], {
        value: { value: 'report', joined: false },
      });
      assert.deepEqual(results[1], {
        value: { value: 'report', joined: true },
      });
      assert.ok(
        'error' in results[2] && results[2].error instanceof SyntaxError,
      );
    },
  );
});

describe('A single flight when the leader cannot decode', () => {
  test(
    'a leader whose own codec cannot decode lands the value for the joiners, and rejects with the decode error',
    { timeout: 10_000 },
    async () => {
      // Arrange: only the leader's codec fails to decode.
      await using directory = await scratchDirectory();
      const refusing: Codec<string> = {
        encode: (value) => value,
        decode: () => {
          throw new SyntaxError('This text cannot be decoded');
        },
      };
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: refusing,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = settle(leading.run('sync', flight.work));
      await flight.started;
      const joined = Promise.withResolvers<void>();
      const joiner = settle(
        other.run('sync', neverRuns, { onJoin: joined.resolve }),
      );
      await joined.promise;

      // Act
      flight.finish('report');
      const [led, join] = await Promise.all([leader, joiner]);

      // Assert
      assert.ok('error' in led && led.error instanceof SyntaxError);
      assert.deepEqual(join, { value: { value: 'report', joined: true } });
    },
  );
});

describe('A single flight when a caller cancels', () => {
  test(
    'a joiner that cancels rejects with its reason, and the leader and the other joiners still get the value',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = settle(leading.run('sync', flight.work));
      await flight.started;
      const cancel = new AbortController();
      const firstJoined = Promise.withResolvers<void>();
      const secondJoined = Promise.withResolvers<void>();
      const cancelling = settle(
        other.run('sync', neverRuns, {
          signal: cancel.signal,
          onJoin: firstJoined.resolve,
        }),
      );
      const staying = settle(
        other.run('sync', neverRuns, { onJoin: secondJoined.resolve }),
      );
      await firstJoined.promise;
      await secondJoined.promise;
      const reason = new Error('The caller left');

      // Act
      cancel.abort(reason);
      const cancelled = await cancelling;
      flight.finish('report');

      // Assert
      assert.deepEqual(cancelled, { error: reason });
      assert.deepEqual(await staying, {
        value: { value: 'report', joined: true },
      });
      assert.deepEqual(await leader, {
        value: { value: 'report', joined: false },
      });
    },
  );

  test(
    "a leader's caller that cancels rejects with its reason, and the work runs on and lands for the joiners",
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const cancel = new AbortController();
      const leader = settle(
        leading.run('sync', flight.work, { signal: cancel.signal }),
      );
      await flight.started;
      const joined = Promise.withResolvers<void>();
      const joiner = settle(
        other.run('sync', neverRuns, { onJoin: joined.resolve }),
      );
      await joined.promise;
      const reason = new Error('The leader left');

      // Act
      cancel.abort(reason);
      const cancelled = await leader;
      flight.finish('report');

      // Assert
      assert.deepEqual(cancelled, { error: reason });
      assert.deepEqual(await joiner, {
        value: { value: 'report', joined: true },
      });
    },
  );

  test(
    'a caller whose signal aborted already neither leads nor joins',
    { timeout: 10_000 },
    async () => {
      // Arrange: a flight is in progress for one key, and none for another.
      await using directory = await scratchDirectory();
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = settle(flights.run('sync', flight.work));
      await flight.started;
      const reason = new Error('Cancelled before the call');
      let joins = 0;
      const free = controlledWork<string>();

      // Act
      const busy = await settle(
        flights.run('sync', neverRuns, {
          signal: AbortSignal.abort(reason),
          onJoin: () => joins++,
        }),
      );
      const idle = await settle(
        flights.run('other', free.work, { signal: AbortSignal.abort(reason) }),
      );
      // Had the aborted call reached the coordinator, it would lead `other` now, and this call would join it.
      const afterwards = await settle(flights.run('other', async () => 'led'));
      flight.finish('report');

      // Assert
      assert.deepEqual(busy, { error: reason });
      assert.deepEqual(idle, { error: reason });
      assert.equal(joins, 0);
      assert.equal(free.runs, 0);
      assert.deepEqual(afterwards, { value: { value: 'led', joined: false } });
      assert.deepEqual(await leader, {
        value: { value: 'report', joined: false },
      });
    },
  );
});

describe('A single flight shares the run in progress', () => {
  test(
    'concurrent callers of one key share one run: the others get its value as joiners, and each hears onJoin once',
    { timeout: 10_000 },
    async (t) => {
      // Arrange: two callers of one instance, and one of another instance on the same directory.
      await using directory = await scratchDirectory();
      await using first = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using second = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      let joins = 0;
      const onJoin = () => joins++;

      // Act
      const calls = [
        settle(first.run('sync', flight.work, { onJoin })),
        settle(first.run('sync', flight.work, { onJoin })),
        settle(second.run('sync', flight.work, { onJoin })),
      ];
      await flight.started;
      // The joiners hear onJoin before the outcome, so the flight ends once both did.
      await waitUntil(
        t,
        () => joins === 2,
        () => `${joins} callers joined`,
      );
      flight.finish('report');
      const results = await Promise.all(calls);

      // Assert
      assert.equal(flight.runs, 1);
      assert.equal(joins, 2);
      assert.deepEqual(
        results.map((result) =>
          'value' in result ? result.value.value : result.error,
        ),
        ['report', 'report', 'report'],
      );
      assert.deepEqual(
        results
          .map((result) => 'value' in result && result.value.joined)
          .sort(),
        [false, true, true],
      );
    },
  );

  test(
    'a call after a flight ended runs the work again, also after a flight that failed',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      let runs = 0;

      // Act
      const first = await settle(
        flights.run('sync', async () => `run ${++runs}`),
      );
      const failed = await settle(
        flights.run('sync', async () => {
          runs++;
          throw new Error('The run failed');
        }),
      );
      const third = await settle(
        flights.run('sync', async () => `run ${++runs}`),
      );

      // Assert
      assert.deepEqual(first, { value: { value: 'run 1', joined: false } });
      assert.ok('error' in failed);
      assert.deepEqual(third, { value: { value: 'run 3', joined: false } });
    },
  );

  test(
    'different keys fly side by side, and directories never share a flight',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using one = await scratchDirectory();
      await using another = await scratchDirectory();
      await using flights = new SingleFlight({
        directory: one.path,
        codec: asText,
      });
      await using elsewhere = new SingleFlight({
        directory: another.path,
        codec: asText,
      });
      const sync = controlledWork<string>();
      const report = controlledWork<string>();
      const remote = controlledWork<string>();

      // Act: the other directory's call comes while this directory's flight of `sync` is in progress.
      const calls = [
        settle(flights.run('sync', sync.work)),
        settle(flights.run('report', report.work)),
      ];
      await Promise.all([sync.started, report.started]);
      calls.push(settle(elsewhere.run('sync', remote.work)));
      await remote.started;
      sync.finish('sync');
      report.finish('report');
      remote.finish('remote');
      const results = await Promise.all(calls);

      // Assert: each one led its own flight.
      assert.deepEqual(results, [
        { value: { value: 'sync', joined: false } },
        { value: { value: 'report', joined: false } },
        { value: { value: 'remote', joined: false } },
      ]);
    },
  );

  test(
    'a caller in a worker thread joins the flight that the main thread leads',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = settle(flights.run('sync', flight.work));
      await flight.started;
      const index = new URL('./index.ts', import.meta.url);
      const worker = new Worker(
        new URL(
          `data:text/javascript,${encodeURIComponent(`
          import { parentPort } from 'node:worker_threads';
          import { SingleFlight } from ${JSON.stringify(index.href)};
          const flights = new SingleFlight({
            directory: ${JSON.stringify(directory.path)},
            codec: { encode: (value) => value, decode: (text) => text },
          });
          const result = await flights.run(
            'sync',
            async () => 'the worker ran the work',
            { onJoin: () => parentPort.postMessage({ type: 'joined' }) },
          );
          await flights[Symbol.asyncDispose]();
          parentPort.postMessage({ type: 'value', ...result });
        `)}`,
        ),
      );
      try {
        // Act
        const [joined] = await once(worker, 'message');
        flight.finish('the main thread ran the work');
        const [value] = await once(worker, 'message');

        // Assert
        assert.deepEqual(joined, { type: 'joined' });
        assert.deepEqual(value, {
          type: 'value',
          value: 'the main thread ran the work',
          joined: true,
        });
        assert.deepEqual(await leader, {
          value: { value: 'the main thread ran the work', joined: false },
        });
      } finally {
        await worker.terminate();
      }
    },
  );
});

describe('A single flight gives the leader what the joiners get', () => {
  test(
    'the leader and the joiners get the value through the codec: dates come back as dates, and what JSON drops is gone for the leader too',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      interface Report {
        at: Date;
        draft?: string | undefined;
      }
      const dated: Codec<Report> = {
        encode: (value) => JSON.stringify(value),
        decode: (text) => {
          const parsed: unknown = JSON.parse(text);
          assert.ok(
            typeof parsed === 'object' &&
              parsed !== null &&
              'at' in parsed &&
              typeof parsed.at === 'string',
          );
          return { at: new Date(parsed.at) };
        },
      };
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: dated,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: dated,
      });
      const at = new Date('2026-10-09T12:00:00.000Z');
      const flight = controlledWork<Report>();
      const original: Report = { at, draft: undefined };
      const leader = settle(leading.run('report', flight.work));
      await flight.started;
      const joined = Promise.withResolvers<void>();
      const joiner = settle(
        other.run('report', async () => original, { onJoin: joined.resolve }),
      );
      await joined.promise;

      // Act
      flight.finish(original);
      const results = await Promise.all([leader, joiner]);

      // Assert
      for (const result of results) {
        assert.ok('value' in result);
        assert.ok(result.value.value.at instanceof Date);
        assert.equal(result.value.value.at.toISOString(), at.toISOString());
        assert.deepEqual(Object.keys(result.value.value), ['at']);
      }
      assert.ok('value' in results[0]);
      assert.notEqual(results[0].value.value, original);
    },
  );

  test(
    "each flight's lease token carries the coordinator's epoch in its high 32 bits, and grows from flight to flight",
    { timeout: 10_000 },
    async () => {
      // Arrange: a new directory, so the coordinator's term is the first.
      await using directory = await scratchDirectory();
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });

      // Act
      const tokens: bigint[] = [];
      for (const key of ['first', 'second']) {
        await flights.run(key, async ({ token }) => {
          tokens.push(token.value);
          return key;
        });
      }

      // Assert
      const [first, second] = tokens;
      assert.ok(first !== undefined && second !== undefined);
      assert.equal(first >> 32n, 1n);
      assert.equal(second >> 32n, 1n);
      assert.ok(second > first);
    },
  );
});

describe('A single flight in its directory', () => {
  test(
    'the first coordinator of a directory has no grace window: its first call leads at once',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      const graceWindow = 5000;
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: asText,
        graceWindow,
      });
      const started = performance.now();

      // Act
      await flights.run('sync', async () => 'done');
      const took = performance.now() - started;

      // Assert
      assert.ok(
        took < graceWindow / 2,
        `The first call took ${Math.round(took)} ms`,
      );
    },
  );

  test(
    'a coordinator after one that disposed with no flight in progress has no grace window: its first call leads at once',
    { timeout: 10_000 },
    async () => {
      // Arrange: the first coordinator runs once and disposes, as a process that runs alone does.
      await using directory = await scratchDirectory();
      const graceWindow = 5000;
      {
        await using first = new SingleFlight({
          directory: directory.path,
          codec: asText,
          graceWindow,
        });
        await first.run('sync', async () => 'done');
      }
      await using second = new SingleFlight({
        directory: directory.path,
        codec: asText,
        graceWindow,
      });
      const started = performance.now();

      // Act
      await second.run('sync', async () => 'done again');
      const took = performance.now() - started;

      // Assert
      assert.equal(
        await fsPromises.readFile(join(directory.path, 'flight.epoch'), 'utf8'),
        '2',
        'The second call must start the second term',
      );
      assert.ok(
        took < graceWindow / 2,
        `The first call of the second term took ${Math.round(took)} ms`,
      );
    },
  );

  test(
    'a coordinator that disposes inside its own grace window leaves a grace window to the next coordinator',
    { timeout: 10_000 },
    async (t) => {
      // Arrange: the first term resigns without a clean shutdown, as a coordinator of a published version does.
      await using directory = await scratchDirectory();
      const graceWindow = 1000;
      const earlier = await new SqliteElection({
        directory: directory.path,
        claimFile: 'flight.lock',
        epochFile: 'flight.epoch',
        pollInterval: 10,
      }).campaign();
      assert.ok(earlier, 'The first campaign in an empty directory must win');
      await earlier.resign();
      {
        // So it is still in its grace window when it disposes.
        await using inWindow = new SingleFlight({
          directory: directory.path,
          codec: asText,
          graceWindow: 60_000,
        });
        void settle(inWindow.run('sync', async () => 'never'));
        await waitUntil(
          t,
          () => existsSync(join(directory.path, 'flight.sock')),
          'The second term must start serving',
        );
      }
      await using next = new SingleFlight({
        directory: directory.path,
        codec: asText,
        graceWindow,
      });
      const started = performance.now();

      // Act
      await next.run('sync', async () => 'done');
      const took = performance.now() - started;

      // Assert
      assert.equal(
        await fsPromises.readFile(join(directory.path, 'flight.epoch'), 'utf8'),
        '3',
        'The call must start the third term',
      );
      assert.ok(
        took >= graceWindow - 100,
        `The first call of the third term led after ${Math.round(took)} ms, inside a window of ${graceWindow} ms`,
      );
    },
  );

  test(
    'a single flight and a socket lock store of @zukhruf/mutex share a directory without meeting',
    { timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      await using store = new SocketStore(directory.path);
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const mutex = new Mutex(store);

      // Act
      const locked = await settle(mutex.acquire('sync', async () => 'locked'));
      const flown = await settle(flights.run('sync', async () => 'flown'));
      const lockedAgain = await settle(
        mutex.acquire('sync', async () => 'locked again'),
      );

      // Assert
      assert.deepEqual(locked, { value: 'locked' });
      assert.deepEqual(flown, { value: { value: 'flown', joined: false } });
      assert.deepEqual(lockedAgain, { value: 'locked again' });
    },
  );

  test(
    'a directory on a network file system fails the call with NetworkDirectoryError',
    {
      skip:
        process.platform === 'linux'
          ? false
          : 'Only Linux statfs reports a stable file system type',
      timeout: 10_000,
    },
    async () => {
      // Arrange: statfs reports NFS for the directory, as the kernel does for a mount.
      await using directory = await scratchDirectory();
      const realStatfs = fsPromises.statfs;
      mock.method(
        fsPromises,
        'statfs',
        async (path: string, options: { bigint: true }) => {
          const stats = await realStatfs(path, options);
          return path.startsWith(directory.path)
            ? { ...stats, type: 0x6969n }
            : stats;
        },
      );
      syncBuiltinESMExports();
      try {
        await using flights = new SingleFlight({
          directory: directory.path,
          codec: asText,
        });

        // Act
        const result = await settle(flights.run('sync', async () => 'never'));

        // Assert
        assert.ok(
          'error' in result && result.error instanceof NetworkDirectoryError,
          String(result),
        );
        assert.equal(result.error.fileSystem, 'NFS');
      } finally {
        mock.restoreAll();
        syncBuiltinESMExports();
      }
    },
  );
});

describe('A single flight when it is disposed', () => {
  test(
    'a leader that disposes as soon as its call returns still hands the value to its joiners',
    { timeout: 10_000 },
    async () => {
      // Arrange: the leader's instance is the coordinator too, so disposing it also ends the term.
      await using directory = await scratchDirectory();
      await using leading = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      const flight = controlledWork<string>();
      const leader = leading.run('sync', flight.work).then(async (result) => {
        await leading[Symbol.asyncDispose]();
        return result;
      });
      await flight.started;
      const joined = Promise.withResolvers<void>();
      const joiner = settle(
        other.run('sync', neverRuns, { onJoin: joined.resolve }),
      );
      await joined.promise;

      // Act
      flight.finish('report');

      // Assert
      assert.deepEqual(await leader, { value: 'report', joined: false });
      assert.deepEqual(await joiner, {
        value: { value: 'report', joined: true },
      });
    },
  );

  test(
    'a disposed single flight refuses calls, and the other instances elect a new coordinator and go on',
    { timeout: 10_000 },
    async () => {
      // Arrange: the first instance to call is the coordinator.
      await using directory = await scratchDirectory();
      await using coordinating = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await coordinating.run('warm-up', async () => 'warm');
      await using other = new SingleFlight({
        directory: directory.path,
        codec: asText,
      });
      await other.run('warm-up', async () => 'warm');

      // Act
      await coordinating[Symbol.asyncDispose]();
      const refused = await settle(
        coordinating.run('sync', async () => 'never'),
      );
      const carriedOn = await settle(
        other.run('sync', async () => 'carried on'),
      );

      // Assert
      assert.ok('error' in refused && refused.error instanceof Error);
      assert.equal(refused.error.message, 'This single flight is closed.');
      assert.deepEqual(carriedOn, {
        value: { value: 'carried on', joined: false },
      });
    },
  );
});
