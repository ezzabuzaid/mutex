import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { describe, test } from 'node:test';
import type { TestContext } from 'node:test';

import { SingleFlight } from './index.ts';
import {
  type CallerOptions,
  callerSource,
  journalOf,
} from './testing/caller.ts';
import { scratchDirectory } from './testing/scratch-directory.ts';
import { newProcessTimeout, waitUntil } from './testing/wait-until.ts';
import { type WorkerProcess, startWorker } from './testing/worker-process.ts';

const onUnix = {
  skip:
    process.platform === 'win32'
      ? 'Stopping and resuming a process needs POSIX signals'
      : false,
};

/** A caller process on `directory`, ready for orders. */
async function caller(
  t: TestContext,
  directory: string,
  name: string,
  options?: CallerOptions,
) {
  const worker = startWorker(callerSource(directory, options), name);
  await waitUntil(
    t,
    () => worker.has('ready'),
    () => worker.stderr,
    newProcessTimeout,
  );
  return worker;
}

/**
 * The coordinator, by construction: the first process that makes a call while
 * no other process uses the directory wins the first term.
 */
async function coordinatorOf(
  t: TestContext,
  directory: string,
  options?: CallerOptions,
) {
  const coordinator = await caller(t, directory, 'coordinator', options);
  coordinator.child.send({ type: 'run', call: 'warm-up', key: 'warm-up' });
  await heard(t, coordinator, 'leading', 'warm-up');
  coordinator.child.send({ type: 'finish', call: 'warm-up', value: 'warm' });
  await heard(t, coordinator, 'value', 'warm-up');
  return coordinator;
}

/** Waits until `worker` reported `type` for `call`, and returns that report. */
async function heard(
  t: TestContext,
  worker: WorkerProcess,
  type: string,
  call: string,
  timeout = newProcessTimeout,
) {
  const find = () =>
    worker.messages.find(
      (message) => message.type === type && message.call === call,
    );
  await waitUntil(
    t,
    () => find() !== undefined,
    () =>
      `No ${type} for ${call}; got ${JSON.stringify(worker.messages)} ${worker.stderr}`,
    timeout,
  );
  const message = find();
  assert.ok(message);
  return message;
}

/** Whether the call `call` of `worker` leads by now. */
function led(worker: WorkerProcess, call: string) {
  return worker.messages.some(
    (message) => message.type === 'leading' && message.call === call,
  );
}

/** Runs `key` in `worker` as the call `call`. */
function run(worker: WorkerProcess, call: string, key: string) {
  worker.child.send({ type: 'run', call, key });
}

/** The runs of the work, as `<process>:work:<key>`, in order. */
async function journal(directory: string) {
  const text = await readFile(journalOf(directory), 'utf8');
  return text.split('\n').filter(Boolean);
}

async function epochOf(directory: string) {
  return readFile(join(directory, 'flight.epoch'), 'utf8').catch(() => '');
}

describe('A single flight across processes when a leader stops', () => {
  test(
    'a joiner whose leader process stops gets FlightInterruptedError at once, and its next call leads a new flight',
    { timeout: 30_000 },
    async (t) => {
      // Arrange: the coordinator, a leader in its own process, and a joiner in another.
      await using directory = await scratchDirectory();
      await using _coordinator = await coordinatorOf(t, directory.path);
      await using leader = await caller(t, directory.path, 'leader');
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner');
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');

      // Act
      leader.child.kill('SIGKILL');
      await leader.closed;
      // The coordinator lives, so it learns of the stop from the closed connection at once, not from a timeout.
      const interrupted = await heard(t, joiner, 'error', 'j', 1000);
      run(joiner, 'next', 'sync');
      await heard(t, joiner, 'leading', 'next');
      joiner.child.send({ type: 'finish', call: 'next', value: 'second' });
      const next = await heard(t, joiner, 'value', 'next');

      // Assert
      assert.equal(interrupted.name, 'FlightInterruptedError');
      assert.deepEqual(next.value, 'second');
      assert.equal(next.joined, false);
      assert.deepEqual(await journal(directory.path), [
        'coordinator:work:warm-up',
        'leader:work:sync',
        'joiner:work:sync',
      ]);
    },
  );

  test(
    'joiners of a leader that was also the coordinator get FlightInterruptedError from the next coordinator, and nobody runs the work again',
    { timeout: 30_000 },
    async (t) => {
      // Arrange: the coordinator leads the flight itself, and two processes join it.
      await using directory = await scratchDirectory();
      await using coordinator = await caller(t, directory.path, 'coordinator', {
        graceWindow: 300,
      });
      run(coordinator, 'l', 'sync');
      await heard(t, coordinator, 'leading', 'l');
      await using first = await caller(t, directory.path, 'first', {
        graceWindow: 300,
      });
      run(first, 'j', 'sync');
      await heard(t, first, 'joined', 'j');
      await using second = await caller(t, directory.path, 'second', {
        graceWindow: 300,
      });
      run(second, 'j', 'sync');
      await heard(t, second, 'joined', 'j');

      // Act: a joiner's process takes over, and waits for a reassertion that never comes.
      coordinator.child.kill('SIGKILL');
      await coordinator.closed;
      const firstEnd = await heard(t, first, 'error', 'j');
      const secondEnd = await heard(t, second, 'error', 'j');

      // Assert
      assert.equal(firstEnd.name, 'FlightInterruptedError');
      assert.equal(secondEnd.name, 'FlightInterruptedError');
      assert.equal(await epochOf(directory.path), '2');
      assert.deepEqual(await journal(directory.path), [
        'coordinator:work:sync',
      ]);
    },
  );
});

/** Waits until a new coordinator won its term: it records the term's epoch before it serves. */
async function newTerm(t: TestContext, directory: string, epoch: string) {
  await t.waitFor(async () => assert.equal(await epochOf(directory), epoch), {
    interval: 5,
    timeout: newProcessTimeout,
  });
}

/**
 * Stops `coordinator`, and makes `next` its successor by construction: the
 * other processes are frozen until `next`, the only one left to campaign,
 * has won the term. `next` must have a connection already, so it notices the
 * loss and campaigns at once.
 */
async function handOver(
  t: TestContext,
  directory: string,
  coordinator: WorkerProcess,
  frozen: WorkerProcess[],
) {
  for (const worker of frozen) worker.child.kill('SIGSTOP');
  try {
    coordinator.child.kill('SIGKILL');
    await coordinator.closed;
    await newTerm(t, directory, '2');
  } finally {
    for (const worker of frozen) worker.child.kill('SIGCONT');
  }
}

/** A process with an open connection, ready to campaign when the coordinator stops. */
async function connected(
  t: TestContext,
  directory: string,
  name: string,
  options?: CallerOptions,
) {
  const worker = await caller(t, directory, name, options);
  await windowEnd(t, worker, `${name}-warm-up`);
  return worker;
}

/**
 * A fresh run of a key nobody else uses leads only once the coordinator's
 * grace window is over, so its `leading` report is the end of the window.
 */
async function windowEnd(t: TestContext, worker: WorkerProcess, call: string) {
  run(worker, call, call);
  await heard(t, worker, 'leading', call);
  worker.child.send({ type: 'finish', call, value: call });
  await heard(t, worker, 'value', call);
}

describe('A single flight across processes when the coordinator stops', () => {
  test(
    'a leader in another process keeps its flight, and every joiner gets its value',
    { timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using probe = await connected(t, directory.path, 'probe', options);

      // Act: the probe takes over, and the leader lands after the probe's grace window.
      await handOver(t, directory.path, coordinator, [leader, joiner]);
      await windowEnd(t, probe, 'window');
      leader.child.send({ type: 'finish', call: 'l', value: { files: 42 } });
      const led = await heard(t, leader, 'value', 'l');
      const joined = await heard(t, joiner, 'value', 'j');

      // Assert
      assert.deepEqual(led.value, { files: 42 });
      assert.deepEqual(joined.value, { files: 42 });
      assert.equal(joined.joined, true);
      assert.deepEqual(
        (await journal(directory.path)).filter((line) =>
          line.endsWith(':sync'),
        ),
        ['leader:work:sync'],
      );
    },
  );

  test(
    'a leader that lands during the next coordinator grace window gets its value to the joiners before the window ends',
    { timeout: 30_000 },
    async (t) => {
      // Arrange: a long window. The leader leads two flights, and the joiner joins both.
      await using directory = await scratchDirectory();
      const options = { graceWindow: 3000 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      run(leader, 'o', 'other');
      await heard(t, leader, 'leading', 'o');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      run(joiner, 'k', 'other');
      await heard(t, joiner, 'joined', 'k');
      await using probe = await connected(t, directory.path, 'probe', options);
      await handOver(t, directory.path, coordinator, [leader, joiner]);
      run(probe, 'window', 'window');
      // The landing of `other` goes out behind the leader's reassertions, so
      // once it reached the joiner, the probe holds the flight of `sync` again.
      leader.child.send({ type: 'finish', call: 'o', value: 'other' });
      await heard(t, joiner, 'value', 'k');

      // Act: the leader lands its reasserted flight inside the probe's window.
      leader.child.send({ type: 'finish', call: 'l', value: 'landed' });
      const joined = await heard(t, joiner, 'value', 'j');
      const windowOpen = !led(probe, 'window');

      // Assert
      assert.equal(joined.value, 'landed');
      assert.ok(
        windowOpen,
        'The joiner got the value only after the window ended',
      );
    },
  );

  test(
    'joiners of a flight whose leader stops during the next coordinator grace window get FlightInterruptedError before the window ends',
    { timeout: 30_000 },
    async (t) => {
      // Arrange: the leader leads two flights, and the joiner joins both.
      await using directory = await scratchDirectory();
      const options = { graceWindow: 3000 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      run(leader, 'o', 'other');
      await heard(t, leader, 'leading', 'o');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      run(joiner, 'k', 'other');
      await heard(t, joiner, 'joined', 'k');
      await using probe = await connected(t, directory.path, 'probe', options);

      // Act: the probe takes over, the leader reasserts its flights to it, and then the leader stops.
      await handOver(t, directory.path, coordinator, [leader, joiner]);
      run(probe, 'window', 'window');
      // The landing of `other` goes out behind the leader's reassertions, so
      // once it reached the joiner, the next coordinator holds both flights.
      leader.child.send({ type: 'finish', call: 'o', value: 'other' });
      await heard(t, joiner, 'value', 'k');
      leader.child.kill('SIGKILL');
      await leader.closed;
      const ended = await heard(t, joiner, 'error', 'j');
      const windowOpen = !led(probe, 'window');

      // Assert
      assert.equal(ended.name, 'FlightInterruptedError');
      assert.ok(
        windowOpen,
        'The joiner learned it only after the window ended',
      );
    },
  );
});

describe('A single flight across processes when the coordinator disposes', () => {
  test(
    'a coordinator that disposes while another process leads a flight leaves a grace window to the next coordinator, so the leader keeps its flight and the joiner gets its value',
    { timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');

      // Act: the coordinator disposes, and the leader lands at the next coordinator.
      coordinator.child.send({ type: 'dispose' });
      await waitUntil(
        t,
        () => coordinator.has('disposed'),
        () => coordinator.stderr,
        newProcessTimeout,
      );
      await newTerm(t, directory.path, '2');
      leader.child.send({ type: 'finish', call: 'l', value: { files: 42 } });
      const led = await heard(t, leader, 'value', 'l');
      const joined = await heard(t, joiner, 'value', 'j');

      // Assert
      assert.deepEqual(led.value, { files: 42 });
      assert.deepEqual(joined.value, { files: 42 });
      assert.equal(joined.joined, true);
      assert.equal(leader.has('lost'), false, 'The leader lost its lease');
      assert.deepEqual(
        (await journal(directory.path)).filter((line) =>
          line.endsWith(':sync'),
        ),
        ['leader:work:sync'],
      );
    },
  );

  test(
    'a coordinator that stops right after a landing still delivers it to a joiner that reads slowly',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange: this process coordinates and leads, and the joiner's process will not read for a while.
      await using directory = await scratchDirectory();
      await using flights = new SingleFlight({
        directory: directory.path,
        codec: {
          encode: (value: string) => JSON.stringify(value),
          decode: (text) => {
            const value: unknown = JSON.parse(text);
            assert.equal(typeof value, 'string');
            return String(value);
          },
        },
      });
      await flights.run('warm-up', async () => 'warm');
      const started = Promise.withResolvers<void>();
      const ending = Promise.withResolvers<string>();
      const leading = flights.run('sync', async () => {
        started.resolve();
        return ending.promise;
      });
      await started.promise;
      await using joiner = await caller(t, directory.path, 'joiner');
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      // Larger than a socket buffer, so most of the landing waits in this process while the joiner is frozen.
      const report = 'x'.repeat(2 * 1024 * 1024);

      // Act: the leader lands, and its process stops coordinating at once.
      joiner.child.kill('SIGSTOP');
      let disposing: Promise<void> | undefined;
      try {
        ending.resolve(report);
        await leading;
        disposing = flights[Symbol.asyncDispose]();
        // The server removes its socket file in the step that closes it and ends each connection.
        await t.waitFor(
          () =>
            assert.equal(
              existsSync(join(directory.path, 'flight.sock')),
              false,
            ),
          { interval: 5, timeout: newProcessTimeout },
        );
      } finally {
        joiner.child.kill('SIGCONT');
      }
      await disposing;
      const joined = await heard(t, joiner, 'value', 'j');

      // Assert
      assert.equal(typeof joined.value, 'string');
      assert.equal(String(joined.value).length, report.length);
    },
  );
});

describe('A single flight across processes in the gaps of a failover', () => {
  test(
    'a call that is on its way to a coordinator when the coordinator stops leads or joins at the next one, and is never interrupted',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange: the coordinator is frozen, so it never answers the call.
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using next = await connected(t, directory.path, 'next', options);
      coordinator.child.kill('SIGSTOP');
      run(next, 'fresh', 'sync');
      await heard(t, next, 'called', 'fresh');

      // Act: the call joined nothing, so the next coordinator takes it as a new call.
      coordinator.child.kill('SIGKILL');
      await coordinator.closed;
      await heard(t, next, 'leading', 'fresh');
      next.child.send({ type: 'finish', call: 'fresh', value: 'led' });
      const result = await heard(t, next, 'value', 'fresh');

      // Assert
      assert.equal(result.value, 'led');
      assert.equal(result.joined, false);
    },
  );

  test(
    'a leader that lands while it has no coordinator keeps its landing, and the joiners get it after the leader reasserts',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 3000 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using _probe = await connected(t, directory.path, 'probe', options);

      // Act: the work ends while the leader's coordinator is gone. Whichever the
      // leader hears first, the end of its work or the lost connection, its
      // first landing cannot reach a coordinator.
      leader.child.kill('SIGSTOP');
      joiner.child.kill('SIGSTOP');
      try {
        coordinator.child.kill('SIGKILL');
        await coordinator.closed;
        await newTerm(t, directory.path, '2');
        leader.child.send({
          type: 'finish',
          call: 'l',
          value: 'landed in the gap',
        });
      } finally {
        joiner.child.kill('SIGCONT');
        leader.child.kill('SIGCONT');
      }
      const led = await heard(t, leader, 'value', 'l');
      const joined = await heard(t, joiner, 'value', 'j');

      // Assert
      assert.equal(led.value, 'landed in the gap');
      assert.equal(joined.value, 'landed in the gap');
      assert.deepEqual(
        (await journal(directory.path)).filter((line) =>
          line.endsWith(':sync'),
        ),
        ['leader:work:sync'],
      );
    },
  );

  test(
    'a fresh run during the grace window waits for it, and then joins the flight that its leader reasserted meanwhile',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange: the leader is frozen, so its flight is not reasserted when the fresh run arrives.
      await using directory = await scratchDirectory();
      const options = { graceWindow: 3000 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using probe = await connected(t, directory.path, 'probe', options);
      leader.child.kill('SIGSTOP');
      try {
        coordinator.child.kill('SIGKILL');
        await coordinator.closed;
        await newTerm(t, directory.path, '2');

        // Act
        run(probe, 'fresh', 'sync');
      } finally {
        leader.child.kill('SIGCONT');
      }
      await heard(t, probe, 'joined', 'fresh');
      leader.child.send({ type: 'finish', call: 'l', value: 'once' });
      const fresh = await heard(t, probe, 'value', 'fresh');

      // Assert: the fresh run never led beside the frozen leader.
      assert.equal(fresh.value, 'once');
      assert.equal(fresh.joined, true);
      assert.deepEqual(
        (await journal(directory.path)).filter((line) =>
          line.endsWith(':sync'),
        ),
        ['leader:work:sync'],
      );
    },
  );

  test(
    'a fresh run during the grace window of a key that nobody reasserts leads only once the window is over',
    { timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const graceWindow = 1500;
      await using coordinator = await coordinatorOf(t, directory.path, {
        graceWindow,
      });
      await using probe = await connected(t, directory.path, 'probe', {
        graceWindow,
      });

      // Act
      coordinator.child.kill('SIGKILL');
      await coordinator.closed;
      await newTerm(t, directory.path, '2');
      // The window starts after the epoch is on disk, so it ends no earlier than this.
      const termSeen = performance.now();
      run(probe, 'fresh', 'sync');
      await heard(t, probe, 'leading', 'fresh');
      const waited = performance.now() - termSeen;

      // Assert
      assert.ok(
        waited >= graceWindow - 100,
        `The fresh run led after ${Math.round(waited)} ms, inside a window of ${graceWindow} ms`,
      );
    },
  );

  test(
    'a fresh run during the grace window after the reasserted flight landed leads a new flight, and never gets the old value',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 3000 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using probe = await connected(t, directory.path, 'probe', options);
      await handOver(t, directory.path, coordinator, [leader, joiner]);
      leader.child.send({ type: 'finish', call: 'l', value: 'first' });
      await heard(t, joiner, 'value', 'j');

      // Act: the first flight ended inside the window; this run comes after it.
      run(probe, 'fresh', 'sync');
      await heard(t, probe, 'leading', 'fresh');
      probe.child.send({ type: 'finish', call: 'fresh', value: 'second' });
      const fresh = await heard(t, probe, 'value', 'fresh');

      // Assert
      assert.equal(fresh.value, 'second');
      assert.equal(fresh.joined, false);
      assert.deepEqual(
        (await journal(directory.path)).filter((line) =>
          line.endsWith(':sync'),
        ),
        ['leader:work:sync', 'probe:work:sync'],
      );
    },
  );

  test(
    'a leader that misses the grace window loses its lease with LeaseLostError, and its joiners get FlightInterruptedError',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using probe = await connected(t, directory.path, 'probe', options);

      // Act: the leader stays frozen until the probe's window is over.
      leader.child.kill('SIGSTOP');
      try {
        await handOver(t, directory.path, coordinator, [joiner]);
        await windowEnd(t, probe, 'window');
      } finally {
        leader.child.kill('SIGCONT');
      }
      const lost = await heard(t, leader, 'lost', 'l');
      leader.child.send({ type: 'finish', call: 'l', value: 'stale' });
      const led = await heard(t, leader, 'error', 'l');
      const joined = await heard(t, joiner, 'error', 'j');

      // Assert
      assert.equal(lost.reason, 'LeaseLostError');
      assert.equal(led.name, 'LeaseLostError');
      assert.equal(joined.name, 'FlightInterruptedError');
    },
  );

  test(
    'a leader whose work throws after its lease is lost rejects with LeaseLostError that carries the error, and its joiners get FlightInterruptedError',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using probe = await connected(t, directory.path, 'probe', options);
      leader.child.kill('SIGSTOP');
      try {
        await handOver(t, directory.path, coordinator, [joiner]);
        await windowEnd(t, probe, 'window');
      } finally {
        leader.child.kill('SIGCONT');
      }
      await heard(t, leader, 'lost', 'l');

      // Act: the work fails with its own error, not with the reason of the lost lease.
      leader.child.send({
        type: 'fail',
        call: 'l',
        message: 'The disk is full',
        code: 'ENOSPC',
      });
      const led = await heard(t, leader, 'error', 'l');
      const joined = await heard(t, joiner, 'error', 'j');

      // Assert
      assert.equal(led.name, 'LeaseLostError');
      assert.equal(led.subject, 'sync');
      assert.deepEqual(led.cause, {
        message: 'The disk is full',
        code: 'ENOSPC',
      });
      assert.equal(joined.name, 'FlightInterruptedError');
    },
  );

  test(
    'a leader whose work throws the reason of its lost lease rejects with that LeaseLostError, not with a second one around it',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using probe = await connected(t, directory.path, 'probe', options);
      leader.child.kill('SIGSTOP');
      try {
        await handOver(t, directory.path, coordinator, [joiner]);
        await windowEnd(t, probe, 'window');
      } finally {
        leader.child.kill('SIGCONT');
      }
      await heard(t, leader, 'lost', 'l');

      // Act: the work stops with signal.throwIfAborted().
      leader.child.send({ type: 'throw-reason', call: 'l' });
      const led = await heard(t, leader, 'error', 'l');

      // Assert: the reason of the signal has no cause; a second LeaseLostError would carry the first as its cause.
      assert.equal(led.name, 'LeaseLostError');
      assert.equal(led.subject, 'sync');
      assert.equal(led.cause, undefined);
    },
  );

  test(
    'a leader whose codec cannot encode the value after its lease is lost rejects with LeaseLostError that carries the codec error',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange
      await using directory = await scratchDirectory();
      const options = { graceWindow: 300 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', {
        ...options,
        codec: 'encode-throws',
      });
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      await using probe = await connected(t, directory.path, 'probe', options);
      leader.child.kill('SIGSTOP');
      try {
        await handOver(t, directory.path, coordinator, [joiner]);
        await windowEnd(t, probe, 'window');
      } finally {
        leader.child.kill('SIGCONT');
      }
      await heard(t, leader, 'lost', 'l');

      // Act
      leader.child.send({ type: 'finish', call: 'l', value: 'stale' });
      const led = await heard(t, leader, 'error', 'l');

      // Assert
      assert.equal(led.name, 'LeaseLostError');
      assert.equal(led.subject, 'sync');
      assert.deepEqual(led.cause, { message: 'This value cannot be encoded' });
    },
  );

  test(
    'when the next coordinator stops too, inside its grace window, the flight survives both, and the joiner gets its value',
    { ...onUnix, timeout: 30_000 },
    async (t) => {
      // Arrange: the leader leads two flights, and the joiner joins both.
      await using directory = await scratchDirectory();
      const options = { graceWindow: 3000 };
      await using coordinator = await coordinatorOf(t, directory.path, options);
      await using leader = await caller(t, directory.path, 'leader', options);
      run(leader, 'l', 'sync');
      await heard(t, leader, 'leading', 'l');
      run(leader, 'o', 'other');
      await heard(t, leader, 'leading', 'o');
      await using joiner = await caller(t, directory.path, 'joiner', options);
      run(joiner, 'j', 'sync');
      await heard(t, joiner, 'joined', 'j');
      run(joiner, 'k', 'other');
      await heard(t, joiner, 'joined', 'k');
      await using probe = await connected(t, directory.path, 'probe', options);
      await handOver(t, directory.path, coordinator, [leader, joiner]);
      // Both processes re-sent their flights to the probe before the landing of `other` reached the joiner.
      leader.child.send({ type: 'finish', call: 'o', value: 'other' });
      await heard(t, joiner, 'value', 'k');

      // Act: the probe stops inside its window, and a third term begins.
      probe.child.kill('SIGKILL');
      await probe.closed;
      await newTerm(t, directory.path, '3');
      leader.child.send({ type: 'finish', call: 'l', value: 'survived' });
      const joined = await heard(t, joiner, 'value', 'j');

      // Assert
      assert.equal(joined.value, 'survived');
      assert.deepEqual(
        (await journal(directory.path)).filter((line) =>
          line.endsWith(':sync'),
        ),
        ['leader:work:sync'],
      );
    },
  );
});
