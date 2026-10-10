import assert from 'node:assert/strict';
import { addAbortListener } from 'node:events';
import { readFileSync } from 'node:fs';
import { connect } from 'node:net';
import { join } from 'node:path';
import { describe, test } from 'node:test';

import { SqliteElection } from '@zukhruf/election';
import type { FileLock } from '@zukhruf/fs';
import { LeaseLostError } from '@zukhruf/lease';

import { SingleFlight } from '../index.ts';
import { scratchDirectory } from '../testing/scratch-directory.ts';
import { waitUntil } from '../testing/wait-until.ts';
import { FlightServer } from './flight-server.ts';

/**
 * The election of the single flights, with a backend that takes the claim
 * away when `loss` aborts. SQLite never takes a claim from a living leader,
 * so only such a backend can show what a server does with a lost term.
 */
class LosableElection extends SqliteElection {
  readonly #loss: AbortSignal;

  constructor(directory: string, loss: AbortSignal) {
    super({
      directory,
      claimFile: 'flight.lock',
      epochFile: 'flight.epoch',
      pollInterval: 10,
    });
    this.#loss = loss;
  }

  // The parameters are optional because SqliteElection's watch declares none.
  protected override watch(
    _claim?: FileLock,
    lose?: (reason: Error) => void,
  ): Disposable {
    return addAbortListener(this.#loss, () =>
      lose?.(new Error('The backend took the claim away')),
    );
  }
}

/** Whether anything listens on `socketPath`. */
function listening(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect(socketPath);
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    socket.once('error', () => resolve(false));
  });
}

/** The epoch of the last term in `directory`, as its file records it. */
function epochOf(directory: string) {
  try {
    return readFileSync(join(directory, 'flight.epoch'), 'utf8');
  } catch {
    return '';
  }
}

const asText = {
  encode: (value: string) => value,
  decode: (text: string) => text,
};

const onUnixSockets = {
  skip:
    process.platform === 'win32'
      ? 'The server listens on the Unix socket path'
      : false,
};

describe('Flight server', () => {
  test(
    'a flight server started for a term that is already lost rejects with LeaseLostError and leaves nothing listening',
    { ...onUnixSockets, timeout: 10_000 },
    async () => {
      // Arrange
      await using directory = await scratchDirectory();
      const socketPath = join(directory.path, 'flight.sock');
      const loss = new AbortController();
      const term = await new LosableElection(
        directory.path,
        loss.signal,
      ).campaign();
      assert.ok(term, 'The first campaign in an empty directory must win');
      loss.abort();

      // Act
      const starting = FlightServer.start(socketPath, term, { graceWindow: 0 });

      // Assert
      await assert.rejects(starting, LeaseLostError);
      assert.equal(
        await listening(socketPath),
        false,
        'A server for a lost term must not serve',
      );
    },
  );

  test(
    'a flight server whose term is lost closes without close(), and a follower then coordinates at a higher epoch',
    { ...onUnixSockets, timeout: 10_000 },
    async (t) => {
      // Arrange: a server for a term its backend can take away, and a single flight that follows it.
      await using directory = await scratchDirectory();
      const loss = new AbortController();
      const term = await new LosableElection(
        directory.path,
        loss.signal,
      ).campaign();
      assert.ok(term, 'The first campaign in an empty directory must win');
      const server = await FlightServer.start(
        join(directory.path, 'flight.sock'),
        term,
        { graceWindow: 0 },
      );
      try {
        await using flights = new SingleFlight({
          directory: directory.path,
          codec: asText,
          graceWindow: 50,
        });
        await flights.run('warm-up', async () => 'warm');

        // Act
        loss.abort();

        // Assert: the server dropped its follower, which then won the next term.
        await waitUntil(
          t,
          () => epochOf(directory.path) === String(term.epoch + 1n),
          () =>
            `The follower must coordinate once the server of the lost term closes; flight.epoch is ${JSON.stringify(epochOf(directory.path))}`,
        );
        let token = 0n;
        await flights.run('product:42', async (lease) => {
          token = lease.token.value;
          return 'done';
        });
        assert.ok(
          token >> 32n > term.epoch,
          `The new coordinator's token ${token} must carry an epoch above the lost term's ${term.epoch}`,
        );
      } finally {
        await server.close();
      }
    },
  );
});
