import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { once } from 'node:events';
import { Socket, createServer } from 'node:net';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { describe, test } from 'node:test';

import type { CampaignOptions, Term } from '@zukhruf/election';

import { isRecord } from '../shared/is-record.ts';
import { scratchDirectory } from '../testing/scratch-directory.ts';
import { ElectingConnector } from './electing-connector.ts';
import { flightElection } from './flight-election.ts';

describe('Electing connector', () => {
  test('a connect aborted while it campaigns resigns the term it wins and never serves', async () => {
    // Arrange: nobody serves the directory, and the abort lands while the campaign wins.
    await using directory = await scratchDirectory();
    const election = flightElection(directory.path, 10);
    const abort = new AbortController();
    const served: Term[] = [];
    const connector = new ElectingConnector({
      socketPath: join(directory.path, 'flight.sock'),
      election: {
        campaign: async (options?: CampaignOptions) => {
          const won = await election.campaign(options);
          abort.abort();
          return won;
        },
      },
      pollInterval: 10,
      serve: async (term) => {
        served.push(term);
      },
    });

    // Act
    const connecting = connector.connect(abort.signal);

    // Assert
    await assert.rejects(connecting, { name: 'AbortError' });
    assert.deepEqual(served, [], 'An aborted connect must not start serving');
    await using next = await flightElection(directory.path, 10).campaign();
    assert.ok(next, 'An aborted connect must resign the term it won');
  });

  test('a connect aborted before it starts never campaigns', async () => {
    // Arrange
    await using directory = await scratchDirectory();
    let campaigns = 0;
    const connector = new ElectingConnector({
      socketPath: join(directory.path, 'flight.sock'),
      election: {
        campaign: async () => {
          campaigns++;
          return undefined;
        },
      },
      pollInterval: 10,
      serve: async () => {},
    });

    // Act
    const connecting = connector.connect(AbortSignal.abort());

    // Assert
    await assert.rejects(connecting, { name: 'AbortError' });
    assert.equal(campaigns, 0, 'An aborted connect must not campaign');
  });

  // A wait that ignores the abort lasts the 60 s poll interval, and it must
  // fail the test, not hang it.
  test(
    'a connect aborted while it waits for its next try rejects with the reason of the signal',
    { timeout: 5000 },
    async () => {
      // Arrange: nobody serves the socket, and no campaign wins. The abort lands
      // after the lost campaign returned, so the connect waits for its next try.
      await using directory = await scratchDirectory();
      const abort = new AbortController();
      const reason = new Error('The single flight closed');
      const connector = new ElectingConnector({
        socketPath: join(directory.path, 'flight.sock'),
        election: {
          campaign: async () => {
            setImmediate(() => abort.abort(reason));
            return undefined;
          },
        },
        pollInterval: 60_000,
        serve: async () => {},
      });

      // Act
      const error = await connector.connect(abort.signal).then(
        () => assert.fail('An aborted connect must reject'),
        (error: unknown) => error,
      );

      // Assert
      assert.equal(error, reason);
    },
  );

  // A connector that opens another number of sockets before it waits never
  // gets the abort, and its 60 s wait must fail the test, not hang it.
  test(
    'a connect aborted while it outlasts a coordinator that hangs up rejects with the reason of the signal',
    { timeout: 5000 },
    async () => {
      // Arrange: a coordinator hangs up on each hello, and no campaign wins.
      await using directory = await scratchDirectory();
      const socketPath =
        process.platform === 'win32'
          ? `\\\\.\\pipe\\single-flight-test-${randomUUID()}`
          : join(directory.path, 'flight.sock');
      const peers = new Set<Socket>();
      const coordinator = createServer((peer) => {
        peers.add(peer);
        peer.on('error', () => {});
        createInterface({ input: peer }).once('line', () => peer.destroy());
      });
      coordinator.listen(socketPath);
      await once(coordinator, 'listening');
      const abort = new AbortController();
      const reason = new Error('The single flight closed');
      // The first hang-up sends the connect to outlast the coordinator. After
      // the second hang-up, it waits for its next try: the abort lands in that
      // wait, one turn after the socket closes.
      let reached = 0;
      const onSocket = (message: unknown) => {
        if (!isRecord(message) || !(message.socket instanceof Socket)) return;
        if (++reached !== 2) return;
        message.socket.once('close', () =>
          setImmediate(() => abort.abort(reason)),
        );
      };
      subscribe('net.client.socket', onSocket);
      const connector = new ElectingConnector({
        socketPath,
        election: { campaign: async () => undefined },
        pollInterval: 60_000,
        serve: async () => {},
      });

      try {
        // Act
        const error = await connector.connect(abort.signal).then(
          () => assert.fail('An aborted connect must reject'),
          (error: unknown) => error,
        );

        // Assert
        assert.equal(error, reason);
        assert.equal(
          reached,
          2,
          'The connect must reach the coordinator two times',
        );
      } finally {
        unsubscribe('net.client.socket', onSocket);
        for (const peer of peers) peer.destroy();
        await new Promise<void>((resolve) =>
          coordinator.close(() => resolve()),
        );
      }
    },
  );
});
