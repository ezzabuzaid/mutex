import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { type Socket, connect, createServer } from 'node:net';
import { join } from 'node:path';
import { describe, test } from 'node:test';

import { scratchDirectory } from '../testing/scratch-directory.ts';
import { waitUntil } from '../testing/wait-until.ts';
import { SocketConnection } from './socket-connection.ts';

describe('Socket connection', () => {
  test('a peer that dies while this side writes is reported as closed, not thrown', async (t) => {
    // Arrange: a connection whose peer accepts and then destroys its end at once.
    await using directory = await scratchDirectory();
    const socketPath =
      process.platform === 'win32'
        ? `\\\\.\\pipe\\single-flight-test-${randomUUID()}`
        : join(directory.path, 'peer.sock');
    const peers: Socket[] = [];
    const server = createServer((peer) => {
      peers.push(peer);
      peer.destroy();
    });
    server.listen(socketPath);
    await once(server, 'listening');
    const socket = connect(socketPath);
    await once(socket, 'connect');
    const connection = new SocketConnection<{ n: number }, unknown>(
      socket,
      (message): message is unknown => true,
    );
    let closed = false;
    connection.once('close', () => (closed = true));

    try {
      // Act: keep writing until the dead peer makes a write fail.
      for (let n = 0; n < 1000 && !closed; n++) {
        await connection.send({ n }).catch(() => {});
      }

      // Assert: the loss arrives as `close`, and the process did not crash.
      await waitUntil(
        t,
        () => closed,
        'The connection must report the dead peer as closed',
      );
      assert.equal(closed, true);
    } finally {
      socket.destroy();
      server.close();
    }
  });
});
