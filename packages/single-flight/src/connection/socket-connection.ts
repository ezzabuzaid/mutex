import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createInterface } from 'node:readline';

import type { Connection, ConnectionEvents } from './connection.ts';
import { leaveErrorsToClose } from './leave-errors-to-close.ts';

/**
 * Newline-delimited JSON over a stream socket. A stream has no message
 * boundaries (writes arrive merged and split), and JSON escapes newlines inside
 * strings, so one line is always one message. `isIncoming` checks each message
 * from the peer; a line that is not one closes the connection.
 */
export class SocketConnection<Outgoing, Incoming>
  extends EventEmitter<ConnectionEvents<Incoming>>
  implements Connection<Outgoing, Incoming>
{
  readonly #socket: Socket;

  constructor(
    socket: Socket,
    isIncoming: (message: unknown) => message is Incoming,
  ) {
    super();
    this.#socket = socket;
    leaveErrorsToClose(socket);
    const lines = createInterface({ input: socket, crlfDelay: Infinity });
    lines.on('line', (line) => {
      try {
        const parsed: unknown = JSON.parse(line);
        if (isIncoming(parsed)) this.emit('message', parsed);
        else this.close();
      } catch {
        this.close();
      }
    });
    // readline re-emits the socket's errors (e.g. EPIPE when the peer died).
    leaveErrorsToClose(lines);
    socket.once('close', () => this.emit('close'));
  }

  send(message: Outgoing): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.#socket.writable) {
        reject(new Error('The socket is closed.'));
        return;
      }
      this.#socket.write(`${JSON.stringify(message)}\n`, (error) =>
        error ? reject(error) : resolve(),
      );
    });
  }

  ref() {
    this.#socket.ref();
  }

  unref() {
    this.#socket.unref();
  }

  close() {
    this.#socket.destroy();
  }
}
