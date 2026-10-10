import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createInterface } from 'node:readline';
import { promisify } from 'node:util';

import type { Connection, ConnectionEvents } from './connection.ts';
import { jsonLine } from './json-line.ts';
import { leaveErrorsToClose } from './leave-errors-to-close.ts';

/**
 * Newline-delimited JSON over a stream socket. A stream has no message
 * boundaries (writes arrive merged and split), so each message is one line
 * (see `jsonLine`). `isIncoming` checks each message from the peer; a line
 * that is not one closes the connection.
 */
export class SocketConnection<Outgoing, Incoming>
  extends EventEmitter<ConnectionEvents<Incoming>>
  implements Connection<Outgoing, Incoming>
{
  readonly #socket: Socket;
  /** A write after the socket closed reports the error through its callback. */
  readonly #write: (line: string) => Promise<void>;

  constructor(
    socket: Socket,
    isIncoming: (message: unknown) => message is Incoming,
  ) {
    super();
    this.#socket = socket;
    this.#write = promisify<string, void>(socket.write).bind(socket);
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
    return this.#write(jsonLine(message));
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
