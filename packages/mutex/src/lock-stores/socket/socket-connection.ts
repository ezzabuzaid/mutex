import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createInterface } from 'node:readline';
import { promisify } from 'node:util';

import type { Connection, ConnectionEvents } from '../remote/connection.ts';
import { jsonLine } from './json-line.ts';
import { leaveErrorsToClose } from './leave-errors-to-close.ts';

/**
 * Newline-delimited JSON over a stream socket. A stream has no message
 * boundaries (writes arrive merged and split), so each message is one line
 * (see `jsonLine`). A line that is not JSON breaks the framing, so it closes
 * the connection; any JSON value goes to the listener, which checks it.
 */
export class SocketConnection<Outgoing>
  extends EventEmitter<ConnectionEvents>
  implements Connection<Outgoing>
{
  readonly #socket: Socket;
  /** A write after the socket closed reports the error through its callback. */
  readonly #write: (line: string) => Promise<void>;

  constructor(socket: Socket) {
    super();
    this.#socket = socket;
    this.#write = promisify<string, void>(socket.write).bind(socket);
    // readline removes its own 'error' listener from the socket when the
    // interface closes on the peer's FIN, so an error after that (e.g. a reset
    // after the half-close) needs this listener of the socket's own.
    leaveErrorsToClose(socket);
    const lines = createInterface({ input: socket, crlfDelay: Infinity });
    lines.on('line', (line) => {
      try {
        this.emit('message', JSON.parse(line));
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
