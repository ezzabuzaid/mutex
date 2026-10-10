import { addAbortListener, once } from 'node:events';
import { rm } from 'node:fs/promises';
import { type Server, type Socket, createServer } from 'node:net';

import type { Term } from '@zukhruf/election';
import { EpochTokenSource } from '@zukhruf/fencing';

import { welcome } from '../connection/handshake.ts';
import { SocketConnection } from '../connection/socket-connection.ts';
import type { FlightResponse } from '../protocol/flight-protocol.ts';
import { FlightCoordinator } from './flight-coordinator.ts';

export interface FlightServerOptions {
  /** Milliseconds the new term starts no flight, so leaders from the last term can reassert theirs. */
  graceWindow: number;
}

/**
 * Serves a `FlightCoordinator` on a Unix socket for one term. Lease tokens
 * carry the term's epoch, so they outrank every token of earlier terms. A
 * term that is lost may already be another process's, so the server closes
 * itself when the term's signal aborts.
 */
export class FlightServer {
  readonly #server: Server;
  readonly #connections: Set<Socket>;
  readonly #coordinator: FlightCoordinator;
  readonly #term: Term;
  readonly #closeOnLoss: Disposable;

  private constructor(
    server: Server,
    connections: Set<Socket>,
    coordinator: FlightCoordinator,
    term: Term,
  ) {
    this.#server = server;
    this.#connections = connections;
    this.#coordinator = coordinator;
    this.#term = term;
    this.#closeOnLoss = addAbortListener(term.signal, () => void this.close());
  }

  static async start(
    socketPath: string,
    term: Term,
    { graceWindow }: FlightServerOptions,
  ): Promise<FlightServer> {
    // Only the coordinator gets here, so removing a dead coordinator's socket file cannot race another server.
    // Windows removes a named pipe when its process stops, so there is no file to remove.
    if (process.platform !== 'win32') {
      await rm(socketPath, { force: true });
    }
    const coordinator = new FlightCoordinator({
      tokens: new EpochTokenSource(term.epoch),
      graceWindow,
    });
    const connections = new Set<Socket>();
    // Serving others never keeps the coordinator alive: when its own work ends, it exits and another process takes over.
    const server = createServer((socket) => {
      socket.unref();
      connections.add(socket);
      socket.once('close', () => connections.delete(socket));
      // Only a process that speaks this protocol is served, so this coordinator never reads a message it would misread.
      void welcome(socket).then((speaksOurs) => {
        if (!speaksOurs) return;
        coordinator.serve(new SocketConnection<FlightResponse>(socket));
      });
    });
    server.listen(socketPath);
    await once(server, 'listening');
    server.unref();
    const started = new FlightServer(server, connections, coordinator, term);
    // The term may have been lost while the server started.
    if (term.signal.aborted) {
      await started.close();
      term.signal.throwIfAborted();
    }
    return started;
  }

  /**
   * Hands the term over. Ending every connection tells the other processes to
   * elect a successor and reassert their flights during its grace window;
   * `close` alone would wait for clients that never disconnect on their own.
   * Each connection ends only after what was written to it, such as an
   * outcome for a joiner, is sent. The term ends only after the socket is
   * gone: in the other order, this close could remove a successor's socket file.
   * The term resigns clean when the coordinator leaves nothing to recover, so
   * the successor skips its grace window; a lost term records nothing.
   * The first call removes the file at once, so a second caller finds it gone,
   * and it too resolves only once the term ended as the first call ended it.
   */
  async close() {
    this.#closeOnLoss[Symbol.dispose]();
    const closed = this.#server[Symbol.asyncDispose]();
    // Read in the step that ends the connections: each session that ends
    // withdraws its flights, so later there is never a flight in progress.
    // An answer written after this step, such as the `lead` of a run whose
    // token is being minted, never reaches its client, which sends the run
    // again to the successor. So no client knows of a flight this read missed.
    const clean = this.#coordinator.leavesNothingToRecover;
    for (const socket of this.#connections) socket.destroySoon();
    await closed;
    await this.#term.resign({ clean });
  }
}
