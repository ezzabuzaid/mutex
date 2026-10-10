import { type Socket, connect } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';

import { untilAborted } from '@zukhruf/async';
import type { LeaderElection, Term } from '@zukhruf/election';

import {
  type FlightRequest,
  type FlightResponse,
  isFlightResponse,
} from '../protocol/flight-protocol.ts';
import type { FlightConnection, FlightConnector } from './connector.ts';
import { type Greeting, PROTOCOL_VERSION, greet } from './handshake.ts';
import { ProtocolVersionError } from './protocol-version-error.ts';
import { SocketConnection } from './socket-connection.ts';

/**
 * How long a coordinator may keep its term while it hangs up on every hello.
 * One that stops ends its term within moments.
 */
const HANDSHAKE_PATIENCE = 1000;

export interface ElectingConnectorOptions {
  socketPath: string;
  /** Only `campaign`: the connector never runs a backend's own steps, so an election of any backend fits. */
  election: Pick<LeaderElection<unknown>, 'campaign'>;
  pollInterval: number;
  /** Starts serving for a term this process just won. */
  serve(term: Term): Promise<void>;
}

/**
 * Reaches the coordinator's socket, and when nobody serves it, campaigns to
 * become the coordinator. Never gives up: a lost coordinator is always
 * replaced by a candidate. An aborted connect never starts serving: a term
 * won after the abort is resigned.
 */
export class ElectingConnector implements FlightConnector {
  readonly #options: ElectingConnectorOptions;

  constructor(options: ElectingConnectorOptions) {
    this.#options = options;
  }

  async connect(signal: AbortSignal): Promise<FlightConnection> {
    const { socketPath, pollInterval } = this.#options;
    for (;;) {
      const coordinator = await reachUnlessAborted(socketPath, signal);
      if (coordinator) {
        const connection =
          (await this.#follow(coordinator, signal)) ??
          (await this.#outlastHangUp(signal));
        if (connection) return connection;
      }
      const term = await this.#campaign(pollInterval, signal);
      if (term) {
        const own = await this.#coordinate(term, signal);
        if (own) return own;
      } else {
        await untilAborted(delay(pollInterval, undefined, { signal }), signal);
      }
    }
  }

  /**
   * After a coordinator hung up on the hello: a coordinator that stops frees
   * its term within moments, so this process coordinates or reaches the
   * successor soon enough to reassert its flights in the successor's grace
   * window. A coordinator that keeps its term and hangs up on every hello
   * fails the connect after HANDSHAKE_PATIENCE. Resolves `undefined` once
   * nothing listens.
   */
  async #outlastHangUp(
    signal: AbortSignal,
  ): Promise<FlightConnection | undefined> {
    const { socketPath, pollInterval } = this.#options;
    const deadline = performance.now() + HANDSHAKE_PATIENCE;
    for (;;) {
      const term = await this.#campaign(0, signal);
      if (term) return this.#coordinate(term, signal);
      const again = await reachUnlessAborted(socketPath, signal);
      if (!again) return undefined;
      const connection = await this.#follow(again, signal);
      if (connection) return connection;
      if (performance.now() >= deadline) {
        throw new ProtocolVersionError(PROTOCOL_VERSION, undefined);
      }
      await untilAborted(delay(pollInterval, undefined, { signal }), signal);
    }
  }

  /**
   * Campaigns until `timeout`; a term won after `signal` aborted is resigned.
   * The election gives up a term that it wins after the abort, but the abort
   * can also land after the campaign resolved, before this step continues.
   */
  async #campaign(
    timeout: number,
    signal: AbortSignal,
  ): Promise<Term | undefined> {
    const term = await this.#options.election.campaign({ timeout, signal });
    if (signal.aborted) await term?.resign();
    signal.throwIfAborted();
    return term;
  }

  /** Serves the term this process just won, and reaches its own server like any other process. */
  async #coordinate(
    term: Term,
    signal: AbortSignal,
  ): Promise<FlightConnection | undefined> {
    try {
      await this.#options.serve(term);
    } catch (error) {
      // A term that nothing serves would stop every other candidate from coordinating.
      await term.resign();
      throw error;
    }
    const own = await reachUnlessAborted(this.#options.socketPath, signal);
    if (!own) return undefined;
    const greeting = await greetUnlessAborted(own, signal);
    if (greeting.kind === 'welcome') return flightConnection(own);
    own.destroy();
    return undefined;
  }

  /** Uses the coordinator on `socket` if it speaks this protocol; `undefined` when it hung up on the hello. */
  async #follow(
    socket: Socket,
    signal: AbortSignal,
  ): Promise<FlightConnection | undefined> {
    const greeting = await greetUnlessAborted(socket, signal);
    if (greeting.kind === 'welcome') return flightConnection(socket);
    socket.destroy();
    if (greeting.kind === 'refused') {
      throw new ProtocolVersionError(PROTOCOL_VERSION, greeting.version);
    }
    return undefined;
  }
}

function flightConnection(socket: Socket): FlightConnection {
  return new SocketConnection<FlightRequest, FlightResponse>(
    socket,
    isFlightResponse,
  );
}

/** Like `reach`, but a socket reached after `signal` aborted is destroyed, not returned. */
async function reachUnlessAborted(
  socketPath: string,
  signal: AbortSignal,
): Promise<Socket | undefined> {
  signal.throwIfAborted();
  const socket = await reach(socketPath);
  if (signal.aborted) socket?.destroy();
  signal.throwIfAborted();
  return socket;
}

/** Resolves `undefined` when nothing listens yet (no file, or a dead coordinator's file). */
function reach(socketPath: string): Promise<Socket | undefined> {
  return new Promise((resolve) => {
    const socket = connect(socketPath);
    const fail = () => {
      socket.destroy();
      resolve(undefined);
    };
    socket.once('error', fail);
    socket.once('connect', () => {
      socket.off('error', fail);
      resolve(socket);
    });
  });
}

/** Greets the coordinator on `socket`; a client that closes meanwhile gives the socket up. */
async function greetUnlessAborted(
  socket: Socket,
  signal: AbortSignal,
): Promise<Greeting> {
  try {
    return await untilAborted(greet(socket), signal);
  } catch (error) {
    socket.destroy();
    throw error;
  }
}
