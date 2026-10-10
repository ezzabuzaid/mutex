import { FencingToken } from '@zukhruf/fencing';

import type { Connection } from '../connection/connection.ts';
import {
  type FlightResponse,
  type RequestEnvelope,
  isFlightRequest,
  isRequestEnvelope,
} from '../protocol/flight-protocol.ts';
import type { FlightCoordinator } from './flight-coordinator.ts';
import { type Inbox, Party } from './party.ts';

/**
 * One client connection. It turns each request into a party of the
 * coordinator, and when the connection is gone, it withdraws them all: the
 * flights it leads are interrupted, and the flights it joined lose a joiner.
 */
export class Session implements Inbox {
  readonly #coordinator: FlightCoordinator;
  readonly #parties = new Map<string, Party>();
  #phase: SessionPhase;

  constructor(
    coordinator: FlightCoordinator,
    connection: Connection<FlightResponse>,
  ) {
    this.#coordinator = coordinator;
    this.#phase = new Serving(connection);
    connection.on('message', (message) => {
      // A message with no op or no id cannot be answered, so it is ignored, and the peer keeps its flights (ADR 0008).
      if (!isRequestEnvelope(message)) return;
      this.#handle(message);
    });
    connection.once('close', () => this.#end());
  }

  reply(response: FlightResponse) {
    this.#phase.reply(response);
  }

  forget(id: string) {
    this.#parties.delete(id);
  }

  #handle(request: RequestEnvelope) {
    // A newer process may ask what this version does not know. The answer
    // keeps its connection, and with it every flight it leads.
    if (!isFlightRequest(request)) {
      return this.reply({ op: 'unsupported', id: request.id });
    }
    switch (request.op) {
      case 'run': {
        const party = this.#open(request.id);
        if (!party) return;
        void (
          request.flight === undefined
            ? this.#coordinator.run(party, request.key)
            : this.#coordinator.rejoin(party, request.key, request.flight)
        ).catch(() => this.#phase.close());
        return;
      }
      case 'land': {
        const role = this.#parties.get(request.id)?.role;
        if (role?.kind !== 'leading') {
          return this.reply({ op: 'rejected', id: request.id });
        }
        return this.#coordinator.land(role.flight, request.outcome);
      }
      case 'cancel':
        return this.#withdraw(request.id);
      case 'reassert': {
        const party = this.#open(request.id);
        if (!party) return;
        return this.#coordinator.reassert(
          party,
          request.key,
          FencingToken.parse(request.token)!,
        );
      }
    }
  }

  /** A client never sends one id twice on one connection; a second request with it is ignored. */
  #open(id: string): Party | undefined {
    if (this.#parties.has(id)) return undefined;
    const party = new Party(id, this);
    this.#parties.set(id, party);
    return party;
  }

  #withdraw(id: string) {
    const party = this.#parties.get(id);
    if (!party) return;
    const role = party.role;
    party.end();
    if (role.kind === 'joining') role.flight.remove(party);
    if (role.kind === 'leading') this.#coordinator.interrupt(role.flight);
  }

  /** Ends Serving first, so nothing is answered on a connection that is gone. */
  #end() {
    this.#phase = new Ended();
    for (const id of [...this.#parties.keys()]) this.#withdraw(id);
  }
}

interface SessionPhase {
  reply(response: FlightResponse): void;
  close(): void;
}

/** The client is connected: the session answers it. */
class Serving implements SessionPhase {
  readonly #connection: Connection<FlightResponse>;

  constructor(connection: Connection<FlightResponse>) {
    this.#connection = connection;
  }

  reply(response: FlightResponse) {
    // A failed send means the client is gone; `close` then withdraws its parties.
    this.#connection.send(response).catch(() => {});
  }

  close() {
    this.#connection.close();
  }
}

/** The client is gone: nothing is answered. */
class Ended implements SessionPhase {
  reply() {}

  close() {}
}
