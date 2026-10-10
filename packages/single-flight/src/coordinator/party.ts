import type { FlightResponse } from '../protocol/flight-protocol.ts';
import type { Flight } from './flight.ts';

/** A response to one party, without the id: the party fills in its own. */
type Answer = {
  [Op in FlightResponse['op']]: Omit<Extract<FlightResponse, { op: Op }>, 'id'>;
}[FlightResponse['op']];

/** Where a party's answers go: the session of the party's connection. */
export interface Inbox {
  reply(response: FlightResponse): void;
  /** The party is answered for good, so the connection may use its id no more. */
  forget(id: string): void;
}

/** What a party is to the coordinator now. */
export type Role =
  /** A run that waits for the grace window, a token, or the reassertion of its flight. */
  | { kind: 'waiting' }
  | { kind: 'leading'; flight: Flight }
  | { kind: 'joining'; flight: Flight }
  /** Answered for good, withdrawn, or its connection is gone. */
  | { kind: 'gone' };

/** One request of one connection: a run that comes to lead a flight or to join one, until it is answered for good. */
export class Party {
  readonly id: string;
  readonly #inbox: Inbox;
  #role: Role = { kind: 'waiting' };

  constructor(id: string, inbox: Inbox) {
    this.id = id;
    this.#inbox = inbox;
  }

  get role(): Role {
    return this.#role;
  }

  lead(flight: Flight) {
    this.#role = { kind: 'leading', flight };
    this.#reply({ op: 'lead', token: flight.token.toString() });
  }

  /** A leader from before a failover claimed its flight again; it already knows that it leads. */
  resume(flight: Flight) {
    this.#role = { kind: 'leading', flight };
  }

  join(flight: Flight) {
    this.#role = { kind: 'joining', flight };
    this.#reply({ op: 'joined', flight: flight.token.toString() });
  }

  /** Answers for good with `answer`, or ends without an answer when it was withdrawn or its connection is gone. */
  end(answer?: Answer) {
    if (this.#role.kind === 'gone') return;
    this.#role = { kind: 'gone' };
    this.#inbox.forget(this.id);
    if (answer) this.#reply(answer);
  }

  #reply(answer: Answer) {
    // The id goes right after the op, so each line keeps its bytes: op, id, then the fields of the op.
    this.#inbox.reply(Object.assign({ op: answer.op, id: this.id }, answer));
  }
}
