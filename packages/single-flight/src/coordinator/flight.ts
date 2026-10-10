import type { FencingToken } from '@zukhruf/fencing';

import type { Outcome } from '../protocol/flight-protocol.ts';
import type { Party } from './party.ts';

/** A flight in progress: the key, the leader that runs the work, its lease token, and the parties that joined. */
export class Flight {
  readonly key: string;
  readonly token: FencingToken;
  readonly leader: Party;
  readonly #joiners = new Set<Party>();

  constructor(key: string, token: FencingToken, leader: Party) {
    this.key = key;
    this.token = token;
    this.leader = leader;
  }

  /** Whether this is the flight with the token `flight`, which a rejoin names. */
  is(flight: string): boolean {
    return flight === this.token.toString();
  }

  add(joiner: Party) {
    this.#joiners.add(joiner);
    joiner.join(this);
  }

  remove(joiner: Party) {
    this.#joiners.delete(joiner);
  }

  /** Pushes the outcome to every joiner, then tells the leader that it was handed over. */
  land(outcome: Outcome) {
    for (const joiner of this.#joiners) {
      joiner.end({ op: 'landed', outcome });
    }
    this.#joiners.clear();
    this.leader.end({ op: 'ack' });
  }

  /** Ends without an outcome: every joiner learns that its flight is lost, and none runs the work again. */
  interrupt() {
    for (const joiner of this.#joiners) {
      joiner.end({ op: 'interrupted' });
    }
    this.#joiners.clear();
  }
}
