import { Latch } from '@zukhruf/async';
import type { FencingToken, TokenSource } from '@zukhruf/fencing';

import type { Connection } from '../connection/connection.ts';
import type { FlightResponse, Outcome } from '../protocol/flight-protocol.ts';
import { Flight } from './flight.ts';
import type { Party } from './party.ts';
import { Session } from './session.ts';

export interface FlightCoordinatorOptions {
  /** Mints the lease token of each flight this coordinator starts. */
  tokens: TokenSource;
  /**
   * Milliseconds after start during which no flight starts, so leaders from a
   * previous coordinator can reassert their flights first. Zero for a
   * coordinator that never replaces another.
   */
  graceWindow: number;
}

interface Phase {
  run(party: Party, key: string): Promise<void>;
  rejoin(party: Party, key: string, flight: string): Promise<void>;
  reassert(party: Party, key: string, token: FencingToken): void;
  land(flight: Flight, outcome: Outcome): void;
  interrupt(flight: Flight): void;
}

/**
 * Coordinates the flights of one term. A run leads a new flight of its key, or
 * joins the flight of that key in progress, in one step, so two runs never
 * both lead. The outcome is pushed to every joiner as soon as it lands.
 */
export class FlightCoordinator {
  #phase: Phase;

  constructor({ tokens, graceWindow }: FlightCoordinatorOptions) {
    const flights = new Map<string, Flight>();
    const coordinating = new Coordinating(flights, tokens);
    if (graceWindow > 0) {
      const over = new Latch();
      this.#phase = new GraceWindow(flights, over, coordinating);
      setTimeout(() => {
        // Coordinating is current before the latch lets the waiting runs resume.
        this.#phase = coordinating;
        over.open();
      }, graceWindow).unref();
    } else {
      this.#phase = coordinating;
    }
  }

  serve(connection: Connection<FlightResponse>): void {
    new Session(this, connection);
  }

  run(party: Party, key: string): Promise<void> {
    return this.#phase.run(party, key);
  }

  /** A joiner that lost its connection, back for the flight it joined: it never leads, so its work never runs twice. */
  rejoin(party: Party, key: string, flight: string): Promise<void> {
    return this.#phase.rejoin(party, key, flight);
  }

  reassert(party: Party, key: string, token: FencingToken): void {
    this.#phase.reassert(party, key, token);
  }

  land(flight: Flight, outcome: Outcome): void {
    this.#phase.land(flight, outcome);
  }

  /** The leader withdrew or its connection is gone. */
  interrupt(flight: Flight): void {
    this.#phase.interrupt(flight);
  }
}

/** The only phase that starts flights. A leader that reasserts now is too late. */
class Coordinating implements Phase {
  readonly #flights: Map<string, Flight>;
  readonly #tokens: TokenSource;

  constructor(flights: Map<string, Flight>, tokens: TokenSource) {
    this.#flights = flights;
    this.#tokens = tokens;
  }

  async run(party: Party, key: string) {
    // A token that goes unused is harmless: tokens only need to grow.
    const token = await this.#tokens.next(key);
    // The client may have withdrawn the run while the token was minted.
    if (party.role.kind !== 'waiting') return;
    const inProgress = this.#flights.get(key);
    if (inProgress) return inProgress.add(party);
    const flight = new Flight(key, token, party);
    this.#flights.set(key, flight);
    party.lead(flight);
  }

  /** A rejoin joins only its own flight; any other answer could be a second run of the work. */
  async rejoin(party: Party, key: string, flight: string) {
    if (party.role.kind !== 'waiting') return;
    const inProgress = this.#flights.get(key);
    if (inProgress?.is(flight)) return inProgress.add(party);
    party.end({ op: 'interrupted' });
  }

  reassert(party: Party) {
    party.end({ op: 'rejected' });
  }

  land(flight: Flight, outcome: Outcome) {
    this.#forget(flight);
    flight.land(outcome);
  }

  interrupt(flight: Flight) {
    this.#forget(flight);
    flight.interrupt();
  }

  #forget(flight: Flight) {
    if (this.#flights.get(flight.key) === flight) {
      this.#flights.delete(flight.key);
    }
  }
}

/** How a reasserted flight ended during the window, for rejoins that arrive after its end. */
type Ending = { token: string } & ({ landed: Outcome } | { interrupted: true });

/** Starts no flight, so leaders from a previous coordinator can reassert theirs first. */
class GraceWindow implements Phase {
  readonly #flights: Map<string, Flight>;
  readonly #over: Latch;
  readonly #next: Coordinating;
  readonly #endings = new Map<string, Ending>();
  /** Wakes the rejoins that wait for a reassertion of their key. */
  readonly #wakers = new Map<string, Set<() => void>>();

  constructor(flights: Map<string, Flight>, over: Latch, next: Coordinating) {
    this.#flights = flights;
    this.#over = over;
    this.#next = next;
  }

  /** A fresh run leads or joins only once every leader had its chance to reassert. */
  async run(party: Party, key: string) {
    await this.#over.wait();
    return this.#next.run(party, key);
  }

  /**
   * A rejoin gets its flight's outcome when the flight's leader reasserted it
   * and it ends during the window, and joins it while it is in progress. Once
   * the window ends, it is answered as in any other phase.
   */
  async rejoin(party: Party, key: string, flight: string) {
    const over = this.#over.wait().then(() => 'over' as const);
    for (;;) {
      if (party.role.kind !== 'waiting') return;
      const inProgress = this.#flights.get(key);
      if (inProgress?.is(flight)) return inProgress.add(party);
      const ending = this.#endings.get(key);
      if (ending?.token === flight) {
        return party.end(
          'landed' in ending
            ? { op: 'landed', outcome: ending.landed }
            : { op: 'interrupted' },
        );
      }
      const woken = await Promise.race([over, this.#reassertionOf(key)]);
      if (woken === 'over') return this.#next.rejoin(party, key, flight);
    }
  }

  /** For two claims on one key, the newer token wins, and the older flight is lost. */
  reassert(party: Party, key: string, token: FencingToken) {
    const claimed = this.#flights.get(key);
    if (claimed && !token.isNewerThan(claimed.token)) {
      return party.end({ op: 'rejected' });
    }
    if (claimed) {
      claimed.leader.end({ op: 'rejected' });
      this.interrupt(claimed);
    }
    const flight = new Flight(key, token, party);
    this.#flights.set(key, flight);
    party.resume(flight);
    const wakers = this.#wakers.get(key);
    this.#wakers.delete(key);
    for (const wake of wakers ?? []) wake();
  }

  land(flight: Flight, outcome: Outcome) {
    this.#endings.set(flight.key, {
      token: flight.token.toString(),
      landed: outcome,
    });
    this.#next.land(flight, outcome);
  }

  interrupt(flight: Flight) {
    this.#endings.set(flight.key, {
      token: flight.token.toString(),
      interrupted: true,
    });
    this.#next.interrupt(flight);
  }

  #reassertionOf(key: string): Promise<'reasserted'> {
    const { promise, resolve } = Promise.withResolvers<'reasserted'>();
    let wakers = this.#wakers.get(key);
    if (!wakers) this.#wakers.set(key, (wakers = new Set()));
    wakers.add(() => resolve('reasserted'));
    return promise;
  }
}
