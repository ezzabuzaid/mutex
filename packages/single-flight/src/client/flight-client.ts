import { randomUUID } from 'node:crypto';

import { untilAborted } from '@zukhruf/async';
import { FencingToken } from '@zukhruf/fencing';
import { LeaseController } from '@zukhruf/lease';

import type { ConnectionSupervisor } from '../connection/connection-supervisor.ts';
import {
  type FlightRequest,
  type FlightResponse,
  type Outcome,
  isFlightResponse,
} from '../protocol/flight-protocol.ts';

/** This process leads a flight: it runs the work and lands the outcome. */
export interface Lead {
  readonly token: FencingToken;
  /** Aborts with a `LeaseLostError` once the flight is no longer this leader's. */
  readonly signal: AbortSignal;
  /** Ends the flight with `outcome` for every joiner. Kept and sent again until a coordinator acknowledges it. */
  land(outcome: Outcome): void;
}

/** How the coordinator answered a run for good. */
export type Answer =
  | { kind: 'lead'; lead: Lead }
  | { kind: 'landed'; outcome: Outcome }
  | { kind: 'interrupted' };

export interface RunRequest {
  signal?: AbortSignal | undefined;
  onJoin?: (() => void) | undefined;
}

/** Keeps only what settles the caller's promise; the caller keeps the promise. */
interface Pending extends Pick<
  PromiseWithResolvers<Answer>,
  'resolve' | 'reject'
> {
  key: string;
  onJoin: (() => void) | undefined;
  /** `queued` waits for a connection, and `sent` went out on the open one. */
  delivery: 'queued' | 'sent';
  /** The flight that the coordinator said this run joined, so after a lost connection it rejoins that flight only. */
  flight: string | undefined;
}

interface Held {
  key: string;
  token: FencingToken;
  lease: LeaseController;
  /** The outcome this leader landed, kept until a coordinator acknowledges it. */
  landing: Outcome | undefined;
}

/**
 * Sends runs to the coordinator over a supervised connection. When the
 * connection is replaced, it reasserts the flights this process leads, sends
 * their landings again, and sends again the runs that wait for an answer. A
 * run that had joined a flight rejoins that flight only, so it never leads; a
 * run that got no answer had joined nothing, and goes again as a plain run.
 */
export class FlightClient {
  readonly #link: ConnectionSupervisor<FlightRequest>;
  readonly #pending = new Map<string, Pending>();
  readonly #held = new Map<string, Held>();
  /** Wakes `close` each time a flight this process leads is acknowledged or lost. */
  readonly #released = new Set<() => void>();

  constructor(link: ConnectionSupervisor<FlightRequest>) {
    this.#link = link;
    link.on('connected', () => this.#resume());
    // A message this client cannot read answers nothing it waits for.
    link.on('message', (message) => {
      if (isFlightResponse(message)) this.#receive(message);
    });
    link.on('disconnected', () => this.#interrupt());
    link.on('failed', (error) => this.#fail(error));
  }

  async run(key: string, { signal, onJoin }: RunRequest): Promise<Answer> {
    signal?.throwIfAborted();
    if (this.#link.status === 'closed') {
      throw new Error('This single flight is closed.');
    }
    const id = randomUUID();
    const { promise, resolve, reject } = Promise.withResolvers<Answer>();
    const pending: Pending = {
      key,
      onJoin,
      delivery: 'queued',
      flight: undefined,
      resolve,
      reject,
    };
    this.#pending.set(id, pending);
    this.#updateRef();
    this.#link.open();
    this.#dispatch(id, pending);
    try {
      return await untilAborted(promise, signal);
    } catch (error) {
      if (signal?.aborted) this.#withdraw(id);
      throw error;
    }
  }

  /**
   * Disconnects for good, once every landing reached a coordinator: a landing
   * is the outcome its joiners wait for. The coordinator then interrupts the
   * flights this process still leads without an outcome.
   */
  async close() {
    while ([...this.#held.values()].some(({ landing }) => landing)) {
      await new Promise<void>((resolve) => this.#released.add(resolve));
    }
    const closing = this.#link.close();
    for (const id of [...this.#pending.keys()]) {
      this.#take(id)?.reject(new Error('This single flight is closed.'));
    }
    await closing;
  }

  #dispatch(id: string, pending: Pending) {
    const { key, flight } = pending;
    const request: FlightRequest =
      flight === undefined
        ? { op: 'run', id, key }
        : { op: 'run', id, key, flight };
    if (this.#link.send(request)) pending.delivery = 'sent';
  }

  /**
   * The caller gave up. A run the coordinator may have heard is withdrawn;
   * one that already leads is not pending any more, so its flight runs on.
   */
  #withdraw(id: string) {
    const pending = this.#take(id);
    if (pending?.delivery === 'sent') this.#link.send({ op: 'cancel', id });
  }

  #hold(id: string, key: string, token: FencingToken): Lead {
    const held: Held = {
      key,
      token,
      lease: new LeaseController(key),
      landing: undefined,
    };
    this.#held.set(id, held);
    return {
      token,
      signal: held.lease.signal,
      land: (outcome) => {
        // A lost flight is no longer this leader's to land.
        if (this.#held.get(id) !== held || held.landing) return;
        held.landing = outcome;
        this.#updateRef();
        this.#link.send({ op: 'land', id, outcome });
      },
    };
  }

  /** The flight of `id` is no longer this leader's, so its lease is told and nothing reasserts it. */
  #lose(id: string) {
    const held = this.#held.get(id);
    if (!held) return;
    this.#release(id);
    held.lease.lose();
  }

  /** The flight of `id` needs nothing more from this process. */
  #release(id: string) {
    this.#held.delete(id);
    this.#updateRef();
    const waiting = [...this.#released];
    this.#released.clear();
    for (const wake of waiting) wake();
  }

  #receive(response: FlightResponse) {
    switch (response.op) {
      case 'lead': {
        // A run withdrawn meanwhile sent `cancel`, which interrupts the flight at the coordinator.
        const pending = this.#take(response.id);
        if (!pending) return;
        const token = FencingToken.parse(response.token)!;
        pending.resolve({
          kind: 'lead',
          lead: this.#hold(response.id, pending.key, token),
        });
        return;
      }
      case 'joined': {
        const pending = this.#pending.get(response.id);
        if (!pending) return;
        const first = pending.flight === undefined;
        pending.flight = response.flight;
        if (first) this.#tellJoined(response.id, pending);
        return;
      }
      case 'landed':
        this.#take(response.id)?.resolve({
          kind: 'landed',
          outcome: response.outcome,
        });
        return;
      case 'interrupted':
        this.#take(response.id)?.resolve({ kind: 'interrupted' });
        return;
      case 'ack': {
        // The joiners have the outcome, so there is nothing left to reassert, and no later loss of the lease.
        const held = this.#held.get(response.id);
        if (!held?.landing) return;
        this.#release(response.id);
        held.lease.end();
        return;
      }
      case 'rejected':
        this.#lose(response.id);
        return;
      case 'unsupported':
        this.#take(response.id)?.reject(
          new Error("The single flight's coordinator does not know a run."),
        );
        return;
    }
  }

  /** The caller's `onJoin` runs once per run; a throw from it fails that run only. */
  #tellJoined(id: string, pending: Pending) {
    try {
      pending.onJoin?.();
    } catch (error) {
      this.#withdraw(id);
      pending.reject(error);
    }
  }

  /** A new connection: reassert the flights this process leads and land them again, then send what waits. */
  #resume() {
    // A new connection starts with its adapter's default, which may not match whether this client waits.
    this.#updateRef();
    for (const [id, { key, token, landing }] of this.#held) {
      this.#link.send({ op: 'reassert', id, key, token: token.toString() });
      if (landing) this.#link.send({ op: 'land', id, outcome: landing });
    }
    for (const [id, pending] of this.#pending) {
      if (pending.delivery === 'queued') this.#dispatch(id, pending);
    }
  }

  /** Nothing sent on the lost connection will be answered, so every run goes again on the next one. */
  #interrupt() {
    for (const pending of this.#pending.values()) pending.delivery = 'queued';
  }

  /** No coordinator heard the reassertions, so the flights this process leads may be interrupted already. */
  #fail(error: unknown) {
    for (const id of [...this.#held.keys()]) this.#lose(id);
    for (const id of [...this.#pending.keys()]) this.#take(id)?.reject(error);
  }

  /** Removes a run that is about to be answered, so a late answer for it is ignored. */
  #take(id: string): Pending | undefined {
    const pending = this.#pending.get(id);
    if (!pending) return undefined;
    this.#pending.delete(id);
    this.#updateRef();
    return pending;
  }

  /** A run waiting for its answer, or a landing not yet acknowledged, keeps the process alive. */
  #updateRef() {
    const landing = [...this.#held.values()].some(({ landing }) => landing);
    if (this.#pending.size > 0 || landing) this.#link.ref();
    else this.#link.unref();
  }
}
