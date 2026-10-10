import { untilAborted } from '@zukhruf/async';
import type { FencedLease } from '@zukhruf/fencing';
import { LeaseLostError } from '@zukhruf/lease';

import { FlightClient, type Lead } from './client/flight-client.ts';
import type { Codec } from './codec.ts';
import { ConnectionSupervisor } from './connection/connection-supervisor.ts';
import { ElectingConnector } from './connection/electing-connector.ts';
import { flightElection } from './connection/flight-election.ts';
import { LocalDirectoryConnector } from './connection/local-directory-connector.ts';
import { socketPathFor } from './connection/socket-path.ts';
import { FlightServer } from './coordinator/flight-server.ts';
import {
  FlightFailedError,
  FlightInterruptedError,
  failureOf,
} from './errors.ts';
import type { Outcome } from './protocol/flight-protocol.ts';

/** Milliseconds between two attempts to reach the coordinator or to win its term. */
const POLL_INTERVAL = 10;

export interface SingleFlightOptions<T> {
  /**
   * The local folder where the processes that share flights meet. They keep
   * `flight.lock`, `flight.epoch` and `flight.sock` there, so every process
   * that names the same folder is in one group, and callers of one key share
   * a flight across that group. There is no default: one shared default would
   * put every application on the host into one group, and unrelated
   * applications that use the same key would join each other's flights. It
   * must be on a local file system.
   */
  directory: string;
  /** Turns the flight's value into text for the processes that joined it, and back. */
  codec: Codec<T>;
  /**
   * Milliseconds that a new coordinator starts no flight after the previous
   * one stopped, so the leaders of flights in progress can claim them again.
   * Must exceed how long a process takes to reconnect. Defaults to 500.
   */
  graceWindow?: number;
}

export interface RunOptions {
  /** Called once, when this caller joins a flight that another caller leads. */
  onJoin?: (() => void) | undefined;
  /** Stops the wait of this caller only, which then rejects with `signal.reason`. The flight runs on. */
  signal?: AbortSignal | undefined;
}

/** What a caller gets: the value of the flight, and whether the caller joined a flight that another caller led. */
export interface FlightValue<T> {
  readonly value: T;
  readonly joined: boolean;
}

/** How the leader's own flight ended for the leader's caller. */
type Settled<T> = { value: T } | { error: unknown };

/**
 * Callers of one key share the flight in progress, in this process and in
 * every process that uses the same directory. One process of the group is
 * elected the coordinator. Each call asks it, in one step, to lead a new
 * flight of the key or to join the flight in progress. The leader runs the
 * work, and the coordinator pushes the outcome to every joiner.
 */
export class SingleFlight<T> implements AsyncDisposable {
  readonly #client: FlightClient;
  readonly #codec: Codec<T>;
  /** The flight servers this process started; disposing closes them. */
  readonly #servers = new AsyncDisposableStack();

  constructor({ directory, codec, graceWindow = 500 }: SingleFlightOptions<T>) {
    this.#codec = codec;
    const socketPath = socketPathFor(directory);
    this.#client = new FlightClient(
      new ConnectionSupervisor(
        new LocalDirectoryConnector(
          directory,
          new ElectingConnector({
            socketPath,
            election: flightElection(directory, POLL_INTERVAL),
            pollInterval: POLL_INTERVAL,
            serve: async (term) => {
              // Until serving has fully started, a failure closes the new server, so no server outlives its term.
              await using starting = new AsyncDisposableStack();
              starting.adopt(
                await FlightServer.start(socketPath, term, {
                  // The first term of a directory has no flights to wait for,
                  // and neither has a term after a clean shutdown: the
                  // coordinator before it left no flight in progress.
                  graceWindow:
                    term.epoch > 1n && !term.afterCleanShutdown
                      ? graceWindow
                      : 0,
                }),
                (started) => started.close(),
              );
              this.#servers.use(starting.move());
            },
          }),
        ),
      ),
    );
  }

  /**
   * Runs `work` for `key` as the leader of a new flight, or joins the flight
   * of `key` in progress. `work` gets the flight's lease: its token, and a
   * signal that aborts with `LeaseLostError` once the flight is no longer this
   * leader's. After that loss, the leader's call rejects with `LeaseLostError`,
   * also when the work throws its own error. A joiner gets the leader's value
   * through the codec, or rejects with `FlightFailedError` or
   * `FlightInterruptedError`.
   */
  async run(
    key: string,
    work: (lease: FencedLease) => Promise<T>,
    { onJoin, signal }: RunOptions = {},
  ): Promise<FlightValue<T>> {
    const answer = await this.#client.run(key, { signal, onJoin });
    switch (answer.kind) {
      case 'landed':
        return { value: this.#read(key, answer.outcome), joined: true };
      case 'interrupted':
        throw new FlightInterruptedError(key);
      case 'lead': {
        // The signal stops only this caller's wait. The flight runs on for its joiners.
        const settled = await untilAborted(
          this.#fly(key, work, answer.lead),
          signal,
        );
        if ('error' in settled) throw settled.error;
        return { value: settled.value, joined: false };
      }
    }
  }

  /** Disconnects, and stops serving as the coordinator: the other processes elect a successor. */
  async [Symbol.asyncDispose]() {
    await this.#client.close();
    await this.#servers.disposeAsync();
  }

  /** Runs the work and lands its outcome. Never rejects, so a caller that stopped waiting leaves no rejection behind. */
  async #fly(
    key: string,
    work: (lease: FencedLease) => Promise<T>,
    lead: Lead,
  ): Promise<Settled<T>> {
    const { token, signal: lost } = lead;
    let text: string;
    try {
      text = this.#codec.encode(await work({ token, signal: lost }));
    } catch (error) {
      lead.land({ failure: failureOf(error) });
      // After a loss, every failure rejects with LeaseLostError: the work did not run alone (ADR 0007).
      if (!lost.aborted || error === lost.reason) return { error };
      return { error: new LeaseLostError(key, { cause: error }) };
    }
    // A lost flight's joiners were told that it is interrupted, so this value reaches nobody.
    if (lost.aborted) return { error: lost.reason };
    lead.land({ value: text });
    try {
      return { value: this.#codec.decode(text) };
    } catch (error) {
      return { error };
    }
  }

  #read(key: string, outcome: Outcome): T {
    if ('failure' in outcome) throw new FlightFailedError(key, outcome.failure);
    return this.#codec.decode(outcome.value);
  }
}
