import { EventEmitter } from 'node:events';

import { Latch } from '@zukhruf/async';

import type { Connection } from './connection.ts';
import type { Connector } from './connector.ts';

export type SupervisorStatus =
  'idle' | 'connecting' | 'connected' | 'unavailable' | 'closed';

export interface SupervisorEvents {
  /** A connection is open: the first one, or the replacement for a lost one. */
  connected: [];
  message: [message: unknown];
  /** The open connection is gone and a replacement is on its way. Nothing sent on it will be answered. */
  disconnected: [];
  /** The connector can never connect again. */
  unavailable: [];
  /** One attempt to connect failed; the next `open` tries again. */
  failed: [error: unknown];
}

/**
 * Keeps one connection open at a time. It connects on `open`, replaces a
 * connection that is lost, and stops for good when its connector has no peer
 * left. It never reads the messages it carries: what a replacement must be
 * told again is for its listener to decide.
 */
export class ConnectionSupervisor<
  Outgoing,
> extends EventEmitter<SupervisorEvents> {
  readonly #supervision: Supervision<Outgoing>;

  constructor(connector: Connector<Outgoing>) {
    super();
    this.#supervision = new Supervision(connector, this);
  }

  get status(): SupervisorStatus {
    return this.#supervision.state.status;
  }

  /** Starts connecting, unless a connection is open, on its way, or never coming. */
  open() {
    this.#supervision.state.open();
  }

  /** Whether `message` was handed to an open connection. A failed delivery is reported as a loss. */
  send(message: Outgoing): boolean {
    return this.#supervision.state.send(message);
  }

  /**
   * Keeps the process alive through the open connection. A replacement starts
   * with its adapter's default, so a listener that waits applies this again on
   * `connected`.
   */
  ref() {
    this.#supervision.state.reference(true);
  }

  unref() {
    this.#supervision.state.reference(false);
  }

  /** Closes for good, after any connect still on its way has stopped. */
  close(): Promise<void> {
    return this.#supervision.state.close();
  }
}

interface State<Outgoing> {
  readonly status: SupervisorStatus;
  /** Runs once the state is current, so whatever it starts can check whether it still is. */
  enter(): void;
  open(): void;
  send(message: Outgoing): boolean;
  reference(referenced: boolean): void;
  close(): Promise<void>;
}

/** What the states share: the current state, and how to move to the next. */
class Supervision<Outgoing> {
  readonly connector: Connector<Outgoing>;
  readonly events: EventEmitter<SupervisorEvents>;
  state: State<Outgoing>;

  constructor(
    connector: Connector<Outgoing>,
    events: EventEmitter<SupervisorEvents>,
  ) {
    this.connector = connector;
    this.events = events;
    this.state = new Idle(this);
  }

  /** A state that stopped being current must not act on what it started. */
  isCurrent(state: State<Outgoing>): boolean {
    return this.state === state;
  }

  become(state: State<Outgoing>) {
    this.state = state;
    state.enter();
  }
}

class Idle<Outgoing> implements State<Outgoing> {
  readonly status = 'idle';
  readonly #supervision: Supervision<Outgoing>;

  constructor(supervision: Supervision<Outgoing>) {
    this.#supervision = supervision;
  }

  enter() {}

  open() {
    this.#supervision.become(new Connecting(this.#supervision));
  }

  send(): boolean {
    return false;
  }

  reference() {}

  async close() {
    this.#supervision.become(new Closed());
  }
}

class Connecting<Outgoing> implements State<Outgoing> {
  readonly status = 'connecting';
  readonly #supervision: Supervision<Outgoing>;
  readonly #abort = new AbortController();
  /** Opens when the connect ends, so `close` can wait for it. */
  readonly #ended = new Latch();

  constructor(supervision: Supervision<Outgoing>) {
    this.#supervision = supervision;
  }

  enter() {
    void this.#connect().finally(() => this.#ended.open());
  }

  async #connect() {
    const supervision = this.#supervision;
    let connection: Connection<Outgoing> | undefined;
    try {
      connection = await Promise.try(() =>
        supervision.connector.connect(this.#abort.signal),
      );
    } catch (error) {
      if (!supervision.isCurrent(this)) return;
      supervision.become(new Idle(supervision));
      supervision.events.emit('failed', error);
      return;
    }
    if (!supervision.isCurrent(this)) {
      connection?.close();
      return;
    }
    supervision.become(
      connection
        ? new Connected(supervision, connection)
        : new Unavailable(supervision),
    );
  }

  open() {}

  send(): boolean {
    return false;
  }

  reference() {}

  async close() {
    this.#supervision.become(new Closed());
    this.#abort.abort();
    await this.#ended.wait();
  }
}

class Connected<Outgoing> implements State<Outgoing> {
  readonly status = 'connected';
  readonly #supervision: Supervision<Outgoing>;
  readonly #connection: Connection<Outgoing>;

  constructor(
    supervision: Supervision<Outgoing>,
    connection: Connection<Outgoing>,
  ) {
    this.#supervision = supervision;
    this.#connection = connection;
  }

  enter() {
    const supervision = this.#supervision;
    this.#connection.on('message', (message) => {
      if (supervision.isCurrent(this))
        supervision.events.emit('message', message);
    });
    this.#connection.once('close', () => this.#lose());
    supervision.events.emit('connected');
  }

  open() {}

  /** The connection may report its loss while it sends, so the state is checked after the call. */
  send(message: Outgoing): boolean {
    this.#connection.send(message).catch(() => this.#lose());
    return this.#supervision.isCurrent(this);
  }

  reference(referenced: boolean) {
    if (referenced) this.#connection.ref();
    else this.#connection.unref();
  }

  async close() {
    this.#supervision.become(new Closed());
    this.#connection.close();
  }

  /** A close event and a failed send can both report one loss; only the first counts. */
  #lose() {
    const supervision = this.#supervision;
    if (!supervision.isCurrent(this)) return;
    this.#connection.close();
    // Leave this state before the listener hears of the loss, so its sends wait for the replacement.
    supervision.become(new Connecting(supervision));
    supervision.events.emit('disconnected');
  }
}

class Unavailable<Outgoing> implements State<Outgoing> {
  readonly status = 'unavailable';
  readonly #supervision: Supervision<Outgoing>;

  constructor(supervision: Supervision<Outgoing>) {
    this.#supervision = supervision;
  }

  enter() {
    this.#supervision.events.emit('unavailable');
  }

  open() {}

  send(): boolean {
    return false;
  }

  reference() {}

  async close() {
    this.#supervision.become(new Closed());
  }
}

class Closed<Outgoing> implements State<Outgoing> {
  readonly status = 'closed';

  enter() {}

  open() {}

  send(): boolean {
    return false;
  }

  reference() {}

  async close() {}
}
