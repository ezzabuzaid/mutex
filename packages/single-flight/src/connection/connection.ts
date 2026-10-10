import type { EventEmitter } from 'node:events';

export interface ConnectionEvents {
  /** Unchecked: the protocol owner that listens checks each message before it reads it. */
  message: [message: unknown];
  /** Emitted once, when the peer is gone. */
  close: [];
}

/**
 * A message channel to one peer. Adapters wrap IPC channels and sockets.
 * Subscribe in the task that receives the connection: a message that arrives
 * while nobody listens is not kept.
 */
export interface Connection<Outgoing> extends EventEmitter<ConnectionEvents> {
  /** Rejects when the message cannot be delivered because the peer is gone. */
  send(message: Outgoing): Promise<void>;
  /** Keeps the process alive while its owner waits for a message. */
  ref(): void;
  /** Lets the process exit; messages and closure are still reported while it runs. */
  unref(): void;
  close(): void;
}
