import type { EventEmitter } from 'node:events';

/**
 * Keeps the errors of a socket, or of a stream reading it, from stopping the
 * process: an `error` with no listener does that. Every such error is followed
 * by the socket's `close`, which is where the lost connection is reported, so
 * this listener does nothing.
 */
export function leaveErrorsToClose(emitter: EventEmitter): void {
  emitter.on('error', () => {});
}
