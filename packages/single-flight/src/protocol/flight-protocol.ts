import { FencingToken } from '@zukhruf/fencing';

import { isRecord } from '../shared/is-record.ts';

/** A leader's error as text, because an Error object cannot cross a process boundary. */
export interface Failure {
  readonly name: string;
  readonly message: string;
  /** The error's `code`, when it has one, as Node.js errors do. */
  readonly code?: string | number;
}

/** How a flight landed: the value as the codec encoded it, or the leader's failure. */
export type Outcome = { value: string } | { failure: Failure };

/** Tokens and flights travel as decimal strings because JSON has no bigint. */
export type FlightRequest =
  /**
   * Leads the flight of `key` when none is in progress, or joins the flight in
   * progress. After a lost connection, a run that the coordinator had told
   * which flight it joined is sent again with that flight's token: such a
   * rejoin never leads, so a joiner whose leader died never runs the work
   * again. A run that got no answer has joined nothing, and goes again as a
   * plain run.
   */
  | { op: 'run'; id: string; key: string; flight?: string }
  /** The leader of `id` ends its flight with `outcome`. Kept by the client until the coordinator acknowledges it. */
  | { op: 'land'; id: string; outcome: Outcome }
  /** Withdraws a run that is not answered yet, or a joiner from its flight. */
  | { op: 'cancel'; id: string }
  /** A leader from before a failover claims its flight again, during the new coordinator's grace window. */
  | { op: 'reassert'; id: string; key: string; token: string };

export type FlightResponse =
  /** The run leads a new flight; `token` is its lease's fencing token. */
  | { op: 'lead'; id: string; token: string }
  /** The run joined the flight in progress, whose leader holds the token `flight`. */
  | { op: 'joined'; id: string; flight: string }
  | { op: 'landed'; id: string; outcome: Outcome }
  /** The flight that the run joined ended without an outcome: its leader stopped or lost its lease. */
  | { op: 'interrupted'; id: string }
  /** The coordinator handed a landing to the joiners. */
  | { op: 'ack'; id: string }
  /** The flight of `id` is no longer its leader's: a reassertion or a landing came too late. */
  | { op: 'rejected'; id: string }
  /** The coordinator does not know the request: it runs an older version. */
  | { op: 'unsupported'; id: string };

/** A request whose `op` this process may not know. A coordinator answers one it does not know with `unsupported`. */
export interface RequestEnvelope {
  op: string;
  id: string;
}

const isToken = (value: unknown): value is string =>
  typeof value === 'string' && FencingToken.parse(value) !== null;

export function isRequestEnvelope(
  message: unknown,
): message is RequestEnvelope {
  return (
    isRecord(message) &&
    typeof message.op === 'string' &&
    typeof message.id === 'string'
  );
}

/**
 * Messages arrive from another process, so their shape is checked before
 * use. `isRequestEnvelope` already checked the `op` and the `id`, so only the
 * fields of each request are left.
 */
export function isFlightRequest(
  request: RequestEnvelope,
): request is FlightRequest {
  switch (request.op) {
    case 'run':
      return (
        'key' in request &&
        typeof request.key === 'string' &&
        (!('flight' in request) || isToken(request.flight))
      );
    case 'land':
      return 'outcome' in request && isOutcome(request.outcome);
    case 'cancel':
      return true;
    case 'reassert':
      return (
        'key' in request &&
        typeof request.key === 'string' &&
        'token' in request &&
        isToken(request.token)
      );
    default:
      return false;
  }
}

export function isFlightResponse(message: unknown): message is FlightResponse {
  if (!isRecord(message) || typeof message.id !== 'string') return false;
  switch (message.op) {
    case 'lead':
      return isToken(message.token);
    case 'joined':
      return isToken(message.flight);
    case 'landed':
      return isOutcome(message.outcome);
    case 'interrupted':
    case 'ack':
    case 'rejected':
    case 'unsupported':
      return true;
    default:
      return false;
  }
}

function isOutcome(value: unknown): value is Outcome {
  if (!isRecord(value)) return false;
  if ('value' in value) return typeof value.value === 'string';
  return isFailure(value.failure);
}

function isFailure(value: unknown): value is Failure {
  return (
    isRecord(value) &&
    typeof value.name === 'string' &&
    typeof value.message === 'string' &&
    (value.code === undefined ||
      typeof value.code === 'string' ||
      typeof value.code === 'number')
  );
}
