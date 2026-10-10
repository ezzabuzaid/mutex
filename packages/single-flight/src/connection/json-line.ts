/**
 * One message as one line of the socket's newline-delimited JSON. JSON escapes
 * newlines inside strings, so one line is always one message.
 */
export function jsonLine(message: unknown): string {
  return `${JSON.stringify(message)}\n`;
}
