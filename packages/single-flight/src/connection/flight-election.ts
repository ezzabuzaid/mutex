import { SqliteElection } from '@zukhruf/election';

/**
 * The election of the single flights that share `directory`. Every published
 * version claims `flight.lock` and counts terms in `flight.epoch`
 * (flight-protocol.test.ts pins both), so single flights of two versions
 * elect one coordinator between them. The socket lock store of
 * `@zukhruf/mutex` names its files `leader.*`, so it never joins this
 * election in a shared directory.
 */
export function flightElection(
  directory: string,
  pollInterval: number,
): SqliteElection {
  return new SqliteElection({
    directory,
    claimFile: 'flight.lock',
    epochFile: 'flight.epoch',
    pollInterval,
  });
}
