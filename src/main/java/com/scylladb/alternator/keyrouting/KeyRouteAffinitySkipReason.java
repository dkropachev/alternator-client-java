package com.scylladb.alternator.keyrouting;

/**
 * Explains why a request fell back from key affinity to round-robin routing.
 *
 * @author dmitry.kropachev
 * @since 2.0.5
 */
public enum KeyRouteAffinitySkipReason {
  REQUEST_NOT_QUALIFYING,
  TABLE_NAME_UNAVAILABLE,
  PARTITION_KEY_NOT_CACHED,
  PARTITION_KEY_NOT_PRESENT
}
