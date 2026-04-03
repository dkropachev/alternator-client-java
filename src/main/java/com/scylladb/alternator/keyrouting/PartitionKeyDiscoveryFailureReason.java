package com.scylladb.alternator.keyrouting;

/**
 * Explains why partition key discovery failed for a table.
 *
 * @author dmitry.kropachev
 * @since 2.0.5
 */
public enum PartitionKeyDiscoveryFailureReason {
  TABLE_NOT_FOUND,
  ACCESS_DENIED,
  MISSING_HASH_KEY,
  TRANSIENT_ERROR,
  UNEXPECTED_ERROR
}
