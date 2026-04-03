package com.scylladb.alternator.keyrouting;

/**
 * Terminal reasons for partition key discovery failure.
 */
public enum PartitionKeyDiscoveryFailureReason {
  NO_HASH_KEY("no_hash_key"),
  TABLE_NOT_FOUND("table_not_found"),
  ACCESS_DENIED("access_denied"),
  VALIDATION_ERROR("validation_error"),
  PERMANENT_ERROR("permanent_error"),
  MAX_RETRIES_EXCEEDED("max_retries_exceeded"),
  UNEXPECTED_ERROR("unexpected_error");

  private final String tagValue;

  PartitionKeyDiscoveryFailureReason(String tagValue) {
    this.tagValue = tagValue;
  }

  /**
   * Returns the stable tag value for metrics/logging integrations.
   *
   * @return the tag value
   */
  public String getTagValue() {
    return tagValue;
  }

  @Override
  public String toString() {
    return tagValue;
  }
}
