package com.scylladb.alternator.keyrouting;

/**
 * Reasons why key route affinity fell back to round-robin routing.
 */
public enum KeyRouteAffinityFallbackReason {
  REQUEST_NOT_QUALIFYING("request_not_qualifying"),
  TABLE_NAME_UNAVAILABLE("table_name_unavailable"),
  PK_NOT_CACHED("pk_not_cached"),
  PK_VALUE_UNAVAILABLE("pk_value_unavailable");

  private final String tagValue;

  KeyRouteAffinityFallbackReason(String tagValue) {
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
