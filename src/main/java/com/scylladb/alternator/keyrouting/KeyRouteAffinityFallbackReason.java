package com.scylladb.alternator.keyrouting;

/**
 * Reason why a key affinity request fell back to round-robin routing.
 *
 * @author dmitry.kropachev
 * @since 2.1.0
 */
public enum KeyRouteAffinityFallbackReason {
  MODE_DISABLED("mode_disabled"),
  REQUEST_NOT_QUALIFYING("request_not_qualifying"),
  TABLE_NAME_UNAVAILABLE("table_name_unavailable"),
  PARTITION_KEY_NOT_CACHED("pk_not_cached"),
  PARTITION_KEY_MISSING("pk_missing_in_request");

  private final String label;

  KeyRouteAffinityFallbackReason(String label) {
    this.label = label;
  }

  /**
   * Returns a stable lowercase label suitable for metrics tags and log output.
   *
   * @return the reason label
   */
  public String label() {
    return label;
  }
}
