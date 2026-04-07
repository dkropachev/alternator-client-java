package com.scylladb.alternator.keyrouting;

/** Canonical labels emitted by the optional {@link KeyRouteAffinityMetrics} callbacks. */
public enum KeyRouteAffinityMetricLabel {
  UNKNOWN_TABLE("unknown"),
  NOT_QUALIFYING_REQUEST("not_qualifying_request"),
  TABLE_NAME_MISSING("table_name_missing"),
  PK_NOT_CACHED("pk_not_cached"),
  PK_VALUE_MISSING("pk_value_missing"),
  NO_HASH_KEY("no_hash_key"),
  RESOURCE_NOT_FOUND("resource_not_found"),
  ACCESS_DENIED("access_denied"),
  VALIDATION_ERROR("validation_error"),
  CLIENT_ERROR("client_error"),
  TRANSIENT_ERROR("transient_error"),
  UNEXPECTED_ERROR("unexpected_error");

  private final String value;

  KeyRouteAffinityMetricLabel(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }
}
