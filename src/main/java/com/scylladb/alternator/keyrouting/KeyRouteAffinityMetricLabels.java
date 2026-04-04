package com.scylladb.alternator.keyrouting;

/**
 * String labels emitted by the optional {@link KeyRouteAffinityMetrics} callbacks.
 *
 * @author dmitry.kropachev
 * @since 2.0.5
 */
public final class KeyRouteAffinityMetricLabels {

  /** Fallback table label used when a request does not expose a table name. */
  public static final String UNKNOWN_TABLE = "unknown";

  /** Request did not qualify for affinity in the configured mode. */
  public static final String NOT_QUALIFYING_REQUEST = "not_qualifying_request";

  /** Request did not expose a table name. */
  public static final String TABLE_NAME_MISSING = "table_name_missing";

  /** Partition-key metadata was not cached for the table. */
  public static final String PK_NOT_CACHED = "pk_not_cached";

  /** Request did not contain the expected partition-key value. */
  public static final String PK_VALUE_MISSING = "pk_value_missing";

  /** DescribeTable returned no HASH key in the table schema. */
  public static final String NO_HASH_KEY = "no_hash_key";

  /** DescribeTable returned a resource-not-found error. */
  public static final String RESOURCE_NOT_FOUND = "resource_not_found";

  /** DescribeTable failed with an access denied error. */
  public static final String ACCESS_DENIED = "access_denied";

  /** DescribeTable failed with a validation error. */
  public static final String VALIDATION_ERROR = "validation_error";

  /** DescribeTable failed with another non-retryable client error. */
  public static final String CLIENT_ERROR = "client_error";

  /** DescribeTable exhausted retries after transient failures. */
  public static final String TRANSIENT_ERROR = "transient_error";

  /** DescribeTable failed with an unexpected exception type. */
  public static final String UNEXPECTED_ERROR = "unexpected_error";

  private KeyRouteAffinityMetricLabels() {}
}
