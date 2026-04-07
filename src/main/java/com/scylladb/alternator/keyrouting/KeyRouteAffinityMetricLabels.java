package com.scylladb.alternator.keyrouting;

/**
 * Backward-compatible string aliases for {@link KeyRouteAffinityMetricLabel} values.
 *
 * @author dmitry.kropachev
 * @since 2.0.5
 */
public final class KeyRouteAffinityMetricLabels {

  /** Fallback table label used when a request does not expose a table name. */
  public static final String UNKNOWN_TABLE = KeyRouteAffinityMetricLabel.UNKNOWN_TABLE.value();

  /** Request did not qualify for affinity in the configured mode. */
  public static final String NOT_QUALIFYING_REQUEST =
      KeyRouteAffinityMetricLabel.NOT_QUALIFYING_REQUEST.value();

  /** Request did not expose a table name. */
  public static final String TABLE_NAME_MISSING =
      KeyRouteAffinityMetricLabel.TABLE_NAME_MISSING.value();

  /** Partition-key metadata was not cached for the table. */
  public static final String PK_NOT_CACHED = KeyRouteAffinityMetricLabel.PK_NOT_CACHED.value();

  /** Request did not contain the expected partition-key value. */
  public static final String PK_VALUE_MISSING =
      KeyRouteAffinityMetricLabel.PK_VALUE_MISSING.value();

  /** DescribeTable returned no HASH key in the table schema. */
  public static final String NO_HASH_KEY = KeyRouteAffinityMetricLabel.NO_HASH_KEY.value();

  /** DescribeTable returned a resource-not-found error. */
  public static final String RESOURCE_NOT_FOUND =
      KeyRouteAffinityMetricLabel.RESOURCE_NOT_FOUND.value();

  /** DescribeTable failed with an access denied error. */
  public static final String ACCESS_DENIED = KeyRouteAffinityMetricLabel.ACCESS_DENIED.value();

  /** DescribeTable failed with a validation error. */
  public static final String VALIDATION_ERROR =
      KeyRouteAffinityMetricLabel.VALIDATION_ERROR.value();

  /** DescribeTable failed with another non-retryable client error. */
  public static final String CLIENT_ERROR = KeyRouteAffinityMetricLabel.CLIENT_ERROR.value();

  /** DescribeTable exhausted retries after transient failures. */
  public static final String TRANSIENT_ERROR = KeyRouteAffinityMetricLabel.TRANSIENT_ERROR.value();

  /** DescribeTable failed with an unexpected exception type. */
  public static final String UNEXPECTED_ERROR =
      KeyRouteAffinityMetricLabel.UNEXPECTED_ERROR.value();

  private KeyRouteAffinityMetricLabels() {}
}
