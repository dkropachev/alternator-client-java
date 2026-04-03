package com.scylladb.alternator.keyrouting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for key-based route affinity.
 *
 * <p>This class holds the affinity type, optional pre-configured partition key information per
 * table, and optional observability callbacks for metrics collection. If partition key info is not
 * provided for a table, it will be discovered automatically via DescribeTable.
 *
 * <p><strong>Important:</strong> Key route affinity only works reliably with synchronous DynamoDB
 * clients ({@link software.amazon.awssdk.services.dynamodb.DynamoDbClient}). With async clients,
 * the ThreadLocal-based context passing mechanism may fail due to cross-thread execution. Do not
 * use key route affinity with {@link software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * KeyRouteAffinityConfig config = KeyRouteAffinityConfig.builder()
 *     .withType(KeyRouteAffinity.RMW)
 *     .withPkInfo("users", "user_id")
 *     .withPkInfo("orders", "order_id")
 *     .withMetricsListener(myMetricsListener)
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public class KeyRouteAffinityConfig {
  private final KeyRouteAffinity type;
  private final Map<String, String> pkInfoPerTable;
  private final KeyRouteAffinityMetricsListener metricsListener;

  private KeyRouteAffinityConfig(
      KeyRouteAffinity type,
      Map<String, String> pkInfoPerTable,
      KeyRouteAffinityMetricsListener metricsListener) {
    this.type = type != null ? type : KeyRouteAffinity.NONE;
    this.pkInfoPerTable = Collections.unmodifiableMap(new HashMap<>(pkInfoPerTable));
    this.metricsListener = metricsListener;
  }

  /**
   * Returns the route affinity type.
   *
   * @return the affinity type, never null
   */
  public KeyRouteAffinity getType() {
    return type;
  }

  /**
   * Returns the pre-configured partition key info per table.
   *
   * @return unmodifiable map of table name to partition key attribute name
   */
  public Map<String, String> getPkInfoPerTable() {
    return pkInfoPerTable;
  }

  /**
   * Returns the optional metrics listener used for affinity observability.
   *
   * <p>A {@code null} value means metrics callbacks are disabled.
   *
   * @return the metrics listener, or {@code null} if disabled
   * @since 2.1.0
   */
  public KeyRouteAffinityMetricsListener getMetricsListener() {
    return metricsListener;
  }

  /**
   * Checks if route affinity is enabled (type is not NONE).
   *
   * @return true if route affinity is enabled
   */
  public boolean isEnabled() {
    return type != KeyRouteAffinity.NONE;
  }

  /**
   * Checks if metrics callbacks are enabled.
   *
   * @return true if a metrics listener is configured
   */
  public boolean isMetricsEnabled() {
    return metricsListener != null;
  }

  /**
   * Creates a new builder for KeyRouteAffinityConfig.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a config with the specified type and no pre-configured PK info.
   *
   * @param type the route affinity type
   * @return a new config instance
   */
  public static KeyRouteAffinityConfig of(KeyRouteAffinity type) {
    return new KeyRouteAffinityConfig(type, Collections.<String, String>emptyMap(), null);
  }

  /** Builder for {@link KeyRouteAffinityConfig}. */
  public static class Builder {
    private KeyRouteAffinity type = KeyRouteAffinity.NONE;
    private final Map<String, String> pkInfoPerTable = new HashMap<>();
    private KeyRouteAffinityMetricsListener metricsListener;

    Builder() {}

    /**
     * Sets the route affinity type.
     *
     * @param type the affinity type
     * @return this builder
     */
    public Builder withType(KeyRouteAffinity type) {
      this.type = type;
      return this;
    }

    /**
     * Adds partition key info for a table.
     *
     * @param tableName the table name
     * @param pkAttributeName the partition key attribute name
     * @return this builder
     */
    public Builder withPkInfo(String tableName, String pkAttributeName) {
      if (tableName != null && pkAttributeName != null) {
        pkInfoPerTable.put(tableName, pkAttributeName);
      }
      return this;
    }

    /**
     * Adds partition key info for multiple tables.
     *
     * @param pkInfo map of table name to partition key attribute name
     * @return this builder
     */
    public Builder withPkInfoMap(Map<String, String> pkInfo) {
      if (pkInfo != null) {
        pkInfoPerTable.putAll(pkInfo);
      }
      return this;
    }

    /**
     * Enables optional key affinity metrics callbacks.
     *
     * <p>Metrics are disabled by default. Supply a thread-safe, non-blocking listener to export
     * counters and gauges to your preferred metrics backend.
     *
     * @param metricsListener the metrics listener, or {@code null} to disable callbacks
     * @return this builder
     * @since 2.1.0
     */
    public Builder withMetricsListener(KeyRouteAffinityMetricsListener metricsListener) {
      this.metricsListener = metricsListener;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return a new KeyRouteAffinityConfig instance
     */
    public KeyRouteAffinityConfig build() {
      return new KeyRouteAffinityConfig(type, pkInfoPerTable, metricsListener);
    }
  }
}
