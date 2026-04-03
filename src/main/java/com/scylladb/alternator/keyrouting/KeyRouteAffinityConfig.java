package com.scylladb.alternator.keyrouting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for key-based route affinity.
 *
 * <p>This class holds the affinity type and optional pre-configured partition key information per
 * table. If partition key info is not provided for a table, it will be discovered automatically via
 * DescribeTable.
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
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public class KeyRouteAffinityConfig {
  private final KeyRouteAffinity type;
  private final Map<String, String> pkInfoPerTable;
  private final KeyRouteAffinityMetricsCollector metricsCollector;

  private KeyRouteAffinityConfig(
      KeyRouteAffinity type,
      Map<String, String> pkInfoPerTable,
      KeyRouteAffinityMetricsCollector metricsCollector) {
    this.type = type != null ? type : KeyRouteAffinity.NONE;
    this.pkInfoPerTable = Collections.unmodifiableMap(new HashMap<>(pkInfoPerTable));
    this.metricsCollector =
        metricsCollector != null ? metricsCollector : KeyRouteAffinityMetricsCollector.NO_OP;
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
   * Returns the configured metrics collector.
   *
   * <p>If metrics were not configured, this returns a no-op collector.
   *
   * @return the metrics collector, never null
   */
  public KeyRouteAffinityMetricsCollector getMetricsCollector() {
    return metricsCollector;
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
    return new KeyRouteAffinityConfig(
        type,
        Collections.<String, String>emptyMap(),
        KeyRouteAffinityMetricsCollector.NO_OP);
  }

  /** Builder for {@link KeyRouteAffinityConfig}. */
  public static class Builder {
    private KeyRouteAffinity type = KeyRouteAffinity.NONE;
    private final Map<String, String> pkInfoPerTable = new HashMap<>();
    private KeyRouteAffinityMetricsCollector metricsCollector =
        KeyRouteAffinityMetricsCollector.NO_OP;

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
     * Sets an optional metrics collector for affinity decisions and partition-key discovery.
     *
     * <p>Pass {@code null} to disable metrics collection.
     *
     * @param metricsCollector the metrics collector to use
     * @return this builder
     */
    public Builder withMetricsCollector(KeyRouteAffinityMetricsCollector metricsCollector) {
      this.metricsCollector =
          metricsCollector != null
              ? metricsCollector
              : KeyRouteAffinityMetricsCollector.NO_OP;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return a new KeyRouteAffinityConfig instance
     */
    public KeyRouteAffinityConfig build() {
      return new KeyRouteAffinityConfig(type, pkInfoPerTable, metricsCollector);
    }
  }
}
