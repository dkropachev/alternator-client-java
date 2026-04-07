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
 * <p>Metrics and structured decision logging are optional. When disabled, the affinity path keeps
 * the same behavior as before and avoids metrics callback overhead.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * KeyRouteAffinityConfig config = KeyRouteAffinityConfig.builder()
 *     .withType(KeyRouteAffinity.RMW)
 *     .withPkInfo("users", "user_id")
 *     .withMetricsEnabled(true)
 *     .withDebugLoggingEnabled(true)
 *     .withMetricsCallback(new AffinityMetricsCallback() {
 *       @Override
 *       public void onAffinityApplied(String table, String pkValue, URI targetNode, KeyRouteAffinity mode) {
 *         // Forward to your metrics backend here.
 *       }
 *     })
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public class KeyRouteAffinityConfig {
  private final KeyRouteAffinity type;
  private final Map<String, String> pkInfoPerTable;
  private final boolean metricsEnabled;
  private final boolean debugLoggingEnabled;
  private final AffinityMetricsCallback metricsCallback;

  private KeyRouteAffinityConfig(
      KeyRouteAffinity type,
      Map<String, String> pkInfoPerTable,
      boolean metricsEnabled,
      boolean debugLoggingEnabled,
      AffinityMetricsCallback metricsCallback) {
    this.type = type != null ? type : KeyRouteAffinity.NONE;
    this.pkInfoPerTable = Collections.unmodifiableMap(new HashMap<>(pkInfoPerTable));
    this.metricsEnabled = metricsEnabled;
    this.debugLoggingEnabled = debugLoggingEnabled;
    this.metricsCallback = metricsCallback != null ? metricsCallback : AffinityMetricsCallback.NO_OP;
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
   * Checks if route affinity is enabled (type is not NONE).
   *
   * @return true if route affinity is enabled
   */
  public boolean isEnabled() {
    return type != KeyRouteAffinity.NONE;
  }

  /**
   * Returns whether metrics callbacks should be invoked.
   *
   * @return true when metrics are enabled
   */
  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /**
   * Returns whether detailed affinity decision logging is enabled.
   *
   * @return true when debug/info observability logs should be emitted
   */
  public boolean isDebugLoggingEnabled() {
    return debugLoggingEnabled;
  }

  /**
   * Returns the configured metrics callback.
   *
   * @return the callback, never null
   */
  public AffinityMetricsCallback getMetricsCallback() {
    return metricsCallback;
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
        false,
        false,
        AffinityMetricsCallback.NO_OP);
  }

  /** Builder for {@link KeyRouteAffinityConfig}. */
  public static class Builder {
    private KeyRouteAffinity type = KeyRouteAffinity.NONE;
    private final Map<String, String> pkInfoPerTable = new HashMap<>();
    private boolean metricsEnabled = false;
    private boolean debugLoggingEnabled = false;
    private AffinityMetricsCallback metricsCallback = AffinityMetricsCallback.NO_OP;

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
     * Enables or disables metrics callbacks.
     *
     * @param enabled true to enable metrics callbacks
     * @return this builder
     */
    public Builder withMetricsEnabled(boolean enabled) {
      this.metricsEnabled = enabled;
      return this;
    }

    /**
     * Enables or disables additional DEBUG/INFO affinity logging.
     *
     * @param enabled true to enable detailed logging
     * @return this builder
     */
    public Builder withDebugLoggingEnabled(boolean enabled) {
      this.debugLoggingEnabled = enabled;
      return this;
    }

    /**
     * Sets the metrics callback implementation.
     *
     * @param metricsCallback callback to receive affinity events
     * @return this builder
     */
    public Builder withMetricsCallback(AffinityMetricsCallback metricsCallback) {
      this.metricsCallback =
          metricsCallback != null ? metricsCallback : AffinityMetricsCallback.NO_OP;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return a new KeyRouteAffinityConfig instance
     */
    public KeyRouteAffinityConfig build() {
      return new KeyRouteAffinityConfig(
          type, pkInfoPerTable, metricsEnabled, debugLoggingEnabled, metricsCallback);
    }
  }
}
