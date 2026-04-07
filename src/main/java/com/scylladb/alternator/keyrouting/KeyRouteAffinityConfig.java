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
  private final KeyRouteAffinityMetrics metrics;

  private KeyRouteAffinityConfig(
      KeyRouteAffinity type, Map<String, String> pkInfoPerTable, KeyRouteAffinityMetrics metrics) {
    this.type = type != null ? type : KeyRouteAffinity.NONE;
    this.pkInfoPerTable = Collections.unmodifiableMap(new HashMap<>(pkInfoPerTable));
    this.metrics = metrics != null ? metrics : KeyRouteAffinityMetrics.NO_OP;
  }

  public KeyRouteAffinity getType() {
    return type;
  }

  public Map<String, String> getPkInfoPerTable() {
    return pkInfoPerTable;
  }

  public KeyRouteAffinityMetrics getMetrics() {
    return metrics;
  }

  public boolean isEnabled() {
    return type != KeyRouteAffinity.NONE;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static KeyRouteAffinityConfig of(KeyRouteAffinity type) {
    return new KeyRouteAffinityConfig(
        type, Collections.<String, String>emptyMap(), KeyRouteAffinityMetrics.NO_OP);
  }

  /** Builder for {@link KeyRouteAffinityConfig}. */
  public static class Builder {
    private KeyRouteAffinity type = KeyRouteAffinity.NONE;
    private final Map<String, String> pkInfoPerTable = new HashMap<>();
    private KeyRouteAffinityMetrics metrics = KeyRouteAffinityMetrics.NO_OP;

    Builder() {}

    public Builder withType(KeyRouteAffinity type) {
      this.type = type;
      return this;
    }

    public Builder withPkInfo(String tableName, String pkAttributeName) {
      if (tableName != null && pkAttributeName != null) {
        pkInfoPerTable.put(tableName, pkAttributeName);
      }
      return this;
    }

    public Builder withPkInfoMap(Map<String, String> pkInfo) {
      if (pkInfo != null) {
        pkInfoPerTable.putAll(pkInfo);
      }
      return this;
    }

    public Builder withMetrics(KeyRouteAffinityMetrics metrics) {
      this.metrics = metrics != null ? metrics : KeyRouteAffinityMetrics.NO_OP;
      return this;
    }

    public KeyRouteAffinityConfig build() {
      return new KeyRouteAffinityConfig(type, pkInfoPerTable, metrics);
    }
  }
}
