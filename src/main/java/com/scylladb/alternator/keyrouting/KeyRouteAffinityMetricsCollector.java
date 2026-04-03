package com.scylladb.alternator.keyrouting;

import java.net.URI;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Callback interface for optional key route affinity metrics integration.
 *
 * <p>Applications can provide an implementation via {@link
 * KeyRouteAffinityConfig.Builder#withMetricsCollector(KeyRouteAffinityMetricsCollector)} to bridge
 * key route affinity events into a metrics backend such as Micrometer, Prometheus, or a custom
 * in-house system.
 *
 * <p>When no collector is configured, the library uses {@link #NO_OP} and does not emit metrics
 * callbacks.
 *
 * <p>Implementations must be thread-safe because callbacks can be invoked from both request threads
 * and the background partition-key discovery thread.
 */
public interface KeyRouteAffinityMetricsCollector {

  /** Shared no-op collector used when metrics are disabled. */
  KeyRouteAffinityMetricsCollector NO_OP = new KeyRouteAffinityMetricsCollector() {};

  /**
   * Called for every request processed by the affinity interceptor.
   *
   * @param tableName the table name when available, otherwise {@code null}
   * @param mode the configured affinity mode
   */
  default void onRequest(String tableName, KeyRouteAffinity mode) {}

  /**
   * Called when a request is routed using deterministic key affinity.
   *
   * @param tableName the table name
   * @param mode the configured affinity mode
   * @param partitionKeyName the partition key attribute name
   * @param partitionKeyValue the partition key value used for hashing
   * @param targetNode the selected Alternator node
   */
  default void onAffinityApplied(
      String tableName,
      KeyRouteAffinity mode,
      String partitionKeyName,
      AttributeValue partitionKeyValue,
      URI targetNode) {}

  /**
   * Called when the interceptor falls back to round-robin routing.
   *
   * @param tableName the table name when available, otherwise {@code null}
   * @param mode the configured affinity mode
   * @param reason the fallback reason
   */
  default void onRoundRobinFallback(
      String tableName, KeyRouteAffinity mode, KeyRouteAffinityFallbackReason reason) {}

  /**
   * Called when partition key metadata is found in the resolver cache.
   *
   * @param tableName the table name
   */
  default void onPartitionKeyCacheHit(String tableName) {}

  /**
   * Called when partition key metadata is not yet present in the resolver cache.
   *
   * @param tableName the table name
   */
  default void onPartitionKeyCacheMiss(String tableName) {}

  /**
   * Called when asynchronous partition key discovery is scheduled for a table.
   *
   * @param tableName the table name
   */
  default void onPartitionKeyDiscoveryTriggered(String tableName) {}

  /**
   * Called when partition key discovery completes successfully.
   *
   * @param tableName the table name
   * @param partitionKeyName the discovered partition key attribute name
   */
  default void onPartitionKeyDiscoverySuccess(String tableName, String partitionKeyName) {}

  /**
   * Called when partition key discovery reaches a terminal failure state.
   *
   * @param tableName the table name
   * @param reason the failure reason
   */
  default void onPartitionKeyDiscoveryFailed(
      String tableName, PartitionKeyDiscoveryFailureReason reason) {}

  /**
   * Called whenever the partition key cache size changes.
   *
   * @param cacheSize the current number of cached table-to-partition-key mappings
   */
  default void onPartitionKeyCacheSizeChanged(int cacheSize) {}

  /**
   * Called whenever the number of tables in failure cooldown changes.
   *
   * @param failedTableCount the current number of tables in permanent-failure cooldown
   */
  default void onPartitionKeyFailedTableCountChanged(int failedTableCount) {}
}
