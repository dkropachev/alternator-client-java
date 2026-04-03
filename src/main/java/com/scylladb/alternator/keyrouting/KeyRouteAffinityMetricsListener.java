package com.scylladb.alternator.keyrouting;

/**
 * Optional callback interface for key route affinity metrics.
 *
 * <p>Metrics are disabled by default. Provide an implementation via {@link
 * KeyRouteAffinityConfig.Builder#withMetricsListener(KeyRouteAffinityMetricsListener)} to receive
 * request and partition-key discovery events and export them to a metrics backend such as
 * Prometheus, Micrometer, or OpenTelemetry.
 *
 * <p>Implementations must be thread-safe, non-blocking, and must not throw.
 *
 * @author dmitry.kropachev
 * @since 2.1.0
 */
public interface KeyRouteAffinityMetricsListener {

  /**
   * Called for every request processed by the affinity interceptor.
   *
   * @param tableName the DynamoDB table name, or {@code null} when unavailable
   * @param mode the configured affinity mode
   */
  default void onRequest(String tableName, KeyRouteAffinity mode) {}

  /**
   * Called when a request uses deterministic key affinity routing.
   *
   * @param tableName the DynamoDB table name
   * @param mode the configured affinity mode
   */
  default void onAffinityApplied(String tableName, KeyRouteAffinity mode) {}

  /**
   * Called when a request falls back to round-robin routing.
   *
   * @param tableName the DynamoDB table name, or {@code null} when unavailable
   * @param mode the configured affinity mode
   * @param reason the fallback reason
   */
  default void onRoundRobinFallback(
      String tableName, KeyRouteAffinity mode, KeyRouteAffinityFallbackReason reason) {}

  /**
   * Called when partition-key metadata is found in the local cache.
   *
   * @param tableName the DynamoDB table name
   */
  default void onPartitionKeyCacheHit(String tableName) {}

  /**
   * Called when partition-key metadata is missing from the local cache.
   *
   * @param tableName the DynamoDB table name
   */
  default void onPartitionKeyCacheMiss(String tableName) {}

  /**
   * Called when asynchronous partition-key discovery is scheduled.
   *
   * @param tableName the DynamoDB table name
   */
  default void onPartitionKeyDiscoveryTriggered(String tableName) {}

  /**
   * Called when asynchronous partition-key discovery completes successfully.
   *
   * @param tableName the DynamoDB table name
   * @param partitionKeyName the discovered partition key attribute name
   */
  default void onPartitionKeyDiscoverySuccess(String tableName, String partitionKeyName) {}

  /**
   * Called when asynchronous partition-key discovery fails.
   *
   * @param tableName the DynamoDB table name
   * @param reason a stable reason code or exception type
   */
  default void onPartitionKeyDiscoveryFailed(String tableName, String reason) {}

  /**
   * Called when the partition-key cache size changes.
   *
   * @param cacheSize the current cache size
   */
  default void onPartitionKeyCacheSizeChanged(int cacheSize) {}

  /**
   * Called when the number of tables in failure cooldown changes.
   *
   * @param failedTableCount the current number of failed tables
   */
  default void onFailedTableCountChanged(int failedTableCount) {}
}
