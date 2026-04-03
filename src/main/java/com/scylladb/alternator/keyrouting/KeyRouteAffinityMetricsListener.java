package com.scylladb.alternator.keyrouting;

import java.net.URI;

/**
 * Listener for optional key route affinity metrics and observability events.
 *
 * <p>All callbacks are optional and execute inline on request-processing or discovery threads, so
 * implementations should stay fast and non-blocking.
 *
 * @author dmitry.kropachev
 * @since 2.0.5
 */
public interface KeyRouteAffinityMetricsListener {

  /**
   * Called when a request uses key affinity routing.
   *
   * @param tableName the DynamoDB table name, or null if unavailable
   * @param mode the configured affinity mode
   * @param targetNode the selected Alternator node
   */
  default void onAffinityApplied(String tableName, KeyRouteAffinity mode, URI targetNode) {}

  /**
   * Called when a request falls back to round-robin routing.
   *
   * @param tableName the DynamoDB table name, or null if unavailable
   * @param mode the configured affinity mode
   * @param reason the fallback reason
   * @param targetNode the selected Alternator node
   */
  default void onAffinitySkipped(
      String tableName, KeyRouteAffinity mode, KeyRouteAffinitySkipReason reason, URI targetNode) {}

  /**
   * Called when partition key info is found in the resolver cache.
   *
   * @param tableName the table name
   * @param cacheSize the current number of cached table mappings
   */
  default void onPartitionKeyCacheHit(String tableName, int cacheSize) {}

  /**
   * Called when partition key info is missing from the resolver cache.
   *
   * @param tableName the table name
   * @param cacheSize the current number of cached table mappings
   */
  default void onPartitionKeyCacheMiss(String tableName, int cacheSize) {}

  /**
   * Called when the resolver cache size changes.
   *
   * @param cacheSize the current number of cached table mappings
   */
  default void onPartitionKeyCacheSizeChanged(int cacheSize) {}

  /**
   * Called when partition key discovery is submitted for asynchronous execution.
   *
   * @param tableName the table name being discovered
   */
  default void onPartitionKeyDiscoveryTriggered(String tableName) {}

  /**
   * Called when partition key discovery succeeds.
   *
   * @param tableName the table name
   * @param partitionKeyName the discovered partition key attribute name
   */
  default void onPartitionKeyDiscoverySucceeded(String tableName, String partitionKeyName) {}

  /**
   * Called when partition key discovery fails.
   *
   * @param tableName the table name
   * @param reason the failure reason
   */
  default void onPartitionKeyDiscoveryFailed(
      String tableName, PartitionKeyDiscoveryFailureReason reason) {}

  /**
   * Called when the number of tables in permanent-failure cooldown changes.
   *
   * @param failedTableCount the current cooldown table count
   */
  default void onPartitionKeyFailureCooldownSizeChanged(int failedTableCount) {}
}
