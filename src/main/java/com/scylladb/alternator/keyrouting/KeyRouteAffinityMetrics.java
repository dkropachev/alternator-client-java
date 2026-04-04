package com.scylladb.alternator.keyrouting;

/**
 * Optional callback interface for key route affinity metrics.
 *
 * <p>The library stays metrics-backend agnostic and emits lightweight callbacks only when a metrics
 * implementation is configured via {@link KeyRouteAffinityConfig.Builder#withMetrics}. Users can
 * bridge these callbacks to Micrometer, Prometheus, CloudWatch, or a custom metrics system without
 * adding a hard dependency to this library.
 *
 * <p>Canonical metric names:
 *
 * <ul>
 *   <li>{@code alternator.affinity.requests.total}
 *   <li>{@code alternator.affinity.requests.affinity}
 *   <li>{@code alternator.affinity.requests.roundrobin}
 *   <li>{@code alternator.affinity.pk.cache.hits}
 *   <li>{@code alternator.affinity.pk.cache.misses}
 *   <li>{@code alternator.affinity.pk.discovery.triggered}
 *   <li>{@code alternator.affinity.pk.discovery.success}
 *   <li>{@code alternator.affinity.pk.discovery.failed}
 *   <li>{@code alternator.affinity.pk.cache.size}
 *   <li>{@code alternator.affinity.pk.failed.tables}
 * </ul>
 *
 * <p>Recommended labels/tags: {@code table}, {@code mode}, and {@code reason}.
 *
 * <p>Known reason values:
 *
 * <ul>
 *   <li>Request fallback reasons: {@code not_qualifying_request}, {@code table_name_missing},
 *       {@code pk_not_cached}, {@code pk_value_missing}
 *   <li>PK discovery failure reasons: {@code no_hash_key}, {@code resource_not_found}, {@code
 *       access_denied}, {@code validation_error}, {@code client_error}, {@code transient_error},
 *       {@code unexpected_error}
 * </ul>
 *
 * <p>All methods are no-ops by default so implementations can override only the callbacks they
 * need.
 *
 * @author dmitry.kropachev
 * @since 2.0.5
 */
public interface KeyRouteAffinityMetrics {

  /**
   * Called for every request processed by the affinity interceptor.
   *
   * @param tableName the table name, or {@code unknown} when unavailable
   * @param mode the configured affinity mode
   */
  default void onRequest(String tableName, KeyRouteAffinity mode) {}

  /**
   * Called when affinity routing is applied.
   *
   * @param tableName the table name
   * @param mode the configured affinity mode
   */
  default void onAffinityApplied(String tableName, KeyRouteAffinity mode) {}

  /**
   * Called when a request falls back to round-robin routing.
   *
   * @param tableName the table name, or {@code unknown} when unavailable
   * @param mode the configured affinity mode
   * @param reason the fallback reason label
   */
  default void onAffinitySkipped(String tableName, KeyRouteAffinity mode, String reason) {}

  /**
   * Called when a cached partition key mapping is found.
   *
   * @param tableName the table name
   */
  default void onPartitionKeyCacheHit(String tableName) {}

  /**
   * Called when a cached partition key mapping is missing.
   *
   * @param tableName the table name
   */
  default void onPartitionKeyCacheMiss(String tableName) {}

  /**
   * Called when a new partition key discovery attempt is scheduled.
   *
   * @param tableName the table name
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
   * @param reason the failure reason label
   */
  default void onPartitionKeyDiscoveryFailed(String tableName, String reason) {}

  /**
   * Called whenever the partition key cache size changes.
   *
   * @param size the current cache size
   */
  default void onPartitionKeyCacheSizeChanged(int size) {}

  /**
   * Called whenever the failed-table count changes.
   *
   * @param count the current number of tables in failure cooldown
   */
  default void onPartitionKeyFailedTablesChanged(int count) {}
}
