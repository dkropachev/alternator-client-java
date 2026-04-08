package com.scylladb.alternator.keyrouting;

import java.net.URI;

/**
 * Optional callback for affinity metrics and routing diagnostics.
 *
 * <p>The callback is invoked only when metrics are enabled via {@link KeyRouteAffinityConfig}. It
 * is dependency-free so callers can adapt the events to Micrometer, Dropwizard, Prometheus, or a
 * custom metrics system without forcing a metrics library on every user of the client.
 *
 * <p>The metric names below mirror the issue requirements and are provided as constants for backend
 * adapters.
 */
public interface AffinityMetricsCallback {

  String REQUESTS_TOTAL = "alternator.affinity.requests.total";
  String REQUESTS_AFFINITY = "alternator.affinity.requests.affinity";
  String REQUESTS_ROUNDROBIN = "alternator.affinity.requests.roundrobin";
  String PK_CACHE_HITS = "alternator.affinity.pk.cache.hits";
  String PK_CACHE_MISSES = "alternator.affinity.pk.cache.misses";
  String PK_DISCOVERY_TRIGGERED = "alternator.affinity.pk.discovery.triggered";
  String PK_DISCOVERY_SUCCESS = "alternator.affinity.pk.discovery.success";
  String PK_DISCOVERY_FAILED = "alternator.affinity.pk.discovery.failed";
  String PK_CACHE_SIZE = "alternator.affinity.pk.cache.size";
  String PK_FAILED_TABLES = "alternator.affinity.pk.failed.tables";

  AffinityMetricsCallback NO_OP = new AffinityMetricsCallback() {};

  /** Called for every request processed by the affinity interceptor. */
  default void onRequestObserved(String tableName, KeyRouteAffinity mode) {}

  /** Called when a request is routed using deterministic key affinity. */
  default void onAffinityApplied(
      String tableName, String partitionKeyValue, URI targetNode, KeyRouteAffinity mode) {}

  /** Called when the interceptor falls back to round-robin routing. */
  default void onRoundRobinFallback(String tableName, KeyRouteAffinity mode, String reason) {}

  /** Called when a cached partition-key mapping is reused. */
  default void onPkCacheHit(String tableName) {}

  /** Called when a partition-key mapping is missing from the cache. */
  default void onPkCacheMiss(String tableName) {}

  /** Called when asynchronous partition-key discovery is kicked off. */
  default void onPkDiscoveryTriggered(String tableName) {}

  /** Called when partition-key discovery succeeds. */
  default void onPkDiscoverySucceeded(String tableName, String partitionKeyAttributeName) {}

  /** Called when partition-key discovery fails. */
  default void onPkDiscoveryFailed(String tableName, String reason) {}

  /** Called whenever the number of cached table-to-PK mappings changes. */
  default void onPkCacheSizeChanged(int size) {}

  /** Called whenever the failed-table cooldown set changes. */
  default void onFailedTablesCountChanged(int count) {}
}
