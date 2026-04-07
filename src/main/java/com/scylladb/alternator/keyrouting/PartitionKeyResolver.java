package com.scylladb.alternator.keyrouting;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/**
 * Resolves partition key attribute names for DynamoDB tables.
 *
 * <p>Caches results to avoid repeated DescribeTable calls. Supports both pre-configured partition
 * key info and automatic discovery.
 *
 * <p><strong>Retry Behavior:</strong> Transient failures (network errors, throttling, server
 * errors) are retried with exponential backoff up to {@link #MAX_RETRIES} times. Permanent failures
 * (table not found, access denied) are not retried but allow future discovery attempts after a
 * cooldown period.
 *
 * <p><strong>Limitation:</strong> Auto-discovery via {@link #triggerDiscovery(String,
 * DynamoDbClient)} only works with synchronous clients ({@link DynamoDbClient}). For async clients,
 * pre-configure partition key names using {@link KeyRouteAffinityConfig.Builder#withPkInfo(String,
 * String)} to avoid discovery calls.
 *
 * <p><strong>Resource Management:</strong> This class manages an internal executor for async
 * discovery. Call {@link #close()} or {@link #shutdown()} when done to release resources.
 * Implements {@link AutoCloseable} for use with try-with-resources.
 *
 * <p>Thread-safe for concurrent access.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public class PartitionKeyResolver implements AutoCloseable {

  private static final Logger logger = Logger.getLogger(PartitionKeyResolver.class.getName());
  static final int MAX_RETRIES = 3;
  static final long INITIAL_RETRY_DELAY_MS = 100;
  static final long MAX_RETRY_DELAY_MS = 2000;
  static final long PERMANENT_FAILURE_COOLDOWN_MS = 5 * 60 * 1000;
  static final double MAX_JITTER_PERCENT = 0.2;

  private final ConcurrentHashMap<String, String> cache;
  private final Set<String> discoveryInProgress;
  private final ConcurrentHashMap<String, FailureRecord> failedTables;
  private final ExecutorService discoveryExecutor;
  private final KeyRouteAffinityMetrics metrics;

  private static class FailureRecord {
    final long timestamp;

    FailureRecord() {
      this.timestamp = System.currentTimeMillis();
    }

    boolean canRetry() {
      return System.currentTimeMillis() - timestamp > PERMANENT_FAILURE_COOLDOWN_MS;
    }
  }

  public PartitionKeyResolver(Map<String, String> preConfigured) {
    this(preConfigured, KeyRouteAffinityMetrics.NO_OP);
  }

  public PartitionKeyResolver(Map<String, String> preConfigured, KeyRouteAffinityMetrics metrics) {
    this.cache = new ConcurrentHashMap<>();
    if (preConfigured != null) {
      this.cache.putAll(preConfigured);
    }
    this.discoveryInProgress = ConcurrentHashMap.newKeySet();
    this.failedTables = new ConcurrentHashMap<>();
    this.metrics = metrics != null ? metrics : KeyRouteAffinityMetrics.NO_OP;
    this.discoveryExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "pk-discovery");
              t.setDaemon(true);
              return t;
            });
    notifyCacheSizeChanged();
    notifyFailedTableCountChanged();
  }

  public String getPartitionKeyName(String tableName) {
    String partitionKeyName = cache.get(tableName);
    if (tableName != null) {
      if (partitionKeyName != null) {
        metrics.onPartitionKeyCacheHit(tableName);
      } else {
        metrics.onPartitionKeyCacheMiss(tableName);
      }
    }
    return partitionKeyName;
  }

  public void triggerDiscovery(String tableName, DynamoDbClient client) {
    if (cache.containsKey(tableName)) {
      return;
    }

    FailureRecord failureRecord = failedTables.get(tableName);
    if (failureRecord != null && !failureRecord.canRetry()) {
      return;
    }

    if (!discoveryInProgress.add(tableName)) {
      return;
    }
    if (cache.containsKey(tableName)) {
      discoveryInProgress.remove(tableName);
      return;
    }

    if (failedTables.remove(tableName) != null) {
      notifyFailedTableCountChanged();
    }

    metrics.onPartitionKeyDiscoveryTriggered(tableName);
    discoveryExecutor.submit(() -> discoverWithRetry(tableName, client));
  }

  private void discoverWithRetry(String tableName, DynamoDbClient client) {
    int attempt = 0;
    long delay = INITIAL_RETRY_DELAY_MS;

    try {
      while (attempt <= MAX_RETRIES) {
        try {
          DescribeTableResponse response =
              client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());

          for (KeySchemaElement element : response.table().keySchema()) {
            if (element.keyType() == KeyType.HASH) {
              String pkName = element.attributeName();
              cache.put(tableName, pkName);
              logger.log(
                  Level.INFO,
                  "PK discovery completed: table={0}, pk_attribute={1}",
                  new Object[] {tableName, pkName});
              metrics.onPartitionKeyDiscoverySucceeded(tableName, pkName);
              notifyCacheSizeChanged();
              return;
            }
          }
          recordPermanentFailure(tableName, KeyRouteAffinityMetricLabels.NO_HASH_KEY, null);
          return;

        } catch (ResourceNotFoundException e) {
          recordPermanentFailure(
              tableName, KeyRouteAffinityMetricLabels.RESOURCE_NOT_FOUND, e.getMessage());
          return;

        } catch (DynamoDbException e) {
          if (isPermanentFailure(e)) {
            recordPermanentFailure(tableName, permanentFailureReason(e), e.getMessage());
            return;
          }

          attempt++;
          if (attempt > MAX_RETRIES) {
            recordTransientFailure(
                tableName, KeyRouteAffinityMetricLabels.TRANSIENT_ERROR, e.getMessage());
            return;
          }

          long jitteredDelay = calculateJitteredDelay(delay);
          logger.log(
              Level.FINE,
              "Transient error discovering partition key for table {0}, retry {1}/{2} after {3}ms: {4}",
              new Object[] {tableName, attempt, MAX_RETRIES, jitteredDelay, e.getMessage()});

          sleep(jitteredDelay);
          delay = Math.min(delay * 2, MAX_RETRY_DELAY_MS);

        } catch (Exception e) {
          attempt++;
          if (attempt > MAX_RETRIES) {
            recordTransientFailure(
                tableName, KeyRouteAffinityMetricLabels.UNEXPECTED_ERROR, e.getMessage());
            return;
          }

          long jitteredDelay = calculateJitteredDelay(delay);
          if (logger.isLoggable(Level.FINE)) {
            logger.log(
                Level.FINE,
                "Transient error discovering partition key for table "
                    + tableName
                    + ", retry "
                    + attempt
                    + "/"
                    + MAX_RETRIES
                    + " after "
                    + jitteredDelay
                    + "ms",
                e);
          }

          sleep(jitteredDelay);
          delay = Math.min(delay * 2, MAX_RETRY_DELAY_MS);
        }
      }
    } finally {
      discoveryInProgress.remove(tableName);
    }
  }

  private boolean isPermanentFailure(DynamoDbException e) {
    if (e.statusCode() == 403) {
      return true;
    }
    if (e.awsErrorDetails() != null) {
      String errorCode = e.awsErrorDetails().errorCode();
      return "AccessDeniedException".equals(errorCode) || "ValidationException".equals(errorCode);
    }
    return e.statusCode() >= 400 && e.statusCode() < 500 && e.statusCode() != 429;
  }

  private String permanentFailureReason(DynamoDbException e) {
    if (e.statusCode() == 403) {
      return KeyRouteAffinityMetricLabels.ACCESS_DENIED;
    }
    if (e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null) {
      String errorCode = e.awsErrorDetails().errorCode();
      if ("ValidationException".equals(errorCode)) {
        return KeyRouteAffinityMetricLabels.VALIDATION_ERROR;
      }
      return KeyRouteAffinityMetricLabels.CLIENT_ERROR;
    }
    return KeyRouteAffinityMetricLabels.CLIENT_ERROR;
  }

  private long calculateJitteredDelay(long baseDelay) {
    double jitterRange = baseDelay * MAX_JITTER_PERCENT;
    double jitter = (Math.random() * 2 - 1) * jitterRange;
    return Math.max(1, Math.round(baseDelay + jitter));
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  public void register(String tableName, String pkAttributeName) {
    cache.put(tableName, pkAttributeName);
    notifyCacheSizeChanged();
  }

  public boolean hasPartitionKeyInfo(String tableName) {
    return cache.containsKey(tableName);
  }

  public boolean isInFailureCooldown(String tableName) {
    FailureRecord record = failedTables.get(tableName);
    return record != null && !record.canRetry();
  }

  public void clearFailure(String tableName) {
    if (failedTables.remove(tableName) != null) {
      notifyFailedTableCountChanged();
    }
  }

  public int getFailedTableCount() {
    return failedTables.size();
  }

  private void recordPermanentFailure(String tableName, String reason, String detail) {
    failedTables.put(tableName, new FailureRecord());
    notifyFailedTableCountChanged();
    metrics.onPartitionKeyDiscoveryFailed(tableName, reason);
    logFailure(Level.INFO, tableName, reason, detail);
  }

  private void recordTransientFailure(String tableName, String reason, String detail) {
    failedTables.remove(tableName);
    notifyFailedTableCountChanged();
    metrics.onPartitionKeyDiscoveryFailed(tableName, reason);
    logFailure(Level.WARNING, tableName, reason, detail);
  }

  private void logFailure(Level level, String tableName, String reason, String detail) {
    String message =
        detail == null || detail.isEmpty()
            ? "PK discovery failed: table={0}, reason={1}"
            : "PK discovery failed: table={0}, reason={1}, detail={2}";
    if (detail == null || detail.isEmpty()) {
      logger.log(level, message, new Object[] {tableName, reason});
    } else {
      logger.log(level, message, new Object[] {tableName, reason, detail});
    }
  }

  private void notifyCacheSizeChanged() {
    metrics.onPartitionKeyCacheSizeChanged(cache.size());
  }

  private void notifyFailedTableCountChanged() {
    metrics.onPartitionKeyFailedTablesChanged(failedTables.size());
  }

  @Override
  public void close() {
    shutdown();
  }

  public void shutdown() {
    discoveryExecutor.shutdown();
    try {
      if (!discoveryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        discoveryExecutor.shutdownNow();
        logger.log(
            Level.FINE,
            "Partition key discovery executor did not terminate gracefully, forced shutdown");
      }
    } catch (InterruptedException e) {
      discoveryExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
