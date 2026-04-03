package com.scylladb.alternator.keyrouting;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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

  /**
   * Maximum number of retry attempts for transient failures.
   *
   * @see com.scylladb.alternator.AlternatorConfig#RECOMMENDED_PARTITION_KEY_DISCOVERY_MAX_RETRIES
   */
  static final int MAX_RETRIES = 3;

  /**
   * Initial delay between retries in milliseconds.
   *
   * @see
   *     com.scylladb.alternator.AlternatorConfig#RECOMMENDED_PARTITION_KEY_DISCOVERY_INITIAL_DELAY_MS
   */
  static final long INITIAL_RETRY_DELAY_MS = 100;

  /**
   * Maximum delay between retries in milliseconds.
   *
   * @see com.scylladb.alternator.AlternatorConfig#RECOMMENDED_PARTITION_KEY_DISCOVERY_MAX_DELAY_MS
   */
  static final long MAX_RETRY_DELAY_MS = 2000;

  /**
   * Cooldown period after permanent failure before allowing another discovery attempt (5 min).
   *
   * @see com.scylladb.alternator.AlternatorConfig#RECOMMENDED_PARTITION_KEY_DISCOVERY_COOLDOWN_MS
   */
  static final long PERMANENT_FAILURE_COOLDOWN_MS = 5 * 60 * 1000;

  /** Maximum jitter as a percentage of the base delay (20%). */
  static final double MAX_JITTER_PERCENT = 0.2;

  private final ConcurrentHashMap<String, String> cache;
  private final Set<String> discoveryInProgress;
  private final ConcurrentHashMap<String, FailureRecord> failedTables;
  private final ExecutorService discoveryExecutor;
  private final KeyRouteAffinityMetricsListener metricsListener;

  /** Records information about a failed discovery attempt. */
  private static class FailureRecord {
    final long timestamp;
    final boolean permanent;

    FailureRecord(boolean permanent) {
      this.timestamp = System.currentTimeMillis();
      this.permanent = permanent;
    }

    boolean canRetry() {
      if (!permanent) {
        return true; // Transient failures can always be retried via triggerDiscovery
      }
      // Permanent failures have a cooldown period
      return System.currentTimeMillis() - timestamp > PERMANENT_FAILURE_COOLDOWN_MS;
    }
  }

  /**
   * Creates a new resolver with pre-configured partition key info.
   *
   * @param preConfigured map of table name to partition key attribute name
   */
  public PartitionKeyResolver(Map<String, String> preConfigured) {
    this(preConfigured, null);
  }

  /**
   * Creates a new resolver with pre-configured partition key info and an optional metrics
   * listener.
   *
   * @param preConfigured map of table name to partition key attribute name
   * @param metricsListener optional metrics listener, or {@code null} to disable metrics callbacks
   * @since 2.1.0
   */
  public PartitionKeyResolver(
      Map<String, String> preConfigured, KeyRouteAffinityMetricsListener metricsListener) {
    this.cache = new ConcurrentHashMap<>();
    if (preConfigured != null) {
      this.cache.putAll(preConfigured);
    }
    this.discoveryInProgress = ConcurrentHashMap.newKeySet();
    this.failedTables = new ConcurrentHashMap<>();
    this.metricsListener = metricsListener;
    this.discoveryExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "pk-discovery");
              t.setDaemon(true);
              return t;
            });
    notifyMetrics(listener -> listener.onPartitionKeyCacheSizeChanged(cache.size()));
    notifyMetrics(listener -> listener.onFailedTableCountChanged(getFailedTableCount()));
  }

  /**
   * Gets the cached partition key name for a table.
   *
   * @param tableName the table name
   * @return the partition key attribute name, or null if not yet known
   */
  public String getPartitionKeyName(String tableName) {
    String partitionKeyName = cache.get(tableName);
    if (partitionKeyName != null) {
      notifyMetrics(listener -> listener.onPartitionKeyCacheHit(tableName));
    } else {
      notifyMetrics(listener -> listener.onPartitionKeyCacheMiss(tableName));
    }
    return partitionKeyName;
  }

  /**
   * Triggers async discovery of partition key for a table.
   *
   * <p>If discovery is already in progress for this table, this call is a no-op. Discovery happens
   * asynchronously and updates the cache when complete.
   *
   * <p>Transient failures (network errors, throttling) are retried with exponential backoff.
   * Permanent failures (table not found, access denied) are recorded and will block further
   * discovery attempts until a cooldown period expires.
   *
   * @param tableName the table name
   * @param client the DynamoDB client to use for DescribeTable
   */
  public void triggerDiscovery(String tableName, DynamoDbClient client) {
    triggerDiscoveryIfNeeded(tableName, client);
  }

  /**
   * Triggers async discovery of partition key for a table if needed.
   *
   * @param tableName the table name
   * @param client the DynamoDB client to use for DescribeTable
   * @return {@code true} if a new discovery task was scheduled
   * @since 2.1.0
   */
  public boolean triggerDiscoveryIfNeeded(String tableName, DynamoDbClient client) {
    if (cache.containsKey(tableName)) {
      return false; // Already cached
    }

    // Check if this table previously failed and is still in cooldown
    FailureRecord failureRecord = failedTables.get(tableName);
    if (failureRecord != null && !failureRecord.canRetry()) {
      return false; // Still in cooldown period after permanent failure
    }

    if (!discoveryInProgress.add(tableName)) {
      return false; // Discovery already in progress
    }
    // Double-check after acquiring the discovery lock to avoid race condition
    // where another thread may have populated the cache between our first check
    // and acquiring the lock
    if (cache.containsKey(tableName)) {
      discoveryInProgress.remove(tableName);
      return false; // Another thread cached it while we were waiting
    }

    // Clear any previous failure record since we're retrying
    if (failedTables.remove(tableName) != null) {
      notifyMetrics(listener -> listener.onFailedTableCountChanged(getFailedTableCount()));
    }

    notifyMetrics(listener -> listener.onPartitionKeyDiscoveryTriggered(tableName));
    discoveryExecutor.submit(() -> discoverWithRetry(tableName, client));
    return true;
  }

  /**
   * Performs discovery with exponential backoff retry for transient failures.
   *
   * @param tableName the table name
   * @param client the DynamoDB client
   */
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
              notifyMetrics(listener -> listener.onPartitionKeyDiscoverySuccess(tableName, pkName));
              notifyMetrics(listener -> listener.onPartitionKeyCacheSizeChanged(cache.size()));
              return; // Success
            }
          }
          // No HASH key found - this shouldn't happen for valid tables
          recordFailedDiscovery(tableName, "no_hash_key", true);
          return;

        } catch (ResourceNotFoundException e) {
          // Table doesn't exist - permanent failure, don't retry
          recordFailedDiscovery(tableName, failureReason(e), true);
          return;

        } catch (DynamoDbException e) {
          if (isPermanentFailure(e)) {
            // Access denied or other permanent error - don't retry
            recordFailedDiscovery(tableName, failureReason(e), true);
            return;
          }

          // Transient error - retry with backoff
          attempt++;
          if (attempt > MAX_RETRIES) {
            recordFailedDiscovery(tableName, "max_retries_exceeded", false);
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
          // Network errors or other transient issues - retry with backoff
          attempt++;
          if (attempt > MAX_RETRIES) {
            recordFailedDiscovery(tableName, "max_retries_exceeded", false);
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

  /**
   * Checks if a DynamoDB exception represents a permanent failure that should not be retried.
   *
   * @param e the exception to check
   * @return true if this is a permanent failure
   */
  private boolean isPermanentFailure(DynamoDbException e) {
    // Access denied is permanent
    if (e.statusCode() == 403) {
      return true;
    }
    if (e.awsErrorDetails() != null) {
      String errorCode = e.awsErrorDetails().errorCode();
      // AccessDeniedException and ValidationException are permanent
      return "AccessDeniedException".equals(errorCode) || "ValidationException".equals(errorCode);
    }
    // 4xx errors (except 429 throttling) are generally permanent
    return e.statusCode() >= 400 && e.statusCode() < 500 && e.statusCode() != 429;
  }

  /**
   * Calculates a jittered delay to prevent thundering herd.
   *
   * @param baseDelay the base delay in milliseconds
   * @return the jittered delay in milliseconds
   */
  private long calculateJitteredDelay(long baseDelay) {
    // Add random jitter: base delay ± up to MAX_JITTER_PERCENT
    double jitterRange = baseDelay * MAX_JITTER_PERCENT;
    double jitter =
        (Math.random() * 2 - 1) * jitterRange; // Random value in [-jitterRange, +jitterRange]
    return Math.max(1, Math.round(baseDelay + jitter)); // Ensure minimum 1ms delay
  }

  /**
   * Sleeps for the specified duration, handling interruption.
   *
   * @param millis the sleep duration in milliseconds
   */
  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Manually registers a table's partition key name.
   *
   * @param tableName the table name
   * @param pkAttributeName the partition key attribute name
   */
  public void register(String tableName, String pkAttributeName) {
    cache.put(tableName, pkAttributeName);
    if (failedTables.remove(tableName) != null) {
      notifyMetrics(listener -> listener.onFailedTableCountChanged(getFailedTableCount()));
    }
    notifyMetrics(listener -> listener.onPartitionKeyCacheSizeChanged(cache.size()));
  }

  /**
   * Checks if partition key info is available for a table.
   *
   * @param tableName the table name
   * @return true if PK info is cached
   */
  public boolean hasPartitionKeyInfo(String tableName) {
    return cache.containsKey(tableName);
  }

  /**
   * Checks if discovery for a table has failed and is in cooldown.
   *
   * @param tableName the table name
   * @return true if discovery failed and cannot be retried yet
   */
  public boolean isInFailureCooldown(String tableName) {
    FailureRecord record = failedTables.get(tableName);
    return record != null && !record.canRetry();
  }

  /**
   * Clears the failure record for a table, allowing immediate retry.
   *
   * <p>This is useful when the underlying issue has been resolved (e.g., table created, permissions
   * granted).
   *
   * @param tableName the table name
   */
  public void clearFailure(String tableName) {
    if (failedTables.remove(tableName) != null) {
      notifyMetrics(listener -> listener.onFailedTableCountChanged(getFailedTableCount()));
    }
  }

  /**
   * Returns the number of tables currently in failure cooldown.
   *
   * @return the count of failed tables
   */
  public int getFailedTableCount() {
    int count = 0;
    for (FailureRecord record : failedTables.values()) {
      if (!record.canRetry()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Closes this resolver and releases its resources.
   *
   * <p>This is an alias for {@link #shutdown()} to support {@link AutoCloseable}.
   */
  @Override
  public void close() {
    shutdown();
  }

  /**
   * Shuts down the discovery executor and waits for any in-flight tasks to complete.
   *
   * <p>Waits up to 5 seconds for graceful shutdown. If tasks don't complete in time, forces
   * immediate shutdown.
   */
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

  private void recordFailedDiscovery(String tableName, String reason, boolean permanent) {
    logger.log(
        Level.INFO,
        "PK discovery failed: table={0}, reason={1}",
        new Object[] {tableName, reason});
    failedTables.put(tableName, new FailureRecord(permanent));
    notifyMetrics(listener -> listener.onPartitionKeyDiscoveryFailed(tableName, reason));
    notifyMetrics(listener -> listener.onFailedTableCountChanged(getFailedTableCount()));
  }

  private String failureReason(Throwable throwable) {
    if (throwable instanceof ResourceNotFoundException) {
      return "resource_not_found";
    }
    if (throwable instanceof DynamoDbException) {
      DynamoDbException dynamoDbException = (DynamoDbException) throwable;
      if (dynamoDbException.awsErrorDetails() != null
          && dynamoDbException.awsErrorDetails().errorCode() != null
          && !dynamoDbException.awsErrorDetails().errorCode().isEmpty()) {
        String errorCode = dynamoDbException.awsErrorDetails().errorCode();
        if ("AccessDeniedException".equals(errorCode)) {
          return "access_denied";
        }
        if ("ValidationException".equals(errorCode)) {
          return "validation_exception";
        }
        return errorCode;
      }
    }
    return "unexpected_error";
  }

  private void notifyMetrics(Consumer<KeyRouteAffinityMetricsListener> callback) {
    if (metricsListener == null) {
      return;
    }
    try {
      callback.accept(metricsListener);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Key route affinity metrics listener threw an exception", e);
    }
  }
}
