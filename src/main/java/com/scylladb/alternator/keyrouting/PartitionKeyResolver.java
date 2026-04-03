package com.scylladb.alternator.keyrouting;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
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

  public enum DiscoveryTriggerResult {
    TRIGGERED("triggered"),
    ALREADY_IN_PROGRESS("already_in_progress"),
    COOLDOWN_ACTIVE("cooldown_active"),
    ALREADY_CACHED("already_cached");

    private final String logValue;

    DiscoveryTriggerResult(String logValue) {
      this.logValue = logValue;
    }

    public String getLogValue() {
      return logValue;
    }
  }

  private final ConcurrentHashMap<String, String> cache;
  private final Set<String> discoveryInProgress;
  private final ConcurrentHashMap<String, FailureRecord> failedTables;
  private final ExecutorService discoveryExecutor;
  private final KeyRouteAffinityMetricsCollector metricsCollector;
  private final boolean metricsEnabled;

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
    this(preConfigured, KeyRouteAffinityMetricsCollector.NO_OP);
  }

  /**
   * Creates a new resolver with pre-configured partition key info and an optional metrics
   * collector.
   *
   * @param preConfigured map of table name to partition key attribute name
   * @param metricsCollector metrics collector to notify, or {@code null} to disable metrics
   */
  public PartitionKeyResolver(
      Map<String, String> preConfigured, KeyRouteAffinityMetricsCollector metricsCollector) {
    this.cache = new ConcurrentHashMap<>();
    if (preConfigured != null) {
      this.cache.putAll(preConfigured);
    }
    this.discoveryInProgress = ConcurrentHashMap.newKeySet();
    this.failedTables = new ConcurrentHashMap<>();
    this.discoveryExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "pk-discovery");
              t.setDaemon(true);
              return t;
            });
    this.metricsCollector =
        metricsCollector != null
            ? metricsCollector
            : KeyRouteAffinityMetricsCollector.NO_OP;
    this.metricsEnabled = this.metricsCollector != KeyRouteAffinityMetricsCollector.NO_OP;

    updateCacheSizeGauge();
    updateFailedTableCountGauge();
  }

  /**
   * Gets the cached partition key name for a table.
   *
   * @param tableName the table name
   * @return the partition key attribute name, or null if not yet known
   */
  public String getPartitionKeyName(String tableName) {
    String pkName = cache.get(tableName);
    if (metricsEnabled) {
      if (pkName != null) {
        metricsCollector.onPartitionKeyCacheHit(tableName);
      } else {
        metricsCollector.onPartitionKeyCacheMiss(tableName);
      }
    }
    return pkName;
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
    triggerDiscoveryInternal(tableName, client);
  }

  public DiscoveryTriggerResult triggerDiscoveryWithResult(String tableName, DynamoDbClient client) {
    return triggerDiscoveryInternal(tableName, client);
  }

  private DiscoveryTriggerResult triggerDiscoveryInternal(String tableName, DynamoDbClient client) {
    if (cache.containsKey(tableName)) {
      return DiscoveryTriggerResult.ALREADY_CACHED;
    }

    // Check if this table previously failed and is still in cooldown
    FailureRecord failureRecord = failedTables.get(tableName);
    if (failureRecord != null && !failureRecord.canRetry()) {
      return DiscoveryTriggerResult.COOLDOWN_ACTIVE;
    }

    if (!discoveryInProgress.add(tableName)) {
      return DiscoveryTriggerResult.ALREADY_IN_PROGRESS;
    }

    // Double-check after acquiring the discovery lock to avoid race condition
    // where another thread may have populated the cache between our first check
    // and acquiring the lock
    if (cache.containsKey(tableName)) {
      discoveryInProgress.remove(tableName);
      return DiscoveryTriggerResult.ALREADY_CACHED;
    }

    // Clear any previous failure record since we're retrying
    if (failedTables.remove(tableName) != null) {
      updateFailedTableCountGauge();
    }

    if (metricsEnabled) {
      metricsCollector.onPartitionKeyDiscoveryTriggered(tableName);
    }

    discoveryExecutor.submit(() -> discoverWithRetry(tableName, client));
    return DiscoveryTriggerResult.TRIGGERED;
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
              handleDiscoverySuccess(tableName, element.attributeName());
              return;
            }
          }

          // No HASH key found - this shouldn't happen for valid tables
          recordFailure(
              tableName,
              true,
              PartitionKeyDiscoveryFailureReason.NO_HASH_KEY,
              Level.INFO,
              "PK discovery failed: table={0}, reason={1}",
              new Object[] {
                tableName, PartitionKeyDiscoveryFailureReason.NO_HASH_KEY.getTagValue()
              });
          return;

        } catch (ResourceNotFoundException e) {
          // Table doesn't exist - permanent failure, don't retry
          recordFailure(
              tableName,
              true,
              PartitionKeyDiscoveryFailureReason.TABLE_NOT_FOUND,
              Level.INFO,
              "PK discovery failed: table={0}, reason={1}",
              new Object[] {tableName, PartitionKeyDiscoveryFailureReason.TABLE_NOT_FOUND.getTagValue()});
          return;

        } catch (DynamoDbException e) {
          if (isPermanentFailure(e)) {
            PartitionKeyDiscoveryFailureReason reason = classifyPermanentFailure(e);
            recordFailure(
                tableName,
                true,
                reason,
                Level.INFO,
                "PK discovery failed: table={0}, reason={1}",
                new Object[] {tableName, reason.getTagValue()});
            return;
          }

          // Transient error - retry with backoff
          attempt++;
          if (attempt > MAX_RETRIES) {
            recordFailure(
                tableName,
                false,
                PartitionKeyDiscoveryFailureReason.MAX_RETRIES_EXCEEDED,
                Level.INFO,
                "PK discovery failed: table={0}, reason={1}, attempts={2}",
                new Object[] {
                  tableName,
                  PartitionKeyDiscoveryFailureReason.MAX_RETRIES_EXCEEDED.getTagValue(),
                  MAX_RETRIES + 1
                });
            return;
          }

          long jitteredDelay = calculateJitteredDelay(delay);
          logger.log(
              Level.FINE,
              "Transient error discovering partition key for table {0}, retry {1}/{2} after {3}ms: {4}",
              new Object[] {tableName, attempt, MAX_RETRIES, jitteredDelay, safeMessage(e)});

          sleep(jitteredDelay);
          delay = Math.min(delay * 2, MAX_RETRY_DELAY_MS);

        } catch (Exception e) {
          // Network errors or other transient issues - retry with backoff
          attempt++;
          if (attempt > MAX_RETRIES) {
            recordFailure(
                tableName,
                false,
                PartitionKeyDiscoveryFailureReason.MAX_RETRIES_EXCEEDED,
                Level.INFO,
                "PK discovery failed: table={0}, reason={1}, attempts={2}",
                new Object[] {
                  tableName,
                  PartitionKeyDiscoveryFailureReason.MAX_RETRIES_EXCEEDED.getTagValue(),
                  MAX_RETRIES + 1
                });
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

  private void handleDiscoverySuccess(String tableName, String partitionKeyName) {
    String previous = cache.put(tableName, partitionKeyName);
    logger.log(
        Level.INFO,
        "PK discovery completed: table={0}, pk_attribute={1}",
        new Object[] {tableName, partitionKeyName});

    if (metricsEnabled) {
      metricsCollector.onPartitionKeyDiscoverySuccess(tableName, partitionKeyName);
    }
    if (previous == null) {
      updateCacheSizeGauge();
    }
  }

  private void recordFailure(
      String tableName,
      boolean permanent,
      PartitionKeyDiscoveryFailureReason reason,
      Level level,
      String message,
      Object[] args) {
    logger.log(level, message, args);
    failedTables.put(tableName, new FailureRecord(permanent));
    if (metricsEnabled) {
      metricsCollector.onPartitionKeyDiscoveryFailed(tableName, reason);
    }
    updateFailedTableCountGauge();
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

  private PartitionKeyDiscoveryFailureReason classifyPermanentFailure(DynamoDbException e) {
    if (e.statusCode() == 403) {
      return PartitionKeyDiscoveryFailureReason.ACCESS_DENIED;
    }

    AwsErrorDetails awsErrorDetails = e.awsErrorDetails();
    if (awsErrorDetails == null) {
      return PartitionKeyDiscoveryFailureReason.PERMANENT_ERROR;
    }

    String errorCode = awsErrorDetails.errorCode();
    if ("AccessDeniedException".equals(errorCode)) {
      return PartitionKeyDiscoveryFailureReason.ACCESS_DENIED;
    }
    if ("ValidationException".equals(errorCode)) {
      return PartitionKeyDiscoveryFailureReason.VALIDATION_ERROR;
    }
    return PartitionKeyDiscoveryFailureReason.PERMANENT_ERROR;
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
    String previous = cache.put(tableName, pkAttributeName);
    if (failedTables.remove(tableName) != null) {
      updateFailedTableCountGauge();
    }
    if (previous == null) {
      updateCacheSizeGauge();
    }
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
      updateFailedTableCountGauge();
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

  private void updateCacheSizeGauge() {
    if (metricsEnabled) {
      metricsCollector.onPartitionKeyCacheSizeChanged(cache.size());
    }
  }

  private void updateFailedTableCountGauge() {
    if (metricsEnabled) {
      metricsCollector.onPartitionKeyFailedTableCountChanged(getFailedTableCount());
    }
  }

  private String safeMessage(Throwable throwable) {
    return throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getName();
  }
}
