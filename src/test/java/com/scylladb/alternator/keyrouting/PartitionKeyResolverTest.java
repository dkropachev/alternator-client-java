package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.text.MessageFormat;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Unit tests for PartitionKeyResolver.
 *
 * @author dmitry.kropachev
 */
public class PartitionKeyResolverTest {

  private PartitionKeyResolver resolver;
  private DynamoDbClient mockClient;
  private RecordingMetricsCollector metricsCollector;

  private static class RecordingMetricsCollector implements KeyRouteAffinityMetricsCollector {
    final AtomicInteger cacheHits = new AtomicInteger();
    final AtomicInteger cacheMisses = new AtomicInteger();
    final AtomicInteger discoveryTriggered = new AtomicInteger();
    final CopyOnWriteArrayList<String> discoverySuccess = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<PartitionKeyDiscoveryFailureReason> discoveryFailures =
        new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<Integer> cacheSizes = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<Integer> failedTableCounts = new CopyOnWriteArrayList<>();

    @Override
    public void onPartitionKeyCacheHit(String tableName) {
      cacheHits.incrementAndGet();
    }

    @Override
    public void onPartitionKeyCacheMiss(String tableName) {
      cacheMisses.incrementAndGet();
    }

    @Override
    public void onPartitionKeyDiscoveryTriggered(String tableName) {
      discoveryTriggered.incrementAndGet();
    }

    @Override
    public void onPartitionKeyDiscoverySuccess(String tableName, String partitionKeyName) {
      discoverySuccess.add(tableName + ":" + partitionKeyName);
    }

    @Override
    public void onPartitionKeyDiscoveryFailed(
        String tableName, PartitionKeyDiscoveryFailureReason reason) {
      discoveryFailures.add(reason);
    }

    @Override
    public void onPartitionKeyCacheSizeChanged(int cacheSize) {
      cacheSizes.add(cacheSize);
    }

    @Override
    public void onPartitionKeyFailedTableCountChanged(int failedTableCount) {
      failedTableCounts.add(failedTableCount);
    }
  }

  private static class TestLogHandler extends Handler {
    final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
      String message = record.getMessage();
      if (record.getParameters() != null) {
        message = MessageFormat.format(message, record.getParameters());
      }
      messages.add(message);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }

  @Before
  public void setUp() {
    metricsCollector = new RecordingMetricsCollector();
    resolver = new PartitionKeyResolver(null, metricsCollector);
    mockClient = mock(DynamoDbClient.class);
  }

  @After
  public void tearDown() {
    if (resolver != null) {
      resolver.shutdown();
    }
  }

  // ========== Basic functionality tests ==========

  @Test
  public void testPreConfiguredPartitionKeys() {
    Map<String, String> preConfigured = new HashMap<>();
    preConfigured.put("users", "user_id");
    preConfigured.put("orders", "order_id");

    resolver.shutdown(); // Shutdown default resolver
    resolver = new PartitionKeyResolver(preConfigured, metricsCollector);

    assertEquals("user_id", resolver.getPartitionKeyName("users"));
    assertEquals("order_id", resolver.getPartitionKeyName("orders"));
    assertNull(resolver.getPartitionKeyName("unknown"));
  }

  @Test
  public void testManualRegistration() {
    assertNull(resolver.getPartitionKeyName("products"));

    resolver.register("products", "product_id");

    assertEquals("product_id", resolver.getPartitionKeyName("products"));
    assertTrue(resolver.hasPartitionKeyInfo("products"));
  }

  @Test
  public void testMetricsCollectorTracksCacheHitsMissesAndGaugeChanges() {
    assertEquals(Arrays.asList(0), metricsCollector.cacheSizes);
    assertEquals(Arrays.asList(0), metricsCollector.failedTableCounts);

    assertNull(resolver.getPartitionKeyName("products"));
    assertEquals(1, metricsCollector.cacheMisses.get());

    resolver.register("products", "product_id");

    assertEquals("product_id", resolver.getPartitionKeyName("products"));
    assertEquals(1, metricsCollector.cacheHits.get());
    assertEquals(Integer.valueOf(1), metricsCollector.cacheSizes.get(metricsCollector.cacheSizes.size() - 1));
  }

  @Test
  public void testSuccessfulDiscovery() throws Exception {
    // Setup mock response
    DescribeTableResponse response = createDescribeTableResponse("session_id");
    when(mockClient.describeTable(any(DescribeTableRequest.class))).thenReturn(response);

    // Trigger discovery
    CountDownLatch latch = new CountDownLatch(1);
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            invocation -> {
              latch.countDown();
              return response;
            });

    resolver.triggerDiscovery("sessions", mockClient);

    // Wait for async discovery to complete
    assertTrue("Discovery should complete", latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100); // Give time for cache update

    assertEquals("session_id", resolver.getPartitionKeyName("sessions"));
    assertFalse(resolver.isInFailureCooldown("sessions"));
    assertEquals(1, metricsCollector.discoveryTriggered.get());
    assertTrue(metricsCollector.discoverySuccess.contains("sessions:session_id"));
    assertEquals(Integer.valueOf(1), metricsCollector.cacheSizes.get(metricsCollector.cacheSizes.size() - 1));
  }

  @Test
  public void testDiscoverySkipsAlreadyCached() {
    resolver.register("users", "user_id");

    // Should not call client since already cached
    resolver.triggerDiscovery("users", mockClient);

    verify(mockClient, never()).describeTable(any(DescribeTableRequest.class));
  }

  // ========== Retry logic tests ==========

  @Test
  public void testRetryOnTransientError() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    // Fail twice, then succeed
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  int attempt = attempts.incrementAndGet();
                  if (attempt < 3) {
                    // Simulate transient error (500 Internal Server Error)
                    throw DynamoDbException.builder()
                        .message("Internal Server Error")
                        .statusCode(500)
                        .build();
                  }
                  latch.countDown();
                  return createDescribeTableResponse("item_id");
                });

    resolver.triggerDiscovery("items", mockClient);

    assertTrue("Discovery should complete", latch.await(10, TimeUnit.SECONDS));
    Thread.sleep(100); // Give time for cache update

    assertEquals("item_id", resolver.getPartitionKeyName("items"));
    assertEquals(3, attempts.get()); // 2 failures + 1 success
  }

  @Test
  public void testNoRetryOnResourceNotFound() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  attempts.incrementAndGet();
                  latch.countDown();
                  throw ResourceNotFoundException.builder().message("Table not found").build();
                });

    resolver.triggerDiscovery("nonexistent", mockClient);

    assertTrue("Discovery should complete", latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertNull(resolver.getPartitionKeyName("nonexistent"));
    assertEquals(1, attempts.get()); // No retries
    assertTrue(resolver.isInFailureCooldown("nonexistent"));
  }

  @Test
  public void testNoRetryOnAccessDenied() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  attempts.incrementAndGet();
                  latch.countDown();
                  throw DynamoDbException.builder()
                      .message("Access Denied")
                      .statusCode(403)
                      .awsErrorDetails(
                          AwsErrorDetails.builder()
                              .errorCode("AccessDeniedException")
                              .errorMessage("Access Denied")
                              .build())
                      .build();
                });

    resolver.triggerDiscovery("protected", mockClient);

    assertTrue("Discovery should complete", latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertNull(resolver.getPartitionKeyName("protected"));
    assertEquals(1, attempts.get()); // No retries for permanent failure
    assertTrue(resolver.isInFailureCooldown("protected"));
  }

  @Test
  public void testRetryOnThrottling() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    // Throttle twice, then succeed
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  int attempt = attempts.incrementAndGet();
                  if (attempt < 3) {
                    // 429 Too Many Requests - should retry
                    throw DynamoDbException.builder()
                        .message("Rate exceeded")
                        .statusCode(429)
                        .awsErrorDetails(
                            AwsErrorDetails.builder()
                                .errorCode("ThrottlingException")
                                .errorMessage("Rate exceeded")
                                .build())
                        .build();
                  }
                  latch.countDown();
                  return createDescribeTableResponse("throttled_pk");
                });

    resolver.triggerDiscovery("throttled", mockClient);

    assertTrue("Discovery should complete", latch.await(10, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertEquals("throttled_pk", resolver.getPartitionKeyName("throttled"));
    assertEquals(3, attempts.get()); // 2 throttles + 1 success
  }

  @Test
  public void testMaxRetriesExceeded() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    // Always fail with transient error
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  int attempt = attempts.incrementAndGet();
                  if (attempt > PartitionKeyResolver.MAX_RETRIES) {
                    latch.countDown();
                  }
                  throw DynamoDbException.builder()
                      .message("Service Unavailable")
                      .statusCode(503)
                      .build();
                });

    resolver.triggerDiscovery("failing", mockClient);

    assertTrue("Discovery should complete after max retries", latch.await(10, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertNull(resolver.getPartitionKeyName("failing"));
    assertEquals(PartitionKeyResolver.MAX_RETRIES + 1, attempts.get());
    // Transient failures allow immediate retry via triggerDiscovery
    assertFalse(resolver.isInFailureCooldown("failing"));
  }

  // ========== Failure cooldown tests ==========

  @Test
  public void testClearFailure() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    // Fail with permanent error
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  latch.countDown();
                  throw ResourceNotFoundException.builder().message("Not found").build();
                });

    resolver.triggerDiscovery("missing", mockClient);
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertTrue(resolver.isInFailureCooldown("missing"));

    // Clear the failure
    resolver.clearFailure("missing");

    assertFalse(resolver.isInFailureCooldown("missing"));
    assertEquals(
        Integer.valueOf(0),
        metricsCollector.failedTableCounts.get(metricsCollector.failedTableCounts.size() - 1));
  }

  @Test
  public void testFailedTableCount() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  latch.countDown();
                  throw ResourceNotFoundException.builder().message("Not found").build();
                });

    resolver.triggerDiscovery("missing1", mockClient);
    resolver.triggerDiscovery("missing2", mockClient);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(200);

    assertEquals(2, resolver.getFailedTableCount());

    resolver.clearFailure("missing1");
    assertEquals(1, resolver.getFailedTableCount());
  }

  @Test
  public void testMetricsCollectorTracksDiscoveryFailure() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  latch.countDown();
                  throw ResourceNotFoundException.builder().message("Not found").build();
                });

    resolver.triggerDiscovery("missing", mockClient);
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertTrue(metricsCollector.discoveryFailures.contains(PartitionKeyDiscoveryFailureReason.TABLE_NOT_FOUND));
    assertEquals(
        Integer.valueOf(1),
        metricsCollector.failedTableCounts.get(metricsCollector.failedTableCounts.size() - 1));
  }

  @Test
  public void testDiscoverySuccessLoggedAtInfo() throws Exception {
    Logger logger = Logger.getLogger(PartitionKeyResolver.class.getName());
    TestLogHandler handler = new TestLogHandler();
    Level previousLevel = logger.getLevel();
    boolean previousUseParentHandlers = logger.getUseParentHandlers();
    logger.setLevel(Level.INFO);
    logger.setUseParentHandlers(false);
    logger.addHandler(handler);

    try {
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenReturn(createDescribeTableResponse("session_id"));

      resolver.triggerDiscovery("sessions", mockClient);
      Thread.sleep(200);

      assertTrue(handler.messages.contains("PK discovery completed: table=sessions, pk_attribute=session_id"));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
      logger.setUseParentHandlers(previousUseParentHandlers);
    }
  }

  @Test
  public void testDiscoveryFailureLoggedAtInfo() throws Exception {
    Logger logger = Logger.getLogger(PartitionKeyResolver.class.getName());
    TestLogHandler handler = new TestLogHandler();
    Level previousLevel = logger.getLevel();
    boolean previousUseParentHandlers = logger.getUseParentHandlers();
    logger.setLevel(Level.INFO);
    logger.setUseParentHandlers(false);
    logger.addHandler(handler);

    try {
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenThrow(ResourceNotFoundException.builder().message("Not found").build());

      resolver.triggerDiscovery("missing", mockClient);
      Thread.sleep(200);

      assertTrue(handler.messages.contains("PK discovery failed: table=missing, reason=table_not_found"));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
      logger.setUseParentHandlers(previousUseParentHandlers);
    }
  }

  @Test
  public void testDiscoveryBlockedDuringCooldown() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch firstLatch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  attempts.incrementAndGet();
                  firstLatch.countDown();
                  throw ResourceNotFoundException.builder().message("Not found").build();
                });

    // First discovery - fails
    resolver.triggerDiscovery("blocked", mockClient);
    assertTrue(firstLatch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertTrue(resolver.isInFailureCooldown("blocked"));
    int attemptAfterFirstFailure = attempts.get();

    // Second discovery - should be blocked by cooldown
    resolver.triggerDiscovery("blocked", mockClient);
    Thread.sleep(100);

    // No additional attempts should have been made
    assertEquals(attemptAfterFirstFailure, attempts.get());
  }

  // ========== Concurrent discovery tests ==========

  @Test
  public void testConcurrentDiscoveryForSameTable() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  attempts.incrementAndGet();
                  startLatch.await(); // Wait for signal to proceed
                  completeLatch.countDown();
                  return createDescribeTableResponse("concurrent_pk");
                });

    // Trigger multiple discoveries for the same table
    resolver.triggerDiscovery("concurrent", mockClient);
    resolver.triggerDiscovery("concurrent", mockClient);
    resolver.triggerDiscovery("concurrent", mockClient);

    Thread.sleep(100); // Let the discovery start

    // Release the blocked discovery
    startLatch.countDown();
    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertEquals("concurrent_pk", resolver.getPartitionKeyName("concurrent"));
    assertEquals(1, attempts.get()); // Only one actual discovery attempt
  }

  // ========== Helper methods ==========

  private DescribeTableResponse createDescribeTableResponse(String partitionKeyName) {
    return DescribeTableResponse.builder()
        .table(
            TableDescription.builder()
                .tableName("test-table")
                .keySchema(
                    Arrays.asList(
                        KeySchemaElement.builder()
                            .attributeName(partitionKeyName)
                            .keyType(KeyType.HASH)
                            .build(),
                        KeySchemaElement.builder()
                            .attributeName("sort_key")
                            .keyType(KeyType.RANGE)
                            .build()))
                .build())
        .build();
  }
}
