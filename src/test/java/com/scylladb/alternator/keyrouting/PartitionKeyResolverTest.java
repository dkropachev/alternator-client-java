package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private KeyRouteAffinityMetrics metricsCallback;

  @Before
  public void setUp() {
    metricsCallback = mock(KeyRouteAffinityMetrics.class);
    resolver = new PartitionKeyResolver(null, metricsCallback);
    mockClient = mock(DynamoDbClient.class);
  }

  @After
  public void tearDown() {
    if (resolver != null) {
      resolver.shutdown();
    }
  }

  @Test
  public void testPreConfiguredPartitionKeys() {
    Map<String, String> preConfigured = new HashMap<>();
    preConfigured.put("users", "user_id");
    preConfigured.put("orders", "order_id");

    resolver.shutdown();
    resolver = new PartitionKeyResolver(preConfigured, metricsCallback);

    assertEquals("user_id", resolver.getPartitionKeyName("users"));
    assertEquals("order_id", resolver.getPartitionKeyName("orders"));
    assertNull(resolver.getPartitionKeyName("unknown"));

    verify(metricsCallback).onPartitionKeyCacheHit("users");
    verify(metricsCallback).onPartitionKeyCacheHit("orders");
    verify(metricsCallback).onPartitionKeyCacheMiss("unknown");
  }

  @Test
  public void testManualRegistration() {
    assertNull(resolver.getPartitionKeyName("products"));

    resolver.register("products", "product_id");

    assertEquals("product_id", resolver.getPartitionKeyName("products"));
    assertTrue(resolver.hasPartitionKeyInfo("products"));
    verify(metricsCallback, atLeastOnce()).onPartitionKeyCacheSizeChanged(1);
  }

  @Test
  public void testSuccessfulDiscovery() throws Exception {
    DescribeTableResponse response = createDescribeTableResponse("session_id");
    CountDownLatch latch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            invocation -> {
              latch.countDown();
              return response;
            });

    resolver.triggerDiscovery("sessions", mockClient);

    assertTrue("Discovery should complete", latch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertEquals("session_id", resolver.getPartitionKeyName("sessions"));
    assertFalse(resolver.isInFailureCooldown("sessions"));
    verify(metricsCallback).onPartitionKeyDiscoveryTriggered("sessions");
    verify(metricsCallback).onPartitionKeyDiscoverySucceeded("sessions", "session_id");
    verify(metricsCallback, atLeastOnce()).onPartitionKeyCacheSizeChanged(1);
  }

  @Test
  public void testDiscoverySkipsAlreadyCached() {
    resolver.register("users", "user_id");

    resolver.triggerDiscovery("users", mockClient);

    verify(mockClient, never()).describeTable(any(DescribeTableRequest.class));
  }

  @Test
  public void testRetryOnTransientError() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  int attempt = attempts.incrementAndGet();
                  if (attempt < 3) {
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
    Thread.sleep(100);

    assertEquals("item_id", resolver.getPartitionKeyName("items"));
    assertEquals(3, attempts.get());
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
    assertEquals(1, attempts.get());
    assertTrue(resolver.isInFailureCooldown("nonexistent"));
    assertEquals(1, resolver.getFailedTableCount());
    verify(metricsCallback)
        .onPartitionKeyDiscoveryFailed(
            "nonexistent", KeyRouteAffinityMetricLabels.RESOURCE_NOT_FOUND);
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
    assertEquals(1, attempts.get());
    assertTrue(resolver.isInFailureCooldown("protected"));
    verify(metricsCallback)
        .onPartitionKeyDiscoveryFailed("protected", KeyRouteAffinityMetricLabels.ACCESS_DENIED);
  }

  @Test
  public void testRetryOnThrottling() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            (Answer<DescribeTableResponse>)
                invocation -> {
                  int attempt = attempts.incrementAndGet();
                  if (attempt < 3) {
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
    assertEquals(3, attempts.get());
  }

  @Test
  public void testMaxRetriesExceeded() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

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
    assertFalse(resolver.isInFailureCooldown("failing"));
    assertEquals(0, resolver.getFailedTableCount());
    verify(metricsCallback, atLeastOnce()).onPartitionKeyFailedTablesChanged(0);
  }

  @Test
  public void testClearFailure() throws Exception {
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

    assertTrue(resolver.isInFailureCooldown("missing"));
    resolver.clearFailure("missing");
    assertFalse(resolver.isInFailureCooldown("missing"));
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
  public void testInfoLogOnDiscoveryFailure() throws Exception {
    Logger logger = Logger.getLogger(PartitionKeyResolver.class.getName());
    List<LogRecord> records = new java.util.ArrayList<>();
    Handler handler = new CapturingHandler(records);
    Level originalLevel = logger.getLevel();

    logger.addHandler(handler);
    logger.setLevel(Level.INFO);
    try {
      CountDownLatch latch = new CountDownLatch(1);
      when(mockClient.describeTable(any(DescribeTableRequest.class)))
          .thenAnswer(
              invocation -> {
                latch.countDown();
                throw ResourceNotFoundException.builder().message("Not found").build();
              });

      resolver.triggerDiscovery("missing_table", mockClient);

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      Thread.sleep(100);

      assertTrue(
          records.stream()
              .anyMatch(
                  record ->
                      record.getLevel() == Level.INFO
                          && record
                              .getMessage()
                              .contains("PK discovery failed: table={0}, reason={1}")));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
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

    resolver.triggerDiscovery("blocked", mockClient);
    assertTrue(firstLatch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertTrue(resolver.isInFailureCooldown("blocked"));
    int attemptAfterFirstFailure = attempts.get();

    resolver.triggerDiscovery("blocked", mockClient);
    Thread.sleep(100);

    assertEquals(attemptAfterFirstFailure, attempts.get());
  }

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
                  startLatch.await();
                  completeLatch.countDown();
                  return createDescribeTableResponse("concurrent_pk");
                });

    resolver.triggerDiscovery("concurrent", mockClient);
    resolver.triggerDiscovery("concurrent", mockClient);
    resolver.triggerDiscovery("concurrent", mockClient);

    Thread.sleep(100);

    startLatch.countDown();
    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    assertEquals("concurrent_pk", resolver.getPartitionKeyName("concurrent"));
    assertEquals(1, attempts.get());
  }

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

  private static class CapturingHandler extends Handler {
    private final List<LogRecord> records;

    CapturingHandler(List<LogRecord> records) {
      this.records = records;
    }

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }
}
