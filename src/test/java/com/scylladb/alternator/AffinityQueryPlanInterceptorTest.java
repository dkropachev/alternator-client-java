package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetricLabels;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetrics;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Comprehensive tests for AffinityQueryPlanInterceptor covering all DynamoDB operation variants
 * with RMW, ANY_WRITE, and NONE modes.
 *
 * <p>This test verifies routing behavior through a full DynamoDB client with mock HTTP, covering
 * 30+ operation variants to match Go's WithKeyRouteAffinity test coverage.
 *
 * <p>For each operation variant, the test checks whether affinity routing (same key always goes to
 * the same node) or round-robin routing (requests spread across nodes) is used, depending on the
 * affinity mode.
 *
 * @author dmitry.kropachev
 */
public class AffinityQueryPlanInterceptorTest {

  private static final String TABLE_NAME = "test_table";
  private static final String PK_NAME = "pk";
  private static final String PK_VALUE = "test-pk-value-123";

  private List<URI> testNodeUris;
  private MockSdkHttpClient mockHttpClient;

  @Before
  public void setUp() throws Exception {
    testNodeUris = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      testNodeUris.add(new URI("http://127.0.0." + i + ":8000"));
    }
    mockHttpClient = new MockSdkHttpClient();
  }

  @After
  public void tearDown() {
    mockHttpClient.close();
  }

  // ========== Helper methods ==========

  private DynamoDbClient createClient(KeyRouteAffinityConfig keyAffinity) {
    AlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(testNodeUris);
    ClientOverrideConfiguration.Builder overrideBuilder = ClientOverrideConfiguration.builder();
    if (keyAffinity != null && keyAffinity.isEnabled()) {
      overrideBuilder.addExecutionInterceptor(
          new AffinityQueryPlanInterceptor(keyAffinity, liveNodes));
    } else {
      overrideBuilder.addExecutionInterceptor(new BasicQueryPlanInterceptor(liveNodes));
    }
    return DynamoDbClient.builder()
        .region(Region.of("test-region"))
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .endpointOverride(testNodeUris.get(0))
        .httpClient(mockHttpClient)
        .overrideConfiguration(overrideBuilder.build())
        .build();
  }

  private KeyRouteAffinityConfig buildConfig(KeyRouteAffinity type) {
    return KeyRouteAffinityConfig.builder().withType(type).withPkInfo(TABLE_NAME, PK_NAME).build();
  }

  /** Verify that an operation routes to the same node consistently (affinity applies). */
  private void assertAffinityRouting(Runnable operation) {
    URI expectedUri = null;
    for (int i = 0; i < 5; i++) {
      mockHttpClient.clearCapturedRequests();
      operation.run();
      URI uri = mockHttpClient.getLastRequestUri();
      assertNotNull("Request should have been captured", uri);
      if (expectedUri == null) {
        expectedUri = uri;
      } else {
        assertEquals("Should route to same node (iteration " + i + ")", expectedUri, uri);
      }
    }
  }

  /** Verify that an operation routes to different nodes (round-robin, no affinity). */
  private void assertRoundRobinRouting(Runnable operation) {
    Set<URI> nodesUsed = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      mockHttpClient.clearCapturedRequests();
      operation.run();
      URI uri = mockHttpClient.getLastRequestUri();
      assertNotNull("Request should have been captured", uri);
      nodesUsed.add(uri);
    }
    assertTrue("Should use multiple nodes (got " + nodesUsed.size() + ")", nodesUsed.size() > 1);
  }

  private Map<String, AttributeValue> makeKey() {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(PK_NAME, AttributeValue.builder().s(PK_VALUE).build());
    return key;
  }

  private Map<String, AttributeValue> makeItem() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK_NAME, AttributeValue.builder().s(PK_VALUE).build());
    item.put("data", AttributeValue.builder().s("value").build());
    return item;
  }

  // ========== PutItem variants ==========

  // --- Simple PutItem (no conditions) ---

  @Test
  public void testPutItemSimple_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemSimple_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemSimple_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build()));
    } finally {
      client.close();
    }
  }

  // --- PutItem with conditionExpression ---

  @Test
  public void testPutItemWithConditionExpression_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .conditionExpression("attribute_not_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemWithConditionExpression_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .conditionExpression("attribute_not_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemWithConditionExpression_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .conditionExpression("attribute_not_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- PutItem with expected (legacy) ---

  @Test
  public void testPutItemWithExpected_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemWithExpected_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemWithExpected_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- PutItem with returnValues=ALL_OLD ---

  @Test
  public void testPutItemReturnValuesAllOld_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemReturnValuesAllOld_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemReturnValuesAllOld_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- PutItem with returnValues=NONE ---

  @Test
  public void testPutItemReturnValuesNone_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .returnValues(ReturnValue.NONE)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemReturnValuesNone_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .returnValues(ReturnValue.NONE)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemReturnValuesNone_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .item(makeItem())
                      .returnValues(ReturnValue.NONE)
                      .build()));
    } finally {
      client.close();
    }
  }

  // ========== UpdateItem variants ==========

  // --- Simple UpdateItem (no updates, no conditions) ---

  @Test
  public void testUpdateItemSimple_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemSimple_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemSimple_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with updateExpression ---

  @Test
  public void testUpdateItemWithUpdateExpression_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .updateExpression("SET #d = :v")
                      .expressionAttributeNames(Collections.singletonMap("#d", "data"))
                      .expressionAttributeValues(
                          Collections.singletonMap(":v", AttributeValue.builder().s("new").build()))
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemWithUpdateExpression_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .updateExpression("SET #d = :v")
                      .expressionAttributeNames(Collections.singletonMap("#d", "data"))
                      .expressionAttributeValues(
                          Collections.singletonMap(":v", AttributeValue.builder().s("new").build()))
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemWithUpdateExpression_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .updateExpression("SET #d = :v")
                      .expressionAttributeNames(Collections.singletonMap("#d", "data"))
                      .expressionAttributeValues(
                          Collections.singletonMap(":v", AttributeValue.builder().s("new").build()))
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with conditionExpression ---

  @Test
  public void testUpdateItemWithConditionExpression_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .conditionExpression("attribute_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemWithConditionExpression_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .conditionExpression("attribute_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemWithConditionExpression_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .conditionExpression("attribute_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with expected (legacy) ---

  @Test
  public void testUpdateItemWithExpected_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemWithExpected_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemWithExpected_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with returnValues=ALL_OLD ---

  @Test
  public void testUpdateItemReturnValuesAllOld_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesAllOld_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesAllOld_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with returnValues=UPDATED_OLD ---

  @Test
  public void testUpdateItemReturnValuesUpdatedOld_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.UPDATED_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesUpdatedOld_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.UPDATED_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesUpdatedOld_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.UPDATED_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with returnValues=ALL_NEW ---

  @Test
  public void testUpdateItemReturnValuesAllNew_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_NEW)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesAllNew_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_NEW)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesAllNew_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_NEW)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with returnValues=UPDATED_NEW (NOT RMW) ---

  @Test
  public void testUpdateItemReturnValuesUpdatedNew_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.UPDATED_NEW)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesUpdatedNew_Rmw_RoundRobin() {
    // UPDATED_NEW does NOT trigger RMW - can be computed from update alone
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.UPDATED_NEW)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemReturnValuesUpdatedNew_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.UPDATED_NEW)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with ADD action (legacy) ---

  @Test
  public void testUpdateItemAddAction_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "counter",
          AttributeValueUpdate.builder()
              .action(AttributeAction.ADD)
              .value(AttributeValue.builder().n("1").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemAddAction_Rmw_Affinity() {
    // ADD action triggers RMW because it requires reading the current value
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "counter",
          AttributeValueUpdate.builder()
              .action(AttributeAction.ADD)
              .value(AttributeValue.builder().n("1").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemAddAction_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "counter",
          AttributeValueUpdate.builder()
              .action(AttributeAction.ADD)
              .value(AttributeValue.builder().n("1").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with DELETE action + value (legacy) ---

  @Test
  public void testUpdateItemDeleteActionWithValue_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "tags",
          AttributeValueUpdate.builder()
              .action(AttributeAction.DELETE)
              .value(AttributeValue.builder().ss("old-tag").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemDeleteActionWithValue_Rmw_Affinity() {
    // DELETE action with value triggers RMW (removes elements from a set)
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "tags",
          AttributeValueUpdate.builder()
              .action(AttributeAction.DELETE)
              .value(AttributeValue.builder().ss("old-tag").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemDeleteActionWithValue_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "tags",
          AttributeValueUpdate.builder()
              .action(AttributeAction.DELETE)
              .value(AttributeValue.builder().ss("old-tag").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with DELETE action without value (NOT RMW) ---

  @Test
  public void testUpdateItemDeleteActionWithoutValue_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "obsolete_field", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemDeleteActionWithoutValue_Rmw_RoundRobin() {
    // DELETE without value just removes the attribute - no read needed
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "obsolete_field", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemDeleteActionWithoutValue_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "obsolete_field", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- UpdateItem with PUT action (NOT RMW) ---

  @Test
  public void testUpdateItemPutAction_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "data",
          AttributeValueUpdate.builder()
              .action(AttributeAction.PUT)
              .value(AttributeValue.builder().s("new-value").build())
              .build());
      assertAffinityRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemPutAction_Rmw_RoundRobin() {
    // PUT action is a simple overwrite - no read needed
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "data",
          AttributeValueUpdate.builder()
              .action(AttributeAction.PUT)
              .value(AttributeValue.builder().s("new-value").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemPutAction_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "data",
          AttributeValueUpdate.builder()
              .action(AttributeAction.PUT)
              .value(AttributeValue.builder().s("new-value").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .attributeUpdates(updates)
                      .build()));
    } finally {
      client.close();
    }
  }

  // ========== DeleteItem variants ==========

  // --- Simple DeleteItem ---

  @Test
  public void testDeleteItemSimple_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemSimple_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemSimple_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  // --- DeleteItem with conditionExpression ---

  @Test
  public void testDeleteItemWithConditionExpression_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .conditionExpression("attribute_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemWithConditionExpression_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .conditionExpression("attribute_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemWithConditionExpression_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .conditionExpression("attribute_exists(pk)")
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- DeleteItem with expected (legacy) ---

  @Test
  public void testDeleteItemWithExpected_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemWithExpected_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemWithExpected_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertRoundRobinRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .expected(expected)
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- DeleteItem with returnValues=ALL_OLD ---

  @Test
  public void testDeleteItemReturnValuesAllOld_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemReturnValuesAllOld_Rmw_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertAffinityRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemReturnValuesAllOld_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(TABLE_NAME)
                      .key(makeKey())
                      .returnValues(ReturnValue.ALL_OLD)
                      .build()));
    } finally {
      client.close();
    }
  }

  // ========== Read operations (never use affinity in any mode) ==========

  // --- GetItem ---

  @Test
  public void testGetItem_AnyWrite_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertRoundRobinRouting(
          () ->
              client.getItem(
                  GetItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testGetItem_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.getItem(
                  GetItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testGetItem_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.getItem(
                  GetItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  // --- Query ---

  @Test
  public void testQuery_AnyWrite_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertRoundRobinRouting(
          () ->
              client.query(
                  QueryRequest.builder()
                      .tableName(TABLE_NAME)
                      .keyConditionExpression("pk = :v")
                      .expressionAttributeValues(
                          Collections.singletonMap(
                              ":v", AttributeValue.builder().s(PK_VALUE).build()))
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testQuery_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () ->
              client.query(
                  QueryRequest.builder()
                      .tableName(TABLE_NAME)
                      .keyConditionExpression("pk = :v")
                      .expressionAttributeValues(
                          Collections.singletonMap(
                              ":v", AttributeValue.builder().s(PK_VALUE).build()))
                      .build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testQuery_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () ->
              client.query(
                  QueryRequest.builder()
                      .tableName(TABLE_NAME)
                      .keyConditionExpression("pk = :v")
                      .expressionAttributeValues(
                          Collections.singletonMap(
                              ":v", AttributeValue.builder().s(PK_VALUE).build()))
                      .build()));
    } finally {
      client.close();
    }
  }

  // --- Scan ---

  @Test
  public void testScan_AnyWrite_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertRoundRobinRouting(
          () -> client.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testScan_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRoundRobinRouting(
          () -> client.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testScan_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRoundRobinRouting(
          () -> client.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    } finally {
      client.close();
    }
  }

  // --- BatchWriteItem ---

  @Test
  public void testBatchWriteItem_AnyWrite_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder().putRequest(PutRequest.builder().item(makeItem()).build()).build());
      requestItems.put(TABLE_NAME, writes);

      assertRoundRobinRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItem_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder().putRequest(PutRequest.builder().item(makeItem()).build()).build());
      requestItems.put(TABLE_NAME, writes);

      assertRoundRobinRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItem_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder().putRequest(PutRequest.builder().item(makeItem()).build()).build());
      requestItems.put(TABLE_NAME, writes);

      assertRoundRobinRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  // --- BatchWriteItem with DeleteRequest ---

  @Test
  public void testBatchWriteItemDelete_AnyWrite_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder()
              .deleteRequest(DeleteRequest.builder().key(makeKey()).build())
              .build());
      requestItems.put(TABLE_NAME, writes);

      assertRoundRobinRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  // --- BatchGetItem ---

  @Test
  public void testBatchGetItem_AnyWrite_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          KeysAndAttributes.builder().keys(Collections.singletonList(makeKey())).build());

      assertRoundRobinRouting(
          () ->
              client.batchGetItem(
                  BatchGetItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchGetItem_Rmw_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          KeysAndAttributes.builder().keys(Collections.singletonList(makeKey())).build());

      assertRoundRobinRouting(
          () ->
              client.batchGetItem(
                  BatchGetItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchGetItem_None_RoundRobin() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          KeysAndAttributes.builder().keys(Collections.singletonList(makeKey())).build());

      assertRoundRobinRouting(
          () ->
              client.batchGetItem(
                  BatchGetItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  // ========== Cross-operation affinity consistency ==========

  @Test
  public void testSameKeyAcrossOperationTypes_AnyWrite_SameNode() {
    // In ANY_WRITE mode, PutItem, UpdateItem, and DeleteItem for the same key
    // should all route to the same node.
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      mockHttpClient.clearCapturedRequests();
      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build());
      URI putUri = mockHttpClient.getLastRequestUri();

      mockHttpClient.clearCapturedRequests();
      client.updateItem(UpdateItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build());
      URI updateUri = mockHttpClient.getLastRequestUri();

      mockHttpClient.clearCapturedRequests();
      client.deleteItem(DeleteItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build());
      URI deleteUri = mockHttpClient.getLastRequestUri();

      assertEquals(
          "PutItem and UpdateItem for same key should route to same node", putUri, updateUri);
      assertEquals(
          "PutItem and DeleteItem for same key should route to same node", putUri, deleteUri);
    } finally {
      client.close();
    }
  }

  @Test
  public void testSameKeyAcrossOperationTypes_Rmw_SameNode() {
    // In RMW mode, conditional operations for the same key should route to the same node.
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      mockHttpClient.clearCapturedRequests();
      client.putItem(
          PutItemRequest.builder()
              .tableName(TABLE_NAME)
              .item(makeItem())
              .conditionExpression("attribute_not_exists(pk)")
              .build());
      URI putUri = mockHttpClient.getLastRequestUri();

      mockHttpClient.clearCapturedRequests();
      client.updateItem(
          UpdateItemRequest.builder()
              .tableName(TABLE_NAME)
              .key(makeKey())
              .conditionExpression("attribute_exists(pk)")
              .build());
      URI updateUri = mockHttpClient.getLastRequestUri();

      mockHttpClient.clearCapturedRequests();
      client.deleteItem(
          DeleteItemRequest.builder()
              .tableName(TABLE_NAME)
              .key(makeKey())
              .conditionExpression("attribute_exists(pk)")
              .build());
      URI deleteUri = mockHttpClient.getLastRequestUri();

      assertEquals(
          "Conditional PutItem and UpdateItem for same key should route to same node",
          putUri,
          updateUri);
      assertEquals(
          "Conditional PutItem and DeleteItem for same key should route to same node",
          putUri,
          deleteUri);
    } finally {
      client.close();
    }
  }

  // ========== Null/disabled config ==========

  @Test
  public void testNullConfig_RoundRobin() {
    // Passing null config should use basic round-robin
    DynamoDbClient client = createClient(null);
    try {
      assertRoundRobinRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testMetricsAndDebugLogWhenAffinityApplied() {
    RecordingMetrics metrics = new RecordingMetrics();
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo(TABLE_NAME, PK_NAME)
            .withMetrics(metrics)
            .build();
    Logger logger = Logger.getLogger(AffinityQueryPlanInterceptor.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler = new CapturingHandler(records);
    Level originalLevel = logger.getLevel();
    boolean originalUseParentHandlers = logger.getUseParentHandlers();

    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
    logger.setUseParentHandlers(false);
    logger.setLevel(Level.FINE);
    try {
      AlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(testNodeUris);
      AffinityQueryPlanInterceptor interceptor =
          new AffinityQueryPlanInterceptor(config, liveNodes);
      ExecutionAttributes executionAttributes = new ExecutionAttributes();
      PutItemRequest request =
          PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build();
      Context.BeforeExecution beforeExecution = mock(Context.BeforeExecution.class);
      when(beforeExecution.request()).thenReturn(request);
      Context.ModifyHttpRequest modifyHttpRequest = mock(Context.ModifyHttpRequest.class);
      when(modifyHttpRequest.httpRequest())
          .thenReturn(
              SdkHttpRequest.builder()
                  .protocol("http")
                  .host("localhost")
                  .port(8000)
                  .method(SdkHttpMethod.POST)
                  .encodedPath("/")
                  .build());

      interceptor.beforeExecution(beforeExecution, executionAttributes);
      interceptor.modifyHttpRequest(modifyHttpRequest, executionAttributes);

      assertEquals(1, metrics.requestsTotal);
      assertEquals(1, metrics.requestsAffinity);
      assertEquals(0, metrics.requestsRoundRobin);
      assertEquals(1, metrics.cacheHits);
      assertEquals(0, metrics.cacheMisses);
      assertTrue(
          records.stream()
              .anyMatch(
                  record ->
                      record.getLevel() == Level.FINE
                          && "Key affinity applied: table={0}, pk_fingerprint={1}, target={2}"
                              .equals(record.getMessage())
                          && record.getParameters() != null
                          && record.getParameters().length == 3
                          && TABLE_NAME.equals(record.getParameters()[0])
                          && Long.toUnsignedString(
                                  AttributeValueHasher.hash(makeItem().get(PK_NAME)), 16)
                              .equals(record.getParameters()[1])
                          && record.getParameters()[2] instanceof URI));
    } finally {
      logger.removeHandler(handler);
      logger.setUseParentHandlers(originalUseParentHandlers);
      logger.setLevel(originalLevel);
    }
  }

  @Test
  public void testMetricsWhenTableNameMissingUseUnknownTableLabel() {
    RecordingMetrics metrics = new RecordingMetrics();
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withMetrics(metrics)
            .build();
    AlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(testNodeUris);
    AffinityQueryPlanInterceptor interceptor = new AffinityQueryPlanInterceptor(config, liveNodes);
    ExecutionAttributes executionAttributes = new ExecutionAttributes();
    UpdateItemRequest request = UpdateItemRequest.builder().key(makeKey()).build();
    Context.BeforeExecution beforeExecution = mock(Context.BeforeExecution.class);
    when(beforeExecution.request()).thenReturn(request);

    interceptor.beforeExecution(beforeExecution, executionAttributes);

    assertEquals(1, metrics.requestsTotal);
    assertEquals(0, metrics.requestsAffinity);
    assertEquals(1, metrics.requestsRoundRobin);
    assertEquals(KeyRouteAffinityMetricLabels.UNKNOWN_TABLE, metrics.lastRequestTable);
    assertEquals(KeyRouteAffinityMetricLabels.TABLE_NAME_MISSING, metrics.lastSkipReason);
  }

  @Test
  public void testMetricsAndDebugLogWhenAffinitySkippedForMissingPkCache() {
    RecordingMetrics metrics = new RecordingMetrics();
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withMetrics(metrics)
            .build();
    Logger logger = Logger.getLogger(AffinityQueryPlanInterceptor.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler = new CapturingHandler(records);
    Level originalLevel = logger.getLevel();
    boolean originalUseParentHandlers = logger.getUseParentHandlers();

    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
    logger.setUseParentHandlers(false);
    logger.setLevel(Level.FINE);
    try {
      AlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(testNodeUris);
      AffinityQueryPlanInterceptor interceptor =
          new AffinityQueryPlanInterceptor(config, liveNodes);
      ExecutionAttributes executionAttributes = new ExecutionAttributes();
      PutItemRequest request =
          PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build();
      Context.BeforeExecution beforeExecution = mock(Context.BeforeExecution.class);
      when(beforeExecution.request()).thenReturn(request);
      Context.ModifyHttpRequest modifyHttpRequest = mock(Context.ModifyHttpRequest.class);
      when(modifyHttpRequest.httpRequest())
          .thenReturn(
              SdkHttpRequest.builder()
                  .protocol("http")
                  .host("localhost")
                  .port(8000)
                  .method(SdkHttpMethod.POST)
                  .encodedPath("/")
                  .build());

      interceptor.beforeExecution(beforeExecution, executionAttributes);
      interceptor.modifyHttpRequest(modifyHttpRequest, executionAttributes);

      assertEquals(1, metrics.requestsTotal);
      assertEquals(0, metrics.requestsAffinity);
      assertEquals(1, metrics.requestsRoundRobin);
      assertEquals(0, metrics.cacheHits);
      assertEquals(1, metrics.cacheMisses);
      assertEquals(0, metrics.discoveryTriggered);
      assertEquals(KeyRouteAffinityMetricLabels.PK_NOT_CACHED, metrics.lastSkipReason);
      assertTrue(
          records.stream()
              .anyMatch(
                  record ->
                      record.getLevel() == Level.FINE
                          && "Key affinity skipped: table={0}, reason={1}"
                              .equals(record.getMessage())
                          && record.getParameters() != null
                          && record.getParameters().length == 2
                          && TABLE_NAME.equals(record.getParameters()[0])
                          && KeyRouteAffinityMetricLabels.PK_NOT_CACHED.equals(
                              record.getParameters()[1])));
    } finally {
      logger.removeHandler(handler);
      logger.setUseParentHandlers(originalUseParentHandlers);
      logger.setLevel(originalLevel);
    }
  }

  // ========== Mock implementations ==========

  /**
   * Mock AlternatorLiveNodes that provides a fixed list of nodes without network calls.
   *
   * <p>This mock:
   *
   * <ul>
   *   <li>Does not start any background threads
   *   <li>Provides a fixed list of test nodes
   *   <li>Supports LazyQueryPlan creation for key affinity (via base class)
   *   <li>Implements round-robin across all test nodes
   * </ul>
   */
  private static class MockAlternatorLiveNodes extends AlternatorLiveNodes {
    private final List<URI> nodes;
    private final java.util.concurrent.atomic.AtomicInteger counter =
        new java.util.concurrent.atomic.AtomicInteger(0);

    MockAlternatorLiveNodes(List<URI> nodes) {
      super(
          AlternatorConfig.builder()
              .withSeedHosts(Collections.singletonList(nodes.get(0).getHost()))
              .withScheme(nodes.get(0).getScheme())
              .withPort(nodes.get(0).getPort())
              .build());
      this.nodes = new ArrayList<>(nodes);
    }

    @Override
    protected List<URI> getLiveNodesInternal() {
      return nodes;
    }

    @Override
    public URI nextAsURI() {
      return nodes.get(Math.abs(counter.getAndIncrement() % nodes.size()));
    }

    @Override
    public void start() {}

    @Override
    public List<URI> getLiveNodes() {
      return Collections.unmodifiableList(new ArrayList<>(nodes));
    }
  }

  private static class RecordingMetrics implements KeyRouteAffinityMetrics {
    int requestsTotal;
    int requestsAffinity;
    int requestsRoundRobin;
    int cacheHits;
    int cacheMisses;
    int discoveryTriggered;
    String lastRequestTable;
    String lastSkipReason;

    @Override
    public void onRequest(String tableName, KeyRouteAffinity mode) {
      requestsTotal++;
      lastRequestTable = tableName;
    }

    @Override
    public void onAffinityApplied(String tableName, KeyRouteAffinity mode) {
      requestsAffinity++;
    }

    @Override
    public void onAffinitySkipped(String tableName, KeyRouteAffinity mode, String reason) {
      requestsRoundRobin++;
      lastSkipReason = reason;
    }

    @Override
    public void onPartitionKeyCacheHit(String tableName) {
      cacheHits++;
    }

    @Override
    public void onPartitionKeyCacheMiss(String tableName) {
      cacheMisses++;
    }

    @Override
    public void onPartitionKeyDiscoveryTriggered(String tableName) {
      discoveryTriggered++;
    }
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

  /**
   * Mock SdkHttpClient that captures HTTP requests and returns mock DynamoDB responses.
   *
   * <p>This allows verification of which node (URI) each request was sent to without requiring an
   * actual server.
   */
  private static class MockSdkHttpClient implements SdkHttpClient {
    private final List<CapturedRequest> capturedRequests = new CopyOnWriteArrayList<>();

    void clearCapturedRequests() {
      capturedRequests.clear();
    }

    URI getLastRequestUri() {
      if (capturedRequests.isEmpty()) {
        return null;
      }
      return capturedRequests.get(capturedRequests.size() - 1).uri;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      URI uri = request.httpRequest().getUri();
      capturedRequests.add(new CapturedRequest(uri));
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          String body = "{}";
          byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
          SdkHttpFullResponse response =
              SdkHttpFullResponse.builder()
                  .statusCode(200)
                  .putHeader("Content-Type", "application/x-amz-json-1.0")
                  .putHeader("Content-Length", String.valueOf(bodyBytes.length))
                  .build();
          return HttpExecuteResponse.builder()
              .response(response)
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bodyBytes)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "MockSdkHttpClient";
    }

    private static class CapturedRequest {
      final URI uri;

      CapturedRequest(URI uri) {
        this.uri = uri;
      }
    }
  }
}
