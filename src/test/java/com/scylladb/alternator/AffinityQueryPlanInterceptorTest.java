/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
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
 * the same node) or random routing (requests spread across nodes) is used, depending on the
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
    return createClient(keyAffinity, liveNodes);
  }

  private DynamoDbClient createClient(
      KeyRouteAffinityConfig keyAffinity, AlternatorLiveNodes liveNodes) {
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

  /**
   * Verify that an operation routes to different nodes (random query-plan routing, no affinity).
   */
  private void assertRandomRouting(Runnable operation) {
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
    return makeItem(PK_VALUE);
  }

  private Map<String, AttributeValue> makeItem(String pkValue) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK_NAME, AttributeValue.builder().s(pkValue).build());
    item.put("data", AttributeValue.builder().s("value").build());
    return item;
  }

  private URI firstAffinityRouteForPk(String pkValue) {
    long hash = AttributeValueHasher.hash(AttributeValue.builder().s(pkValue).build());
    LazyQueryPlan plan = new LazyQueryPlan(new MockAlternatorLiveNodes(testNodeUris), hash);
    return plan.next();
  }

  private String findPkValueWithDifferentRoute(String prefix, String otherPkValue) {
    URI otherRoute = firstAffinityRouteForPk(otherPkValue);
    for (int i = 0; i < 100; i++) {
      String candidate = prefix + i;
      if (!firstAffinityRouteForPk(candidate).equals(otherRoute)) {
        return candidate;
      }
    }
    fail("Could not find test partition key with a different affinity route");
    return null;
  }

  private String findPkValueWithPreferredRoute(String prefix, List<URI> candidates, URI expected) {
    for (int i = 0; i < 1000; i++) {
      String candidate = prefix + i;
      long hash = AttributeValueHasher.hash(AttributeValue.builder().s(candidate).build());
      if (expected.equals(firstNodeForSeed(candidates, hash))) {
        return candidate;
      }
    }
    fail("Could not find test partition key with expected route " + expected);
    return null;
  }

  private URI firstNodeForSeed(List<URI> candidates, long seed) {
    LazyQueryPlan plan = new LazyQueryPlan(new MockAlternatorLiveNodes(candidates), seed);
    return plan.hasNext() ? plan.next() : null;
  }

  private List<URI> collectNodes(LazyQueryPlan plan) {
    List<URI> collected = new ArrayList<>();
    while (plan.hasNext()) {
      collected.add(plan.next());
    }
    return collected;
  }

  private void assertSameRoute(String message, URI expected, URI actual) {
    assertNotNull(message + ": actual route should not be null", actual);
    assertEquals(message + ": scheme", expected.getScheme(), actual.getScheme());
    assertEquals(message + ": host", expected.getHost(), actual.getHost());
    assertEquals(message + ": port", expected.getPort(), actual.getPort());
  }

  // ========== PutItem variants ==========

  // --- Simple PutItem (no conditions) ---

  @Test
  @CoversRequirements("AFF-REQ-004")
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
  public void testPutItemSimple_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemSimple_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testPutItemWithConditionExpression_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testPutItemWithExpected_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertRandomRouting(
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
  public void testPutItemReturnValuesAllOld_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testPutItemReturnValuesNone_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
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
  public void testPutItemReturnValuesNone_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemSimple_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
          () ->
              client.updateItem(
                  UpdateItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testUpdateItemSimple_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemWithUpdateExpression_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemWithConditionExpression_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemWithExpected_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertRandomRouting(
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
  public void testUpdateItemReturnValuesAllOld_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemReturnValuesUpdatedOld_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemReturnValuesAllNew_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemReturnValuesUpdatedNew_Rmw_RandomRouting() {
    // UPDATED_NEW does NOT trigger RMW - can be computed from update alone
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
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
  public void testUpdateItemReturnValuesUpdatedNew_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testUpdateItemAddAction_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "counter",
          AttributeValueUpdate.builder()
              .action(AttributeAction.ADD)
              .value(AttributeValue.builder().n("1").build())
              .build());
      assertRandomRouting(
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
  public void testUpdateItemDeleteActionWithValue_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "tags",
          AttributeValueUpdate.builder()
              .action(AttributeAction.DELETE)
              .value(AttributeValue.builder().ss("old-tag").build())
              .build());
      assertRandomRouting(
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
  public void testUpdateItemDeleteActionWithoutValue_Rmw_RandomRouting() {
    // DELETE without value just removes the attribute - no read needed
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "obsolete_field", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
      assertRandomRouting(
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
  public void testUpdateItemDeleteActionWithoutValue_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "obsolete_field", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
      assertRandomRouting(
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
  public void testUpdateItemPutAction_Rmw_RandomRouting() {
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
      assertRandomRouting(
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
  public void testUpdateItemPutAction_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, AttributeValueUpdate> updates = new HashMap<>();
      updates.put(
          "data",
          AttributeValueUpdate.builder()
              .action(AttributeAction.PUT)
              .value(AttributeValue.builder().s("new-value").build())
              .build());
      assertRandomRouting(
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
  public void testDeleteItemSimple_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
          () ->
              client.deleteItem(
                  DeleteItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testDeleteItemSimple_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testDeleteItemWithConditionExpression_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testDeleteItemWithExpected_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put(
          "data",
          ExpectedAttributeValue.builder()
              .value(AttributeValue.builder().s("old").build())
              .build());
      assertRandomRouting(
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
  public void testDeleteItemReturnValuesAllOld_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testGetItem_AnyWrite_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertRandomRouting(
          () ->
              client.getItem(
                  GetItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testGetItem_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
          () ->
              client.getItem(
                  GetItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testGetItem_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
          () ->
              client.getItem(
                  GetItemRequest.builder().tableName(TABLE_NAME).key(makeKey()).build()));
    } finally {
      client.close();
    }
  }

  // --- Query ---

  @Test
  public void testQuery_AnyWrite_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertRandomRouting(
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
  public void testQuery_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(
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
  public void testQuery_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(
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
  public void testScan_AnyWrite_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      assertRandomRouting(() -> client.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testScan_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      assertRandomRouting(() -> client.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testScan_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      assertRandomRouting(() -> client.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    } finally {
      client.close();
    }
  }

  // --- BatchWriteItem ---

  @Test
  public void testBatchWriteItem_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder().putRequest(PutRequest.builder().item(makeItem()).build()).build());
      requestItems.put(TABLE_NAME, writes);

      assertAffinityRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItem_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder().putRequest(PutRequest.builder().item(makeItem()).build()).build());
      requestItems.put(TABLE_NAME, writes);

      assertRandomRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItem_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder().putRequest(PutRequest.builder().item(makeItem()).build()).build());
      requestItems.put(TABLE_NAME, writes);

      assertRandomRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  // --- BatchWriteItem with DeleteRequest ---

  @Test
  public void testBatchWriteItemDelete_AnyWrite_Affinity() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      List<WriteRequest> writes = new ArrayList<>();
      writes.add(
          WriteRequest.builder()
              .deleteRequest(DeleteRequest.builder().key(makeKey()).build())
              .build());
      requestItems.put(TABLE_NAME, writes);

      assertAffinityRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItemMajorityPreferredNodeWins() {
    String majorityPk = PK_VALUE;
    String otherPk = findPkValueWithDifferentRoute("other-route-", majorityPk);
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
      requestItems.put(
          TABLE_NAME,
          Arrays.asList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(otherPk)).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(majorityPk)).build())
                  .build(),
              WriteRequest.builder()
                  .deleteRequest(DeleteRequest.builder().key(makeKey()).build())
                  .build()));

      URI expectedRoute = firstAffinityRouteForPk(majorityPk);

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
      URI actualRoute = mockHttpClient.getLastRequestUri();

      assertSameRoute(
          "BatchWrite should route to the majority preferred node", expectedRoute, actualRoute);
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItemNonKeyAttributesDoNotAffectRouting() {
    String majorityPk = PK_VALUE;
    String otherPk = findPkValueWithDifferentRoute("other-route-", majorityPk);
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValue> majorityA = makeItem(majorityPk);
      majorityA.put("data", AttributeValue.builder().s("alpha").build());
      Map<String, AttributeValue> majorityB = makeItem(majorityPk);
      majorityB.put("data", AttributeValue.builder().s("zeta").build());

      Map<String, List<WriteRequest>> firstRequestItems = new LinkedHashMap<>();
      firstRequestItems.put(
          TABLE_NAME,
          Arrays.asList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(otherPk)).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(majorityA).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(majorityB).build())
                  .build()));

      Map<String, AttributeValue> majorityC = makeItem(majorityPk);
      majorityC.put("data", AttributeValue.builder().s("middle").build());
      Map<String, AttributeValue> majorityD = makeItem(majorityPk);
      majorityD.put("data", AttributeValue.builder().s("beta").build());

      Map<String, List<WriteRequest>> secondRequestItems = new LinkedHashMap<>();
      secondRequestItems.put(
          TABLE_NAME,
          Arrays.asList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(majorityC).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(otherPk)).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(majorityD).build())
                  .build()));

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(
          BatchWriteItemRequest.builder().requestItems(firstRequestItems).build());
      URI firstRoute = mockHttpClient.getLastRequestUri();

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(
          BatchWriteItemRequest.builder().requestItems(secondRequestItems).build());
      URI secondRoute = mockHttpClient.getLastRequestUri();

      URI expectedRoute = firstAffinityRouteForPk(majorityPk);
      assertSameRoute(
          "First request should route by partition-key votes", expectedRoute, firstRoute);
      assertSameRoute(
          "Second request should route by partition-key votes", expectedRoute, secondRoute);
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItemMissingMetadataContinuesWithUsableCandidate() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo(TABLE_NAME, PK_NAME)
            .build();
    DynamoDbClient client = createClient(config);
    try {
      Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
      requestItems.put(
          "unknown_table",
          Collections.singletonList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem("unknown-pk")).build())
                  .build()));
      requestItems.put(
          TABLE_NAME,
          Collections.singletonList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(PK_VALUE)).build())
                  .build()));

      URI expectedRoute = firstAffinityRouteForPk(PK_VALUE);

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
      URI actualRoute = mockHttpClient.getLastRequestUri();

      assertSameRoute(
          "BatchWrite should continue after missing table metadata", expectedRoute, actualRoute);
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItemSkipsMissingAndUnsupportedPartitionKeys() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValue> missingPk = new HashMap<>();
      missingPk.put("data", AttributeValue.builder().s("missing").build());
      Map<String, AttributeValue> unsupportedPk = new HashMap<>();
      unsupportedPk.put(PK_NAME, AttributeValue.builder().bool(true).build());

      Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
      requestItems.put(
          TABLE_NAME,
          Arrays.asList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(missingPk).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(unsupportedPk).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(PK_VALUE)).build())
                  .build()));

      URI expectedRoute = firstAffinityRouteForPk(PK_VALUE);

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
      URI actualRoute = mockHttpClient.getLastRequestUri();

      assertSameRoute(
          "BatchWrite should route by the supported candidate", expectedRoute, actualRoute);
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItemTieUsesLexicographicNodeOrder() {
    String firstPk = PK_VALUE;
    String secondPk = findPkValueWithDifferentRoute("tie-route-", firstPk);
    URI firstRoute = firstAffinityRouteForPk(firstPk);
    URI secondRoute = firstAffinityRouteForPk(secondPk);
    URI expectedRoute =
        firstRoute.toString().compareTo(secondRoute.toString()) <= 0 ? firstRoute : secondRoute;

    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
      requestItems.put(
          TABLE_NAME,
          Arrays.asList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(firstPk)).build())
                  .build(),
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(secondPk)).build())
                  .build()));

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
      URI actualRoute = mockHttpClient.getLastRequestUri();

      assertSameRoute("Tied votes should use lexicographic node order", expectedRoute, actualRoute);
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchWriteItemNoUsableCandidatesFallsBackToRandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValue> unsupportedPk = new HashMap<>();
      unsupportedPk.put(PK_NAME, AttributeValue.builder().bool(true).build());
      Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
      requestItems.put(
          TABLE_NAME,
          Collections.singletonList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(unsupportedPk).build())
                  .build()));

      assertRandomRouting(
          () ->
              client.batchWriteItem(
                  BatchWriteItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  @CoversRequirements("AFF-REQ-006")
  public void testBatchWriteItemSkipsDownPreferredNodeUsingStableKnownOrder() {
    URI downNode = testNodeUris.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            testNodeUris, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    String pkValue = findPkValueWithPreferredRoute("down-route-", testNodeUris, downNode);
    long hash = AttributeValueHasher.hash(AttributeValue.builder().s(pkValue).build());
    List<URI> allHealthyOrder =
        collectNodes(new LazyQueryPlan(new MockAlternatorLiveNodes(testNodeUris), hash));
    assertEquals(downNode, allHealthyOrder.get(0));
    URI expectedAfterSkip = allHealthyOrder.get(1);

    liveNodes.reportNodeResult(
        downNode,
        NodeHealthObservation.TRAFFIC_FAILURE,
        liveNodes.getNodeHealthGeneration(downNode));

    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE), liveNodes);
    try {
      Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
      requestItems.put(
          TABLE_NAME,
          Collections.singletonList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(makeItem(pkValue)).build())
                  .build()));

      mockHttpClient.clearCapturedRequests();
      client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
      URI route = mockHttpClient.getLastRequestUri();

      assertNotEquals("Down preferred node should be skipped", downNode.getHost(), route.getHost());
      assertSameRoute("Batch write should use next healthy node", expectedAfterSkip, route);
    } finally {
      client.close();
    }
  }

  @Test
  public void testPutItemUnsupportedPartitionKeyTypeFallsBackToRandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, AttributeValue> item = new HashMap<>();
      item.put(PK_NAME, AttributeValue.builder().bool(true).build());

      assertRandomRouting(
          () -> client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item).build()));
    } finally {
      client.close();
    }
  }

  // --- BatchGetItem ---

  @Test
  public void testBatchGetItem_AnyWrite_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.ANY_WRITE));
    try {
      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          KeysAndAttributes.builder().keys(Collections.singletonList(makeKey())).build());

      assertRandomRouting(
          () ->
              client.batchGetItem(
                  BatchGetItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchGetItem_Rmw_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.RMW));
    try {
      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          KeysAndAttributes.builder().keys(Collections.singletonList(makeKey())).build());

      assertRandomRouting(
          () ->
              client.batchGetItem(
                  BatchGetItemRequest.builder().requestItems(requestItems).build()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testBatchGetItem_None_RandomRouting() {
    DynamoDbClient client = createClient(buildConfig(KeyRouteAffinity.NONE));
    try {
      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          KeysAndAttributes.builder().keys(Collections.singletonList(makeKey())).build());

      assertRandomRouting(
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
  public void testNullConfig_RandomRouting() {
    // Passing null config should use basic random query-plan routing
    DynamoDbClient client = createClient(null);
    try {
      assertRandomRouting(
          () ->
              client.putItem(
                  PutItemRequest.builder().tableName(TABLE_NAME).item(makeItem()).build()));
    } finally {
      client.close();
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
   * </ul>
   */
  private static class MockAlternatorLiveNodes extends AlternatorLiveNodes {
    private final List<URI> nodes;

    MockAlternatorLiveNodes(List<URI> nodes) {
      this(nodes, NodeHealthConfig.getDefault());
    }

    MockAlternatorLiveNodes(List<URI> nodes, NodeHealthConfig nodeHealthConfig) {
      super(
          AlternatorConfig.builder()
              .withSeedHosts(nodeHosts(nodes))
              .withScheme(nodes.get(0).getScheme())
              .withPort(nodes.get(0).getPort())
              .withNodeHealthConfig(nodeHealthConfig)
              .build());
      this.nodes = new ArrayList<>(nodes);
      for (URI node : nodes) {
        reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
      }
    }

    @Override
    protected List<URI> getDiscoveredNodesInternal() {
      return nodes;
    }

    @Override
    public void start() {}
  }

  private static List<String> nodeHosts(List<URI> nodes) {
    List<String> hosts = new ArrayList<>();
    for (URI node : nodes) {
      hosts.add(node.getHost());
    }
    return hosts;
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
