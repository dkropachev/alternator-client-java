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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Integration tests for KeyRouteAffinity with full client.
 *
 * <p>These tests verify key route affinity at the HTTP request level by:
 *
 * <ul>
 *   <li>Building a full DynamoDbClient with query plan interceptors
 *   <li>Mocking HTTP at the SdkHttpClient layer to capture actual requests
 *   <li>Using a mock AlternatorLiveNodes to provide a controlled node list
 *   <li>Verifying requests are routed to correct nodes based on partition key
 * </ul>
 *
 * @author dmitry.kropachev
 */
public class KeyRouteAffinityClientTest {

  private List<URI> testNodeUris;
  private MockSdkHttpClient mockHttpClient;

  @Before
  public void setUp() throws Exception {
    // Create 5 test node URIs
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

  /**
   * Creates a DynamoDbClient with key route affinity using controlled mock components.
   *
   * <p>This method builds the client manually to inject:
   *
   * <ul>
   *   <li>A mock AlternatorLiveNodes that provides a fixed node list without network calls
   *   <li>A mock SdkHttpClient that captures all HTTP requests for verification
   *   <li>The query plan interceptor for request routing (affinity or basic)
   * </ul>
   */
  private DynamoDbClient createClientWithKeyAffinity(KeyRouteAffinityConfig keyAffinity) {
    // Create mock AlternatorLiveNodes that provides our test nodes
    AlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(testNodeUris);

    // Build override configuration with query plan interceptor
    ClientOverrideConfiguration.Builder overrideBuilder = ClientOverrideConfiguration.builder();
    if (keyAffinity != null && keyAffinity.isEnabled()) {
      overrideBuilder.addExecutionInterceptor(
          new AffinityQueryPlanInterceptor(keyAffinity, liveNodes));
    } else {
      overrideBuilder.addExecutionInterceptor(new BasicQueryPlanInterceptor(liveNodes));
    }

    // Build the DynamoDB client with our mock components
    // Use first test node as base endpoint - interceptor will override with actual target
    return DynamoDbClient.builder()
        .region(Region.of("test-region"))
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .endpointOverride(testNodeUris.get(0))
        .httpClient(mockHttpClient)
        .overrideConfiguration(overrideBuilder.build())
        .build();
  }

  // ========== Tests for same key routing to same node ==========

  @Test
  public void testSamePartitionKeyRoutesToSameNode() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKeyValue = "user-123";

      // Execute first request
      Map<String, AttributeValue> item1 = new HashMap<>();
      item1.put("user_id", AttributeValue.builder().s(partitionKeyValue).build());
      item1.put("data", AttributeValue.builder().s("data1").build());

      PutItemRequest request1 = PutItemRequest.builder().tableName("users").item(item1).build();

      mockHttpClient.clearCapturedRequests();
      client.putItem(request1);
      URI host1 = mockHttpClient.getLastRequestUri();
      assertNotNull("First request should have been made", host1);

      // Execute second request with same key
      Map<String, AttributeValue> item2 = new HashMap<>();
      item2.put("user_id", AttributeValue.builder().s(partitionKeyValue).build());
      item2.put("data", AttributeValue.builder().s("data2").build());

      PutItemRequest request2 = PutItemRequest.builder().tableName("users").item(item2).build();

      mockHttpClient.clearCapturedRequests();
      client.putItem(request2);
      URI host2 = mockHttpClient.getLastRequestUri();
      assertNotNull("Second request should have been made", host2);

      assertEquals("Same partition key should route to same node", host1, host2);
    } finally {
      client.close();
    }
  }

  @Test
  public void testSamePartitionKeyConsistentAcrossMultipleRequests() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("orders", "order_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKey = "order-abc-123";
      URI expectedUri = null;

      // Make 10 requests with the same key and verify they all go to the same node
      for (int i = 0; i < 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("order_id", AttributeValue.builder().s(partitionKey).build());
        item.put("iteration", AttributeValue.builder().n(String.valueOf(i)).build());

        PutItemRequest request = PutItemRequest.builder().tableName("orders").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        assertNotNull("Request " + i + " should have been made", targetUri);

        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals(
              "Request " + i + " should route to same node as previous requests",
              expectedUri,
              targetUri);
        }
      }
    } finally {
      client.close();
    }
  }

  // ========== Tests for different keys routing to different nodes ==========

  @Test
  public void testDifferentPartitionKeysCanRouteToDifferentNodes() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      Set<URI> nodesUsed = new HashSet<>();
      String[] partitionKeys = {
        "user-1",
        "user-2",
        "user-3",
        "user-4",
        "user-5",
        "user-100",
        "user-200",
        "user-300",
        "user-400",
        "user-500"
      };

      for (String pk : partitionKeys) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s(pk).build());

        PutItemRequest request = PutItemRequest.builder().tableName("users").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        assertNotNull("Request for key " + pk + " should have been made", targetUri);
        nodesUsed.add(targetUri);
      }

      // With 10 different keys and 5 nodes, we should use more than 1 node
      assertTrue(
          "Different partition keys should route to multiple nodes (got " + nodesUsed.size() + ")",
          nodesUsed.size() > 1);
    } finally {
      client.close();
    }
  }

  @Test
  public void testDifferentPartitionKeysAreDeterministic() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("sessions", "session_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String[] partitionKeys = {"session-aaa", "session-bbb", "session-ccc"};
      Map<String, URI> firstPassRouting = new HashMap<>();
      Map<String, URI> secondPassRouting = new HashMap<>();

      // First pass - record where each key routes
      for (String pk : partitionKeys) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("session_id", AttributeValue.builder().s(pk).build());

        PutItemRequest request = PutItemRequest.builder().tableName("sessions").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);
        firstPassRouting.put(pk, mockHttpClient.getLastRequestUri());
      }

      // Second pass - verify routing is the same
      for (String pk : partitionKeys) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("session_id", AttributeValue.builder().s(pk).build());

        PutItemRequest request = PutItemRequest.builder().tableName("sessions").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);
        secondPassRouting.put(pk, mockHttpClient.getLastRequestUri());
      }

      // Verify deterministic routing
      for (String pk : partitionKeys) {
        assertEquals(
            "Key " + pk + " should route to same node on both passes",
            firstPassRouting.get(pk),
            secondPassRouting.get(pk));
      }
    } finally {
      client.close();
    }
  }

  // ========== Tests for RMW mode vs ANY_WRITE mode ==========

  @Test
  public void testRmwModeOnlyAppliesToConditionalWrites() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKeyValue = "user-123";

      // Simple PutItem without conditions - uses random query-plan routing, may hit different nodes
      Set<URI> nodesForSimplePut = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s(partitionKeyValue).build());
        item.put("data", AttributeValue.builder().s("data" + i).build());

        PutItemRequest simplePut = PutItemRequest.builder().tableName("users").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(simplePut);
        nodesForSimplePut.add(mockHttpClient.getLastRequestUri());
      }

      // PutItem with condition - should always route to same node for same key
      URI expectedNodeForConditional = null;
      for (int i = 0; i < 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s(partitionKeyValue).build());
        item.put("data", AttributeValue.builder().s("conditional" + i).build());

        PutItemRequest conditionalPut =
            PutItemRequest.builder()
                .tableName("users")
                .item(item)
                .conditionExpression("attribute_not_exists(user_id)")
                .build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(conditionalPut);

        URI targetNode = mockHttpClient.getLastRequestUri();
        if (expectedNodeForConditional == null) {
          expectedNodeForConditional = targetNode;
        } else {
          assertEquals(
              "Conditional writes should always route to same node",
              expectedNodeForConditional,
              targetNode);
        }
      }

      // Verify conditional writes go to one consistent node
      assertNotNull("Conditional writes should have a target node", expectedNodeForConditional);

      // Simple puts should use random query-plan routing (multiple nodes) since RMW doesn't apply
      assertTrue(
          "Simple puts in RMW mode should use random query-plan routing (got "
              + nodesForSimplePut.size()
              + " nodes)",
          nodesForSimplePut.size() > 1);
    } finally {
      client.close();
    }
  }

  @Test
  public void testAnyWriteModeAppliesToAllWrites() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKeyValue = "user-456";

      // ALL writes (even without conditions) should route to same node in ANY_WRITE mode
      URI expectedNode = null;
      for (int i = 0; i < 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s(partitionKeyValue).build());
        item.put("data", AttributeValue.builder().s("data" + i).build());

        PutItemRequest request = PutItemRequest.builder().tableName("users").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetNode = mockHttpClient.getLastRequestUri();
        if (expectedNode == null) {
          expectedNode = targetNode;
        } else {
          assertEquals(
              "All writes in ANY_WRITE mode should route to same node for same key",
              expectedNode,
              targetNode);
        }
      }
    } finally {
      client.close();
    }
  }

  // ========== Tests for UpdateItem ==========

  @Test
  public void testUpdateItemSameKeyRoutesToSameNode() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("user_id", AttributeValue.builder().s("user-999").build());

      // First update
      UpdateItemRequest request1 =
          UpdateItemRequest.builder()
              .tableName("users")
              .key(key)
              .updateExpression("SET counter = counter + :val")
              .expressionAttributeValues(
                  Collections.singletonMap(":val", AttributeValue.builder().n("1").build()))
              .build();

      mockHttpClient.clearCapturedRequests();
      client.updateItem(request1);
      URI host1 = mockHttpClient.getLastRequestUri();

      // Second update with same key
      UpdateItemRequest request2 =
          UpdateItemRequest.builder()
              .tableName("users")
              .key(key)
              .updateExpression("SET counter = counter + :val2")
              .expressionAttributeValues(
                  Collections.singletonMap(":val2", AttributeValue.builder().n("2").build()))
              .build();

      mockHttpClient.clearCapturedRequests();
      client.updateItem(request2);
      URI host2 = mockHttpClient.getLastRequestUri();

      assertEquals("UpdateItem requests with same key should route to same node", host1, host2);
    } finally {
      client.close();
    }
  }

  // ========== Tests for numeric partition keys ==========

  @Test
  public void testNumericPartitionKeyRoutesConsistently() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("products", "product_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      URI expectedUri = null;
      for (int i = 0; i < 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("product_id", AttributeValue.builder().n("12345").build());
        item.put("iteration", AttributeValue.builder().n(String.valueOf(i)).build());

        PutItemRequest request = PutItemRequest.builder().tableName("products").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals("Numeric partition key should route consistently", expectedUri, targetUri);
        }
      }

      assertNotNull("Requests should have been routed", expectedUri);
    } finally {
      client.close();
    }
  }

  // ========== Tests for hash distribution ==========

  @Test
  public void testHashDistributionAcrossNodes() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("data", "id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      // Use host:port string as key to avoid URI path/equality issues
      Map<String, Integer> nodeHits = new HashMap<>();
      for (URI uri : testNodeUris) {
        nodeHits.put(uri.getHost() + ":" + uri.getPort(), 0);
      }

      // Generate many requests with different keys
      int numRequests = 1000;
      for (int i = 0; i < numRequests; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("key-" + i).build());

        PutItemRequest request = PutItemRequest.builder().tableName("data").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        String hostPort = targetUri.getHost() + ":" + targetUri.getPort();
        nodeHits.put(hostPort, nodeHits.getOrDefault(hostPort, 0) + 1);
      }

      // Verify distribution - each node should get some requests
      for (Map.Entry<String, Integer> entry : nodeHits.entrySet()) {
        assertTrue(
            "Node "
                + entry.getKey()
                + " should receive some requests (got "
                + entry.getValue()
                + ")",
            entry.getValue() > 0);
      }

      // Verify reasonable distribution (no node gets more than 50% of requests)
      for (Map.Entry<String, Integer> entry : nodeHits.entrySet()) {
        assertTrue(
            "Node "
                + entry.getKey()
                + " should not receive more than 50% of requests (got "
                + entry.getValue()
                + ")",
            entry.getValue() < numRequests / 2);
      }
    } finally {
      client.close();
    }
  }

  // ========== Tests for NONE mode ==========

  @Test
  public void testNoneModeUsesRandomRouting() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.NONE)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      // In NONE mode, same key should use random query-plan routing across nodes
      Set<URI> nodesUsed = new HashSet<>();
      String partitionKey = "user-same-key";

      for (int i = 0; i < 20; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s(partitionKey).build());
        item.put("data", AttributeValue.builder().s("data" + i).build());

        PutItemRequest request = PutItemRequest.builder().tableName("users").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);
        nodesUsed.add(mockHttpClient.getLastRequestUri());
      }

      // With 20 requests in random query-plan mode across 5 nodes, we should see multiple nodes
      assertTrue(
          "NONE mode should use random query-plan routing across multiple nodes (got "
              + nodesUsed.size()
              + ")",
          nodesUsed.size() > 1);
    } finally {
      client.close();
    }
  }

  // ========== Tests for binary partition keys ==========

  @Test
  public void testBinaryPartitionKeyRoutesConsistently() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("bindata", "bin_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      byte[] binaryKey = new byte[] {0x01, 0x02, 0x03, (byte) 0xff, (byte) 0xfe};
      URI expectedUri = null;

      for (int i = 0; i < 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("bin_id", AttributeValue.builder().b(SdkBytes.fromByteArray(binaryKey)).build());
        item.put("iteration", AttributeValue.builder().n(String.valueOf(i)).build());

        PutItemRequest request = PutItemRequest.builder().tableName("bindata").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals(
              "Binary partition key should route consistently to same node",
              expectedUri,
              targetUri);
        }
      }

      assertNotNull("Requests should have been routed", expectedUri);
    } finally {
      client.close();
    }
  }

  @Test
  public void testDifferentBinaryPartitionKeysCanRouteToDifferentNodes() {
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("bindata", "bin_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      Set<URI> nodesUsed = new HashSet<>();

      // Generate different binary keys
      for (int i = 0; i < 20; i++) {
        byte[] binaryKey = new byte[] {(byte) i, (byte) (i + 1), (byte) (i * 2)};

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("bin_id", AttributeValue.builder().b(SdkBytes.fromByteArray(binaryKey)).build());

        PutItemRequest request = PutItemRequest.builder().tableName("bindata").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);
        nodesUsed.add(mockHttpClient.getLastRequestUri());
      }

      // With different binary keys, we should use multiple nodes
      assertTrue(
          "Different binary partition keys should route to multiple nodes (got "
              + nodesUsed.size()
              + ")",
          nodesUsed.size() > 1);
    } finally {
      client.close();
    }
  }

  @Test
  public void testBinaryKeyWithHighBytesRoutesConsistently() {
    // Test binary keys with bytes >= 0x80 to verify unsigned byte handling
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo("bindata", "bin_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      byte[] binaryKey = new byte[] {(byte) 0xff, (byte) 0x80, (byte) 0x7f, (byte) 0x00};
      URI expectedUri = null;

      for (int i = 0; i < 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("bin_id", AttributeValue.builder().b(SdkBytes.fromByteArray(binaryKey)).build());

        PutItemRequest request = PutItemRequest.builder().tableName("bindata").item(item).build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals(
              "Binary key with high bytes should route consistently", expectedUri, targetUri);
        }
      }
    } finally {
      client.close();
    }
  }

  // ========== Tests for RMW mode with specific triggers ==========

  @Test
  public void testRmwModeWithUpdateExpressionRoutesToSameNode() {
    // UpdateExpression triggers LWT in Alternator, so RMW mode should apply
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("counters", "counter_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKey = "counter-abc";
      URI expectedUri = null;

      for (int i = 0; i < 10; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("counter_id", AttributeValue.builder().s(partitionKey).build());

        // UpdateItem with UpdateExpression (LWT in Alternator)
        UpdateItemRequest request =
            UpdateItemRequest.builder()
                .tableName("counters")
                .key(key)
                .updateExpression("SET #cnt = if_not_exists(#cnt, :zero) + :inc")
                .expressionAttributeNames(Collections.singletonMap("#cnt", "count"))
                .expressionAttributeValues(
                    new HashMap<String, AttributeValue>() {
                      {
                        put(":zero", AttributeValue.builder().n("0").build());
                        put(":inc", AttributeValue.builder().n("1").build());
                      }
                    })
                .build();

        mockHttpClient.clearCapturedRequests();
        client.updateItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals(
              "RMW mode with UpdateExpression should route to same node for same key",
              expectedUri,
              targetUri);
        }
      }

      assertNotNull("Requests should have been routed", expectedUri);
    } finally {
      client.close();
    }
  }

  @Test
  public void testRmwModeWithReturnValuesAllOld() {
    // ReturnValues=ALL_OLD requires reading the item, triggering RMW
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKey = "user-return-old";
      URI expectedUri = null;

      for (int i = 0; i < 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s(partitionKey).build());
        item.put("data", AttributeValue.builder().s("data" + i).build());

        // PutItem with ReturnValues=ALL_OLD
        PutItemRequest request =
            PutItemRequest.builder()
                .tableName("users")
                .item(item)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        mockHttpClient.clearCapturedRequests();
        client.putItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals(
              "RMW mode with ReturnValues=ALL_OLD should route to same node",
              expectedUri,
              targetUri);
        }
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testRmwModeWithConditionExpressionOnDelete() {
    // DeleteItem with condition expression triggers RMW
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("sessions", "session_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      String partitionKey = "session-conditional-delete";
      URI expectedUri = null;

      for (int i = 0; i < 5; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("session_id", AttributeValue.builder().s(partitionKey).build());

        // DeleteItem with condition expression
        DeleteItemRequest request =
            DeleteItemRequest.builder()
                .tableName("sessions")
                .key(key)
                .conditionExpression("attribute_exists(session_id)")
                .build();

        mockHttpClient.clearCapturedRequests();
        client.deleteItem(request);

        URI targetUri = mockHttpClient.getLastRequestUri();
        if (expectedUri == null) {
          expectedUri = targetUri;
        } else {
          assertEquals(
              "RMW mode with conditional delete should route to same node", expectedUri, targetUri);
        }
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testRmwModeSimpleDeleteUsesRandomRouting() {
    // Simple DeleteItem without conditions should use random query-plan routing in RMW mode
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("sessions", "session_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      Set<URI> nodesUsed = new HashSet<>();
      String partitionKey = "session-simple-delete";

      for (int i = 0; i < 20; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("session_id", AttributeValue.builder().s(partitionKey).build());

        // Simple DeleteItem without conditions
        DeleteItemRequest request =
            DeleteItemRequest.builder().tableName("sessions").key(key).build();

        mockHttpClient.clearCapturedRequests();
        client.deleteItem(request);
        nodesUsed.add(mockHttpClient.getLastRequestUri());
      }

      // Simple deletes in RMW mode should use random query-plan routing
      assertTrue(
          "Simple delete in RMW mode should use random query-plan routing (got "
              + nodesUsed.size()
              + " nodes)",
          nodesUsed.size() > 1);
    } finally {
      client.close();
    }
  }

  @Test
  public void testRmwModeWithReturnValuesUpdatedNew_DoesNotTrigger() {
    // UPDATED_NEW does NOT trigger RMW - can be computed from update alone
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbClient client = createClientWithKeyAffinity(keyAffinity);

    try {
      Set<URI> nodesUsed = new HashSet<>();
      String partitionKey = "user-updated-new";

      for (int i = 0; i < 20; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("user_id", AttributeValue.builder().s(partitionKey).build());

        // UpdateItem with only ReturnValues=UPDATED_NEW (no condition, no expression)
        UpdateItemRequest request =
            UpdateItemRequest.builder()
                .tableName("users")
                .key(key)
                .returnValues(ReturnValue.UPDATED_NEW)
                .build();

        mockHttpClient.clearCapturedRequests();
        client.updateItem(request);
        nodesUsed.add(mockHttpClient.getLastRequestUri());
      }

      // UPDATED_NEW alone doesn't trigger RMW, should use random query-plan routing
      assertTrue(
          "UPDATED_NEW alone in RMW mode should use random query-plan routing (got "
              + nodesUsed.size()
              + ")",
          nodesUsed.size() > 1);
    } finally {
      client.close();
    }
  }

  // ========== Tests for async client key affinity support ==========

  @Test
  public void testAsyncClientAcceptsKeyAffinity() {
    // Key route affinity now works with async clients using ExecutionAttributes
    // instead of ThreadLocal, so the async client should accept key affinity config.
    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .withKeyRouteAffinity(KeyRouteAffinity.ANY_WRITE)
            .build();
    assertNotNull("Async client should be created with key affinity", client);
    client.close();
  }

  @Test
  public void testAsyncClientAcceptsKeyAffinityConfig() {
    // Also test with full KeyRouteAffinityConfig object
    KeyRouteAffinityConfig keyAffinity =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("users", "user_id")
            .build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .withKeyRouteAffinity(keyAffinity)
            .build();
    assertNotNull("Async client should be created with key affinity config", client);
    client.close();
  }

  @Test
  public void testAsyncClientAcceptsNoneMode() {
    // NONE mode should also be accepted
    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .withKeyRouteAffinity(KeyRouteAffinity.NONE)
            .build();
    assertNotNull("Async client should be created with NONE mode", client);
    client.close();
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
      super(
          AlternatorConfig.builder()
              .withSeedHosts(nodeHosts(nodes))
              .withScheme(nodes.get(0).getScheme())
              .withPort(nodes.get(0).getPort())
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
    public void start() {
      // Don't start background thread in tests
    }

    private static List<String> nodeHosts(List<URI> nodes) {
      List<String> hosts = new ArrayList<>();
      for (URI node : nodes) {
        hosts.add(node.getHost());
      }
      return hosts;
    }
  }

  /**
   * Mock SdkHttpClient that captures HTTP requests and returns mock DynamoDB responses.
   *
   * <p>This allows verification of which node (URI) each request was sent to without requiring an
   * actual server.
   */
  private static class MockSdkHttpClient implements SdkHttpClient {
    private final List<CapturedRequest> capturedRequests = new CopyOnWriteArrayList<>();
    private volatile boolean closed = false;

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
      SdkHttpRequest httpRequest = request.httpRequest();
      URI uri = httpRequest.getUri();

      capturedRequests.add(new CapturedRequest(uri));

      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          return createDynamoDbResponse();
        }

        @Override
        public void abort() {}
      };
    }

    private HttpExecuteResponse createDynamoDbResponse() {
      // Return a minimal DynamoDB success response
      String responseBody = "{}";
      byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);

      SdkHttpFullResponse response =
          SdkHttpFullResponse.builder()
              .statusCode(200)
              .putHeader("Content-Type", "application/x-amz-json-1.0")
              .putHeader("Content-Length", String.valueOf(body.length))
              .build();

      return HttpExecuteResponse.builder()
          .response(response)
          .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
          .build();
    }

    @Override
    public void close() {
      closed = true;
    }

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
