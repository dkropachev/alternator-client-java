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
import static org.junit.Assume.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration tests for AlternatorDynamoDbAsyncClient. These tests require a running ScyllaDB
 * cluster with Alternator enabled.
 *
 * <p>Tests run against both HTTP and HTTPS endpoints provisioned by the CCM harness. Set
 * environment variables to configure:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS: Set to "true" to enable these tests
 *   <li>SCYLLA_VERSION: CCM Scylla package selector
 * </ul>
 */
@RunWith(Parameterized.class)
public class AlternatorDynamoDbAsyncClientIT {

  private final URI seedUri;

  public AlternatorDynamoDbAsyncClientIT(String scheme, URI seedUri) {
    this.seedUri = seedUri;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return IntegrationTestConfig.httpAndHttpsEndpoints();
  }

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  private AlternatorDynamoDbAsyncClientWrapper buildClient(String dc, String rackName) {
    RoutingScope scope = deriveRoutingScope(dc, rackName);
    return AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(seedUri)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withRoutingScope(scope)
        .buildWithAlternatorAPI();
  }

  private static RoutingScope deriveRoutingScope(String dc, String rackName) {
    if (dc == null || dc.isEmpty()) {
      return ClusterScope.create();
    }
    if (rackName == null || rackName.isEmpty()) {
      return DatacenterScope.of(dc, ClusterScope.create());
    }
    return RackScope.of(dc, rackName, DatacenterScope.of(dc, ClusterScope.create()));
  }

  private AlternatorDynamoDbAsyncClientWrapper buildClient() {
    return AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(seedUri)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .buildWithAlternatorAPI();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_WrongDC() throws Exception {
    // Wrong datacenter should gracefully fall back to no datacenter filtering
    AlternatorDynamoDbAsyncClientWrapper client = buildClient("wrongDC", "");

    // Should have discovered nodes (fallback should work)
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node after fallback", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_CorrectDC() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(IntegrationTestConfig.DATACENTER, "");

    // Should have discovered nodes
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_WrongRack() throws Exception {
    // With wrong rack, pickSupportedDatacenterRack should fallback gracefully
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(IntegrationTestConfig.DATACENTER, "wrongRack");

    // Should have fallen back and discovered nodes
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node after fallback", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_CorrectRack() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(IntegrationTestConfig.DATACENTER, IntegrationTestConfig.RACK);

    // Should have discovered nodes
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackDatacenterFeatureIsSupported() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(IntegrationTestConfig.DATACENTER, "");

    boolean supported = client.checkIfRackDatacenterFeatureIsSupported();

    assertTrue("ScyllaDB should support rack/datacenter filtering", supported);

    client.close();
  }

  @Test
  public void testRoutingFallback() throws Exception {
    // Create with wrong datacenter - should fallback
    AlternatorDynamoDbAsyncClientWrapper client = buildClient("wrongDC", "");

    // Should have discovered nodes (fallback should work)
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node after fallback", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testUpdateLiveNodes() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient();

    // The background thread is already started by AlternatorLiveNodes
    // Wait for node list to update
    Thread.sleep(1000);

    List<URI> updatedNodes = client.getLiveNodes();

    // In a multi-node cluster, we should have more nodes
    // In a single-node cluster, we should still have at least one
    assertFalse("Should have at least one node after update", updatedNodes.isEmpty());

    client.close();
  }

  @Test
  public void testNodeDiscoveryWithRandomQueryPlan() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(IntegrationTestConfig.DATACENTER, "");

    // Wait for node discovery
    Thread.sleep(1000);

    List<URI> allNodes = client.getLiveNodes();
    assertFalse("Should have at least one live node", allNodes.isEmpty());

    Set<URI> metNodes = new HashSet<>();
    LazyQueryPlan queryPlan = new LazyQueryPlan(client.getAlternatorLiveNodes());
    while (queryPlan.hasNext()) {
      metNodes.add(queryPlan.next());
    }

    assertEquals("Should visit all live nodes via random query plan", new HashSet<>(allNodes), metNodes);

    client.close();
  }

  @Test
  public void testDynamoDBOperations() throws Exception {
    String tableName = "java_async_client_test_table_" + seedUri.getScheme();

    AlternatorDynamoDbAsyncClientWrapper wrapper = buildClient();
    DynamoDbAsyncClient client = wrapper.getClient();

    try {
      // Clean up if table exists
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
        Thread.sleep(500);
      } catch (Exception e) {
        // Table doesn't exist, that's fine
      }

      // Create table
      client
          .createTable(
              CreateTableRequest.builder()
                  .tableName(tableName)
                  .keySchema(
                      KeySchemaElement.builder().attributeName("ID").keyType(KeyType.HASH).build())
                  .attributeDefinitions(
                      AttributeDefinition.builder()
                          .attributeName("ID")
                          .attributeType(ScalarAttributeType.S)
                          .build())
                  .provisionedThroughput(
                      ProvisionedThroughput.builder()
                          .readCapacityUnits(1L)
                          .writeCapacityUnits(1L)
                          .build())
                  .build())
          .get();

      // Put item
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName(tableName)
                  .item(
                      java.util.Map.of(
                          "ID", AttributeValue.builder().s("123").build(),
                          "Name", AttributeValue.builder().s("test-value").build()))
                  .build())
          .get();

      GetItemResponse getResult = getItemEventually(client, tableName, "123");
      java.util.Map<String, AttributeValue> item = getResult.item();

      assertNotNull("Should get item back", item);
      assertTrue("Should get ID attribute back", item.containsKey("ID"));
      assertTrue("Should get Name attribute back", item.containsKey("Name"));
      assertEquals("123", item.get("ID").s());
      assertEquals("test-value", item.get("Name").s());

      // Delete item
      client
          .deleteItem(
              DeleteItemRequest.builder()
                  .tableName(tableName)
                  .key(java.util.Map.of("ID", AttributeValue.builder().s("123").build()))
                  .build())
          .get();

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
    } finally {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
      } catch (Exception e) {
        // Best effort cleanup
      }

      wrapper.close();
    }
  }

  private GetItemResponse getItemEventually(
      DynamoDbAsyncClient client, String tableName, String id) throws Exception {
    GetItemRequest request =
        GetItemRequest.builder()
            .tableName(tableName)
            .key(java.util.Map.of("ID", AttributeValue.builder().s(id).build()))
            .consistentRead(true)
            .build();
    GetItemResponse response = null;
    for (int attempt = 0; attempt < 10; attempt++) {
      response = client.getItem(request).get();
      java.util.Map<String, AttributeValue> item = response.item();
      if (item != null && item.containsKey("ID") && item.containsKey("Name")) {
        return response;
      }
      Thread.sleep(200);
    }
    return response;
  }

  @Test
  public void testAccessToAlternatorLiveNodes() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient();

    // Test that wrapper exposes live nodes
    AlternatorLiveNodes liveNodes = client.getAlternatorLiveNodes();
    assertNotNull("Should be able to access live nodes", liveNodes);

    // Test that wrapper exposes live nodes list directly
    List<URI> nodes = client.getLiveNodes();
    assertNotNull("Should be able to access live nodes from wrapper", nodes);

    client.close();
  }

  @Test
  public void testClientWithCompressionEnabled() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withRoutingScope(DatacenterScope.of(IntegrationTestConfig.DATACENTER, ClusterScope.create()))
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(512)
            .buildWithAlternatorAPI();

    // Verify client is functional with compression enabled
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Perform a simple async operation to verify compression doesn't break requests
    try {
      wrapper.getClient().listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // ListTables should work with compression enabled
      fail("Async ListTables should succeed with compression enabled: " + e.getMessage());
    }

    wrapper.close();
  }

  @Test
  public void testCompressionWithLargePayload() throws Exception {
    // Track if Content-Encoding: gzip header was seen
    AtomicBoolean compressionHeaderSeen = new AtomicBoolean(false);

    ExecutionInterceptor compressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(
                    value -> {
                      if (value.contains("gzip")) {
                        compressionHeaderSeen.set(true);
                      }
                    });
          }
        };

    // Build client with the compression verifier interceptor
    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(compressionVerifier).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100) // Low threshold to ensure compression kicks in
            .build();

    // Create a large payload that should trigger compression (> 100 bytes threshold)
    StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeValue.append("This is a test value that should be compressed. ");
    }

    // Perform an async operation with the large payload
    try {
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName("nonexistent_table_for_compression_test")
                  .item(
                      java.util.Map.of(
                          "ID", AttributeValue.builder().s("compression-test").build(),
                          "LargeData", AttributeValue.builder().s(largeValue.toString()).build()))
                  .build())
          .get();
    } catch (Exception e) {
      // Table doesn't exist - that's expected, we just want to verify the request was compressed
    }

    assertTrue(
        "Content-Encoding: gzip header should be present for large payloads",
        compressionHeaderSeen.get());

    client.close();
  }

  @Test
  public void testNoCompressionForSmallPayload() throws Exception {
    // Track if Content-Encoding: gzip header was seen
    AtomicBoolean compressionHeaderSeen = new AtomicBoolean(false);

    ExecutionInterceptor compressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(
                    value -> {
                      if (value.contains("gzip")) {
                        compressionHeaderSeen.set(true);
                      }
                    });
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(compressionVerifier).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(10000) // High threshold - small requests won't compress
            .build();

    // Perform a small async operation (listTables request is small)
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the header
    }

    assertFalse(
        "Content-Encoding: gzip header should NOT be present for small payloads",
        compressionHeaderSeen.get());

    client.close();
  }

  @Test
  public void testResponseCompressionDisabledByDefaultDoesNotSetAcceptEncoding() throws Exception {
    AtomicReference<String> acceptEncoding = new AtomicReference<>();

    ExecutionInterceptor responseCompressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Accept-Encoding")
                .ifPresent(acceptEncoding::set);
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(responseCompressionVerifier)
            .build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .build();

    try {
      client.listTables(ListTablesRequest.builder().limit(1).build()).get();
      assertNull(acceptEncoding.get());
    } finally {
      client.close();
    }
  }

  @Test
  public void testResponseCompressionNegotiatesAcceptEncoding() throws Exception {
    AtomicReference<String> acceptEncoding = new AtomicReference<>();

    ExecutionInterceptor responseCompressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Accept-Encoding")
                .ifPresent(acceptEncoding::set);
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(responseCompressionVerifier)
            .build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withResponseCompression(
                ResponseCompressionAlgorithm.GZIP, ResponseCompressionAlgorithm.DEFLATE)
            .build();

    try {
      client.listTables(ListTablesRequest.builder().limit(1).build()).get();
      assertEquals(ResponseCompressionInterceptor.ACCEPT_ENCODING, acceptEncoding.get());
    } finally {
      client.close();
    }
  }

  @Test
  public void testResponseCompressionNegotiatesCustomAcceptEncoding() throws Exception {
    AtomicReference<String> acceptEncoding = new AtomicReference<>();

    ExecutionInterceptor responseCompressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Accept-Encoding")
                .ifPresent(acceptEncoding::set);
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(responseCompressionVerifier)
            .build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withResponseCompression(ResponseCompressionAlgorithm.DEFLATE)
            .build();

    try {
      client.listTables(ListTablesRequest.builder().limit(1).build()).get();
      assertEquals("deflate", acceptEncoding.get());
    } finally {
      client.close();
    }
  }

  @Test
  public void testHeadersOptimizationFiltersHeaders() throws Exception {
    // Track which headers are seen in the request
    Set<String> seenHeaders = new HashSet<>();

    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withOptimizeHeaders(true)
            .build();

    // Perform an async operation
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the headers
    }

    // Required headers should be present
    assertTrue("Host header should be present", seenHeaders.contains("Host"));
    assertTrue("X-Amz-Target header should be present", seenHeaders.contains("X-Amz-Target"));
    assertTrue("Content-Type header should be present", seenHeaders.contains("Content-Type"));
    assertTrue("Authorization header should be present", seenHeaders.contains("Authorization"));
    assertTrue("X-Amz-Date header should be present", seenHeaders.contains("X-Amz-Date"));

    // Note: Header filtering happens at HTTP client level (after SDK interceptors run).
    // SDK interceptors see unfiltered headers; actual wire traffic is filtered.
    // The filtering is verified in HeadersFilteringSdkAsyncHttpClientTest.

    client.close();
  }

  @Test
  public void testHeadersOptimizationWithCustomWhitelist() throws Exception {
    // Track which headers are seen in the request
    Set<String> seenHeaders = new HashSet<>();

    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };

    // Custom whitelist that keeps User-Agent while filtering non-required SDK metadata
    Set<String> customWhitelist =
        new HashSet<>(
            java.util.Arrays.asList(
                "Host",
                "X-Amz-Target",
                "Content-Type",
                "Content-Length",
                "Authorization",
                "X-Amz-Date",
                "Connection",
                "User-Agent" // Include User-Agent in custom whitelist
                ));

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customWhitelist)
            .build();

    // Perform an async operation
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the headers
    }

    // Custom whitelist headers should be present
    assertTrue(
        "User-Agent should be present (custom whitelist)", seenHeaders.contains("User-Agent"));
    assertTrue("Host header should be present", seenHeaders.contains("Host"));

    // Headers not in custom whitelist should be filtered
    assertFalse(
        "X-Amz-Sdk-Invocation-Id should be filtered",
        seenHeaders.contains("X-Amz-Sdk-Invocation-Id"));

    client.close();
  }

  @Test
  public void testHeadersOptimizationDisabledByDefault() throws Exception {
    // Track which headers are seen in the request
    Set<String> seenHeaders = new HashSet<>();

    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };

    // Don't enable headers optimization (use default settings)
    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .build();

    // Perform an async operation
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the headers
    }

    // When disabled, SDK metadata headers should still be present
    assertTrue(
        "User-Agent should be present when optimization disabled",
        seenHeaders.contains("User-Agent"));

    client.close();
  }

  @Test
  public void testHeadersOptimizationWithCompression() throws Exception {
    // Track headers and compression
    Set<String> seenHeaders = new HashSet<>();
    AtomicBoolean compressionHeaderSeen = new AtomicBoolean(false);

    ExecutionInterceptor tracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(
                    value -> {
                      if (value.contains("gzip")) {
                        compressionHeaderSeen.set(true);
                      }
                    });
          }
        };

    // Enable both headers optimization and compression
    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(tracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .withOptimizeHeaders(true)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .build();

    // Create a large payload that should trigger compression
    StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeValue.append("This is a test value that should be compressed. ");
    }

    // Perform an async operation with the large payload
    try {
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName("nonexistent_table_for_test")
                  .item(
                      java.util.Map.of(
                          "ID", AttributeValue.builder().s("test").build(),
                          "LargeData", AttributeValue.builder().s(largeValue.toString()).build()))
                  .build())
          .get();
    } catch (Exception e) {
      // Expected - table doesn't exist
    }

    // Verify both features work together
    assertTrue(
        "Content-Encoding should be present for compression",
        seenHeaders.contains("Content-Encoding"));
    assertTrue("Compression should have been applied", compressionHeaderSeen.get());

    // Note: Header filtering happens at HTTP client level (after SDK interceptors run).
    // SDK interceptors see unfiltered headers; actual wire traffic is filtered.

    client.close();
  }

}
