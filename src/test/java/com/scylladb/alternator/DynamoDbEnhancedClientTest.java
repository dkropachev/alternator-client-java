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
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Unit tests verifying that DynamoDB Enhanced Client works correctly with Alternator load
 * balancing.
 *
 * <p>The enhanced client wraps the standard DynamoDbClient, so load balancing should work
 * transparently when the underlying client is created with AlternatorDynamoDbClient.builder().
 *
 * <p>Uses loopback addresses (127.0.0.x) instead of fake hostnames to avoid DNS lookup timeouts.
 * Connection-refused errors on loopback addresses are instant, keeping unit tests fast.
 */
public class DynamoDbEnhancedClientTest {

  /** Simple model class for testing. */
  @DynamoDbBean
  public static class TestItem {
    private String id;
    private String data;

    public TestItem() {}

    public TestItem(String id, String data) {
      this.id = id;
      this.data = data;
    }

    @DynamoDbPartitionKey
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getData() {
      return data;
    }

    public void setData(String data) {
      this.data = data;
    }
  }

  private static final StaticCredentialsProvider TEST_CREDENTIALS =
      StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

  /**
   * Returns a free local port. The port is released immediately so it can be used by the test. This
   * ensures connection-refused errors (instant) instead of DNS lookup timeouts (slow).
   */
  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * Creates 3 loopback-address URIs on a random unused port for fast-failing connection tests. Uses
   * distinct loopback IPs (127.0.0.1, 127.0.0.2, 127.0.0.3) so AlternatorLiveNodes treats them as
   * separate nodes, while avoiding DNS lookup timeouts that occur with fake hostnames.
   */
  private static List<URI> createLocalNodes() throws IOException, URISyntaxException {
    int port = findFreePort();
    return Arrays.asList(
        new URI("http://127.0.0.1:" + port),
        new URI("http://127.0.0.2:" + port),
        new URI("http://127.0.0.3:" + port));
  }

  private static AlternatorLiveNodes liveNodesWithHealthDisabled(List<URI> nodes) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(
                Arrays.asList(
                    nodes.get(0).getHost(), nodes.get(1).getHost(), nodes.get(2).getHost()))
            .withScheme(nodes.get(0).getScheme())
            .withPort(nodes.get(0).getPort())
            .withNodeHealthConfig(NodeHealthConfig.disabled())
            .build();
    return new AlternatorLiveNodes(config);
  }

  @Test
  public void testEnhancedClientCanBeBuiltFromAlternatorClient()
      throws URISyntaxException, IOException {
    List<URI> nodes = createLocalNodes();

    AlternatorLiveNodes liveNodes = liveNodesWithHealthDisabled(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build();

    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .overrideConfiguration(overrideConfig)
            .credentialsProvider(TEST_CREDENTIALS)
            .region(Region.US_EAST_1)
            .build();

    // Build enhanced client from load-balanced client
    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    assertNotNull("Enhanced client should be created successfully", enhancedClient);

    // Verify we can get a table reference
    DynamoDbTable<TestItem> table =
        enhancedClient.table("test_table", TableSchema.fromBean(TestItem.class));
    assertNotNull("Table reference should be created", table);

    client.close();
  }

  @Test
  public void testEnhancedAsyncClientCanBeBuiltFromAlternatorAsyncClient()
      throws URISyntaxException, IOException {
    List<URI> nodes = createLocalNodes();

    AlternatorLiveNodes liveNodes = liveNodesWithHealthDisabled(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build();

    DynamoDbAsyncClient asyncClient =
        DynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .overrideConfiguration(overrideConfig)
            .credentialsProvider(TEST_CREDENTIALS)
            .region(Region.US_EAST_1)
            .build();

    // Build enhanced async client from load-balanced async client
    DynamoDbEnhancedAsyncClient enhancedAsyncClient =
        DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(asyncClient).build();

    assertNotNull("Enhanced async client should be created successfully", enhancedAsyncClient);

    asyncClient.close();
  }

  @Test
  public void testLoadBalancingWorksWithEnhancedClient() throws URISyntaxException, IOException {
    List<URI> nodes = createLocalNodes();

    AlternatorLiveNodes liveNodes = liveNodesWithHealthDisabled(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Track which hosts are being targeted
    Set<String> targetedHosts = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger requestCount = new AtomicInteger(0);

    ExecutionInterceptor hostTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            String host = context.httpRequest().host();
            targetedHosts.add(host);
            requestCount.incrementAndGet();
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .addExecutionInterceptor(hostTracker)
            .apiCallTimeout(Duration.ofMillis(500))
            .build();

    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .credentialsProvider(TEST_CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .region(Region.US_EAST_1)
            .build();

    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    DynamoDbTable<TestItem> table =
        enhancedClient.table("test_table", TableSchema.fromBean(TestItem.class));

    // Make multiple requests - with random distribution across 3 nodes
    for (int i = 0; i < 30; i++) {
      final int index = i;
      try {
        table.getItem(r -> r.key(k -> k.partitionValue("item-" + index)));
      } catch (Exception e) {
        // Expected - no actual server, but the request was sent
      }
    }

    // Verify that multiple hosts were targeted (random distribution)
    // Note: SDK may retry failed requests, so we verify distribution rather than exact count
    assertTrue("Should have made at least 30 requests", requestCount.get() >= 30);
    // With random distribution over 30 requests across 3 nodes, we should hit all nodes
    assertEquals(
        "Random distribution should hit all 3 nodes over 30+ requests", 3, targetedHosts.size());
    assertTrue("Should have targeted 127.0.0.1", targetedHosts.contains("127.0.0.1"));
    assertTrue("Should have targeted 127.0.0.2", targetedHosts.contains("127.0.0.2"));
    assertTrue("Should have targeted 127.0.0.3", targetedHosts.contains("127.0.0.3"));

    client.close();
  }

  @Test
  public void testLoadBalancingWorksWithEnhancedAsyncClient()
      throws URISyntaxException, IOException {
    List<URI> nodes = createLocalNodes();

    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(nodes, "http", nodes.get(0).getPort(), "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Track which hosts are being targeted
    Set<String> targetedHosts = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger requestCount = new AtomicInteger(0);

    ExecutionInterceptor hostTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            String host = context.httpRequest().host();
            targetedHosts.add(host);
            requestCount.incrementAndGet();
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .addExecutionInterceptor(hostTracker)
            .apiCallTimeout(Duration.ofMillis(500))
            .build();

    DynamoDbAsyncClient asyncClient =
        DynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .credentialsProvider(TEST_CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .region(Region.US_EAST_1)
            .build();

    DynamoDbEnhancedAsyncClient enhancedAsyncClient =
        DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(asyncClient).build();

    software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable<TestItem> table =
        enhancedAsyncClient.table("test_table", TableSchema.fromBean(TestItem.class));

    // Make multiple async requests - with random distribution across 3 nodes
    List<java.util.concurrent.CompletableFuture<?>> futures = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      final int index = i;
      futures.add(
          table
              .getItem(r -> r.key(k -> k.partitionValue("item-" + index)))
              .exceptionally(e -> null)); // Ignore errors - no actual server
    }

    // Wait for all requests to complete (or fail)
    java.util.concurrent.CompletableFuture.allOf(
            futures.toArray(new java.util.concurrent.CompletableFuture[0]))
        .join();

    // Verify that multiple hosts were targeted (random distribution)
    // Note: SDK may retry failed requests, so we verify distribution rather than exact count
    assertTrue("Should have made at least 30 requests", requestCount.get() >= 30);
    // With random distribution over 30 requests across 3 nodes, we should hit all nodes
    assertEquals(
        "Random distribution should hit all 3 nodes over 30+ requests", 3, targetedHosts.size());
    assertTrue("Should have targeted 127.0.0.1", targetedHosts.contains("127.0.0.1"));
    assertTrue("Should have targeted 127.0.0.2", targetedHosts.contains("127.0.0.2"));
    assertTrue("Should have targeted 127.0.0.3", targetedHosts.contains("127.0.0.3"));

    asyncClient.close();
  }

  @Test
  public void testEnhancedClientWithQueryPlanInterceptor() throws URISyntaxException, IOException {
    List<URI> nodes = createLocalNodes();

    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(nodes, "http", nodes.get(0).getPort(), "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Track hosts
    Set<String> targetedHosts = Collections.synchronizedSet(new HashSet<>());

    ExecutionInterceptor hostTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            targetedHosts.add(context.httpRequest().host());
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .addExecutionInterceptor(hostTracker)
            .apiCallTimeout(Duration.ofMillis(500))
            .build();

    // Using BasicQueryPlanInterceptor for load balancing
    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .credentialsProvider(TEST_CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .region(Region.US_EAST_1)
            .build();

    // Build enhanced client - this is the pattern users should follow
    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    DynamoDbTable<TestItem> table =
        enhancedClient.table("test_table", TableSchema.fromBean(TestItem.class));

    // Multiple operations should hit different nodes
    for (int i = 0; i < 30; i++) {
      try {
        table.putItem(new TestItem("id-" + i, "data-" + i));
      } catch (Exception e) {
        // Expected - no actual server
      }
    }

    // With random distribution over 30 requests across 3 nodes, we should hit all nodes
    assertEquals("Should target all 3 nodes", 3, targetedHosts.size());

    client.close();
  }

  @Test
  public void testTableSchemaFromBeanWorks() {
    // Verify that the @DynamoDbBean annotation works correctly
    TableSchema<TestItem> schema = TableSchema.fromBean(TestItem.class);

    assertNotNull("Schema should be created", schema);
    assertEquals(
        "Should have partition key 'id'", "id", schema.tableMetadata().primaryPartitionKey());
  }
}
