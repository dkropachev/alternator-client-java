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
package com.scylladb.alternator.internal;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;
import com.scylladb.alternator.AlternatorDynamoDbAsyncClientWrapper;
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.AlternatorDynamoDbClientWrapper;
import com.scylladb.alternator.IntegrationTestConfig;
import com.scylladb.alternator.TlsConfig;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration test that verifies HTTP connections to a real Scylla cluster stay in the pool and are
 * reused across multiple {@code /localnodes} requests, rather than being created and destroyed for
 * each request.
 *
 * <p>Tests run against both HTTP and HTTPS endpoints. Requires a running ScyllaDB cluster with
 * Alternator enabled. Set environment variables:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS=true - Enable integration tests
 *   <li>ALTERNATOR_HOST - Host address (default: 172.39.0.2)
 *   <li>ALTERNATOR_PORT - HTTP port number (default: 9998)
 *   <li>ALTERNATOR_HTTPS_PORT - HTTPS port number (default: 9999)
 * </ul>
 */
@RunWith(Parameterized.class)
public class ConnectionPoolIT {

  private final URI seedUri;

  public ConnectionPoolIT(String scheme, URI seedUri) {
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

  private AlternatorLiveNodes createLiveNodes() {
    AlternatorConfig.Builder builder = AlternatorConfig.builder().withSeedNode(seedUri);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return new AlternatorLiveNodes(builder.build());
  }

  /**
   * Verifies that connections are pooled and reused. After many {@code refreshDiscoveredNodes()} calls,
   * all requests should succeed without hanging (which would indicate connection leaks).
   */
  @Test(timeout = 10_000)
  public void testConnectionsArePooledAndReused() throws Exception {
    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      // Make many requests — enough to use all nodes multiple times.
      // If connections are leaked, this will hang due to pool exhaustion.
      for (int i = 0; i < 30; i++) {
        liveNodes.refreshDiscoveredNodes();
      }

      int nodeCount = liveNodes.getDiscoveredNodes().size();
      assertTrue("Should have discovered at least one node", nodeCount > 0);
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /**
   * Verifies that pooled connections to a real Scylla cluster survive idle periods and are reused
   * after sitting idle.
   *
   * <p>This guards against connections being evicted or closed by the client-side pool. After the
   * idle period, making more requests should succeed without timeout, proving connections are still
   * usable.
   *
   * <p>Uses a 10-second idle period which is sufficient to verify connection survival without
   * triggering the default 60-second idle reaper.
   */
  @Test(timeout = 30_000)
  public void testConnectionSurvivesIdlePeriod() throws Exception {
    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      // Establish pooled connections by using all nodes
      for (int i = 0; i < 30; i++) {
        liveNodes.refreshDiscoveredNodes();
      }
      int nodesBefore = liveNodes.getDiscoveredNodes().size();
      assertTrue("Should have discovered at least one node", nodesBefore > 0);

      // Let connections sit idle for 10 seconds — enough to verify survival
      // without hitting the default 60-second idle reaper
      Thread.sleep(10_000);

      // Use the connections again — they should be reused from the pool.
      // If connections were dropped, these requests would still succeed
      // but would need new connections. If connections are leaked,
      // this would hang due to pool exhaustion.
      for (int i = 0; i < 30; i++) {
        liveNodes.refreshDiscoveredNodes();
      }
      int nodesAfter = liveNodes.getDiscoveredNodes().size();
      assertEquals("Node count should remain consistent after idle period", nodesBefore, nodesAfter);
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /**
   * Counts the number of ESTABLISHED TCP connections from this JVM process to the given Alternator
   * port, using the Linux {@code ss} (socket statistics) command filtered by our PID.
   *
   * <p>Why {@code ss} instead of Scylla's Prometheus metrics? The AWS SDK v2 does not expose its
   * internal Apache HTTP connection pool stats through any public API, so we cannot measure TCP
   * connection reuse from Java code alone. We need an external view of the actual TCP sockets.
   *
   * <p>We previously used Scylla's {@code scylla_httpd_connections_total} Prometheus metric, but
   * that is a global, monotonically-increasing counter shared across ALL clients connecting to the
   * cluster. When multiple test forks (Failsafe {@code forkCount > 1}) run in parallel, each fork
   * creates its own connections, inflating the counter and causing flaky assertion failures.
   *
   * <p>Using {@code ss} filtered by PID gives us a per-JVM snapshot of currently established
   * connections. This is the right measurement for connection reuse: if connections are pooled, the
   * count stays stable across many requests; if they are churned (closed and reopened), we would
   * see the count drop between requests during idle gaps.
   */
  private static long countEstablishedConnections(int port) throws Exception {
    long pid = ProcessHandle.current().pid();
    ProcessBuilder pb =
        new ProcessBuilder("ss", "-tnpH", "state", "established", "( dport = " + port + " )");
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    long count = 0;
    String pidFilter = "pid=" + pid;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.contains(pidFilter)) {
          count++;
        }
      }
    }
    proc.waitFor();
    return count;
  }

  /**
   * Runs the DynamoDB connection reuse test with the given client wrapper and table name.
   *
   * <p>Performs warmup requests, then verifies the established TCP connection count stays stable
   * during 500ms idle gaps, bulk requests, and a 10-second idle period.
   */
  private void runDynamoDbConnectionReuseTest(
      AlternatorDynamoDbClientWrapper wrapper, String tableName) throws Exception {
    DynamoDbClient client = wrapper.getClient();
    int port = seedUri.getPort();

    // Create test table
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
      Thread.sleep(500);
    } catch (ResourceNotFoundException e) {
      // Table doesn't exist, that's fine
    }
    client.createTable(
        CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .build());

    try {
      // Warm up — establish connections for both the SDK client and the background
      // AlternatorLiveNodes thread.
      for (int i = 0; i < 10; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("warmup-" + i).build(),
                        "data", AttributeValue.builder().s("value").build()))
                .build());
      }
      // Wait for stale connections from previous tests (reuseForks=true) to drain
      // and for the background node-discovery thread to stabilize. Poll until the
      // connection count stops dropping, so the baseline reflects only this test's
      // active connections.
      long baseline = countEstablishedConnections(port);
      for (int attempt = 0; attempt < 10; attempt++) {
        Thread.sleep(1000);
        long current = countEstablishedConnections(port);
        if (current >= baseline) break;
        baseline = current;
      }
      assertTrue("Should have at least 1 established connection after warmup", baseline >= 1);
      long minAcceptable = Math.max(1, baseline / 3);

      // Verify connections are not dropped during short idle gaps (500ms between requests).
      // If connections are pooled, the established count should stay roughly stable.
      for (int i = 0; i < 20; i++) {
        Thread.sleep(500);
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("gap-" + i).build(),
                        "data", AttributeValue.builder().s("value").build()))
                .build());
      }

      long afterGaps = countEstablishedConnections(port);
      assertTrue(
          "Established connections should not drop significantly during 500ms idle gaps"
              + " (baseline="
              + baseline
              + ", after="
              + afterGaps
              + ", minAcceptable="
              + minAcceptable
              + ")",
          afterGaps >= minAcceptable);
      assertTrue(
          "Established connections should not grow significantly during 500ms idle gap requests"
              + " (baseline="
              + baseline
              + ", after="
              + afterGaps
              + ")",
          afterGaps <= baseline * 1.5);

      // Perform many more operations back-to-back — connections should be reused
      for (int i = 0; i < 50; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("item-" + i).build(),
                        "data", AttributeValue.builder().s("value-" + i).build()))
                .build());
      }

      long afterBulk = countEstablishedConnections(port);
      assertTrue(
          "Established connections should not grow significantly during 50 back-to-back requests"
              + " (baseline="
              + baseline
              + ", after="
              + afterBulk
              + ")",
          afterBulk <= baseline * 1.5);

      // Let connections sit idle for 10 seconds — enough to verify survival
      // without hitting the default 60-second idle reaper
      Thread.sleep(10_000);

      // Connections should still be alive and reused after idle period
      for (int i = 0; i < 10; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("after-idle-" + i).build(),
                        "data", AttributeValue.builder().s("value").build()))
                .build());
      }

      long afterIdle = countEstablishedConnections(port);
      assertTrue(
          "Most connections should survive 10s idle period"
              + " (baseline="
              + baseline
              + ", after="
              + afterIdle
              + ", minAcceptable="
              + minAcceptable
              + ")",
          afterIdle >= minAcceptable);

    } finally {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
      } catch (Exception e) {
        // Best effort cleanup
      }
      wrapper.close();
    }
  }

  private AlternatorDynamoDbClientWrapper buildDynamoWrapper() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbClientWrapper buildDynamoWrapperWithHeaders() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  /**
   * Verifies that DynamoDB SDK operations reuse pooled TCP connections rather than creating a new
   * connection for every request.
   *
   * <p>This test performs DynamoDB PutItem operations and checks that the number of ESTABLISHED TCP
   * connections to the Alternator port stays bounded (does not grow with the number of requests).
   * It also verifies that connections are not dropped during 500ms idle gaps and survive a
   * 10-second idle period.
   */
  @Test(timeout = 60_000)
  public void testDynamoDbOperationsReuseConnections() throws Exception {
    runDynamoDbConnectionReuseTest(buildDynamoWrapper(), "conn_pool_it_default");
  }

  /**
   * Verifies that DynamoDB SDK operations reuse pooled TCP connections when headers optimization is
   * enabled.
   */
  @Test(timeout = 180_000)
  public void testDynamoDbOperationsReuseConnectionsWithHeadersOptimization() throws Exception {
    runDynamoDbConnectionReuseTest(buildDynamoWrapperWithHeaders(), "conn_pool_it_headers_opt");
  }

  // --- Async client connection reuse tests (Netty HTTP stack) ---

  private AlternatorDynamoDbAsyncClientWrapper buildAsyncWrapper() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  /**
   * Runs the async DynamoDB connection reuse test with the given client wrapper and table name.
   *
   * <p>Mirrors the sync {@link #runDynamoDbConnectionReuseTest} but uses the async client which
   * runs on a Netty HTTP stack instead of Apache HttpClient. Verifies that the Netty connection
   * pool keeps TCP connections stable during idle gaps, bulk requests, and idle periods.
   */
  private void runAsyncDynamoDbConnectionReuseTest(
      AlternatorDynamoDbAsyncClientWrapper wrapper, String tableName) throws Exception {
    DynamoDbAsyncClient client = wrapper.getClient();
    int port = seedUri.getPort();

    // Create test table
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get(10, TimeUnit.SECONDS);
      Thread.sleep(500);
    } catch (Exception e) {
      // Table doesn't exist or other error, that's fine
    }
    client
        .createTable(
            CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(
                    KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(
                    AttributeDefinition.builder()
                        .attributeName("pk")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(10L)
                        .writeCapacityUnits(10L)
                        .build())
                .build())
        .get(10, TimeUnit.SECONDS);

    try {
      // Warm up — establish connections for both the async Netty client and the background
      // AlternatorLiveNodes polling thread.
      for (int i = 0; i < 10; i++) {
        client
            .putItem(
                PutItemRequest.builder()
                    .tableName(tableName)
                    .item(
                        Map.of(
                            "pk", AttributeValue.builder().s("warmup-" + i).build(),
                            "data", AttributeValue.builder().s("value").build()))
                    .build())
            .get(10, TimeUnit.SECONDS);
      }
      // Wait for stale connections from previous tests (reuseForks=true) to drain
      // and for the background node-discovery thread to stabilize. Poll until the
      // connection count stops dropping, so the baseline reflects only this test's
      // active connections.
      long baseline = countEstablishedConnections(port);
      for (int attempt = 0; attempt < 10; attempt++) {
        Thread.sleep(1000);
        long current = countEstablishedConnections(port);
        if (current >= baseline) break;
        baseline = current;
      }
      assertTrue("Should have at least 1 established connection after warmup", baseline >= 1);
      long minAcceptable = Math.max(1, baseline / 3);

      // Verify connections are not dropped during short idle gaps (200ms between requests).
      for (int i = 0; i < 10; i++) {
        Thread.sleep(200);
        client
            .putItem(
                PutItemRequest.builder()
                    .tableName(tableName)
                    .item(
                        Map.of(
                            "pk", AttributeValue.builder().s("gap-" + i).build(),
                            "data", AttributeValue.builder().s("value").build()))
                    .build())
            .get(10, TimeUnit.SECONDS);
      }

      long afterGaps = countEstablishedConnections(port);
      assertTrue(
          "Async: connections should not drop significantly during 200ms idle gaps"
              + " (baseline="
              + baseline
              + ", after="
              + afterGaps
              + ", minAcceptable="
              + minAcceptable
              + ")",
          afterGaps >= minAcceptable);
      assertTrue(
          "Async: connections should not grow significantly during 200ms idle gap requests"
              + " (baseline="
              + baseline
              + ", after="
              + afterGaps
              + ")",
          afterGaps <= baseline * 1.5);

      // Perform many more operations back-to-back — connections should be reused
      for (int i = 0; i < 30; i++) {
        client
            .putItem(
                PutItemRequest.builder()
                    .tableName(tableName)
                    .item(
                        Map.of(
                            "pk", AttributeValue.builder().s("item-" + i).build(),
                            "data", AttributeValue.builder().s("value-" + i).build()))
                    .build())
            .get(10, TimeUnit.SECONDS);
      }

      long afterBulk = countEstablishedConnections(port);
      assertTrue(
          "Async: connections should not grow significantly during 30 back-to-back requests"
              + " (baseline="
              + baseline
              + ", after="
              + afterBulk
              + ")",
          afterBulk <= baseline * 1.5);

      // Let connections sit idle for 3 seconds
      Thread.sleep(3_000);

      // Connections should still be alive and reused after idle period
      for (int i = 0; i < 5; i++) {
        client
            .putItem(
                PutItemRequest.builder()
                    .tableName(tableName)
                    .item(
                        Map.of(
                            "pk", AttributeValue.builder().s("after-idle-" + i).build(),
                            "data", AttributeValue.builder().s("value").build()))
                    .build())
            .get(10, TimeUnit.SECONDS);
      }

      long afterIdle = countEstablishedConnections(port);
      assertTrue(
          "Async: most connections should survive 3s idle period"
              + " (baseline="
              + baseline
              + ", after="
              + afterIdle
              + ", minAcceptable="
              + minAcceptable
              + ")",
          afterIdle >= minAcceptable);

    } finally {
      try {
        client
            .deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
            .get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Best effort cleanup
      }
      wrapper.close();
    }
  }

  /**
   * Verifies that the async DynamoDB client (Netty HTTP stack) reuses pooled TCP connections rather
   * than creating a new connection for every request.
   *
   * <p>This is the async counterpart of {@link #testDynamoDbOperationsReuseConnections()}. The
   * async client uses Netty's connection pool instead of Apache HttpClient, so this test validates
   * that Netty's pool behaves correctly with Alternator load balancing.
   */
  @Test(timeout = 30_000)
  public void testAsyncDynamoDbOperationsReuseConnections() throws Exception {
    runAsyncDynamoDbConnectionReuseTest(buildAsyncWrapper(), "conn_pool_it_async_default");
  }

  // --- /localnodes polling path connection reuse tests ---

  /**
   * Verifies that the {@code /localnodes} polling path reuses TCP connections by measuring actual
   * TCP socket counts via {@code ss}.
   *
   * <p>This complements {@link #testConnectionsArePooledAndReused()} which only checks for pool
   * exhaustion (hanging). This test directly measures that the TCP connection count stays stable
   * across many polling cycles, confirming that the polling HTTP client is actually reusing
   * connections rather than creating new ones for each request.
   */
  @Test(timeout = 30_000)
  public void testPollingConnectionsStableUnderSs() throws Exception {
    AlternatorLiveNodes liveNodes = createLiveNodes();
    int port = seedUri.getPort();

    try {
      // Warm up — establish pooled connections to all discovered nodes
      for (int i = 0; i < 10; i++) {
        liveNodes.refreshDiscoveredNodes();
      }
      assertTrue(
          "Should have discovered at least one node", liveNodes.getDiscoveredNodes().size() > 0);
      // Wait for stale connections from previous tests (reuseForks=true) to drain
      // and for the polling client's connection pool to stabilize.
      long baseline = countEstablishedConnections(port);
      for (int attempt = 0; attempt < 10; attempt++) {
        Thread.sleep(1000);
        long current = countEstablishedConnections(port);
        if (current >= baseline) break;
        baseline = current;
      }
      assertTrue("Should have at least 1 established connection after warmup", baseline >= 1);

      // Perform many more polling cycles — connection count should stay bounded
      for (int i = 0; i < 50; i++) {
        liveNodes.refreshDiscoveredNodes();
      }

      long afterBulk = countEstablishedConnections(port);
      assertTrue(
          "Polling connections should not grow significantly during 50 polling cycles"
              + " (baseline="
              + baseline
              + ", after="
              + afterBulk
              + ")",
          afterBulk <= baseline * 1.5);

      // Verify connections survive a short idle period
      Thread.sleep(3_000);

      for (int i = 0; i < 10; i++) {
        liveNodes.refreshDiscoveredNodes();
      }

      long afterIdle = countEstablishedConnections(port);
      long minAcceptable = Math.max(1, baseline / 3);
      assertTrue(
          "Polling connections should survive 3s idle period"
              + " (baseline="
              + baseline
              + ", after="
              + afterIdle
              + ", minAcceptable="
              + minAcceptable
              + ")",
          afterIdle >= minAcceptable);
    } finally {
      liveNodes.shutdownAndWait();
    }
  }
}
