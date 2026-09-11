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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration tests that verify each async HTTP client implementation works correctly with the
 * Alternator load balancer, including headers optimization, compression, connection pool config, and
 * TLS.
 *
 * <p>Tests cover Netty and CRT async clients against a CCM-provisioned ScyllaDB cluster. Set
 * environment variables:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS=true
 *   <li>SCYLLA_VERSION selects the CCM Scylla package
 * </ul>
 */
public class HttpClientImplementationAsyncIT {

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  // ===========================================================================
  // Netty Async Client
  // ===========================================================================

  @Test
  public void testNettyClientNodeDiscovery() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper = buildNettyWrapper();
    try {
      assertFalse("Netty client should discover at least one node", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testNettyClientDynamoDbOperations() throws Exception {
    runAsyncDynamoDbCrudTest(buildNettyWrapper(), "netty_async_it_table");
  }

  @Test
  public void testNettyClientWithConfigPropagation() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withMaxConnections(50)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withNettyHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();

    try {
      assertFalse("Should discover nodes with Netty config", wrapper.getLiveNodes().isEmpty());
      AlternatorConfig rc = wrapper.getAlternatorConfig();
      assertEquals(50, rc.getMaxConnections());
      assertEquals(30000, rc.getConnectionMaxIdleTimeMs());
      assertEquals(60000, rc.getConnectionTimeToLiveMs());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testNettyClientWithCustomizer() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withNettyHttpClientCustomizer(builder -> builder.maxConcurrency(100))
            .buildWithAlternatorAPI();
    runAsyncDynamoDbCrudTest(wrapper, "netty_customizer_it_table");
  }

  @Test
  public void testNettyClientHttps() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(TlsConfig.trustAll())
            .withNettyHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    try {
      assertFalse("Netty HTTPS client should discover nodes", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testNettyClientHttpsWithCustomCa() throws Exception {
    Path caCertPath = IntegrationTestConfig.CA_CERT_PATH;
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        caCertPath != null && Files.exists(caCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(caCertPath).withTrustSystemCaCerts(false).build();

    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withNettyHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    try {
      assertFalse(
          "Netty HTTPS client with custom CA should discover nodes",
          wrapper.getLiveNodes().isEmpty());
      wrapper.getClient().listTables(ListTablesRequest.builder().limit(1).build()).get();
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testNettyClientHttpsWithCustomCaCrud() throws Exception {
    Path caCertPath = IntegrationTestConfig.CA_CERT_PATH;
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        caCertPath != null && Files.exists(caCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(caCertPath).withTrustSystemCaCerts(false).build();

    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withNettyHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    runAsyncDynamoDbCrudTest(wrapper, "netty_custom_ca_it_table");
  }

  @Test
  public void testNettyClientWithHeadersOptimization() throws Exception {
    verifyHeadersOptimization(
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withNettyHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testNettyClientWithCompression() throws Exception {
    verifyCompression(
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withNettyHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testNettyClientWithHeadersOptimizationAndCompression() throws Exception {
    verifyHeadersAndCompression(
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withNettyHttpClientCustomizer(builder -> {}));
  }


  // ===========================================================================
  // CRT Async Client
  // ===========================================================================

  @Test
  public void testCrtAsyncClientNodeDiscovery() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper = buildCrtAsyncWrapper();
    try {
      assertFalse(
          "CRT async client should discover at least one node", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testCrtAsyncClientDynamoDbOperations() throws Exception {
    runAsyncDynamoDbCrudTest(buildCrtAsyncWrapper(), "crt_async_it_table");
  }

  @Test
  public void testCrtAsyncClientWithConfigPropagation() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withMaxConnections(50)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtAsyncHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();

    try {
      assertFalse("Should discover nodes with CRT async config", wrapper.getLiveNodes().isEmpty());
      AlternatorConfig rc = wrapper.getAlternatorConfig();
      assertEquals(50, rc.getMaxConnections());
      assertEquals(30000, rc.getConnectionMaxIdleTimeMs());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testCrtAsyncClientWithCustomizer() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withCrtAsyncHttpClientCustomizer(builder -> builder.maxConcurrency(100))
            .buildWithAlternatorAPI();
    runAsyncDynamoDbCrudTest(wrapper, "crt_async_customizer_it_table");
  }

  @Test
  public void testCrtAsyncClientHttps() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(TlsConfig.trustAll())
            .withCrtAsyncHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    try {
      assertFalse("CRT async HTTPS client should discover nodes", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  /**
   * CRT HTTP client uses native AWS-CRT TLS and does not support custom CA certificates via Java
   * TrustManagers. This test verifies that initialization fails fast when custom CA is configured
   * with the CRT async client.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCrtAsyncClientHttpsWithCustomCaFailsOnInit() throws Exception {
    Path caCertPath = IntegrationTestConfig.CA_CERT_PATH;
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        caCertPath != null && Files.exists(caCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(caCertPath).withTrustSystemCaCerts(false).build();

    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withTlsConfig(tlsConfig)
        .withCrtAsyncHttpClientCustomizer(builder -> {})
        .buildWithAlternatorAPI();
  }

  @Test
  public void testCrtAsyncClientWithHeadersOptimization() throws Exception {
    verifyHeadersOptimization(
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withCrtAsyncHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testCrtAsyncClientWithCompression() throws Exception {
    verifyCompression(
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withCrtAsyncHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testCrtAsyncClientWithHeadersOptimizationAndCompression() throws Exception {
    verifyHeadersAndCompression(
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withCrtAsyncHttpClientCustomizer(builder -> {}));
  }


  // ===========================================================================
  // Auto-detection (no explicit customizer)
  // ===========================================================================

  @Test
  public void testAutoDetectedAsyncClientNodeDiscovery() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();
    try {
      assertFalse("Auto-detected async client should discover nodes", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAutoDetectedAsyncClientDynamoDbOperations() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();
    runAsyncDynamoDbCrudTest(wrapper, "autodetect_async_it_table");
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private AlternatorDynamoDbAsyncClientWrapper buildNettyWrapper() {
    return AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withNettyHttpClientCustomizer(builder -> {})
        .buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbAsyncClientWrapper buildCrtAsyncWrapper() {
    return AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withCrtAsyncHttpClientCustomizer(builder -> {})
        .buildWithAlternatorAPI();
  }

  /**
   * Verifies headers optimization is active: required headers are present at the SDK interceptor
   * level.
   */
  private void verifyHeadersOptimization(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder clientBuilder) {
    Set<String> seenHeaders = new HashSet<>();
    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes attrs) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };
    clientBuilder.overrideConfiguration(
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build());

    DynamoDbAsyncClient client = clientBuilder.build();
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore
    } finally {
      client.close();
    }

    assertTrue("Host header should be present", seenHeaders.contains("Host"));
    assertTrue("X-Amz-Target header should be present", seenHeaders.contains("X-Amz-Target"));
    assertTrue("Content-Type header should be present", seenHeaders.contains("Content-Type"));
    assertTrue("Authorization header should be present", seenHeaders.contains("Authorization"));
    assertTrue("X-Amz-Date header should be present", seenHeaders.contains("X-Amz-Date"));
  }

  /**
   * Verifies that GZIP compression is applied: sends a large payload and checks that
   * Content-Encoding: gzip header appears.
   */
  private void verifyCompression(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder clientBuilder) {
    AtomicBoolean gzipSeen = new AtomicBoolean(false);
    ExecutionInterceptor compressionTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes attrs) {
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(v -> {
                  if (v.contains("gzip")) {
                    gzipSeen.set(true);
                  }
                });
          }
        };
    clientBuilder.overrideConfiguration(
        ClientOverrideConfiguration.builder().addExecutionInterceptor(compressionTracker).build());

    DynamoDbAsyncClient client = clientBuilder.build();
    try {
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName("nonexistent_table_for_compression_test")
                  .item(Map.of(
                      "pk", AttributeValue.builder().s("k").build(),
                      "data", AttributeValue.builder().s(largePayload()).build()))
                  .build())
          .get();
    } catch (Exception e) {
      // Expected — table doesn't exist
    } finally {
      client.close();
    }

    assertTrue("Content-Encoding: gzip should be present for large payloads", gzipSeen.get());
  }

  /**
   * Verifies that headers optimization and compression work together: the required headers are
   * present, and Content-Encoding: gzip is applied for large payloads.
   */
  private void verifyHeadersAndCompression(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder clientBuilder) {
    Set<String> seenHeaders = new HashSet<>();
    AtomicBoolean gzipSeen = new AtomicBoolean(false);
    ExecutionInterceptor tracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes attrs) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(v -> {
                  if (v.contains("gzip")) {
                    gzipSeen.set(true);
                  }
                });
          }
        };
    clientBuilder.overrideConfiguration(
        ClientOverrideConfiguration.builder().addExecutionInterceptor(tracker).build());

    DynamoDbAsyncClient client = clientBuilder.build();
    try {
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName("nonexistent_table_for_combined_test")
                  .item(Map.of(
                      "pk", AttributeValue.builder().s("k").build(),
                      "data", AttributeValue.builder().s(largePayload()).build()))
                  .build())
          .get();
    } catch (Exception e) {
      // Expected
    } finally {
      client.close();
    }

    assertTrue("Content-Encoding should be present", seenHeaders.contains("Content-Encoding"));
    assertTrue("Compression should be applied", gzipSeen.get());
    assertTrue("Host header should be present", seenHeaders.contains("Host"));
    assertTrue("X-Amz-Target header should be present", seenHeaders.contains("X-Amz-Target"));
  }

  private void runAsyncDynamoDbCrudTest(
      AlternatorDynamoDbAsyncClientWrapper wrapper, String tableName) throws Exception {
    DynamoDbAsyncClient client = wrapper.getClient();
    try {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
        Thread.sleep(500);
      } catch (Exception e) {
        // OK
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
                          .readCapacityUnits(1L)
                          .writeCapacityUnits(1L)
                          .build())
                  .build())
          .get();

      client
          .putItem(
              PutItemRequest.builder()
                  .tableName(tableName)
                  .item(Map.of(
                      "pk", AttributeValue.builder().s("key1").build(),
                      "data", AttributeValue.builder().s("value1").build()))
                  .build())
          .get();

      GetItemResponse response =
          client
              .getItem(
                  GetItemRequest.builder()
                      .tableName(tableName)
                      .key(Map.of("pk", AttributeValue.builder().s("key1").build()))
                      .build())
              .get();
      assertNotNull("Should retrieve the item", response.item());
      assertEquals("key1", response.item().get("pk").s());
      assertEquals("value1", response.item().get("data").s());

      client
          .deleteItem(
              DeleteItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("pk", AttributeValue.builder().s("key1").build()))
                  .build())
          .get();

      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
    } finally {
      wrapper.close();
    }
  }

  private static String largePayload() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("This is a test value that should be compressed. ");
    }
    return sb.toString();
  }
}
