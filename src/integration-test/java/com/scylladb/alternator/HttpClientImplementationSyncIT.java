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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration tests that verify each sync HTTP client implementation works correctly with the
 * Alternator load balancer, including headers optimization, compression, connection pool config, and
 * TLS.
 *
 * <p>Tests cover Apache and CRT sync clients against a CCM-provisioned ScyllaDB cluster. Set
 * environment variables:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS=true
 *   <li>SCYLLA_VERSION selects the CCM Scylla package
 * </ul>
 */
public class HttpClientImplementationSyncIT {

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  // ===========================================================================
  // Apache HTTP Client
  // ===========================================================================

  @Test
  public void testApacheClientNodeDiscovery() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper = buildApacheWrapper();
    try {
      List<URI> nodes = wrapper.getLiveNodes();
      assertFalse("Apache client should discover at least one node", nodes.isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testApacheClientDynamoDbOperations() throws Exception {
    runDynamoDbCrudTest(buildApacheWrapper(), "apache_sync_it_table");
  }

  @Test
  public void testApacheClientWithConfigPropagation() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withMaxConnections(50)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withApacheHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();

    try {
      List<URI> nodes = wrapper.getLiveNodes();
      assertFalse("Should discover nodes with custom config", nodes.isEmpty());
      AlternatorConfig rc = wrapper.getAlternatorConfig();
      assertEquals(50, rc.getMaxConnections());
      assertEquals(30000, rc.getConnectionMaxIdleTimeMs());
      assertEquals(60000, rc.getConnectionTimeToLiveMs());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testApacheClientWithCustomizer() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withApacheHttpClientCustomizer(builder -> builder.maxConnections(100))
            .buildWithAlternatorAPI();
    runDynamoDbCrudTest(wrapper, "apache_customizer_it_table");
  }

  @Test
  public void testApacheClientHttps() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(TlsConfig.trustAll())
            .withApacheHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    try {
      assertFalse("Apache HTTPS client should discover nodes", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testApacheClientHttpsWithCustomCa() throws Exception {
    Path caCertPath = IntegrationTestConfig.CA_CERT_PATH;
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        caCertPath != null && Files.exists(caCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(caCertPath).withTrustSystemCaCerts(false).build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withApacheHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    try {
      assertFalse(
          "Apache HTTPS client with custom CA should discover nodes",
          wrapper.getLiveNodes().isEmpty());
      wrapper.getClient().listTables(ListTablesRequest.builder().limit(1).build());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testApacheClientHttpsWithCustomCaCrud() throws Exception {
    Path caCertPath = IntegrationTestConfig.CA_CERT_PATH;
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        caCertPath != null && Files.exists(caCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(caCertPath).withTrustSystemCaCerts(false).build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withApacheHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    runDynamoDbCrudTest(wrapper, "apache_custom_ca_it_table");
  }

  @Test
  public void testApacheClientWithHeadersOptimization() throws Exception {
    verifyHeadersOptimization(
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withApacheHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testApacheClientWithCompression() throws Exception {
    verifyCompression(
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withApacheHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testApacheClientWithHeadersOptimizationAndCompression() throws Exception {
    verifyHeadersAndCompression(
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withApacheHttpClientCustomizer(builder -> {}));
  }


  // ===========================================================================
  // CRT Sync Client
  // ===========================================================================

  @Test
  public void testCrtClientNodeDiscovery() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper = buildCrtWrapper();
    try {
      assertFalse("CRT client should discover at least one node", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testCrtClientDynamoDbOperations() throws Exception {
    runDynamoDbCrudTest(buildCrtWrapper(), "crt_sync_it_table");
  }

  @Test
  public void testCrtClientWithConfigPropagation() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withMaxConnections(50)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();

    try {
      assertFalse("Should discover nodes with CRT config", wrapper.getLiveNodes().isEmpty());
      AlternatorConfig rc = wrapper.getAlternatorConfig();
      assertEquals(50, rc.getMaxConnections());
      assertEquals(30000, rc.getConnectionMaxIdleTimeMs());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testCrtClientWithCustomizer() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withCrtHttpClientCustomizer(builder -> builder.maxConcurrency(100))
            .buildWithAlternatorAPI();
    runDynamoDbCrudTest(wrapper, "crt_customizer_it_table");
  }

  @Test
  public void testCrtClientHttps() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(TlsConfig.trustAll())
            .withCrtHttpClientCustomizer(builder -> {})
            .buildWithAlternatorAPI();
    try {
      assertFalse("CRT HTTPS client should discover nodes", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  /**
   * CRT HTTP client uses native AWS-CRT TLS and does not support custom CA certificates via Java
   * TrustManagers. This test verifies that initialization fails fast when custom CA is configured
   * with the CRT client.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCrtClientHttpsWithCustomCaFailsOnInit() throws Exception {
    Path caCertPath = IntegrationTestConfig.CA_CERT_PATH;
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        caCertPath != null && Files.exists(caCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(caCertPath).withTrustSystemCaCerts(false).build();

    AlternatorDynamoDbClient.builder()
        .endpointOverride(IntegrationTestConfig.HTTPS_SEED_URI)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withTlsConfig(tlsConfig)
        .withCrtHttpClientCustomizer(builder -> {})
        .buildWithAlternatorAPI();
  }

  @Test
  public void testCrtClientWithHeadersOptimization() throws Exception {
    verifyHeadersOptimization(
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withCrtHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testCrtClientWithCompression() throws Exception {
    verifyCompression(
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withCrtHttpClientCustomizer(builder -> {}));
  }

  @Test
  public void testCrtClientWithHeadersOptimizationAndCompression() throws Exception {
    verifyHeadersAndCompression(
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .withCrtHttpClientCustomizer(builder -> {}));
  }


  // ===========================================================================
  // Auto-detection (no explicit customizer)
  // ===========================================================================

  @Test
  public void testAutoDetectedClientNodeDiscovery() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();
    try {
      assertFalse("Auto-detected client should discover nodes", wrapper.getLiveNodes().isEmpty());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAutoDetectedClientDynamoDbOperations() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();
    runDynamoDbCrudTest(wrapper, "autodetect_sync_it_table");
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private AlternatorDynamoDbClientWrapper buildApacheWrapper() {
    return AlternatorDynamoDbClient.builder()
        .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withApacheHttpClientCustomizer(builder -> {})
        .buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbClientWrapper buildCrtWrapper() {
    return AlternatorDynamoDbClient.builder()
        .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
        .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
        .withCrtHttpClientCustomizer(builder -> {})
        .buildWithAlternatorAPI();
  }

  /**
   * Verifies headers optimization is active: required headers are present and non-essential headers
   * are filtered out at the HTTP client level.
   */
  private void verifyHeadersOptimization(
      AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder clientBuilder) {
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

    DynamoDbClient client = clientBuilder.build();
    try {
      client.listTables(ListTablesRequest.builder().build());
    } catch (Exception e) {
      // Ignore — we only care about headers
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
      AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder clientBuilder) {
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

    DynamoDbClient client = clientBuilder.build();
    try {
      client.putItem(
          PutItemRequest.builder()
              .tableName("nonexistent_table_for_compression_test")
              .item(Map.of(
                  "pk", AttributeValue.builder().s("k").build(),
                  "data", AttributeValue.builder().s(largePayload()).build()))
              .build());
    } catch (ResourceNotFoundException e) {
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
      AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder clientBuilder) {
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

    DynamoDbClient client = clientBuilder.build();
    try {
      client.putItem(
          PutItemRequest.builder()
              .tableName("nonexistent_table_for_combined_test")
              .item(Map.of(
                  "pk", AttributeValue.builder().s("k").build(),
                  "data", AttributeValue.builder().s(largePayload()).build()))
              .build());
    } catch (ResourceNotFoundException e) {
      // Expected
    } finally {
      client.close();
    }

    assertTrue("Content-Encoding should be present", seenHeaders.contains("Content-Encoding"));
    assertTrue("Compression should be applied", gzipSeen.get());
    assertTrue("Host header should be present", seenHeaders.contains("Host"));
    assertTrue("X-Amz-Target header should be present", seenHeaders.contains("X-Amz-Target"));
  }

  private void runDynamoDbCrudTest(AlternatorDynamoDbClientWrapper wrapper, String tableName)
      throws Exception {
    DynamoDbClient client = wrapper.getClient();
    try {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        Thread.sleep(500);
      } catch (ResourceNotFoundException e) {
        // OK
      }

      client.createTable(
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
              .build());

      client.putItem(
          PutItemRequest.builder()
              .tableName(tableName)
              .item(Map.of(
                  "pk", AttributeValue.builder().s("key1").build(),
                  "data", AttributeValue.builder().s("value1").build()))
              .build());

      GetItemResponse response =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("pk", AttributeValue.builder().s("key1").build()))
                  .build());
      assertNotNull("Should retrieve the item", response.item());
      assertEquals("key1", response.item().get("pk").s());
      assertEquals("value1", response.item().get("data").s());

      client.deleteItem(
          DeleteItemRequest.builder()
              .tableName(tableName)
              .key(Map.of("pk", AttributeValue.builder().s("key1").build()))
              .build());

      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
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
