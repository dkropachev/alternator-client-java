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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/**
 * Integration tests for HTTP connection reuse and pooling through the load-balanced client.
 *
 * <p>These tests verify that rapid sequential and parallel requests succeed without connection
 * errors, which implicitly validates that HTTP connection reuse and pooling work correctly.
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
public class HttpConnectionReuseIT {

  private final URI seedUri;

  public HttpConnectionReuseIT(String scheme, URI seedUri) {
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

  private AlternatorDynamoDbClientWrapper buildSyncWrapper() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

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

  @Test
  public void testSerialRequestsSucceed() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper = buildSyncWrapper();
    DynamoDbClient client = wrapper.getClient();

    try {
      int requestCount = 20;
      int successCount = 0;

      for (int i = 0; i < requestCount; i++) {
        ListTablesResponse response = client.listTables(ListTablesRequest.builder().build());
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All serial requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testParallelRequestsSucceed() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper = buildSyncWrapper();
    DynamoDbClient client = wrapper.getClient();
    ExecutorService executor = Executors.newFixedThreadPool(10);

    try {
      int requestCount = 50;
      List<Future<ListTablesResponse>> futures = new ArrayList<>();

      for (int i = 0; i < requestCount; i++) {
        futures.add(executor.submit(() -> client.listTables(ListTablesRequest.builder().build())));
      }

      int successCount = 0;
      for (int i = 0; i < futures.size(); i++) {
        ListTablesResponse response = futures.get(i).get(30, TimeUnit.SECONDS);
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All parallel requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
      wrapper.close();
    }
  }

  @Test
  public void testAsyncSerialRequestsSucceed() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper = buildAsyncWrapper();
    DynamoDbAsyncClient client = wrapper.getClient();

    try {
      int requestCount = 20;
      int successCount = 0;

      for (int i = 0; i < requestCount; i++) {
        ListTablesResponse response =
            client.listTables(ListTablesRequest.builder().build()).get(10, TimeUnit.SECONDS);
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All serial async requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAsyncParallelRequestsSucceed() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper = buildAsyncWrapper();
    DynamoDbAsyncClient client = wrapper.getClient();

    try {
      int requestCount = 50;
      @SuppressWarnings("unchecked")
      CompletableFuture<ListTablesResponse>[] futures = new CompletableFuture[requestCount];

      for (int i = 0; i < requestCount; i++) {
        futures[i] = client.listTables(ListTablesRequest.builder().build());
      }

      CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

      int successCount = 0;
      for (int i = 0; i < requestCount; i++) {
        ListTablesResponse response = futures[i].get();
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All parallel async requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      wrapper.close();
    }
  }
}
