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
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
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
 * Integration tests for CRUD operations through the load-balanced client.
 *
 * <p>These tests verify that standard DynamoDB operations (CreateTable, PutItem, GetItem,
 * DeleteItem, DeleteTable) work correctly when requests are distributed across multiple Alternator
 * nodes via the load balancer.
 *
 * <p>Tests run against both HTTP and HTTPS endpoints. Set environment variables to configure:
 *
 * <ul>
 *   <li>ALTERNATOR_HOST: Host address (default: 172.39.0.2)
 *   <li>ALTERNATOR_PORT: HTTP port number (default: 9998)
 *   <li>ALTERNATOR_HTTPS_PORT: HTTPS port number (default: 9999)
 *   <li>INTEGRATION_TESTS: Set to "true" to enable these tests
 * </ul>
 */
@RunWith(Parameterized.class)
public class DynamoDbOperationsIT {

  private final URI seedUri;

  public DynamoDbOperationsIT(String scheme, URI seedUri) {
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

  private static String uniqueTableName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  private static CreateTableRequest createTableRequest(String tableName) {
    return CreateTableRequest.builder()
        .tableName(tableName)
        .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("pk")
                .attributeType(ScalarAttributeType.S)
                .build())
        .provisionedThroughput(
            ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
        .build();
  }

  private static void safeDeleteTable(DynamoDbClient client, String tableName) {
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (ResourceNotFoundException e) {
      // Table does not exist, nothing to clean up
    }
  }

  private static void safeDeleteTableAsync(DynamoDbAsyncClient client, String tableName) {
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
    } catch (Exception e) {
      // Table does not exist or other error, nothing to clean up
    }
  }

  @Test
  public void testSyncCrudOperations() throws Exception {
    String tableName = uniqueTableName("sync_crud_it");

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    try {
      // Clean up if table exists from a previous failed run
      safeDeleteTable(client, tableName);
      Thread.sleep(500);

      // Create table
      client.createTable(createTableRequest(tableName));

      // Put item
      Map<String, AttributeValue> item =
          Map.of(
              "pk", AttributeValue.builder().s("item-1").build(),
              "data", AttributeValue.builder().s("hello-world").build(),
              "count", AttributeValue.builder().n("42").build());
      client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());

      // Get item and verify data
      GetItemResponse getResponse =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("pk", AttributeValue.builder().s("item-1").build()))
                  .build());

      assertNotNull("GetItem response should not be null", getResponse.item());
      assertFalse("GetItem response should contain the item", getResponse.item().isEmpty());
      assertEquals("item-1", getResponse.item().get("pk").s());
      assertEquals("hello-world", getResponse.item().get("data").s());
      assertEquals("42", getResponse.item().get("count").n());

      // Delete item
      client.deleteItem(
          DeleteItemRequest.builder()
              .tableName(tableName)
              .key(Map.of("pk", AttributeValue.builder().s("item-1").build()))
              .build());

      // Verify item is deleted
      GetItemResponse deletedResponse =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("pk", AttributeValue.builder().s("item-1").build()))
                  .build());

      assertTrue(
          "Item should be deleted",
          deletedResponse.item() == null || deletedResponse.item().isEmpty());

      // Delete table
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (Exception e) {
      // Attempt cleanup on failure
      safeDeleteTable(client, tableName);
      throw e;
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAsyncCrudOperations() throws Exception {
    String tableName = uniqueTableName("async_crud_it");

    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();

    DynamoDbAsyncClient client = wrapper.getClient();

    try {
      // Clean up if table exists from a previous failed run
      safeDeleteTableAsync(client, tableName);
      Thread.sleep(500);

      // Create table
      client.createTable(createTableRequest(tableName)).get(10, TimeUnit.SECONDS);

      // Put item
      Map<String, AttributeValue> item =
          Map.of(
              "pk", AttributeValue.builder().s("async-item-1").build(),
              "data", AttributeValue.builder().s("async-hello").build(),
              "count", AttributeValue.builder().n("99").build());
      client
          .putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
          .get(10, TimeUnit.SECONDS);

      // Get item and verify data
      GetItemResponse getResponse =
          client
              .getItem(
                  GetItemRequest.builder()
                      .tableName(tableName)
                      .key(Map.of("pk", AttributeValue.builder().s("async-item-1").build()))
                      .build())
              .get(10, TimeUnit.SECONDS);

      assertNotNull("GetItem response should not be null", getResponse.item());
      assertFalse("GetItem response should contain the item", getResponse.item().isEmpty());
      assertEquals("async-item-1", getResponse.item().get("pk").s());
      assertEquals("async-hello", getResponse.item().get("data").s());
      assertEquals("99", getResponse.item().get("count").n());

      // Delete item
      client
          .deleteItem(
              DeleteItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("pk", AttributeValue.builder().s("async-item-1").build()))
                  .build())
          .get(10, TimeUnit.SECONDS);

      // Verify item is deleted
      GetItemResponse deletedResponse =
          client
              .getItem(
                  GetItemRequest.builder()
                      .tableName(tableName)
                      .key(Map.of("pk", AttributeValue.builder().s("async-item-1").build()))
                      .build())
              .get(10, TimeUnit.SECONDS);

      assertTrue(
          "Item should be deleted",
          deletedResponse.item() == null || deletedResponse.item().isEmpty());

      // Delete table
      client
          .deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
          .get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      // Attempt cleanup on failure
      safeDeleteTableAsync(client, tableName);
      throw e;
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testOperationsAcrossMultipleNodes() throws Exception {
    String tableName = uniqueTableName("multinode_it");

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    try {
      // Clean up if table exists from a previous failed run
      safeDeleteTable(client, tableName);
      Thread.sleep(500);

      // Create table
      client.createTable(createTableRequest(tableName));

      // Write many items - requests will be distributed across nodes via random query-plan routing
      int itemCount = 30;
      for (int i = 0; i < itemCount; i++) {
        Map<String, AttributeValue> item =
            Map.of(
                "pk", AttributeValue.builder().s("key-" + i).build(),
                "value", AttributeValue.builder().s("value-" + i).build(),
                "index", AttributeValue.builder().n(String.valueOf(i)).build());
        client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
      }

      // Read back all items and verify data integrity
      // Since requests hit different nodes, this validates that data is consistent across the
      // cluster
      int verifiedCount = 0;
      for (int i = 0; i < itemCount; i++) {
        GetItemResponse response =
            client.getItem(
                GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("pk", AttributeValue.builder().s("key-" + i).build()))
                    .build());

        assertNotNull("Item key-" + i + " should exist", response.item());
        assertFalse("Item key-" + i + " should not be empty", response.item().isEmpty());
        assertEquals("key-" + i, response.item().get("pk").s());
        assertEquals("value-" + i, response.item().get("value").s());
        assertEquals(String.valueOf(i), response.item().get("index").n());
        verifiedCount++;
      }

      assertEquals("All items should be readable across nodes", itemCount, verifiedCount);

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (Exception e) {
      // Attempt cleanup on failure
      safeDeleteTable(client, tableName);
      throw e;
    } finally {
      wrapper.close();
    }
  }
}
