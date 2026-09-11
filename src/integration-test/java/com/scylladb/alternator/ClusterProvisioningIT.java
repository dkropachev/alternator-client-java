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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.scylladb.alternator.internal.TlsContextFactory;
import com.scylladb.alternator.testinfra.AlternatorConnection;
import com.scylladb.alternator.testinfra.AlternatorTransport;
import com.scylladb.alternator.testinfra.ClusterSecuritySpec;
import com.scylladb.alternator.testinfra.ClusterSpecs;
import com.scylladb.alternator.testinfra.ClusterTopology;
import com.scylladb.alternator.testinfra.PrivateClusterLease;
import com.scylladb.alternator.testinfra.ReusableClusterLease;
import com.scylladb.alternator.testinfra.TestClusterNode;
import com.scylladb.alternator.testinfra.TestClusters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HttpsURLConnection;
import org.junit.Before;
import org.junit.Test;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.schema.CoreSchema;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Real-cluster contract tests for the CCM test infrastructure. */
public class ClusterProvisioningIT {
  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        Boolean.parseBoolean(System.getenv().getOrDefault("INTEGRATION_TESTS", "false")));
  }

  @Test
  @CoversRequirements("CCM-REQ-003")
  public void sameSpecReusesClusterWithIndependentResourceScopes() throws Exception {
    try (ReusableClusterLease first = TestClusters.acquireReusable(ClusterSpecs.defaultSpec());
        ReusableClusterLease second = TestClusters.acquireReusable(ClusterSpecs.defaultSpec())) {
      assertEquals(first.cluster().instanceId(), second.cluster().instanceId());
      String firstTable = first.resources().newTableName("first");
      String secondTable = second.resources().newTableName("second");
      try (AlternatorDynamoDbClientWrapper wrapper =
          second.cluster().clientBuilder(AlternatorTransport.HTTP).buildWithAlternatorAPI()) {
        DynamoDbClient client = wrapper.getClient();
        createTable(client, firstTable);
        createTable(client, secondTable);
        assertEquals(firstTable, describeTable(client, firstTable));
        assertEquals(secondTable, describeTable(client, secondTable));

        first.close();

        assertThrows(
            ResourceNotFoundException.class,
            () ->
                client.describeTable(DescribeTableRequest.builder().tableName(firstTable).build()));
        assertEquals(secondTable, describeTable(client, secondTable));
      }
      second.close();
    }
  }

  @Test
  @CoversRequirements("CCM-REQ-002")
  public void authorizedClusterProvidesWorkingCredentials() throws Exception {
    try (ReusableClusterLease lease =
        TestClusters.acquireReusable(
            ClusterSpecs.defaultSpec()
                .withTopology(ClusterTopology.singleDatacenter(1))
                .withTransports(AlternatorTransport.HTTP)
                .withSecurity(ClusterSecuritySpec.ENFORCED))) {
      AlternatorConnection connection = lease.cluster().connection(AlternatorTransport.HTTP);
      try (AlternatorDynamoDbClientWrapper unauthorizedWrapper =
              AlternatorDynamoDbClient.builder()
                  .endpointOverride(connection.seedEndpoint())
                  .credentialsProvider(
                      StaticCredentialsProvider.create(
                          AwsBasicCredentials.create("wrong-user", "wrong-password")))
                  .buildWithAlternatorAPI();
          AlternatorDynamoDbClientWrapper authorizedWrapper =
              lease.cluster().clientBuilder(AlternatorTransport.HTTP).buildWithAlternatorAPI()) {
        DynamoDbClient unauthorized = unauthorizedWrapper.getClient();
        DynamoDbClient authorized = authorizedWrapper.getClient();
        boolean rejected = false;
        try {
          unauthorized.listTables();
        } catch (DynamoDbException expected) {
          rejected = true;
        }
        assertTrue("Wrong credentials must not be accepted", rejected);
        assertNotNull(authorized.listTables().tableNames());
      }
    }
  }

  @Test
  @CoversRequirements("CCM-REQ-004")
  public void privateHttpsClusterCanChangeNodeLifecycleAndTopology() throws Exception {
    try (PrivateClusterLease lease =
        TestClusters.provisionPrivate(
            ClusterSpecs.defaultSpec()
                .withTopology(ClusterTopology.singleDatacenter(1))
                .withTransports(AlternatorTransport.HTTPS))) {
      TestClusterNode original = lease.cluster().nodes().get(0);
      TestClusterNode added = lease.control().addNode("dc1", "RAC1");
      assertEquals(2, lease.cluster().nodes().size());
      AlternatorConnection https = lease.cluster().connection(AlternatorTransport.HTTPS);
      URI addedEndpoint = URI.create("https://" + added.address() + ":8043");
      assertHttpsEndpointWorks(addedEndpoint, https.caCertificatePath());

      lease.control().stop();
      awaitEndpointClosed(https.seedEndpoint());
      awaitEndpointClosed(addedEndpoint);
      lease.control().start();
      assertHttpsEndpointWorks(https.seedEndpoint(), https.caCertificatePath());
      assertHttpsEndpointWorks(addedEndpoint, https.caCertificatePath());

      lease.control().stopNode(added);
      awaitEndpointClosed(addedEndpoint);
      lease.control().startNode(added);
      assertHttpsEndpointWorks(addedEndpoint, https.caCertificatePath());
      lease.control().removeNode(original);
      assertEquals(1, lease.cluster().nodes().size());

      TestClusterNode replacement = lease.control().addNode("dc1", "RAC1");
      assertEquals(original.address(), replacement.address());
      assertHttpsEndpointWorks(
          URI.create("https://" + replacement.address() + ":8043"), https.caCertificatePath());
      lease.control().removeNode(replacement);
    }
  }

  private static void createTable(DynamoDbClient client, String tableName) {
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
                    .readCapacityUnits(1L)
                    .writeCapacityUnits(1L)
                    .build())
            .build());
  }

  private static String describeTable(DynamoDbClient client, String tableName) {
    return client
        .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
        .table()
        .tableName();
  }

  private static void assertHttpsEndpointWorks(URI endpoint, Path caCertificate) throws Exception {
    HttpsURLConnection connection =
        (HttpsURLConnection) endpoint.toURL().openConnection(Proxy.NO_PROXY);
    connection.setSSLSocketFactory(
        TlsContextFactory.createSslContext(
                TlsConfig.builder()
                    .withCaCertPath(caCertificate)
                    .withTrustSystemCaCerts(false)
                    .build())
            .getSocketFactory());
    connection.setConnectTimeout(1000);
    connection.setReadTimeout(1000);
    try {
      assertTrue("Added node HTTPS endpoint failed", connection.getResponseCode() / 100 == 2);
    } finally {
      connection.disconnect();
    }
  }

  private static void awaitEndpointClosed(URI endpoint) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), 500);
      } catch (IOException closed) {
        return;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("HTTPS endpoint remained reachable after stop: " + endpoint);
  }

  @Test
  public void yamlOverridesSurviveClusterAndNodeConfigurationUpdates() throws Exception {
    String instanceId;
    try (PrivateClusterLease lease =
        TestClusters.provisionPrivate(
            ClusterSpecs.defaultSpec()
                .withTopology(ClusterTopology.singleDatacenter(1))
                .withTransports(AlternatorTransport.HTTPS)
                .withYamlOverride("hinted_handoff_enabled", "false")
                .withYamlOverride("commitlog_sync", "batch")
                .withYamlOverride("commitlog_sync_batch_window_in_ms", "17")
                .withYamlOverride("commitlog_sync_period_in_ms", "null")
                .withYamlOverride("experimental_features", "null"))) {
      instanceId = lease.cluster().instanceId();
      lease.control().addNode("dc1", "RAC1");
      assertEquals(2, lease.cluster().nodes().size());
    }

    Path diagnosticsRoot = diagnosticsRoot();
    for (String node : new String[] {"node1", "node2"}) {
      Path config =
          diagnosticsRoot
              .resolve(instanceId)
              .resolve(instanceId)
              .resolve(node)
              .resolve("conf/scylla.yaml");
      Map<String, Object> yaml = readYaml(config);
      assertEquals(Boolean.FALSE, yaml.get("hinted_handoff_enabled"));
      assertEquals("batch", yaml.get("commitlog_sync"));
      assertEquals(17, yaml.get("commitlog_sync_batch_window_in_ms"));
      assertTrue(yaml.containsKey("commitlog_sync_period_in_ms"));
      assertEquals(null, yaml.get("commitlog_sync_period_in_ms"));
      assertTrue(yaml.containsKey("experimental_features"));
      assertEquals(null, yaml.get("experimental_features"));
    }
  }

  private static Path diagnosticsRoot() {
    String configured = System.getenv("SCYLLA_CCM_DIAGNOSTICS_DIR");
    if (configured != null && !configured.trim().isEmpty()) {
      return Path.of(configured);
    }
    return Path.of("target", "ccm").toAbsolutePath();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readYaml(Path path) throws Exception {
    LoadSettings options =
        LoadSettings.builder().setAllowDuplicateKeys(false).setSchema(new CoreSchema()).build();
    Object parsed =
        new Load(options).loadFromString(Files.readString(path, StandardCharsets.UTF_8));
    assertTrue("Expected YAML mapping in " + path, parsed instanceof Map);
    return (Map<String, Object>) parsed;
  }
}
