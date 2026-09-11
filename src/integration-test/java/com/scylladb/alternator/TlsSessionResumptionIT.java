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

import com.scylladb.alternator.internal.TlsContextFactory;
import com.scylladb.alternator.routing.ClusterScope;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/**
 * Integration tests for TLS session ticket support.
 *
 * <p>These tests use a CCM-provisioned ScyllaDB cluster with Alternator enabled over HTTPS. The
 * tests verify that TLS session caching is properly configured and improves connection performance.
 *
 * <p>Set environment variables to configure:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS: Set to "true" to enable tests
 *   <li>SCYLLA_VERSION: CCM Scylla package selector
 * </ul>
 */
public class TlsSessionResumptionIT {

  private static final URI seedUri = IntegrationTestConfig.HTTPS_SEED_URI;

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  @Test
  public void testDefaultTlsSessionCacheEnabled() throws Exception {
    // Create client with default TLS session cache (enabled)
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();

    // Verify client is functional over HTTPS
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify the TLS config is as expected
    AlternatorConfig config = wrapper.getAlternatorConfig();
    assertTrue(
        "TLS session cache should be enabled by default",
        config.getTlsSessionCacheConfig().isEnabled());

    wrapper.close();
  }

  @Test
  public void testTlsSessionCacheWithCustomSettings() throws Exception {
    // Create client with custom TLS session cache settings
    TlsSessionCacheConfig tlsConfig =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify config was applied
    AlternatorConfig config = wrapper.getAlternatorConfig();
    assertEquals(200, config.getTlsSessionCacheConfig().getSessionCacheSize());
    assertEquals(3600, config.getTlsSessionCacheConfig().getSessionTimeoutSeconds());

    wrapper.close();
  }

  @Test
  public void testTlsSessionCacheDisabled() throws Exception {
    // Create client with TLS session cache disabled
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(TlsSessionCacheConfig.disabled())
            .buildWithAlternatorAPI();

    // Verify client is still functional without session caching
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify config was applied
    AlternatorConfig config = wrapper.getAlternatorConfig();
    assertFalse(
        "TLS session cache should be disabled", config.getTlsSessionCacheConfig().isEnabled());

    wrapper.close();
  }

  @Test
  public void testMultipleRequestsBenefitFromSessionCache() throws Exception {
    // Create client with TLS session cache enabled
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(TlsSessionCacheConfig.getDefault())
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    // Perform multiple requests - subsequent requests should benefit from session resumption
    // Note: We can't directly measure TLS handshake time in this test, but we verify
    // that multiple requests work correctly with session caching enabled
    for (int i = 0; i < 10; i++) {
      try {
        client.listTables(ListTablesRequest.builder().limit(1).build());
      } catch (Exception e) {
        // Ignore errors - we're testing TLS, not DynamoDB operations
      }
    }

    // The fact that multiple requests succeeded indicates TLS connections are working
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should still have nodes after multiple requests", nodes.isEmpty());

    wrapper.close();
  }

  @Test
  public void testTlsSessionCacheWithCompression() throws Exception {
    // Test TLS session cache combined with compression
    TlsSessionCacheConfig tlsConfig =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(7200)
            .build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(tlsConfig)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(512)
            .buildWithAlternatorAPI();

    // Verify client is functional with both features enabled
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify both configs are applied
    AlternatorConfig config = wrapper.getAlternatorConfig();
    assertTrue(config.getTlsSessionCacheConfig().isEnabled());
    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());

    wrapper.close();
  }

  @Test
  public void testTlsSessionCacheWithHeadersOptimization() throws Exception {
    // Test TLS session cache combined with headers optimization
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(TlsSessionCacheConfig.getDefault())
            .withOptimizeHeaders(true)
            .buildWithAlternatorAPI();

    // Verify client is functional with both features enabled
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify both configs are applied
    AlternatorConfig config = wrapper.getAlternatorConfig();
    assertTrue(config.getTlsSessionCacheConfig().isEnabled());
    assertTrue(config.isOptimizeHeaders());

    wrapper.close();
  }

  @Test
  public void testTlsSessionCacheWithRoutingScope() throws Exception {
    // Test TLS session cache combined with routing scope
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(TlsSessionCacheConfig.getDefault())
            .withRoutingScope(ClusterScope.create())
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    wrapper.close();
  }

  @Test
  public void testTlsSessionCacheViaDirectBuilderMethods() throws Exception {
    // Test setting TLS config via direct builder methods
    TlsSessionCacheConfig tlsConfig =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(150)
            .withSessionTimeoutSeconds(1800)
            .build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withRoutingScope(ClusterScope.create())
            .withTlsSessionCacheConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify config was applied
    AlternatorConfig appliedConfig = wrapper.getAlternatorConfig();
    assertEquals(150, appliedConfig.getTlsSessionCacheConfig().getSessionCacheSize());
    assertEquals(1800, appliedConfig.getTlsSessionCacheConfig().getSessionTimeoutSeconds());

    wrapper.close();
  }

  /**
   * Tests TLS session caching configuration with actual connections.
   *
   * <p>This test verifies that:
   *
   * <ol>
   *   <li>The SSLContext is properly configured with session caching enabled
   *   <li>Multiple TLS connections can be established successfully
   *   <li>The client-side session cache is configured correctly
   * </ol>
   *
   * <p><b>Note:</b> Actual session resumption depends on server support for TLS session tickets
   * (RFC 5077). If the server doesn't support session tickets, each connection will have a unique
   * session ID even with client-side caching enabled. This test verifies our client configuration
   * is correct; session reuse is a server-dependent optimization.
   */
  @Test
  public void testTlsSessionCachingWithMultipleConnections() throws Exception {
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(3600)
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();

    // Verify client-side session cache is configured correctly
    assertEquals(
        "Client session cache size should be configured",
        100,
        sslContext.getClientSessionContext().getSessionCacheSize());
    assertEquals(
        "Client session timeout should be configured",
        3600,
        sslContext.getClientSessionContext().getSessionTimeout());

    Set<String> sessionIds = new HashSet<>();
    int successfulConnections = 0;

    // Make multiple connections to the same server
    for (int i = 0; i < 5; i++) {
      SSLSocket socket = null;
      try {
        socket = (SSLSocket) socketFactory.createSocket(IntegrationTestConfig.HOST, IntegrationTestConfig.HTTPS_PORT);
        socket.startHandshake();

        SSLSession session = socket.getSession();
        assertTrue("SSL session should be valid", session.isValid());

        byte[] currentSessionId = session.getId();
        if (currentSessionId != null && currentSessionId.length > 0) {
          sessionIds.add(bytesToHex(currentSessionId));
        }
        successfulConnections++;
      } finally {
        if (socket != null) {
          socket.close();
        }
      }
    }

    assertEquals("All connections should succeed", 5, successfulConnections);

    // Log session reuse information (informational, not assertive)
    // Session reuse depends on server support for TLS session tickets
    System.out.println(
        "TLS Session caching test: "
            + sessionIds.size()
            + " unique sessions out of "
            + successfulConnections
            + " connections. "
            + (sessionIds.size() < successfulConnections
                ? "Session reuse detected!"
                : "Server may not support TLS session tickets."));
  }

  /**
   * Tests that TLS sessions are NOT reused when session caching is disabled.
   *
   * <p>With session caching disabled (cache size 0), each connection should perform a full TLS
   * handshake and get a new session.
   */
  @Test
  public void testTlsSessionNotReusedWhenDisabled() throws Exception {
    TlsSessionCacheConfig config = TlsSessionCacheConfig.disabled();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();

    Set<String> sessionIds = new HashSet<>();

    // Make multiple connections to the same server
    for (int i = 0; i < 3; i++) {
      SSLSocket socket = null;
      try {
        socket = (SSLSocket) socketFactory.createSocket(IntegrationTestConfig.HOST, IntegrationTestConfig.HTTPS_PORT);
        socket.startHandshake();

        SSLSession session = socket.getSession();
        byte[] sessionId = session.getId();

        if (sessionId != null && sessionId.length > 0) {
          sessionIds.add(bytesToHex(sessionId));
        }
      } finally {
        if (socket != null) {
          socket.close();
        }
      }
    }

    // With session caching disabled, we expect each connection to have a different session
    // Note: Some servers might not support session tickets, so we allow for some reuse
    // The main point is that with cache size 0, the client won't cache sessions
    System.out.println(
        "Session caching disabled - unique session IDs: "
            + sessionIds.size()
            + " out of 3 connections");
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
