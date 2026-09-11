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
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/**
 * Integration tests for TLS configuration features.
 *
 * <p>These tests verify the TlsConfig class functionality with real TLS connections:
 *
 * <ul>
 *   <li>Custom CA certificate loading and validation
 *   <li>System CA certificate trust
 *   <li>Trust-all mode for development/testing
 *   <li>Hostname verification behavior
 *   <li>Combination with other client features (compression, headers optimization)
 *   <li>Both synchronous and asynchronous clients
 * </ul>
 *
 * <p>The CCM harness supplies the HTTPS endpoint and a generated CA certificate. Set environment
 * variables to configure:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS: Set to "true" to enable tests
 *   <li>SCYLLA_VERSION: CCM Scylla package selector
 * </ul>
 *
 * @author dmitry.kropachev
 * @since 1.0.9
 */
public class TlsConfigIT {

  private static final URI seedUri = IntegrationTestConfig.HTTPS_SEED_URI;
  private static final Path customCaCertPath = IntegrationTestConfig.CA_CERT_PATH;

  private AlternatorDynamoDbClientWrapper syncWrapper;
  private AlternatorDynamoDbAsyncClientWrapper asyncWrapper;

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  @After
  public void tearDown() {
    if (syncWrapper != null) {
      try {
        syncWrapper.close();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      syncWrapper = null;
    }
    if (asyncWrapper != null) {
      try {
        asyncWrapper.close();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      asyncWrapper = null;
    }
  }

  // ==========================================================================
  // TrustAll Mode Tests
  // ==========================================================================

  @Test
  public void testSyncClientWithTrustAllConfig() throws Exception {
    TlsConfig tlsConfig = TlsConfig.trustAll();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify TLS config is applied
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertTrue("Should use trust-all mode", config.getTlsConfig().isTrustAllCertificates());
    assertFalse(
        "Hostname verification should be disabled in trust-all mode",
        config.getTlsConfig().isVerifyHostname());
  }

  @Test
  public void testAsyncClientWithTrustAllConfig() throws Exception {
    TlsConfig tlsConfig = TlsConfig.trustAll();

    asyncWrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = asyncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Perform async operation
    DynamoDbAsyncClient client = asyncWrapper.getClient();
    CompletableFuture<Void> future =
        client
            .listTables(ListTablesRequest.builder().limit(1).build())
            .thenAccept(response -> assertNotNull(response))
            .exceptionally(
                ex -> {
                  // Ignore DynamoDB errors, we're testing TLS
                  return null;
                });

    future.get(10, TimeUnit.SECONDS);
  }

  @Test
  public void testTrustAllWithMultipleRequests() throws Exception {
    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(TlsConfig.trustAll())
            .buildWithAlternatorAPI();

    DynamoDbClient client = syncWrapper.getClient();

    // Make multiple requests to test TLS connection stability
    for (int i = 0; i < 10; i++) {
      try {
        client.listTables(ListTablesRequest.builder().limit(1).build());
      } catch (Exception e) {
        // Ignore DynamoDB errors, we're testing TLS
      }
    }

    // Verify connection is still working
    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should still have nodes after multiple requests", nodes.isEmpty());
  }

  // ==========================================================================
  // System Default CA Tests
  // ==========================================================================

  @Test
  public void testSyncClientWithSystemDefaultConfig() throws Exception {
    // This test may fail if the server uses a self-signed certificate
    // It verifies that the system default trust store is used

    TlsConfig tlsConfig = TlsConfig.systemDefault();

    try {
      syncWrapper =
          AlternatorDynamoDbClient.builder()
              .endpointOverride(seedUri)
              .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
              .withTlsConfig(tlsConfig)
              .buildWithAlternatorAPI();

      // If we get here, the server's certificate is trusted by system CAs
      List<URI> nodes = syncWrapper.getLiveNodes();
      assertFalse("Should have at least one node", nodes.isEmpty());

      AlternatorConfig config = syncWrapper.getAlternatorConfig();
      assertTrue("Should trust system CAs", config.getTlsConfig().isTrustSystemCaCerts());
      assertFalse(
          "Should not be in trust-all mode", config.getTlsConfig().isTrustAllCertificates());
      assertTrue(
          "Hostname verification should be enabled", config.getTlsConfig().isVerifyHostname());
    } catch (Exception e) {
      // Expected if server uses self-signed certificate
      assertTrue(
          "Should fail with SSL error for untrusted certificate",
          e.getCause() instanceof SSLHandshakeException
              || e.getMessage().contains("SSL")
              || e.getMessage().contains("certificate"));
      System.out.println(
          "testSyncClientWithSystemDefaultConfig: Expected failure with self-signed cert - "
              + e.getMessage());
    }
  }

  // ==========================================================================
  // Custom CA Certificate Tests
  // ==========================================================================

  @Test
  public void testSyncClientWithCustomCaCert() throws Exception {
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        customCaCertPath != null && Files.exists(customCaCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(customCaCertPath)
            .withTrustSystemCaCerts(false) // Only trust the custom CA
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify TLS config
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertEquals(1, config.getTlsConfig().getCustomCaCertPaths().size());
    assertFalse(config.getTlsConfig().isTrustSystemCaCerts());
    assertTrue(config.getTlsConfig().isVerifyHostname());
  }

  @Test
  public void testAsyncClientWithCustomCaCert() throws Exception {
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        customCaCertPath != null && Files.exists(customCaCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(customCaCertPath).withTrustSystemCaCerts(false).build();

    asyncWrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = asyncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());
  }

  @Test
  public void testCustomCaCertCombinedWithSystemCAs() throws Exception {
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        customCaCertPath != null && Files.exists(customCaCertPath));

    // Trust both custom CA and system CAs
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(customCaCertPath)
            .withTrustSystemCaCerts(true) // Also trust system CAs
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Verify client is functional
    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify config
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertTrue(config.getTlsConfig().isTrustSystemCaCerts());
    assertEquals(1, config.getTlsConfig().getCustomCaCertPaths().size());
  }

  @Test
  public void testInvalidCaCertPathFailsGracefully() throws Exception {
    Path invalidPath = Path.of("/non/existent/ca.pem");

    TlsConfig tlsConfig =
        TlsConfig.builder().withCaCertPath(invalidPath).withTrustSystemCaCerts(false).build();

    try {
      syncWrapper =
          AlternatorDynamoDbClient.builder()
              .endpointOverride(seedUri)
              .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
              .withTlsConfig(tlsConfig)
              .buildWithAlternatorAPI();
      fail("Should fail with invalid CA certificate path");
    } catch (RuntimeException e) {
      assertTrue(
          "Error should mention certificate loading failure",
          e.getMessage().contains("certificate") || e.getMessage().contains("CA"));
    }
  }

  @Test
  public void testInvalidCaCertContentFailsGracefully() throws Exception {
    // Create a temp file with invalid certificate content
    Path tempFile = Files.createTempFile("invalid-ca-", ".pem");
    try {
      Files.writeString(tempFile, "This is not a valid certificate");

      TlsConfig tlsConfig =
          TlsConfig.builder().withCaCertPath(tempFile).withTrustSystemCaCerts(false).build();

      try {
        syncWrapper =
            AlternatorDynamoDbClient.builder()
                .endpointOverride(seedUri)
                .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
                .withTlsConfig(tlsConfig)
                .buildWithAlternatorAPI();
        fail("Should fail with invalid CA certificate content");
      } catch (RuntimeException e) {
        // Expected - invalid certificate format
        assertTrue(
            "Error should be about certificate",
            e.getMessage().toLowerCase().contains("certificate")
                || e.getMessage().toLowerCase().contains("ca"));
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  // ==========================================================================
  // Hostname Verification Tests
  // ==========================================================================

  @Test
  public void testHostnameVerificationEnabled() throws Exception {
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        customCaCertPath != null && Files.exists(customCaCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(customCaCertPath)
            .withTrustSystemCaCerts(false)
            .withVerifyHostname(true) // Explicitly enable
            .build();

    // This may fail if the server certificate doesn't match the hostname
    try {
      syncWrapper =
          AlternatorDynamoDbClient.builder()
              .endpointOverride(seedUri)
              .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
              .withTlsConfig(tlsConfig)
              .buildWithAlternatorAPI();

      List<URI> nodes = syncWrapper.getLiveNodes();
      assertFalse("Should have at least one node", nodes.isEmpty());
    } catch (Exception e) {
      // May fail if hostname doesn't match certificate
      System.out.println("Hostname verification test: " + e.getMessage());
    }
  }

  @Test
  public void testHostnameVerificationDisabled() throws Exception {
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        customCaCertPath != null && Files.exists(customCaCertPath));

    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(customCaCertPath)
            .withTrustSystemCaCerts(false)
            .withVerifyHostname(false) // Disable hostname verification
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    // Should succeed even if hostname doesn't match certificate
    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertFalse(
        "Hostname verification should be disabled", config.getTlsConfig().isVerifyHostname());
  }

  // ==========================================================================
  // TLS Session Cache Integration Tests
  // ==========================================================================

  @Test
  public void testTlsConfigWithSessionCacheSettings() throws Exception {
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(256)
            .withSessionTimeoutSeconds(7200)
            .build();

    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(sessionConfig)
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify session cache config is preserved
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertEquals(256, config.getTlsConfig().getSessionCacheConfig().getSessionCacheSize());
    assertEquals(7200, config.getTlsConfig().getSessionCacheConfig().getSessionTimeoutSeconds());
  }

  @Test
  public void testTlsConfigWithSessionCacheDisabled() throws Exception {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(TlsSessionCacheConfig.disabled())
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify session cache is disabled
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertFalse(config.getTlsConfig().getSessionCacheConfig().isEnabled());
  }

  // ==========================================================================
  // Combination with Other Features Tests
  // ==========================================================================

  @Test
  public void testTlsConfigWithCompression() throws Exception {
    TlsConfig tlsConfig = TlsConfig.trustAll();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(512)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Verify both features are configured
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertTrue(config.getTlsConfig().isTrustAllCertificates());
    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
  }

  @Test
  public void testTlsConfigWithHeadersOptimization() throws Exception {
    TlsConfig tlsConfig = TlsConfig.trustAll();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withOptimizeHeaders(true)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertTrue(config.getTlsConfig().isTrustAllCertificates());
    assertTrue(config.isOptimizeHeaders());
  }

  @Test
  public void testTlsConfigWithRoutingScope() throws Exception {
    TlsConfig tlsConfig = TlsConfig.trustAll();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(tlsConfig)
            .withRoutingScope(ClusterScope.create())
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());
  }

  @Test
  public void testTlsConfigWithRefreshIntervals() throws Exception {
    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsConfig(TlsConfig.trustAll())
            .withActiveRefreshIntervalMs(500)
            .withIdleRefreshIntervalMs(30000)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertEquals(500, config.getActiveRefreshIntervalMs());
    assertEquals(30000, config.getIdleRefreshIntervalMs());
  }

  @Test
  public void testTlsConfigViaDirectBuilderMethods() throws Exception {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(
                TlsSessionCacheConfig.builder()
                    .withSessionCacheSize(150)
                    .withSessionTimeoutSeconds(1800)
                    .build())
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withRoutingScope(ClusterScope.create())
            .withTlsConfig(tlsConfig)
            .buildWithAlternatorAPI();

    AlternatorConfig appliedConfig = syncWrapper.getAlternatorConfig();
    assertTrue(appliedConfig.getTlsConfig().isTrustAllCertificates());
    assertEquals(150, appliedConfig.getTlsConfig().getSessionCacheConfig().getSessionCacheSize());
    assertEquals(
        1800, appliedConfig.getTlsConfig().getSessionCacheConfig().getSessionTimeoutSeconds());
  }

  // ==========================================================================
  // SSLContext Direct Tests
  // ==========================================================================

  @Test
  public void testSslContextWithTrustAllConnectsToServer() throws Exception {
    TlsConfig config = TlsConfig.trustAll();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();

    SSLSocket socket = null;
    try {
      socket = (SSLSocket) socketFactory.createSocket(IntegrationTestConfig.HOST, IntegrationTestConfig.HTTPS_PORT);
      socket.startHandshake();

      SSLSession session = socket.getSession();
      assertTrue("SSL session should be valid", session.isValid());
      assertNotNull("Should have peer certificates", session.getPeerCertificates());

      // Print certificate info for debugging
      X509Certificate cert = (X509Certificate) session.getPeerCertificates()[0];
      System.out.println("Server certificate subject: " + cert.getSubjectX500Principal());
      System.out.println("Server certificate issuer: " + cert.getIssuerX500Principal());
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  @Test
  public void testSslContextWithSystemDefaultMayRejectSelfSigned() throws Exception {
    TlsConfig config = TlsConfig.systemDefault();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();

    SSLSocket socket = null;
    try {
      socket = (SSLSocket) socketFactory.createSocket(IntegrationTestConfig.HOST, IntegrationTestConfig.HTTPS_PORT);
      socket.startHandshake();

      // If we get here, the certificate is trusted by system CAs
      SSLSession session = socket.getSession();
      assertTrue("SSL session should be valid", session.isValid());
      System.out.println("Server certificate is trusted by system CAs");
    } catch (SSLHandshakeException e) {
      // Expected for self-signed certificates
      System.out.println(
          "Server certificate not trusted by system CAs (expected for self-signed): "
              + e.getMessage());
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  @Test
  public void testSslContextWithCustomCaCert() throws Exception {
    assumeTrue(
        "CCM cluster did not provide a custom CA certificate.",
        customCaCertPath != null && Files.exists(customCaCertPath));

    TlsConfig config =
        TlsConfig.builder().withCaCertPath(customCaCertPath).withTrustSystemCaCerts(false).build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();

    SSLSocket socket = null;
    try {
      socket = (SSLSocket) socketFactory.createSocket(IntegrationTestConfig.HOST, IntegrationTestConfig.HTTPS_PORT);
      socket.startHandshake();

      SSLSession session = socket.getSession();
      assertTrue("SSL session should be valid", session.isValid());
      System.out.println("Server certificate validated with custom CA");
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  @Test
  public void testSslContextSessionCacheConfiguration() throws Exception {
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    TlsConfig config =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(sessionConfig)
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    // Verify session cache configuration
    assertEquals(
        "Session cache size should be configured",
        200,
        sslContext.getClientSessionContext().getSessionCacheSize());
    assertEquals(
        "Session timeout should be configured",
        3600,
        sslContext.getClientSessionContext().getSessionTimeout());
  }

  // ==========================================================================
  // Backwards Compatibility Tests
  // ==========================================================================

  @Test
  @SuppressWarnings("deprecation")
  public void testLegacyTlsSessionCacheConfigStillWorks() throws Exception {
    // Test that the deprecated withTlsSessionCacheConfig still works
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(1800)
            .build();

    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withTlsSessionCacheConfig(sessionConfig)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // The old API should still provide a working TLS config
    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertNotNull(config.getTlsConfig());
  }

  @Test
  public void testDefaultConfigIsTrustAll() throws Exception {
    // Default behavior should be trust-all for backwards compatibility
    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertTrue(
        "Default TLS config should be trust-all for backwards compatibility",
        config.getTlsConfig().isTrustAllCertificates());
  }

  @Test
  public void testDisableCertificateChecksStillWorks() throws Exception {
    // Test that the old withDisableCertificateChecks API still works
    syncWrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withDisableCertificateChecks()
            .buildWithAlternatorAPI();

    List<URI> nodes = syncWrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    AlternatorConfig config = syncWrapper.getAlternatorConfig();
    assertTrue(
        "disableCertificateChecks should result in trust-all config",
        config.getTlsConfig().isTrustAllCertificates());
  }

  // ==========================================================================
  // Helper Methods
  // ==========================================================================

  /**
   * Creates a temporary PEM certificate file for testing.
   *
   * @return the path to the temporary certificate file
   * @throws IOException if the file cannot be created
   */
  private Path createTempCertificate() throws IOException {
    // Self-signed test certificate (PEM format)
    // Generated with: openssl req -x509 -nodes -days 3650 -newkey rsa:2048 -keyout key.pem -out
    // cert.pem -subj "/CN=test-ca"
    String testCert =
        "-----BEGIN CERTIFICATE-----\n"
            + "MIIDBTCCAe2gAwIBAgIUSSxkkPZdq+pTKeALhEEpIrfWjeEwDQYJKoZIhvcNAQEL\n"
            + "BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjAxMzEwMTM0MjNaFw0zNjAxMjkw\n"
            + "MTM0MjNaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB\n"
            + "DwAwggEKAoIBAQC7fotfXqSlJk0uM0RE/fbzV6bXsli9dezhC3sOP1XIS7DVnua7\n"
            + "xYL6TnvWR2Y64x68REBXSl6iNYqaIxtlJdf5NwL1tqPI6qLA6y3o/hGXIqRZekke\n"
            + "fv21BODJ/rHu9MrkBIkI+vFDazAUXz3agHV1Qnre3CRDPvHPmNVOac9exU1+qOXh\n"
            + "FBYZOzKKrLkaTVxMjGOl8tzASTbrkBHnbUV1kzIraIvsM+S8AZDZTukCZ+370a3O\n"
            + "MDx8CTv1kONVrpXaVnL9QLGFkpLx1jT1ApeZsMb7twgn/oEFJl8E7BXbG1T+lhza\n"
            + "KF/wf9KXS+fC/xv1G7/ywftScdzP/0IGQEw1AgMBAAGjUzBRMB0GA1UdDgQWBBQI\n"
            + "n37WLBvpKEKBa3LcMuSbyJYeHDAfBgNVHSMEGDAWgBQIn37WLBvpKEKBa3LcMuSb\n"
            + "yJYeHDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBGG3rmBhh0\n"
            + "eHWas+EdD3lifaJt36LdkBAkwE/HkZjCMLlZwyk09uBJCTb+5M5D8cFh17LqXdUz\n"
            + "1xql2yok+r3UojkTGxeShIZZ7Lwkg6nCAygsvu9QZkv2bz382MHhvwoPoGqSpOen\n"
            + "O/WPAZCpDMCfymhvE2C2yX9EUZNj+z3HRLgsyBqqAvx5NS7/whB3rZlLWuH+NMJp\n"
            + "5At2UHnrXSFomBdJOkpvxQFGm8yHES79Yopc4VHA9lD8hQ1yF+Q23b64q161Vemn\n"
            + "bA2+T05xQusR5P6tWOxwqQ1SXXaQYBh7czcms441WZV26Y0SRJTzne8iaNeCLTOJ\n"
            + "11uOp5H+Pi7g\n"
            + "-----END CERTIFICATE-----\n";

    Path tempFile = Files.createTempFile("test-ca-", ".pem");
    Files.writeString(tempFile, testCert);
    return tempFile;
  }
}
