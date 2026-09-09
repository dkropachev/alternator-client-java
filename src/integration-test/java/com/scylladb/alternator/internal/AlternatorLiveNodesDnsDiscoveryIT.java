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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.AlternatorDynamoDbClientWrapper;
import com.scylladb.alternator.IntegrationTestConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.conn.DnsResolver;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/** Integration tests for DNS-backed live-node discovery against a real Alternator cluster. */
public class AlternatorLiveNodesDnsDiscoveryIT {

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  @Test
  public void testDnsEntrypointDiscoversLiveClusterNodes() throws Exception {
    try (DnsEntrypointProxy proxy = new DnsEntrypointProxy()) {
      URI seedUri = new URI("http://localhost:" + proxy.getPort());
      AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config);

      try {
        liveNodes.updateLiveNodes();

        assertSuccessfulDiscovery(proxy, liveNodes.getLiveNodes(), seedUri);
      } finally {
        liveNodes.shutdownAndWait();
      }
    }
  }

  @Test
  public void testIpv6LiteralEntrypointDiscoversLiveClusterNodes() throws Exception {
    try (DnsEntrypointProxy proxy = new DnsEntrypointProxy(InetAddress.getByName("::1"), 0)) {
      URI seedUri = new URI("http://[::1]:" + proxy.getPort());
      AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config);

      try {
        liveNodes.updateLiveNodes();

        assertSuccessfulDiscovery(proxy, liveNodes.getLiveNodes(), seedUri);
      } finally {
        liveNodes.shutdownAndWait();
      }
    }
  }

  @Test
  public void testDualStackDnsEntrypointFallsBackToIpv4() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    try (DnsEntrypointProxy ipv4Proxy = new DnsEntrypointProxy(ipv4, 0)) {
      assertDualStackFallback(ipv4Proxy, ipv6, ipv4);
    }
  }

  @Test
  public void testDualStackDnsEntrypointFallsBackToIpv6() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    try (DnsEntrypointProxy ipv6Proxy = new DnsEntrypointProxy(ipv6, 0)) {
      assertDualStackFallback(ipv6Proxy, ipv4, ipv6);
    }
  }

  @Test
  public void testDnsEntrypointSupportsDynamoDbOperationsAfterDiscovery() throws Exception {
    try (DnsEntrypointProxy proxy = new DnsEntrypointProxy(0);
        AlternatorDynamoDbClientWrapper wrapper =
            AlternatorDynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:" + proxy.getPort()))
                .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
                .buildWithAlternatorAPI()) {
      wrapper.getAlternatorLiveNodes().updateLiveNodes();
      URI seedUri = URI.create("http://localhost:" + proxy.getPort());

      assertSuccessfulDiscovery(proxy, wrapper.getLiveNodes(), seedUri);

      int seedRequests = proxy.getSeedDataRequestCount();
      int discoveredNodeRequests = proxy.getDiscoveredNodeDataRequestCount();
      wrapper.getClient().listTables(ListTablesRequest.builder().limit(1).build());
      assertEquals(
          "DynamoDB request must not fall back to the DNS seed",
          seedRequests,
          proxy.getSeedDataRequestCount());
      assertTrue(
          "DynamoDB request must use a discovered node",
          proxy.getDiscoveredNodeDataRequestCount() > discoveredNodeRequests);
    }
  }

  private void assertDualStackFallback(
      DnsEntrypointProxy reachableProxy, InetAddress... resolvedAddresses) throws Exception {
    DnsResolver resolver = host -> resolvedAddresses;
    SdkHttpClient httpClient =
        ApacheHttpClient.builder()
            .dnsResolver(resolver)
            .connectionTimeout(Duration.ofSeconds(2))
            .socketTimeout(Duration.ofSeconds(5))
            .build();
    try {
      AlternatorConfig config =
          AlternatorConfig.builder()
              .withSeedHost("dual.test")
              .withScheme("http")
              .withPort(reachableProxy.getPort())
              .build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

      liveNodes.updateLiveNodes();

      assertSuccessfulDiscovery(
          reachableProxy,
          liveNodes.getLiveNodes(),
          URI.create("http://dual.test:" + reachableProxy.getPort()));
    } finally {
      httpClient.close();
    }
  }

  private void assertSuccessfulDiscovery(
      DnsEntrypointProxy proxy, List<URI> liveNodes, URI seedUri) {
    assertTrue("DNS entrypoint should be contacted", proxy.getRequestCount() > 0);
    assertFalse("Should discover live cluster nodes", liveNodes.isEmpty());
    assertFalse("Configured seed should be replaced by discovered nodes", liveNodes.contains(seedUri));
  }

  private static class DnsEntrypointProxy implements AutoCloseable {
    private final HttpServer server;
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicInteger seedDataRequestCount = new AtomicInteger(0);
    private final AtomicInteger discoveredNodeDataRequestCount = new AtomicInteger(0);

    DnsEntrypointProxy() throws IOException {
      this(InetAddress.getLoopbackAddress(), 0);
    }

    DnsEntrypointProxy(int port) throws IOException {
      this(new InetSocketAddress(port));
    }

    DnsEntrypointProxy(InetAddress listenAddress, int port) throws IOException {
      this(new InetSocketAddress(listenAddress, port));
    }

    private DnsEntrypointProxy(InetSocketAddress listenAddress) throws IOException {
      server = HttpServer.create(listenAddress, 0);
      server.createContext("/", this::handleRequest);
      server.start();
    }

    int getPort() {
      return server.getAddress().getPort();
    }

    int getRequestCount() {
      return requestCount.get();
    }

    int getSeedDataRequestCount() {
      return seedDataRequestCount.get();
    }

    int getDiscoveredNodeDataRequestCount() {
      return discoveredNodeDataRequestCount.get();
    }

    @Override
    public void close() {
      server.stop(0);
    }

    private void handleRequest(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
      requestCount.incrementAndGet();
      String path = exchange.getRequestURI().toASCIIString();
      if (!exchange.getRequestURI().getPath().equals("/localnodes")) {
        String host = exchange.getRequestHeaders().getFirst("Host");
        String normalizedHost = host == null ? "" : host.toLowerCase(java.util.Locale.ROOT);
        if (normalizedHost.equals("localhost")
            || normalizedHost.startsWith("localhost:")
            || normalizedHost.equals("[::1]")
            || normalizedHost.startsWith("[::1]:")
            || normalizedHost.equals("127.0.0.1")
            || normalizedHost.startsWith("127.0.0.1:")) {
          seedDataRequestCount.incrementAndGet();
        } else {
          discoveredNodeDataRequestCount.incrementAndGet();
        }
      }
      URI upstream =
          URI.create(
              "http://"
                  + IntegrationTestConfig.HOST
                  + ":"
                  + IntegrationTestConfig.HTTP_PORT
                  + path);
      HttpURLConnection connection = (HttpURLConnection) upstream.toURL().openConnection();
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);
      connection.setRequestMethod(exchange.getRequestMethod());
      exchange
          .getRequestHeaders()
          .forEach(
              (name, values) -> {
                if (!"Host".equalsIgnoreCase(name) && !"Content-Length".equalsIgnoreCase(name)) {
                  connection.setRequestProperty(name, String.join(",", values));
                }
              });
      byte[] requestBody = exchange.getRequestBody().readAllBytes();
      if (requestBody.length > 0) {
        connection.setDoOutput(true);
        try (OutputStream output = connection.getOutputStream()) {
          output.write(requestBody);
        }
      }

      int status = connection.getResponseCode();
      byte[] body = new byte[0];
      InputStream responseBody = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      if (responseBody != null) {
        try (InputStream input = responseBody) {
          body = input.readAllBytes();
        }
      }
      String contentType = connection.getHeaderField("Content-Type");
      if (contentType != null) {
        exchange.getResponseHeaders().add("Content-Type", contentType);
      }
      try {
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
          output.write(body);
        }
      } finally {
        connection.disconnect();
      }
    }
  }
}
