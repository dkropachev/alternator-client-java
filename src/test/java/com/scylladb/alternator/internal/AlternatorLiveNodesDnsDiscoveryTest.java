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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RoutingScope;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.DnsResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/** Unit tests for DNS-backed live-node discovery. */
public class AlternatorLiveNodesDnsDiscoveryTest {

  private HttpServer server;
  private int port;
  private final AtomicInteger requestCount = new AtomicInteger(0);

  @Before
  public void setUp() throws IOException {
    requestCount.set(0);
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/localnodes",
        exchange -> {
          requestCount.incrementAndGet();
          byte[] body = "[\"localhost\",\"node-a.internal\"]".getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    server.start();
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  /** Verifies discovery works when the configured entrypoint is a DNS name. */
  @Test(timeout = 10000)
  public void testDnsEntrypointDiscoversDnsNodeRecords() throws Exception {
    URI seedUri = new URI("http://localhost:" + port);
    AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config);

    try {
      liveNodes.updateLiveNodes();

      assertEquals("DNS seed should be contacted", 1, requestCount.get());
      assertEquals(2, liveNodes.getDiscoveredNodes().size());
      assertEquals(1, liveNodes.getLiveNodes().size());
      assertEquals("localhost", liveNodes.getLiveNodes().get(0).getHost());
      assertEquals("node-a.internal", liveNodes.getQuarantinedNodes().get(0).getHost());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /** Verifies raw IPv6 literals are bracketed for discovery, Host, and learned-node routing. */
  @Test(timeout = 10000)
  public void testIpv6LiteralDiscoveryAndRouting() throws Exception {
    AtomicReference<String> hostHeader = new AtomicReference<>();
    HttpServer ipv6Server =
        startServer(
            InetAddress.getByName("::1"),
            "[\"::1\"]",
            exchange -> hostHeader.set(exchange.getRequestHeaders().getFirst("Host")));
    int ipv6Port = ipv6Server.getAddress().getPort();
    SdkHttpClient httpClient = apacheClient(null);
    try {
      AlternatorConfig config =
          AlternatorConfig.builder()
              .withSeedHosts(Arrays.asList("::1"))
              .withScheme("http")
              .withPort(ipv6Port)
              .build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

      liveNodes.updateLiveNodes();

      assertEquals("http://[::1]:" + ipv6Port, liveNodes.nextAsURI().toString());
      assertEquals("[::1]:" + ipv6Port, hostHeader.get());
    } finally {
      httpClient.close();
      ipv6Server.stop(0);
    }
  }

  /** Verifies A-only, AAAA-only, and both cross-family DNS fallback orders. */
  @Test(timeout = 10000)
  public void testSingleAndDualFamilyDnsEntrypoints() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");

    assertDnsDiscovery(ipv4, ipv4);
    assertDnsDiscovery(ipv6, ipv6);
    assertDnsDiscovery(ipv4, ipv6, ipv4);
    assertDnsDiscovery(ipv6, ipv4, ipv6);
  }

  /** Verifies either address family can be selected when both DNS records are reachable. */
  @Test(timeout = 10000)
  public void testBothDnsFamiliesReachable() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    HttpServer ipv6Server = startServer(ipv6, 0, "[\"ipv6-node.test\"]", exchange -> {});
    int dualPort = ipv6Server.getAddress().getPort();
    HttpServer ipv4Server = startServer(ipv4, dualPort, "[\"ipv4-node.test\"]", exchange -> {});
    try {
      assertFirstReachableDnsFamily(dualPort, "ipv6-node.test", ipv6, ipv4);
      assertFirstReachableDnsFamily(dualPort, "ipv4-node.test", ipv4, ipv6);
    } finally {
      ipv4Server.stop(0);
      ipv6Server.stop(0);
    }
  }

  /** Verifies a malformed response retains the seed and a later refresh can recover. */
  @Test(timeout = 10000)
  public void testMalformedResponseRetainsSeedThenNextRefreshRecovers() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    HttpServer malformedServer =
        startServer(
            ipv4,
            0,
            "not-json",
            exchange -> exchange.getResponseHeaders().set("Connection", "close"));
    int dualPort = malformedServer.getAddress().getPort();
    HttpServer validServer =
        startServer(ipv6, dualPort, "[\"recovered-node.test\"]", exchange -> {});
    AtomicInteger resolutions = new AtomicInteger();
    SdkHttpClient httpClient =
        apacheClient(
            host ->
                resolutions.getAndIncrement() == 0
                    ? new InetAddress[] {ipv4, ipv6}
                    : new InetAddress[] {ipv6, ipv4});
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dualPort), httpClient);

      liveNodes.updateLiveNodes();
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());

      liveNodes.updateLiveNodes();
      assertEquals("recovered-node.test", liveNodes.nextAsURI().getHost());
      assertTrue("DNS should be resolved again after malformed response", resolutions.get() >= 2);
    } finally {
      httpClient.close();
      validServer.stop(0);
      malformedServer.stop(0);
    }
  }

  /**
   * Verifies requests use learned nodes and discovery can recover through the original DNS seed.
   */
  @Test(timeout = 10000)
  public void testLearnedNodeRoutingAndOriginalDnsSeedRecovery() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    AtomicReference<String> seedResponse = new AtomicReference<>("[\"learned.test\"]");
    AtomicInteger seedDiscoveryRequests = new AtomicInteger();
    AtomicInteger learnedDiscoveryRequests = new AtomicInteger();
    AtomicInteger operationRequests = new AtomicInteger();
    HttpServer seedServer =
        startServer(
            ipv4,
            0,
            exchange -> seedResponse.get(),
            exchange -> seedDiscoveryRequests.incrementAndGet());
    int dualPort = seedServer.getAddress().getPort();
    HttpServer learnedServer =
        startServer(
            ipv6,
            dualPort,
            "[\"learned.test\"]",
            exchange -> learnedDiscoveryRequests.incrementAndGet());
    learnedServer.createContext(
        "/operation",
        exchange -> {
          operationRequests.incrementAndGet();
          writeResponse(exchange, "ok");
        });
    AtomicBoolean recovering = new AtomicBoolean(false);
    SdkHttpClient httpClient =
        apacheClient(
            host -> {
              if ("learned.test".equals(host)) {
                return new InetAddress[] {ipv6};
              }
              if ("dual.test".equals(host) && recovering.get()) {
                return new InetAddress[] {ipv6, ipv4};
              }
              return new InetAddress[] {ipv4};
            });
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dualPort), httpClient);

      liveNodes.updateLiveNodes();
      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertEquals(1, seedDiscoveryRequests.get());

      assertEquals(200, executeGet(httpClient, liveNodes.nextAsURI().resolve("/operation")));
      assertEquals(1, operationRequests.get());
      assertEquals(
          "Normal requests must not return to the discovery seed", 1, seedDiscoveryRequests.get());

      liveNodes.updateLiveNodes();
      assertEquals(1, learnedDiscoveryRequests.get());
      assertEquals("learned.test", liveNodes.nextAsURI().getHost());

      learnedServer.stop(0);
      seedResponse.set("[\"recovered.test\"]");
      recovering.set(true);
      liveNodes.updateLiveNodes();

      assertEquals("recovered.test", liveNodes.nextAsURI().getHost());
      assertEquals(2, seedDiscoveryRequests.get());
    } finally {
      httpClient.close();
      learnedServer.stop(0);
      seedServer.stop(0);
    }
  }

  /** Verifies valid, strict-invalid, and fallback scopes over a dual-stack DNS entrypoint. */
  @Test(timeout = 10000)
  public void testScopeBehaviorOverDualStackDns() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    AtomicReference<String> lastQuery = new AtomicReference<>();
    HttpServer scopeServer =
        startServer(
            ipv6,
            0,
            exchange -> {
              String query = exchange.getRequestURI().getRawQuery();
              lastQuery.set(query);
              if ("dc=dc1".equals(query)) {
                return "[\"scoped-node.test\"]";
              }
              if (query == null) {
                return "[\"fallback-node.test\"]";
              }
              return "[]";
            },
            exchange -> {});
    int dualPort = scopeServer.getAddress().getPort();
    DnsResolver resolver = host -> new InetAddress[] {ipv4, ipv6};
    try {
      SdkHttpClient validClient = apacheClient(resolver);
      try {
        AlternatorLiveNodes valid =
            new AlternatorLiveNodes(
                dnsConfig(dualPort, DatacenterScope.of("dc1", null)), validClient);
        valid.updateLiveNodes();
        assertEquals("scoped-node.test", valid.nextAsURI().getHost());
        assertEquals("dc=dc1", lastQuery.get());
      } finally {
        validClient.close();
      }

      SdkHttpClient strictClient = apacheClient(resolver);
      try {
        AlternatorLiveNodes strict =
            new AlternatorLiveNodes(
                dnsConfig(dualPort, DatacenterScope.of("wrong", null)), strictClient);
        try {
          strict.checkIfRackAndDatacenterSetCorrectly();
          fail("Expected invalid strict scope to be rejected");
        } catch (AlternatorLiveNodes.ValidationError expected) {
          assertTrue(expected.getMessage().contains("routing scope may be set incorrectly"));
        }
      } finally {
        strictClient.close();
      }

      SdkHttpClient fallbackClient = apacheClient(resolver);
      try {
        AlternatorLiveNodes fallback =
            new AlternatorLiveNodes(
                dnsConfig(dualPort, DatacenterScope.of("wrong", ClusterScope.create())),
                fallbackClient);
        fallback.updateLiveNodes();
        assertEquals("fallback-node.test", fallback.nextAsURI().getHost());
        assertNull(lastQuery.get());
      } finally {
        fallbackClient.close();
      }
    } finally {
      scopeServer.stop(0);
    }
  }

  /** Verifies an unavailable dual-stack entrypoint returns promptly and preserves its seed. */
  @Test(timeout = 10000)
  public void testAllDnsRecordsUnavailableKeepsSeed() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    HttpServer closedServer = startServer(ipv4, "[]", exchange -> {});
    int closedPort = closedServer.getAddress().getPort();
    closedServer.stop(0);
    SdkHttpClient httpClient = apacheClient(host -> new InetAddress[] {ipv6, ipv4});
    try {
      AlternatorConfig config = dnsConfig(closedPort);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

      long startNanos = System.nanoTime();
      liveNodes.updateLiveNodes();
      long elapsedMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();

      assertEquals(1, liveNodes.getDiscoveredNodes().size());
      assertEquals(1, liveNodes.getQuarantinedNodes().size());
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());
      assertTrue("Unavailable DNS records must return promptly", elapsedMillis < 5000);
    } finally {
      httpClient.close();
    }
  }

  private void assertFirstReachableDnsFamily(
      int dnsPort, String expectedHost, InetAddress... resolvedAddresses) throws Exception {
    SdkHttpClient httpClient = apacheClient(host -> resolvedAddresses);
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dnsPort), httpClient);
      liveNodes.updateLiveNodes();
      assertEquals(expectedHost, liveNodes.nextAsURI().getHost());
    } finally {
      httpClient.close();
    }
  }

  private void assertDnsDiscovery(InetAddress listenAddress, InetAddress... resolvedAddresses)
      throws Exception {
    AtomicReference<String> hostHeader = new AtomicReference<>();
    HttpServer dnsServer =
        startServer(
            listenAddress,
            "[\"dual.test\"]",
            exchange -> hostHeader.set(exchange.getRequestHeaders().getFirst("Host")));
    int dnsPort = dnsServer.getAddress().getPort();
    SdkHttpClient httpClient = apacheClient(host -> resolvedAddresses);
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dnsPort), httpClient);

      liveNodes.updateLiveNodes();

      assertEquals(1, liveNodes.getLiveNodes().size());
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());
      assertEquals("dual.test:" + dnsPort, hostHeader.get());
    } finally {
      httpClient.close();
      dnsServer.stop(0);
    }
  }

  private AlternatorConfig dnsConfig(int dnsPort) {
    return AlternatorConfig.builder()
        .withSeedHosts(Arrays.asList("dual.test"))
        .withScheme("http")
        .withPort(dnsPort)
        .build();
  }

  private AlternatorConfig dnsConfig(int dnsPort, RoutingScope routingScope) {
    return AlternatorConfig.builder()
        .withSeedHosts(Arrays.asList("dual.test"))
        .withScheme("http")
        .withPort(dnsPort)
        .withRoutingScope(routingScope)
        .build();
  }

  private SdkHttpClient apacheClient(DnsResolver dnsResolver) {
    ApacheHttpClient.Builder builder =
        ApacheHttpClient.builder()
            .connectionTimeout(Duration.ofMillis(500))
            .socketTimeout(Duration.ofSeconds(2));
    if (dnsResolver != null) {
      builder.dnsResolver(dnsResolver);
    }
    return builder.build();
  }

  private HttpServer startServer(
      InetAddress listenAddress, String responseBody, ExchangeObserver observer)
      throws IOException {
    return startServer(listenAddress, 0, responseBody, observer);
  }

  private HttpServer startServer(
      InetAddress listenAddress, int listenPort, String responseBody, ExchangeObserver observer)
      throws IOException {
    return startServer(listenAddress, listenPort, exchange -> responseBody, observer);
  }

  private HttpServer startServer(
      InetAddress listenAddress,
      int listenPort,
      ResponseProvider responseProvider,
      ExchangeObserver observer)
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(listenAddress, listenPort), 0);
    httpServer.createContext(
        "/localnodes",
        exchange -> {
          observer.observe(exchange);
          writeResponse(exchange, responseProvider.response(exchange));
        });
    httpServer.start();
    return httpServer;
  }

  private int executeGet(SdkHttpClient httpClient, URI uri) throws IOException {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(uri)
            .method(SdkHttpMethod.GET)
            .putHeader("Host", uri.getHost() + ":" + uri.getPort())
            .build();
    ExecutableHttpRequest executable =
        httpClient.prepareRequest(HttpExecuteRequest.builder().request(request).build());
    HttpExecuteResponse response = executable.call();
    if (response.responseBody().isPresent()) {
      try (AbortableInputStream ignored = response.responseBody().get()) {
        // Closing body releases connection.
      }
    }
    return response.httpResponse().statusCode();
  }

  private static void writeResponse(HttpExchange exchange, String responseBody) throws IOException {
    byte[] body = responseBody.getBytes();
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(body);
    }
  }

  private interface ExchangeObserver {
    void observe(HttpExchange exchange);
  }

  private interface ResponseProvider {
    String response(HttpExchange exchange);
  }
}
