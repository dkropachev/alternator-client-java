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

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Unit tests for cluster-wide discovery across user-provided seed nodes. */
public class AlternatorLiveNodesClusterDiscoveryTest {

  private static class DiscoveryHttpClient implements SdkHttpClient {
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();
    private final Map<String, String> responsesByHost;
    private final Set<String> failingHosts;

    DiscoveryHttpClient(Map<String, String> responsesByHost) {
      this(responsesByHost, Collections.<String>emptySet());
    }

    DiscoveryHttpClient(Map<String, String> responsesByHost, Set<String> failingHosts) {
      this.responsesByHost = responsesByHost;
      this.failingHosts = failingHosts;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      capturedRequests.add(httpRequest);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          String host = httpRequest.host();
          if (failingHosts.contains(host)) {
            throw new IOException("simulated discovery failure for " + host);
          }
          if (!responsesByHost.containsKey(host)) {
            throw new AssertionError("Unexpected discovery host: " + host);
          }
          byte[] body = responsesByHost.get(host).getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "DiscoveryHttpClient";
    }
  }

  @Test
  public void testClusterScopeMergesConfiguredSeedNodes() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc1-node1.example.com", "[\"dc1-node1.example.com\",\"dc1-node2.example.com\"]");
    responses.put("dc2-node1.example.com", "[\"dc2-node1.example.com\",\"dc2-node2.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.refreshDiscoveredNodes();

    assertEquals(
        new LinkedHashSet<>(
            Arrays.asList(
                "dc1-node1.example.com",
                "dc1-node2.example.com",
                "dc2-node1.example.com",
                "dc2-node2.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
    assertEquals(
        new HashSet<>(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com")),
        capturedHostSet(httpClient.capturedRequests));
    for (SdkHttpRequest request : httpClient.capturedRequests) {
      assertEquals("/localnodes", request.encodedPath());
      assertTrue(request.rawQueryParameters().isEmpty());
    }
  }

  @Test
  public void testClusterScopeKeepsSuccessfulDiscoveryWhenAnotherSeedFails() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc1-node1.example.com", "[\"dc1-node1.example.com\"]");
    DiscoveryHttpClient httpClient =
        new DiscoveryHttpClient(responses, new HashSet<>(Arrays.asList("dc2-node1.example.com")));

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.refreshDiscoveredNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("dc1-node1.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
    assertEquals(
        new HashSet<>(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com")),
        capturedHostSet(httpClient.capturedRequests));
  }

  @Test
  public void testDiscoveryUsesKnownLiveNodesBeforeInitialSeed() throws Exception {
    Map<String, String> responses = new HashMap<>();
    Set<String> failingHosts = new HashSet<>();
    responses.put("seed.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    responses.put("node2.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    responses.put("node3.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses, failingHosts);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();
    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
    assertTrue(liveNodes.getLiveNodes().isEmpty());

    failingHosts.add("seed.example.com");
    httpClient.capturedRequests.clear();
    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        hostSet(liveNodes.getLiveNodes()));
    assertEquals(
        new HashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        capturedHostSet(httpClient.capturedRequests));
  }

  @Test
  public void testDiscoveryFallsBackToInitialSeedWhenLiveNodesReturnNoNodes() throws Exception {
    Map<String, String> responses = new HashMap<>();
    Set<String> failingHosts = new HashSet<>();
    responses.put("seed.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    responses.put("node2.example.com", "[]");
    responses.put("node3.example.com", "[]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses, failingHosts);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();
    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
    assertTrue(liveNodes.getLiveNodes().isEmpty());

    failingHosts.add("node2.example.com");
    responses.put("seed.example.com", "[\"recovered.example.com\"]");
    httpClient.capturedRequests.clear();
    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("recovered.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
    assertTrue(liveNodes.getLiveNodes().isEmpty());
    List<String> captured = capturedHosts(httpClient.capturedRequests);
    assertEquals(3, captured.size());
    assertEquals(
        new HashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        new HashSet<>(captured.subList(0, 2)));
    assertEquals("seed.example.com", captured.get(2));
  }

  @Test
  public void testRackScopeTriesNextSeedWhenFirstSeedIsInWrongDatacenter() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc2-node1.example.com", "[]");
    responses.put("dc1-node1.example.com", "[\"dc1-rack1-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.refreshDiscoveredNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("dc1-rack1-node.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
    List<String> captured = capturedHosts(httpClient.capturedRequests);
    assertTrue(captured.size() == 1 || captured.size() == 2);
    assertTrue(captured.contains("dc1-node1.example.com"));
    assertEquals("dc1-node1.example.com", captured.get(captured.size() - 1));
    for (SdkHttpRequest request : httpClient.capturedRequests) {
      assertTrue(request.rawQueryParameters().containsKey("dc"));
      assertTrue(request.rawQueryParameters().containsKey("rack"));
    }
  }

  @Test
  public void testScopedDiscoveryUsesSeedsWithoutPublishingThem() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc2-node1.example.com", "[]");
    responses.put("dc1-node1.example.com", "[\"dc1-rack1-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.refreshDiscoveredNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("dc1-rack1-node.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));

    responses.put("dc1-rack1-node.example.com", "[]");
    responses.put("dc1-node1.example.com", "[\"dc1-rack1-node2.example.com\"]");
    httpClient.capturedRequests.clear();

    liveNodes.refreshDiscoveredNodes();

    List<String> captured = capturedHosts(httpClient.capturedRequests);
    assertTrue(captured.size() == 2 || captured.size() == 3);
    assertEquals("dc1-rack1-node.example.com", captured.get(0));
    assertEquals("dc1-node1.example.com", captured.get(captured.size() - 1));
    if (captured.size() == 3) {
      assertEquals("dc2-node1.example.com", captured.get(1));
    }
    assertEquals(
        new LinkedHashSet<>(Arrays.asList("dc1-rack1-node2.example.com")),
        hostSet(liveNodes.getDiscoveredNodes()));
  }

  @Test
  public void testScopedLocalNodesQueryValuesAreEncodedOnce() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("seed.example.com", "[\"scoped-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(RackScope.of("dc&prod", "rack=1/blue", null))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();

    SdkHttpRequest request = httpClient.capturedRequests.get(0);
    String encodedQuery = request.encodedQueryParameters().get();
    assertTrue(encodedQuery.contains("dc=dc%26prod"));
    assertTrue(encodedQuery.contains("rack=rack%3D1%2Fblue"));
    assertFalse(encodedQuery.contains("%25"));
  }

  @Test
  public void testLocalNodesParserHandlesWhitespaceAndEscapedStrings() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("seed.example.com", " [ \"node1.example.com\" , \"node\\u0032.example.com\" ] ");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node1.example.com", "node2.example.com")),
        hostSet(liveNodes.getLiveNodes()));
  }

  @Test
  public void testCustomScopeLocalNodesQueryValuesAreEncodedWhenNeeded() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("seed.example.com", "[\"scoped-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(customScope("dc=us east"))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();

    SdkHttpRequest request = httpClient.capturedRequests.get(0);
    assertEquals("dc=us%20east", request.encodedQueryParameters().get());
  }

  @Test
  public void testRackScopeValidationTriesNextSeedWhenFirstSeedIsInWrongDatacenter()
      throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc2-node1.example.com", "[]");
    responses.put("dc1-node1.example.com", "[\"dc1-rack1-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.checkIfRackAndDatacenterSetCorrectly();

    List<String> captured = capturedHosts(httpClient.capturedRequests);
    assertTrue(captured.size() == 1 || captured.size() == 2);
    assertTrue(captured.contains("dc1-node1.example.com"));
    assertEquals("dc1-node1.example.com", captured.get(captured.size() - 1));
    for (SdkHttpRequest request : httpClient.capturedRequests) {
      assertTrue(request.rawQueryParameters().containsKey("dc"));
      assertTrue(request.rawQueryParameters().containsKey("rack"));
    }
  }

  @Test
  public void testNon200DiscoveryResponseBodyIsConsumedOnce() throws Exception {
    CloseTrackingInputStream body = new CloseTrackingInputStream("Service Unavailable");
    SdkHttpClient httpClient =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return HttpExecuteResponse.builder()
                    .response(SdkHttpFullResponse.builder().statusCode(503).build())
                    .responseBody(AbortableInputStream.create(body, body::abort))
                    .build();
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "non-200";
          }
        };

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.refreshDiscoveredNodes();

    assertEquals(1, body.closeCount());
    assertEquals(0, body.readAfterCloseCount());
    assertEquals(0, body.abortCount());
  }

  private static final class CloseTrackingInputStream extends InputStream {
    private final ByteArrayInputStream delegate;
    private int closeCount;
    private int readAfterCloseCount;
    private int abortCount;
    private boolean closed;

    private CloseTrackingInputStream(String body) {
      this.delegate = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int read() throws IOException {
      ensureOpen();
      return delegate.read();
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
      ensureOpen();
      return delegate.read(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
      closeCount++;
      closed = true;
      delegate.close();
    }

    private void abort() {
      abortCount++;
    }

    private int closeCount() {
      return closeCount;
    }

    private int readAfterCloseCount() {
      return readAfterCloseCount;
    }

    private int abortCount() {
      return abortCount;
    }

    private void ensureOpen() throws IOException {
      if (closed) {
        readAfterCloseCount++;
        throw new IOException("read after close");
      }
    }
  }

  private static Set<String> hostSet(List<URI> uris) {
    Set<String> hosts = new LinkedHashSet<>();
    for (URI uri : uris) {
      hosts.add(uri.getHost());
    }
    return hosts;
  }

  private static Set<String> capturedHostSet(List<SdkHttpRequest> requests) {
    Set<String> hosts = new HashSet<>();
    for (SdkHttpRequest request : requests) {
      hosts.add(request.host());
    }
    return hosts;
  }

  private static List<String> capturedHosts(List<SdkHttpRequest> requests) {
    List<String> hosts = new ArrayList<>();
    for (SdkHttpRequest request : requests) {
      hosts.add(request.host());
    }
    return hosts;
  }

  private static RoutingScope customScope(String query) {
    return new RoutingScope() {
      @Override
      public String getName() {
        return "Custom";
      }

      @Override
      public String getDescription() {
        return "Custom";
      }

      @Override
      public RoutingScope getFallback() {
        return null;
      }

      @Override
      public String getLocalNodesQuery() {
        return query;
      }
    };
  }
}
