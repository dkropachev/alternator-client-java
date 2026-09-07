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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Tests for GitHub issue #83: Fallback behavior when all nodes in a scope become unreachable.
 *
 * <p>When all nodes in the client's current scope become unreachable (IOException), the scope
 * fallback chain should still be traversed. These tests assert that fallback behavior and that
 * configured seed nodes remain discovery candidates without being leaked into scoped routing.
 *
 * @see <a href="https://github.com/scylladb/alternator-client-java/issues/83">Issue #83</a>
 */
public class AlternatorLiveNodesScopeFallbackTest {

  /**
   * Mock SdkHttpClient that throws IOException on call(), simulating an unreachable node. Tracks
   * the number of attempts made.
   */
  private static class UnreachableHttpClient implements SdkHttpClient {
    final AtomicInteger callCount = new AtomicInteger(0);
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequests.add(request.httpRequest());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          callCount.incrementAndGet();
          throw new IOException("Connection refused (simulated unreachable node)");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "UnreachableHttpClient";
    }
  }

  /**
   * Mock SdkHttpClient that returns a valid response with the given node list. Tracks captured
   * requests.
   */
  private static class ReachableHttpClient implements SdkHttpClient {
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();
    private final String responseJson;

    ReachableHttpClient(String responseJson) {
      this.responseJson = responseJson;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequests.add(request.httpRequest());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          byte[] body = responseJson.getBytes(StandardCharsets.UTF_8);
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
      return "ReachableHttpClient";
    }
  }

  /**
   * Verifies that when all nodes are unreachable, refreshDiscoveredNodes() should traverse the
   * entire scope fallback chain (Rack -&gt; DC -&gt; Cluster) before giving up.
   *
   * <p>With a 3-level fallback chain, all 3 scope levels should be attempted even when nodes throw
   * IOException. The method should not throw IOException until all scopes have been exhausted.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testScopeFallbackTraversedOnIOException() throws Exception {
    UnreachableHttpClient unreachableClient = new UnreachableHttpClient();

    // Configure with RackScope -> DatacenterScope -> ClusterScope fallback chain
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, unreachableClient);

    // refreshDiscoveredNodes() should attempt all 3 scope levels before giving up.
    // It may throw IOException after exhausting all scopes, but the key assertion is
    // that all 3 scopes were tried.
    try {
      liveNodes.refreshDiscoveredNodes();
    } catch (IOException e) {
      // Acceptable to throw after exhausting all scopes
    }

    // All 3 scope levels should have been attempted
    assertTrue(
        "All 3 scope levels (Rack, DC, Cluster) should be attempted on IOException, "
            + "but only "
            + unreachableClient.callCount.get()
            + " attempt(s) were made",
        unreachableClient.callCount.get() >= 3);
  }

  /**
   * Verifies that seed nodes remain available as discovery candidates without being published into
   * the discovered routing ring.
   *
   * <p>After a successful discovery replaces discovered nodes with new nodes, the seed URL should
   * stay out of the routable discovered-node list unless it was returned by discovery. Later
   * discovery cycles should still use the original seed entry point.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testSeedNodesUsedForDiscoveryButNotPublishedAfterRefresh() throws Exception {
    // Phase 1: Successful discovery returns nodes B,C (not the seed A)
    ReachableHttpClient reachableClient = new ReachableHttpClient("[\"10.0.0.2\",\"10.0.0.3\"]");

    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, reachableClient);

    // Initial discovered nodes should contain seed (10.0.0.1)
    List<URI> initialList = liveNodes.getDiscoveredNodes();
    assertEquals(1, initialList.size());
    assertEquals("10.0.0.1", initialList.get(0).getHost());

    // After a successful update, discovered nodes should contain only discovered routing nodes.
    liveNodes.refreshDiscoveredNodes();
    List<URI> updatedList = liveNodes.getDiscoveredNodes();
    List<String> hosts = new ArrayList<>();
    for (URI uri : updatedList) {
      hosts.add(uri.getHost());
    }
    assertTrue("Should contain discovered node 10.0.0.2", hosts.contains("10.0.0.2"));
    assertTrue("Should contain discovered node 10.0.0.3", hosts.contains("10.0.0.3"));
    assertFalse(
        "Seed node 10.0.0.1 should not be published as a discovered routing node",
        hosts.contains("10.0.0.1"));

    reachableClient.capturedRequests.clear();
    liveNodes.refreshDiscoveredNodes();

    assertEquals(2, reachableClient.capturedRequests.size());
    Set<String> contactedHosts = new HashSet<>();
    for (SdkHttpRequest request : reachableClient.capturedRequests) {
      contactedHosts.add(request.host());
    }
    assertEquals(new HashSet<>(Arrays.asList("10.0.0.2", "10.0.0.3")), contactedHosts);
  }

  /**
   * Verifies that when getNodes() returns an empty list (node is reachable but scope has no nodes),
   * the scope fallback works correctly. This is the "happy path" for fallback and should continue
   * to work after the fix.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testFallbackWorksWhenScopeReturnsEmptyList() throws Exception {
    // This client will return an empty list for rack/dc-scoped queries and a non-empty list
    // for the unscoped (cluster) query.
    AtomicInteger requestCount = new AtomicInteger(0);
    SdkHttpClient scopeAwareClient =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            SdkHttpRequest httpReq = request.httpRequest();
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                requestCount.incrementAndGet();
                String responseJson;
                // If the query includes rack or dc parameters, return empty
                // Only the cluster-wide query (no dc/rack) returns nodes
                boolean hasScopeFilter =
                    httpReq.rawQueryParameters().containsKey("rack")
                        || httpReq.rawQueryParameters().containsKey("dc");
                if (hasScopeFilter) {
                  responseJson = "[]";
                } else {
                  responseJson = "[\"10.0.0.5\"]";
                }
                byte[] body = responseJson.getBytes(StandardCharsets.UTF_8);
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
            return "ScopeAwareHttpClient";
          }
        };

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, scopeAwareClient);

    // This should NOT throw - the fallback chain should traverse Rack -> DC -> Cluster
    liveNodes.refreshDiscoveredNodes();

    // All 3 scopes should have been queried (rack returned empty, dc returned empty,
    // cluster returned a node)
    assertEquals(
        "Fallback should traverse all 3 scope levels when empty lists are returned",
        3,
        requestCount.get());

    // Discovered nodes should now contain the cluster-scope routing node. The seed remains a
    // discovery fallback, but is not published unless returned by discovery.
    List<URI> nodes = liveNodes.getDiscoveredNodes();
    List<String> nodeHosts = new ArrayList<>();
    for (URI uri : nodes) {
      nodeHosts.add(uri.getHost());
    }
    assertTrue("Should contain cluster-scope node 10.0.0.5", nodeHosts.contains("10.0.0.5"));
    assertFalse(
        "Should not publish seed node 10.0.0.1 as routing node", nodeHosts.contains("10.0.0.1"));
  }

  /**
   * Verifies that repeated refreshDiscoveredNodes() calls with all-unreachable discovered nodes
   * should eventually try the seed nodes, allowing recovery if the seeds are still alive.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testRepeatedUpdatesWithUnreachableNodesEventuallyTrySeedNodes() throws Exception {
    UnreachableHttpClient unreachableClient = new UnreachableHttpClient();

    // Seeds are 10.0.0.2, 10.0.0.3, 10.0.0.4 (simulating post-discovery state where
    // seeds == discovered nodes, and all are now unreachable)
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("10.0.0.2", "10.0.0.3", "10.0.0.4"))
            .withScheme("http")
            .withPort(8000)
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, unreachableClient);

    // Simulate multiple update cycles
    for (int i = 0; i < 9; i++) {
      try {
        liveNodes.refreshDiscoveredNodes();
      } catch (IOException e) {
        // May throw, that's fine for this test
      }
    }

    // The liveNodes list should remain unchanged (the unreachable nodes should not be removed
    // without replacement)
    List<URI> currentNodes = liveNodes.getDiscoveredNodes();
    assertEquals(
        "liveNodes list should remain unchanged after failed updates", 3, currentNodes.size());

    // Verify the requests contacted all 3 seed nodes
    List<String> contactedHosts = new ArrayList<>();
    for (SdkHttpRequest req : unreachableClient.capturedRequests) {
      contactedHosts.add(req.host());
    }
    assertTrue("Should have contacted node 10.0.0.2", contactedHosts.contains("10.0.0.2"));
    assertTrue("Should have contacted node 10.0.0.3", contactedHosts.contains("10.0.0.3"));
    assertTrue("Should have contacted node 10.0.0.4", contactedHosts.contains("10.0.0.4"));
  }

  /**
   * Verifies that IOException fallback behavior should be consistent with empty-list fallback
   * behavior. Both scenarios should traverse the full scope chain.
   *
   * <p>With the same scope configuration (Rack -&gt; DC -&gt; Cluster), the number of scope levels
   * attempted should be the same regardless of whether the failure is an empty list or an
   * IOException.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testIOExceptionFallbackConsistentWithEmptyListFallback() throws Exception {
    // Scope chain: Rack -> DC -> Cluster (3 levels)
    AlternatorConfig.Builder configBuilder =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())));

    // --- Case 1: Empty-list responses ---
    AtomicInteger emptyListRequestCount = new AtomicInteger(0);
    SdkHttpClient emptyListClient =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                emptyListRequestCount.incrementAndGet();
                byte[] body = "[]".getBytes(StandardCharsets.UTF_8);
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
            return "EmptyListClient";
          }
        };

    AlternatorLiveNodes liveNodesEmptyCase =
        new AlternatorLiveNodes(configBuilder.build(), emptyListClient);

    liveNodesEmptyCase.refreshDiscoveredNodes();
    int emptyListScopeAttempts = emptyListRequestCount.get();

    // --- Case 2: IOException responses ---
    UnreachableHttpClient ioExceptionClient = new UnreachableHttpClient();

    AlternatorLiveNodes liveNodesIOExceptionCase =
        new AlternatorLiveNodes(configBuilder.build(), ioExceptionClient);

    try {
      liveNodesIOExceptionCase.refreshDiscoveredNodes();
    } catch (IOException e) {
      // May throw after exhausting all scopes
    }
    int ioExceptionScopeAttempts = ioExceptionClient.callCount.get();

    // Both cases should traverse the same number of scope levels
    assertEquals(
        "IOException fallback should traverse the same number of scope levels as empty-list "
            + "fallback. Empty-list traversed "
            + emptyListScopeAttempts
            + " levels, but "
            + "IOException only traversed "
            + ioExceptionScopeAttempts
            + " levels",
        emptyListScopeAttempts,
        ioExceptionScopeAttempts);
  }
}
