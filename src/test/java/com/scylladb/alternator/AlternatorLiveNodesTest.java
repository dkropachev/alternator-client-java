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

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.junit.Test;

/**
 * Unit tests for AlternatorLiveNodes functionality that doesn't require a live server.
 *
 * @author dmitry.kropachev
 */
public class AlternatorLiveNodesTest {

  @Test
  public void testConstructorWithSingleNode() throws URISyntaxException {
    URI node = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");
    assertNotNull(liveNodes);
  }

  @Test
  public void testConstructorWithMultipleNodes() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    assertNotNull(liveNodes);
  }

  @Test(expected = RuntimeException.class)
  public void testConstructorWithNullNodes() {
    new AlternatorLiveNodes(null, "http", 8000, "", "");
  }

  @Test(expected = RuntimeException.class)
  public void testConstructorWithEmptyNodes() {
    new AlternatorLiveNodes(Collections.<URI>emptyList(), "http", 8000, "", "");
  }

  @Test
  public void testInitialNodesAreQuarantined() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"), new URI("http://node2.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    List<URI> result = liveNodes.getActiveNodes();

    assertTrue(result.isEmpty());
    assertEquals(nodes, liveNodes.getLiveNodes());
    assertEquals(nodes, liveNodes.getQuarantinedNodes());
  }

  @Test
  public void testGetLiveNodesReturnsUnmodifiableList() throws URISyntaxException {
    URI node = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");

    List<URI> result = liveNodes.getLiveNodes();

    try {
      result.add(new URI("http://newnode:8000"));
      fail("getLiveNodes() should return unmodifiable list");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testGetActiveNodesExcludesDownNodes() throws URISyntaxException {
    URI active = new URI("http://node1.example.com:8000");
    URI down = new URI("http://node2.example.com:8000");
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(Arrays.asList(active, down), "http", 8000, "", "");

    markNodeDown(liveNodes, down);

    assertEquals(Arrays.asList(active, down), liveNodes.getDiscoveredNodes());
    assertEquals(Arrays.asList(active, down), liveNodes.getLiveNodes());
    assertTrue(liveNodes.getActiveNodes().isEmpty());
    assertEquals(Collections.singletonList(active), liveNodes.getQuarantinedNodes());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedNextAsUriUsesEligibleNodesAndPreservesPathAndQuery()
      throws URISyntaxException {
    URI first = new URI("http://node1.example.com:8000");
    URI down = new URI("http://node2.example.com:8000");
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(Arrays.asList(first, down), "http", 8000, "", "");
    markNodeDown(liveNodes, down);

    assertEquals(first, liveNodes.nextAsURI());
    assertEquals(
        new URI("http://node1.example.com:8000/localnodes?rack=r1"),
        liveNodes.nextAsURI("/localnodes", "rack=r1"));
  }

  @Test
  public void testDeprecatedLiveNodesInternalOverrideFeedsQueryPlan() throws URISyntaxException {
    URI seed = new URI("http://seed.example.com:8000");
    URI overrideNode = new URI("http://override.example.com:8000");
    LegacyLiveNodesOverride liveNodes =
        new LegacyLiveNodesOverride(seed, Collections.singletonList(overrideNode));

    assertEquals(
        Collections.singletonList(overrideNode), collectNodes(new LazyQueryPlan(liveNodes)));
    assertEquals(Collections.singletonList(seed), liveNodes.baseLiveNodesInternal());
  }

  @Test
  public void testNewQueryPlan() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);

    assertNotNull(plan);
    assertTrue(plan.hasNext());

    Set<URI> returnedNodes = new HashSet<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    assertEquals(new HashSet<>(nodes), returnedNodes);
  }

  @Test
  public void testNewQueryPlanWithSeed() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    long seed = 42L;
    LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes, seed);
    LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes, seed);

    // Same seed should produce same first node
    URI first1 = plan1.next();
    URI first2 = plan2.next();

    assertEquals("Same seed should produce same first node", first1, first2);
  }

  @Test
  public void testValidateURI() throws Exception {
    URI validUri = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(validUri, "", "");

    // Should not throw
    liveNodes.validateURI(validUri);
  }

  @Test
  public void testValidateURIWithInvalidThrowsException() throws Exception {
    URI validUri = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(validUri, "", "");

    // Create a URI that is valid as a URI but not as a URL (relative URI)
    URI invalidUri = new URI(null, null, "/relative/path", null);

    // The method should throw some exception for invalid URIs
    // The actual exception type depends on the kind of invalid URI
    try {
      liveNodes.validateURI(invalidUri);
      fail("Should throw an exception for invalid URI");
    } catch (AlternatorLiveNodes.ValidationError e) {
      // Expected for MalformedURLException
    } catch (IllegalArgumentException e) {
      // Also acceptable - thrown for relative URIs
    }
  }

  @Test
  public void testHttpsScheme() throws URISyntaxException {
    URI node = new URI("https://localhost:8043");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");

    URI result = new LazyQueryPlan(liveNodes).next();

    assertEquals("https", result.getScheme());
    assertEquals(8043, result.getPort());
  }

  private static List<URI> collectNodes(LazyQueryPlan plan) {
    List<URI> collected = new ArrayList<>();
    while (plan.hasNext()) {
      collected.add(plan.next());
    }
    return collected;
  }

  private static void markNodeDown(AlternatorLiveNodes liveNodes, URI node) {
    for (int i = 0; i < NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD; i++) {
      liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    }
  }

  private static final class LegacyLiveNodesOverride extends AlternatorLiveNodes {
    private final List<URI> nodes;

    private LegacyLiveNodesOverride(URI seed, List<URI> nodes) {
      super(seed, "", "");
      this.nodes = new ArrayList<>(nodes);
    }

    @Override
    @Deprecated
    protected List<URI> getLiveNodesInternal() {
      return nodes;
    }

    private List<URI> baseLiveNodesInternal() {
      return super.getLiveNodesInternal();
    }
  }
}
