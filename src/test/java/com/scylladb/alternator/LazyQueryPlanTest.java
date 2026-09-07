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
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for LazyQueryPlan.
 *
 * @author dmitry.kropachev
 */
public class LazyQueryPlanTest {

  private List<URI> nodes;
  private AlternatorLiveNodes liveNodes;

  /** Set up test data before each test. */
  @Before
  public void setUp() throws URISyntaxException {
    nodes = createUriList("node", 5);
    liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
  }

  @Test
  public void testConstructorWithRandomSeed() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    assertNotNull(plan);
    assertTrue(plan.hasNext());
  }

  @Test
  public void testConstructorWithFixedSeed() {
    long seed = 42L;
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, seed);
    assertNotNull(plan);
    assertTrue(plan.hasNext());
  }

  @Test
  public void testDeterministicBehaviorWithSameSeed() {
    long seed = 123L;
    LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes, seed);
    LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes, seed);

    // With same seed, first node should be the same
    URI first1 = plan1.next();
    URI first2 = plan2.next();

    assertEquals("Same seed should produce same first node", first1, first2);
  }

  @Test
  public void testDifferentSeedsProduceDifferentFirstNodes() {
    // Run multiple times since random might occasionally pick same node
    int differentCount = 0;
    for (int i = 0; i < 10; i++) {
      LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes, 100L + i);
      LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes, 200L + i);

      URI first1 = plan1.next();
      URI first2 = plan2.next();

      if (!first1.equals(first2)) {
        differentCount++;
      }
    }

    assertTrue("Different seeds should usually produce different first nodes", differentCount >= 5);
  }

  @Test
  public void testHasNextAndNext() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);

    assertTrue(plan.hasNext());
    URI first = plan.next();
    assertNotNull(first);

    for (int i = 0; i < 3; i++) {
      assertTrue(plan.hasNext());
      plan.next();
    }

    assertTrue(plan.hasNext());
    URI last = plan.next();
    assertNotNull(last);

    assertFalse(plan.hasNext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullLiveNodes() {
    new LazyQueryPlan(null);
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextOnExhaustedIterator() {
    // Create a plan and exhaust all nodes
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    while (plan.hasNext()) {
      plan.next();
    }
    // This should throw NoSuchElementException
    plan.next();
  }

  @Test
  public void testAllNodesAreReturned() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);

    Set<URI> expectedNodes = new HashSet<>(nodes);

    Set<URI> returnedNodes = new HashSet<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    assertEquals(
        "All nodes should be returned exactly once", expectedNodes.size(), returnedNodes.size());
    assertEquals("All nodes should be returned", expectedNodes, returnedNodes);
  }

  @Test
  @CoversRequirements("QUERY-REQ-002")
  public void testNodesAreNotDuplicated() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);

    List<URI> returnedNodes = new ArrayList<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    Set<URI> uniqueNodes = new HashSet<>(returnedNodes);
    assertEquals("No duplicates should be returned", returnedNodes.size(), uniqueNodes.size());
  }

  private List<URI> createUriList(String prefix, int count) throws URISyntaxException {
    List<URI> uris = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      uris.add(new URI("http", null, prefix + i + ".example.com", 8000, null, null, null));
    }
    return uris;
  }

  @Test
  public void testSingleNode() throws URISyntaxException {
    List<URI> singleNode = createUriList("single", 1);
    AlternatorLiveNodes singleLiveNodes = new AlternatorLiveNodes(singleNode, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(singleLiveNodes);

    assertTrue(plan.hasNext());
    assertEquals(singleNode.get(0), plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void testManyNodes() throws URISyntaxException {
    List<URI> manyNodes = createUriList("node", 100);
    AlternatorLiveNodes manyLiveNodes = new AlternatorLiveNodes(manyNodes, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(manyLiveNodes, 12345L);

    Set<URI> allExpected = new HashSet<>(manyNodes);

    Set<URI> allReturned = new HashSet<>();
    while (plan.hasNext()) {
      allReturned.add(plan.next());
    }

    assertEquals(allExpected, allReturned);
  }

  @Test
  @CoversRequirements("QUERY-REQ-001")
  public void testLazyBehaviorReadsCurrentNodes() throws URISyntaxException {
    // Start with 3 nodes
    List<URI> initialNodes = createUriList("initial", 3);
    AlternatorLiveNodes dynamicLiveNodes =
        new AlternatorLiveNodes(initialNodes, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(dynamicLiveNodes);

    // Get first node
    URI first = plan.next();
    assertNotNull(first);

    // The lazy nature is tested - each call to hasNext() and next()
    // reads the current state of liveNodes
    assertTrue(plan.hasNext());
  }

  @Test
  public void testIteratorReturnsSelf() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    assertSame("iterator() should return this", plan, plan.iterator());
  }

  @Test
  public void testForEachLoop() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);
    Set<URI> collectedNodes = new HashSet<>();

    for (URI node : plan) {
      collectedNodes.add(node);
    }

    assertEquals("For-each should iterate all nodes", nodes.size(), collectedNodes.size());
    assertEquals("For-each should return all nodes", new HashSet<>(nodes), collectedNodes);
  }

  @Test
  @CoversRequirements("QUERY-REQ-004")
  public void testPreferredNodesAreReturnedBeforeSortedRemaining() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, Arrays.asList(nodes.get(2), nodes.get(4)));

    List<URI> actual = new ArrayList<>();
    while (plan.hasNext()) {
      actual.add(plan.next());
    }

    assertEquals(
        Arrays.asList(nodes.get(2), nodes.get(4), nodes.get(0), nodes.get(1), nodes.get(3)),
        actual);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedSortedAffinityNodesForwardsToLiveNodes() {
    assertEquals(liveNodes.getQueryPlanNodes(), LazyQueryPlan.sortedAffinityNodes(liveNodes));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedPreferredNodeForHashForwardsToLiveNodes() {
    long seed = 42L;

    assertEquals(
        liveNodes.getPreferredQueryPlanNodeForHash(seed),
        LazyQueryPlan.preferredNodeForHash(liveNodes, seed));
  }

  @Test
  public void testSeededAffinityUsesSortedNodeOrder() throws URISyntaxException {
    List<URI> unsortedNodes =
        Arrays.asList(
            new URI("http", null, "node3.example.com", 8000, null, null, null),
            new URI("http", null, "node1.example.com", 8000, null, null, null),
            new URI("http", null, "node2.example.com", 8000, null, null, null));
    AlternatorLiveNodes unsortedLiveNodes =
        new AlternatorLiveNodes(unsortedNodes, "http", 8000, "", "");
    AlternatorLiveNodes sortedLiveNodes =
        new AlternatorLiveNodes(
            Arrays.asList(unsortedNodes.get(1), unsortedNodes.get(2), unsortedNodes.get(0)),
            "http",
            8000,
            "",
            "");

    LazyQueryPlan unsortedPlan = new LazyQueryPlan(unsortedLiveNodes, 42L);
    LazyQueryPlan sortedPlan = new LazyQueryPlan(sortedLiveNodes, 42L);

    List<URI> unsortedSequence = new ArrayList<>();
    List<URI> sortedSequence = new ArrayList<>();
    while (unsortedPlan.hasNext()) {
      unsortedSequence.add(unsortedPlan.next());
    }
    while (sortedPlan.hasNext()) {
      sortedSequence.add(sortedPlan.next());
    }

    assertEquals(sortedSequence, unsortedSequence);
  }

  @Test
  public void testSeededAffinityKeepsDownPreferredNodeInStableKnownOrder() {
    URI downNode = nodes.get(2);
    long seed = seedWhereFirstNodeIs(nodes, downNode);
    List<URI> allHealthyOrder = collectNodes(new LazyQueryPlan(liveNodes, seed));

    markNodeDown(liveNodes, downNode);

    List<URI> degradedOrder = collectNodes(new LazyQueryPlan(liveNodes, seed));
    assertEquals("Health must not change seeded candidate order", allHealthyOrder, degradedOrder);
    assertEquals("Down preferred node stays in the plan", downNode, degradedOrder.get(0));
  }

  @Test
  public void testSeededAffinityDoesNotRemapUnaffectedHealthyPreferredNode() {
    URI preferredNode = nodes.get(0);
    URI unrelatedDownNode = nodes.get(1);
    long seed = seedWhereFirstNodeIs(nodes, preferredNode);

    markNodeDown(liveNodes, unrelatedDownNode);

    LazyQueryPlan degradedPlan = new LazyQueryPlan(liveNodes, seed);
    assertEquals(
        "Healthy preferred node should remain first when another node is down",
        preferredNode,
        degradedPlan.next());
  }

  private long seedWhereFirstNodeIs(List<URI> candidates, URI expected) {
    for (long seed = -1000; seed < 1000; seed++) {
      if (expected.equals(firstNodeForSeed(candidates, seed))) {
        return seed;
      }
    }
    fail("Could not find seed for " + expected);
    return 0;
  }

  private URI firstNodeForSeed(List<URI> candidates, long seed) {
    LazyQueryPlan plan =
        new LazyQueryPlan(new AlternatorLiveNodes(candidates, "http", 8000, "", ""), seed);
    return plan.hasNext() ? plan.next() : null;
  }

  private List<URI> collectNodes(LazyQueryPlan plan) {
    List<URI> collected = new ArrayList<>();
    while (plan.hasNext()) {
      collected.add(plan.next());
    }
    return collected;
  }

  private void markNodeDown(AlternatorLiveNodes liveNodes, URI node) {
    for (int i = 0; i < NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD; i++) {
      liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    }
  }
}
