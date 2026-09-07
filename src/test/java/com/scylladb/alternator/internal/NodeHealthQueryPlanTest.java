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

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

public class NodeHealthQueryPlanTest {
  private static final URI ACTIVE_A = node("active-a.local");
  private static final URI ACTIVE_B = node("active-b.local");
  private static final URI QUARANTINED_A = node("quarantined-a.local");
  private static final URI QUARANTINED_B = node("quarantined-b.local");
  private static final URI DOWN = node("down.local");

  @Test
  public void regularPlanReturnsActiveThenQuarantineInSourceRelativeOrder() {
    AlternatorLiveNodes liveNodes =
        liveNodes(Arrays.asList(ACTIVE_A, ACTIVE_B, QUARANTINED_A, QUARANTINED_B));
    quarantine(liveNodes, QUARANTINED_A);
    quarantine(liveNodes, QUARANTINED_B);
    NodeHealthQueryPlan plan = regular(liveNodes, QUARANTINED_B, ACTIVE_B, QUARANTINED_A, ACTIVE_A);

    assertEquals(ACTIVE_B, plan.nextRouteCandidate());
    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_B, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    URI nextCycleFirst = plan.nextRouteCandidate();
    List<URI> nextCycle =
        Arrays.asList(
            nextCycleFirst,
            plan.nextRouteCandidate(),
            plan.nextRouteCandidate(),
            plan.nextRouteCandidate());
    assertTrue(Arrays.asList(ACTIVE_A, ACTIVE_B).contains(nextCycleFirst));
    assertTrue(Arrays.asList(ACTIVE_A, ACTIVE_B).contains(nextCycle.get(1)));
    assertTrue(Arrays.asList(QUARANTINED_A, QUARANTINED_B).contains(nextCycle.get(2)));
    assertTrue(Arrays.asList(QUARANTINED_A, QUARANTINED_B).contains(nextCycle.get(3)));
    assertEquals(
        new HashSet<>(Arrays.asList(ACTIVE_A, ACTIVE_B, QUARANTINED_A, QUARANTINED_B)),
        new HashSet<>(nextCycle));
  }

  @Test
  public void affinityPlanMakesTwoPassesOverSameDeterministicOrder() {
    AlternatorLiveNodes liveNodes =
        liveNodes(Arrays.asList(ACTIVE_A, ACTIVE_B, QUARANTINED_A, QUARANTINED_B));
    quarantine(liveNodes, QUARANTINED_A);
    quarantine(liveNodes, QUARANTINED_B);
    NodeHealthQueryPlan plan =
        affinity(liveNodes, QUARANTINED_B, ACTIVE_B, QUARANTINED_A, ACTIVE_A);

    assertEquals(ACTIVE_B, plan.nextRouteCandidate());
    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_B, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    assertEquals(ACTIVE_B, plan.nextRouteCandidate());
    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_B, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
  }

  @Test
  public void noActivePlanReturnsAllQuarantinedInSourceOrder() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(QUARANTINED_A, QUARANTINED_B));
    quarantine(liveNodes, QUARANTINED_A);
    quarantine(liveNodes, QUARANTINED_B);
    NodeHealthQueryPlan plan = affinity(liveNodes, QUARANTINED_B, QUARANTINED_A);

    assertEquals(QUARANTINED_B, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_B, plan.nextRouteCandidate());
  }

  @Test
  public void downNodesAreExcludedFromBothPasses() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(ACTIVE_A, DOWN, QUARANTINED_A));
    markDown(liveNodes, DOWN);
    quarantine(liveNodes, QUARANTINED_A);
    NodeHealthQueryPlan plan = affinity(liveNodes, DOWN, QUARANTINED_A, ACTIVE_A);

    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
  }

  @Test
  public void healthIsRecheckedBeforeCandidateSelection() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(ACTIVE_A, QUARANTINED_A));
    quarantine(liveNodes, QUARANTINED_A);
    NodeHealthQueryPlan plan = affinity(liveNodes, ACTIVE_A, QUARANTINED_A);
    markDown(liveNodes, ACTIVE_A);

    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
  }

  @Test
  public void duplicateCanonicalEndpointsAreReturnedOncePerCycle() {
    URI implicitPort = URI.create("http://active-a.local");
    URI explicitPort = URI.create("http://active-a.local:80");
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(implicitPort));
    NodeHealthQueryPlan plan = regular(liveNodes, implicitPort, explicitPort);

    assertEquals(implicitPort, plan.nextRouteCandidate());
    assertEquals(implicitPort, plan.nextRouteCandidate());
  }

  @Test
  public void legacyEndpointFallbackIsIgnoredWhenStartingNextCycle() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(ACTIVE_A));
    NodeHealthQueryPlan plan = regular(liveNodes, ACTIVE_A);

    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    assertEquals(ACTIVE_A, plan.nextRouteCandidate(node("fallback.local")));
  }

  @Test
  public void recoveredUntriedNodeCanJoinCurrentCycle() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(ACTIVE_A, DOWN));
    markDown(liveNodes, DOWN);
    NodeHealthQueryPlan plan = affinity(liveNodes, DOWN, ACTIVE_A);

    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    liveNodes.reportNodeResult(DOWN, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(DOWN, plan.nextRouteCandidate());
  }

  @Test
  public void allDownTrafficPlanReturnsNoRouteWithoutCycling() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(ACTIVE_A, DOWN));
    markDown(liveNodes, ACTIVE_A);
    markDown(liveNodes, DOWN);
    NodeHealthQueryPlan plan = affinity(liveNodes, ACTIVE_A, DOWN);

    assertNull(plan.nextRouteCandidate());
    assertNull(plan.nextRouteCandidate());
  }

  @Test
  public void probePlanUsesTheSameActiveThenQuarantineOrder() {
    AlternatorLiveNodes liveNodes = liveNodes(Arrays.asList(ACTIVE_A, QUARANTINED_A));
    quarantine(liveNodes, QUARANTINED_A);
    NodeHealthQueryPlan plan =
        liveNodes.newProbeQueryPlan(ordered(liveNodes, QUARANTINED_A, ACTIVE_A));

    assertEquals(ACTIVE_A, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    assertNull(plan.nextRouteCandidate());
  }

  private static NodeHealthQueryPlan regular(AlternatorLiveNodes liveNodes, URI... order) {
    return liveNodes.newRegularQueryPlan(ordered(liveNodes, order));
  }

  private static NodeHealthQueryPlan affinity(AlternatorLiveNodes liveNodes, URI... order) {
    return liveNodes.newAffinityQueryPlan(ordered(liveNodes, order));
  }

  private static LazyQueryPlan ordered(AlternatorLiveNodes liveNodes, URI... order) {
    return new LazyQueryPlan(liveNodes, Arrays.asList(order));
  }

  private static void quarantine(AlternatorLiveNodes liveNodes, URI node) {
    markDown(liveNodes, node);
    liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
  }

  private static void markDown(AlternatorLiveNodes liveNodes, URI node) {
    liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
  }

  private static AlternatorLiveNodes liveNodes(List<URI> nodes) {
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineFailureThreshold(1)
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(alternatorConfig(nodes, config), new NoopHttpClient());
    for (URI node : nodes) {
      liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    }
    return liveNodes;
  }

  private static AlternatorConfig alternatorConfig(List<URI> nodes, NodeHealthConfig config) {
    List<String> hosts = new ArrayList<>();
    for (URI node : nodes) {
      hosts.add(node.getHost());
    }
    return AlternatorConfig.builder()
        .withSeedHosts(hosts)
        .withScheme(nodes.get(0).getScheme())
        .withPort(nodes.get(0).getPort())
        .withNodeHealthConfig(config)
        .build();
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static final class NoopHttpClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public software.amazon.awssdk.http.HttpExecuteResponse call() throws IOException {
          throw new IOException("not used");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "noop";
    }
  }
}
