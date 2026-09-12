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
import com.scylladb.alternator.CoversRequirements;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

public class AlternatorLiveNodesNodeHealthTest {
  @Test
  public void generationUnawarePublicApiRejectsTrafficObservations() {
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.getDefault(), new LocalNodesHttpClient());
    URI node = node("active.local");
    NodeHealthStatus before = liveNodes.getNodeHealthStatus(node);

    try {
      liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
      fail("expected generation-unaware traffic report to be rejected");
    } catch (IllegalArgumentException expected) {
      assertTrue(
          expected.getMessage().contains("reportNodeResult(URI, NodeHealthObservation, long)"));
    }

    assertEquals(before.getState(), liveNodes.getNodeHealthStatus(node).getState());
    assertEquals(
        before.getConsecutiveFailures(),
        liveNodes.getNodeHealthStatus(node).getConsecutiveFailures());
  }

  @Test
  public void publicProbeReportsCannotActivateRemovedQuarantinedNode() throws Exception {
    AlternatorLiveNodes liveNodes =
        directLiveNodes(NodeHealthConfig.getDefault(), new LocalNodesHttpClient());
    URI removed = node("candidate.local");
    publishDiscoveredNodes(liveNodes, Arrays.asList(node("replacement.local")));

    liveNodes.reportNodeResult(removed, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(removed).getState());

    liveNodes.reportNodeResult(
        removed, NodeHealthObservation.PROBE_SUCCESS, liveNodes.getNodeHealthGeneration(removed));
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(removed).getState());
  }

  @Test
  public void publicProbeReportsAdvanceDownNodeRecoveryWithoutUsingTrafficGeneration() {
    AlternatorLiveNodes liveNodes =
        directLiveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(2)
                .build(),
            new LocalNodesHttpClient());
    URI node = node("candidate.local");
    liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    reportCurrentTraffic(liveNodes, node, NodeHealthObservation.TRAFFIC_FAILURE);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(node).getState());

    liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(1, liveNodes.getNodeHealthStatus(node).getConsecutiveSuccesses());

    liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS, Long.MIN_VALUE);
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(node).getState());
  }

  @Test
  @CoversRequirements("HEALTH-REQ-008")
  public void lateDiscoverySuccessCannotActivateRemovedQuarantinedNode() throws Exception {
    BlockingLocalNodesHttpClient httpClient = new BlockingLocalNodesHttpClient("[]", 1);
    AlternatorLiveNodes liveNodes = directLiveNodes(NodeHealthConfig.getDefault(), httpClient);
    URI removed = node("candidate.local");
    FutureTask<Void> refresh =
        new FutureTask<>(
            () -> {
              liveNodes.refreshDiscoveredNodes();
              return null;
            });
    Thread refreshThread = new Thread(refresh, "late-discovery-test");
    refreshThread.setDaemon(true);
    refreshThread.start();

    try {
      assertTrue(httpClient.awaitBlockedCall());
      publishDiscoveredNodes(liveNodes, Arrays.asList(node("replacement.local")));
    } finally {
      httpClient.releaseBlockedCall();
    }
    refresh.get(5, TimeUnit.SECONDS);

    assertFalse(liveNodes.getDiscoveredNodes().contains(removed));
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(removed).getState());
  }

  @Test
  public void absentSeedFallbackCanStillRecordSuccessfulDirectContact() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"current.local\"]", "current.local");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("seed.local", "current.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI seed = node("seed.local");
    URI current = node("current.local");
    publishDiscoveredNodes(liveNodes, Arrays.asList(current));

    liveNodes.refreshDiscoveredNodes();

    assertFalse(liveNodes.getDiscoveredNodes().contains(seed));
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(seed).getState());
  }

  @Test
  public void lateFeatureCheckSuccessCannotActivateRemovedQuarantinedNode() throws Exception {
    BlockingLocalNodesHttpClient httpClient =
        new BlockingLocalNodesHttpClient("[\"candidate.local\"]", 1);
    AlternatorLiveNodes liveNodes = directLiveNodes(NodeHealthConfig.getDefault(), httpClient);
    URI removed = node("candidate.local");
    FutureTask<Boolean> featureCheck =
        new FutureTask<>(liveNodes::checkIfRackDatacenterFeatureIsSupported);
    Thread featureThread = new Thread(featureCheck, "late-feature-check-test");
    featureThread.setDaemon(true);
    featureThread.start();

    try {
      assertTrue(httpClient.awaitBlockedCall());
      publishDiscoveredNodes(liveNodes, Arrays.asList(node("replacement.local")));
    } finally {
      httpClient.releaseBlockedCall();
    }

    assertFalse(featureCheck.get(5, TimeUnit.SECONDS));
    assertFalse(liveNodes.getDiscoveredNodes().contains(removed));
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(removed).getState());
  }

  @Test
  public void disabledHealthExposesActiveNodesAndIssuesNoPhysicalHealthProbes() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient();
    AlternatorLiveNodes liveNodes = directLiveNodes(NodeHealthConfig.disabled(), httpClient);
    URI seed = node("candidate.local");
    URI discovered = node("discovered.local");
    publishDiscoveredNodes(liveNodes, Arrays.asList(seed, discovered));

    assertEquals(Arrays.asList(seed, discovered), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getQuarantinedNodes().isEmpty());
    assertTrue(liveNodes.getDownNodes().isEmpty());

    for (int i = 0; i < NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD + 1; i++) {
      liveNodes.reportNodeResult(
          seed, NodeHealthObservation.TRAFFIC_FAILURE, liveNodes.getNodeHealthGeneration(seed));
    }
    liveNodes.reportNodeResult(seed, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(
        discovered,
        NodeHealthObservation.TRAFFIC_SUCCESS,
        liveNodes.getNodeHealthGeneration(discovered));

    assertEquals(Arrays.asList(seed, discovered), liveNodes.getActiveNodes());
    assertTrue(liveNodes.probeQuarantinedNodes().isEmpty());
    assertTrue(liveNodes.probeQuarantinedNodesAsync().get(5, TimeUnit.SECONDS).isEmpty());
    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    liveNodes.scheduleBackgroundHealthProbes();
    assertTrue(httpClient.requests.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void liveDiscoveryCandidatesUseOneTopologySnapshotAndOneClassificationPerEndpoint()
      throws Exception {
    URI active = URI.create("HTTP://ACTIVE.LOCAL:8080/path");
    URI activeDuplicate = node("active.local");
    URI quarantined = node("quarantined.local");
    URI down = node("down.local");
    List<URI> snapshot = Arrays.asList(quarantined, down, active, activeDuplicate);
    AtomicLong topologyReads = new AtomicLong();
    AtomicLong healthClassifications = new AtomicLong();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("candidate.local"))
            .withScheme("http")
            .withPort(8080)
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, new LocalNodesHttpClient()) {
          @Override
          protected List<URI> getDiscoveredNodesInternal() {
            topologyReads.incrementAndGet();
            return snapshot;
          }

          @Override
          NodeHealthState getQueryPlanNodeState(URI candidate) {
            healthClassifications.incrementAndGet();
            URI key = NodeHealthStore.canonicalNodeKey(candidate);
            if (key.equals(NodeHealthStore.canonicalNodeKey(active))) {
              return NodeHealthState.ACTIVE;
            }
            if (key.equals(NodeHealthStore.canonicalNodeKey(quarantined))) {
              return NodeHealthState.QUARANTINED;
            }
            return NodeHealthState.DOWN;
          }
        };
    Method method = AlternatorLiveNodes.class.getDeclaredMethod("liveDiscoveryCandidates");
    method.setAccessible(true);

    List<URI> candidates = (List<URI>) method.invoke(liveNodes);

    assertEquals(Arrays.asList(active, quarantined, down), candidates);
    assertEquals(1, topologyReads.get());
    assertEquals(3, healthClassifications.get());
  }

  @Test
  @CoversRequirements("HEALTH-REQ-005")
  public void discoveryActivatesContactedSeedButQuarantinesNewNodesUntilDirectProbe()
      throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"seed.local\",\"new.local\"]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("seed.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI seed = node("seed.local");
    URI discovered = node("new.local");

    assertEquals(Arrays.asList(seed), liveNodes.getQuarantinedNodes());
    assertTrue(liveNodes.getActiveNodes().isEmpty());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(seed), liveNodes.getActiveNodes());
    assertEquals(Arrays.asList(discovered), liveNodes.getQuarantinedNodes());

    assertEquals(Arrays.asList(discovered), liveNodes.probeQuarantinedNodes());

    assertEquals(Arrays.asList(discovered, seed), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getQuarantinedNodes().isEmpty());
  }

  @Test
  public void probeQuarantinedNodesReturnsPartialSuccessAndLeavesFailuresQuarantined() {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[]", 1);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("node-a.local", "node-b.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    List<URI> successful = liveNodes.probeQuarantinedNodes();

    assertEquals(1, successful.size());
    assertEquals(successful, liveNodes.getActiveNodes());
    assertEquals(1, liveNodes.getQuarantinedNodes().size());
  }

  @Test
  public void probeQuarantinedNodesSkipsActiveAndDownNodes() {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("active.local", "down.local", "quarantined.local"))
            .withScheme("http")
            .withPort(8080)
            .withNodeHealthConfig(
                NodeHealthConfig.builder().withQuarantineFailureThreshold(1).build())
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI active = node("active.local");
    URI down = node("down.local");
    URI quarantined = node("quarantined.local");
    liveNodes.reportNodeResult(active, NodeHealthObservation.PROBE_SUCCESS);
    reportCurrentTraffic(liveNodes, down, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(quarantined), liveNodes.probeQuarantinedNodes());

    assertEquals(1, httpClient.requests.size());
    assertEquals("quarantined.local", httpClient.requests.get(0).host());
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(active).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(down).getState());
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(quarantined).getState());
  }

  @Test
  public void topologyRefreshUsesHealthBucketsAndDownFallbackIsHealthNeutral() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("down.local", "quarantined.local", "active.local"))
            .withScheme("http")
            .withPort(8080)
            .withNodeHealthConfig(
                NodeHealthConfig.builder().withQuarantineFailureThreshold(1).build())
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI active = node("active.local");
    URI quarantined = node("quarantined.local");
    URI down = node("down.local");
    liveNodes.reportNodeResult(active, NodeHealthObservation.PROBE_SUCCESS);
    reportCurrentTraffic(liveNodes, down, NodeHealthObservation.TRAFFIC_FAILURE);
    int downRecoveryBefore = liveNodes.getNodeHealthStatus(down).getConsecutiveSuccesses();

    liveNodes.refreshDiscoveredNodes();

    assertEquals(3, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("quarantined.local", httpClient.requests.get(1).host());
    assertEquals("down.local", httpClient.requests.get(2).host());
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(quarantined).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(down).getState());
    assertEquals(downRecoveryBefore, liveNodes.getNodeHealthStatus(down).getConsecutiveSuccesses());
  }

  @Test
  public void runDownNodeProbesMovesRecoveredNodeToQuarantine() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeRecoverySuccessThreshold(1).build(),
            new LocalNodesHttpClient());
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    List<URI> recovered = liveNodes.runDownNodeProbes();

    assertEquals(Arrays.asList(recovering), recovered);
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void runDownNodeProbesRequiresConfiguredRecoverySuccessesBeforeQuarantine() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeRecoverySuccessThreshold(2).build(),
            new LocalNodesHttpClient());
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
    assertEquals(1, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());

    assertEquals(Arrays.asList(recovering), liveNodes.runDownNodeProbes());
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());
    assertEquals(0, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());
  }

  @Test
  public void featureCheckUsesRandomQueryPlanNode() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]");
    AlternatorLiveNodes liveNodes = liveNodes(NodeHealthConfig.getDefault(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertFalse(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(2, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("active.local", httpClient.requests.get(1).host());
    assertEquals("/localnodes", httpClient.requests.get(0).encodedPath());
    assertEquals("/localnodes", httpClient.requests.get(1).encodedPath());
    assertTrue(httpClient.requests.get(0).rawQueryParameters().containsKey("rack"));
    assertTrue(httpClient.requests.get(1).rawQueryParameters().isEmpty());
  }

  @Test
  public void featureCheckTriesNextQueryPlanNodeWhenOneNodeFails() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]", 1);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("node-a.local", "node-b.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, httpClient) {
          @Override
          public List<URI> getQueryPlanNodes() {
            return Arrays.asList(node("node-a.local"), node("node-b.local"));
          }
        };

    assertFalse(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(3, httpClient.requests.size());
    assertTrue(httpClient.requests.get(0).rawQueryParameters().containsKey("rack"));
    assertNotEquals(httpClient.requests.get(0).host(), httpClient.requests.get(1).host());
    assertEquals(httpClient.requests.get(1).host(), httpClient.requests.get(2).host());
    assertTrue(httpClient.requests.get(1).rawQueryParameters().containsKey("rack"));
    assertTrue(httpClient.requests.get(2).rawQueryParameters().isEmpty());
  }

  @Test
  public void featureCheckProbeOutcomesDoNotAffectActiveNodeHealth() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]", 1);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI active = node("active.local");

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected feature check failure");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // expected
    }
    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());

    assertFalse(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void featureCheckEmptyValidResponseStillActivatesContactedQuarantinedNode()
      throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[]");
    AlternatorLiveNodes liveNodes = directLiveNodes(NodeHealthConfig.getDefault(), httpClient);
    URI candidate = node("candidate.local");

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected empty node list to prevent feature detection");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      assertTrue(expected.getMessage().contains("returned empty list"));
    }

    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(candidate).getState());
    assertEquals(2, httpClient.requests.size());
  }

  @Test
  public void featureCheckFirstSuccessfulContactSurvivesSecondRequestFailure() throws Exception {
    AlternatorLiveNodes liveNodes =
        directLiveNodes(NodeHealthConfig.getDefault(), new SecondRequestFailureHttpClient());
    URI candidate = node("candidate.local");

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected second feature-detection request to fail");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      assertTrue(expected.getMessage().contains("failed to read list of nodes"));
    }

    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(candidate).getState());
  }

  @Test
  public void featureCheckRejectsNullQueryPlanNodeWithoutNpe() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("active.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, httpClient) {
          @Override
          public List<URI> getQueryPlanNodes() {
            return Arrays.asList((URI) null);
          }
        };

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("Expected FailedToCheck");
    } catch (AlternatorLiveNodes.FailedToCheck e) {
      assertEquals("No live nodes available", e.getMessage());
    }
    assertTrue(httpClient.requests.isEmpty());
  }

  @Test
  public void randomQueryPlanKeepsQuarantinedCandidateInKnownNodeSet() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            new LocalNodesHttpClient(),
            null,
            Arrays.asList(
                "active01.local",
                "active02.local",
                "active03.local",
                "active04.local",
                "active05.local",
                "active06.local",
                "active07.local",
                "active08.local",
                "active09.local",
                "active10.local",
                "active11.local",
                "active12.local",
                "recovering.local"));
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    assertTrue(collectNodes(new LazyQueryPlan(liveNodes)).contains(recovering));
  }

  @Test
  public void preferredQueryPlanNodeForHashKeepsQuarantinedPreferredNode() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");
    long recoveringHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), recovering);

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    URI preferred = liveNodes.getPreferredQueryPlanNodeForHash(recoveringHash);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, recoveringHash);

    assertEquals(recovering, preferred);
    assertEquals(preferred, plan.next());
  }

  @Test
  public void quarantineHashAssignmentKeepsDownNodeButFilterSkipsIt() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");
    long recoveringHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), recovering);

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(
        Arrays.asList(recovering, active), liveNodes.getQueryPlanNodesForHash(recoveringHash));

    markNodeDown(liveNodes, recovering);

    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
    assertEquals(
        Arrays.asList(recovering, active), liveNodes.getQueryPlanNodesForHash(recoveringHash));
    NodeHealthQueryPlan plan =
        liveNodes.newAffinityQueryPlan(new LazyQueryPlan(liveNodes, recoveringHash));
    assertEquals(active, plan.nextRouteCandidate());
  }

  @Test
  public void quarantinePromotesAfterSuccessfulTraffic() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(2)
                .build(),
            new LocalNodesHttpClient());
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());

    reportCurrentTraffic(liveNodes, recovering, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());

    reportCurrentTraffic(liveNodes, recovering, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertTrue(liveNodes.getQuarantinedNodes().isEmpty());
    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(Arrays.asList(node("active.local"), recovering), liveNodes.getActiveNodes());
  }

  @Test
  public void backgroundRecoversDownNodeAndActivatesItWithDirectProbe() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeProbePeriodMs(10)
                .withQuarantineSuccessThreshold(2)
                .build(),
            httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!liveNodes.getActiveNodes().contains(recovering) && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    } finally {
      liveNodes.shutdownAndWait();
    }

    assertFalse(httpClient.requests.isEmpty());
    assertTrue(liveNodes.getActiveNodes().contains(recovering));
  }

  @Test
  public void backgroundDirectlyProbesAndActivatesNewlyDiscoveredNode() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\",\"new.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeProbePeriodMs(10).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI discovered = node("new.local");

    liveNodes.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!liveNodes.getActiveNodes().contains(discovered) && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    } finally {
      liveNodes.shutdownAndWait();
    }

    assertTrue(liveNodes.getActiveNodes().contains(discovered));
    assertTrue(
        httpClient.requests.stream().anyMatch(request -> "new.local".equals(request.host())));
  }

  @Test
  public void refreshPublishesDiscoveredNodesInSortedOrder() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"z.local\",\"a.local\"]");
    AlternatorLiveNodes liveNodes = liveNodes(NodeHealthConfig.getDefault(), httpClient);

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("a.local"), node("z.local")), liveNodes.getDiscoveredNodes());
  }

  @Test
  public void localNodesNonOkDoesNotMarkActiveSeedDown() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient(400);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void duplicateSeedDiscoveryFailureRunsOncePerRefresh() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient(400);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build(),
            httpClient,
            null,
            Arrays.asList("active.local", "active.local"));
    URI active = node("active.local");

    liveNodes.refreshDiscoveredNodes();

    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
    assertEquals(1, httpClient.requests.size());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(active), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
  }

  @Test
  public void localNodesRuntimeFailureDoesNotMarkActiveSeedDown() throws Exception {
    RuntimeFailureHttpClient httpClient = new RuntimeFailureHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void localNodesInvalidBodyDoesNotMarkActiveSeedDown() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("not-json");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void localNodesMissingBodyDoesNotMarkActiveSeedDown() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient((String) null);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void featureCheckRuntimeFailureDoesNotMarkSelectedNodeDown() throws Exception {
    RuntimeFailureHttpClient httpClient = new RuntimeFailureHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected feature check failure");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // expected
    }

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void featureCheckInvalidBodyDoesNotMarkSelectedNodeDown() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("not-json");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected feature check failure");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // expected
    }

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void rediscoveredNodeKeepsPreviousHealthState() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"transient.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI transientNode = node("transient.local");

    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, transientNode);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setResponseBody("[\"other.local\"]");
    liveNodes.refreshDiscoveredNodes();
    assertFalse(liveNodes.getDiscoveredNodes().contains(transientNode));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setResponseBody("[\"transient.local\"]");
    liveNodes.refreshDiscoveredNodes();

    assertTrue(liveNodes.getDiscoveredNodes().contains(transientNode));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());
    assertFalse(liveNodes.getActiveNodes().contains(transientNode));
  }

  @Test
  public void runDownNodeProbesSkipsDownNodeMissingFromDiscoveredRing() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"transient.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI transientNode = node("transient.local");

    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, transientNode);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setResponseBody("[\"other.local\"]");
    liveNodes.refreshDiscoveredNodes();
    httpClient.requests.clear();

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertTrue(httpClient.requests.isEmpty());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());
  }

  @Test
  public void runDownNodeProbesProbesDownSeedMissingFromDiscoveredRing() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]", 1);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .build(),
            httpClient,
            null,
            Arrays.asList("seed.local", "active.local"));
    URI seed = node("seed.local");

    liveNodes.refreshDiscoveredNodes();
    assertFalse(liveNodes.getDiscoveredNodes().contains(seed));
    markNodeDown(liveNodes, seed);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(seed).getState());
    httpClient.requests.clear();

    assertEquals(Arrays.asList(seed), liveNodes.runDownNodeProbes());

    assertEquals(1, httpClient.requests.size());
    assertEquals("seed.local", httpClient.requests.get(0).host());
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(seed).getState());
  }

  @Test
  public void duplicateDiscoveredDownNodeProbeCountsOncePerCycle() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"recovering.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(2)
                .build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI recovering = node("recovering.local");

    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, recovering);
    httpClient.requests.clear();

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());

    assertEquals(1, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());
    assertEquals(1, httpClient.requests.size());

    assertEquals(Arrays.asList(recovering), liveNodes.runDownNodeProbes());
  }

  @Test
  public void omittedActiveNodeDoesNotMakeDownDiscoveredNodeEligible() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"stale-active.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("seed.local"));
    URI seed = node("seed.local");
    URI current = node("current.local");

    liveNodes.refreshDiscoveredNodes();
    liveNodes.probeQuarantinedNodes();
    assertTrue(liveNodes.getActiveNodes().contains(node("stale-active.local")));

    httpClient.setResponseBody("[\"current.local\"]");
    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, seed);
    markNodeDown(liveNodes, current);

    assertTrue(liveNodes.getActiveNodes().isEmpty());
    NodeHealthQueryPlan plan = liveNodes.newRegularQueryPlan(new LazyQueryPlan(liveNodes));
    assertNull(plan.nextRouteCandidate());
  }

  @Test
  public void downNodeProbeRequiresHttpOk() {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient(403);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void clusterRefreshUsesDownNodeAsHealthNeutralControlPlaneFallback() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.builder().withDownNodeProbePeriodMs(1).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    httpClient.requests.clear();
    liveNodes.refreshDiscoveredNodes();

    assertEquals(2, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("recovering.local", httpClient.requests.get(1).host());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void backgroundDiscoveryDoesNotMarkClientActivity() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.getDefault(), httpClient, DatacenterScope.of("dc1", null));

    assertEquals(0, lastActivityTime(liveNodes).get());
    liveNodes.refreshDiscoveredNodes();

    assertEquals(0, lastActivityTime(liveNodes).get());
  }

  @Test
  public void scopedRefreshDoesNotChangeActiveThenQuarantineQueryOrder() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            httpClient,
            DatacenterScope.of("dc1", null));
    URI active = node("active.local");
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodes());
    NodeHealthQueryPlan firstPlan =
        liveNodes.newAffinityQueryPlan(
            new LazyQueryPlan(liveNodes, Arrays.asList(active, recovering)));
    assertEquals(active, firstPlan.nextRouteCandidate());
    NodeHealthQueryPlan secondPlan =
        liveNodes.newAffinityQueryPlan(
            new LazyQueryPlan(liveNodes, Arrays.asList(active, recovering)));
    assertEquals(active, secondPlan.nextRouteCandidate());
    assertEquals(recovering, secondPlan.nextRouteCandidate());
  }

  private static AlternatorLiveNodes liveNodes(
      NodeHealthConfig nodeHealthConfig, SdkHttpClient httpClient) {
    return liveNodes(nodeHealthConfig, httpClient, null);
  }

  private static AlternatorLiveNodes directLiveNodes(
      NodeHealthConfig nodeHealthConfig, SdkHttpClient httpClient) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("candidate.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .withNodeHealthConfig(nodeHealthConfig)
            .build();
    return new AlternatorLiveNodes(config, httpClient);
  }

  private static AlternatorLiveNodes liveNodes(
      NodeHealthConfig nodeHealthConfig, SdkHttpClient httpClient, RoutingScope routingScope) {
    return liveNodes(
        nodeHealthConfig,
        httpClient,
        routingScope,
        Arrays.asList("active.local", "recovering.local"));
  }

  private static AlternatorLiveNodes liveNodes(
      NodeHealthConfig nodeHealthConfig,
      SdkHttpClient httpClient,
      RoutingScope routingScope,
      List<String> seedHosts) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(seedHosts)
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .withRoutingScope(routingScope)
            .withNodeHealthConfig(nodeHealthConfig)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    for (String seedHost : seedHosts) {
      liveNodes.reportNodeResult(node(seedHost), NodeHealthObservation.PROBE_SUCCESS, false);
    }
    return liveNodes;
  }

  private static long hashWhereFirstNodeIs(List<URI> nodes, URI expected) {
    for (long hash = -1000; hash < 1000; hash++) {
      if (expected.equals(AlternatorLiveNodes.firstNodeWithSeed(nodes, hash))) {
        return hash;
      }
    }
    throw new AssertionError("Could not find hash for " + expected);
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static void markNodeDown(AlternatorLiveNodes liveNodes, URI node) {
    for (int i = 0; i < NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD; i++) {
      reportCurrentTraffic(liveNodes, node, NodeHealthObservation.TRAFFIC_FAILURE);
    }
  }

  private static void reportCurrentTraffic(
      AlternatorLiveNodes liveNodes, URI node, NodeHealthObservation observation) {
    liveNodes.reportNodeResult(node, observation, liveNodes.getNodeHealthGeneration(node));
  }

  private static List<URI> collectNodes(LazyQueryPlan plan) {
    List<URI> nodes = new java.util.ArrayList<>();
    while (plan.hasNext()) {
      nodes.add(plan.next());
    }
    return nodes;
  }

  private static AtomicLong lastActivityTime(AlternatorLiveNodes liveNodes) throws Exception {
    Field field = AlternatorLiveNodes.class.getDeclaredField("lastActivityTime");
    field.setAccessible(true);
    return (AtomicLong) field.get(liveNodes);
  }

  private static void publishDiscoveredNodes(AlternatorLiveNodes liveNodes, List<URI> nodes)
      throws Exception {
    Method method = AlternatorLiveNodes.class.getDeclaredMethod("setDiscoveredNodes", List.class);
    method.setAccessible(true);
    method.invoke(liveNodes, nodes);
  }

  private static final class LocalNodesHttpClient implements SdkHttpClient {
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();
    private volatile String responseBody;
    private final String failingHost;
    private final int statusCode;
    private final AtomicLong requestsToFail;

    private LocalNodesHttpClient() {
      this("[]");
    }

    private LocalNodesHttpClient(int statusCode) {
      this("[]", null, statusCode);
    }

    private LocalNodesHttpClient(String responseBody) {
      this(responseBody, null);
    }

    private LocalNodesHttpClient(String responseBody, String failingHost) {
      this(responseBody, failingHost, 200);
    }

    private LocalNodesHttpClient(String responseBody, int requestsToFail) {
      this(responseBody, null, 200, requestsToFail);
    }

    private LocalNodesHttpClient(String responseBody, String failingHost, int statusCode) {
      this(responseBody, failingHost, statusCode, 0);
    }

    private LocalNodesHttpClient(
        String responseBody, String failingHost, int statusCode, long requestsToFail) {
      this.responseBody = responseBody;
      this.failingHost = failingHost;
      this.statusCode = statusCode;
      this.requestsToFail = new AtomicLong(requestsToFail);
    }

    private void setResponseBody(String responseBody) {
      this.responseBody = responseBody;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      requests.add(httpRequest);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (requestsToFail.getAndUpdate(value -> value > 0 ? value - 1 : value) > 0) {
            throw new IOException("simulated connection failure");
          }
          if (httpRequest.host().equals(failingHost)) {
            throw new IOException("simulated connection failure for " + failingHost);
          }
          HttpExecuteResponse.Builder response =
              HttpExecuteResponse.builder()
                  .response(SdkHttpFullResponse.builder().statusCode(statusCode).build());
          if (responseBody != null) {
            byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
            response.responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)));
          }
          return response.build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "localnodes";
    }
  }

  private static final class BlockingLocalNodesHttpClient implements SdkHttpClient {
    private final String responseBody;
    private final long blockedCallNumber;
    private final AtomicLong callNumber = new AtomicLong();
    private final CountDownLatch blockedCallStarted = new CountDownLatch(1);
    private final CountDownLatch releaseBlockedCall = new CountDownLatch(1);

    private BlockingLocalNodesHttpClient(String responseBody, long blockedCallNumber) {
      this.responseBody = responseBody;
      this.blockedCallNumber = blockedCallNumber;
    }

    private boolean awaitBlockedCall() throws InterruptedException {
      return blockedCallStarted.await(5, TimeUnit.SECONDS);
    }

    private void releaseBlockedCall() {
      releaseBlockedCall.countDown();
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (callNumber.incrementAndGet() == blockedCallNumber) {
            blockedCallStarted.countDown();
            try {
              if (!releaseBlockedCall.await(5, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting to release blocked request");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("interrupted waiting to release blocked request", e);
            }
          }
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {
          releaseBlockedCall.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "blocking-localnodes";
    }
  }

  private static final class SecondRequestFailureHttpClient implements SdkHttpClient {
    private final AtomicLong callNumber = new AtomicLong();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (callNumber.incrementAndGet() == 2) {
            throw new IOException("simulated second-request failure");
          }
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
      return "second-request-failure";
    }
  }

  private static final class RuntimeFailureHttpClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          throw new IllegalStateException("simulated runtime transport failure");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "runtime-failure";
    }
  }
}
