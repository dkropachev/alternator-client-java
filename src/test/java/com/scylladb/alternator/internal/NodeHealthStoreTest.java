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

import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class NodeHealthStoreTest {
  @Test
  public void failuresMarkNodeDownAfterConfiguredThreshold() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    assertEquals(Arrays.asList(node), store.getActiveNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getActiveNodes().isEmpty());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void httpDefaultPortReportMatchesNodeWithoutExplicitPort() {
    URI configured = URI.create("http://node1.local");
    URI reported = URI.create("http://node1.local:80");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(configured));

    store.reportNodeResult(reported, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(configured), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(configured).getState());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(reported).getState());
  }

  @Test
  public void httpsDefaultPortReportMatchesNodeWithoutExplicitPort() {
    URI configured = URI.create("https://node1.local");
    URI reported = URI.create("https://node1.local:443");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(configured));

    store.reportNodeResult(reported, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(configured), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(configured).getState());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(reported).getState());
  }

  @Test
  public void endpointIdentityIgnoresCaseAndNonNetworkUriComponents() {
    URI configured = URI.create("HTTP://user@NODE1.LOCAL:80/configured/path?query=value#fragment");
    URI reported = URI.create("http://node1.local/other/path?different=query#other-fragment");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(configured));

    store.reportNodeResult(reported, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(configured), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(configured).getState());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(reported).getState());
  }

  @Test
  public void endpointIdentityKeepsSchemesAndNonDefaultPortsDistinct() {
    URI http = URI.create("http://node1.local:8080/path");
    URI https = URI.create("https://node1.local:8080/path");
    URI otherPort = URI.create("http://node1.local:8081/path");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(http, https, otherPort));

    store.reportNodeResult(
        URI.create("http://node1.local:8080/other"), NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(http).getState());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(https).getState());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(otherPort).getState());
  }

  @Test
  public void newlyAddedNodeCanStartQuarantinedAndDirectProbePromotesIt() {
    URI node = node("node1.local");
    NodeHealthStore store =
        new NodeHealthStore(NodeHealthConfig.getDefault(), Collections.emptyList());

    store.addQuarantinedNode(node);

    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(Arrays.asList(node), store.getActiveNodes());
  }

  @Test
  public void consecutiveFailuresMarkNodeDown() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void downNodeMovesToQuarantineAfterConfiguredRecoverySuccesses() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .withQuarantineSuccessThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    assertEquals(Arrays.asList(node), store.getDownNodes());

    assertEquals(
        Collections.emptyList(),
        store.probeDownNodes(
            (uri, status) ->
                status.getState() == NodeHealthState.DOWN
                    ? NodeHealthObservation.PROBE_SUCCESS
                    : NodeHealthObservation.PROBE_FAILURE));

    assertEquals(Arrays.asList(node), store.getDownNodes());
    NodeHealthStatus firstSuccessStatus = store.getNodeStatus(node);
    assertEquals(NodeHealthState.DOWN, firstSuccessStatus.getState());
    assertEquals(1, firstSuccessStatus.getConsecutiveSuccesses());

    assertEquals(
        Arrays.asList(node),
        store.probeDownNodes(
            (uri, status) ->
                status.getState() == NodeHealthState.DOWN
                    ? NodeHealthObservation.PROBE_SUCCESS
                    : NodeHealthObservation.PROBE_FAILURE));

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertTrue(store.getDownNodes().isEmpty());
    NodeHealthStatus status = store.getNodeStatus(node);
    assertEquals(NodeHealthState.QUARANTINED, status.getState());
    assertEquals(0, status.getConsecutiveSuccesses());
  }

  @Test
  public void downNodeProbeCountsDuplicateCandidatesOncePerCycle() {
    URI node = URI.create("http://node1.local");
    URI nodeWithDefaultPort = URI.create("http://node1.local:80");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));
    AtomicInteger probes = new AtomicInteger();

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(
        store
            .probeDownNodes(
                Arrays.asList(node, nodeWithDefaultPort, node),
                (uri, status) -> {
                  probes.incrementAndGet();
                  return NodeHealthObservation.PROBE_SUCCESS;
                })
            .isEmpty());

    assertEquals(1, probes.get());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void probeSuccessDoesNotClearActiveTrafficFailures() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);

    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void probeOutcomesDoNotAffectActiveNodeHealth() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveFailures());
    assertTrue(store.getDownNodes().isEmpty());
  }

  @Test
  public void downTrafficSuccessDoesNotMoveNodeToQuarantine() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .withQuarantineSuccessThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    NodeHealthStatus status = store.getNodeStatus(node);
    assertEquals(NodeHealthState.DOWN, status.getState());
    assertEquals(1, status.getConsecutiveFailures());
    assertEquals(0, status.getConsecutiveSuccesses());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
  }

  @Test
  public void downIgnoresTrafficAndProbeFailureResetsRecoveryProgress() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);

    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
  }

  @Test
  public void quarantinePromotesToActiveAfterConfiguredTrafficSuccesses() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void downNodeDoesNotPromoteImmediatelyWhenQuarantineThresholdIsOne() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void trafficFromGenerationBeforeDownIsIgnoredAfterRecovery() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(1)
            .withQuarantineFailureThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));
    long staleGeneration = store.getNodeStatus(node).getGeneration();

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());

    assertFalse(
        store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS, staleGeneration));
    assertFalse(
        store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE, staleGeneration));
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveSuccesses());

    long currentGeneration = store.getNodeStatus(node).getGeneration();
    assertTrue(
        store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS, currentGeneration));
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
  }

  @Test
  public void probeSuccessPromotesQuarantinedNode() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(3)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void quarantineFailuresResetPromotionProgressAndReachConfiguredDownThreshold() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(3)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .withQuarantineFailureThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveSuccesses());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void quarantineProbeFailureDoesNotAffectTrafficCounters() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .withQuarantineFailureThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);

    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);

    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void disabledKeepsAllNodesActive() {
    URI node = node("node1.local");
    NodeHealthStore store = new NodeHealthStore(NodeHealthConfig.disabled(), Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(
        Collections.emptyList(),
        store.probeDownNodes((uri, status) -> NodeHealthObservation.PROBE_SUCCESS));
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static void reportFailures(NodeHealthStore store, URI node, int count) {
    for (int i = 0; i < count; i++) {
      store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    }
  }
}
