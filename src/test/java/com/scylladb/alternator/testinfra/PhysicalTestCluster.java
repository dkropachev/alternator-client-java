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
package com.scylladb.alternator.testinfra;

import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder;
import com.scylladb.alternator.TlsConfig;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** Mutable physical cluster hidden behind read-only or private lease views. */
final class PhysicalTestCluster implements TestClusterInfo {
  private enum LifecycleState {
    OPEN,
    REMOVING,
    REMOVAL_FAILED,
    CLOSED
  }

  private enum NodeState {
    RUNNING,
    STOPPED,
    DECOMMISSIONED
  }

  private final CcmProvisioner provisioner;
  private final String instanceId;
  private final int ccmId;
  private final Path ccmDirectory;
  private final ClusterSpec spec;
  private final List<TestClusterNode> nodes;
  private final Map<TestClusterNode, NodeState> nodeStates = new IdentityHashMap<>();
  private final Path caCertificatePath;
  private final AwsCredentialsProvider credentials;
  private LifecycleState lifecycleState = LifecycleState.OPEN;
  private boolean dirty;
  private boolean recoveryRequired;

  PhysicalTestCluster(
      CcmProvisioner provisioner,
      String instanceId,
      int ccmId,
      Path ccmDirectory,
      ClusterSpec spec,
      List<TestClusterNode> nodes,
      Path caCertificatePath,
      AwsCredentialsProvider credentials) {
    this.provisioner = provisioner;
    this.instanceId = instanceId;
    this.ccmId = ccmId;
    this.ccmDirectory = ccmDirectory;
    this.spec = spec;
    this.nodes = new ArrayList<>(nodes);
    for (TestClusterNode node : nodes) {
      nodeStates.put(node, NodeState.RUNNING);
    }
    this.caCertificatePath = caCertificatePath;
    this.credentials = credentials;
  }

  @Override
  public String instanceId() {
    return instanceId;
  }

  @Override
  public ClusterSpec spec() {
    return spec;
  }

  @Override
  public synchronized List<TestClusterNode> nodes() {
    return Collections.unmodifiableList(new ArrayList<>(nodes));
  }

  int ccmId() {
    return ccmId;
  }

  Path ccmDirectory() {
    return ccmDirectory;
  }

  Path caCertificatePath() {
    return caCertificatePath;
  }

  @Override
  public synchronized AlternatorConnection connection(AlternatorTransport transport) {
    if (!spec.transports().contains(transport)) {
      throw new IllegalStateException("Cluster '" + instanceId + "' does not provide " + transport);
    }
    String scheme = transport == AlternatorTransport.HTTP ? "http" : "https";
    int port =
        transport == AlternatorTransport.HTTP
            ? CcmProvisioner.HTTP_PORT
            : CcmProvisioner.HTTPS_PORT;
    List<URI> endpoints = new ArrayList<>();
    for (TestClusterNode node : nodes) {
      endpoints.add(URI.create(scheme + "://" + node.address() + ":" + port));
    }
    return new AlternatorConnection(
        endpoints.get(0),
        endpoints,
        credentials,
        transport == AlternatorTransport.HTTPS ? caCertificatePath : null);
  }

  @Override
  public AlternatorDynamoDbClientBuilder clientBuilder(AlternatorTransport transport) {
    AlternatorConnection connection = connection(transport);
    AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder().endpointOverride(connection.seedEndpoint());
    if (connection.credentials() != null) {
      builder.credentialsProvider(connection.credentials());
    }
    if (transport == AlternatorTransport.HTTPS && connection.caCertificatePath() != null) {
      builder.withTlsConfig(
          TlsConfig.builder()
              .withCaCertPath(connection.caCertificatePath())
              .withTrustSystemCaCerts(false)
              .build());
    }
    return builder;
  }

  synchronized void start() throws Exception {
    ensureMutable();
    List<TestClusterNode> stopped = new ArrayList<>();
    for (TestClusterNode node : nodes) {
      if (nodeStates.get(node) == NodeState.DECOMMISSIONED) {
        continue;
      }
      if (probeNodeRunning(node)) {
        provisioner.waitForNodeReady(this, node);
        nodeStates.put(node, NodeState.RUNNING);
      } else {
        nodeStates.put(node, NodeState.STOPPED);
        stopped.add(node);
      }
    }
    if (stopped.isEmpty()) {
      return;
    }
    try {
      provisioner.start(this, stopped);
      for (TestClusterNode node : stopped) {
        nodeStates.put(node, NodeState.RUNNING);
      }
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  synchronized void stop() throws Exception {
    ensureMutable();
    boolean hasRunningNode = false;
    for (TestClusterNode node : nodes) {
      NodeState cachedState = nodeStates.get(node);
      if (cachedState == NodeState.DECOMMISSIONED) {
        continue;
      }
      if (probeNodeRunning(node)) {
        nodeStates.put(node, NodeState.RUNNING);
        hasRunningNode = true;
      } else {
        nodeStates.put(node, NodeState.STOPPED);
        hasRunningNode |= cachedState == NodeState.RUNNING;
      }
    }
    if (!hasRunningNode) {
      return;
    }
    try {
      provisioner.stop(this);
      for (TestClusterNode node : nodes) {
        if (nodeStates.get(node) != NodeState.DECOMMISSIONED) {
          nodeStates.put(node, NodeState.STOPPED);
        }
      }
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  synchronized void startNode(TestClusterNode requested) throws Exception {
    ensureMutable();
    TestClusterNode node = getNode(requested);
    NodeState state = nodeStates.get(node);
    if (state == NodeState.DECOMMISSIONED) {
      throw new IllegalStateException("Cannot start decommissioned node " + node.name());
    }
    if (probeNodeRunning(node)) {
      provisioner.waitForNodeReady(this, node);
      nodeStates.put(node, NodeState.RUNNING);
      return;
    }
    nodeStates.put(node, NodeState.STOPPED);
    try {
      provisioner.startNode(this, node);
      nodeStates.put(node, NodeState.RUNNING);
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  synchronized void stopNode(TestClusterNode requested) throws Exception {
    ensureMutable();
    TestClusterNode node = getNode(requested);
    NodeState state = nodeStates.get(node);
    if (state == NodeState.DECOMMISSIONED) {
      throw new IllegalStateException("Cannot stop decommissioned node " + node.name());
    }
    if (!probeNodeRunning(node) && state == NodeState.STOPPED) {
      nodeStates.put(node, NodeState.STOPPED);
      return;
    }
    nodeStates.put(node, NodeState.RUNNING);
    try {
      provisioner.stopNode(this, node);
      nodeStates.put(node, NodeState.STOPPED);
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  synchronized TestClusterNode addNode(String datacenter, String rack) throws Exception {
    ensureMutable();
    try {
      TestClusterNode node = provisioner.addNode(this, datacenter, rack);
      nodes.add(node);
      nodeStates.put(node, NodeState.RUNNING);
      return node;
    } catch (CcmProvisioner.CcmNodeProvisioningException exception) {
      if (CcmProvisioner.requiresNextRunRecovery(exception)) {
        recoveryRequired = true;
        dirty = true;
      }
      if (exception.clusterStateAmbiguous()) {
        dirty = true;
      }
      if (exception.nodeRemainsProvisioned()) {
        TestClusterNode node = exception.node();
        nodes.add(node);
        nodeStates.put(node, NodeState.STOPPED);
        dirty = true;
      }
      throw exception;
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  synchronized TestClusterNode addNode(TestClusterPool pool, String datacenter, String rack)
      throws Exception {
    ensureMutable();
    pool.reserveAdditionalPrivateNode(this);
    return addNode(datacenter, rack);
  }

  synchronized void removeNode(TestClusterNode requested) throws Exception {
    ensureMutable();
    TestClusterNode node = getNode(requested);
    if (nodeStates.get(node) != NodeState.DECOMMISSIONED && activeNodeCount() == 1) {
      throw new IllegalStateException("Cannot remove the final node from a cluster");
    }
    try {
      if (nodeStates.get(node) == NodeState.STOPPED) {
        provisioner.startNode(this, node);
        nodeStates.put(node, NodeState.RUNNING);
      }
      if (nodeStates.get(node) != NodeState.DECOMMISSIONED) {
        provisioner.decommissionNode(this, node);
        nodeStates.put(node, NodeState.DECOMMISSIONED);
      }
      provisioner.deleteNodeState(this, node);
      nodes.remove(node);
      nodeStates.remove(node);
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  synchronized void removeNode(TestClusterPool pool, TestClusterNode node) throws Exception {
    removeNode(node);
    pool.releaseAdditionalPrivateNode(this);
  }

  synchronized void markDirty() {
    dirty = true;
  }

  synchronized void markRecoveryRequired() {
    dirty = true;
    recoveryRequired = true;
  }

  synchronized boolean isDirty() {
    return dirty;
  }

  /** Whole-cluster removal is the sole operation allowed after an ambiguous command failure. */
  synchronized void removePhysical() throws Exception {
    if (lifecycleState == LifecycleState.CLOSED) {
      return;
    }
    if (recoveryRequired) {
      throw new IllegalStateException(
          "Cluster '" + instanceId + "' must be recovered by the next test JVM");
    }
    if (lifecycleState == LifecycleState.REMOVING) {
      throw new IllegalStateException("Cluster removal is already in progress");
    }
    lifecycleState = LifecycleState.REMOVING;
    try {
      provisioner.remove(this);
      lifecycleState = LifecycleState.CLOSED;
    } catch (Exception exception) {
      lifecycleState = LifecycleState.REMOVAL_FAILED;
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  private void recordAmbiguousFailure(Throwable failure) {
    dirty = true;
    recoveryRequired |= CcmProvisioner.requiresNextRunRecovery(failure);
  }

  private boolean probeNodeRunning(TestClusterNode node) throws Exception {
    try {
      return provisioner.isNodeRunning(this, node);
    } catch (Exception exception) {
      recordAmbiguousFailure(exception);
      throw exception;
    }
  }

  private TestClusterNode getNode(TestClusterNode requested) {
    for (TestClusterNode node : nodes) {
      if (node == requested) {
        return node;
      }
    }
    throw new IllegalArgumentException("Node is not part of cluster: " + requested.name());
  }

  private int activeNodeCount() {
    int count = 0;
    for (NodeState state : nodeStates.values()) {
      if (state != NodeState.DECOMMISSIONED) {
        count++;
      }
    }
    return count;
  }

  private void ensureMutable() {
    if (lifecycleState != LifecycleState.OPEN) {
      throw new IllegalStateException(
          "Cluster '" + instanceId + "' is closing or has already been removed");
    }
    if (dirty) {
      throw new IllegalStateException(
          "Cluster '" + instanceId + "' has ambiguous state and must be removed");
    }
  }
}
