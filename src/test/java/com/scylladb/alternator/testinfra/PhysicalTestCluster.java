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
final class PhysicalTestCluster implements TestClusterInfo, PrivateClusterControl {
  private enum LifecycleState {
    OPEN,
    CLOSING,
    REMOVAL_FAILED,
    CLOSED
  }

  private enum NodeState {
    RUNNING,
    RUNNING_UNVERIFIED,
    STOPPED,
    DECOMMISSIONED,
    UNKNOWN
  }

  private enum AmbiguousOperation {
    START,
    STOP,
    DECOMMISSION
  }

  private final CcmProvisioner provisioner;
  private final String instanceId;
  private final int ccmId;
  private final Path ccmDirectory;
  private final ClusterSpec spec;
  private final List<TestClusterNode> nodes;
  private final Map<TestClusterNode, NodeState> nodeStates = new IdentityHashMap<>();
  private final Map<TestClusterNode, AmbiguousOperation> ambiguousOperations =
      new IdentityHashMap<>();
  private final Path caCertificatePath;
  private final AwsCredentialsProvider credentials;
  private LifecycleState lifecycleState = LifecycleState.OPEN;

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

  @Override
  public synchronized void start() throws Exception {
    ensureOpen();
    boolean requiresStart = false;
    boolean hasRetainedDecommissionedNode = false;
    for (TestClusterNode node : nodes) {
      resolveAmbiguousState(node);
      if (nodeStates.get(node) == NodeState.DECOMMISSIONED) {
        hasRetainedDecommissionedNode = true;
        continue;
      }
      if (provisioner.isNodeRunning(this, node)) {
        try {
          provisioner.waitForNodeReady(this, node);
          setNodeState(node, NodeState.RUNNING);
        } catch (Exception exception) {
          setNodeState(node, NodeState.RUNNING_UNVERIFIED);
          throw exception;
        }
      } else {
        setNodeState(node, NodeState.STOPPED);
        requiresStart = true;
      }
    }
    if (!requiresStart) {
      return;
    }
    if (hasRetainedDecommissionedNode) {
      // A decommissioned node can remain in CCM metadata when deleting its state failed. Using
      // the cluster-wide start command would resurrect it, so start active nodes individually.
      for (TestClusterNode node : nodes) {
        if (nodeStates.get(node) != NodeState.DECOMMISSIONED) {
          ensureNodeRunning(node);
        }
      }
      return;
    }
    for (TestClusterNode node : nodes) {
      markUnknown(node, AmbiguousOperation.START);
    }
    try {
      provisioner.start(this);
      setNonDecommissionedNodes(NodeState.RUNNING);
    } catch (Exception exception) {
      reconcileAllRunningStates(exception, NodeState.RUNNING_UNVERIFIED);
      throw exception;
    }
  }

  @Override
  public synchronized void stop() throws Exception {
    ensureOpen();
    boolean requiresStop = false;
    for (TestClusterNode node : nodes) {
      resolveAmbiguousState(node);
      if (nodeStates.get(node) == NodeState.DECOMMISSIONED) {
        continue;
      }
      if (provisioner.isNodeRunning(this, node)) {
        setNodeState(node, NodeState.RUNNING);
        requiresStop = true;
      } else {
        setNodeState(node, NodeState.STOPPED);
      }
    }
    if (!requiresStop) {
      return;
    }
    for (TestClusterNode node : nodes) {
      if (nodeStates.get(node) != NodeState.DECOMMISSIONED) {
        markUnknown(node, AmbiguousOperation.STOP);
      }
    }
    try {
      provisioner.stop(this);
      setNonDecommissionedNodes(NodeState.STOPPED);
    } catch (Exception exception) {
      reconcileAllRunningStates(exception, NodeState.RUNNING);
      throw exception;
    }
  }

  @Override
  public synchronized void startNode(TestClusterNode node) throws Exception {
    ensureOpen();
    ensureNodeRunning(getNode(node));
  }

  @Override
  public synchronized void stopNode(TestClusterNode node) throws Exception {
    ensureOpen();
    TestClusterNode existing = getNode(node);
    resolveAmbiguousState(existing);
    NodeState state = nodeStates.get(existing);
    if (state == NodeState.DECOMMISSIONED) {
      throw new IllegalStateException("Cannot stop decommissioned node " + existing.name());
    }
    if (!provisioner.isNodeRunning(this, existing)) {
      setNodeState(existing, NodeState.STOPPED);
      return;
    }
    setNodeState(existing, NodeState.RUNNING);
    markUnknown(existing, AmbiguousOperation.STOP);
    try {
      provisioner.stopNode(this, existing);
      setNodeState(existing, NodeState.STOPPED);
    } catch (Exception exception) {
      reconcileRunningState(existing, exception, NodeState.RUNNING);
      throw exception;
    }
  }

  @Override
  public synchronized TestClusterNode addNode(String datacenter, String rack) throws Exception {
    ensureOpen();
    try {
      TestClusterNode node = provisioner.addNode(this, datacenter, rack);
      nodes.add(node);
      setNodeState(node, NodeState.RUNNING);
      return node;
    } catch (CcmProvisioner.CcmNodeProvisioningException exception) {
      if (exception.nodeRemainsProvisioned()) {
        TestClusterNode node = exception.node();
        nodes.add(node);
        markUnknown(node, AmbiguousOperation.START);
        reconcileRunningState(node, exception, NodeState.RUNNING_UNVERIFIED);
      }
      throw exception;
    }
  }

  synchronized TestClusterNode addNode(TestClusterPool pool, String datacenter, String rack)
      throws Exception {
    ensureOpen();
    pool.reserveAdditionalPrivateNode(this);
    try {
      return addNode(datacenter, rack);
    } catch (CcmProvisioner.CcmNodeProvisioningException exception) {
      if (!exception.nodeRemainsProvisioned()) {
        pool.releaseAdditionalPrivateNode(this);
      }
      throw exception;
    } catch (Exception exception) {
      pool.releaseAdditionalPrivateNode(this);
      throw exception;
    }
  }

  @Override
  public synchronized void removeNode(TestClusterNode node) throws Exception {
    ensureOpen();
    TestClusterNode existing = getNode(node);
    for (TestClusterNode current : nodes) {
      resolveAmbiguousState(current);
    }
    if (nodeStates.get(existing) != NodeState.DECOMMISSIONED && nonDecommissionedNodeCount() == 1) {
      throw new IllegalStateException("Cannot remove the final node from a cluster");
    }
    if (nodeStates.get(existing) != NodeState.DECOMMISSIONED) {
      ensureNodeProcessRunning(existing);
      markUnknown(existing, AmbiguousOperation.DECOMMISSION);
      try {
        provisioner.decommissionNode(this, existing);
        setNodeState(existing, NodeState.DECOMMISSIONED);
      } catch (Exception exception) {
        if (!reconcileDecommissionState(existing, exception)) {
          throw exception;
        }
      }
    }
    provisioner.deleteNodeState(this, existing);
    nodes.remove(existing);
    nodeStates.remove(existing);
    ambiguousOperations.remove(existing);
  }

  synchronized void removeNode(TestClusterPool pool, TestClusterNode node) throws Exception {
    removeNode(node);
    pool.releaseAdditionalPrivateNode(this);
  }

  private TestClusterNode getNode(TestClusterNode requested) {
    for (TestClusterNode node : nodes) {
      if (node == requested) {
        return node;
      }
    }
    throw new IllegalArgumentException("Node is not part of cluster: " + requested.name());
  }

  /** Serializes whole-cluster removal with every private control operation. */
  synchronized void removePhysical() throws Exception {
    if (lifecycleState == LifecycleState.CLOSED) {
      return;
    }
    if (lifecycleState == LifecycleState.CLOSING) {
      throw new IllegalStateException("Cluster removal is already in progress");
    }
    lifecycleState = LifecycleState.CLOSING;
    try {
      provisioner.remove(this);
      lifecycleState = LifecycleState.CLOSED;
    } catch (Exception exception) {
      lifecycleState = LifecycleState.REMOVAL_FAILED;
      throw exception;
    }
  }

  private void ensureOpen() {
    if (lifecycleState != LifecycleState.OPEN) {
      throw new IllegalStateException(
          "Cluster '" + instanceId + "' is closing or has already been removed");
    }
  }

  private void markUnknown(TestClusterNode node, AmbiguousOperation operation) {
    nodeStates.put(node, NodeState.UNKNOWN);
    ambiguousOperations.put(node, operation);
  }

  private void setNodeState(TestClusterNode node, NodeState state) {
    nodeStates.put(node, state);
    ambiguousOperations.remove(node);
  }

  private void setNonDecommissionedNodes(NodeState state) {
    for (TestClusterNode node : nodes) {
      if (nodeStates.get(node) != NodeState.DECOMMISSIONED) {
        setNodeState(node, state);
      }
    }
  }

  private int nonDecommissionedNodeCount() {
    int count = 0;
    for (TestClusterNode node : nodes) {
      if (nodeStates.get(node) != NodeState.DECOMMISSIONED) {
        count++;
      }
    }
    return count;
  }

  private void reconcileAllRunningStates(Exception failure, NodeState runningState) {
    for (TestClusterNode node : nodes) {
      if (nodeStates.get(node) == NodeState.UNKNOWN) {
        reconcileRunningState(node, failure, runningState);
      }
    }
  }

  private void reconcileRunningState(
      TestClusterNode node, Exception failure, NodeState runningState) {
    try {
      setNodeState(node, provisioner.isNodeRunning(this, node) ? runningState : NodeState.STOPPED);
    } catch (Exception probeFailure) {
      failure.addSuppressed(probeFailure);
    }
  }

  private boolean reconcileDecommissionState(TestClusterNode node, Exception failure) {
    try {
      if (provisioner.isNodeDecommissioned(this, node)) {
        setNodeState(node, NodeState.DECOMMISSIONED);
        return true;
      }
      reconcileRunningState(node, failure, NodeState.RUNNING);
      return false;
    } catch (Exception probeFailure) {
      failure.addSuppressed(probeFailure);
      return false;
    }
  }

  private void resolveAmbiguousState(TestClusterNode node) throws Exception {
    if (nodeStates.get(node) != NodeState.UNKNOWN) {
      return;
    }
    AmbiguousOperation operation = ambiguousOperations.get(node);
    if (operation == AmbiguousOperation.DECOMMISSION) {
      if (provisioner.isNodeDecommissioned(this, node)) {
        setNodeState(node, NodeState.DECOMMISSIONED);
      } else {
        setNodeState(
            node, provisioner.isNodeRunning(this, node) ? NodeState.RUNNING : NodeState.STOPPED);
      }
    } else {
      setNodeState(
          node,
          provisioner.isNodeRunning(this, node)
              ? operation == AmbiguousOperation.START
                  ? NodeState.RUNNING_UNVERIFIED
                  : NodeState.RUNNING
              : NodeState.STOPPED);
    }
  }

  private void ensureNodeRunning(TestClusterNode node) throws Exception {
    resolveAmbiguousState(node);
    NodeState state = nodeStates.get(node);
    if (state == NodeState.DECOMMISSIONED) {
      throw new IllegalStateException("Cannot start decommissioned node " + node.name());
    }
    if (provisioner.isNodeRunning(this, node)) {
      try {
        provisioner.waitForNodeReady(this, node);
        setNodeState(node, NodeState.RUNNING);
        return;
      } catch (Exception exception) {
        setNodeState(node, NodeState.RUNNING_UNVERIFIED);
        throw exception;
      }
    }
    setNodeState(node, NodeState.STOPPED);
    markUnknown(node, AmbiguousOperation.START);
    try {
      provisioner.startNode(this, node);
      setNodeState(node, NodeState.RUNNING);
    } catch (Exception exception) {
      reconcileRunningState(node, exception, NodeState.RUNNING_UNVERIFIED);
      throw exception;
    }
  }

  private void ensureNodeProcessRunning(TestClusterNode node) throws Exception {
    resolveAmbiguousState(node);
    if (nodeStates.get(node) == NodeState.DECOMMISSIONED) {
      throw new IllegalStateException("Cannot start decommissioned node " + node.name());
    }
    if (provisioner.isNodeRunning(this, node)) {
      setNodeState(node, NodeState.RUNNING);
      return;
    }
    setNodeState(node, NodeState.STOPPED);
    markUnknown(node, AmbiguousOperation.START);
    try {
      provisioner.startNode(this, node);
      setNodeState(node, NodeState.RUNNING);
    } catch (Exception exception) {
      reconcileRunningState(node, exception, NodeState.RUNNING);
      if (nodeStates.get(node) != NodeState.RUNNING) {
        throw exception;
      }
    }
  }
}
