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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/** Failure and concurrency tests for physical-cluster ownership. */
public class ClusterLifecycleTest {
  @Test
  public void nodeHandlesUseIdentityAndFinalNodeCannotBeRemoved() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster single = provisioner.newCluster(oneNodeSpec("single"), "single", 1);

    assertThrows(IllegalStateException.class, () -> single.removeNode(single.nodes().get(0)));
    assertEquals(0, provisioner.decommissionCount.get());
    assertEquals(0, provisioner.deleteNodeCount.get());

    PhysicalTestCluster cluster = provisioner.newCluster(twoNodeSpec("identity"), "identity", 2);
    TestClusterNode original = cluster.nodes().get(1);
    TestClusterNode foreign =
        new TestClusterNode(
            original.name(), original.address(), original.datacenter(), original.rack());
    assertThrows(IllegalArgumentException.class, () -> cluster.removeNode(foreign));

    cluster.removeNode(original);
    TestClusterNode replacement = cluster.addNode("dc1", "RAC1");
    assertEquals(original.name(), replacement.name());
    assertNotSame(original, replacement);
    assertThrows(IllegalArgumentException.class, () -> cluster.removeNode(original));
  }

  @Test
  public void ambiguousStartIsProbedBeforeItCanBeRepeated() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster cluster = provisioner.newCluster(oneNodeSpec("start"), "start", 3);
    TestClusterNode node = cluster.nodes().get(0);
    cluster.stopNode(node);
    provisioner.failStartAfterEffect.set(true);
    provisioner.nodeReady.set(false);

    assertThrows(IllegalStateException.class, () -> cluster.startNode(node));
    assertThrows(IllegalStateException.class, () -> cluster.startNode(node));
    provisioner.nodeReady.set(true);
    cluster.startNode(node);

    assertEquals(1, provisioner.startNodeCount.get());
    assertEquals(5, provisioner.runningProbeCount.get());
    assertEquals(2, provisioner.readinessWaitCount.get());
  }

  @Test
  public void nodeControlsRefreshCachedStateBeforeNoOp() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster cluster = provisioner.newCluster(oneNodeSpec("drift"), "drift", 6);
    TestClusterNode node = cluster.nodes().get(0);

    provisioner.running.remove(node);
    cluster.startNode(node);
    assertEquals(1, provisioner.startNodeCount.get());

    cluster.stopNode(node);
    provisioner.running.add(node);
    cluster.stopNode(node);
    assertEquals(2, provisioner.stopNodeCount.get());
  }

  @Test
  public void ambiguouslyCompletedDecommissionContinuesWithStateDeletion() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.failDecommissionAfterEffect.set(true);
    PhysicalTestCluster cluster =
        provisioner.newCluster(twoNodeSpec("decommission"), "decommission", 4);

    cluster.removeNode(cluster.nodes().get(1));

    assertEquals(1, provisioner.decommissionCount.get());
    assertEquals(1, provisioner.decommissionProbeCount.get());
    assertEquals(1, provisioner.deleteNodeCount.get());
    assertEquals(1, cluster.nodes().size());
  }

  @Test
  public void decommissionedNodeAwaitingDeletionDoesNotCountAsAReplacement() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.failNextDelete.set(true);
    PhysicalTestCluster cluster =
        provisioner.newCluster(twoNodeSpec("logical-count"), "logical-count", 5);
    TestClusterNode first = cluster.nodes().get(0);
    TestClusterNode second = cluster.nodes().get(1);

    assertThrows(IllegalStateException.class, () -> cluster.removeNode(second));
    assertThrows(IllegalStateException.class, () -> cluster.removeNode(first));

    assertEquals(1, provisioner.decommissionCount.get());
  }

  @Test
  public void unhealthyAlternatorEndpointDoesNotPreventDecommission() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.nodeReady.set(false);
    PhysicalTestCluster cluster = provisioner.newCluster(twoNodeSpec("unhealthy"), "unhealthy", 7);

    cluster.removeNode(cluster.nodes().get(1));

    assertEquals(1, provisioner.decommissionCount.get());
    assertEquals(0, provisioner.readinessWaitCount.get());
  }

  @Test
  public void clusterStartDoesNotResurrectADecommissionedNodeAwaitingDeletion() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.failNextDelete.set(true);
    PhysicalTestCluster cluster = provisioner.newCluster(twoNodeSpec("restart"), "restart", 8);
    TestClusterNode active = cluster.nodes().get(0);
    TestClusterNode retained = cluster.nodes().get(1);
    assertThrows(IllegalStateException.class, () -> cluster.removeNode(retained));

    cluster.stop();
    cluster.start();

    assertTrue(provisioner.running.contains(active));
    assertFalse(provisioner.running.contains(retained));
    assertEquals(1, provisioner.startNodeCount.get());
  }

  @Test
  public void privateCloseSerializesWithAddAndReleasesEveryReservation() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.blockAdds.set(true);
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 2, resources -> {});
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec("private"));
      Future<TestClusterNode> adding =
          executor.submit(() -> lease.control().addNode("dc1", "RAC1"));
      assertTrue(provisioner.addStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                lease.close();
                return null;
              });

      Thread.sleep(100);
      assertFalse("Close must wait for the in-flight control", closing.isDone());
      provisioner.allowAdd.countDown();
      adding.get(5, TimeUnit.SECONDS);
      closing.get(5, TimeUnit.SECONDS);
      assertThrows(IllegalStateException.class, lease.control()::start);

      try (ReusableClusterLease replacement = pool.acquireReusable(twoNodeSpec("replacement"))) {
        assertEquals(2, replacement.cluster().nodes().size());
      }
    } finally {
      provisioner.allowAdd.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void failedPrivateCloseCanBeRetriedButControlsRemainDisabled() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 2, resources -> {});
    try {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("idle"))) {}
      PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec("retry"));
      provisioner.failNextRemoval.set(true);

      assertThrows(IllegalStateException.class, lease::close);
      assertThrows(IllegalStateException.class, lease.control()::stop);
      assertThrows(IllegalStateException.class, () -> lease.control().addNode("dc1", "RAC1"));
      assertEquals("Rejected control must not retry removal", 1, provisioner.removeCount.get());
      lease.close();

      assertEquals(2, provisioner.removeCount.get());
    } finally {
      pool.close();
    }
  }

  @Test
  public void shutdownOwnsAnEvictionAfterItLeavesTheReuseIndex() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.blockRemovals.set(true);
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("first"))) {}
      Future<ReusableClusterLease> eviction =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("second")));
      assertTrue(provisioner.removalStarted.await(5, TimeUnit.SECONDS));

      Future<?> closing =
          executor.submit(
              () -> {
                pool.close();
                return null;
              });
      Thread.sleep(100);
      assertFalse("Shutdown must retain and join the evicted cluster", closing.isDone());

      provisioner.allowRemoval.countDown();
      closing.get(5, TimeUnit.SECONDS);
      ExecutionException acquisitionFailure =
          assertThrows(ExecutionException.class, () -> eviction.get(5, TimeUnit.SECONDS));
      assertTrue(acquisitionFailure.getCause() instanceof IllegalStateException);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowRemoval.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void failedEvictionWakesAdmissionWaitingForItsRetry() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.blockRemovals.set(true);
    provisioner.failNextRemoval.set(true);
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("first"))) {}
      Future<ReusableClusterLease> failingEviction =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("second")));
      assertTrue(provisioner.removalStarted.await(5, TimeUnit.SECONDS));
      Future<ReusableClusterLease> waiting =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("third")));
      Thread.sleep(100);
      assertFalse(waiting.isDone());

      provisioner.allowRemoval.countDown();
      assertThrows(ExecutionException.class, () -> failingEviction.get(5, TimeUnit.SECONDS));
      try (ReusableClusterLease admitted = waiting.get(5, TimeUnit.SECONDS)) {
        assertEquals(1, admitted.cluster().nodes().size());
      }
      assertEquals(2, provisioner.removeCount.get());
    } finally {
      provisioner.allowRemoval.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void shutdownWaitsForCleanupThatAlreadyStarted() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    CountDownLatch allowCleanup = new CountDownLatch(1);
    TestClusterPool pool =
        new TestClusterPool(
            provisioner,
            ClusterCapacity.fromAvailableMemory(8192),
            1,
            resources -> {
              cleanupStarted.countDown();
              allowCleanup.await();
            });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      ReusableClusterLease lease = pool.acquireReusable(oneNodeSpec("cleanup"));
      Future<?> releasing =
          executor.submit(
              () -> {
                lease.close();
                return null;
              });
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                pool.close();
                return null;
              });

      Thread.sleep(100);
      assertFalse(closing.isDone());
      assertEquals(0, provisioner.removeCount.get());
      allowCleanup.countDown();
      releasing.get(5, TimeUnit.SECONDS);
      closing.get(5, TimeUnit.SECONDS);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      allowCleanup.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void tableDeletionPollingUsesOneBoundedDeadline() throws Exception {
    DynamoDbClient client = mock(DynamoDbClient.class);
    when(client.listTables(any(ListTablesRequest.class)))
        .thenReturn(
            ListTablesResponse.builder().tableNames("owned_table", "foreign_table").build());
    when(client.describeTable(
            any(software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.class)))
        .thenReturn(DescribeTableResponse.builder().build());

    assertThrows(
        TimeoutException.class,
        () -> TestResourceScope.cleanupTables(client, "owned_", Duration.ofMillis(20)));
  }

  private static ClusterSpec oneNodeSpec(String identity) {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(1))
        .withTransports(AlternatorTransport.HTTP)
        .withYamlOverride("cluster_identity", identity);
  }

  private static ClusterSpec twoNodeSpec(String identity) {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(2))
        .withTransports(AlternatorTransport.HTTP)
        .withYamlOverride("cluster_identity", identity);
  }

  private static final class LifecycleProvisioner extends CcmProvisioner {
    final AtomicInteger startNodeCount = new AtomicInteger();
    final AtomicInteger stopNodeCount = new AtomicInteger();
    final AtomicInteger decommissionCount = new AtomicInteger();
    final AtomicInteger deleteNodeCount = new AtomicInteger();
    final AtomicInteger runningProbeCount = new AtomicInteger();
    final AtomicInteger decommissionProbeCount = new AtomicInteger();
    final AtomicInteger readinessWaitCount = new AtomicInteger();
    final AtomicInteger removeCount = new AtomicInteger();
    final AtomicBoolean failStartAfterEffect = new AtomicBoolean();
    final AtomicBoolean failDecommissionAfterEffect = new AtomicBoolean();
    final AtomicBoolean failNextDelete = new AtomicBoolean();
    final AtomicBoolean nodeReady = new AtomicBoolean(true);
    final AtomicBoolean failNextRemoval = new AtomicBoolean();
    final AtomicBoolean blockAdds = new AtomicBoolean();
    final AtomicBoolean blockRemovals = new AtomicBoolean();
    final CountDownLatch addStarted = new CountDownLatch(1);
    final CountDownLatch allowAdd = new CountDownLatch(1);
    final CountDownLatch removalStarted = new CountDownLatch(1);
    final CountDownLatch allowRemoval = new CountDownLatch(1);
    final java.util.Set<TestClusterNode> running =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    final java.util.Set<TestClusterNode> decommissioned =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    LifecycleProvisioner() throws Exception {
      super(Files.createTempDirectory("lifecycle-provisioner-"));
    }

    PhysicalTestCluster newCluster(ClusterSpec spec, String instanceId, int ccmId) {
      List<TestClusterNode> nodes = new ArrayList<>();
      for (int index = 1; index <= spec.topology().nodeCount(); index++) {
        TestClusterNode node =
            new TestClusterNode("node" + index, "127.0." + ccmId + "." + index, "dc1", "RAC1");
        nodes.add(node);
        running.add(node);
      }
      return new PhysicalTestCluster(
          this, instanceId, ccmId, runDirectory().resolve(instanceId), spec, nodes, null, null);
    }

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) {
      return newCluster(spec, instanceId, ccmId);
    }

    @Override
    void startNode(PhysicalTestCluster cluster, TestClusterNode node) {
      startNodeCount.incrementAndGet();
      running.add(node);
      if (failStartAfterEffect.getAndSet(false)) {
        throw new IllegalStateException("start failed after taking effect");
      }
    }

    @Override
    void stopNode(PhysicalTestCluster cluster, TestClusterNode node) {
      stopNodeCount.incrementAndGet();
      running.remove(node);
    }

    @Override
    void stop(PhysicalTestCluster cluster) {
      running.clear();
    }

    @Override
    void waitForNodeReady(PhysicalTestCluster cluster, TestClusterNode node) {
      readinessWaitCount.incrementAndGet();
      if (!nodeReady.get()) {
        throw new IllegalStateException("node is alive but not ready");
      }
    }

    @Override
    TestClusterNode addNode(PhysicalTestCluster cluster, String datacenter, String rack)
        throws Exception {
      if (blockAdds.get()) {
        addStarted.countDown();
        allowAdd.await();
      }
      int index = 1;
      while (containsName(cluster.nodes(), "node" + index)) {
        index++;
      }
      TestClusterNode node =
          new TestClusterNode(
              "node" + index, "127.0." + cluster.ccmId() + "." + index, datacenter, rack);
      running.add(node);
      return node;
    }

    @Override
    void decommissionNode(PhysicalTestCluster cluster, TestClusterNode node) {
      decommissionCount.incrementAndGet();
      running.remove(node);
      decommissioned.add(node);
      if (failDecommissionAfterEffect.getAndSet(false)) {
        throw new IllegalStateException("decommission failed after taking effect");
      }
    }

    @Override
    void deleteNodeState(PhysicalTestCluster cluster, TestClusterNode node) {
      deleteNodeCount.incrementAndGet();
      if (failNextDelete.getAndSet(false)) {
        throw new IllegalStateException("node deletion failed");
      }
      running.remove(node);
      decommissioned.remove(node);
    }

    @Override
    boolean isNodeRunning(PhysicalTestCluster cluster, TestClusterNode node) {
      runningProbeCount.incrementAndGet();
      return running.contains(node);
    }

    @Override
    boolean isNodeDecommissioned(PhysicalTestCluster cluster, TestClusterNode node) {
      decommissionProbeCount.incrementAndGet();
      return decommissioned.contains(node);
    }

    @Override
    boolean isHealthy(PhysicalTestCluster cluster) {
      return true;
    }

    @Override
    void remove(PhysicalTestCluster cluster) throws Exception {
      removeCount.incrementAndGet();
      removalStarted.countDown();
      if (blockRemovals.get()) {
        allowRemoval.await();
      }
      if (failNextRemoval.getAndSet(false)) {
        throw new IllegalStateException("remove failed");
      }
    }

    private static boolean containsName(List<TestClusterNode> nodes, String name) {
      for (TestClusterNode node : nodes) {
        if (node.name().equals(name)) {
          return true;
        }
      }
      return false;
    }
  }
}
