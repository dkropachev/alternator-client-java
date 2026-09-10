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
import static org.junit.Assume.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Lifecycle tests for cached state and the dirty-cluster fail-safe. */
public class ClusterLifecycleTest {
  @Test
  public void repeatedNodeControlsUseCachedState() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster cluster = provisioner.newCluster(oneNodeSpec(), "cached", 1);
    TestClusterNode node = cluster.nodes().get(0);

    cluster.startNode(node);
    cluster.stopNode(node);
    cluster.stopNode(node);
    cluster.startNode(node);
    cluster.startNode(node);

    assertEquals(1, provisioner.stopNodeCount.get());
    assertEquals(1, provisioner.startNodeCount.get());
    assertFalse(cluster.isDirty());
  }

  @Test
  public void ambiguousLifecycleFailureMakesOnlyWholeRemovalAvailable() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster cluster = provisioner.newCluster(oneNodeSpec(), "dirty", 2);
    TestClusterNode node = cluster.nodes().get(0);
    provisioner.failNextStop.set(true);

    assertThrows(IllegalStateException.class, () -> cluster.stopNode(node));
    assertTrue(cluster.isDirty());
    assertThrows(IllegalStateException.class, () -> cluster.startNode(node));
    assertThrows(IllegalStateException.class, cluster::stop);

    cluster.removePhysical();
    cluster.removePhysical();
    assertEquals(1, provisioner.removeCount.get());
  }

  @Test
  public void successfulAddRollbackLeavesClusterUsable() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster cluster = provisioner.newCluster(oneNodeSpec(), "add-rollback", 3);
    provisioner.failAddWithSuccessfulRollback.set(true);

    assertThrows(
        CcmProvisioner.CcmNodeProvisioningException.class, () -> cluster.addNode("dc1", "RAC1"));
    assertFalse(cluster.isDirty());
    assertEquals(1, cluster.nodes().size());

    TestClusterNode added = cluster.addNode("dc1", "RAC1");
    assertEquals("node2", added.name());
    assertEquals(2, cluster.nodes().size());
  }

  @Test
  public void failedAddRollbackRetainsNodeAndMarksClusterDirty() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster cluster = provisioner.newCluster(oneNodeSpec(), "add-dirty", 4);
    provisioner.failAddWithFailedRollback.set(true);

    assertThrows(
        CcmProvisioner.CcmNodeProvisioningException.class, () -> cluster.addNode("dc1", "RAC1"));
    assertEquals(2, cluster.nodes().size());
    assertTrue(cluster.isDirty());
    assertThrows(IllegalStateException.class, () -> cluster.addNode("dc1", "RAC1"));
    cluster.removePhysical();
  }

  @Test
  public void nodeHandlesUseIdentityAndTheFinalNodeCannotBeRemoved() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    PhysicalTestCluster one = provisioner.newCluster(oneNodeSpec(), "one", 5);
    assertThrows(IllegalStateException.class, () -> one.removeNode(one.nodes().get(0)));

    PhysicalTestCluster two = provisioner.newCluster(twoNodeSpec(), "two", 6);
    TestClusterNode original = two.nodes().get(1);
    TestClusterNode copy =
        new TestClusterNode(
            original.name(), original.address(), original.datacenter(), original.rack());
    assertThrows(IllegalArgumentException.class, () -> two.removeNode(copy));
    two.removeNode(original);
    TestClusterNode replacement = two.addNode("dc1", "RAC1");
    assertNotSame(original, replacement);
    assertEquals(original.name(), replacement.name());
  }

  @Test
  public void failedPrivateCloseCanBeRetriedAndControlsStayDisabled() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    TestClusterPool pool = new TestClusterPool(provisioner, 2, resources -> {});
    PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec());
    provisioner.failNextRemove.set(true);

    assertThrows(IllegalStateException.class, lease::close);
    assertThrows(IllegalStateException.class, lease.control()::start);
    lease.close();
    lease.close();
    assertEquals(2, provisioner.removeCount.get());
    pool.close();
  }

  @Test
  public void processCleanupFailuresQuarantineStartStopAndAddUntilNextJvm() throws Exception {
    LifecycleProvisioner startProvisioner = new LifecycleProvisioner();
    PhysicalTestCluster startCluster =
        startProvisioner.newCluster(oneNodeSpec(), "start-quarantine", 7);
    startCluster.stop();
    startProvisioner.failNextStartWithCleanup.set(true);
    assertThrows(CcmProvisioner.CcmProcessCleanupException.class, startCluster::start);
    assertSameJvmRemovalRefused(startCluster, startProvisioner);

    LifecycleProvisioner stopProvisioner = new LifecycleProvisioner();
    PhysicalTestCluster stopCluster =
        stopProvisioner.newCluster(oneNodeSpec(), "stop-quarantine", 8);
    stopProvisioner.failNextStopWithCleanup.set(true);
    assertThrows(CcmProvisioner.CcmProcessCleanupException.class, stopCluster::stop);
    assertSameJvmRemovalRefused(stopCluster, stopProvisioner);

    LifecycleProvisioner addProvisioner = new LifecycleProvisioner();
    PhysicalTestCluster addCluster = addProvisioner.newCluster(oneNodeSpec(), "add-quarantine", 9);
    addProvisioner.failNextAddWithCleanup.set(true);
    assertThrows(
        CcmProvisioner.CcmNodeProvisioningException.class, () -> addCluster.addNode("dc1", "RAC1"));
    assertSameJvmRemovalRefused(addCluster, addProvisioner);
  }

  @Test
  public void processCleanupFailureKeepsDurableOwnershipAcrossRepeatedClose() throws Exception {
    assumeTrue(
        "Durable CCM ownership requires Linux",
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
    Path root = Files.createTempDirectory("ccm-next-jvm-quarantine-");
    CcmRunState state = CcmRunState.open(root, (run, manifest) -> {});
    LifecycleProvisioner provisioner = new LifecycleProvisioner(state.runDirectory());
    TestClusterPool pool = new TestClusterPool(provisioner, 1, resources -> {}, state);
    PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec());
    provisioner.failNextRemoveWithCleanup.set(true);

    assertThrows(CcmProvisioner.CcmProcessCleanupException.class, lease::close);
    Path manifest = onlyEntry(state.runDirectory().resolve("owned"));
    Path reservation = onlyOwnerEntry(root.resolve("ccm-id-locks"));
    assertTrue(Files.isRegularFile(manifest));
    assertTrue(Files.isRegularFile(reservation));

    assertThrows(IllegalStateException.class, lease::close);
    assertThrows(IllegalStateException.class, pool::close);
    assertEquals(1, provisioner.removeCount.get());
    assertTrue(Files.isRegularFile(manifest));
    assertTrue(Files.isRegularFile(reservation));
  }

  @Test
  public void privateAddHonorsTheConfiguredNodeLimit() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    TestClusterPool pool = new TestClusterPool(provisioner, 1, resources -> {});
    try (PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec())) {
      assertThrows(IllegalStateException.class, () -> lease.control().addNode("dc1", "RAC1"));
      assertEquals(0, provisioner.addCount.get());
    }
    pool.close();
  }

  @Test
  public void privateLeaseCloseAndPoolCloseShareOneRemoval() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.blockRemove.set(true);
    TestClusterPool pool = new TestClusterPool(provisioner, 1, resources -> {});
    PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec());
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> leaseClose = executor.submit(() -> close(lease));
      assertTrue(provisioner.removeStarted.await(5, TimeUnit.SECONDS));
      Future<?> poolClose = executor.submit(() -> close(pool));
      provisioner.allowRemove.countDown();
      leaseClose.get(5, TimeUnit.SECONDS);
      poolClose.get(5, TimeUnit.SECONDS);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowRemove.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void reusableLeaseReleaseDoesNotRacePoolRemoval() throws Exception {
    LifecycleProvisioner provisioner = new LifecycleProvisioner();
    provisioner.blockRemove.set(true);
    TestClusterPool pool = new TestClusterPool(provisioner, 1, resources -> {});
    ReusableClusterLease lease = pool.acquireReusable(oneNodeSpec());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> poolClose = executor.submit(() -> close(pool));
      assertTrue(provisioner.removeStarted.await(5, TimeUnit.SECONDS));
      lease.close();
      provisioner.allowRemove.countDown();
      poolClose.get(5, TimeUnit.SECONDS);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowRemove.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  private static Void close(AutoCloseable closeable) throws Exception {
    closeable.close();
    return null;
  }

  private static void assertSameJvmRemovalRefused(
      PhysicalTestCluster cluster, LifecycleProvisioner provisioner) {
    assertThrows(IllegalStateException.class, cluster::removePhysical);
    assertThrows(IllegalStateException.class, cluster::removePhysical);
    assertEquals(0, provisioner.removeCount.get());
  }

  private static Path onlyEntry(Path directory) throws Exception {
    try (java.util.stream.Stream<Path> entries = Files.list(directory)) {
      List<Path> paths = entries.collect(java.util.stream.Collectors.toList());
      assertEquals(1, paths.size());
      return paths.get(0);
    }
  }

  private static Path onlyOwnerEntry(Path directory) throws Exception {
    try (java.util.stream.Stream<Path> entries = Files.list(directory)) {
      List<Path> paths =
          entries
              .filter(path -> path.getFileName().toString().endsWith(".owner"))
              .collect(java.util.stream.Collectors.toList());
      assertEquals(1, paths.size());
      return paths.get(0);
    }
  }

  private static ClusterSpec oneNodeSpec() {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(1))
        .withTransports(AlternatorTransport.HTTP);
  }

  private static ClusterSpec twoNodeSpec() {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(2))
        .withTransports(AlternatorTransport.HTTP);
  }

  private static final class LifecycleProvisioner extends CcmProvisioner {
    final AtomicInteger startNodeCount = new AtomicInteger();
    final AtomicInteger stopNodeCount = new AtomicInteger();
    final AtomicInteger addCount = new AtomicInteger();
    final AtomicInteger removeCount = new AtomicInteger();
    final AtomicBoolean failNextStop = new AtomicBoolean();
    final AtomicBoolean failAddWithSuccessfulRollback = new AtomicBoolean();
    final AtomicBoolean failAddWithFailedRollback = new AtomicBoolean();
    final AtomicBoolean failNextStartWithCleanup = new AtomicBoolean();
    final AtomicBoolean failNextStopWithCleanup = new AtomicBoolean();
    final AtomicBoolean failNextAddWithCleanup = new AtomicBoolean();
    final AtomicBoolean failNextRemove = new AtomicBoolean();
    final AtomicBoolean failNextRemoveWithCleanup = new AtomicBoolean();
    final AtomicBoolean blockRemove = new AtomicBoolean();
    final CountDownLatch removeStarted = new CountDownLatch(1);
    final CountDownLatch allowRemove = new CountDownLatch(1);
    final Set<TestClusterNode> running = Collections.newSetFromMap(new IdentityHashMap<>());

    LifecycleProvisioner() throws Exception {
      this(Files.createTempDirectory("lifecycle-provisioner-"));
    }

    LifecycleProvisioner(Path runDirectory) throws Exception {
      super(runDirectory, "/bin/true");
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
          this,
          instanceId,
          ccmId,
          runDirectory().resolve("clusters").resolve(instanceId),
          spec,
          nodes,
          null,
          null);
    }

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) {
      return newCluster(spec, instanceId, ccmId);
    }

    @Override
    void startNode(PhysicalTestCluster cluster, TestClusterNode node) {
      startNodeCount.incrementAndGet();
      running.add(node);
    }

    @Override
    void stopNode(PhysicalTestCluster cluster, TestClusterNode node) {
      stopNodeCount.incrementAndGet();
      running.remove(node);
      if (failNextStop.getAndSet(false)) {
        throw new IllegalStateException("ambiguous stop");
      }
    }

    @Override
    void stop(PhysicalTestCluster cluster) throws CcmProcessCleanupException {
      running.clear();
      if (failNextStopWithCleanup.getAndSet(false)) {
        throw new CcmProcessCleanupException("unproven stop cleanup");
      }
    }

    @Override
    void start(PhysicalTestCluster cluster, List<TestClusterNode> nodes)
        throws CcmProcessCleanupException {
      running.addAll(nodes);
      if (failNextStartWithCleanup.getAndSet(false)) {
        throw new CcmProcessCleanupException("unproven start cleanup");
      }
    }

    @Override
    TestClusterNode addNode(PhysicalTestCluster cluster, String datacenter, String rack)
        throws CcmNodeProvisioningException {
      addCount.incrementAndGet();
      int index = 1;
      while (containsName(cluster.nodes(), "node" + index)) {
        index++;
      }
      TestClusterNode node =
          new TestClusterNode(
              "node" + index, "127.0." + cluster.ccmId() + "." + index, datacenter, rack);
      if (failAddWithSuccessfulRollback.getAndSet(false)) {
        throw new CcmNodeProvisioningException(
            node, false, new IllegalStateException("add failed"), null);
      }
      if (failAddWithFailedRollback.getAndSet(false)) {
        throw new CcmNodeProvisioningException(
            node,
            true,
            new IllegalStateException("add failed"),
            new IllegalStateException("rollback failed"));
      }
      if (failNextAddWithCleanup.getAndSet(false)) {
        throw new CcmNodeProvisioningException(
            node,
            true,
            new IllegalStateException("add failed"),
            new CcmProcessCleanupException("unproven add cleanup"));
      }
      running.add(node);
      return node;
    }

    @Override
    void decommissionNode(PhysicalTestCluster cluster, TestClusterNode node) {
      running.remove(node);
    }

    @Override
    void deleteNodeState(PhysicalTestCluster cluster, TestClusterNode node) {
      running.remove(node);
    }

    @Override
    void remove(PhysicalTestCluster cluster) throws Exception {
      removeCount.incrementAndGet();
      removeStarted.countDown();
      if (blockRemove.get()) {
        allowRemove.await();
      }
      if (failNextRemove.getAndSet(false)) {
        throw new IllegalStateException("remove failed");
      }
      if (failNextRemoveWithCleanup.getAndSet(false)) {
        throw new CcmProcessCleanupException("unproven remove cleanup");
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
