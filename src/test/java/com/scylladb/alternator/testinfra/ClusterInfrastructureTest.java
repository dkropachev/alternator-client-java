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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.scylladb.alternator.CoversRequirements;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.Test;

/** Focused tests for the intentionally single-slot cluster harness. */
public class ClusterInfrastructureTest {
  @BeforeClass
  public static void requireLinux() {
    assumeTrue(
        "CCM infrastructure tests require Linux",
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
  }

  @Test
  public void matchingReusableLeasesShareOnePhysicalClusterAndIndependentResources()
      throws Exception {
    try (Harness harness = new Harness(3)) {
      ClusterSpec spec = oneNodeSpec("shared");
      ReusableClusterLease first = harness.pool.acquireReusable(spec);
      ReusableClusterLease second = harness.pool.acquireReusable(spec);

      assertEquals(1, harness.provisioner.provisionCount.get());
      assertEquals(first.cluster().instanceId(), second.cluster().instanceId());
      assertNotEquals(
          first.resources().newTableName("table"), second.resources().newTableName("table"));

      first.close();
      second.close();
      try (ReusableClusterLease reused = harness.pool.acquireReusable(spec)) {
        assertEquals(first.cluster().instanceId(), reused.cluster().instanceId());
      }
      assertEquals(1, harness.provisioner.healthCount.get());
      assertEquals(1, harness.provisioner.provisionCount.get());
    }
  }

  @Test
  @CoversRequirements("CCM-REQ-005")
  public void incompatibleActiveRequestsFailFastAndIdleClustersAreReplaced() throws Exception {
    try (Harness harness = new Harness(3)) {
      ClusterSpec firstSpec = oneNodeSpec("first");
      ClusterSpec secondSpec = oneNodeSpec("second");
      ReusableClusterLease active = harness.pool.acquireReusable(firstSpec);

      assertThrows(IllegalStateException.class, () -> harness.pool.acquireReusable(secondSpec));
      assertThrows(IllegalStateException.class, () -> harness.pool.provisionPrivate(firstSpec));
      active.close();

      try (ReusableClusterLease replacement = harness.pool.acquireReusable(secondSpec)) {
        assertEquals(2, harness.provisioner.provisionCount.get());
        assertEquals(1, harness.provisioner.removeCount.get());
      }
      try (PrivateClusterLease privateLease = harness.pool.provisionPrivate(firstSpec)) {
        assertThrows(IllegalStateException.class, () -> harness.pool.acquireReusable(firstSpec));
        assertEquals(3, harness.provisioner.provisionCount.get());
        assertEquals(2, harness.provisioner.removeCount.get());
      }
      assertEquals(3, harness.provisioner.removeCount.get());
    }
  }

  @Test
  public void unhealthyIdleReusableClusterIsReplaced() throws Exception {
    try (Harness harness = new Harness(1)) {
      ClusterSpec spec = oneNodeSpec("health");
      try (ReusableClusterLease ignored = harness.pool.acquireReusable(spec)) {}
      harness.provisioner.healthy = false;

      try (ReusableClusterLease replacement = harness.pool.acquireReusable(spec)) {
        assertEquals(2, harness.provisioner.provisionCount.get());
        assertEquals(1, harness.provisioner.removeCount.get());
      }
    }
  }

  @Test
  public void configuredNodeLimitIsLowerThanTheHardNineNodeLimit() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ClusterSpec().withTopology(ClusterTopology.singleDatacenter(10)));

    try (Harness harness = new Harness(2)) {
      ClusterSpec threeNodes =
          new ClusterSpec()
              .withTopology(ClusterTopology.singleDatacenter(3))
              .withTransports(AlternatorTransport.HTTP);
      assertThrows(IllegalStateException.class, () -> harness.pool.provisionPrivate(threeNodes));
      assertEquals(0, harness.provisioner.provisionCount.get());
    }
  }

  @Test
  public void addressProbeUsesBindAndOnlyChecksRequestedAlternatorTransports() throws Exception {
    int id = 97;
    ClusterSpec http = oneNodeSpec("http");
    ClusterSpec https = http.withTransports(AlternatorTransport.HTTPS);

    try (SocketChannel boundButNotListening = SocketChannel.open()) {
      boundButNotListening.bind(
          new java.net.InetSocketAddress("127.0." + id + ".1", CcmProvisioner.HTTP_PORT));
      assertFalse(CcmRunState.isAddressRangeAvailable(http, id));
      assertTrue(CcmRunState.isAddressRangeAvailable(https, id));
    }

    try (ServerSocketChannel httpsListener = ServerSocketChannel.open()) {
      httpsListener.bind(
          new java.net.InetSocketAddress("127.0." + id + ".1", CcmProvisioner.HTTPS_PORT));
      assertTrue(CcmRunState.isAddressRangeAvailable(http, id));
      assertFalse(CcmRunState.isAddressRangeAvailable(https, id));
    }
  }

  @Test
  public void addressProbeCanSkipAbsentJmxOnlyForAProvenPinnedToolAndPackage() throws Exception {
    int id = 96;
    ClusterSpec pinned = oneNodeSpec("jmx");
    Path project = Files.createTempDirectory("pinned-ccm-project-");
    Path environment = project.resolve("bin/scylla-ccm-" + CcmProvisioner.PINNED_CCM_COMMIT);
    Path executable = Files.createDirectories(environment.resolve("bin")).resolve("ccm");
    Files.writeString(executable, "#!/bin/sh\nexit 0\n");
    executable.toFile().setExecutable(true);
    Files.writeString(environment.resolve(".install-complete"), CcmProvisioner.PINNED_CCM_COMMIT);
    assertTrue(CcmProvisioner.isRepositoryPinnedCcm(executable.toString(), project));
    assertFalse(CcmProvisioner.isRepositoryPinnedCcm("ccm", project));

    try (ServerSocketChannel jmxListener = ServerSocketChannel.open()) {
      jmxListener.bind(
          new java.net.InetSocketAddress("127.0." + id + ".1", CcmProvisioner.JMX_PORT));
      assertTrue(CcmRunState.isAddressRangeAvailable(pinned, id, false));
      assertFalse(CcmRunState.isAddressRangeAvailable(pinned, id, true));
    }
  }

  @Test
  public void durableReservationExistsUntilPhysicalRemovalCompletes() throws Exception {
    Harness harness = new Harness(1);
    Path owner;
    try {
      try (ReusableClusterLease ignored = harness.pool.acquireReusable(oneNodeSpec("owner"))) {
        owner =
            CcmRunState.idOwnerPath(
                harness.root.resolve("ccm-id-locks"), harness.provisioner.lastCcmId);
        assertTrue(Files.isRegularFile(owner));
      }
      assertTrue(Files.isRegularFile(owner));
    } finally {
      harness.close();
    }
    assertFalse(Files.exists(owner));
  }

  private static ClusterSpec oneNodeSpec(String identity) {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(1))
        .withTransports(AlternatorTransport.HTTP)
        .withYamlOverride("cluster_identity", identity);
  }

  private static final class Harness implements AutoCloseable {
    final Path root;
    final CcmRunState state;
    final FakeProvisioner provisioner;
    final TestClusterPool pool;

    Harness(int maximumNodes) throws Exception {
      root = Files.createTempDirectory("single-slot-ccm-");
      state = CcmRunState.open(root, (run, manifest) -> {});
      provisioner = new FakeProvisioner(state.runDirectory());
      pool = new TestClusterPool(provisioner, maximumNodes, resources -> {}, state);
    }

    @Override
    public void close() throws Exception {
      pool.close();
    }
  }

  private static final class FakeProvisioner extends CcmProvisioner {
    final AtomicInteger provisionCount = new AtomicInteger();
    final AtomicInteger removeCount = new AtomicInteger();
    final AtomicInteger healthCount = new AtomicInteger();
    volatile boolean healthy = true;
    volatile int lastCcmId;

    FakeProvisioner(Path runDirectory) throws Exception {
      super(runDirectory, "/bin/true");
    }

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) {
      provisionCount.incrementAndGet();
      lastCcmId = ccmId;
      List<TestClusterNode> nodes = new ArrayList<>();
      for (int index = 1; index <= spec.topology().nodeCount(); index++) {
        nodes.add(
            new TestClusterNode("node" + index, "127.0." + ccmId + "." + index, "dc1", "RAC1"));
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
    boolean isHealthy(PhysicalTestCluster cluster) {
      healthCount.incrementAndGet();
      return healthy;
    }

    @Override
    void remove(PhysicalTestCluster cluster) {
      removeCount.incrementAndGet();
      healthy = true;
    }
  }
}
