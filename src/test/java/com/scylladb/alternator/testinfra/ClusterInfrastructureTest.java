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

import com.scylladb.alternator.CoversRequirements;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/** Unit-level contract tests for CCM cluster infrastructure. */
public class ClusterInfrastructureTest {
  @Test
  public void ccmAddressPreflightIncludesTheScyllaApiPort() throws Exception {
    assertAddressPreflightDetects(CcmProvisioner.API_PORT);
  }

  @Test
  public void ccmAddressPreflightIncludesEveryAdditionalFixedListener() throws Exception {
    for (int port : new int[] {7199, 9180, 19042}) {
      assertAddressPreflightDetects(port);
    }
  }

  private static void assertAddressPreflightDetects(int port) throws Exception {
    ServerSocket apiListener = null;
    int selectedId = -1;
    for (int id = 90; id < 100 && apiListener == null; id++) {
      if (TestClusterPool.isCcmAddressRangeInUse(id)) {
        continue;
      }
      ServerSocket candidate = new ServerSocket();
      try {
        candidate.bind(new InetSocketAddress("127.0." + id + ".1", port));
        apiListener = candidate;
        selectedId = id;
      } catch (IOException exception) {
        candidate.close();
      }
    }

    assertTrue("No free loopback address was available for port " + port, apiListener != null);
    try {
      assertTrue(TestClusterPool.isCcmAddressRangeInUse(selectedId));
    } finally {
      apiListener.close();
    }
  }

  @Test
  public void defaultCcmIdLocksAreOutsideEachStandaloneRunDirectory() throws Exception {
    Path firstRun = Files.createTempDirectory("ccm-standalone-first-");
    Path secondRun = Files.createTempDirectory("ccm-standalone-second-");
    Path lockRoot = TestClusterPool.defaultCcmIdLockRoot();

    assertFalse(lockRoot.startsWith(firstRun));
    assertFalse(lockRoot.startsWith(secondRun));
    assertEquals(lockRoot, TestClusterPool.defaultCcmIdLockRoot());
  }

  @Test
  public void idAllocationSkipsInvalidEntriesInItsOwnedLockDirectory() throws Exception {
    Path lockRoot = Files.createTempDirectory("ccm-id-locks-");
    Files.createDirectory(TestClusterPool.ccmIdLockPath(lockRoot, 1));
    FakeProvisioner provisioner = new FakeProvisioner();

    try (TestClusterPool pool =
            new TestClusterPool(
                provisioner,
                ClusterCapacity.fromAvailableMemory(8192),
                1,
                resources -> {},
                lockRoot);
        ReusableClusterLease lease = pool.acquireReusable(oneNodeSpec("shared-lock"))) {
      int ccmId = provisioner.lastCcmId.get();
      assertNotEquals(1, ccmId);
      Path lockPath = TestClusterPool.ccmIdLockPath(lockRoot, ccmId);
      assertTrue(Files.isRegularFile(lockPath));
    }
  }

  @Test
  public void independentPoolsCannotReserveTheSameCcmId() throws Exception {
    Path lockRoot = Files.createTempDirectory("ccm-shared-id-locks-");
    FakeProvisioner firstProvisioner = new FakeProvisioner();
    FakeProvisioner secondProvisioner = new FakeProvisioner();
    try (TestClusterPool firstPool =
            new TestClusterPool(
                firstProvisioner,
                ClusterCapacity.fromAvailableMemory(8192),
                1,
                resources -> {},
                lockRoot);
        TestClusterPool secondPool =
            new TestClusterPool(
                secondProvisioner,
                ClusterCapacity.fromAvailableMemory(8192),
                1,
                resources -> {},
                lockRoot);
        ReusableClusterLease first = firstPool.acquireReusable(oneNodeSpec("first-process"));
        ReusableClusterLease second = secondPool.acquireReusable(oneNodeSpec("second-process"))) {
      assertNotEquals(firstProvisioner.lastCcmId.get(), secondProvisioner.lastCcmId.get());
    }
  }

  @Test
  public void managedPoolPublishesAndReleasesDurableIdReservation() throws Exception {
    Path lockRoot = Files.createTempDirectory("ccm-durable-id-locks-");
    FakeProvisioner provisioner = new FakeProvisioner();
    Path reservation;
    try (TestClusterPool pool =
            new TestClusterPool(
                provisioner,
                ClusterCapacity.fromAvailableMemory(8192),
                1,
                resources -> {},
                lockRoot,
                true);
        ReusableClusterLease lease = pool.acquireReusable(oneNodeSpec("durable-lock"))) {
      reservation = TestClusterPool.ccmIdReservationPath(lockRoot, provisioner.lastCcmId.get());
      assertTrue(Files.isRegularFile(reservation));
      assertEquals(
          provisioner.runDirectory().toAbsolutePath().normalize().toString(),
          Files.readString(reservation).trim());
    }

    assertFalse(Files.exists(reservation));
  }

  @Test
  public void idAllocationHonorsReservationLeftByAnotherProcess() throws Exception {
    Path lockRoot = Files.createTempDirectory("ccm-existing-reservation-");
    Files.writeString(TestClusterPool.ccmIdReservationPath(lockRoot, 1), "/stale/run\n");
    FakeProvisioner provisioner = new FakeProvisioner();

    try (TestClusterPool pool =
            new TestClusterPool(
                provisioner,
                ClusterCapacity.fromAvailableMemory(8192),
                1,
                resources -> {},
                lockRoot);
        ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("reserved-id"))) {
      assertNotEquals(1, provisioner.lastCcmId.get());
    }
  }

  @Test
  public void typedSpecsValidateAndProduceStableIdentity() {
    ClusterSpec first =
        new ClusterSpec()
            .withYamlOverride("logger_log_level", "info")
            .withYamlOverride("hinted_handoff_enabled", "false");
    ClusterSpec reordered =
        new ClusterSpec()
            .withYamlOverride("hinted_handoff_enabled", "false")
            .withYamlOverride("logger_log_level", "info");

    assertEquals(first.reuseKey(), reordered.reuseKey());
    assertNotEquals(first.reuseKey(), first.withSecurity(ClusterSecuritySpec.ENFORCED).reuseKey());
    assertThrows(
        IllegalArgumentException.class, () -> first.withYamlOverride("alternator_port", "9000"));
    assertThrows(
        IllegalArgumentException.class,
        () -> first.withTopology(ClusterTopology.singleDatacenter(10)));
  }

  @Test
  @CoversRequirements("CCM-REQ-005")
  public void capacityReservesMemoryAndEnforcesLimits() throws Exception {
    ClusterCapacity small = ClusterCapacity.fromAvailableMemory(1024);
    assertEquals(512, small.reservedMemoryMiB());
    assertEquals(512, small.usableMemoryMiB());

    ClusterCapacity normal = ClusterCapacity.fromAvailableMemory(8192);
    assertEquals(2048, normal.reservedMemoryMiB());
    assertEquals(6144, normal.usableMemoryMiB());

    ClusterCapacity large = ClusterCapacity.fromAvailableMemory(100_000);
    assertEquals(4096, large.reservedMemoryMiB());
    assertEquals(95_904, large.usableMemoryMiB());
    assertEquals(4096, AvailableMemoryDetector.parseConfiguredMemoryMiB("4096"));
    assertThrows(
        IllegalStateException.class,
        () -> AvailableMemoryDetector.parseConfiguredMemoryMiB("invalid"));
    assertThrows(
        IllegalStateException.class, () -> AvailableMemoryDetector.parseConfiguredMemoryMiB("0"));

    List<AvailableMemoryDetector.CgroupMemoryPaths> paths =
        AvailableMemoryDetector.resolveCgroupMemoryPaths(
            "0::/ci/job\n", "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n");
    assertEquals(
        Arrays.asList(
            Path.of("/sys/fs/cgroup/ci/job/memory.max"),
            Path.of("/sys/fs/cgroup/ci/memory.max"),
            Path.of("/sys/fs/cgroup/memory.max")),
        paths.stream().map(path -> path.maximumPath).collect(java.util.stream.Collectors.toList()));

    FakeProvisioner provisioner = new FakeProvisioner();
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(4096), 1, resources -> {})) {
      assertThrows(
          IllegalStateException.class,
          () ->
              pool.acquireReusable(
                  new ClusterSpec()
                      .withTopology(ClusterTopology.singleDatacenter(2))
                      .withTransports(AlternatorTransport.HTTP)));
      assertEquals(0, provisioner.provisionCount.get());
    }
  }

  @Test
  public void poolWaitsForActiveCapacityAndEvictsIdleLeastRecentlyUsedCluster() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {})) {
      ClusterSpec firstSpec = oneNodeSpec("first");
      ClusterSpec secondSpec = oneNodeSpec("second");
      ReusableClusterLease first = pool.acquireReusable(firstSpec);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<ReusableClusterLease> waiting =
            executor.submit(() -> pool.acquireReusable(secondSpec));
        Thread.sleep(100);
        assertFalse("An active lease must retain its capacity", waiting.isDone());

        first.close();
        try (ReusableClusterLease second = waiting.get(5, TimeUnit.SECONDS)) {
          assertNotEquals(first.cluster().instanceId(), second.cluster().instanceId());
        }
        assertEquals(2, provisioner.provisionCount.get());
        assertEquals(1, provisioner.removeCount.get());
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void failedCleanupPoisonsReusableCluster() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    AtomicBoolean failCleanup = new AtomicBoolean(true);
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner,
            ClusterCapacity.fromAvailableMemory(8192),
            1,
            resources -> {
              if (failCleanup.getAndSet(false)) {
                throw new IllegalStateException("cleanup failed");
              }
            })) {
      ReusableClusterLease first = pool.acquireReusable(oneNodeSpec("same"));
      assertThrows(IllegalStateException.class, first::close);
      try (ReusableClusterLease replacement = pool.acquireReusable(oneNodeSpec("same"))) {
        assertNotEquals(first.cluster().instanceId(), replacement.cluster().instanceId());
      }
      assertEquals(2, provisioner.provisionCount.get());
      assertEquals(1, provisioner.removeCount.get());
    }
  }

  @Test
  public void interruptedCallerDoesNotCancelSharedProvisioning() throws Exception {
    BlockingProvisioner provisioner = new BlockingProvisioner();
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {})) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<ReusableClusterLease> interrupted =
            executor.submit(() -> pool.acquireReusable(oneNodeSpec("shared")));
        assertTrue(provisioner.provisioningStarted.await(5, TimeUnit.SECONDS));
        Future<ReusableClusterLease> survivor =
            executor.submit(() -> pool.acquireReusable(oneNodeSpec("shared")));

        interrupted.cancel(true);
        provisioner.allowProvisioning.countDown();
        try (ReusableClusterLease lease = survivor.get(5, TimeUnit.SECONDS)) {
          assertEquals(1, lease.cluster().nodes().size());
        }
        assertEquals(1, provisioner.provisionCount.get());
      } finally {
        provisioner.allowProvisioning.countDown();
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void interruptedReuseValidationDoesNotLeakAClusterReference() throws Exception {
    BlockingReuseValidationProvisioner provisioner = new BlockingReuseValidationProvisioner();
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {})) {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("shared"))) {}

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<ReusableClusterLease> interrupted =
            executor.submit(() -> pool.acquireReusable(oneNodeSpec("shared")));
        assertTrue(provisioner.validationStarted.await(5, TimeUnit.SECONDS));
        assertTrue(interrupted.cancel(true));
        provisioner.allowValidation.countDown();

        Future<ReusableClusterLease> replacement =
            executor.submit(() -> pool.acquireReusable(oneNodeSpec("replacement")));
        try (ReusableClusterLease ignored = replacement.get(5, TimeUnit.SECONDS)) {
          assertEquals(2, provisioner.provisionCount.get());
        }
      } finally {
        provisioner.allowValidation.countDown();
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void failedRemovalIsRetriedDuringPoolShutdown() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    try {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("first"))) {}
      provisioner.failNextRemoval.set(true);
      assertThrows(IllegalStateException.class, () -> pool.acquireReusable(oneNodeSpec("second")));

      pool.close();
      assertEquals(2, provisioner.removeCount.get());
    } finally {
      pool.close();
    }
  }

  @Test
  public void failedRemovalIsRetriedByAdmissionInsteadOfWaitingForever() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {})) {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("first"))) {}
      provisioner.failNextRemoval.set(true);
      assertThrows(IllegalStateException.class, () -> pool.acquireReusable(oneNodeSpec("second")));

      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("second"))) {
        assertEquals(2, provisioner.provisionCount.get());
      }
      assertEquals(2, provisioner.removeCount.get());
    }
  }

  @Test
  public void repeatedFailedRemovalMakesAdmissionFailPromptly() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("first"))) {}
      provisioner.failRemovals.set(true);
      assertThrows(IllegalStateException.class, () -> pool.acquireReusable(oneNodeSpec("second")));

      Future<ReusableClusterLease> retry =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("second")));
      java.util.concurrent.ExecutionException failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class, () -> retry.get(5, TimeUnit.SECONDS));
      assertTrue(failure.getCause() instanceof IllegalStateException);
    } finally {
      provisioner.failRemovals.set(false);
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void failedPrivateRemovalIsRetriedByAdmission() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    try (TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {})) {
      PrivateClusterLease privateLease = pool.provisionPrivate(oneNodeSpec("private"));
      provisioner.failNextRemoval.set(true);
      assertThrows(IllegalStateException.class, privateLease::close);

      try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("replacement"))) {
        assertEquals(2, provisioner.provisionCount.get());
      }
      assertEquals(2, provisioner.removeCount.get());
    }
  }

  @Test
  public void privateReleaseAndShutdownShareOnePhysicalRemoval() throws Exception {
    BlockingRemovalProvisioner provisioner = new BlockingRemovalProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    PrivateClusterLease lease = pool.provisionPrivate(oneNodeSpec("single-flight"));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> releasing =
          executor.submit(
              () -> {
                lease.close();
                return null;
              });
      assertTrue(provisioner.removalStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                pool.close();
                return null;
              });

      Thread.sleep(100);
      assertFalse(closing.isDone());
      assertEquals(1, provisioner.removeCount.get());
      provisioner.allowRemoval.countDown();

      releasing.get(5, TimeUnit.SECONDS);
      closing.get(5, TimeUnit.SECONDS);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowRemoval.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void failedShutdownRemovalCanBeRetried() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    try (ReusableClusterLease ignored = pool.acquireReusable(oneNodeSpec("retry-close"))) {}

    provisioner.failNextRemoval.set(true);
    assertThrows(Exception.class, pool::close);

    pool.close();
    assertEquals(2, provisioner.removeCount.get());
  }

  @Test
  public void shutdownCleansRollbackFailurePublishedDuringProvisioning() throws Exception {
    FailedProvisioningProvisioner provisioner = new FailedProvisioningProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<ReusableClusterLease> acquiring =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("late-failure")));
      assertTrue(provisioner.provisioningStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                pool.close();
                return null;
              });

      Thread.sleep(100);
      provisioner.allowFailure.countDown();

      closing.get(5, TimeUnit.SECONDS);
      assertThrows(java.util.concurrent.ExecutionException.class, acquiring::get);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowFailure.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void shutdownWaitsForPrivateProvisioningAndRejectsTheLateLease() throws Exception {
    BlockingPrivateProvisioner provisioner = new BlockingPrivateProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<PrivateClusterLease> acquiring =
          executor.submit(() -> pool.provisionPrivate(oneNodeSpec("private")));
      assertTrue(provisioner.provisioningStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                pool.close();
                return null;
              });

      Thread.sleep(100);
      assertFalse("Shutdown must wait for in-flight private provisioning", closing.isDone());
      provisioner.allowProvisioning.countDown();

      closing.get(5, TimeUnit.SECONDS);
      java.util.concurrent.ExecutionException failure =
          assertThrows(java.util.concurrent.ExecutionException.class, acquiring::get);
      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowProvisioning.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void shutdownRejectsReusableLeaseThatFinishesProvisioningLate() throws Exception {
    BlockingProvisioner provisioner = new BlockingProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<ReusableClusterLease> acquiring =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("late-reusable")));
      assertTrue(provisioner.provisioningStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                pool.close();
                return null;
              });

      Thread.sleep(100);
      assertFalse(closing.isDone());
      provisioner.allowProvisioning.countDown();

      closing.get(5, TimeUnit.SECONDS);
      java.util.concurrent.ExecutionException failure =
          assertThrows(java.util.concurrent.ExecutionException.class, acquiring::get);
      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertEquals(1, provisioner.removeCount.get());
    } finally {
      provisioner.allowProvisioning.countDown();
      executor.shutdownNow();
      pool.close();
    }
  }

  @Test
  public void interruptedShutdownFinishesCleanupBeforeRestoringInterrupt() throws Exception {
    BlockingProvisioner provisioner = new BlockingProvisioner();
    TestClusterPool pool =
        new TestClusterPool(
            provisioner, ClusterCapacity.fromAvailableMemory(8192), 1, resources -> {});
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicReference<Throwable> closeFailure = new AtomicReference<>();
    AtomicBoolean interruptRestored = new AtomicBoolean();
    Thread closing =
        new Thread(
            () -> {
              try {
                pool.close();
              } catch (Throwable exception) {
                closeFailure.set(exception);
                interruptRestored.set(Thread.currentThread().isInterrupted());
              }
            });
    try {
      Future<ReusableClusterLease> acquiring =
          executor.submit(() -> pool.acquireReusable(oneNodeSpec("interrupted-close")));
      assertTrue(provisioner.provisioningStarted.await(5, TimeUnit.SECONDS));
      closing.start();
      Thread.sleep(100);
      closing.interrupt();
      Thread.sleep(100);
      assertTrue("Shutdown must keep waiting for provisioning", closing.isAlive());

      provisioner.allowProvisioning.countDown();
      closing.join(5000);

      assertFalse("Shutdown did not finish", closing.isAlive());
      assertTrue(closeFailure.get() instanceof InterruptedException);
      assertTrue(interruptRestored.get());
      assertEquals(1, provisioner.removeCount.get());
      java.util.concurrent.ExecutionException acquisitionFailure =
          assertThrows(java.util.concurrent.ExecutionException.class, acquiring::get);
      assertTrue(acquisitionFailure.getCause() instanceof IllegalStateException);
    } finally {
      provisioner.allowProvisioning.countDown();
      executor.shutdownNow();
      if (closing.isAlive()) {
        closing.interrupt();
        closing.join(5000);
      }
      pool.close();
    }
  }

  @Test
  public void failureBeforeCcmCreationDoesNotClaimFailedRollback() throws Exception {
    Path runDirectory = Files.createTempDirectory("missing-ccm-provisioner-");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, runDirectory.resolve("missing-ccm").toString());

    Exception failure =
        assertThrows(
            Exception.class,
            () -> provisioner.provision(oneNodeSpec("missing-ccm"), "missing-ccm", 7));

    assertFalse(failure instanceof CcmProvisioner.CcmClusterProvisioningException);
  }

  @Test
  public void failedCreateRollsBackCcmStateWrittenBeforeTheError() throws Exception {
    Path runDirectory = Files.createTempDirectory("partial-create-provisioner-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "partial-create-ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "printf '%s\\n' \"$1\" >> \"$3/invocations\"\n"
                + "if [[ \"$1\" == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  echo 'create failed after metadata'\n"
                + "  exit 17\n"
                + "fi\n"
                + "if [[ \"$1\" == remove ]]; then\n"
                + "  rm -rf \"$3/$4\"\n"
                + "  rm -f \"$3/CURRENT\"\n"
                + "fi\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());

    Exception failure =
        assertThrows(
            Exception.class,
            () -> provisioner.provision(oneNodeSpec("partial-create"), "partial-create", 7));

    assertTrue(failure instanceof CcmProvisioner.CcmCommandException);
    Path ccmDirectory = runDirectory.resolve("clusters").resolve("partial-create");
    assertFalse(Files.exists(ccmDirectory.resolve("partial-create")));
    assertFalse(Files.exists(ccmDirectory.resolve("CURRENT")));
    assertEquals("create\nremove\n", Files.readString(ccmDirectory.resolve("invocations")));
  }

  @Test
  public void timedOutCreateIsLoggedAndRolledBack() throws Exception {
    Path runDirectory = Files.createTempDirectory("timed-out-create-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "timed-out-create-ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "if [[ \"$1\" == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  echo 'create timed out after metadata'\n"
                + "  while true; do printf -v ignored x; done\n"
                + "fi\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, ccm.toString(), Duration.ofMillis(100));

    Exception failure =
        assertThrows(
            Exception.class,
            () -> provisioner.provision(oneNodeSpec("timed-out-create"), "timed-out", 7));

    assertTrue(failure instanceof CcmProvisioner.CcmCommandException);
    Path ccmDirectory = runDirectory.resolve("clusters").resolve("timed-out");
    assertFalse(Files.exists(ccmDirectory.resolve("timed-out")));
    String commandLog = Files.readString(ccmDirectory.resolve("ccm-commands.log"));
    assertTrue(commandLog, commandLog.contains("create timed out after metadata"));
    assertTrue(commandLog, commandLog.contains("[exit timeout]"));
  }

  @Test
  public void failedCreateRetainsIdentityWhenRollbackAlsoFails() throws Exception {
    Path runDirectory = Files.createTempDirectory("failed-create-rollback-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "failed-rollback-ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "if [[ \"$1\" == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "fi\n"
                + "echo 'command failed without removing state'\n"
                + "exit 29\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());

    CcmProvisioner.CcmClusterProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmClusterProvisioningException.class,
            () -> provisioner.provision(oneNodeSpec("failed-rollback"), "failed-rollback", 7));

    assertEquals("failed-rollback", failure.cluster().instanceId());
    assertTrue(
        Files.exists(failure.cluster().ccmDirectory().resolve("failed-rollback/cluster.conf")));
    assertEquals(1, failure.getSuppressed().length);
  }

  @Test
  public void alreadyRemovedClusterMakesCleanupIdempotent() throws Exception {
    Path runDirectory = Files.createTempDirectory("idempotent-remove-provisioner-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "remove-then-fail-ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n"
                + "echo 'cluster is already absent'\n"
                + "exit 19\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCcmBackedCluster(provisioner, "already-removed", 7);

    provisioner.remove(cluster);
    provisioner.remove(cluster);

    assertFalse(Files.exists(cluster.ccmDirectory().resolve(cluster.instanceId())));
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("CURRENT")));
  }

  @Test
  public void interruptedCommandIsLoggedAndRollbackRunsWithInterruptDeferred() throws Exception {
    Path runDirectory = Files.createTempDirectory("interrupted-command-provisioner-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "interruptible-ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "if [[ \"$1\" == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  exit 0\n"
                + "fi\n"
                + "if [[ \"$1\" == updateconf ]]; then\n"
                + "  echo 'partial update output'\n"
                + "  touch \"$3/update-started\"\n"
                + "  while true; do printf -v ignored x; done\n"
                + "fi\n"
                + "if [[ \"$1\" == remove ]]; then\n"
                + "  touch \"$3/rollback-ran\"\n"
                + "  rm -rf \"$3/$4\"\n"
                + "  rm -f \"$3/CURRENT\"\n"
                + "fi\n");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, ccm.toString(), Duration.ofSeconds(30));
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean interruptRestored = new AtomicBoolean();
    Thread provisioning =
        new Thread(
            () -> {
              try {
                provisioner.provision(oneNodeSpec("interrupted-command"), "interrupted", 7);
              } catch (Throwable exception) {
                failure.set(exception);
                interruptRestored.set(Thread.currentThread().isInterrupted());
              }
            });

    provisioning.start();
    Path ccmDirectory = runDirectory.resolve("clusters").resolve("interrupted");
    waitForFile(ccmDirectory.resolve("update-started"));
    provisioning.interrupt();
    provisioning.join(5000);

    assertFalse("Provisioning thread did not finish", provisioning.isAlive());
    assertTrue(failure.get() instanceof InterruptedException);
    assertTrue(interruptRestored.get());
    assertTrue(Files.exists(ccmDirectory.resolve("rollback-ran")));
    String commandLog = Files.readString(ccmDirectory.resolve("ccm-commands.log"));
    assertTrue(commandLog, commandLog.contains("partial update output"));
    assertTrue(commandLog, commandLog.contains("[exit interrupted]"));
  }

  @Test
  public void nodeRemovalErrorIsSuccessOnlyWhenNodeDirectoryIsAbsent() throws Exception {
    Path runDirectory = Files.createTempDirectory("idempotent-node-remove-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "node-remove-ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "rm -rf \"$4/cluster/node1\"\n"
                + "echo 'node already absent'\n"
                + "exit 23\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCcmBackedCluster(provisioner, "cluster", 7);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster").resolve("node1");
    Files.createDirectories(nodeDirectory);

    provisioner.deleteNodeState(cluster, cluster.nodes().get(0));

    assertFalse(Files.exists(nodeDirectory));
  }

  @Test
  public void nodeRemovalFailureRetainsNodeWhenDirectoryRemains() throws Exception {
    Path runDirectory = Files.createTempDirectory("failed-node-remove-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "failed-node-remove-ccm",
            "#!/usr/bin/env bash\n" + "echo 'node state remains'\n" + "exit 31\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCcmBackedCluster(provisioner, "cluster", 7);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster").resolve("node1");
    Files.createDirectories(nodeDirectory);

    assertThrows(
        CcmProvisioner.CcmCommandException.class,
        () -> provisioner.deleteNodeState(cluster, cluster.nodes().get(0)));

    assertTrue(Files.exists(nodeDirectory));
  }

  @Test
  public void nodeAddFailureReportsSuccessfulRollbackWhenNoNodeStateExists() throws Exception {
    Path runDirectory = Files.createTempDirectory("failed-node-add-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "failed-node-add-ccm",
            "#!/usr/bin/env bash\n" + "echo 'node command failed'\n" + "exit 37\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCcmBackedCluster(provisioner, "cluster", 7);

    CcmProvisioner.CcmNodeProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmNodeProvisioningException.class,
            () -> provisioner.addNode(cluster, "dc1", "RAC1"));

    assertFalse(failure.nodeRemainsProvisioned());
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("cluster/node2")));
  }

  @Test
  public void nodeAddFailureRetainsNodeWhenRollbackCannotRemovePartialState() throws Exception {
    Path runDirectory = Files.createTempDirectory("partial-node-add-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "partial-node-add-ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "if [[ \"$1\" == add ]]; then\n"
                + "  mkdir -p \"$3/cluster/$4\"\n"
                + "  printf 'name: cluster\\nnodes: [node1, node2]\\n' > \"$3/cluster/cluster.conf\"\n"
                + "fi\n"
                + "echo 'node command failed with state remaining'\n"
                + "exit 41\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCcmBackedCluster(provisioner, "cluster", 7);

    CcmProvisioner.CcmNodeProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmNodeProvisioningException.class,
            () -> provisioner.addNode(cluster, "dc1", "RAC1"));

    assertTrue(failure.nodeRemainsProvisioned());
    assertTrue(Files.exists(cluster.ccmDirectory().resolve("cluster/node2")));
  }

  @Test
  public void resourceNamesUseTheDynamoDbAsciiAlphabet() throws Exception {
    FakeProvisioner provisioner = new FakeProvisioner();
    PhysicalTestCluster cluster =
        provisioner.provision(oneNodeSpec("resource"), "resource-cluster", 7);
    TestResourceScope resources = new TestResourceScope(cluster, "Rún / id", 42);

    String tableName = resources.newTableName("Hint ☃ !");

    assertTrue(tableName.matches("[a-z0-9_.-]+"));
    assertTrue(tableName.length() <= 255);
  }

  private static ClusterSpec oneNodeSpec(String identity) {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(1))
        .withTransports(AlternatorTransport.HTTP)
        .withYamlOverride("cluster_identity", identity);
  }

  private static Path writeExecutable(Path directory, String name, String contents)
      throws Exception {
    Path executable = directory.resolve(name);
    Files.writeString(executable, contents, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(executable, PosixFilePermissions.fromString("rwx------"));
    return executable;
  }

  private static PhysicalTestCluster createCcmBackedCluster(
      CcmProvisioner provisioner, String instanceId, int ccmId) throws Exception {
    Path ccmDirectory =
        provisioner.runDirectory().resolve("clusters").resolve(instanceId + "-config");
    Path clusterDirectory = ccmDirectory.resolve(instanceId);
    Files.createDirectories(clusterDirectory);
    Files.writeString(
        clusterDirectory.resolve("cluster.conf"),
        "name: " + instanceId + "\nnodes:\n  - node1\n",
        StandardCharsets.UTF_8);
    Files.writeString(ccmDirectory.resolve("CURRENT"), instanceId + "\n", StandardCharsets.UTF_8);
    TestClusterNode node = new TestClusterNode("node1", "127.0." + ccmId + ".1", "dc1", "RAC1");
    return new PhysicalTestCluster(
        provisioner,
        instanceId,
        ccmId,
        ccmDirectory,
        oneNodeSpec(instanceId),
        List.of(node),
        null,
        null);
  }

  private static void waitForFile(Path path) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.exists(path) && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue("Timed out waiting for " + path, Files.exists(path));
  }

  private static class FakeProvisioner extends CcmProvisioner {
    final AtomicInteger provisionCount = new AtomicInteger();
    final AtomicInteger removeCount = new AtomicInteger();
    final AtomicInteger lastCcmId = new AtomicInteger();
    final AtomicBoolean failNextRemoval = new AtomicBoolean();
    final AtomicBoolean failRemovals = new AtomicBoolean();

    FakeProvisioner() throws Exception {
      super(Files.createTempDirectory("fake-ccm-provisioner-"));
    }

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) throws Exception {
      provisionCount.incrementAndGet();
      lastCcmId.set(ccmId);
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
      return true;
    }

    @Override
    void remove(PhysicalTestCluster cluster) throws Exception {
      removeCount.incrementAndGet();
      if (failRemovals.get() || failNextRemoval.getAndSet(false)) {
        throw new IllegalStateException("remove failed");
      }
    }
  }

  private static final class BlockingProvisioner extends FakeProvisioner {
    final CountDownLatch provisioningStarted = new CountDownLatch(1);
    final CountDownLatch allowProvisioning = new CountDownLatch(1);

    BlockingProvisioner() throws Exception {}

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) throws Exception {
      provisioningStarted.countDown();
      allowProvisioning.await();
      return super.provision(spec, instanceId, ccmId);
    }
  }

  private static final class FailedProvisioningProvisioner extends FakeProvisioner {
    final CountDownLatch provisioningStarted = new CountDownLatch(1);
    final CountDownLatch allowFailure = new CountDownLatch(1);

    FailedProvisioningProvisioner() throws Exception {}

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) throws Exception {
      provisioningStarted.countDown();
      allowFailure.await();
      PhysicalTestCluster cluster = super.provision(spec, instanceId, ccmId);
      throw new CcmClusterProvisioningException(
          cluster,
          new IllegalStateException("provision failed"),
          new IllegalStateException("rollback failed"));
    }
  }

  private static final class BlockingReuseValidationProvisioner extends FakeProvisioner {
    final AtomicInteger healthChecks = new AtomicInteger();
    final CountDownLatch validationStarted = new CountDownLatch(1);
    final CountDownLatch allowValidation = new CountDownLatch(1);

    BlockingReuseValidationProvisioner() throws Exception {}

    @Override
    boolean isHealthy(PhysicalTestCluster cluster) {
      if (healthChecks.incrementAndGet() == 1) {
        return true;
      }
      validationStarted.countDown();
      try {
        allowValidation.await();
        return true;
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  private static final class BlockingPrivateProvisioner extends FakeProvisioner {
    final CountDownLatch provisioningStarted = new CountDownLatch(1);
    final CountDownLatch allowProvisioning = new CountDownLatch(1);

    BlockingPrivateProvisioner() throws Exception {}

    @Override
    PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) throws Exception {
      provisioningStarted.countDown();
      allowProvisioning.await();
      return super.provision(spec, instanceId, ccmId);
    }
  }

  private static final class BlockingRemovalProvisioner extends FakeProvisioner {
    final CountDownLatch removalStarted = new CountDownLatch(1);
    final CountDownLatch allowRemoval = new CountDownLatch(1);

    BlockingRemovalProvisioner() throws Exception {}

    @Override
    void remove(PhysicalTestCluster cluster) throws Exception {
      removeCount.incrementAndGet();
      removalStarted.countDown();
      allowRemoval.await();
    }
  }
}
