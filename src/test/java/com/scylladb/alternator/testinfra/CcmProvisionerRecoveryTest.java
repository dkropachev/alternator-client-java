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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.BeforeClass;
import org.junit.Test;

/** Focused failure-injection coverage for bounded CCM command and stale-run cleanup. */
public class CcmProvisionerRecoveryTest {
  @BeforeClass
  public static void requireLinux() {
    assumeTrue(
        "CCM process recovery tests require Linux",
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
  }

  @Test
  public void timedOutCommandReapsCurrentProcessGroupAndKeepsFinalLog() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-timeout-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "if [[ $1 == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  (trap '' TERM; while :; do sleep 1; done) &\n"
                + "  printf '%s\\n' $! > \"$3/child.pid\"\n"
                + "  echo 'partial create output'\n"
                + "  while :; do sleep 1; done\n"
                + "fi\n"
                + "if [[ $1 == remove ]]; then\n"
                + "  printf 'remove\\n' >> \"$3/invocations\"\n"
                + "  rm -rf \"$3/$4\"\n"
                + "  rm -f \"$3/CURRENT\"\n"
                + "fi\n");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, ccm.toString(), Duration.ofMillis(150));

    Exception failure =
        assertThrows(Exception.class, () -> provisioner.provision(oneNodeSpec(), "timed-out", 7));

    Path ccmDirectory = runDirectory.resolve("clusters/timed-out");
    long childPid = Long.parseLong(Files.readString(ccmDirectory.resolve("child.pid")).trim());
    assertTrue(describe(failure), failure instanceof CcmProvisioner.CcmCommandException);
    assertEventuallyNotRunning(childPid);
    assertEquals("remove\n", Files.readString(ccmDirectory.resolve("invocations")));
    String commandLog = findCommandLogContaining(ccmDirectory, "partial create output");
    assertTrue(commandLog, commandLog.contains("[exit timeout]"));
    assertTrue(
        "Per-command logs must remain after aggregation", countCommandLogs(ccmDirectory) >= 2);
  }

  @Test
  public void interruptedCommandIsReapedLoggedAndRolledBackOnce() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-interrupt-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "printf '%s\\n' \"$1\" >> \"$3/invocations\"\n"
                + "if [[ $1 == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  exit 0\n"
                + "fi\n"
                + "if [[ $1 == updateconf ]]; then\n"
                + "  echo 'partial update output'\n"
                + "  touch \"$3/update-started\"\n"
                + "  while :; do :; done\n"
                + "fi\n"
                + "if [[ $1 == remove ]]; then\n"
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
                provisioner.provision(oneNodeSpec(), "interrupted", 7);
              } catch (Throwable exception) {
                failure.set(exception);
                interruptRestored.set(Thread.currentThread().isInterrupted());
              }
            });

    provisioning.start();
    Path ccmDirectory = runDirectory.resolve("clusters/interrupted");
    waitForFile(ccmDirectory.resolve("update-started"));
    provisioning.interrupt();
    provisioning.join(10_000);

    assertFalse("Provisioning thread did not finish", provisioning.isAlive());
    assertTrue(describe(failure.get()), failure.get() instanceof InterruptedException);
    assertTrue("Caller interrupt was not restored", interruptRestored.get());
    assertEquals(
        "create\nupdateconf\nremove\n", Files.readString(ccmDirectory.resolve("invocations")));
    String commandLog = findCommandLogContaining(ccmDirectory, "partial update output");
    assertTrue(commandLog, commandLog.contains("[exit interrupted]"));
  }

  @Test
  public void clusterRollbackIsAttemptedExactlyOnceAndPreservesFailedState() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-one-cluster-rollback-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "printf '%s\\n' \"$1\" >> \"$3/invocations\"\n"
                + "if [[ $1 == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "fi\n"
                + "echo failure\n"
                + "exit 29\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());

    CcmProvisioner.CcmClusterProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmClusterProvisioningException.class,
            () -> provisioner.provision(oneNodeSpec(), "failed", 7));

    assertEquals("failed", failure.cluster().instanceId());
    assertEquals(
        "create\nremove\n",
        Files.readString(failure.cluster().ccmDirectory().resolve("invocations")));
    assertTrue(
        Files.isRegularFile(failure.cluster().ccmDirectory().resolve("failed/cluster.conf")));
    assertEquals(1, failure.getSuppressed().length);
  }

  @Test
  public void unprovenCommandCleanupSkipsClusterRollbackAndRetainsDirtyOwnership()
      throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-unproven-cluster-cleanup-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "printf '%s\\n' \"$1\" >> \"$3/invocations\"\n"
                + "if [[ $1 == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  exit 29\n"
                + "fi\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n");
    CcmProvisioner provisioner = new PlainCleanupFailureProvisioner(runDirectory, ccm.toString());
    TestClusterPool pool = new TestClusterPool(provisioner, 9, resources -> {});

    CcmProvisioner.CcmClusterProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmClusterProvisioningException.class,
            () -> pool.provisionPrivate(oneNodeSpec()));

    Path ccmDirectory = failure.cluster().ccmDirectory();
    assertEquals("create\n", Files.readString(ccmDirectory.resolve("invocations")));
    assertTrue(
        Files.isRegularFile(
            ccmDirectory.resolve(failure.cluster().instanceId() + "/cluster.conf")));
    assertTrue(
        "Unproven command cleanup must dirty the retained cluster", failure.cluster().isDirty());
    assertTrue(failure.getCause() instanceof CcmProvisioner.CcmProcessCleanupException);
    assertTrue(failure.getCause().getCause() instanceof IOException);

    assertThrows(IllegalStateException.class, pool::close);
    assertThrows(IllegalStateException.class, pool::close);
    assertEquals("create\n", Files.readString(ccmDirectory.resolve("invocations")));
  }

  @Test
  public void failedProvisioningSnapshotRetainsStateInsteadOfRunningRollback() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-provision-diagnostic-failure-");
    Path diagnostics = Files.createDirectories(runDirectory.resolve("external-diagnostics"));
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "printf '%s\\n' \"$1\" >> \"$3/invocations\"\n"
                + "if [[ $1 == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "fi\n"
                + "exit 17\n");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, diagnostics, ccm.toString(), Duration.ofSeconds(5));
    Files.writeString(diagnostics.resolve("cluster"), "blocks diagnostic directory\n");

    CcmProvisioner.CcmClusterProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmClusterProvisioningException.class,
            () -> provisioner.provision(oneNodeSpec(), "cluster", 7));

    assertEquals(
        "create\n", Files.readString(failure.cluster().ccmDirectory().resolve("invocations")));
    assertTrue(
        Files.isRegularFile(failure.cluster().ccmDirectory().resolve("cluster/cluster.conf")));
    assertEquals(1, failure.getSuppressed().length);
  }

  @Test
  public void nodeRollbackIsAttemptedExactlyOnceAndReportsRemainingNode() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-one-node-rollback-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "config=$3\n"
                + "if [[ $1 == node2 ]]; then config=$4; fi\n"
                + "printf '%s %s\\n"
                + "' \"$1\" \"$2\" >> \"$config/invocations\"\n"
                + "if [[ $1 == add ]]; then\n"
                + "  mkdir -p \"$3/cluster/$4\"\n"
                + "  printf 'name: cluster\\n"
                + "nodes: [node1, node2]\\n"
                + "' > \"$3/cluster/cluster.conf\"\n"
                + "fi\n"
                + "echo failure\n"
                + "exit 31\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);

    CcmProvisioner.CcmNodeProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmNodeProvisioningException.class,
            () -> provisioner.addNode(cluster, "dc1", "RAC1"));

    assertTrue(failure.nodeRemainsProvisioned());
    assertEquals("node2", failure.node().name());
    assertEquals(
        "add --config-dir\nnode2 remove\n",
        Files.readString(cluster.ccmDirectory().resolve("invocations")));
    assertTrue(Files.isDirectory(cluster.ccmDirectory().resolve("cluster/node2")));
  }

  @Test
  public void unprovenCommandCleanupSkipsNodeRollbackAndDirtiesCluster() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-unproven-node-cleanup-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "config=$3\n"
                + "if [[ $1 == node2 ]]; then config=$4; fi\n"
                + "printf '%s %s\\n"
                + "' \"$1\" \"$2\" >> \"$config/invocations\"\n"
                + "if [[ $1 == add ]]; then\n"
                + "  mkdir -p \"$3/cluster/node2\"\n"
                + "  printf 'name: cluster\\n"
                + "nodes: [node1, node2]\\n"
                + "' > \"$3/cluster/cluster.conf\"\n"
                + "fi\n"
                + "exit 31\n");
    CcmProvisioner provisioner = new CleanupFailureProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);

    CcmProvisioner.CcmNodeProvisioningException failure =
        assertThrows(
            CcmProvisioner.CcmNodeProvisioningException.class,
            () -> cluster.addNode("dc1", "RAC1"));

    assertTrue(failure.nodeRemainsProvisioned());
    assertEquals(
        "add --config-dir\n", Files.readString(cluster.ccmDirectory().resolve("invocations")));
    assertEquals(2, cluster.nodes().size());
    assertTrue("Unproven node cleanup must dirty the cluster", cluster.isDirty());
    assertThrows(IllegalStateException.class, cluster::removePhysical);
    assertEquals(
        "add --config-dir\n", Files.readString(cluster.ccmDirectory().resolve("invocations")));
  }

  @Test
  public void cleanupStaleClusterAtomicallySanitizesPidMetadataAndPreservesDiagnostics()
      throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-stale-cleanup-");
    Path observed = runDirectory.resolve("sanitized-before-remove");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "node=\"$3/$4/node1\"\n"
                + "[[ -f \"$node/node.conf\" ]]\n"
                + "! grep -q '^pid:' \"$node/node.conf\"\n"
                + "[[ ! -e \"$node/cassandra.pid\" ]]\n"
                + "touch \""
                + observed
                + "\"\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Files.writeString(
        nodeDirectory.resolve("node.conf"),
        "name: node1\nstatus: DOWN\npid: 999999\n",
        StandardCharsets.UTF_8);
    Files.writeString(nodeDirectory.resolve("cassandra.pid"), "999999\n");
    Path nodeLog = Files.createDirectories(nodeDirectory.resolve("logs")).resolve("system.log");
    Files.writeString(nodeLog, "stale diagnostic\n");
    Files.writeString(nodeDirectory.resolve("logs/server.key"), "private material\n");
    Files.writeString(
        cluster.ccmDirectory().resolve("ccm-command-interrupted.log"),
        "> ccm create\npartial output\n");

    provisioner.cleanupStaleCluster("cluster", 7, cluster.ccmDirectory());

    assertTrue(Files.exists(observed));
    Path diagnostics = runDirectory.resolve("diagnostics/cluster");
    assertEquals(
        "> ccm create\npartial output\n",
        Files.readString(diagnostics.resolve("ccm-command-interrupted.log")));
    assertEquals(
        "stale diagnostic\n",
        Files.readString(diagnostics.resolve("cluster/node1/logs/system.log")));
    assertTrue(
        Files.readString(diagnostics.resolve("cluster/node1/node.conf")).contains("pid: 999999"));
    assertFalse(Files.exists(diagnostics.resolve("cluster/node1/logs/server.key")));
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("cluster")));
  }

  @Test
  public void staleCleanupCopiesCommandLogsWithoutCompletedClusterMetadata() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-orphan-command-log-");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, "/bin/true");
    Path ccmDirectory = Files.createDirectories(runDirectory.resolve("clusters/orphan"));
    Files.writeString(ccmDirectory.resolve("ccm-command-orphan.log"), "in-flight output\n");

    provisioner.cleanupStaleCluster("orphan", 8, ccmDirectory);

    assertEquals(
        "in-flight output\n",
        Files.readString(runDirectory.resolve("diagnostics/orphan/ccm-command-orphan.log")));
  }

  @Test
  public void malformedStaleMetadataIsPreservedWithoutInvokingCcm() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-malformed-stale-");
    Path invoked = runDirectory.resolve("invoked");
    Path ccm =
        writeExecutable(
            runDirectory, "ccm", "#!/usr/bin/env bash\n" + "touch \"" + invoked + "\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    Path ccmDirectory = Files.createDirectories(runDirectory.resolve("clusters/config"));
    Path clusterDirectory = Files.createDirectories(ccmDirectory.resolve("cluster"));
    Files.writeString(clusterDirectory.resolve("cluster.conf"), "not: [valid\n");
    Files.writeString(ccmDirectory.resolve("CURRENT"), "cluster\n");
    Files.writeString(ccmDirectory.resolve("ccm-command-before-crash.log"), "details\n");

    assertThrows(
        IOException.class, () -> provisioner.cleanupStaleCluster("cluster", 7, ccmDirectory));

    assertTrue(Files.isRegularFile(clusterDirectory.resolve("cluster.conf")));
    assertFalse(Files.exists(invoked));
    assertEquals(
        "details\n",
        Files.readString(runDirectory.resolve("diagnostics/cluster/ccm-command-before-crash.log")));
  }

  @Test
  public void staleCleanupRejectsConfigSymlinkOutsideRunState() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-stale-symlink-");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, "/bin/true");
    Path outside = Files.createTempDirectory("ccm-stale-outside-");
    Path marker = Files.writeString(outside.resolve("marker"), "outside\n");
    Path ccmDirectory = runDirectory.resolve("clusters/config");
    Files.createSymbolicLink(ccmDirectory, outside);

    assertThrows(
        IOException.class, () -> provisioner.cleanupStaleCluster("cluster", 7, ccmDirectory));

    assertEquals("outside\n", Files.readString(marker));
  }

  @Test
  public void removalIsIdempotentWhenCcmDeletesStateThenFails() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-idempotent-remove-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "printf 'remove\\n' >> \"$3/invocations\"\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n"
                + "exit 19\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);

    provisioner.remove(cluster);
    provisioner.remove(cluster);

    assertEquals("remove\n", Files.readString(cluster.ccmDirectory().resolve("invocations")));
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("cluster")));
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("CURRENT")));
  }

  @Test
  public void clusterRemovalRetainsCleanupFailureAfterMetadataDisappears() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-unproven-remove-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "printf 'remove\\n' >> \"$3/invocations\"\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n"
                + "exit 19\n");
    CcmProvisioner provisioner = new CleanupFailureProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);

    assertThrows(CcmProvisioner.CcmProcessCleanupException.class, cluster::removePhysical);
    assertThrows(IllegalStateException.class, cluster::removePhysical);

    assertEquals("remove\n", Files.readString(cluster.ccmDirectory().resolve("invocations")));
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("cluster")));
  }

  @Test
  public void nodeRemovalRetainsCleanupFailureAfterMetadataDisappears() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-unproven-node-remove-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "config=$4\n"
                + "printf '%s %s\\n' \"$1\" \"$2\" >> \"$config/invocations\"\n"
                + "rm -rf \"$config/cluster/$1\"\n"
                + "printf 'name: cluster\\nnodes: []\\n' > \"$config/cluster/cluster.conf\"\n"
                + "exit 19\n");
    CcmProvisioner provisioner = new CleanupFailureProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    TestClusterNode node = cluster.nodes().get(0);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Files.writeString(nodeDirectory.resolve("node.conf"), "name: node1\nstatus: UP\n");

    assertThrows(
        CcmProvisioner.CcmProcessCleanupException.class,
        () -> provisioner.deleteNodeState(cluster, node));

    assertEquals("node1 remove\n", Files.readString(cluster.ccmDirectory().resolve("invocations")));
    assertFalse(Files.exists(nodeDirectory));
  }

  @Test
  public void failedDiagnosticSnapshotPreventsDestructiveRemoval() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-diagnostic-failure-");
    Path diagnostics = Files.createDirectories(runDirectory.resolve("external-diagnostics"));
    Path invoked = runDirectory.resolve("invoked");
    Path ccm =
        writeExecutable(
            runDirectory, "ccm", "#!/usr/bin/env bash\n" + "touch \"" + invoked + "\"\n");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, diagnostics, ccm.toString(), Duration.ofSeconds(5));
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    Files.writeString(diagnostics.resolve("cluster"), "blocks diagnostic directory\n");

    assertThrows(IOException.class, () -> provisioner.remove(cluster));

    assertFalse(Files.exists(invoked));
    assertTrue(Files.isRegularFile(cluster.ccmDirectory().resolve("cluster/cluster.conf")));
  }

  @Test
  public void diagnosticsDoNotTraverseUnreadableNodeDataDirectories() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-bounded-diagnostics-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "printf 'remove\\n' >> \"$3/invocations\"\n"
                + "chmod -R u+rwx \"$3/$4\"\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Files.writeString(nodeDirectory.resolve("node.conf"), "name: node1\nstatus: UP\n");
    Path systemLog = Files.createDirectories(nodeDirectory.resolve("logs")).resolve("system.log");
    Files.writeString(systemLog, "useful diagnostic\n");
    Path unreadable = Files.createDirectories(nodeDirectory.resolve("data/unreadable"));
    Files.writeString(unreadable.resolve("sstable.db"), "not a diagnostic\n");
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));

    try {
      provisioner.remove(cluster);
    } finally {
      if (Files.exists(unreadable, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
        Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("rwx------"));
      }
    }

    assertEquals(
        "useful diagnostic\n",
        Files.readString(
            runDirectory.resolve("diagnostics/cluster/cluster/node1/logs/system.log")));
    assertEquals("remove\n", Files.readString(cluster.ccmDirectory().resolve("invocations")));
    assertFalse(Files.exists(cluster.ccmDirectory().resolve("cluster")));
  }

  @Test
  public void normalStopAndRemovalRejectEveryForeignLivePidReference() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-foreign-pid-");
    Path invoked = runDirectory.resolve("ccm-invoked");
    Path ccm =
        writeExecutable(
            runDirectory, "ccm", "#!/usr/bin/env bash\n" + "touch \"" + invoked + "\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    TestClusterNode node = cluster.nodes().get(0);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Process foreign = new ProcessBuilder("sleep", "30").start();
    try {
      Files.writeString(
          nodeDirectory.resolve("node.conf"),
          "name: node1\nstatus: UP\npid: " + foreign.pid() + "\n");
      Files.writeString(nodeDirectory.resolve("cassandra.pid"), foreign.pid() + "\n");

      assertThrows(
          CcmProvisioner.CcmProcessCleanupException.class, () -> provisioner.stop(cluster));
      assertThrows(
          CcmProvisioner.CcmProcessCleanupException.class,
          () -> provisioner.stopNode(cluster, node));
      assertThrows(
          CcmProvisioner.CcmProcessCleanupException.class,
          () -> provisioner.deleteNodeState(cluster, node));
      assertThrows(
          CcmProvisioner.CcmProcessCleanupException.class, () -> provisioner.remove(cluster));

      Files.writeString(nodeDirectory.resolve("node.conf"), "name: node1\nstatus: UP\n");
      Files.delete(nodeDirectory.resolve("cassandra.pid"));
      for (String pidFile : List.of("cassandra.pid", "scylla-jmx.pid", "scylla-agent.pid")) {
        Path reference = nodeDirectory.resolve(pidFile);
        Files.writeString(reference, foreign.pid() + "\n");
        assertThrows(
            CcmProvisioner.CcmProcessCleanupException.class, () -> provisioner.remove(cluster));
        Files.delete(reference);
      }

      assertTrue("Foreign process was killed by validation", foreign.isAlive());
      assertFalse("CCM must not run with a foreign PID reference", Files.exists(invoked));
    } finally {
      foreign.destroyForcibly();
      foreign.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void normalStopAndRemovalRejectWrongSameRunProcessReferences() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-wrong-owned-pid-");
    Path invoked = runDirectory.resolve("ccm-invoked");
    Path ccm =
        writeExecutable(
            runDirectory, "ccm", "#!/usr/bin/env bash\n" + "touch \"" + invoked + "\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    TestClusterNode node = cluster.nodes().get(0);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Path otherNodeExecutable =
        Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node2/bin"))
            .resolve("scylla");
    Files.copy(resolveExecutable("sleep"), otherNodeExecutable);
    Files.setPosixFilePermissions(
        otherNodeExecutable, PosixFilePermissions.fromString("rwx------"));
    ProcessBuilder helperBuilder = new ProcessBuilder(otherNodeExecutable.toString(), "30");
    helperBuilder.environment().put("SCYLLA_CCM_RUN_DIR", provisioner.runDirectory().toString());
    Process helper = helperBuilder.start();
    try {
      Files.writeString(
          nodeDirectory.resolve("node.conf"),
          "name: node1\nstatus: UP\npid: " + helper.pid() + "\n");
      Files.writeString(nodeDirectory.resolve("cassandra.pid"), helper.pid() + "\n");
      assertThrows(
          CcmProvisioner.CcmProcessCleanupException.class,
          () -> provisioner.stopNode(cluster, node));
      assertThrows(
          CcmProvisioner.CcmProcessCleanupException.class, () -> provisioner.remove(cluster));

      Files.writeString(nodeDirectory.resolve("node.conf"), "name: node1\nstatus: UP\n");
      Files.delete(nodeDirectory.resolve("cassandra.pid"));
      for (String pidFile : List.of("scylla-jmx.pid", "scylla-agent.pid")) {
        Path reference = nodeDirectory.resolve(pidFile);
        Files.writeString(reference, helper.pid() + "\n");
        assertThrows(
            CcmProvisioner.CcmProcessCleanupException.class, () -> provisioner.remove(cluster));
        Files.delete(reference);
      }

      assertTrue("Same-run helper was killed by validation", helper.isAlive());
      assertFalse("CCM must not run with a wrong same-run PID", Files.exists(invoked));
    } finally {
      helper.destroyForcibly();
      helper.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void normalStopAcceptsTheExpectedOwnedNodeProcess() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-expected-owned-pid-");
    Path invoked = runDirectory.resolve("ccm-invoked");
    Path ccm =
        writeExecutable(
            runDirectory, "ccm", "#!/usr/bin/env bash\n" + "touch \"" + invoked + "\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    TestClusterNode node = cluster.nodes().get(0);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Path executable = Files.createDirectories(nodeDirectory.resolve("bin")).resolve("scylla");
    Files.copy(resolveExecutable("sleep"), executable);
    Files.setPosixFilePermissions(executable, PosixFilePermissions.fromString("rwx------"));
    ProcessBuilder processBuilder = new ProcessBuilder(executable.toString(), "30");
    processBuilder.environment().put("SCYLLA_CCM_RUN_DIR", provisioner.runDirectory().toString());
    Process process = processBuilder.start();
    try {
      Files.writeString(
          nodeDirectory.resolve("node.conf"),
          "name: node1\nstatus: UP\npid: " + process.pid() + "\n");
      Files.writeString(nodeDirectory.resolve("cassandra.pid"), process.pid() + "\n");

      provisioner.stopNode(cluster, node);

      assertTrue("Valid node process did not reach CCM", Files.exists(invoked));
      assertTrue("Preflight killed the valid node process", process.isAlive());
    } finally {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void jmxIdentityAcceptsCcmLauncherAndFinalJavaProcessShapes() {
    Path nodeDirectory = Path.of("/tmp/ccm-jmx-shape/node1");
    String launcher = nodeDirectory.resolve("bin/symlinks/scylla-jmx").toString();
    String jar = nodeDirectory.resolve("bin/scylla-jmx-1.0.jar").toString();

    assertTrue(
        CcmProvisioner.matchesExpectedJmxArguments(
            List.of(launcher, "-Dapiaddress=127.0.1.1", "-jar", jar), nodeDirectory));
    assertTrue(
        CcmProvisioner.matchesExpectedJmxArguments(
            List.of("/usr/lib/jvm/java-17/bin/java", "-Xmx256m", "-jar", jar), nodeDirectory));
    assertFalse(
        CcmProvisioner.matchesExpectedJmxArguments(
            List.of("/usr/bin/python3", "-jar", jar), nodeDirectory));
    assertFalse(
        CcmProvisioner.matchesExpectedJmxArguments(
            List.of("/usr/bin/java", "-jar", nodeDirectory.resolve("bin/other.jar").toString()),
            nodeDirectory));
  }

  @Test
  public void normalRemovalSanitizesDeadPidReferencesBeforeInvokingCcm() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-dead-pid-");
    Path observed = runDirectory.resolve("dead-pids-sanitized");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "node=\"$3/$4/node1\"\n"
                + "! grep -q '^pid:' \"$node/node.conf\"\n"
                + "[[ ! -e $node/cassandra.pid ]]\n"
                + "[[ ! -e $node/scylla-jmx.pid ]]\n"
                + "[[ ! -e $node/scylla-agent.pid ]]\n"
                + "touch \""
                + observed
                + "\"\n"
                + "rm -rf \"$3/$4\"\n"
                + "rm -f \"$3/CURRENT\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory, "cluster", 7);
    Path nodeDirectory = Files.createDirectories(cluster.ccmDirectory().resolve("cluster/node1"));
    Files.writeString(
        nodeDirectory.resolve("node.conf"), "name: node1\nstatus: UP\npid: 999999991\n");
    Files.writeString(nodeDirectory.resolve("cassandra.pid"), "999999992\n");
    Files.writeString(nodeDirectory.resolve("scylla-jmx.pid"), "999999993\n");
    Files.writeString(nodeDirectory.resolve("scylla-agent.pid"), "999999994\n");

    provisioner.remove(cluster);

    assertTrue(Files.exists(observed));
  }

  @Test
  public void operationalDiagnosticsMustNotOverlapStateRoot() throws Exception {
    Path parent = Files.createTempDirectory("ccm-operational-paths-");
    Path stateRoot = parent.resolve("state");
    Path runDirectory =
        Files.createDirectories(
            stateRoot.resolve("runs/ccm-runtime.00000000000000000000000000000001"));

    assertThrows(
        IOException.class,
        () ->
            CcmProvisioner.validateOperationalDirectories(
                runDirectory, stateRoot.resolve("diagnostics")));
    assertThrows(
        IOException.class,
        () -> CcmProvisioner.validateOperationalDirectories(runDirectory, parent));
    CcmProvisioner.validateOperationalDirectories(
        runDirectory, Files.createTempDirectory("ccm-external-diagnostics-"));
  }

  @Test
  public void yamlOverridesUseCcmYaml12ScalarSemantics() {
    assertEquals(123, CcmProvisioner.parseYamlValue("0123"));
    assertEquals("yes", CcmProvisioner.parseYamlValue("yes"));
    assertEquals("on", CcmProvisioner.parseYamlValue("on"));
    assertEquals("1:20", CcmProvisioner.parseYamlValue("1:20"));
  }

  private static ClusterSpec oneNodeSpec() {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(1))
        .withTransports(AlternatorTransport.HTTP);
  }

  private static PhysicalTestCluster createCluster(
      CcmProvisioner provisioner, Path runDirectory, String instanceId, int ccmId)
      throws Exception {
    Path ccmDirectory = runDirectory.resolve("clusters/config");
    Path clusterDirectory = ccmDirectory.resolve(instanceId);
    Files.createDirectories(clusterDirectory);
    Files.writeString(
        clusterDirectory.resolve("cluster.conf"),
        "name: " + instanceId + "\nnodes: [node1]\n",
        StandardCharsets.UTF_8);
    Files.writeString(ccmDirectory.resolve("CURRENT"), instanceId + "\n", StandardCharsets.UTF_8);
    TestClusterNode node = new TestClusterNode("node1", "127.0." + ccmId + ".1", "dc1", "RAC1");
    return new PhysicalTestCluster(
        provisioner, instanceId, ccmId, ccmDirectory, oneNodeSpec(), List.of(node), null, null);
  }

  private static Path writeExecutable(Path directory, String name, String contents)
      throws Exception {
    Path executable = directory.resolve(name);
    Files.writeString(executable, contents, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(executable, PosixFilePermissions.fromString("rwx------"));
    return executable;
  }

  private static Path resolveExecutable(String name) throws IOException {
    String path = System.getenv("PATH");
    if (path != null) {
      for (String directory : path.split(java.io.File.pathSeparator, -1)) {
        Path candidate = directory.isEmpty() ? Path.of(name) : Path.of(directory).resolve(name);
        if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
          return candidate.toAbsolutePath().normalize();
        }
      }
    }
    throw new IOException("Executable not found on PATH: " + name);
  }

  private static final class CleanupFailureProvisioner extends CcmProvisioner {
    CleanupFailureProvisioner(Path runDirectory, String ccmExecutable) throws IOException {
      super(runDirectory, ccmExecutable, Duration.ofSeconds(5));
    }

    @Override
    void terminateCommandProcessGroup(Process process) throws Exception {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
      }
      throw new CcmProcessCleanupException("injected inability to prove command cleanup");
    }
  }

  private static final class PlainCleanupFailureProvisioner extends CcmProvisioner {
    PlainCleanupFailureProvisioner(Path runDirectory, String ccmExecutable) throws IOException {
      super(runDirectory, ccmExecutable, Duration.ofSeconds(5));
    }

    @Override
    void terminateCommandProcessGroup(Process process) throws IOException {
      throw new IOException("injected process-group inspection failure");
    }
  }

  private static String findCommandLogContaining(Path ccmDirectory, String expected)
      throws Exception {
    for (Path commandLog : commandLogs(ccmDirectory)) {
      String contents = Files.readString(commandLog);
      if (contents.contains(expected)) {
        return contents;
      }
    }
    throw new AssertionError("No per-command log contained: " + expected);
  }

  private static int countCommandLogs(Path ccmDirectory) throws Exception {
    return commandLogs(ccmDirectory).size();
  }

  private static List<Path> commandLogs(Path ccmDirectory) throws Exception {
    List<Path> result = new ArrayList<>();
    try (java.nio.file.DirectoryStream<Path> files =
        Files.newDirectoryStream(ccmDirectory, "ccm-command-*.log")) {
      files.forEach(result::add);
    }
    return result;
  }

  private static void assertEventuallyNotRunning(long pid) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (isRunning(pid) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse("PID remains alive after CCM cleanup: " + pid, isRunning(pid));
  }

  private static void waitForFile(Path path) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.isRegularFile(path) && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue("File was not published: " + path, Files.isRegularFile(path));
  }

  private static boolean isRunning(long pid) throws Exception {
    Path statPath = Path.of("/proc", Long.toString(pid), "stat");
    if (!Files.isRegularFile(statPath)) {
      return false;
    }
    String stat = Files.readString(statPath, StandardCharsets.US_ASCII);
    int commandEnd = stat.lastIndexOf(')');
    return commandEnd < 0 || commandEnd + 2 >= stat.length() || stat.charAt(commandEnd + 2) != 'Z';
  }

  private static String describe(Throwable failure) {
    assertNotNull(failure);
    StringBuilder result = new StringBuilder();
    for (Throwable current = failure; current != null; current = current.getCause()) {
      result.append(current).append('\n');
      for (Throwable suppressed : current.getSuppressed()) {
        result.append("  suppressed: ").append(suppressed).append('\n');
      }
    }
    return result.toString();
  }
}
