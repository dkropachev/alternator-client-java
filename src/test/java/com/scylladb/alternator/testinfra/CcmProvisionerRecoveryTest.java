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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.CoversRequirements;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/** Failure-injection coverage for CCM process and retained-node cleanup. */
public class CcmProvisionerRecoveryTest {
  @Test
  @CoversRequirements("CCM-REQ-006")
  public void timedOutCcmCommandReapsItsProcessGroupBeforeRollback() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-process-tree-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "if [[ \"$1\" == create ]]; then\n"
                + "  mkdir -p \"$3/$4\"\n"
                + "  printf 'name: %s\\nnodes: []\\n' \"$4\" > \"$3/$4/cluster.conf\"\n"
                + "  printf '%s\\n' \"$4\" > \"$3/CURRENT\"\n"
                + "  (trap '' TERM; while true; do sleep 1; done) &\n"
                + "  child=$!\n"
                + "  printf '%s\\n' \"$child\" > \"$3/child.pid\"\n"
                + "  while true; do sleep 1; done\n"
                + "fi\n"
                + "if [[ \"$1\" == remove ]]; then\n"
                + "  child=$(< \"$3/child.pid\")\n"
                + "  if [[ -r \"/proc/$child/stat\" ]] && [[ $(sed 's/.*) //' \"/proc/$child/stat\" | cut -d' ' -f1) != Z ]]; then\n"
                + "    touch \"$3/child-alive-during-rollback\"\n"
                + "  fi\n"
                + "  rm -rf \"$3/$4\"\n"
                + "  rm -f \"$3/CURRENT\"\n"
                + "fi\n");
    CcmProvisioner provisioner =
        new CcmProvisioner(runDirectory, ccm.toString(), Duration.ofMillis(150));

    Exception failure =
        assertThrows(
            Exception.class, () -> provisioner.provision(oneNodeSpec(), "process-tree", 7));

    Path configDirectory = runDirectory.resolve("clusters/process-tree");
    long childPid = Long.parseLong(Files.readString(configDirectory.resolve("child.pid")).trim());
    assertTrue(describe(failure), failure instanceof CcmProvisioner.CcmCommandException);
    assertFalse(Files.exists(configDirectory.resolve("child-alive-during-rollback")));
    assertEventuallyNotRunning(childPid);
  }

  @Test
  public void nodeRemovedFromMetadataIsStoppedAndDeleted() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-retained-node-");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "if [[ \"$1\" == node1 && \"$2\" == remove ]]; then\n"
                + "  printf 'name: cluster\\nnodes: []\\n' > \"$4/cluster/cluster.conf\"\n"
                + "  echo 'failed after metadata update'\n"
                + "  exit 42\n"
                + "fi\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster/node1");
    Path nodeBinDirectory = Files.createDirectories(nodeDirectory.resolve("bin"));
    Path fakeScylla =
        writeExecutable(
            nodeBinDirectory, "scylla", "#!/usr/bin/env bash\n" + "while true; do sleep 1; done\n");
    Process process =
        new ProcessBuilder("bash", "-c", "exec -a \"$0\" sleep 300", fakeScylla.toString()).start();
    waitForCommandLine(process.pid(), nodeDirectory.toString());
    Files.writeString(
        nodeDirectory.resolve("cassandra.pid"),
        Long.toString(process.pid()),
        StandardCharsets.US_ASCII);

    try {
      provisioner.deleteNodeState(cluster, cluster.nodes().get(0));

      assertFalse(Files.exists(nodeDirectory));
      assertEventuallyNotRunning(process.pid());
    } finally {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void truncatedPidFileDoesNotBlockNodeCleanupOrLeakUnpublishedProcess() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-truncated-node-pid-");
    Path processWasAlive = runDirectory.resolve("process-was-alive-at-ccm-remove");
    Path observedState = runDirectory.resolve("process-state-at-ccm-remove");
    Path ownedPid = runDirectory.resolve("owned.pid");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "pid=$(< \""
                + ownedPid
                + "\")\n"
                + "state=$(sed 's/.*) //' /proc/$pid/stat 2>/dev/null | cut -d' ' -f1)\n"
                + "printf '%s\\n' \"$state\" > \""
                + observedState
                + "\"\n"
                + "if [[ -n $state && $state != Z ]]; then\n"
                + "  touch \""
                + processWasAlive
                + "\"\n"
                + "fi\n"
                + "printf 'name: cluster\\nnodes: []\\n' > \"$4/cluster/cluster.conf\"\n"
                + "rm -rf \"$4/cluster/node1\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster/node1");
    Path fakeScylla =
        writeExecutable(
            Files.createDirectories(nodeDirectory.resolve("bin")),
            "scylla",
            "#!/usr/bin/env bash\nexec -a \"$0\" sleep 300\n");
    Process process =
        new ProcessBuilder(
                "bash",
                "-c",
                "\"$1\" & printf '%s\\n' $! > \"$2\"; wait",
                "wrapper",
                fakeScylla.toString(),
                ownedPid.toString())
            .start();
    waitForFile(ownedPid);
    long scyllaPid = Long.parseLong(Files.readString(ownedPid).trim());
    waitForCommandLine(scyllaPid, nodeDirectory.toString());
    Files.writeString(nodeDirectory.resolve("cassandra.pid"), "");

    try {
      provisioner.deleteNodeState(cluster, cluster.nodes().get(0));

      assertFalse(
          "Process state at CCM removal was " + Files.readString(observedState).trim(),
          Files.exists(processWasAlive));
      assertEventuallyNotRunning(scyllaPid);
    } finally {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void stoppedNodeDecommissionUsesSurvivingPeerTopology() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-decommission-probe-");
    PeerProbeProvisioner provisioner = new PeerProbeProvisioner(runDirectory);
    PhysicalTestCluster cluster = createTwoNodeCluster(provisioner, runDirectory);
    TestClusterNode target = cluster.nodes().get(1);
    AtomicReference<String> response =
        new AtomicReference<>("[{\"key\":\"127.0.88.1\",\"value\":\"peer\"}]");
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.88.1", 10000), 0);
    server.createContext(
        "/storage_service/host_id",
        exchange -> {
          byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    for (String path :
        List.of(
            "/storage_service/nodes/leaving",
            "/storage_service/nodes/joining",
            "/storage_service/nodes/moving",
            "/gossiper/endpoint/live")) {
      server.createContext(
          path,
          exchange -> {
            byte[] body = "[]".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
          });
    }
    server.start();
    try {
      assertTrue(provisioner.isNodeDecommissioned(cluster, target));

      response.set(
          "[{\"key\":\"127.0.88.1\",\"value\":\"peer\"},"
              + "{\"key\":\"127.0.88.2\",\"value\":\"target\"}]");
      assertThrows(Exception.class, () -> provisioner.isNodeDecommissioned(cluster, target));
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void yamlOverridesUseCcmYaml12ScalarSemantics() {
    assertEquals(123, CcmProvisioner.parseYamlValue("0123"));
    assertEquals("yes", CcmProvisioner.parseYamlValue("yes"));
    assertEquals("on", CcmProvisioner.parseYamlValue("on"));
    assertEquals("1:20", CcmProvisioner.parseYamlValue("1:20"));
  }

  @Test
  public void staleForeignPidIsRemovedBeforeCcmCanSignalIt() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-stale-node-pid-");
    Path invoked = runDirectory.resolve("ccm-saw-sanitized-state");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "node_dir=\"$4/cluster/node1\"\n"
                + "[[ ! -e \"$node_dir/cassandra.pid\" ]]\n"
                + "[[ ! -e \"$node_dir/scylla-agent.pid\" ]]\n"
                + "! grep -q '^pid:' \"$node_dir/node.conf\"\n"
                + "touch \""
                + invoked
                + "\"\n"
                + "printf 'name: cluster\\nnodes: []\\n' > \"$4/cluster/cluster.conf\"\n"
                + "rm -rf \"$node_dir\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster/node1");
    Files.createDirectories(nodeDirectory);
    Path debugLog = Files.createDirectories(nodeDirectory.resolve("logs")).resolve("system.log");
    Files.writeString(debugLog, "debugging\n");
    Path agentConfig =
        Files.createDirectories(nodeDirectory.resolve("conf")).resolve("scylla-manager-agent.yaml");
    Files.writeString(agentConfig, "https: 127.0.0.1:10001\n");
    Process foreign =
        new ProcessBuilder("tail", "-f", debugLog.toString())
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    Process foreignAgentConfigReader =
        new ProcessBuilder("tail", "-f", agentConfig.toString())
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    try {
      Files.writeString(nodeDirectory.resolve("cassandra.pid"), foreign.pid() + "\n");
      Files.writeString(
          nodeDirectory.resolve("scylla-agent.pid"), foreignAgentConfigReader.pid() + "\n");
      Files.writeString(
          nodeDirectory.resolve("node.conf"),
          "name: node1\nstatus: UP\npid: " + foreign.pid() + "\n",
          StandardCharsets.UTF_8);

      provisioner.deleteNodeState(cluster, cluster.nodes().get(0));

      assertTrue(Files.exists(invoked));
      assertTrue("Foreign PID must not be signaled by cleanup or CCM", foreign.isAlive());
      assertTrue(
          "A diagnostic reader of the agent config must not be signaled",
          foreignAgentConfigReader.isAlive());
    } finally {
      foreign.destroyForcibly();
      foreignAgentConfigReader.destroyForcibly();
      foreign.waitFor(5, TimeUnit.SECONDS);
      foreignAgentConfigReader.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void startDiscardsForeignPidBeforeInvokingCcm() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-stale-start-pid-");
    Path invoked = runDirectory.resolve("ccm-start-saw-sanitized-state");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "node_dir=\"$4/cluster/node1\"\n"
                + "[[ ! -e \"$node_dir/cassandra.pid\" ]]\n"
                + "! grep -q '^pid:' \"$node_dir/node.conf\"\n"
                + "touch \""
                + invoked
                + "\"\n");
    NoWaitProvisioner provisioner = new NoWaitProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster/node1");
    Files.createDirectories(nodeDirectory);
    Path debugLog = Files.createDirectories(nodeDirectory.resolve("logs")).resolve("system.log");
    Files.writeString(debugLog, "debugging\n");
    Process foreign =
        new ProcessBuilder("tail", "-f", debugLog.toString())
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    try {
      Files.writeString(nodeDirectory.resolve("cassandra.pid"), foreign.pid() + "\n");
      Files.writeString(
          nodeDirectory.resolve("node.conf"),
          "name: node1\nstatus: DOWN\npid: " + foreign.pid() + "\n",
          StandardCharsets.UTF_8);

      provisioner.startNode(cluster, cluster.nodes().get(0));

      assertTrue(Files.exists(invoked));
      assertTrue("Foreign PID must not be signaled during start", foreign.isAlive());
    } finally {
      foreign.destroyForcibly();
      foreign.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void stopDiscardsForeignPidBeforeInvokingCcm() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-stale-stop-pid-");
    Path invoked = runDirectory.resolve("ccm-stop-saw-sanitized-state");
    Path ccm =
        writeExecutable(
            runDirectory,
            "ccm",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "node_dir=\"$3/cluster/node1\"\n"
                + "[[ ! -e \"$node_dir/cassandra.pid\" ]]\n"
                + "! grep -q '^pid:' \"$node_dir/node.conf\"\n"
                + "touch \""
                + invoked
                + "\"\n");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, ccm.toString());
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory);
    Path nodeDirectory = cluster.ccmDirectory().resolve("cluster/node1");
    Path debugLog = Files.createDirectories(nodeDirectory.resolve("logs")).resolve("system.log");
    Files.writeString(debugLog, "debugging\n");
    Process foreign =
        new ProcessBuilder("tail", "-f", debugLog.toString())
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    try {
      Files.writeString(nodeDirectory.resolve("cassandra.pid"), foreign.pid() + "\n");
      Files.writeString(
          nodeDirectory.resolve("node.conf"),
          "name: node1\nstatus: UP\npid: " + foreign.pid() + "\n",
          StandardCharsets.UTF_8);

      provisioner.stop(cluster);

      assertTrue(Files.exists(invoked));
      assertTrue("Foreign PID must not be signaled during stop", foreign.isAlive());
    } finally {
      foreign.destroyForcibly();
      foreign.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void clusterSymlinkCannotRedirectNodeCleanupOutsideCcmState() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-cluster-symlink-");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, "/bin/true");
    Path ccmDirectory = Files.createDirectories(runDirectory.resolve("clusters/config"));
    Path outsideCluster = Files.createDirectories(runDirectory.resolve("outside"));
    Path outsideNode = Files.createDirectories(outsideCluster.resolve("node1"));
    Path marker = outsideNode.resolve("marker");
    Files.writeString(marker, "owned by test\n");
    Files.writeString(outsideCluster.resolve("cluster.conf"), "nodes: [node1]\n");
    Files.createSymbolicLink(ccmDirectory.resolve("cluster"), outsideCluster);
    Files.writeString(ccmDirectory.resolve("CURRENT"), "cluster\n");
    TestClusterNode node = new TestClusterNode("node1", "127.0.7.1", "dc1", "RAC1");
    PhysicalTestCluster cluster =
        new PhysicalTestCluster(
            provisioner, "cluster", 7, ccmDirectory, oneNodeSpec(), List.of(node), null, null);

    assertThrows(Exception.class, () -> provisioner.deleteNodeState(cluster, node));

    assertTrue(Files.isRegularFile(marker));
  }

  @Test
  public void configSymlinkCannotRedirectNodeCleanupOutsideRunState() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-config-symlink-");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, "/bin/true");
    Path outsideConfig = Files.createTempDirectory("ccm-outside-config-");
    Path outsideCluster = Files.createDirectories(outsideConfig.resolve("cluster/node1"));
    Path marker = outsideCluster.resolve("marker");
    Files.writeString(marker, "owned by test\n");
    Files.writeString(outsideConfig.resolve("cluster/cluster.conf"), "nodes: [node1]\n");
    Files.writeString(outsideConfig.resolve("CURRENT"), "cluster\n");
    Path ccmDirectory = runDirectory.resolve("clusters/config");
    Files.createSymbolicLink(ccmDirectory, outsideConfig);
    TestClusterNode node = new TestClusterNode("node1", "127.0.7.1", "dc1", "RAC1");
    PhysicalTestCluster cluster =
        new PhysicalTestCluster(
            provisioner, "cluster", 7, ccmDirectory, oneNodeSpec(), List.of(node), null, null);

    assertThrows(Exception.class, () -> provisioner.deleteNodeState(cluster, node));

    assertTrue(Files.isRegularFile(marker));
  }

  @Test
  public void healthCheckRestoresCallerInterruption() throws Exception {
    Path runDirectory = Files.createTempDirectory("ccm-health-interrupt-");
    CcmProvisioner provisioner = new CcmProvisioner(runDirectory, "/bin/true");
    PhysicalTestCluster cluster = createCluster(provisioner, runDirectory);

    Thread.currentThread().interrupt();
    try {
      assertFalse(provisioner.isHealthy(cluster));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private static ClusterSpec oneNodeSpec() {
    return new ClusterSpec()
        .withTopology(ClusterTopology.singleDatacenter(1))
        .withTransports(AlternatorTransport.HTTP);
  }

  private static PhysicalTestCluster createCluster(CcmProvisioner provisioner, Path runDirectory)
      throws Exception {
    Path ccmDirectory = runDirectory.resolve("clusters/config");
    Path clusterDirectory = ccmDirectory.resolve("cluster");
    Files.createDirectories(clusterDirectory);
    Files.writeString(
        clusterDirectory.resolve("cluster.conf"),
        "name: cluster\nnodes: [node1]\n",
        StandardCharsets.UTF_8);
    Files.writeString(ccmDirectory.resolve("CURRENT"), "cluster\n", StandardCharsets.UTF_8);
    TestClusterNode node = new TestClusterNode("node1", "127.0.7.1", "dc1", "RAC1");
    return new PhysicalTestCluster(
        provisioner, "cluster", 7, ccmDirectory, oneNodeSpec(), List.of(node), null, null);
  }

  private static PhysicalTestCluster createTwoNodeCluster(
      CcmProvisioner provisioner, Path runDirectory) throws Exception {
    Path ccmDirectory = runDirectory.resolve("clusters/config");
    Path clusterDirectory = ccmDirectory.resolve("cluster");
    Files.createDirectories(clusterDirectory);
    Files.writeString(
        clusterDirectory.resolve("cluster.conf"),
        "name: cluster\nnodes: [node1, node2]\n",
        StandardCharsets.UTF_8);
    Files.writeString(ccmDirectory.resolve("CURRENT"), "cluster\n", StandardCharsets.UTF_8);
    List<TestClusterNode> nodes =
        List.of(
            new TestClusterNode("node1", "127.0.88.1", "dc1", "RAC1"),
            new TestClusterNode("node2", "127.0.88.2", "dc1", "RAC1"));
    ClusterSpec spec =
        new ClusterSpec()
            .withTopology(ClusterTopology.singleDatacenter(2))
            .withTransports(AlternatorTransport.HTTP);
    return new PhysicalTestCluster(
        provisioner, "cluster", 88, ccmDirectory, spec, nodes, null, null);
  }

  private static final class PeerProbeProvisioner extends CcmProvisioner {
    PeerProbeProvisioner(Path runDirectory) throws Exception {
      super(runDirectory, "true");
    }

    @Override
    boolean isNodeRunning(PhysicalTestCluster cluster, TestClusterNode node) {
      return "node1".equals(node.name());
    }
  }

  private static final class NoWaitProvisioner extends CcmProvisioner {
    NoWaitProvisioner(Path runDirectory, String ccm) throws Exception {
      super(runDirectory, ccm);
    }

    @Override
    void waitForNodeReady(PhysicalTestCluster cluster, TestClusterNode node) {}
  }

  private static Path writeExecutable(Path directory, String name, String contents)
      throws Exception {
    Path executable = directory.resolve(name);
    Files.writeString(executable, contents, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(executable, PosixFilePermissions.fromString("rwx------"));
    return executable;
  }

  private static void assertEventuallyNotRunning(long pid) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (isRunning(pid) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse("PID remains alive after CCM cleanup: " + pid, isRunning(pid));
  }

  private static void waitForCommandLine(long pid, String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      Path commandLine = Path.of("/proc", Long.toString(pid), "cmdline");
      if (Files.isRegularFile(commandLine)) {
        String value =
            new String(Files.readAllBytes(commandLine), StandardCharsets.UTF_8).replace('\0', ' ');
        if (value.contains(expected)) {
          return;
        }
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Process command line did not contain " + expected);
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
