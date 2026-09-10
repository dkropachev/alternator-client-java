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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import org.snakeyaml.engine.v2.schema.CoreSchema;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/** Executes CCM commands and turns typed specifications into physical Scylla clusters. */
class CcmProvisioner {
  static final String PINNED_CCM_COMMIT = "d15a2fab9d22fffad8a30c806a7c8e1632e58aae";
  static final int HTTP_PORT = 8080;
  static final int HTTPS_PORT = 8043;
  static final int STORAGE_PORT = 7000;
  static final int API_PORT = 10000;
  static final int JMX_PORT = 7199;

  private static final String GOSSIPING_PROPERTY_FILE_SNITCH =
      "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
  private static final String TEST_USER = "alternator_tests";
  private static final String TEST_SALTED_PASSWORD =
      "$6$IcPWfCigHWVhHTf.$h3.30m5R2CnYqIeniCumbXCBxBxvtYPP3MbZVsjKcu268ESOcrUtSJwf1iO1s83KUT3waITRtTiexBdSWEI0Q/";
  private static final Duration READINESS_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration PROCESS_TERMINATION_GRACE = Duration.ofSeconds(2);
  private static final Duration PROCESS_KILL_TIMEOUT = Duration.ofSeconds(5);
  private static final Set<String> CCM_IMPLICIT_NODE_KEYS =
      Set.of(
          "hinted_handoff_enabled",
          "commitlog_sync",
          "commitlog_sync_period_in_ms",
          "commitlog_sync_batch_window_in_ms");

  private final String ccmExecutable;
  private final Path runDirectory;
  private final Path clustersDirectory;
  private final Path diagnosticsDirectory;
  private final Duration commandTimeout;

  CcmProvisioner(Path runDirectory) throws IOException {
    this(
        runDirectory,
        configuredDiagnosticsDirectory(),
        System.getenv().getOrDefault("SCYLLA_CCM_PATH", "ccm"),
        COMMAND_TIMEOUT,
        true);
  }

  CcmProvisioner(Path runDirectory, String ccmExecutable) throws IOException {
    this(runDirectory, runDirectory.resolve("diagnostics"), ccmExecutable, COMMAND_TIMEOUT);
  }

  CcmProvisioner(Path runDirectory, String ccmExecutable, Duration commandTimeout)
      throws IOException {
    this(runDirectory, runDirectory.resolve("diagnostics"), ccmExecutable, commandTimeout);
  }

  CcmProvisioner(
      Path runDirectory,
      String ccmExecutable,
      Duration commandTimeout,
      Duration normalNodeTerminationGrace)
      throws IOException {
    this(runDirectory, runDirectory.resolve("diagnostics"), ccmExecutable, commandTimeout);
    if (normalNodeTerminationGrace.isNegative() || normalNodeTerminationGrace.isZero()) {
      throw new IllegalArgumentException("normalNodeTerminationGrace must be positive");
    }
  }

  CcmProvisioner(
      Path runDirectory, Path diagnosticsDirectory, String ccmExecutable, Duration commandTimeout)
      throws IOException {
    this(runDirectory, diagnosticsDirectory, ccmExecutable, commandTimeout, false);
  }

  private CcmProvisioner(
      Path runDirectory,
      Path diagnosticsDirectory,
      String ccmExecutable,
      Duration commandTimeout,
      boolean operational)
      throws IOException {
    Path requestedRunDirectory = runDirectory.toAbsolutePath().normalize();
    Files.createDirectories(requestedRunDirectory);
    if (Files.isSymbolicLink(requestedRunDirectory)
        || !Files.isDirectory(requestedRunDirectory, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Refusing unsafe CCM run directory " + requestedRunDirectory);
    }
    this.runDirectory = requestedRunDirectory.toRealPath();
    this.clustersDirectory = this.runDirectory.resolve("clusters");
    try {
      Files.createDirectory(clustersDirectory);
    } catch (FileAlreadyExistsException ignored) {
      // Validate an existing path below rather than following it during recursive creation.
    }
    validateOwnedDirectory(this.runDirectory, clustersDirectory, "clusters");
    this.ccmExecutable = ccmExecutable;
    if (commandTimeout.isNegative() || commandTimeout.isZero()) {
      throw new IllegalArgumentException("commandTimeout must be positive");
    }
    this.commandTimeout = commandTimeout;
    this.diagnosticsDirectory = diagnosticsDirectory.toAbsolutePath().normalize();
    if (operational) {
      validateOperationalDirectories(this.runDirectory, this.diagnosticsDirectory);
    }
    Files.createDirectories(this.diagnosticsDirectory);
    if (Files.isSymbolicLink(this.diagnosticsDirectory)
        || !Files.isDirectory(this.diagnosticsDirectory, LinkOption.NOFOLLOW_LINKS)
        || !this.diagnosticsDirectory.equals(this.diagnosticsDirectory.toRealPath())) {
      throw new IOException("Refusing unsafe CCM diagnostics directory " + diagnosticsDirectory);
    }
  }

  private static Path configuredDiagnosticsDirectory() {
    String configured = System.getenv("SCYLLA_CCM_DIAGNOSTICS_DIR");
    return configured == null || configured.trim().isEmpty()
        ? Path.of("target", "ccm").toAbsolutePath()
        : Path.of(configured);
  }

  static void validateOperationalDirectories(Path runDirectory, Path diagnosticsDirectory)
      throws IOException {
    Path normalizedRun = runDirectory.toAbsolutePath().normalize();
    Path runsDirectory = normalizedRun.getParent();
    Path stateRoot = runsDirectory == null ? null : runsDirectory.getParent();
    if (runsDirectory == null
        || stateRoot == null
        || runsDirectory.getFileName() == null
        || !"runs".equals(runsDirectory.getFileName().toString())) {
      throw new IOException(
          "Operational CCM run directory is not beneath a state-root runs directory");
    }
    Path normalizedDiagnostics = diagnosticsDirectory.toAbsolutePath().normalize();
    if (normalizedDiagnostics.startsWith(stateRoot)
        || stateRoot.startsWith(normalizedDiagnostics)) {
      throw new IOException(
          "CCM diagnostics directory must not overlap the harness state root " + stateRoot);
    }
  }

  Path runDirectory() {
    return runDirectory;
  }

  boolean requiresJmxPortReservation(ClusterSpec spec) {
    return !ClusterSpec.DEFAULT_SCYLLA_VERSION.equals(spec.scyllaVersion())
        || !isRepositoryPinnedCcm(ccmExecutable, projectDirectory());
  }

  static boolean isRepositoryPinnedCcm(String executable, Path projectDirectory) {
    try {
      Path expectedEnvironment =
          projectDirectory
              .toAbsolutePath()
              .normalize()
              .resolve("bin/scylla-ccm-" + PINNED_CCM_COMMIT);
      Path expectedExecutable = expectedEnvironment.resolve("bin/ccm");
      Path marker = expectedEnvironment.resolve(".install-complete");
      Path candidate = Path.of(executable).toAbsolutePath().normalize();
      return Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)
          && Files.isExecutable(candidate)
          && Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)
          && !Files.isSymbolicLink(marker)
          && expectedExecutable.toRealPath().equals(candidate.toRealPath())
          && PINNED_CCM_COMMIT.equals(Files.readString(marker, StandardCharsets.US_ASCII).trim());
    } catch (IOException | RuntimeException unavailable) {
      return false;
    }
  }

  private static Path projectDirectory() {
    return Path.of(System.getProperty("basedir", System.getProperty("user.dir")));
  }

  static boolean requiresNextRunRecovery(Throwable failure) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    return requiresNextRunRecovery(failure, visited);
  }

  private static boolean requiresNextRunRecovery(Throwable failure, Set<Throwable> visited) {
    if (failure == null || !visited.add(failure)) {
      return false;
    }
    if (failure instanceof CcmProcessCleanupException
        || requiresNextRunRecovery(failure.getCause(), visited)) {
      return true;
    }
    for (Throwable suppressed : failure.getSuppressed()) {
      if (requiresNextRunRecovery(suppressed, visited)) {
        return true;
      }
    }
    return false;
  }

  PhysicalTestCluster provision(ClusterSpec spec, String instanceId, int ccmId) throws Exception {
    spec.validate();
    Path ccmDirectory = ownedChild(clustersDirectory, instanceId);
    validateOwnedDirectory(runDirectory, clustersDirectory, "clusters");
    try {
      Files.createDirectory(ccmDirectory);
    } catch (FileAlreadyExistsException ignored) {
      // A retry may reuse state created before the original command failed.
    }
    validateOwnedDirectory(clustersDirectory, ccmDirectory, "CCM config");
    List<TestClusterNode> nodes = buildNodes(spec.topology(), ccmId);
    Path caCertificatePath = null;
    AwsCredentialsProvider credentials = null;
    try {
      if (spec.transports().contains(AlternatorTransport.HTTPS)) {
        caCertificatePath = createCertificateAuthority(instanceId, ccmDirectory);
      }
      if (spec.security().enforceAlternatorAuthorization()) {
        credentials =
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(TEST_USER, TEST_SALTED_PASSWORD));
      }

      List<String> createArguments = new ArrayList<>();
      createArguments.add("create");
      createArguments.add("--config-dir");
      createArguments.add(ccmDirectory.toString());
      createArguments.add(instanceId);
      createArguments.add("--scylla");
      createArguments.add("--version");
      createArguments.add(spec.scyllaVersion());
      createArguments.add("--nodes");
      createArguments.add(firstRackCounts(spec.topology()));
      createArguments.add("--id");
      createArguments.add(Integer.toString(ccmId));
      if (spec.topology().datacenters().size() > 1) {
        createArguments.add("--snitch");
        createArguments.add(GOSSIPING_PROPERTY_FILE_SNITCH);
      }
      runCcm(ccmDirectory, createArguments);
      addAdditionalRackNodes(spec, nodes, ccmDirectory);
      configure(spec, nodes, ccmDirectory);
      if (caCertificatePath != null) {
        for (TestClusterNode node : nodes) {
          configureNodeCertificate(node, ccmDirectory, caCertificatePath);
        }
      }
      applyYamlOverrides(spec, ccmDirectory, nodes, false);
      verifyYamlOverrides(spec, ccmDirectory, nodes);

      PhysicalTestCluster cluster =
          new PhysicalTestCluster(
              this, instanceId, ccmId, ccmDirectory, spec, nodes, caCertificatePath, credentials);
      start(cluster);
      return cluster;
    } catch (Exception provisioningException) {
      boolean restoreInterrupt = clearInterrupt(provisioningException);
      try {
        Exception diagnosticFailure = null;
        try {
          collectDiagnostics(instanceId, ccmDirectory);
        } catch (Exception exception) {
          diagnosticFailure = exception;
        }
        if (provisioningException instanceof CcmProcessCleanupException) {
          IOException retainedState =
              new IOException("CCM command processes remain owned by the failed cluster");
          if (diagnosticFailure != null) {
            retainedState.addSuppressed(diagnosticFailure);
          }
          throw clusterProvisioningException(
              spec,
              instanceId,
              ccmId,
              ccmDirectory,
              nodes,
              caCertificatePath,
              credentials,
              provisioningException,
              retainedState);
        }
        if (diagnosticFailure != null) {
          throw clusterProvisioningException(
              spec,
              instanceId,
              ccmId,
              ccmDirectory,
              nodes,
              caCertificatePath,
              credentials,
              provisioningException,
              diagnosticFailure);
        }
        try {
          removeByName(instanceId, ccmDirectory);
        } catch (Exception rollbackException) {
          throw clusterProvisioningException(
              spec,
              instanceId,
              ccmId,
              ccmDirectory,
              nodes,
              caCertificatePath,
              credentials,
              provisioningException,
              rollbackException);
        }
        throw provisioningException;
      } finally {
        if (restoreInterrupt) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  void start(PhysicalTestCluster cluster) throws Exception {
    start(cluster, cluster.nodes());
  }

  void start(PhysicalTestCluster cluster, List<TestClusterNode> nodes) throws Exception {
    for (TestClusterNode node : nodes) {
      if (!isNodeRunning(cluster, node)) {
        runCcm(cluster.ccmDirectory(), startArguments(cluster, node.name()));
      }
    }
    waitForAlternator(cluster, nodes, READINESS_TIMEOUT);
  }

  void stop(PhysicalTestCluster cluster) throws Exception {
    Path clusterDirectory = currentClusterDirectory(cluster.ccmDirectory());
    if (clusterDirectory == null) {
      throw new IOException("CCM has no current cluster under " + cluster.ccmDirectory());
    }
    prepareClusterProcessReferencesForCcm(clusterDirectory);
    runCcm(
        cluster.ccmDirectory(), List.of("stop", "--config-dir", cluster.ccmDirectory().toString()));
  }

  void startNode(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    if (!isNodeRunning(cluster, node)) {
      runCcm(cluster.ccmDirectory(), startArguments(cluster, node.name()));
    }
    waitForNodeReady(cluster, node);
  }

  void stopNode(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    Path clusterDirectory = currentClusterDirectory(cluster.ccmDirectory());
    if (clusterDirectory == null) {
      throw new IOException("CCM has no current cluster under " + cluster.ccmDirectory());
    }
    prepareNodeProcessReferencesForCcm(clusterDirectory, node.name());
    runCcm(
        cluster.ccmDirectory(),
        List.of(node.name(), "stop", "--config-dir", cluster.ccmDirectory().toString()));
  }

  TestClusterNode addNode(PhysicalTestCluster cluster, String datacenter, String rack)
      throws Exception {
    if (cluster.nodes().size() >= ClusterSpec.MAXIMUM_NODE_COUNT) {
      throw new IllegalStateException(
          "A cluster cannot exceed " + ClusterSpec.MAXIMUM_NODE_COUNT + " nodes");
    }
    int index = 1;
    while (containsNode(cluster.nodes(), "node" + index)) {
      index++;
    }
    TestClusterNode node =
        new TestClusterNode(
            "node" + index, "127.0." + cluster.ccmId() + "." + index, datacenter, rack);
    try {
      runCcm(
          cluster.ccmDirectory(),
          List.of(
              "add",
              "--config-dir",
              cluster.ccmDirectory().toString(),
              node.name(),
              "--scylla",
              "--seeds",
              "--auto-bootstrap",
              "--itf",
              node.address(),
              "--data-center",
              datacenter,
              "--rack",
              rack));
      if (cluster.caCertificatePath() != null) {
        configureNodeCertificate(node, cluster.ccmDirectory(), cluster.caCertificatePath());
      }
      applyYamlOverrides(cluster.spec(), cluster.ccmDirectory(), List.of(node), false);
      verifyYamlOverrides(cluster.spec(), cluster.ccmDirectory(), List.of(node));
      startNode(cluster, node);
      return node;
    } catch (Exception provisioningException) {
      boolean restoreInterrupt = clearInterrupt(provisioningException);
      try {
        Exception diagnosticFailure = null;
        try {
          collectDiagnostics(cluster.instanceId(), cluster.ccmDirectory());
        } catch (Exception exception) {
          diagnosticFailure = exception;
        }
        if (provisioningException instanceof CcmProcessCleanupException) {
          IOException retainedState =
              new IOException("CCM command processes remain owned by the failed node");
          if (diagnosticFailure != null) {
            retainedState.addSuppressed(diagnosticFailure);
          }
          throw new CcmNodeProvisioningException(node, true, provisioningException, retainedState);
        }
        if (diagnosticFailure != null) {
          throw new CcmNodeProvisioningException(
              node, true, provisioningException, diagnosticFailure);
        }
        try {
          removeNodeByName(cluster.ccmDirectory(), node.name());
        } catch (Exception rollbackException) {
          throw new CcmNodeProvisioningException(
              node, true, provisioningException, rollbackException);
        }
        throw new CcmNodeProvisioningException(node, false, provisioningException, null);
      } finally {
        if (restoreInterrupt) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  void decommissionNode(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    runCcm(
        cluster.ccmDirectory(),
        List.of(node.name(), "decommission", "--config-dir", cluster.ccmDirectory().toString()));
  }

  void deleteNodeState(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    collectDiagnostics(cluster.instanceId(), cluster.ccmDirectory());
    removeNodeByName(cluster.ccmDirectory(), node.name());
  }

  boolean isHealthy(PhysicalTestCluster cluster) {
    try {
      waitForAlternator(cluster, cluster.nodes(), Duration.ofSeconds(10));
      return true;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception ignored) {
      return false;
    }
  }

  boolean isNodeRunning(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    Path currentCluster = currentClusterDirectory(cluster.ccmDirectory());
    if (currentCluster == null) {
      return false;
    }
    Path nodeDirectory = ownedChild(currentCluster, node.name());
    if (!Files.exists(nodeDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return false;
    }
    validateOwnedDirectory(currentCluster, nodeDirectory, "node");
    for (long pid : readScyllaPids(nodeDirectory)) {
      java.util.Optional<ProcessHandle> process = ProcessHandle.of(pid);
      if (process.isPresent() && isProcessAlive(process.get())) {
        if (!nodeProcessBelongsTo(process.get(), nodeDirectory)) {
          throw new IOException(
              "CCM node '" + node.name() + "' references an unrelated live PID " + pid);
        }
        return true;
      }
    }
    return false;
  }

  void waitForNodeReady(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    waitForAlternator(cluster, List.of(node), READINESS_TIMEOUT);
  }

  boolean isNodeDecommissioned(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    Path currentCluster = currentClusterDirectory(cluster.ccmDirectory());
    if (currentCluster == null) {
      throw new IOException("CCM has no current cluster under " + cluster.ccmDirectory());
    }
    Path nodeDirectory = ownedChild(currentCluster, node.name());
    Path nodeConfig = nodeDirectory.resolve("node.conf");
    if (Files.isRegularFile(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
      Object status = readYamlMap(nodeConfig).get("status");
      if (status != null && "DECOMMISSIONED".equalsIgnoreCase(status.toString())) {
        return true;
      }
    }
    if (!isNodeRunning(cluster, node)) {
      return isNodeAbsentFromPeerTopology(cluster, node);
    }
    String response =
        getBody(
                URI.create(
                        "http://"
                            + node.address()
                            + ":"
                            + API_PORT
                            + "/storage_service/operation_mode")
                    .toURL())
            .trim();
    String mode =
        response.length() >= 2 && response.startsWith("\"") && response.endsWith("\"")
            ? response.substring(1, response.length() - 1)
            : response;
    if ("DECOMMISSIONED".equalsIgnoreCase(mode)) {
      return true;
    }
    if ("NORMAL".equalsIgnoreCase(mode)) {
      return false;
    }
    throw new IOException(
        "Node '" + node.name() + "' has indeterminate operation mode " + response);
  }

  private boolean isNodeAbsentFromPeerTopology(PhysicalTestCluster cluster, TestClusterNode target)
      throws Exception {
    Exception lastFailure = null;
    boolean observedPeerTopology = false;
    for (TestClusterNode peer : cluster.nodes()) {
      if (peer == target) {
        continue;
      }
      try {
        if (!isNodeRunning(cluster, peer)) {
          continue;
        }
        String response =
            getBody(
                URI.create("http://" + peer.address() + ":" + API_PORT + "/storage_service/host_id")
                    .toURL());
        Object parsed = parseYamlValue(response);
        if (!(parsed instanceof Iterable)) {
          throw new IOException("Invalid host ID topology response from " + peer.address());
        }
        for (Object mapping : (Iterable<?>) parsed) {
          if (mapping instanceof Map
              && target.address().equals(String.valueOf(((Map<?, ?>) mapping).get("key")))) {
            throw new IndeterminateNodeStateException(
                "Stopped node '"
                    + target.name()
                    + "' remains in peer topology; decommission completion is ambiguous");
          }
        }
        assertAddressAbsent(
            peer, target, "/storage_service/nodes/leaving", "leaving-node topology");
        assertAddressAbsent(
            peer, target, "/storage_service/nodes/joining", "joining-node topology");
        assertAddressAbsent(peer, target, "/storage_service/nodes/moving", "moving-node topology");
        assertAddressAbsent(peer, target, "/gossiper/endpoint/live", "live gossip topology");
        observedPeerTopology = true;
      } catch (IndeterminateNodeStateException exception) {
        throw exception;
      } catch (Exception exception) {
        if (lastFailure == null) {
          lastFailure = exception;
        } else {
          lastFailure.addSuppressed(exception);
        }
      }
    }
    if (observedPeerTopology && lastFailure == null) {
      return true;
    }
    throw new IOException(
        "Cannot determine decommission state of stopped node '" + target.name() + "'", lastFailure);
  }

  private static void assertAddressAbsent(
      TestClusterNode peer, TestClusterNode target, String path, String description)
      throws Exception {
    String response =
        getBody(URI.create("http://" + peer.address() + ":" + API_PORT + path).toURL());
    Object parsed = parseYamlValue(response);
    if (!(parsed instanceof Iterable)) {
      throw new IOException("Invalid " + description + " response from " + peer.address());
    }
    for (Object address : (Iterable<?>) parsed) {
      if (target.address().equals(String.valueOf(address))) {
        throw new IndeterminateNodeStateException(
            "Stopped node '"
                + target.name()
                + "' remains in "
                + description
                + "; decommission completion is ambiguous");
      }
    }
  }

  void remove(PhysicalTestCluster cluster) throws Exception {
    collectDiagnostics(cluster.instanceId(), cluster.ccmDirectory());
    removeByName(cluster.instanceId(), cluster.ccmDirectory());
  }

  private static List<String> startArguments(PhysicalTestCluster cluster, String nodeName) {
    List<String> arguments = new ArrayList<>();
    if (nodeName != null) {
      arguments.add(nodeName);
    }
    arguments.add("start");
    arguments.add("--config-dir");
    arguments.add(cluster.ccmDirectory().toString());
    arguments.add("--wait-for-binary-proto");
    arguments.add("--wait-other-notice");
    arguments.add("--jvm_arg=--smp");
    arguments.add("--jvm_arg=" + cluster.spec().resources().smp());
    arguments.add("--jvm_arg=--memory");
    arguments.add("--jvm_arg=" + cluster.spec().resources().memoryMiB() + "M");
    return arguments;
  }

  private static String firstRackCounts(ClusterTopology topology) {
    StringBuilder counts = new StringBuilder();
    for (DatacenterSpec datacenter : topology.datacenters()) {
      if (counts.length() > 0) {
        counts.append(':');
      }
      counts.append(datacenter.racks().get(0).nodeCount());
    }
    return counts.toString();
  }

  private static List<TestClusterNode> buildNodes(ClusterTopology topology, int ccmId) {
    List<TestClusterNode> nodes = new ArrayList<>();
    int index = 0;
    for (int dcIndex = 0; dcIndex < topology.datacenters().size(); dcIndex++) {
      int count = topology.datacenters().get(dcIndex).racks().get(0).nodeCount();
      for (int nodeIndex = 0; nodeIndex < count; nodeIndex++) {
        nodes.add(createNode(++index, ccmId, dcIndex, 0));
      }
    }
    for (int dcIndex = 0; dcIndex < topology.datacenters().size(); dcIndex++) {
      DatacenterSpec datacenter = topology.datacenters().get(dcIndex);
      for (int rackIndex = 1; rackIndex < datacenter.racks().size(); rackIndex++) {
        for (int nodeIndex = 0;
            nodeIndex < datacenter.racks().get(rackIndex).nodeCount();
            nodeIndex++) {
          nodes.add(createNode(++index, ccmId, dcIndex, rackIndex));
        }
      }
    }
    return nodes;
  }

  private static TestClusterNode createNode(
      int index, int ccmId, int datacenterIndex, int rackIndex) {
    return new TestClusterNode(
        "node" + index,
        "127.0." + ccmId + "." + index,
        "dc" + (datacenterIndex + 1),
        "RAC" + (rackIndex + 1));
  }

  private void addAdditionalRackNodes(
      ClusterSpec spec, List<TestClusterNode> nodes, Path ccmDirectory) throws Exception {
    int firstRackNodeCount = 0;
    for (DatacenterSpec datacenter : spec.topology().datacenters()) {
      firstRackNodeCount += datacenter.racks().get(0).nodeCount();
    }
    for (int index = firstRackNodeCount; index < nodes.size(); index++) {
      TestClusterNode node = nodes.get(index);
      runCcm(
          ccmDirectory,
          List.of(
              "add",
              "--config-dir",
              ccmDirectory.toString(),
              node.name(),
              "--scylla",
              "--seeds",
              "--itf",
              node.address(),
              "--data-center",
              node.datacenter(),
              "--rack",
              node.rack()));
    }
  }

  private void configure(ClusterSpec spec, List<TestClusterNode> nodes, Path ccmDirectory)
      throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("alternator_write_isolation", "only_rmw_uses_lwt");
    options.put("endpoint_snitch", GOSSIPING_PROPERTY_FILE_SNITCH);
    options.put("start_native_transport", "true");
    if (spec.transports().contains(AlternatorTransport.HTTP)) {
      options.put("alternator_port", Integer.toString(HTTP_PORT));
    }
    if (spec.transports().contains(AlternatorTransport.HTTPS)) {
      options.put("alternator_https_port", Integer.toString(HTTPS_PORT));
      options.put("alternator_encryption_options.enable_session_tickets", "true");
    }
    if (spec.security().authentication() != AuthenticationMode.ALLOW_ALL) {
      options.put("authenticator", authenticator(spec.security().authentication()));
      options.put("auth_superuser_name", TEST_USER);
      options.put("auth_superuser_salted_password", TEST_SALTED_PASSWORD);
    }
    if (spec.security().authorization() != AuthorizationMode.ALLOW_ALL) {
      options.put("authorizer", authorizer(spec.security().authorization()));
    }
    if (spec.security().enforceAlternatorAuthorization()) {
      options.put("alternator_enforce_authorization", "true");
    }
    options.putAll(spec.scyllaYamlOverrides());
    List<String> arguments = new ArrayList<>();
    arguments.add("updateconf");
    arguments.add("--config-dir");
    arguments.add(ccmDirectory.toString());
    options.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(option -> arguments.add(option.getKey() + ":" + option.getValue()));
    runCcm(ccmDirectory, arguments);
    applyYamlOverrides(spec, ccmDirectory, nodes, true);
  }

  private void applyYamlOverrides(
      ClusterSpec spec, Path ccmDirectory, List<TestClusterNode> nodes, boolean updateClusterState)
      throws IOException {
    if (spec.scyllaYamlOverrides().isEmpty()) {
      return;
    }
    Path clusterDirectory = currentClusterDirectory(ccmDirectory);
    if (clusterDirectory == null) {
      throw new IOException("CCM has no current cluster under " + ccmDirectory);
    }
    Map<String, String> implicitOverrideText = new LinkedHashMap<>();
    for (Map.Entry<String, String> override : spec.scyllaYamlOverrides().entrySet()) {
      if (CCM_IMPLICIT_NODE_KEYS.contains(override.getKey())) {
        implicitOverrideText.put(override.getKey(), override.getValue());
      }
    }
    if (implicitOverrideText.isEmpty()) {
      return;
    }
    Map<String, Object> overrides = parseYamlOverrides(implicitOverrideText);
    if (updateClusterState) {
      Path clusterConfig = clusterDirectory.resolve("cluster.conf");
      Map<String, Object> clusterYaml = readYamlMap(clusterConfig);
      Map<String, Object> configOptions = childMap(clusterYaml, "config_options", true);
      applyDottedValues(configOptions, overrides);
      writeYamlMap(clusterConfig, clusterYaml);
    }

    Set<String> overriddenImplicitKeys = new HashSet<>(overrides.keySet());
    overriddenImplicitKeys.retainAll(CCM_IMPLICIT_NODE_KEYS);
    for (TestClusterNode node : nodes) {
      Path nodeDirectory = ownedChild(clusterDirectory, node.name());
      Path nodeConfig = nodeDirectory.resolve("node.conf");
      if (!overriddenImplicitKeys.isEmpty() && Files.isRegularFile(nodeConfig)) {
        Map<String, Object> nodeYaml = readYamlMap(nodeConfig);
        Map<String, Object> configOptions = childMap(nodeYaml, "config_options", false);
        if (configOptions != null) {
          overriddenImplicitKeys.forEach(configOptions::remove);
          writeYamlMap(nodeConfig, nodeYaml);
        }
      }

      Path scyllaConfig = nodeDirectory.resolve("conf").resolve("scylla.yaml");
      Map<String, Object> scyllaYaml = readYamlMap(scyllaConfig);
      applyDottedValues(scyllaYaml, overrides);
      writeYamlMap(scyllaConfig, scyllaYaml);
    }
  }

  private void verifyYamlOverrides(ClusterSpec spec, Path ccmDirectory, List<TestClusterNode> nodes)
      throws IOException {
    if (spec.scyllaYamlOverrides().isEmpty()) {
      return;
    }
    Path clusterDirectory = currentClusterDirectory(ccmDirectory);
    if (clusterDirectory == null) {
      throw new IOException("CCM has no current cluster under " + ccmDirectory);
    }
    Map<String, String> implicitOverrideText = new LinkedHashMap<>();
    for (Map.Entry<String, String> override : spec.scyllaYamlOverrides().entrySet()) {
      if (CCM_IMPLICIT_NODE_KEYS.contains(override.getKey())) {
        implicitOverrideText.put(override.getKey(), override.getValue());
      }
    }
    if (implicitOverrideText.isEmpty()) {
      return;
    }
    Map<String, Object> expected = parseYamlOverrides(implicitOverrideText);
    Map<String, Object> clusterYaml = readYamlMap(clusterDirectory.resolve("cluster.conf"));
    Map<String, Object> configOptions = childMap(clusterYaml, "config_options", false);
    verifyDottedValues(
        configOptions,
        expected,
        "CCM cluster configuration " + clusterDirectory.resolve("cluster.conf"));
    for (TestClusterNode node : nodes) {
      Path scyllaConfig =
          ownedChild(clusterDirectory, node.name()).resolve("conf").resolve("scylla.yaml");
      verifyDottedValues(
          readYamlMap(scyllaConfig), expected, "Scylla configuration " + scyllaConfig);
    }
  }

  private static Map<String, Object> parseYamlOverrides(Map<String, String> overrides)
      throws IOException {
    Map<String, Object> parsed = new LinkedHashMap<>();
    try {
      for (Map.Entry<String, String> override : overrides.entrySet()) {
        parsed.put(override.getKey(), parseYamlValue(override.getValue()));
      }
      return parsed;
    } catch (RuntimeException exception) {
      throw new IOException("Unable to parse a Scylla YAML override", exception);
    }
  }

  private static void applyDottedValues(Map<String, Object> destination, Map<String, Object> values)
      throws IOException {
    for (Map.Entry<String, Object> value : values.entrySet()) {
      String[] path = value.getKey().split("\\.", -1);
      if (path.length == 1) {
        destination.put(path[0], value.getValue());
      } else if (path.length == 2) {
        childMap(destination, path[0], true).put(path[1], value.getValue());
      } else {
        throw new IOException("Unsupported nested Scylla YAML key " + value.getKey());
      }
    }
  }

  private static void verifyDottedValues(
      Map<String, Object> actual, Map<String, Object> expected, String description)
      throws IOException {
    if (actual == null) {
      throw new IOException(description + " has no configuration options");
    }
    for (Map.Entry<String, Object> entry : expected.entrySet()) {
      String[] path = entry.getKey().split("\\.", -1);
      Map<String, Object> parent = actual;
      String leaf = path[path.length - 1];
      if (path.length == 2) {
        parent = childMap(actual, path[0], false);
      }
      if (parent == null
          || !parent.containsKey(leaf)
          || !Objects.equals(parent.get(leaf), entry.getValue())) {
        throw new IOException(
            description
                + " does not preserve override '"
                + entry.getKey()
                + "' (expected "
                + entry.getValue()
                + ")");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> childMap(
      Map<String, Object> parent, String key, boolean create) throws IOException {
    Object existing = parent.get(key);
    if (existing == null) {
      if (!create) {
        return null;
      }
      Map<String, Object> child = new LinkedHashMap<>();
      parent.put(key, child);
      return child;
    }
    if (!(existing instanceof Map)) {
      if (!create) {
        return null;
      }
      throw new IOException("Scylla YAML key '" + key + "' is not a mapping");
    }
    return (Map<String, Object>) existing;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readYamlMap(Path path) throws IOException {
    rejectSymlink(path);
    try {
      Object loaded = parseYamlValue(Files.readString(path, StandardCharsets.UTF_8));
      if (!(loaded instanceof Map)) {
        throw new IOException("Expected a YAML mapping in " + path);
      }
      return (Map<String, Object>) loaded;
    } catch (RuntimeException exception) {
      throw new IOException("Unable to parse YAML file " + path, exception);
    }
  }

  private static void writeYamlMap(Path path, Map<String, Object> contents) throws IOException {
    rejectSymlink(path);
    String yaml = dumpYamlMap(contents);
    Path temporary = Files.createTempFile(path.getParent(), path.getFileName().toString(), ".tmp");
    try {
      Files.writeString(temporary, yaml, StandardCharsets.UTF_8);
      try {
        Files.move(
            temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException ignored) {
        Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static String dumpYamlMap(Map<String, Object> contents) {
    DumpSettings options =
        DumpSettings.builder()
            .setDefaultFlowStyle(FlowStyle.BLOCK)
            .setSplitLines(false)
            .setSchema(new CoreSchema())
            .build();
    return new Dump(options).dumpToString(contents);
  }

  static Object parseYamlValue(String yaml) {
    return ClusterSpec.parseYamlValue(yaml);
  }

  private static Path ownedChild(Path parent, String child) throws IOException {
    Path normalizedParent = parent.toAbsolutePath().normalize();
    Path normalizedChild = normalizedParent.resolve(child).normalize();
    if (!normalizedChild.getParent().equals(normalizedParent)) {
      throw new IOException("Refusing path outside CCM cluster directory: " + normalizedChild);
    }
    return normalizedChild;
  }

  private static void rejectSymlink(Path path) throws IOException {
    if (Files.isSymbolicLink(path)
        || !Files.exists(path, LinkOption.NOFOLLOW_LINKS)
        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Refusing non-regular CCM configuration file " + path);
    }
  }

  private void prepareClusterProcessReferencesForCcm(Path clusterDirectory) throws IOException {
    Map<String, Object> cluster = readYamlMap(clusterDirectory.resolve("cluster.conf"));
    Object configuredNodes = cluster.get("nodes");
    if (!(configuredNodes instanceof Iterable)) {
      throw new IOException("Invalid nodes list in " + clusterDirectory.resolve("cluster.conf"));
    }
    for (Object configuredNode : (Iterable<?>) configuredNodes) {
      if (!(configuredNode instanceof String) || !isCcmNodeName((String) configuredNode)) {
        throw new IOException(
            "Unsafe node name in "
                + clusterDirectory.resolve("cluster.conf")
                + ": "
                + configuredNode);
      }
      prepareNodeProcessReferencesForCcm(clusterDirectory, (String) configuredNode);
    }
  }

  private void prepareNodeProcessReferencesForCcm(Path clusterDirectory, String nodeName)
      throws IOException {
    Path nodeDirectory = ownedChild(clusterDirectory, nodeName);
    if (!Files.exists(nodeDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedDirectory(clusterDirectory, nodeDirectory, "node");

    Path nodeConfig = nodeDirectory.resolve("node.conf");
    Long configuredPid = null;
    ProcessReferenceState configuredState = ProcessReferenceState.ABSENT;
    if (Files.exists(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
      Map<String, Object> config = readYamlMap(nodeConfig);
      validateNativeNodeMetadata(config, nodeDirectory);
      Object value = config.get("pid");
      if (value != null) {
        configuredPid = parsePidReference(value.toString(), nodeConfig);
        configuredState =
            inspectProcessReference(
                configuredPid, nodeConfig, nodeDirectory, ProcessReferenceKind.SCYLLA);
      }
    }

    Path scyllaPidFile = nodeDirectory.resolve("cassandra.pid");
    Long scyllaPid = readPidReference(scyllaPidFile);
    ProcessReferenceState scyllaState =
        inspectProcessReference(
            scyllaPid, scyllaPidFile, nodeDirectory, ProcessReferenceKind.SCYLLA);
    if (scyllaState == ProcessReferenceState.OWNED
        && (configuredState != ProcessReferenceState.OWNED || !scyllaPid.equals(configuredPid))) {
      throw new CcmProcessCleanupException(
          "CCM cannot safely stop Scylla PID "
              + scyllaPid
              + " because node.conf does not reference the same owned process");
    }

    Path jmxPidFile = nodeDirectory.resolve("scylla-jmx.pid");
    Long jmxPid = readPidReference(jmxPidFile);
    ProcessReferenceState jmxState =
        inspectProcessReference(jmxPid, jmxPidFile, nodeDirectory, ProcessReferenceKind.JMX);
    Path agentPidFile = nodeDirectory.resolve("scylla-agent.pid");
    Long agentPid = readPidReference(agentPidFile);
    ProcessReferenceState agentState =
        inspectProcessReference(agentPid, agentPidFile, nodeDirectory, ProcessReferenceKind.AGENT);

    if (configuredState == ProcessReferenceState.DEAD) {
      sanitizeStaleNodeConfig(nodeConfig, nodeDirectory);
    }
    deleteDeadPidReference(scyllaPidFile, scyllaState);
    deleteDeadPidReference(jmxPidFile, jmxState);
    deleteDeadPidReference(agentPidFile, agentState);
  }

  private ProcessReferenceState inspectProcessReference(
      Long pid, Path source, Path nodeDirectory, ProcessReferenceKind kind) throws IOException {
    if (pid == null) {
      return ProcessReferenceState.ABSENT;
    }
    java.util.Optional<ProcessHandle> process = ProcessHandle.of(pid);
    if (process.isEmpty() || !isProcessAlive(process.get())) {
      return ProcessReferenceState.DEAD;
    }

    Path processDirectory = Path.of("/proc", Long.toString(pid));
    long startTicks = -1;
    try {
      startTicks = readProcessStartTicks(pid);
      if (!Files.getOwner(Path.of("/proc/self/status"), LinkOption.NOFOLLOW_LINKS)
          .equals(Files.getOwner(processDirectory, LinkOption.NOFOLLOW_LINKS))) {
        throw new CcmProcessCleanupException(
            "CCM process reference " + source + " points to foreign live PID " + pid);
      }
      byte[] environment = Files.readAllBytes(processDirectory.resolve("environ"));
      if (!containsEnvironmentEntry(environment, "SCYLLA_CCM_RUN_DIR=" + runDirectory.toString())) {
        if (!isSameLiveProcess(pid, startTicks)) {
          return ProcessReferenceState.DEAD;
        }
        throw new CcmProcessCleanupException(
            "CCM process reference " + source + " points to unrelated live PID " + pid);
      }
      List<String> arguments = readProcessArguments(processDirectory.resolve("cmdline"));
      if (!matchesExpectedProcess(arguments, nodeDirectory, kind)
          || !matchesExpectedExecutable(processDirectory, arguments, kind)) {
        if (!isSameLiveProcess(pid, startTicks)) {
          return ProcessReferenceState.DEAD;
        }
        throw new CcmProcessCleanupException(
            "CCM process reference "
                + source
                + " points to the wrong "
                + kind.description
                + " process "
                + pid);
      }
      if (!isSameLiveProcess(pid, startTicks)) {
        return ProcessReferenceState.DEAD;
      }
      return ProcessReferenceState.OWNED;
    } catch (CcmProcessCleanupException exception) {
      throw exception;
    } catch (IOException exception) {
      if (startTicks >= 0 && !isSameLiveProcess(pid, startTicks)) {
        return ProcessReferenceState.DEAD;
      }
      process = ProcessHandle.of(pid);
      if (process.isEmpty() || !isProcessAlive(process.get())) {
        return ProcessReferenceState.DEAD;
      }
      throw new CcmProcessCleanupException(
          "Cannot prove ownership of live PID " + pid + " referenced by " + source, exception);
    }
  }

  private static boolean matchesExpectedProcess(
      List<String> arguments, Path nodeDirectory, ProcessReferenceKind kind) {
    if (arguments.isEmpty()) {
      return false;
    }
    Path normalizedNode = nodeDirectory.toAbsolutePath().normalize();
    switch (kind) {
      case SCYLLA:
        return arguments.get(0).equals(normalizedNode.resolve("bin/scylla").toString());
      case JMX:
        return matchesExpectedJmxArguments(arguments, normalizedNode);
      case AGENT:
        Path executable;
        try {
          executable = Path.of(arguments.get(0));
        } catch (RuntimeException invalid) {
          return false;
        }
        return executable.isAbsolute()
            && executable.getFileName() != null
            && executable.getFileName().toString().equals("scylla-manager-agent")
            && containsArgumentPair(
                arguments,
                "--config-file",
                normalizedNode.resolve("conf/scylla-manager-agent.yaml").toString());
      default:
        throw new IllegalStateException("Unknown CCM process reference kind " + kind);
    }
  }

  static boolean matchesExpectedJmxArguments(List<String> arguments, Path nodeDirectory) {
    if (arguments.isEmpty()) {
      return false;
    }
    Path normalizedNode = nodeDirectory.toAbsolutePath().normalize();
    boolean expectedLauncher =
        arguments.get(0).equals(normalizedNode.resolve("bin/symlinks/scylla-jmx").toString());
    boolean expectedJava = executableBaseName(arguments.get(0), "java");
    return (expectedLauncher || expectedJava)
        && containsArgumentPair(
            arguments, "-jar", normalizedNode.resolve("bin/scylla-jmx-1.0.jar").toString());
  }

  private static boolean matchesExpectedExecutable(
      Path processDirectory, List<String> arguments, ProcessReferenceKind kind) throws IOException {
    if (kind != ProcessReferenceKind.JMX
        || arguments.isEmpty()
        || !executableBaseName(arguments.get(0), "java")) {
      return true;
    }
    Path executable = Files.readSymbolicLink(processDirectory.resolve("exe"));
    Path fileName = executable.getFileName();
    return fileName != null
        && (fileName.toString().equals("java") || fileName.toString().equals("java (deleted)"));
  }

  private static boolean executableBaseName(String executable, String expected) {
    try {
      Path fileName = Path.of(executable).getFileName();
      return fileName != null && fileName.toString().equals(expected);
    } catch (RuntimeException invalid) {
      return false;
    }
  }

  private static boolean containsArgumentPair(
      List<String> arguments, String option, String expectedValue) {
    for (int index = 0; index + 1 < arguments.size(); index++) {
      if (arguments.get(index).equals(option) && arguments.get(index + 1).equals(expectedValue)) {
        return true;
      }
    }
    return false;
  }

  private static List<String> readProcessArguments(Path commandLine) throws IOException {
    byte[] bytes = Files.readAllBytes(commandLine);
    List<String> arguments = new ArrayList<>();
    int start = 0;
    for (int index = 0; index <= bytes.length; index++) {
      if (index == bytes.length || bytes[index] == 0) {
        if (index > start) {
          arguments.add(new String(bytes, start, index - start, StandardCharsets.UTF_8));
        }
        start = index + 1;
      }
    }
    return arguments;
  }

  private static boolean isSameLiveProcess(long pid, long expectedStartTicks) throws IOException {
    java.util.Optional<ProcessHandle> process = ProcessHandle.of(pid);
    return process.isPresent()
        && isProcessAlive(process.get())
        && readProcessStartTicks(pid) == expectedStartTicks;
  }

  private static long readProcessStartTicks(long pid) throws IOException {
    String stat =
        Files.readString(Path.of("/proc", Long.toString(pid), "stat"), StandardCharsets.US_ASCII);
    int commandEnd = stat.lastIndexOf(')');
    if (commandEnd < 0 || commandEnd + 2 >= stat.length()) {
      throw new IOException("Unable to parse process identity for PID " + pid);
    }
    String[] fields = stat.substring(commandEnd + 2).split("\\s+");
    if (fields.length <= 19) {
      throw new IOException("Unable to parse process identity for PID " + pid);
    }
    try {
      return Long.parseLong(fields[19]);
    } catch (NumberFormatException exception) {
      throw new IOException("Unable to parse process identity for PID " + pid, exception);
    }
  }

  private static Long readPidReference(Path path) throws IOException {
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    rejectSymlink(path);
    return parsePidReference(Files.readString(path, StandardCharsets.US_ASCII).trim(), path);
  }

  private static long parsePidReference(String value, Path source) throws IOException {
    Set<Long> parsed = new HashSet<>();
    addPid(value, source, parsed);
    return parsed.iterator().next();
  }

  private static void deleteDeadPidReference(Path path, ProcessReferenceState state)
      throws IOException {
    if (state == ProcessReferenceState.DEAD) {
      rejectSymlink(path);
      Files.delete(path);
    }
  }

  private static boolean containsEnvironmentEntry(byte[] environment, String expected) {
    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    int start = 0;
    for (int index = 0; index <= environment.length; index++) {
      if (index == environment.length || environment[index] == 0) {
        if (index - start == expectedBytes.length) {
          boolean equal = true;
          for (int offset = 0; offset < expectedBytes.length; offset++) {
            if (environment[start + offset] != expectedBytes[offset]) {
              equal = false;
              break;
            }
          }
          if (equal) {
            return true;
          }
        }
        start = index + 1;
      }
    }
    return false;
  }

  private enum ProcessReferenceState {
    ABSENT,
    DEAD,
    OWNED
  }

  private enum ProcessReferenceKind {
    SCYLLA("Scylla"),
    JMX("JMX"),
    AGENT("manager agent");

    final String description;

    ProcessReferenceKind(String description) {
      this.description = description;
    }
  }

  private static Set<Long> readScyllaPids(Path nodeDirectory) throws IOException {
    Set<Long> pids = new HashSet<>();
    readPidFile(nodeDirectory.resolve("cassandra.pid"), pids);
    Path nodeConfig = nodeDirectory.resolve("node.conf");
    if (Files.exists(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
      Object configuredPid = readYamlMap(nodeConfig).get("pid");
      if (configuredPid != null) {
        addPid(configuredPid.toString(), nodeConfig, pids);
      }
    }
    return pids;
  }

  private static void readPidFile(Path path, Set<Long> pids) throws IOException {
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    rejectSymlink(path);
    addPid(Files.readString(path, StandardCharsets.US_ASCII).trim(), path, pids);
  }

  private static void addPid(String value, Path source, Set<Long> pids) throws IOException {
    try {
      long pid = Long.parseLong(value);
      if (pid < 2) {
        throw new NumberFormatException();
      }
      pids.add(pid);
    } catch (NumberFormatException exception) {
      throw new IOException("Invalid process ID in " + source, exception);
    }
  }

  private static boolean isProcessAlive(ProcessHandle process) {
    if (!process.isAlive()) {
      return false;
    }
    try {
      String stat =
          Files.readString(
              Path.of("/proc", Long.toString(process.pid()), "stat"), StandardCharsets.US_ASCII);
      int commandEnd = stat.lastIndexOf(')');
      return commandEnd < 0
          || commandEnd + 2 >= stat.length()
          || stat.charAt(commandEnd + 2) != 'Z';
    } catch (IOException ignored) {
      return process.isAlive();
    }
  }

  private static boolean nodeProcessBelongsTo(ProcessHandle process, Path nodeDirectory)
      throws IOException {
    Path commandLine = Path.of("/proc", Long.toString(process.pid()), "cmdline");
    byte[] bytes;
    try {
      bytes = Files.readAllBytes(commandLine);
    } catch (IOException exception) {
      if (!isProcessAlive(process)) {
        return false;
      }
      throw new IOException("Unable to inspect CCM node PID " + process.pid(), exception);
    }
    int end = 0;
    while (end < bytes.length && bytes[end] != 0) {
      end++;
    }
    String executable = new String(bytes, 0, end, StandardCharsets.UTF_8);
    return executable.equals(nodeDirectory.resolve("bin/scylla").toString());
  }

  private static String authenticator(AuthenticationMode mode) {
    switch (mode) {
      case PASSWORD:
        return "org.apache.cassandra.auth.PasswordAuthenticator";
      case TRANSITIONAL:
        return "com.scylladb.auth.TransitionalAuthenticator";
      default:
        throw new IllegalArgumentException("No authenticator for " + mode);
    }
  }

  private static String authorizer(AuthorizationMode mode) {
    switch (mode) {
      case CASSANDRA:
        return "org.apache.cassandra.auth.CassandraAuthorizer";
      case TRANSITIONAL:
        return "com.scylladb.auth.TransitionalAuthorizer";
      default:
        throw new IllegalArgumentException("No authorizer for " + mode);
    }
  }

  private Path createCertificateAuthority(String instanceId, Path ccmDirectory) throws Exception {
    Path tlsDirectory = ccmDirectory.resolve("tls");
    Files.createDirectories(tlsDirectory);
    Path certificatePath = tlsDirectory.resolve("ca.crt");
    runCommand(
        ccmDirectory,
        List.of(
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:3072",
            "-sha256",
            "-nodes",
            "-days",
            "730",
            "-subj",
            "/CN=" + instanceId + " test CA",
            "-keyout",
            tlsDirectory.resolve("ca.key").toString(),
            "-out",
            certificatePath.toString()));
    return certificatePath;
  }

  private void configureNodeCertificate(
      TestClusterNode node, Path ccmDirectory, Path caCertificatePath) throws Exception {
    Path tlsDirectory = ccmDirectory.resolve("tls");
    Path nodeDirectory = tlsDirectory.resolve(node.name());
    Files.createDirectories(nodeDirectory);
    Path certificatePath = nodeDirectory.resolve("server.crt");
    Path keyPath = nodeDirectory.resolve("server.key");
    Path requestPath = nodeDirectory.resolve("server.csr");
    Path extensionPath = nodeDirectory.resolve("server.ext");
    Files.writeString(
        extensionPath,
        "basicConstraints=critical,CA:FALSE\n"
            + "keyUsage=critical,digitalSignature,keyEncipherment\n"
            + "extendedKeyUsage=serverAuth\n"
            + "subjectAltName=IP:"
            + node.address()
            + "\n",
        StandardCharsets.US_ASCII);
    runCommand(
        ccmDirectory,
        List.of(
            "openssl",
            "req",
            "-newkey",
            "rsa:3072",
            "-sha256",
            "-nodes",
            "-subj",
            "/CN=" + node.name(),
            "-keyout",
            keyPath.toString(),
            "-out",
            requestPath.toString()));
    runCommand(
        ccmDirectory,
        List.of(
            "openssl",
            "x509",
            "-req",
            "-in",
            requestPath.toString(),
            "-CA",
            caCertificatePath.toString(),
            "-CAkey",
            tlsDirectory.resolve("ca.key").toString(),
            "-CAcreateserial",
            "-days",
            "365",
            "-sha256",
            "-extfile",
            extensionPath.toString(),
            "-out",
            certificatePath.toString()));
    runCcm(
        ccmDirectory,
        List.of(
            node.name(),
            "updateconf",
            "--config-dir",
            ccmDirectory.toString(),
            "alternator_encryption_options.certificate:" + certificatePath,
            "alternator_encryption_options.keyfile:" + keyPath));
  }

  private void waitForAlternator(
      PhysicalTestCluster cluster, List<TestClusterNode> nodes, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Exception lastException = null;
    List<URI> endpoints = new ArrayList<>();
    for (TestClusterNode node : nodes) {
      if (cluster.spec().transports().contains(AlternatorTransport.HTTP)) {
        endpoints.add(URI.create("http://" + node.address() + ":" + HTTP_PORT + "/"));
      }
      if (cluster.spec().transports().contains(AlternatorTransport.HTTPS)) {
        endpoints.add(URI.create("https://" + node.address() + ":" + HTTPS_PORT + "/"));
      }
    }
    while (System.nanoTime() < deadline) {
      boolean allReady = true;
      for (URI endpoint : endpoints) {
        try {
          int status = getStatus(endpoint.toURL());
          if (status < 200 || status >= 300) {
            allReady = false;
            break;
          }
        } catch (Exception exception) {
          lastException = exception;
          allReady = false;
          break;
        }
      }
      if (allReady) {
        return;
      }
      Thread.sleep(1000);
    }
    throw new IOException(
        "Alternator endpoints for cluster '" + cluster.instanceId() + "' did not become ready",
        lastException);
  }

  private static int getStatus(URL url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    if (connection instanceof HttpsURLConnection) {
      TrustManager[] trustAll =
          new TrustManager[] {
            new X509TrustManager() {
              @Override
              public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[0];
              }

              @Override
              public void checkClientTrusted(
                  java.security.cert.X509Certificate[] chain, String authType) {}

              @Override
              public void checkServerTrusted(
                  java.security.cert.X509Certificate[] chain, String authType) {}
            }
          };
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, trustAll, null);
      HttpsURLConnection https = (HttpsURLConnection) connection;
      https.setSSLSocketFactory(context.getSocketFactory());
      HostnameVerifier trustAnyHostname = (hostname, session) -> true;
      https.setHostnameVerifier(trustAnyHostname);
    }
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static String getBody(URL url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    try {
      int status = connection.getResponseCode();
      if (status < 200 || status >= 300) {
        throw new IOException("HTTP " + status + " from " + url);
      }
      return new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      connection.disconnect();
    }
  }

  private void collectDiagnostics(String instanceId, Path ccmDirectory) throws IOException {
    Path normalized = validateCcmDirectoryLocation(ccmDirectory, false);
    if (!Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedDirectory(clustersDirectory, normalized, "CCM config");
    Path destination = ownedChild(diagnosticsDirectory, instanceId);
    prepareDiagnosticDirectory(diagnosticsDirectory, destination);

    try (DirectoryStream<Path> entries = Files.newDirectoryStream(normalized)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (name.equals("ccm-commands.log")
            || (name.startsWith("ccm-command-") && name.endsWith(".log"))) {
          copyDiagnosticFile(entry, destination, Path.of(name), true);
        }
      }
    }

    Path clusterDirectory = ownedChild(normalized, instanceId);
    PathState clusterState = pathState(clusterDirectory);
    if (clusterState == PathState.ABSENT) {
      return;
    }
    if (clusterState == PathState.UNKNOWN) {
      throw new IOException("Cannot determine CCM cluster state at " + clusterDirectory);
    }
    validateOwnedDirectory(normalized, clusterDirectory, "cluster");
    copyOptionalDiagnosticFile(
        clusterDirectory.resolve("cluster.conf"),
        destination,
        Path.of(instanceId, "cluster.conf"),
        false);

    try (DirectoryStream<Path> entries = Files.newDirectoryStream(clusterDirectory)) {
      for (Path nodeDirectory : entries) {
        String nodeName = nodeDirectory.getFileName().toString();
        if (!isCcmNodeName(nodeName)) {
          continue;
        }
        PathState nodeState = pathState(nodeDirectory);
        if (nodeState == PathState.ABSENT) {
          continue;
        }
        if (nodeState == PathState.UNKNOWN) {
          throw new IOException("Cannot determine CCM node state at " + nodeDirectory);
        }
        validateOwnedDirectory(clusterDirectory, nodeDirectory, "node");
        Path nodeRelative = Path.of(instanceId, nodeName);
        copyOptionalDiagnosticFile(
            nodeDirectory.resolve("node.conf"),
            destination,
            nodeRelative.resolve("node.conf"),
            false);

        Path configurationDirectory = nodeDirectory.resolve("conf");
        PathState configurationState = pathState(configurationDirectory);
        if (configurationState == PathState.PRESENT) {
          validateOwnedDirectory(nodeDirectory, configurationDirectory, "node configuration");
          copyOptionalDiagnosticFile(
              configurationDirectory.resolve("scylla.yaml"),
              destination,
              nodeRelative.resolve("conf/scylla.yaml"),
              false);
        } else if (configurationState == PathState.UNKNOWN) {
          throw new IOException(
              "Cannot determine CCM node configuration state at " + configurationDirectory);
        }

        Path logsDirectory = nodeDirectory.resolve("logs");
        PathState logsState = pathState(logsDirectory);
        if (logsState == PathState.PRESENT) {
          validateOwnedDirectory(nodeDirectory, logsDirectory, "node logs");
          copyDiagnosticLogTree(logsDirectory, destination, nodeRelative.resolve("logs"));
        } else if (logsState == PathState.UNKNOWN) {
          throw new IOException("Cannot determine CCM node logs state at " + logsDirectory);
        }
      }
    }
  }

  private static void copyDiagnosticLogTree(
      Path logsDirectory, Path destination, Path destinationRelative) throws IOException {
    Files.walkFileTree(
        logsDirectory,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes)
              throws IOException {
            if (Files.isSymbolicLink(directory) || !attributes.isDirectory()) {
              throw new IOException("Refusing unsafe CCM log directory " + directory);
            }
            if (!directory.equals(logsDirectory)) {
              validateOwnedDirectory(directory.getParent(), directory, "log");
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
              throws IOException {
            if (attributes.isSymbolicLink()) {
              throw new IOException("Refusing symbolic link in CCM logs: " + file);
            }
            if (attributes.isRegularFile() && !isPrivateKeyFile(file)) {
              copyDiagnosticFile(
                  file,
                  destination,
                  destinationRelative.resolve(logsDirectory.relativize(file)),
                  true);
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exception)
              throws IOException {
            if (exception instanceof NoSuchFileException) {
              return FileVisitResult.CONTINUE;
            }
            throw exception;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path directory, IOException exception)
              throws IOException {
            if (exception == null || exception instanceof NoSuchFileException) {
              return FileVisitResult.CONTINUE;
            }
            throw exception;
          }
        });
  }

  private static void copyOptionalDiagnosticFile(
      Path source, Path destination, Path relative, boolean tolerateDisappearance)
      throws IOException {
    PathState state = pathState(source);
    if (state == PathState.ABSENT) {
      return;
    }
    if (state == PathState.UNKNOWN) {
      throw new IOException("Cannot determine diagnostic source state at " + source);
    }
    copyDiagnosticFile(source, destination, relative, tolerateDisappearance);
  }

  private static void copyDiagnosticFile(
      Path source, Path destination, Path relative, boolean tolerateDisappearance)
      throws IOException {
    Path target = destination.resolve(relative).normalize();
    if (!target.startsWith(destination) || target.equals(destination)) {
      throw new IOException("Refusing diagnostic path outside " + destination);
    }
    Path targetParent = target.getParent();
    prepareDiagnosticDirectories(destination, targetParent);
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)
        && (Files.isSymbolicLink(target)
            || !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS))) {
      throw new IOException("Refusing unsafe diagnostic target " + target);
    }
    if (Files.isSymbolicLink(source) || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
      if (tolerateDisappearance && Files.notExists(source, LinkOption.NOFOLLOW_LINKS)) {
        return;
      }
      throw new IOException("Refusing unsafe diagnostic source " + source);
    }

    Path temporary = Files.createTempFile(targetParent, ".ccm-diagnostic-", ".tmp");
    try {
      try (java.nio.channels.SeekableByteChannel channel =
              Files.newByteChannel(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
          InputStream input = Channels.newInputStream(channel);
          OutputStream output =
              Files.newOutputStream(temporary, StandardOpenOption.TRUNCATE_EXISTING)) {
        input.transferTo(output);
      } catch (NoSuchFileException disappeared) {
        if (tolerateDisappearance) {
          return;
        }
        throw disappeared;
      }
      try {
        Files.move(
            temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException ignored) {
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void prepareDiagnosticDirectories(Path root, Path directory) throws IOException {
    if (root.equals(directory)) {
      return;
    }
    Path relative = root.relativize(directory);
    Path current = root;
    for (Path segment : relative) {
      Path child = current.resolve(segment);
      prepareDiagnosticDirectory(current, child);
      current = child;
    }
  }

  private static void prepareDiagnosticDirectory(Path parent, Path directory) throws IOException {
    try {
      Files.createDirectory(directory);
    } catch (FileAlreadyExistsException ignored) {
      // Validate the existing entry below without following it.
    }
    validateOwnedDirectory(parent, directory, "diagnostic");
  }

  private static boolean isPrivateKeyFile(Path path) {
    String fileName = path.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
    return fileName.endsWith(".key") || fileName.endsWith(".pem");
  }

  private void removeByName(String instanceId, Path ccmDirectory) throws Exception {
    Path normalized = validateCcmDirectoryLocation(ccmDirectory, false);
    if (!Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedDirectory(clustersDirectory, normalized, "CCM config");
    Path clusterDirectory = ownedClusterDirectory(normalized, instanceId);
    PathState initialState = pathState(clusterDirectory.resolve("cluster.conf"));
    if (initialState == PathState.ABSENT) {
      cleanupAbsentClusterState(instanceId, normalized);
      return;
    }
    if (initialState == PathState.UNKNOWN) {
      throw new IOException("Cannot determine CCM cluster state for '" + instanceId + "'");
    }
    validateClusterMetadataIfPresent(clusterDirectory, instanceId);
    prepareClusterProcessReferencesForCcm(clusterDirectory);

    Exception commandFailure = null;
    try {
      runCcm(normalized, List.of("remove", "--config-dir", normalized.toString(), instanceId));
    } catch (Exception exception) {
      commandFailure = exception;
    }

    boolean clusterRemoved;
    try {
      clusterRemoved = !clusterStateExists(instanceId, normalized);
    } catch (Exception verificationFailure) {
      if (commandFailure != null) {
        verificationFailure.addSuppressed(commandFailure);
      }
      throw verificationFailure;
    }
    if (clusterRemoved) {
      if (commandFailure instanceof CcmProcessCleanupException) {
        throw commandFailure;
      }
      cleanupAbsentClusterState(instanceId, normalized);
      restoreRemovalInterrupt(commandFailure);
      return;
    }
    if (commandFailure != null) {
      throw commandFailure;
    }
    throw new IOException("CCM reported success but cluster '" + instanceId + "' still exists");
  }

  private void removeNodeByName(Path ccmDirectory, String nodeName) throws Exception {
    Path normalized = requireOwnedCcmDirectory(ccmDirectory);
    if (!nodeStateExists(normalized, nodeName)) {
      return;
    }
    Path clusterDirectory = currentClusterDirectory(normalized);
    if (clusterDirectory == null) {
      return;
    }
    prepareNodeProcessReferencesForCcm(clusterDirectory, nodeName);

    Exception commandFailure = null;
    try {
      runCcm(normalized, List.of(nodeName, "remove", "--config-dir", normalized.toString()));
    } catch (Exception exception) {
      commandFailure = exception;
    }

    boolean nodeRemoved;
    try {
      nodeRemoved = !nodeStateExists(normalized, nodeName);
    } catch (Exception verificationFailure) {
      if (commandFailure != null) {
        verificationFailure.addSuppressed(commandFailure);
      }
      throw verificationFailure;
    }
    if (nodeRemoved) {
      if (commandFailure instanceof CcmProcessCleanupException) {
        throw commandFailure;
      }
      restoreRemovalInterrupt(commandFailure);
      return;
    }
    if (commandFailure != null) {
      throw commandFailure;
    }
    throw new IOException("CCM reported success but node '" + nodeName + "' still exists");
  }

  private boolean nodeStateExists(Path ccmDirectory, String nodeName) throws IOException {
    Path clusterDirectory = currentClusterDirectory(ccmDirectory);
    if (clusterDirectory == null) {
      return false;
    }
    validateClusterMetadataIfPresent(clusterDirectory, clusterDirectory.getFileName().toString());
    Path nodeDirectory = ownedChild(clusterDirectory, nodeName);
    PathState directoryState = pathState(nodeDirectory);
    if (directoryState == PathState.UNKNOWN) {
      throw new IOException("Cannot determine CCM node state at " + nodeDirectory);
    }
    if (directoryState == PathState.PRESENT) {
      validateOwnedDirectory(clusterDirectory, nodeDirectory, "node");
    }
    return directoryState == PathState.PRESENT
        || !nodeAbsentFromClusterMetadata(clusterDirectory, nodeName);
  }

  private static void restoreRemovalInterrupt(Exception removalFailure) {
    if (removalFailure instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private static void validateClusterMetadataIfPresent(Path clusterDirectory, String expectedName)
      throws IOException {
    Path clusterConfig = clusterDirectory.resolve("cluster.conf");
    PathState state = pathState(clusterConfig);
    if (state == PathState.ABSENT) {
      return;
    }
    if (state == PathState.UNKNOWN) {
      throw new IOException("Cannot determine CCM cluster configuration state at " + clusterConfig);
    }
    Map<String, Object> config = readYamlMap(clusterConfig);
    if (!expectedName.equals(config.get("name"))) {
      throw new IOException("CCM cluster configuration has an unexpected name: " + clusterConfig);
    }
    Object configuredNodes = config.get("nodes");
    if (!(configuredNodes instanceof Iterable)) {
      throw new IOException("Invalid nodes list in " + clusterConfig);
    }
    Set<String> nodeNames = new HashSet<>();
    for (Object configuredNode : (Iterable<?>) configuredNodes) {
      if (!(configuredNode instanceof String) || !isCcmNodeName((String) configuredNode)) {
        throw new IOException("Unsafe node name in " + clusterConfig + ": " + configuredNode);
      }
      String nodeName = (String) configuredNode;
      if (!nodeNames.add(nodeName)) {
        throw new IOException("Duplicate node name in " + clusterConfig + ": " + nodeName);
      }
      Path nodeDirectory = ownedChild(clusterDirectory, nodeName);
      if (Files.exists(nodeDirectory, LinkOption.NOFOLLOW_LINKS)) {
        validateOwnedDirectory(clusterDirectory, nodeDirectory, "node");
        Path nodeConfig = nodeDirectory.resolve("node.conf");
        if (Files.exists(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
          validateNativeNodeMetadata(readYamlMap(nodeConfig), nodeDirectory);
        }
      }
    }
    Object configuredSeeds = config.get("seeds");
    if (configuredSeeds instanceof Iterable) {
      for (Object seed : (Iterable<?>) configuredSeeds) {
        if (!(seed instanceof String) || !nodeNames.contains(seed)) {
          throw new IOException("Unsafe seed name in " + clusterConfig + ": " + seed);
        }
      }
    }
  }

  private static boolean isCcmNodeName(String name) {
    return name.matches("node[1-9][0-9]*");
  }

  private static void validateNativeNodeMetadata(Map<String, Object> config, Path nodeDirectory)
      throws IOException {
    String expectedName = nodeDirectory.getFileName().toString();
    if (!expectedName.equals(config.get("name")) || config.containsKey("docker_id")) {
      throw new IOException("Unsafe CCM node configuration: " + nodeDirectory.resolve("node.conf"));
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean nodeAbsentFromClusterMetadata(Path clusterDirectory, String nodeName)
      throws IOException {
    Path clusterConfig = clusterDirectory.resolve("cluster.conf");
    Map<String, Object> yaml = readYamlMap(clusterConfig);
    Object nodes = yaml.get("nodes");
    if (!(nodes instanceof Iterable)) {
      throw new IOException("Invalid nodes list in " + clusterConfig);
    }
    for (Object node : (Iterable<Object>) nodes) {
      if (nodeName.equals(String.valueOf(node))) {
        return false;
      }
    }
    return true;
  }

  private boolean clusterStateExists(String instanceId, Path ccmDirectory) throws IOException {
    Path clusterDirectory = ownedClusterDirectory(ccmDirectory, instanceId);
    PathState state = pathState(clusterDirectory.resolve("cluster.conf"));
    if (state == PathState.UNKNOWN) {
      throw new IOException("Cannot determine CCM cluster state for '" + instanceId + "'");
    }
    return state == PathState.PRESENT;
  }

  private Path currentClusterDirectory(Path ccmDirectory) throws IOException {
    ccmDirectory = requireOwnedCcmDirectory(ccmDirectory);
    Path currentPath = ccmDirectory.resolve("CURRENT");
    PathState currentState = pathState(currentPath);
    if (currentState == PathState.ABSENT) {
      return null;
    }
    if (currentState == PathState.UNKNOWN) {
      throw new IOException("Unable to determine CCM CURRENT state under " + ccmDirectory);
    }
    rejectSymlink(currentPath);
    String current = Files.readString(currentPath, StandardCharsets.UTF_8).trim();
    if (current.isEmpty()) {
      return null;
    }
    return ownedClusterDirectory(ccmDirectory, current);
  }

  private Path ownedClusterDirectory(Path ccmDirectory, String clusterName) throws IOException {
    ccmDirectory = requireOwnedCcmDirectory(ccmDirectory);
    Path clusterDirectory = ownedChild(ccmDirectory, clusterName);
    if (!Files.exists(clusterDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return clusterDirectory;
    }
    validateOwnedDirectory(ccmDirectory, clusterDirectory, "cluster");
    return clusterDirectory;
  }

  private Path validateCcmDirectoryLocation(Path ccmDirectory, boolean requireExists)
      throws IOException {
    validateOwnedDirectory(runDirectory, clustersDirectory, "clusters");
    Path normalized = ccmDirectory.toAbsolutePath().normalize();
    if (!clustersDirectory.equals(normalized.getParent())) {
      throw new IOException("Refusing CCM config outside run state: " + ccmDirectory);
    }
    PathState state = pathState(normalized);
    if (state == PathState.UNKNOWN) {
      throw new IOException("Cannot determine CCM config state: " + ccmDirectory);
    }
    if (state == PathState.ABSENT) {
      if (requireExists) {
        throw new IOException("CCM config directory does not exist: " + ccmDirectory);
      }
      return normalized;
    }
    validateOwnedDirectory(clustersDirectory, normalized, "CCM config");
    return normalized;
  }

  private Path requireOwnedCcmDirectory(Path ccmDirectory) throws IOException {
    return validateCcmDirectoryLocation(ccmDirectory, true);
  }

  private static void validateOwnedDirectory(Path parent, Path directory, String description)
      throws IOException {
    Path normalizedParent = parent.toAbsolutePath().normalize();
    Path normalizedDirectory = directory.toAbsolutePath().normalize();
    if (!normalizedParent.equals(normalizedDirectory.getParent())
        || Files.isSymbolicLink(normalizedDirectory)
        || !Files.isDirectory(normalizedDirectory, LinkOption.NOFOLLOW_LINKS)
        || !normalizedDirectory.equals(normalizedDirectory.toRealPath())) {
      throw new IOException("Refusing unsafe CCM " + description + " directory " + directory);
    }
  }

  private void cleanupAbsentClusterState(String instanceId, Path ccmDirectory) throws IOException {
    Path clusterDirectory = ownedClusterDirectory(ccmDirectory, instanceId);
    if (pathState(clusterDirectory.resolve("cluster.conf")) != PathState.ABSENT) {
      throw new IOException(
          "Refusing to delete CCM-managed cluster state for '" + instanceId + "'");
    }
    deleteRecursively(clusterDirectory);

    Path currentPath = ccmDirectory.resolve("CURRENT");
    if (pathState(currentPath) == PathState.PRESENT) {
      rejectSymlink(currentPath);
      if (instanceId.equals(Files.readString(currentPath, StandardCharsets.UTF_8).trim())) {
        Files.delete(currentPath);
      }
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (pathState(root) == PathState.ABSENT) {
      return;
    }
    if (Files.isSymbolicLink(root)) {
      throw new IOException("Refusing to recursively delete symlink " + root);
    }
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path directory, IOException exception)
              throws IOException {
            if (exception != null) {
              throw exception;
            }
            Files.delete(directory);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static PathState pathState(Path path) {
    if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return PathState.PRESENT;
    }
    return Files.notExists(path, LinkOption.NOFOLLOW_LINKS) ? PathState.ABSENT : PathState.UNKNOWN;
  }

  private static boolean clearInterrupt(Exception exception) {
    boolean interrupted = Thread.interrupted();
    return interrupted || exception instanceof InterruptedException;
  }

  private CcmClusterProvisioningException clusterProvisioningException(
      ClusterSpec spec,
      String instanceId,
      int ccmId,
      Path ccmDirectory,
      List<TestClusterNode> nodes,
      Path caCertificatePath,
      AwsCredentialsProvider credentials,
      Exception provisioningException,
      Exception rollbackException) {
    PhysicalTestCluster cluster =
        new PhysicalTestCluster(
            this, instanceId, ccmId, ccmDirectory, spec, nodes, caCertificatePath, credentials);
    return new CcmClusterProvisioningException(cluster, provisioningException, rollbackException);
  }

  private void runCcm(Path ccmDirectory, List<String> arguments) throws Exception {
    if (!arguments.isEmpty() && !"create".equals(arguments.get(0))) {
      Path currentCluster = currentClusterDirectory(ccmDirectory);
      if (currentCluster != null) {
        validateClusterMetadataIfPresent(currentCluster, currentCluster.getFileName().toString());
      }
    }
    List<String> command = new ArrayList<>();
    command.add(ccmExecutable);
    command.addAll(arguments);
    runCommand(ccmDirectory, command);
  }

  private void runCommand(Path ccmDirectory, List<String> command) throws Exception {
    if (!System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("linux")) {
      throw new UnsupportedOperationException(
          "The native scylla-ccm harness currently supports Linux only");
    }
    ccmDirectory = requireOwnedCcmDirectory(ccmDirectory);
    String commandText = String.join(" ", command);
    Path outputPath = Files.createTempFile(ccmDirectory, "ccm-command-", ".log");
    Files.writeString(
        outputPath,
        "> " + commandText + "\n",
        StandardCharsets.UTF_8,
        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);

    List<String> launchedCommand = new ArrayList<>();
    launchedCommand.add("setsid");
    launchedCommand.addAll(command);
    ProcessBuilder processBuilder =
        new ProcessBuilder(launchedCommand)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(outputPath.toFile()));
    processBuilder.environment().put("SCYLLA_CCM_RUN_DIR", runDirectory.toString());

    Process process;
    try {
      process = processBuilder.start();
    } catch (IOException startFailure) {
      try {
        appendCommandOutcome(outputPath, "start failed");
        appendAggregateLog(ccmDirectory, outputPath);
      } catch (IOException diagnosticFailure) {
        startFailure.addSuppressed(diagnosticFailure);
      }
      throw startFailure;
    }

    boolean timedOut = false;
    InterruptedException interruption = null;
    Exception terminationFailure = null;
    boolean exited = false;
    try {
      exited = process.waitFor(commandTimeout.toNanos(), TimeUnit.NANOSECONDS);
      timedOut = !exited;
    } catch (InterruptedException exception) {
      interruption = exception;
    }

    if (timedOut || interruption != null || (exited && process.exitValue() != 0)) {
      try {
        terminateCommandProcessGroup(process);
      } catch (Exception exception) {
        if (exception instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        terminationFailure =
            exception instanceof CcmProcessCleanupException
                ? exception
                : new CcmProcessCleanupException(
                    "Unable to prove cleanup of CCM command process group " + process.pid(),
                    exception);
      }
    }

    String outcome;
    if (interruption != null) {
      outcome = "interrupted";
    } else if (timedOut) {
      outcome = "timeout";
    } else {
      outcome = Integer.toString(process.exitValue());
    }
    IOException diagnosticFailure = null;
    try {
      appendCommandOutcome(outputPath, outcome);
      appendAggregateLog(ccmDirectory, outputPath);
    } catch (IOException exception) {
      diagnosticFailure = exception;
    }

    String output;
    try {
      output = Files.readString(outputPath, StandardCharsets.UTF_8);
    } catch (IOException exception) {
      if (terminationFailure != null) {
        terminationFailure.addSuppressed(exception);
        if (diagnosticFailure != null) {
          terminationFailure.addSuppressed(diagnosticFailure);
        }
        if (interruption != null) {
          terminationFailure.addSuppressed(interruption);
          Thread.currentThread().interrupt();
        }
        throw terminationFailure;
      }
      if (diagnosticFailure != null) {
        exception.addSuppressed(diagnosticFailure);
      }
      if (interruption != null) {
        exception.addSuppressed(interruption);
        Thread.currentThread().interrupt();
      }
      throw exception;
    }
    System.out.print(output);

    if (interruption != null) {
      if (terminationFailure != null) {
        terminationFailure.addSuppressed(interruption);
        if (diagnosticFailure != null) {
          terminationFailure.addSuppressed(diagnosticFailure);
        }
        Thread.currentThread().interrupt();
        throw terminationFailure;
      }
      if (diagnosticFailure != null) {
        interruption.addSuppressed(diagnosticFailure);
      }
      Thread.currentThread().interrupt();
      throw interruption;
    }
    if (terminationFailure != null) {
      if (diagnosticFailure != null) {
        terminationFailure.addSuppressed(diagnosticFailure);
      }
      throw terminationFailure;
    }
    if (timedOut || process.exitValue() != 0) {
      CcmCommandException commandFailure =
          new CcmCommandException(commandText, timedOut ? -1 : process.exitValue(), output);
      if (diagnosticFailure != null) {
        commandFailure.addSuppressed(diagnosticFailure);
      }
      throw commandFailure;
    }
    if (diagnosticFailure != null) {
      throw diagnosticFailure;
    }
  }

  void terminateCommandProcessGroup(Process process) throws Exception {
    terminateProcessGroup(process);
  }

  private static void appendCommandOutcome(Path outputPath, String outcome) throws IOException {
    Files.writeString(
        outputPath,
        "[exit " + outcome + "]\n",
        StandardCharsets.UTF_8,
        java.nio.file.StandardOpenOption.APPEND);
  }

  private static void appendAggregateLog(Path ccmDirectory, Path outputPath) throws IOException {
    Files.writeString(
        ccmDirectory.resolve("ccm-commands.log"),
        Files.readString(outputPath, StandardCharsets.UTF_8),
        StandardCharsets.UTF_8,
        java.nio.file.StandardOpenOption.CREATE,
        java.nio.file.StandardOpenOption.APPEND);
  }

  private static void terminateProcessGroup(Process process) throws Exception {
    long processGroup = process.pid();
    boolean restoreInterrupt = Thread.interrupted();
    Exception failure = null;
    try {
      try {
        signalProcessGroup(processGroup, "TERM");
      } catch (Exception exception) {
        failure = exception;
      }
      restoreInterrupt |= waitForProcessGroup(processGroup, PROCESS_TERMINATION_GRACE);
      if (processGroupHasLiveMembers(processGroup)) {
        try {
          signalProcessGroup(processGroup, "KILL");
        } catch (Exception exception) {
          if (failure == null) {
            failure = exception;
          } else {
            failure.addSuppressed(exception);
          }
        }
        restoreInterrupt |= waitForProcessGroup(processGroup, PROCESS_KILL_TIMEOUT);
      }
      restoreInterrupt |= waitForDirectProcess(process, PROCESS_KILL_TIMEOUT);
      if (processGroupHasLiveMembers(processGroup) || process.isAlive()) {
        CcmProcessCleanupException survivors =
            new CcmProcessCleanupException(
                "CCM command process group " + processGroup + " survived termination");
        if (failure != null) {
          survivors.addSuppressed(failure);
        }
        throw survivors;
      }
      if (failure != null) {
        // A failed signal is harmless only if the group is now provably empty.
        return;
      }
    } finally {
      if (restoreInterrupt) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void signalProcessGroup(long processGroup, String signal) throws Exception {
    UtilityResult result = runUtility(List.of("kill", "-" + signal, "--", "-" + processGroup));
    if (result.exitCode != 0 && processGroupHasLiveMembers(processGroup)) {
      throw new IOException(
          "Unable to signal CCM process group " + processGroup + ": " + result.output.trim());
    }
  }

  private static boolean waitForProcessGroup(long processGroup, Duration timeout)
      throws IOException {
    boolean interrupted = false;
    long deadline = System.nanoTime() + timeout.toNanos();
    while (processGroupHasLiveMembers(processGroup) && System.nanoTime() < deadline) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    return interrupted;
  }

  private static boolean waitForDirectProcess(Process process, Duration timeout) {
    boolean interrupted = false;
    long deadline = System.nanoTime() + timeout.toNanos();
    while (process.isAlive() && System.nanoTime() < deadline) {
      try {
        process.waitFor(20, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    return interrupted;
  }

  private static boolean processGroupHasLiveMembers(long processGroup) throws IOException {
    UtilityResult result =
        runUtility(List.of("ps", "-o", "stat=", "-g", Long.toString(processGroup)));
    if (result.exitCode != 0 && !result.output.trim().isEmpty()) {
      throw new IOException(
          "Unable to inspect CCM process group " + processGroup + ": " + result.output.trim());
    }
    for (String state : result.output.split("\\R")) {
      String trimmed = state.trim();
      if (!trimmed.isEmpty() && !trimmed.startsWith("Z")) {
        return true;
      }
    }
    return false;
  }

  private static UtilityResult runUtility(List<String> command) throws IOException {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    boolean interrupted = false;
    long deadline = System.nanoTime() + PROCESS_KILL_TIMEOUT.toNanos();
    try {
      while (process.isAlive() && System.nanoTime() < deadline) {
        try {
          process.waitFor(20, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
          interrupted = true;
        }
      }
      if (process.isAlive()) {
        process.destroyForcibly();
        long killDeadline = System.nanoTime() + PROCESS_KILL_TIMEOUT.toNanos();
        while (process.isAlive() && System.nanoTime() < killDeadline) {
          try {
            process.waitFor(20, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ignored) {
            interrupted = true;
          }
        }
      }
      if (process.isAlive()) {
        throw new IOException("Cleanup utility did not terminate: " + String.join(" ", command));
      }
      byte[] output = process.getInputStream().readAllBytes();
      return new UtilityResult(process.exitValue(), new String(output, StandardCharsets.UTF_8));
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static final class UtilityResult {
    final int exitCode;
    final String output;

    UtilityResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }

  void cleanupStaleCluster(String instanceId, int ccmId, Path ccmDirectory) throws Exception {
    if (ccmId < 1 || ccmId > 99) {
      throw new IOException("Invalid CCM ID " + ccmId);
    }
    Path normalized = validateCcmDirectoryLocation(ccmDirectory, false);
    if (!Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedDirectory(clustersDirectory, normalized, "CCM config");

    // Command logs exist before metadata is complete, so snapshot them first.
    collectDiagnostics(instanceId, normalized);
    Path clusterDirectory = ownedClusterDirectory(normalized, instanceId);
    PathState state = pathState(clusterDirectory.resolve("cluster.conf"));
    if (state == PathState.ABSENT) {
      cleanupAbsentClusterState(instanceId, normalized);
      return;
    }
    if (state == PathState.UNKNOWN) {
      throw new IOException("Cannot determine stale CCM cluster state for '" + instanceId + "'");
    }
    validateClusterMetadataIfPresent(clusterDirectory, instanceId);
    sanitizeStaleProcessReferences(clusterDirectory);
    removeByName(instanceId, normalized);
  }

  private static void sanitizeStaleProcessReferences(Path clusterDirectory) throws IOException {
    Map<String, Object> cluster = readYamlMap(clusterDirectory.resolve("cluster.conf"));
    Object configuredNodes = cluster.get("nodes");
    if (!(configuredNodes instanceof Iterable)) {
      throw new IOException("Invalid nodes list in " + clusterDirectory.resolve("cluster.conf"));
    }
    for (Object value : (Iterable<?>) configuredNodes) {
      if (!(value instanceof String) || !isCcmNodeName((String) value)) {
        throw new IOException("Unsafe node name in stale CCM metadata: " + value);
      }
      Path nodeDirectory = ownedChild(clusterDirectory, (String) value);
      if (!Files.exists(nodeDirectory, LinkOption.NOFOLLOW_LINKS)) {
        continue;
      }
      validateOwnedDirectory(clusterDirectory, nodeDirectory, "node");
      Path nodeConfig = nodeDirectory.resolve("node.conf");
      if (Files.exists(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
        sanitizeStaleNodeConfig(nodeConfig, nodeDirectory);
      }
      for (String pidFile : List.of("cassandra.pid", "scylla-jmx.pid", "scylla-agent.pid")) {
        Path path = nodeDirectory.resolve(pidFile);
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
          rejectSymlink(path);
          Files.delete(path);
        }
      }
    }
  }

  private static void sanitizeStaleNodeConfig(Path nodeConfig, Path nodeDirectory)
      throws IOException {
    Map<String, Object> config = readYamlMap(nodeConfig);
    validateNativeNodeMetadata(config, nodeDirectory);
    if (config.remove("pid") == null) {
      return;
    }

    Path temporary = Files.createTempFile(nodeDirectory, ".ccm-sanitize-node.conf-", ".tmp");
    try {
      Files.writeString(
          temporary,
          dumpYamlMap(config),
          StandardCharsets.UTF_8,
          java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
      try {
        Files.setPosixFilePermissions(
            temporary, Files.getPosixFilePermissions(nodeConfig, LinkOption.NOFOLLOW_LINKS));
      } catch (UnsupportedOperationException ignored) {
        // The atomic same-directory replacement is the required safety property.
      }
      try {
        Files.move(
            temporary,
            nodeConfig,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException exception) {
        throw new IOException("Atomic stale node.conf sanitization is unsupported", exception);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private enum PathState {
    PRESENT,
    ABSENT,
    UNKNOWN
  }

  private static boolean containsNode(List<TestClusterNode> nodes, String name) {
    return nodes.stream().anyMatch(node -> node.name().equals(name));
  }

  static final class CcmCommandException extends IOException {
    CcmCommandException(String command, int exitCode, String output) {
      super("CCM command failed with exit code " + exitCode + ": " + command + "\n" + output);
    }
  }

  static final class CcmProcessCleanupException extends IOException {
    CcmProcessCleanupException(String message) {
      super(message);
    }

    CcmProcessCleanupException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final class IndeterminateNodeStateException extends IOException {
    IndeterminateNodeStateException(String message) {
      super(message);
    }
  }

  static final class CcmClusterProvisioningException extends Exception {
    private final PhysicalTestCluster cluster;

    CcmClusterProvisioningException(
        PhysicalTestCluster cluster, Exception provisioningException, Exception rollbackException) {
      super(
          "Failed to provision '" + cluster.instanceId() + "' and CCM rollback failed",
          provisioningException);
      addSuppressed(rollbackException);
      this.cluster = cluster;
    }

    PhysicalTestCluster cluster() {
      return cluster;
    }
  }

  static final class CcmNodeProvisioningException extends Exception {
    private final TestClusterNode node;
    private final boolean nodeRemainsProvisioned;

    CcmNodeProvisioningException(
        TestClusterNode node,
        boolean nodeRemainsProvisioned,
        Exception provisioningException,
        Exception rollbackException) {
      super(
          rollbackException == null
              ? "Failed to provision '" + node.name() + "'; CCM rollback succeeded"
              : "Failed to provision '" + node.name() + "' and CCM rollback failed",
          provisioningException);
      if (rollbackException != null) {
        addSuppressed(rollbackException);
      }
      this.node = node;
      this.nodeRemainsProvisioned = nodeRemainsProvisioned;
    }

    TestClusterNode node() {
      return node;
    }

    boolean nodeRemainsProvisioned() {
      return nodeRemainsProvisioned;
    }
  }
}
