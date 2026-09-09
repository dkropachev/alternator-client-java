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
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
  static final int HTTP_PORT = 8080;
  static final int HTTPS_PORT = 8043;
  static final int STORAGE_PORT = 7000;
  static final int API_PORT = 10000;

  private static final String GOSSIPING_PROPERTY_FILE_SNITCH =
      "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
  private static final String TEST_USER = "alternator_tests";
  private static final String TEST_SALTED_PASSWORD =
      "$6$IcPWfCigHWVhHTf.$h3.30m5R2CnYqIeniCumbXCBxBxvtYPP3MbZVsjKcu268ESOcrUtSJwf1iO1s83KUT3waITRtTiexBdSWEI0Q/";
  private static final Duration READINESS_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration PROCESS_TERMINATION_GRACE = Duration.ofSeconds(2);
  private static final Duration PROCESS_KILL_TIMEOUT = Duration.ofSeconds(5);
  private static final int REFERENCE_SANITIZE_ATTEMPTS = 5;
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
  private final Map<Path, List<RetainedCommandGroup>> retainedCommandGroups = new HashMap<>();
  private final Map<Path, List<OwnedNodeState>> retainedNodeStates = new HashMap<>();

  CcmProvisioner(Path runDirectory) throws IOException {
    this(
        runDirectory,
        configuredDiagnosticsDirectory(runDirectory),
        System.getenv().getOrDefault("SCYLLA_CCM_PATH", "ccm"),
        COMMAND_TIMEOUT);
  }

  CcmProvisioner(Path runDirectory, String ccmExecutable) throws IOException {
    this(runDirectory, runDirectory.resolve("diagnostics"), ccmExecutable, COMMAND_TIMEOUT);
  }

  CcmProvisioner(Path runDirectory, String ccmExecutable, Duration commandTimeout)
      throws IOException {
    this(runDirectory, runDirectory.resolve("diagnostics"), ccmExecutable, commandTimeout);
  }

  CcmProvisioner(
      Path runDirectory, Path diagnosticsDirectory, String ccmExecutable, Duration commandTimeout)
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
    this.commandTimeout = commandTimeout;
    this.diagnosticsDirectory = diagnosticsDirectory;
    Files.createDirectories(diagnosticsDirectory);
  }

  private static Path configuredDiagnosticsDirectory(Path runDirectory) {
    String configured = System.getenv("SCYLLA_CCM_DIAGNOSTICS_DIR");
    return configured == null || configured.trim().isEmpty()
        ? runDirectory.resolve("diagnostics")
        : Path.of(configured);
  }

  Path runDirectory() {
    return runDirectory;
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
        collectDiagnosticsBestEffort(instanceId, ccmDirectory);
        if (provisioningException instanceof CcmProcessCleanupException) {
          throw clusterProvisioningException(
              spec,
              instanceId,
              ccmId,
              ccmDirectory,
              nodes,
              caCertificatePath,
              credentials,
              provisioningException,
              new IOException("CCM child processes remain owned by the failed cluster"));
        }
        boolean clusterStateExists;
        try {
          clusterStateExists = clusterStateExists(instanceId, ccmDirectory);
        } catch (Exception stateException) {
          throw clusterProvisioningException(
              spec,
              instanceId,
              ccmId,
              ccmDirectory,
              nodes,
              caCertificatePath,
              credentials,
              provisioningException,
              stateException);
        }
        if (!clusterStateExists) {
          try {
            reapAndCleanupAbsentClusterState(instanceId, ccmDirectory);
          } catch (Exception cleanupException) {
            throw clusterProvisioningException(
                spec,
                instanceId,
                ccmId,
                ccmDirectory,
                nodes,
                caCertificatePath,
                credentials,
                provisioningException,
                cleanupException);
          }
          throw provisioningException;
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
    for (TestClusterNode node : cluster.nodes()) {
      if (prepareNodeForStart(cluster, node)) {
        runCcm(cluster.ccmDirectory(), startArguments(cluster, node.name()));
      }
    }
    waitForAlternator(cluster, cluster.nodes(), READINESS_TIMEOUT);
  }

  void stop(PhysicalTestCluster cluster) throws Exception {
    reapRetainedCommandGroups(cluster.ccmDirectory());
    Path clusterDirectory = requireCurrentClusterDirectory(cluster.ccmDirectory());
    for (TestClusterNode node : cluster.nodes()) {
      sanitizeAndReapNode(ownedChild(clusterDirectory, node.name()));
    }
    runCcm(
        cluster.ccmDirectory(), List.of("stop", "--config-dir", cluster.ccmDirectory().toString()));
  }

  void startNode(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    if (prepareNodeForStart(cluster, node)) {
      runCcm(cluster.ccmDirectory(), startArguments(cluster, node.name()));
    }
    waitForNodeReady(cluster, node);
  }

  void stopNode(PhysicalTestCluster cluster, TestClusterNode node) throws Exception {
    reapRetainedCommandGroups(cluster.ccmDirectory());
    Path clusterDirectory = requireCurrentClusterDirectory(cluster.ccmDirectory());
    sanitizeAndReapNode(ownedChild(clusterDirectory, node.name()));
  }

  private boolean prepareNodeForStart(PhysicalTestCluster cluster, TestClusterNode node)
      throws Exception {
    reapRetainedCommandGroups(cluster.ccmDirectory());
    if (isNodeRunning(cluster, node)) {
      return false;
    }
    Path clusterDirectory = requireCurrentClusterDirectory(cluster.ccmDirectory());
    sanitizeAndReapNode(ownedChild(clusterDirectory, node.name()));
    return true;
  }

  private Path requireCurrentClusterDirectory(Path ccmDirectory) throws IOException {
    Path clusterDirectory = currentClusterDirectory(ccmDirectory);
    if (clusterDirectory == null) {
      throw new IOException("CCM has no current cluster under " + ccmDirectory);
    }
    return clusterDirectory;
  }

  TestClusterNode addNode(PhysicalTestCluster cluster, String datacenter, String rack)
      throws Exception {
    if (cluster.nodes().size() >= ClusterCapacity.MAXIMUM_NODE_COUNT) {
      throw new IllegalStateException(
          "A cluster cannot exceed " + ClusterCapacity.MAXIMUM_NODE_COUNT + " nodes");
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
        if (provisioningException instanceof CcmProcessCleanupException) {
          throw new CcmNodeProvisioningException(
              node,
              true,
              provisioningException,
              new IOException("CCM child processes remain owned by the failed node"));
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
    for (long pid : readScyllaPidsForProbe(nodeDirectory)) {
      Optional<ProcessHandle> process = ProcessHandle.of(pid);
      if (process.isPresent() && isProcessAlive(process.get())) {
        if (nodeProcessBelongsTo(process.get(), nodeDirectory)) {
          return true;
        }
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
    collectDiagnosticsBestEffort(cluster.instanceId(), cluster.ccmDirectory());
    removeByName(cluster.instanceId(), cluster.ccmDirectory());
    collectDiagnosticsBestEffort(cluster.instanceId(), cluster.ccmDirectory());
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

  private static Set<Long> readNodePids(Path nodeDirectory) throws IOException {
    Set<Long> pids = readScyllaPids(nodeDirectory);
    readPidFile(nodeDirectory.resolve("scylla-jmx.pid"), pids);
    readPidFile(nodeDirectory.resolve("scylla-agent.pid"), pids);
    return pids;
  }

  private static Set<Long> readScyllaPids(Path nodeDirectory) throws IOException {
    Set<Long> pids = new HashSet<>();
    readPidFile(nodeDirectory.resolve("cassandra.pid"), pids);
    Path nodeConfig = nodeDirectory.resolve("node.conf");
    if (Files.isRegularFile(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
      Object configuredPid = readYamlMap(nodeConfig).get("pid");
      if (configuredPid instanceof Number) {
        pids.add(((Number) configuredPid).longValue());
      } else if (configuredPid != null) {
        addPid(configuredPid.toString(), nodeConfig, pids);
      }
    }
    return pids;
  }

  private static Set<Long> readScyllaPidsForProbe(Path nodeDirectory) throws IOException {
    Set<Long> pids = new HashSet<>();
    readPidFileForProbe(nodeDirectory.resolve("cassandra.pid"), pids);
    Path nodeConfig = nodeDirectory.resolve("node.conf");
    if (Files.exists(nodeConfig, LinkOption.NOFOLLOW_LINKS)) {
      Object configuredPid = readYamlMap(nodeConfig).get("pid");
      if (configuredPid != null) {
        try {
          addPid(configuredPid.toString(), nodeConfig, pids);
        } catch (IOException ignored) {
          // A truncated PID scalar means the node cannot be proven live. Mutating control paths
          // atomically quarantine and repair it before invoking CCM.
        }
      }
    }
    return pids;
  }

  private static void readPidFileForProbe(Path path, Set<Long> pids) throws IOException {
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    rejectSymlink(path);
    String contents = Files.readString(path, StandardCharsets.US_ASCII).trim();
    try {
      addPid(contents, path, pids);
    } catch (IOException ignored) {
      // See readScyllaPidsForProbe: invalid contents are not evidence of a live node.
    }
  }

  private static void readPidFile(Path path, Set<Long> pids) throws IOException {
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    rejectSymlink(path);
    addPid(Files.readString(path, StandardCharsets.US_ASCII).trim(), path, pids);
  }

  private static void sanitizeStaleNodeProcessReferences(Path nodeDirectory) throws IOException {
    if (Files.isSymbolicLink(nodeDirectory)
        || !Files.isDirectory(nodeDirectory, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Refusing unsafe CCM node directory " + nodeDirectory);
    }
    for (String name : List.of("cassandra.pid", "scylla-jmx.pid", "scylla-agent.pid")) {
      sanitizePidFile(nodeDirectory.resolve(name), nodeDirectory);
    }
    sanitizeNodeConfig(nodeDirectory.resolve("node.conf"), nodeDirectory);
  }

  private static void sanitizePidFile(Path pidFile, Path nodeDirectory) throws IOException {
    for (int attempt = 0; attempt < REFERENCE_SANITIZE_ATTEMPTS; attempt++) {
      QuarantinedReference reference = claimProcessReference(pidFile, nodeDirectory);
      if (reference == null) {
        return;
      }
      try {
        String contents = Files.readString(reference.claimed, StandardCharsets.US_ASCII).trim();
        long pid;
        try {
          pid = parsePid(contents, pidFile);
        } catch (IOException malformedPid) {
          // An interrupted CCM start can leave an empty/truncated PID file. The reference is
          // quarantined, so a complete process scan can safely reap anything started before
          // PID publication and make the malformed file disposable.
          reapRecognizedNodeProcesses(nodeDirectory);
          discardClaimedReference(reference);
          continue;
        }
        makeProcessReferenceSafe(pid, nodeDirectory);
        discardClaimedReference(reference);
      } catch (IOException exception) {
        restoreClaimAfterFailure(reference, exception);
        throw exception;
      }
      if (!Files.exists(pidFile, LinkOption.NOFOLLOW_LINKS)) {
        if (Files.notExists(pidFile, LinkOption.NOFOLLOW_LINKS)) {
          return;
        }
        throw new IOException("Cannot determine whether CCM PID file exists: " + pidFile);
      }
      // A cooperating writer published a new reference without being overwritten. Claim and
      // validate that new entry before allowing CCM to observe it.
    }
    throw new IOException("CCM PID file changed repeatedly while sanitizing " + pidFile);
  }

  private static void sanitizeNodeConfig(Path nodeConfig, Path nodeDirectory) throws IOException {
    QuarantinedReference reference = claimProcessReference(nodeConfig, nodeDirectory);
    if (reference == null) {
      return;
    }
    try {
      Map<String, Object> yaml = readYamlMap(reference.claimed);
      Object configuredPid = yaml.get("pid");
      if (configuredPid == null) {
        restoreClaimedReference(reference);
        return;
      }
      long pid;
      try {
        pid = parsePid(configuredPid.toString(), nodeConfig);
      } catch (IOException malformedPid) {
        reapRecognizedNodeProcesses(nodeDirectory);
        pid = -1;
      }
      if (pid >= 2) {
        makeProcessReferenceSafe(pid, nodeDirectory);
      }
      yaml.remove("pid");
      Files.writeString(
          reference.prepared,
          dumpYamlMap(yaml),
          StandardCharsets.UTF_8,
          java.nio.file.StandardOpenOption.CREATE_NEW,
          java.nio.file.StandardOpenOption.WRITE);
      copyPosixPermissions(reference.claimed, reference.prepared);
      publishWithoutReplacement(reference.prepared, nodeConfig);
      if (!Files.isSameFile(reference.prepared, nodeConfig)
          || !yaml.equals(readYamlMap(nodeConfig))) {
        throw new IOException("CCM node configuration changed while sanitizing " + nodeConfig);
      }
      Files.delete(reference.prepared);
      discardClaimedReference(reference);
    } catch (IOException exception) {
      restoreClaimAfterFailure(reference, exception);
      throw exception;
    }
  }

  private static QuarantinedReference claimProcessReference(Path reference, Path nodeDirectory)
      throws IOException {
    recoverQuarantinedReference(reference, nodeDirectory);
    if (!Files.exists(reference, LinkOption.NOFOLLOW_LINKS)) {
      if (Files.notExists(reference, LinkOption.NOFOLLOW_LINKS)) {
        return null;
      }
      throw new IOException("Cannot determine whether CCM process reference exists: " + reference);
    }
    rejectSymlink(reference);

    String prefix = ".ccm-sanitize-" + reference.getFileName() + "-";
    Path quarantine = Files.createTempDirectory(nodeDirectory, prefix);
    try {
      try {
        Files.setPosixFilePermissions(
            quarantine, java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"));
      } catch (UnsupportedOperationException ignored) {
        // The no-clobber hard-link publication below is the required safety property.
      }
      Path claimed = quarantine.resolve("claimed");
      try {
        // There is deliberately no non-atomic fallback. The private, newly-created directory
        // guarantees that the target name was absent when this same-filesystem rename began.
        Files.move(reference, claimed, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException exception) {
        throw new IOException("Atomic CCM process-reference quarantine is unsupported", exception);
      }
      if (Files.exists(reference, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("CCM process reference was concurrently replaced: " + reference);
      }
      rejectSymlink(claimed);
      return new QuarantinedReference(
          reference, quarantine, claimed, quarantine.resolve("prepared"));
    } catch (IOException exception) {
      if (isEmptyDirectory(quarantine)) {
        try {
          Files.delete(quarantine);
        } catch (IOException cleanupException) {
          exception.addSuppressed(cleanupException);
        }
      }
      throw exception;
    }
  }

  private static void recoverQuarantinedReference(Path reference, Path nodeDirectory)
      throws IOException {
    String prefix = ".ccm-sanitize-" + reference.getFileName() + "-";
    List<Path> quarantines = new ArrayList<>();
    try (java.nio.file.DirectoryStream<Path> entries =
        Files.newDirectoryStream(
            nodeDirectory, path -> path.getFileName().toString().startsWith(prefix))) {
      entries.forEach(quarantines::add);
    }
    if (quarantines.size() > 1) {
      throw new IOException(
          "Multiple interrupted CCM process-reference transactions exist for " + reference);
    }
    if (quarantines.isEmpty()) {
      return;
    }

    Path quarantine = quarantines.get(0);
    if (Files.isSymbolicLink(quarantine)
        || !Files.isDirectory(quarantine, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Refusing unsafe CCM quarantine directory " + quarantine);
    }
    QuarantinedReference transaction =
        new QuarantinedReference(
            reference, quarantine, quarantine.resolve("claimed"), quarantine.resolve("prepared"));
    validateQuarantineEntries(transaction);
    boolean claimedExists = Files.exists(transaction.claimed, LinkOption.NOFOLLOW_LINKS);
    boolean preparedExists = Files.exists(transaction.prepared, LinkOption.NOFOLLOW_LINKS);
    boolean referenceExists = Files.exists(reference, LinkOption.NOFOLLOW_LINKS);

    if (!claimedExists) {
      if (!preparedExists) {
        Files.delete(quarantine);
        return;
      }
      if (referenceExists) {
        rejectSymlink(reference);
        rejectSymlink(transaction.prepared);
        if (Files.isSameFile(reference, transaction.prepared)) {
          Files.delete(transaction.prepared);
          Files.delete(quarantine);
          return;
        }
      }
      throw new IOException("Cannot recover interrupted CCM sanitization at " + quarantine);
    }

    rejectSymlink(transaction.claimed);
    if (!referenceExists) {
      restoreClaimedReference(transaction);
      return;
    }
    rejectSymlink(reference);
    if (Files.isSameFile(reference, transaction.claimed)) {
      discardClaimedReference(transaction);
      return;
    }
    if ("node.conf".equals(reference.getFileName().toString())
        && completedSanitizedNodeConfig(transaction, nodeDirectory)) {
      discardClaimedReference(transaction);
      return;
    }
    throw new IOException("A concurrent CCM process reference prevents recovery of " + quarantine);
  }

  private static boolean completedSanitizedNodeConfig(
      QuarantinedReference transaction, Path nodeDirectory) throws IOException {
    if (Files.exists(transaction.prepared, LinkOption.NOFOLLOW_LINKS)) {
      rejectSymlink(transaction.prepared);
      if (!Files.isSameFile(transaction.original, transaction.prepared)) {
        return false;
      }
    }
    Map<String, Object> claimed = readYamlMap(transaction.claimed);
    Object configuredPid = claimed.get("pid");
    if (configuredPid == null) {
      return false;
    }
    long pid = parsePid(configuredPid.toString(), transaction.original);
    claimed.remove("pid");
    return claimed.equals(readYamlMap(transaction.original))
        && processReferenceIsStale(pid, nodeDirectory);
  }

  private static void validateQuarantineEntries(QuarantinedReference transaction)
      throws IOException {
    try (java.nio.file.DirectoryStream<Path> entries =
        Files.newDirectoryStream(transaction.quarantine)) {
      for (Path entry : entries) {
        if (!entry.equals(transaction.claimed) && !entry.equals(transaction.prepared)) {
          throw new IOException("Unexpected file in CCM quarantine transaction: " + entry);
        }
        rejectSymlink(entry);
      }
    }
  }

  private static void publishWithoutReplacement(Path source, Path target) throws IOException {
    try {
      Files.createLink(target, source);
    } catch (UnsupportedOperationException exception) {
      throw new IOException("Safe CCM process-reference publication is unsupported", exception);
    }
  }

  private static void restoreClaimedReference(QuarantinedReference transaction) throws IOException {
    if (Files.exists(transaction.original, LinkOption.NOFOLLOW_LINKS)) {
      rejectSymlink(transaction.original);
      if (!Files.isSameFile(transaction.original, transaction.claimed)) {
        throw new IOException(
            "Refusing to overwrite a concurrent CCM process reference " + transaction.original);
      }
    } else if (Files.notExists(transaction.original, LinkOption.NOFOLLOW_LINKS)) {
      publishWithoutReplacement(transaction.claimed, transaction.original);
      if (!Files.isSameFile(transaction.claimed, transaction.original)) {
        throw new IOException("Unable to restore CCM process reference " + transaction.original);
      }
    } else {
      throw new IOException(
          "Cannot determine whether CCM process reference exists: " + transaction.original);
    }
    discardClaimedReference(transaction);
  }

  private static void restoreClaimAfterFailure(
      QuarantinedReference transaction, IOException originalFailure) {
    try {
      if (!Files.exists(transaction.claimed, LinkOption.NOFOLLOW_LINKS)) {
        return;
      }
      if (!Files.exists(transaction.original, LinkOption.NOFOLLOW_LINKS)) {
        if (!Files.notExists(transaction.original, LinkOption.NOFOLLOW_LINKS)) {
          throw new IOException(
              "Cannot determine whether CCM process reference exists: " + transaction.original);
        }
        restoreClaimedReference(transaction);
      } else if (Files.isSameFile(transaction.original, transaction.claimed)) {
        discardClaimedReference(transaction);
      }
    } catch (IOException recoveryFailure) {
      originalFailure.addSuppressed(recoveryFailure);
    }
  }

  private static void discardClaimedReference(QuarantinedReference transaction) throws IOException {
    Files.deleteIfExists(transaction.prepared);
    Files.deleteIfExists(transaction.claimed);
    Files.delete(transaction.quarantine);
  }

  private static void copyPosixPermissions(Path source, Path target) throws IOException {
    try {
      Files.setPosixFilePermissions(
          target, Files.getPosixFilePermissions(source, LinkOption.NOFOLLOW_LINKS));
    } catch (UnsupportedOperationException ignored) {
      // Non-POSIX file systems still get a complete, no-clobber replacement.
    }
  }

  private static boolean isEmptyDirectory(Path directory) throws IOException {
    try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      return !entries.iterator().hasNext();
    }
  }

  private static boolean processReferenceIsStale(long pid, Path nodeDirectory) throws IOException {
    Optional<ProcessHandle> initial = ProcessHandle.of(pid);
    if (initial.isEmpty() || !isProcessAlive(initial.get())) {
      return true;
    }
    if (nodeProcessBelongsTo(initial.get(), nodeDirectory)) {
      return false;
    }
    OwnedProcess foreign = new OwnedProcess(initial.get());
    if (foreign.startTicks.isEmpty()) {
      throw new IOException("Cannot prove that foreign PID " + pid + " retained its identity");
    }
    Optional<ProcessHandle> current = ProcessHandle.of(pid);
    if (current.isEmpty() || !isProcessAlive(current.get())) {
      return true;
    }
    if (!sameProcess(foreign, current.get())) {
      throw new IOException("PID " + pid + " changed identity while sanitizing CCM state");
    }
    if (nodeProcessBelongsTo(current.get(), nodeDirectory)) {
      // The process may have completed exec between the two command-line snapshots.
      return false;
    }
    return true;
  }

  private static void makeProcessReferenceSafe(long pid, Path nodeDirectory) throws IOException {
    Optional<ProcessHandle> initial = ProcessHandle.of(pid);
    if (initial.isEmpty() || !isProcessAlive(initial.get())) {
      return;
    }
    if (!nodeProcessBelongsTo(initial.get(), nodeDirectory)) {
      if (!processReferenceIsStale(pid, nodeDirectory)) {
        throw new IOException("Foreign PID " + pid + " became a CCM node process unexpectedly");
      }
      return;
    }

    OwnedProcess owned = new OwnedProcess(initial.get());
    if (owned.startTicks.isEmpty()) {
      throw new IOException("Cannot capture stable identity for owned PID " + pid);
    }
    reapOwnedNode(new OwnedNodeState(nodeDirectory.toAbsolutePath().normalize(), List.of(owned)));
  }

  private static void sanitizeAndReapNode(Path nodeDirectory) throws IOException {
    sanitizeStaleNodeProcessReferences(nodeDirectory);
    reapRecognizedNodeProcesses(nodeDirectory);
    Set<Long> remainingReferences = readNodePids(nodeDirectory);
    if (!remainingReferences.isEmpty()) {
      throw new IOException(
          "CCM process references remain after safe sanitization in "
              + nodeDirectory
              + ": "
              + remainingReferences);
    }
  }

  private static void reapRecognizedNodeProcesses(Path nodeDirectory) throws IOException {
    java.nio.file.attribute.UserPrincipal currentOwner =
        Files.getOwner(
            Path.of("/proc", Long.toString(ProcessHandle.current().pid())),
            LinkOption.NOFOLLOW_LINKS);
    for (int attempt = 0; attempt < REFERENCE_SANITIZE_ATTEMPTS; attempt++) {
      Map<Long, OwnedProcess> owned = new LinkedHashMap<>();
      try (java.util.stream.Stream<ProcessHandle> processes = ProcessHandle.allProcesses()) {
        java.util.Iterator<ProcessHandle> iterator = processes.iterator();
        while (iterator.hasNext()) {
          ProcessHandle process = iterator.next();
          Path processDirectory = Path.of("/proc", Long.toString(process.pid()));
          try {
            if (!currentOwner.equals(Files.getOwner(processDirectory, LinkOption.NOFOLLOW_LINKS))) {
              continue;
            }
          } catch (java.nio.file.NoSuchFileException ignored) {
            continue;
          }
          if (!isProcessAlive(process) || !nodeProcessBelongsTo(process, nodeDirectory)) {
            continue;
          }
          OwnedProcess identity = new OwnedProcess(process);
          if (identity.startTicks.isEmpty()) {
            throw new IOException(
                "Cannot capture stable identity for CCM node process " + process.pid());
          }
          owned.put(process.pid(), identity);
        }
      }
      if (owned.isEmpty()) {
        return;
      }
      reapOwnedNode(
          new OwnedNodeState(
              nodeDirectory.toAbsolutePath().normalize(), new ArrayList<>(owned.values())));
    }
    throw new IOException("CCM node processes changed repeatedly during cleanup: " + nodeDirectory);
  }

  private static void addPid(String value, Path source, Set<Long> pids) throws IOException {
    pids.add(parsePid(value, source));
  }

  private static long parsePid(String value, Path source) throws IOException {
    try {
      long pid = Long.parseLong(value);
      if (pid < 2) {
        throw new NumberFormatException();
      }
      return pid;
    } catch (NumberFormatException exception) {
      throw new IOException("Invalid process ID in " + source, exception);
    }
  }

  private static boolean nodeProcessBelongsTo(ProcessHandle process, Path nodeDirectory)
      throws IOException {
    Path expectedDirectory = nodeDirectory.toAbsolutePath().normalize();
    List<String> arguments = readProcessArguments(process);
    if (arguments.isEmpty()) {
      return false;
    }
    String executable = arguments.get(0);
    if (executable.equals(expectedDirectory.resolve("bin/scylla").toString())) {
      return true;
    }

    String jmxLauncher = expectedDirectory.resolve("bin/symlinks/scylla-jmx").toString();
    if (executable.equals(jmxLauncher)) {
      return hasJmxJarOperand(arguments, expectedDirectory);
    }
    Path executableName;
    try {
      executableName = Path.of(executable).getFileName();
    } catch (RuntimeException ignored) {
      return false;
    }
    if (executableName == null) {
      return false;
    }
    if ("java".equals(executableName.toString())
        && hasJmxJarOperand(arguments, expectedDirectory)) {
      return true;
    }
    if (!"scylla-manager-agent".equals(executableName.toString())) {
      return false;
    }
    String expectedAgentConfig =
        expectedDirectory.resolve("conf/scylla-manager-agent.yaml").toString();
    for (int index = 1; index + 1 < arguments.size(); index++) {
      if ("--config-file".equals(arguments.get(index))
          && expectedAgentConfig.equals(arguments.get(index + 1))) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasJmxJarOperand(List<String> arguments, Path nodeDirectory) {
    String jarPrefix = nodeDirectory.resolve("bin/scylla-jmx-").toString();
    for (int index = 1; index + 1 < arguments.size(); index++) {
      String jar = arguments.get(index + 1);
      if ("-jar".equals(arguments.get(index))
          && jar.startsWith(jarPrefix)
          && jar.endsWith(".jar")) {
        return true;
      }
    }
    return false;
  }

  private static List<String> readProcessArguments(ProcessHandle process) throws IOException {
    byte[] commandLine;
    try {
      commandLine = Files.readAllBytes(Path.of("/proc", Long.toString(process.pid()), "cmdline"));
    } catch (IOException exception) {
      if (!isProcessAlive(process)) {
        return List.of();
      }
      throw new IOException("Unable to verify ownership of PID " + process.pid(), exception);
    }
    List<String> arguments = new ArrayList<>();
    int start = 0;
    for (int index = 0; index <= commandLine.length; index++) {
      if (index == commandLine.length || commandLine[index] == 0) {
        String argument = new String(commandLine, start, index - start, StandardCharsets.UTF_8);
        if (!argument.isEmpty()) {
          arguments.add(argument);
        }
        start = index + 1;
      }
    }
    return arguments;
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
      if (commandEnd >= 0 && commandEnd + 2 < stat.length()) {
        return stat.charAt(commandEnd + 2) != 'Z';
      }
    } catch (IOException ignored) {
      // A process that disappeared while being inspected is not alive.
      return process.isAlive();
    }
    return process.isAlive();
  }

  private static Optional<Long> readProcessStartTicks(long pid) {
    try {
      String stat =
          Files.readString(Path.of("/proc", Long.toString(pid), "stat"), StandardCharsets.US_ASCII);
      int commandEnd = stat.lastIndexOf(')');
      if (commandEnd < 0 || commandEnd + 2 >= stat.length()) {
        return Optional.empty();
      }
      String[] fields = stat.substring(commandEnd + 2).split("\\s+");
      if (fields.length <= 19) {
        return Optional.empty();
      }
      return Optional.of(Long.parseLong(fields[19]));
    } catch (IOException | NumberFormatException ignored) {
      return Optional.empty();
    }
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

  private void collectDiagnosticsBestEffort(String instanceId, Path ccmDirectory) {
    try {
      if (!Files.isDirectory(ccmDirectory)) {
        return;
      }
      Path destination = diagnosticsDirectory.resolve(instanceId);
      Files.walkFileTree(
          ccmDirectory,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Path relative = ccmDirectory.relativize(file);
              if (shouldCollect(relative)) {
                Path target = destination.resolve(relative);
                Files.createDirectories(target.getParent());
                Files.copy(file, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (Exception ignored) {
      // Diagnostics must not mask the provisioning or cleanup failure.
    }
  }

  private static boolean shouldCollect(Path relativePath) {
    String path = relativePath.toString().replace('\\', '/');
    return path.equals("ccm-commands.log")
        || path.endsWith("/cluster.conf")
        || path.endsWith("/node.conf")
        || path.endsWith("/conf/scylla.yaml")
        || path.contains("/logs/");
  }

  private void removeByName(String instanceId, Path ccmDirectory) throws Exception {
    reapRetainedCommandGroups(ccmDirectory);
    Path clusterDirectory =
        ownedClusterDirectory(ccmDirectory, instanceId).toAbsolutePath().normalize();
    List<OwnedNodeState> ownedNodes =
        retainNodeStates(ccmDirectory, snapshotOwnedNodes(clusterDirectory));
    Exception removalFailure = null;
    try {
      runCcm(ccmDirectory, List.of("remove", "--config-dir", ccmDirectory.toString(), instanceId));
    } catch (Exception exception) {
      removalFailure = exception;
    }
    if (removalFailure instanceof CcmProcessCleanupException) {
      throw removalFailure;
    }

    try {
      if (!clusterStateExists(instanceId, ccmDirectory)) {
        reapOwnedNodes(ownedNodes);
        cleanupAbsentClusterState(instanceId, ccmDirectory);
        clearRetainedNodeStates(ccmDirectory, null);
        restoreRemovalInterrupt(removalFailure);
        return;
      }
    } catch (Exception stateException) {
      if (removalFailure != null && removalFailure != stateException) {
        stateException.addSuppressed(removalFailure);
      }
      throw stateException;
    }
    if (removalFailure != null) {
      throw removalFailure;
    }
    throw new IOException("CCM reported success but cluster '" + instanceId + "' still exists");
  }

  private void removeNodeByName(Path ccmDirectory, String nodeName) throws Exception {
    reapRetainedCommandGroups(ccmDirectory);
    Path currentClusterBefore = currentClusterDirectory(ccmDirectory);
    OwnedNodeState ownedNode = null;
    if (currentClusterBefore != null) {
      Path nodeDirectory = ownedChild(currentClusterBefore, nodeName);
      PathState nodeState = pathState(nodeDirectory);
      if (nodeState == PathState.PRESENT) {
        ownedNode = snapshotOwnedNode(nodeDirectory);
      } else if (nodeState == PathState.UNKNOWN) {
        throw new IOException("Cannot determine CCM node state at " + nodeDirectory);
      }
    }
    List<OwnedNodeState> ownedNodes =
        retainNodeStates(ccmDirectory, ownedNode == null ? List.of() : List.of(ownedNode));
    ownedNode =
        ownedNodes.stream()
            .filter(state -> state.directory.getFileName().toString().equals(nodeName))
            .findFirst()
            .orElse(null);
    Exception removalFailure = null;
    try {
      runCcm(ccmDirectory, List.of(nodeName, "remove", "--config-dir", ccmDirectory.toString()));
    } catch (Exception exception) {
      removalFailure = exception;
    }
    if (removalFailure instanceof CcmProcessCleanupException) {
      throw removalFailure;
    }

    Path currentCluster = currentClusterDirectory(ccmDirectory);
    Path verificationCluster = currentCluster == null ? currentClusterBefore : currentCluster;
    if (verificationCluster != null
        && pathState(verificationCluster.resolve(nodeName)) == PathState.ABSENT) {
      if (ownedNode != null) {
        reapOwnedNode(ownedNode);
      }
      clearRetainedNodeStates(ccmDirectory, nodeName);
      restoreRemovalInterrupt(removalFailure);
      return;
    }
    if (verificationCluster != null
        && pathState(verificationCluster.resolve("cluster.conf")) == PathState.PRESENT
        && nodeAbsentFromClusterMetadata(verificationCluster, nodeName)) {
      if (ownedNode != null) {
        reapOwnedNode(ownedNode);
      }
      Path nodeDirectory = ownedChild(verificationCluster, nodeName);
      deleteRecursively(nodeDirectory);
      if (pathState(nodeDirectory) != PathState.ABSENT) {
        throw new IOException("Node directory remains after fallback cleanup: " + nodeDirectory);
      }
      clearRetainedNodeStates(ccmDirectory, nodeName);
      restoreRemovalInterrupt(removalFailure);
      return;
    }
    if (removalFailure != null) {
      throw removalFailure;
    }
    throw new IOException("CCM reported success but node '" + nodeName + "' still exists");
  }

  private static void restoreRemovalInterrupt(Exception removalFailure) {
    if (removalFailure instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class OwnedProcess {
    final long pid;
    final Optional<Long> startTicks;

    OwnedProcess(ProcessHandle process) {
      this.pid = process.pid();
      this.startTicks = readProcessStartTicks(process.pid());
    }
  }

  private static final class QuarantinedReference {
    final Path original;
    final Path quarantine;
    final Path claimed;
    final Path prepared;

    QuarantinedReference(Path original, Path quarantine, Path claimed, Path prepared) {
      this.original = original;
      this.quarantine = quarantine;
      this.claimed = claimed;
      this.prepared = prepared;
    }
  }

  private static final class RetainedCommandGroup {
    final long processGroup;
    final Path ccmDirectory;
    final List<OwnedProcess> observedProcesses;
    final boolean completeIdentitySnapshot;

    RetainedCommandGroup(
        long processGroup,
        Path ccmDirectory,
        List<OwnedProcess> observedProcesses,
        boolean completeIdentitySnapshot) {
      this.processGroup = processGroup;
      this.ccmDirectory = ccmDirectory;
      this.observedProcesses = observedProcesses;
      this.completeIdentitySnapshot = completeIdentitySnapshot;
    }
  }

  private static final class OwnedNodeState {
    final Path directory;
    final List<OwnedProcess> processes;

    OwnedNodeState(Path directory, List<OwnedProcess> processes) {
      this.directory = directory;
      this.processes = processes;
    }
  }

  private List<OwnedNodeState> retainNodeStates(
      Path ccmDirectory, List<OwnedNodeState> discovered) {
    Path key = ccmDirectory.toAbsolutePath().normalize();
    synchronized (retainedNodeStates) {
      Map<Path, List<OwnedProcess>> merged = new LinkedHashMap<>();
      for (OwnedNodeState state : retainedNodeStates.getOrDefault(key, List.of())) {
        merged.put(state.directory, new ArrayList<>(state.processes));
      }
      for (OwnedNodeState state : discovered) {
        List<OwnedProcess> processes =
            merged.computeIfAbsent(state.directory, ignored -> new ArrayList<>());
        for (OwnedProcess process : state.processes) {
          if (processes.stream()
              .noneMatch(
                  existing ->
                      existing.pid == process.pid
                          && Objects.equals(existing.startTicks, process.startTicks))) {
            processes.add(process);
          }
        }
      }
      List<OwnedNodeState> retained = new ArrayList<>();
      merged.forEach(
          (directory, processes) -> retained.add(new OwnedNodeState(directory, processes)));
      if (retained.isEmpty()) {
        retainedNodeStates.remove(key);
      } else {
        retainedNodeStates.put(key, retained);
      }
      return new ArrayList<>(retained);
    }
  }

  private void clearRetainedNodeStates(Path ccmDirectory, String nodeName) {
    Path key = ccmDirectory.toAbsolutePath().normalize();
    synchronized (retainedNodeStates) {
      if (nodeName == null) {
        retainedNodeStates.remove(key);
        return;
      }
      List<OwnedNodeState> retained = retainedNodeStates.get(key);
      if (retained == null) {
        return;
      }
      retained.removeIf(state -> state.directory.getFileName().toString().equals(nodeName));
      if (retained.isEmpty()) {
        retainedNodeStates.remove(key);
      }
    }
  }

  private static List<OwnedNodeState> snapshotOwnedNodes(Path clusterDirectory) throws IOException {
    List<OwnedNodeState> result = new ArrayList<>();
    if (!Files.isDirectory(clusterDirectory, LinkOption.NOFOLLOW_LINKS)) {
      return result;
    }
    try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(clusterDirectory)) {
      for (Path entry : entries) {
        if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)
            && entry.getFileName().toString().startsWith("node")) {
          result.add(snapshotOwnedNode(entry));
        }
      }
    }
    return result;
  }

  private static OwnedNodeState snapshotOwnedNode(Path nodeDirectory) throws IOException {
    if (Files.isSymbolicLink(nodeDirectory)) {
      throw new IOException("Refusing symlinked CCM node directory " + nodeDirectory);
    }
    sanitizeAndReapNode(nodeDirectory);
    return new OwnedNodeState(nodeDirectory.toAbsolutePath().normalize(), List.of());
  }

  private static void reapOwnedNodes(List<OwnedNodeState> nodes) throws IOException {
    IOException failure = null;
    for (OwnedNodeState node : nodes) {
      try {
        reapOwnedNode(node);
      } catch (IOException exception) {
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void reapOwnedNode(OwnedNodeState node) throws IOException {
    for (OwnedProcess owned : node.processes) {
      Optional<ProcessHandle> current = ProcessHandle.of(owned.pid);
      if (current.isEmpty() || !isProcessAlive(current.get())) {
        continue;
      }
      if (!sameProcess(owned, current.get())) {
        continue;
      }
      if (!nodeProcessBelongsTo(current.get(), node.directory)) {
        throw new IOException("PID " + owned.pid + " changed ownership during CCM cleanup");
      }
      current.get().destroy();
    }
    waitForOwnedProcesses(node.processes, PROCESS_TERMINATION_GRACE);
    for (OwnedProcess owned : node.processes) {
      Optional<ProcessHandle> current = ProcessHandle.of(owned.pid);
      if (current.isPresent()
          && isProcessAlive(current.get())
          && sameProcess(owned, current.get())) {
        if (!nodeProcessBelongsTo(current.get(), node.directory)) {
          throw new IOException("PID " + owned.pid + " changed ownership during CCM cleanup");
        }
        current.get().destroyForcibly();
      }
    }
    waitForOwnedProcesses(node.processes, PROCESS_KILL_TIMEOUT);
    List<Long> survivors = new ArrayList<>();
    for (OwnedProcess owned : node.processes) {
      if (ownedProcessStillAlive(owned)) {
        survivors.add(owned.pid);
      }
    }
    if (!survivors.isEmpty()) {
      throw new IOException("Scylla node processes survived cleanup: " + survivors);
    }
  }

  private static void waitForOwnedProcesses(List<OwnedProcess> processes, Duration timeout) {
    boolean interrupted = false;
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      boolean anyAlive = processes.stream().anyMatch(CcmProvisioner::ownedProcessStillAlive);
      if (!anyAlive) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean ownedProcessStillAlive(OwnedProcess expected) {
    Optional<ProcessHandle> current = ProcessHandle.of(expected.pid);
    if (current.isEmpty() || !isProcessAlive(current.get())) {
      return false;
    }
    Optional<Long> currentStart = readProcessStartTicks(expected.pid);
    if (expected.startTicks.isEmpty() || currentStart.isEmpty()) {
      return true;
    }
    return expected.startTicks.equals(currentStart);
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
    return pathState(ownedClusterDirectory(ccmDirectory, instanceId).resolve("cluster.conf"))
        != PathState.ABSENT;
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
    if (Files.isSymbolicLink(clusterDirectory)
        || !Files.isDirectory(clusterDirectory, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Refusing unsafe CCM cluster directory " + clusterDirectory);
    }
    Path realParent = clusterDirectory.toRealPath().getParent();
    if (!ccmDirectory.equals(realParent)) {
      throw new IOException(
          "Refusing CCM cluster outside its config directory: " + clusterDirectory);
    }
    return clusterDirectory;
  }

  private Path requireOwnedCcmDirectory(Path ccmDirectory) throws IOException {
    validateOwnedDirectory(runDirectory, clustersDirectory, "clusters");
    Path normalized = ccmDirectory.toAbsolutePath().normalize();
    if (!clustersDirectory.equals(normalized.getParent())) {
      throw new IOException("Refusing CCM config outside run state: " + ccmDirectory);
    }
    validateOwnedDirectory(clustersDirectory, normalized, "CCM config");
    return normalized;
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
    if (pathState(currentPath) == PathState.PRESENT
        && instanceId.equals(Files.readString(currentPath, StandardCharsets.UTF_8).trim())) {
      Files.deleteIfExists(currentPath);
    }
  }

  private void reapAndCleanupAbsentClusterState(String instanceId, Path ccmDirectory)
      throws IOException {
    try {
      reapRetainedCommandGroups(ccmDirectory);
    } catch (IOException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new IOException("Unable to reap retained CCM processes before rollback", exception);
    }
    Path clusterDirectory =
        ownedClusterDirectory(ccmDirectory, instanceId).toAbsolutePath().normalize();
    List<OwnedNodeState> ownedNodes =
        retainNodeStates(ccmDirectory, snapshotOwnedNodes(clusterDirectory));
    reapOwnedNodes(ownedNodes);
    cleanupAbsentClusterState(instanceId, ccmDirectory);
    clearRetainedNodeStates(ccmDirectory, null);
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (pathState(root) == PathState.ABSENT) {
      return;
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
    if (Files.exists(path)) {
      return PathState.PRESENT;
    }
    return Files.notExists(path) ? PathState.ABSENT : PathState.UNKNOWN;
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
    reapRetainedCommandGroups(ccmDirectory);
    List<String> command = new ArrayList<>();
    command.add(ccmExecutable);
    command.addAll(arguments);
    runCommand(ccmDirectory, command);
  }

  private void runCommand(Path ccmDirectory, List<String> command) throws Exception {
    if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
      throw new UnsupportedOperationException(
          "The native scylla-ccm harness currently supports Linux only");
    }
    ccmDirectory = requireOwnedCcmDirectory(ccmDirectory);
    Path outputPath = Files.createTempFile(ccmDirectory, "ccm-command-", ".log");
    Process process;
    List<String> launchedCommand = new ArrayList<>();
    launchedCommand.add("setsid");
    launchedCommand.addAll(command);
    try {
      process =
          new ProcessBuilder(launchedCommand)
              .redirectErrorStream(true)
              .redirectOutput(outputPath.toFile())
              .start();
    } catch (IOException startException) {
      try {
        Files.deleteIfExists(outputPath);
      } catch (IOException cleanupException) {
        startException.addSuppressed(cleanupException);
      }
      throw startException;
    }
    boolean exited = false;
    boolean timedOut = false;
    InterruptedException interruption = null;
    Exception terminationFailure = null;
    boolean cleanupInterrupted = false;
    Map<Long, OwnedProcess> observedProcesses = new HashMap<>();
    snapshotProcessTree(process.toHandle(), observedProcesses);
    long commandDeadline = System.nanoTime() + commandTimeout.toNanos();
    try {
      while (!exited) {
        long remainingNanos = commandDeadline - System.nanoTime();
        if (remainingNanos <= 0) {
          timedOut = true;
          terminationFailure = terminateProcessGroup(process, observedProcesses, ccmDirectory);
          cleanupInterrupted |= Thread.interrupted();
          break;
        }
        long waitMillis = Math.max(1, Math.min(100, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        exited = process.waitFor(waitMillis, TimeUnit.MILLISECONDS);
        snapshotProcessTree(process.toHandle(), observedProcesses);
      }
    } catch (InterruptedException exception) {
      interruption = exception;
      snapshotProcessTree(process.toHandle(), observedProcesses);
      terminationFailure = terminateProcessGroup(process, observedProcesses, ccmDirectory);
      cleanupInterrupted |= Thread.interrupted();
    }
    if (exited && process.exitValue() != 0) {
      terminationFailure = terminateProcessGroup(process, observedProcesses, ccmDirectory);
      cleanupInterrupted |= Thread.interrupted();
    }
    long reapDeadline = System.nanoTime() + PROCESS_KILL_TIMEOUT.toNanos();
    while (process.isAlive() && System.nanoTime() < reapDeadline) {
      try {
        process.waitFor(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException exception) {
        if (interruption == null) {
          interruption = exception;
        } else {
          interruption.addSuppressed(exception);
        }
        Exception retryFailure = terminateProcessGroup(process, observedProcesses, ccmDirectory);
        cleanupInterrupted |= Thread.interrupted();
        if (terminationFailure == null) {
          terminationFailure = retryFailure;
        } else if (retryFailure != null && retryFailure != terminationFailure) {
          terminationFailure.addSuppressed(retryFailure);
        }
      }
    }
    if (process.isAlive()) {
      if (!(terminationFailure instanceof CcmProcessCleanupException)) {
        terminationFailure =
            new CcmProcessCleanupException(
                "Direct CCM command process survived termination: " + process.pid());
        retainCommandGroup(ccmDirectory, process.pid(), observedProcesses, false);
      }
    }
    if (cleanupInterrupted && interruption == null) {
      interruption = new InterruptedException("CCM command cleanup was interrupted");
    }

    byte[] output;
    try {
      output = Files.readAllBytes(outputPath);
    } catch (IOException outputException) {
      if (terminationFailure != null) {
        terminationFailure.addSuppressed(outputException);
        if (interruption != null) {
          terminationFailure.addSuppressed(interruption);
          Thread.currentThread().interrupt();
        }
        throw terminationFailure;
      }
      if (interruption != null) {
        outputException.addSuppressed(interruption);
        Thread.currentThread().interrupt();
      }
      try {
        Files.deleteIfExists(outputPath);
      } catch (IOException cleanupException) {
        outputException.addSuppressed(cleanupException);
      }
      throw outputException;
    }
    String commandText = String.join(" ", command);
    String outcome =
        interruption != null
            ? "interrupted"
            : timedOut ? "timeout" : Integer.toString(process.exitValue());
    String log =
        "> "
            + commandText
            + "\n"
            + new String(output, StandardCharsets.UTF_8)
            + "[exit "
            + outcome
            + "]\n";
    IOException diagnosticFailure = null;
    try {
      Files.writeString(
          ccmDirectory.resolve("ccm-commands.log"),
          log,
          StandardCharsets.UTF_8,
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.APPEND);
      System.out.print(log);
    } catch (IOException exception) {
      diagnosticFailure = exception;
    }
    try {
      Files.deleteIfExists(outputPath);
    } catch (IOException exception) {
      if (diagnosticFailure == null) {
        diagnosticFailure = exception;
      } else {
        diagnosticFailure.addSuppressed(exception);
      }
    }
    if (interruption != null) {
      if (diagnosticFailure != null) {
        interruption.addSuppressed(diagnosticFailure);
      }
      if (terminationFailure != null) {
        terminationFailure.addSuppressed(interruption);
        if (diagnosticFailure != null) {
          terminationFailure.addSuppressed(diagnosticFailure);
        }
        Thread.currentThread().interrupt();
        throw terminationFailure;
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
    if (!exited || process.exitValue() != 0) {
      CcmCommandException commandException =
          new CcmCommandException(
              commandText,
              timedOut ? -1 : process.exitValue(),
              new String(output, StandardCharsets.UTF_8));
      if (diagnosticFailure != null) {
        commandException.addSuppressed(diagnosticFailure);
      }
      throw commandException;
    }
    if (diagnosticFailure != null) {
      throw diagnosticFailure;
    }
  }

  private Exception terminateProcessGroup(
      Process process, Map<Long, OwnedProcess> previouslyObserved, Path ccmDirectory) {
    Map<Long, OwnedProcess> owned = new HashMap<>(previouslyObserved);
    snapshotProcessTree(process.toHandle(), owned);
    try {
      Set<ProcessHandle> initialGroup =
          verifiedGroupMembers(process.pid(), owned, ccmDirectory, false);
      initialGroup.forEach(member -> rememberProcess(member, owned));
      if (!initialGroup.isEmpty()) {
        signalProcessGroup(process.pid(), "TERM");
      }
    } catch (Exception exception) {
      retainCommandGroup(ccmDirectory, process.pid(), owned, false);
      CcmProcessCleanupException cleanupFailure =
          new CcmProcessCleanupException(
              "Unable to capture the initial CCM process group " + process.pid());
      cleanupFailure.addSuppressed(exception);
      return cleanupFailure;
    }
    waitForProcesses(currentObservedProcesses(owned), PROCESS_TERMINATION_GRACE);
    waitForProcessGroup(process.pid(), PROCESS_TERMINATION_GRACE);
    snapshotProcessTree(process.toHandle(), owned);
    Set<ProcessHandle> remainingObserved = currentObservedProcesses(owned);
    Set<ProcessHandle> remainingGroup;
    try {
      remainingGroup = verifiedGroupMembers(process.pid(), owned, ccmDirectory, false);
      remainingGroup.forEach(member -> rememberProcess(member, owned));
    } catch (Exception exception) {
      retainCommandGroup(ccmDirectory, process.pid(), owned, false);
      CcmProcessCleanupException cleanupFailure =
          new CcmProcessCleanupException(
              "Unable to verify CCM process group " + process.pid() + " during termination");
      cleanupFailure.addSuppressed(exception);
      return cleanupFailure;
    }
    if (!remainingGroup.isEmpty() || !remainingObserved.isEmpty()) {
      try {
        if (!remainingGroup.isEmpty()) {
          signalProcessGroup(process.pid(), "KILL");
        }
      } catch (Exception exception) {
        retainCommandGroup(ccmDirectory, process.pid(), owned, false);
        CcmProcessCleanupException cleanupFailure =
            new CcmProcessCleanupException("Unable to kill CCM process group " + process.pid());
        cleanupFailure.addSuppressed(exception);
        return cleanupFailure;
      }
      remainingObserved.stream()
          .filter(CcmProvisioner::isProcessAlive)
          .sorted(Comparator.comparingLong(ProcessHandle::pid).reversed())
          .forEach(ProcessHandle::destroyForcibly);
      waitForProcesses(currentObservedProcesses(owned), PROCESS_KILL_TIMEOUT);
      waitForProcessGroup(process.pid(), PROCESS_KILL_TIMEOUT);
    }
    List<Long> survivors = new ArrayList<>();
    for (OwnedProcess observed : owned.values()) {
      if (ownedProcessStillAlive(observed)) {
        survivors.add(observed.pid);
      }
    }
    try {
      Set<ProcessHandle> finalGroup =
          verifiedGroupMembers(process.pid(), owned, ccmDirectory, false);
      finalGroup.forEach(member -> rememberProcess(member, owned));
      survivors.addAll(
          finalGroup.stream()
              .map(ProcessHandle::pid)
              .filter(pid -> !survivors.contains(pid))
              .collect(java.util.stream.Collectors.toList()));
    } catch (Exception exception) {
      retainCommandGroup(ccmDirectory, process.pid(), owned, false);
      CcmProcessCleanupException cleanupFailure =
          new CcmProcessCleanupException(
              "Unable to verify that CCM process group " + process.pid() + " exited");
      cleanupFailure.addSuppressed(exception);
      return cleanupFailure;
    }
    if (!survivors.isEmpty()) {
      IOException survivorFailure =
          new CcmProcessCleanupException(
              "CCM command processes survived termination: " + survivors);
      retainCommandGroup(ccmDirectory, process.pid(), owned, true);
      return survivorFailure;
    }
    return null;
  }

  private void retainCommandGroup(
      Path ccmDirectory,
      long processGroup,
      Map<Long, OwnedProcess> observedProcesses,
      boolean completeIdentitySnapshot) {
    Path key = ccmDirectory.toAbsolutePath().normalize();
    List<OwnedProcess> observed = new ArrayList<>(observedProcesses.values());
    synchronized (retainedCommandGroups) {
      List<RetainedCommandGroup> groups =
          retainedCommandGroups.computeIfAbsent(key, ignored -> new ArrayList<>());
      for (int index = 0; index < groups.size(); index++) {
        RetainedCommandGroup existing = groups.get(index);
        if (existing.processGroup == processGroup) {
          Map<Long, OwnedProcess> merged = new LinkedHashMap<>();
          boolean identitiesComplete = completeIdentitySnapshot;
          for (OwnedProcess process : existing.observedProcesses) {
            merged.put(process.pid, process);
            identitiesComplete &= process.startTicks.isPresent();
          }
          for (OwnedProcess process : observed) {
            OwnedProcess previous = merged.putIfAbsent(process.pid, process);
            identitiesComplete &= process.startTicks.isPresent();
            if (previous != null
                && (!previous.startTicks.isPresent()
                    || !previous.startTicks.equals(process.startTicks))) {
              // An identity that could not be captured, or a reused PID, can never be upgraded
              // into a complete snapshot after the fact.
              identitiesComplete = false;
            }
          }
          groups.set(
              index,
              new RetainedCommandGroup(
                  processGroup,
                  key,
                  new ArrayList<>(merged.values()),
                  existing.completeIdentitySnapshot && identitiesComplete));
          return;
        }
      }
      completeIdentitySnapshot &=
          observed.stream().allMatch(process -> process.startTicks.isPresent());
      groups.add(new RetainedCommandGroup(processGroup, key, observed, completeIdentitySnapshot));
    }
  }

  private Set<ProcessHandle> verifiedGroupMembers(
      long processGroup,
      Map<Long, OwnedProcess> observed,
      Path ccmDirectory,
      boolean allowReusedGroup)
      throws IOException {
    Set<ProcessHandle> observedCurrent = currentObservedProcesses(observed);
    Set<ProcessHandle> verified = new HashSet<>();
    List<ProcessHandle> members = processesInGroup(processGroup);
    boolean hasObservedAnchor = members.stream().anyMatch(observedCurrent::contains);
    boolean hasRecognizedAnchor = false;
    if (members.isEmpty()) {
      // An empty /proc group scan is definitive even when an earlier identity snapshot was
      // incomplete: there is no numeric process group left to retain or accidentally signal.
      return verified;
    }
    if (!hasObservedAnchor) {
      for (ProcessHandle member : members) {
        if (commandProcessBelongsTo(member, ccmDirectory)) {
          hasRecognizedAnchor = true;
          break;
        }
      }
    }
    if (!hasObservedAnchor && !hasRecognizedAnchor) {
      if (allowReusedGroup) {
        // The old process group number was reused after every retained process exited.
        return verified;
      }
      throw new IOException(
          "Cannot distinguish retained process group " + processGroup + " from a reused group");
    }
    verified.addAll(members);
    return verified;
  }

  private boolean commandProcessBelongsTo(ProcessHandle process, Path ccmDirectory)
      throws IOException {
    Path expectedDirectory = ccmDirectory.toAbsolutePath().normalize();
    String directoryPrefix = expectedDirectory + "/";
    List<String> arguments = readProcessArguments(process);
    if (arguments.isEmpty()) {
      return false;
    }
    boolean referencesDirectory =
        arguments.stream()
            .anyMatch(
                argument ->
                    argument.equals(expectedDirectory.toString())
                        || argument.startsWith(directoryPrefix));
    if (!referencesDirectory) {
      return false;
    }
    for (int index = 0; index < arguments.size(); index++) {
      String argument = arguments.get(index);
      String fileName;
      try {
        Path argumentPath = Path.of(argument);
        Path name = argumentPath.getFileName();
        fileName = name == null ? argument : name.toString();
      } catch (RuntimeException ignored) {
        fileName = argument;
      }
      if (index <= 1 && (argument.equals(ccmExecutable) || fileName.equals("ccm"))) {
        return true;
      }
    }
    String executable = arguments.get(0);
    String executableName;
    try {
      Path name = Path.of(executable).getFileName();
      executableName = name == null ? executable : name.toString();
    } catch (RuntimeException ignored) {
      executableName = executable;
    }
    if ("openssl".equals(executableName)
        || (executable.startsWith(directoryPrefix) && executable.endsWith("/bin/scylla"))) {
      return true;
    }
    if (executable.startsWith(directoryPrefix) && executable.endsWith("/bin/symlinks/scylla-jmx")) {
      return hasCcmJmxJarOperand(arguments, directoryPrefix);
    }
    if ("java".equals(executableName) && hasCcmJmxJarOperand(arguments, directoryPrefix)) {
      return true;
    }
    if (!"scylla-manager-agent".equals(executableName)) {
      return false;
    }
    for (int index = 1; index + 1 < arguments.size(); index++) {
      String config = arguments.get(index + 1);
      if ("--config-file".equals(arguments.get(index))
          && config.startsWith(directoryPrefix)
          && config.endsWith("/conf/scylla-manager-agent.yaml")) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasCcmJmxJarOperand(List<String> arguments, String directoryPrefix) {
    for (int index = 1; index + 1 < arguments.size(); index++) {
      String jar = arguments.get(index + 1);
      if ("-jar".equals(arguments.get(index))
          && jar.startsWith(directoryPrefix)
          && jar.contains("/bin/scylla-jmx-")
          && jar.endsWith(".jar")) {
        return true;
      }
    }
    return false;
  }

  private void reapRetainedCommandGroups(Path ccmDirectory) throws Exception {
    Path key = ccmDirectory.toAbsolutePath().normalize();
    List<RetainedCommandGroup> groups;
    synchronized (retainedCommandGroups) {
      groups = new ArrayList<>(retainedCommandGroups.getOrDefault(key, List.of()));
    }
    for (RetainedCommandGroup group : groups) {
      try {
        reapRetainedCommandGroup(group);
      } catch (CcmProcessCleanupException exception) {
        throw exception;
      } catch (Exception exception) {
        CcmProcessCleanupException cleanupFailure =
            new CcmProcessCleanupException(
                "Unable to reap retained CCM process group " + group.processGroup);
        cleanupFailure.addSuppressed(exception);
        throw cleanupFailure;
      }
      synchronized (retainedCommandGroups) {
        List<RetainedCommandGroup> retained = retainedCommandGroups.get(key);
        if (retained != null) {
          retained.removeIf(candidate -> candidate.processGroup == group.processGroup);
          if (retained.isEmpty()) {
            retainedCommandGroups.remove(key);
          }
        }
      }
    }
  }

  private void reapRetainedCommandGroup(RetainedCommandGroup group) throws Exception {
    Map<Long, OwnedProcess> observed = new HashMap<>();
    for (OwnedProcess process : group.observedProcesses) {
      observed.put(process.pid, process);
    }
    Set<ProcessHandle> owned = currentObservedProcesses(observed);
    Set<ProcessHandle> groupMembers =
        verifiedGroupMembers(
            group.processGroup, observed, group.ccmDirectory, group.completeIdentitySnapshot);
    owned.addAll(groupMembers);
    if (!groupMembers.isEmpty()) {
      signalProcessGroup(group.processGroup, "TERM");
    }
    for (ProcessHandle process : owned) {
      if (isProcessAlive(process)) {
        process.destroy();
      }
    }
    waitForProcesses(owned, PROCESS_TERMINATION_GRACE);
    waitForProcessGroup(group.processGroup, PROCESS_TERMINATION_GRACE);

    groupMembers =
        verifiedGroupMembers(
            group.processGroup, observed, group.ccmDirectory, group.completeIdentitySnapshot);
    owned = currentObservedProcesses(observed);
    owned.addAll(groupMembers);
    if (!groupMembers.isEmpty()) {
      signalProcessGroup(group.processGroup, "KILL");
    }
    for (ProcessHandle process : owned) {
      if (isProcessAlive(process)) {
        process.destroyForcibly();
      }
    }
    waitForProcesses(owned, PROCESS_KILL_TIMEOUT);
    waitForProcessGroup(group.processGroup, PROCESS_KILL_TIMEOUT);

    groupMembers =
        verifiedGroupMembers(
            group.processGroup, observed, group.ccmDirectory, group.completeIdentitySnapshot);
    List<Long> observedSurvivors =
        observed.values().stream()
            .filter(CcmProvisioner::ownedProcessStillAlive)
            .map(process -> process.pid)
            .collect(java.util.stream.Collectors.toList());
    if (!groupMembers.isEmpty() || !observedSurvivors.isEmpty()) {
      throw new CcmProcessCleanupException(
          "Retained CCM processes still survive cleanup: group="
              + group.processGroup
              + ", pids="
              + observedSurvivors);
    }
  }

  private static boolean sameProcess(OwnedProcess expected, ProcessHandle actual) {
    Optional<Long> currentStart = readProcessStartTicks(actual.pid());
    return expected.startTicks.isPresent() && expected.startTicks.equals(currentStart);
  }

  private static void snapshotProcessTree(ProcessHandle root, Map<Long, OwnedProcess> owned) {
    rememberProcess(root, owned);
    root.descendants().forEach(process -> rememberProcess(process, owned));
  }

  private static void rememberProcess(ProcessHandle process, Map<Long, OwnedProcess> owned) {
    // Never upgrade an identity that could not be captured. The PID may have been reused between
    // observations; retaining the incomplete first snapshot is the fail-closed representation.
    owned.computeIfAbsent(process.pid(), ignored -> new OwnedProcess(process));
  }

  private static Set<ProcessHandle> currentObservedProcesses(Map<Long, OwnedProcess> observed) {
    Set<ProcessHandle> current = new HashSet<>();
    for (OwnedProcess expected : observed.values()) {
      ProcessHandle.of(expected.pid)
          .filter(CcmProvisioner::isProcessAlive)
          .filter(process -> sameProcess(expected, process))
          .ifPresent(current::add);
    }
    return current;
  }

  private static void waitForProcesses(Set<ProcessHandle> processes, Duration timeout) {
    boolean interrupted = false;
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline
        && processes.stream().anyMatch(CcmProvisioner::isProcessAlive)) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static void waitForProcessGroup(long processGroup, Duration timeout) {
    boolean interrupted = false;
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline && processGroupExists(processGroup)) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static void signalProcessGroup(long processGroup, String signal) throws Exception {
    Process signalProcess =
        new ProcessBuilder("kill", "-" + signal, "--", "-" + processGroup)
            .redirectErrorStream(true)
            .start();
    byte[] output = signalProcess.getInputStream().readAllBytes();
    boolean interrupted = false;
    while (true) {
      try {
        if (signalProcess.waitFor() != 0
            && ProcessHandle.of(processGroup).map(CcmProvisioner::isProcessAlive).orElse(false)) {
          throw new IOException(
              "Unable to signal CCM process group "
                  + processGroup
                  + ": "
                  + new String(output, StandardCharsets.UTF_8));
        }
        break;
      } catch (InterruptedException exception) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean processGroupExists(long processGroup) {
    try {
      return !processesInGroup(processGroup).isEmpty();
    } catch (IOException exception) {
      // Failure to inspect /proc must retain ownership and prevent rollback.
      return true;
    }
  }

  private static List<ProcessHandle> processesInGroup(long processGroup) throws IOException {
    List<ProcessHandle> result = new ArrayList<>();
    try (java.nio.file.DirectoryStream<Path> processes =
        Files.newDirectoryStream(Path.of("/proc"))) {
      for (Path process : processes) {
        String name = process.getFileName().toString();
        if (!name.matches("[0-9]+")) {
          continue;
        }
        try {
          String stat = Files.readString(process.resolve("stat"), StandardCharsets.US_ASCII);
          int commandEnd = stat.lastIndexOf(')');
          if (commandEnd < 0 || commandEnd + 2 >= stat.length()) {
            continue;
          }
          String[] fields = stat.substring(commandEnd + 2).split("\\s+");
          if (fields.length > 2
              && !"Z".equals(fields[0])
              && Long.parseLong(fields[2]) == processGroup) {
            ProcessHandle.of(Long.parseLong(name)).ifPresent(result::add);
          }
        } catch (java.nio.file.NoSuchFileException ignored) {
          // The process exited while /proc was being scanned.
        } catch (IOException | NumberFormatException exception) {
          if (Files.exists(process, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Unable to inspect process group " + processGroup, exception);
          }
        }
      }
      return result;
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

  private static final class CcmProcessCleanupException extends IOException {
    CcmProcessCleanupException(String message) {
      super(message);
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
