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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Durable ownership for one Java CCM harness process. */
final class CcmRunState implements AutoCloseable {
  private static final Object SCAN_MUTEX = new Object();
  private static final String RUN_NAME_PATTERN = "ccm-runtime\\.[0-9a-f]{32}";
  private static final int FORMAT_VERSION = 1;
  private static final int MAXIMUM_METADATA_BYTES = 4096;
  private static final String LEGACY_STAGING_PATTERN = "\\.owner-[0-9]+\\.tmp";
  private static final Duration TERM_GRACE = Duration.ofSeconds(2);
  private static final Duration KILL_GRACE = Duration.ofSeconds(5);

  interface StaleClusterCleanup {
    void cleanup(Path runDirectory, Manifest manifest) throws Exception;
  }

  static final class Manifest {
    private final String instanceId;
    private final int ccmId;

    Manifest(String instanceId, int ccmId) {
      this.instanceId = instanceId;
      this.ccmId = ccmId;
    }

    String instanceId() {
      return instanceId;
    }

    int ccmId() {
      return ccmId;
    }
  }

  static final class ClusterHandle {
    private final CcmRunState owner;
    private final String instanceId;
    private final int ccmId;
    private final Path manifestPath;
    private final IdLease idLease;

    ClusterHandle(
        CcmRunState owner, String instanceId, int ccmId, Path manifestPath, IdLease idLease) {
      this.owner = owner;
      this.instanceId = instanceId;
      this.ccmId = ccmId;
      this.manifestPath = manifestPath;
      this.idLease = idLease;
    }

    int ccmId() {
      return ccmId;
    }
  }

  private static final class IdLease {
    final int id;
    final Path ownerPath;
    final FileChannel channel;
    final FileLock lock;
    boolean reservationReleased;
    boolean lockClosed;

    IdLease(int id, Path ownerPath, FileChannel channel, FileLock lock) {
      this.id = id;
      this.ownerPath = ownerPath;
      this.channel = channel;
      this.lock = lock;
    }

    void release(String expectedOwner) throws IOException {
      if (!reservationReleased) {
        String actualOwner = readReservationOwner(ownerPath);
        if (actualOwner != null && !expectedOwner.equals(actualOwner)) {
          throw new IOException("CCM ID reservation changed owner at " + ownerPath);
        }
        Files.deleteIfExists(ownerPath);
        deleteReservationStagingFiles(ownerPath.getParent(), id);
        reservationReleased = true;
      }
      if (!lockClosed) {
        IOException failure = null;
        try {
          if (lock.isValid()) {
            lock.release();
          }
        } catch (IOException exception) {
          failure = exception;
        }
        try {
          channel.close();
        } catch (IOException exception) {
          failure = combine(failure, exception);
        }
        if (failure != null) {
          throw failure;
        }
        lockClosed = true;
      }
    }
  }

  private static final class Owner {
    final long pid;
    final long startTicks;
    final String bootId;

    Owner(long pid, long startTicks, String bootId) {
      this.pid = pid;
      this.startTicks = startTicks;
      this.bootId = bootId;
    }
  }

  private static final class ProcessUids {
    final long real;
    final long effective;

    ProcessUids(long real, long effective) {
      this.real = real;
      this.effective = effective;
    }

    boolean sharesIdentityWith(ProcessUids other) {
      return real == other.real
          || real == other.effective
          || effective == other.real
          || effective == other.effective;
    }
  }

  private static final class MarkedProcess {
    final ProcessHandle process;
    final long startTicks;

    MarkedProcess(ProcessHandle process, long startTicks) {
      this.process = process;
      this.startTicks = startTicks;
    }

    void destroy(boolean forcibly) {
      try {
        if (process.isAlive() && readProcessStartTicks(process.pid()) == startTicks) {
          if (forcibly) {
            process.destroyForcibly();
          } else {
            process.destroy();
          }
        }
      } catch (IOException disappeared) {
        // The recorded process exited or changed identity before it could be signalled.
      }
    }
  }

  private final Path runsDirectory;
  private final Path idLocksDirectory;
  private final Path scanLockPath;
  private final Path runDirectory;
  private final String runName;
  private final Path manifestsDirectory;
  private final Map<Integer, ClusterHandle> clusters = new HashMap<>();
  private boolean incompleteOwnership;
  private boolean closed;

  private CcmRunState(
      Path runsDirectory, Path idLocksDirectory, Path scanLockPath, Path runDirectory) {
    this.runsDirectory = runsDirectory;
    this.idLocksDirectory = idLocksDirectory;
    this.scanLockPath = scanLockPath;
    this.runDirectory = runDirectory;
    this.runName = runDirectory.getFileName().toString();
    this.manifestsDirectory = runDirectory.resolve("owned");
  }

  static CcmRunState openDefault(StaleClusterCleanup cleanup) throws IOException {
    String configured = System.getenv("SCYLLA_CCM_ROOT");
    Path requested =
        configured == null || configured.trim().isEmpty()
            ? defaultRoot()
            : Path.of(configured).toAbsolutePath().normalize();
    return open(requested, cleanup);
  }

  static CcmRunState open(Path requestedRoot, StaleClusterCleanup cleanup) throws IOException {
    Path root = prepareRoot(requestedRoot);
    Path runs = prepareChildDirectory(root, "runs");
    Path idLocks = prepareChildDirectory(root, "ccm-id-locks");
    Path scanLock = root.resolve("scan.lock");
    return withFileLock(
        scanLock,
        () -> {
          recoverStaleRuns(runs, idLocks, cleanup);
          releaseReservationsForMissingRuns(runs, idLocks);
          Path run = createRunDirectory(runs);
          return new CcmRunState(runs, idLocks, scanLock, run);
        });
  }

  Path runDirectory() {
    return runDirectory;
  }

  synchronized ClusterHandle beginCluster(String instanceId, ClusterSpec spec) throws IOException {
    return beginCluster(instanceId, spec, true);
  }

  synchronized ClusterHandle beginCluster(
      String instanceId, ClusterSpec spec, boolean includeJmxPort) throws IOException {
    ensureOpen();
    validateInstanceId(instanceId);
    if (!clusters.isEmpty()) {
      throw new IllegalStateException("This CCM run already owns a physical cluster");
    }

    IdLease lease = reserveId(spec, includeJmxPort);
    Path manifest = manifestsDirectory.resolve(instanceId + ".properties");
    try {
      writeExclusive(
          manifest,
          "format="
              + FORMAT_VERSION
              + "\ninstance_id="
              + instanceId
              + "\nccm_id="
              + lease.id
              + "\n");
    } catch (IOException exception) {
      try {
        lease.release(runName);
      } catch (IOException cleanupFailure) {
        incompleteOwnership = true;
        exception.addSuppressed(cleanupFailure);
      }
      throw exception;
    }
    ClusterHandle handle = new ClusterHandle(this, instanceId, lease.id, manifest, lease);
    clusters.put(lease.id, handle);
    return handle;
  }

  synchronized void completeCluster(ClusterHandle handle) throws IOException {
    ensureHandle(handle);
    retireClusterConfig(runDirectory, handle.instanceId);
    Files.deleteIfExists(handle.manifestPath);
    handle.idLease.release(runName);
    clusters.remove(handle.ccmId);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    if (!clusters.isEmpty()) {
      throw new IOException("Cannot retire a CCM run while it still owns a cluster");
    }
    if (incompleteOwnership) {
      throw new IOException("Cannot retire a CCM run with incomplete ownership publication");
    }
    withFileLock(
        scanLockPath,
        () -> {
          if (Files.exists(runDirectory, LinkOption.NOFOLLOW_LINKS)) {
            validateRunDirectory(runDirectory, runsDirectory);
            deleteRecursively(runDirectory);
          } else if (!Files.notExists(runDirectory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Cannot determine CCM run state at " + runDirectory);
          }
          return null;
        });
    closed = true;
  }

  static boolean isAddressRangeAvailable(ClusterSpec spec, int id) {
    return isAddressRangeAvailable(spec, id, true);
  }

  static boolean isAddressRangeAvailable(ClusterSpec spec, int id, boolean includeJmxPort) {
    return isAddressRangeAvailable(
        id,
        spec.transports().contains(AlternatorTransport.HTTP),
        spec.transports().contains(AlternatorTransport.HTTPS),
        includeJmxPort);
  }

  private static boolean isAddressRangeAvailable(int id, boolean http, boolean https, boolean jmx) {
    if (id < 1 || id >= 100) {
      return false;
    }
    Set<Integer> ports = new LinkedHashSet<>();
    ports.add(CcmProvisioner.STORAGE_PORT);
    // The exact default relocatable package does not ship the JMX component, so pinned CCM never
    // opens its configured JMX port. Unknown package overrides are checked conservatively.
    if (jmx) {
      ports.add(CcmProvisioner.JMX_PORT);
    }
    ports.add(9042);
    ports.add(9180);
    ports.add(CcmProvisioner.API_PORT);
    ports.add(19042);
    if (http) {
      ports.add(CcmProvisioner.HTTP_PORT);
    }
    if (https) {
      ports.add(CcmProvisioner.HTTPS_PORT);
    }

    for (int node = 1; node <= TestClusterPool.MAXIMUM_NODE_COUNT; node++) {
      String address = "127.0." + id + "." + node;
      for (int port : ports) {
        try (ServerSocketChannel socket = ServerSocketChannel.open()) {
          socket.bind(new InetSocketAddress(address, port));
        } catch (BindException occupied) {
          return false;
        } catch (IOException unavailable) {
          return false;
        }
      }
    }
    return true;
  }

  private IdLease reserveId(ClusterSpec spec, boolean includeJmxPort) throws IOException {
    IOException lastFailure = null;
    for (int id = 1; id < 100; id++) {
      Path lockPath = idLockPath(idLocksDirectory, id);
      Path ownerPath = idOwnerPath(idLocksDirectory, id);
      FileChannel channel = null;
      FileLock lock = null;
      try {
        channel = openLockChannel(lockPath);
        try {
          lock = channel.tryLock();
        } catch (OverlappingFileLockException occupied) {
          channel.close();
          continue;
        }
        if (lock == null) {
          channel.close();
          continue;
        }
        if (Files.exists(ownerPath, LinkOption.NOFOLLOW_LINKS)
            || !isAddressRangeAvailable(spec, id, includeJmxPort)) {
          lock.release();
          channel.close();
          continue;
        }
        deleteReservationStagingFiles(idLocksDirectory, id);
        writeExclusive(ownerPath, runName + "\n");
        return new IdLease(id, ownerPath, channel, lock);
      } catch (IOException exception) {
        lastFailure = exception;
        closeLockBestEffort(lock, channel);
      }
    }
    throw new IOException("No CCM cluster IDs are available", lastFailure);
  }

  private void ensureHandle(ClusterHandle handle) {
    if (handle == null || handle.owner != this || clusters.get(handle.ccmId) != handle) {
      throw new IllegalArgumentException("The CCM cluster handle is not owned by this run");
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("The CCM run state is closed");
    }
  }

  private static void recoverStaleRuns(
      Path runsDirectory, Path idLocksDirectory, StaleClusterCleanup cleanup) throws IOException {
    List<Path> runs = new ArrayList<>();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(runsDirectory)) {
      for (Path entry : entries) {
        if (entry.getFileName().toString().matches(RUN_NAME_PATTERN)) {
          runs.add(entry);
        }
      }
    }
    runs.sort(Comparator.comparing(path -> path.getFileName().toString()));
    for (Path run : runs) {
      try {
        recoverStaleRun(run, runsDirectory, idLocksDirectory, cleanup);
      } catch (Exception exception) {
        if (exception instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        System.err.println("Preserving stale CCM run after cleanup failure: " + run);
        exception.printStackTrace(System.err);
      }
    }
  }

  private static void recoverStaleRun(
      Path run, Path runsDirectory, Path idLocksDirectory, StaleClusterCleanup cleanup)
      throws Exception {
    validateRunDirectory(run, runsDirectory);
    Path ownerPath = run.resolve("OWNER");
    if (Files.exists(ownerPath, LinkOption.NOFOLLOW_LINKS)) {
      Owner owner = readOwner(ownerPath);
      if (isActive(owner)) {
        return;
      }
    } else if (!Files.notExists(ownerPath, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Cannot determine CCM owner state at " + ownerPath);
    }

    terminateMarkedProcesses(run);
    cleanupOwnedStagingFiles(run);
    List<ManifestFile> manifests = readManifests(run);
    if (manifests.size() > 1) {
      throw new IOException("A CCM run contains more than one physical cluster manifest");
    }
    for (ManifestFile manifest : manifests) {
      ensureStaleReservation(
          idLocksDirectory, manifest.value.ccmId(), run.getFileName().toString());
      cleanup.cleanup(run, manifest.value);
      retireClusterConfig(run, manifest.value.instanceId());
      Files.delete(manifest.path);
    }
    assertNoUnownedClusterState(run);
    cleanupOwnedStagingFiles(run);
    String staleRunName = run.getFileName().toString();
    cleanupLegacyReservationStagingFiles(idLocksDirectory, staleRunName);
    List<Integer> reservations = reservationsOwnedBy(idLocksDirectory, staleRunName);
    deleteRecursively(run);
    for (int id : reservations) {
      releaseStaleReservation(idLocksDirectory, id, staleRunName);
    }
  }

  private static final class ManifestFile {
    final Path path;
    final Manifest value;

    ManifestFile(Path path, Manifest value) {
      this.path = path;
      this.value = value;
    }
  }

  private static List<ManifestFile> readManifests(Path run) throws IOException {
    Path owned = run.resolve("owned");
    if (Files.notExists(owned, LinkOption.NOFOLLOW_LINKS)) {
      return new ArrayList<>();
    }
    validateOwnedChildDirectory(run, owned, "manifest directory");
    List<ManifestFile> manifests = new ArrayList<>();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(owned)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (!name.endsWith(".properties")) {
          throw new IOException("Unexpected CCM ownership metadata at " + entry);
        }
        Map<String, String> values =
            readKeyValueFile(entry, Set.of("format", "instance_id", "ccm_id"));
        requireFormat(values, entry);
        String instanceId = values.get("instance_id");
        validateInstanceId(instanceId);
        if (!name.equals(instanceId + ".properties")) {
          throw new IOException("CCM manifest name does not match its instance at " + entry);
        }
        int ccmId = parseCcmId(values.get("ccm_id"), entry);
        manifests.add(new ManifestFile(entry, new Manifest(instanceId, ccmId)));
      }
    }
    manifests.sort(Comparator.comparing(file -> file.path.getFileName().toString()));
    return manifests;
  }

  private static void cleanupOwnedStagingFiles(Path run) throws IOException {
    Path owned = run.resolve("owned");
    if (Files.notExists(owned, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedChildDirectory(run, owned, "manifest directory");
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(owned)) {
      for (Path entry : entries) {
        if (isMetadataStagingName(entry.getFileName().toString())) {
          validateStagingFile(entry);
          Files.delete(entry);
        }
      }
    }
  }

  private static void retireClusterConfig(Path run, String instanceId) throws IOException {
    Path clusters = run.resolve("clusters");
    if (Files.notExists(clusters, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedChildDirectory(run, clusters, "clusters directory");
    Path config = clusters.resolve(instanceId);
    if (Files.notExists(config, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedChildDirectory(clusters, config, "cluster config directory");
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(config)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (name.equals("tls")) {
          validateOwnedChildDirectory(config, entry, "TLS directory");
          validateOwnedTree(entry);
        } else if (!Files.isSymbolicLink(entry)
            && Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)
            && (name.equals("ccm-commands.log")
                || (name.startsWith("ccm-command-") && name.endsWith(".log")))) {
          requireCurrentUserOwner(entry, "CCM command log");
        } else {
          throw new IOException("Unexpected state remains in retired CCM config at " + entry);
        }
      }
    }
    deleteRecursively(config);
  }

  private static void validateOwnedTree(Path root) throws IOException {
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes)
              throws IOException {
            if (Files.isSymbolicLink(directory)) {
              throw new IOException("Unsafe symbolic link in CCM-owned tree at " + directory);
            }
            requireCurrentUserOwner(directory, "CCM-owned directory");
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
              throws IOException {
            if (!attributes.isRegularFile() || Files.isSymbolicLink(file)) {
              throw new IOException("Unsafe entry in CCM-owned tree at " + file);
            }
            requireCurrentUserOwner(file, "CCM-owned file");
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static void assertNoUnownedClusterState(Path run) throws IOException {
    Path clusters = run.resolve("clusters");
    if (Files.notExists(clusters, LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    validateOwnedChildDirectory(run, clusters, "clusters directory");
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(clusters)) {
      if (entries.iterator().hasNext()) {
        throw new IOException("A stale CCM run contains cluster state without a valid manifest");
      }
    }
  }

  private static Owner readOwner(Path ownerPath) throws IOException {
    Map<String, String> values =
        readKeyValueFile(ownerPath, Set.of("format", "pid", "start_ticks", "boot_id"));
    requireFormat(values, ownerPath);
    try {
      long pid = Long.parseLong(values.get("pid"));
      long startTicks = Long.parseLong(values.get("start_ticks"));
      String bootId = values.get("boot_id");
      if (pid < 1 || startTicks < 1 || !bootId.matches("[0-9a-fA-F-]{36}")) {
        throw new NumberFormatException();
      }
      return new Owner(pid, startTicks, bootId);
    } catch (NumberFormatException | NullPointerException exception) {
      throw new IOException("Malformed CCM owner metadata at " + ownerPath, exception);
    }
  }

  private static boolean isActive(Owner owner) throws IOException {
    if (!currentBootId().equals(owner.bootId)) {
      return false;
    }
    Path process = Path.of("/proc", Long.toString(owner.pid));
    if (Files.notExists(process, LinkOption.NOFOLLOW_LINKS)) {
      return false;
    }
    try {
      return readProcessStartTicks(owner.pid) == owner.startTicks;
    } catch (NoSuchFileException disappeared) {
      return false;
    }
  }

  private static void terminateMarkedProcesses(Path run) throws IOException, InterruptedException {
    List<MarkedProcess> processes = markedProcesses(run);
    for (MarkedProcess process : processes) {
      process.destroy(false);
    }
    if (!waitForMarkedProcesses(run, TERM_GRACE)) {
      processes = markedProcesses(run);
      for (MarkedProcess process : processes) {
        process.destroy(true);
      }
      if (!waitForMarkedProcesses(run, KILL_GRACE)) {
        throw new IOException("Processes carrying the stale CCM run marker did not terminate");
      }
    }
  }

  private static boolean waitForMarkedProcesses(Path run, Duration timeout)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() - deadline < 0) {
      if (markedProcesses(run).isEmpty()) {
        return true;
      }
      Thread.sleep(50);
    }
    return markedProcesses(run).isEmpty();
  }

  private static List<MarkedProcess> markedProcesses(Path run) throws IOException {
    List<MarkedProcess> result = new ArrayList<>();
    ProcessUids currentUids = readProcessUids(Path.of("/proc/self/status"));
    long currentPid = ProcessHandle.current().pid();
    String runPath = run.toAbsolutePath().normalize().toString();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(Path.of("/proc"))) {
      for (Path processDirectory : entries) {
        String name = processDirectory.getFileName().toString();
        if (!name.matches("[0-9]+")) {
          continue;
        }
        long pid;
        try {
          pid = Long.parseLong(name);
        } catch (NumberFormatException ignored) {
          continue;
        }
        try {
          ProcessUids candidateUids = readProcessUids(processDirectory.resolve("status"));
          if (!currentUids.sharesIdentityWith(candidateUids)) {
            continue;
          }
          long startTicks = readProcessStartTicks(pid);
          boolean commandReferencesRun = false;
          try {
            commandReferencesRun =
                containsRunPathArgument(
                    Files.readAllBytes(processDirectory.resolve("cmdline")), runPath);
          } catch (AccessDeniedException inaccessible) {
            // The exact inherited environment marker is the ownership proof.
          }
          boolean ownedByEnvironment;
          try {
            byte[] environment = Files.readAllBytes(processDirectory.resolve("environ"));
            ownedByEnvironment =
                containsEnvironmentEntry(environment, "SCYLLA_CCM_RUN_DIR=" + runPath);
          } catch (AccessDeniedException inaccessible) {
            if (commandReferencesRun) {
              throw new IOException(
                  "Cannot prove ownership of process "
                      + pid
                      + " whose command references stale CCM run "
                      + run,
                  inaccessible);
            }
            // An opaque same-UID process with no visible reference to this run is unrelated.
            continue;
          }
          if (!ownedByEnvironment || readProcessStartTicks(pid) != startTicks) {
            continue;
          }
          if (pid == currentPid) {
            throw new IOException("The current JVM carries a supposedly stale CCM run marker");
          }
          Optional<ProcessHandle> process = ProcessHandle.of(pid);
          if (process.isPresent() && process.get().isAlive()) {
            result.add(new MarkedProcess(process.get(), startTicks));
          }
        } catch (NoSuchFileException disappeared) {
          // Processes can disappear at any point during a /proc scan.
        }
      }
    }
    return result;
  }

  private static boolean containsRunPathArgument(byte[] commandLine, String runPath) {
    int start = 0;
    for (int index = 0; index <= commandLine.length; index++) {
      if (index == commandLine.length || commandLine[index] == 0) {
        String argument = new String(commandLine, start, index - start, StandardCharsets.UTF_8);
        if (argument.equals(runPath) || argument.startsWith(runPath + "/")) {
          return true;
        }
        start = index + 1;
      }
    }
    return false;
  }

  private static ProcessUids readProcessUids(Path status) throws IOException {
    String uidLine = null;
    for (String line : Files.readString(status, StandardCharsets.US_ASCII).split("\\R")) {
      if (line.startsWith("Uid:")) {
        if (uidLine != null) {
          throw new IOException("Duplicate Uid field in " + status);
        }
        uidLine = line.substring("Uid:".length()).trim();
      }
    }
    if (uidLine == null) {
      throw new IOException("Missing Uid field in " + status);
    }
    String[] values = uidLine.split("\\s+", -1);
    if (values.length != 4) {
      throw new IOException("Malformed Uid field in " + status);
    }
    long[] parsed = new long[values.length];
    for (int index = 0; index < values.length; index++) {
      if (!values[index].matches("[0-9]+")) {
        throw new IOException("Malformed Uid field in " + status);
      }
      try {
        parsed[index] = Long.parseLong(values[index]);
      } catch (NumberFormatException exception) {
        throw new IOException("Malformed Uid field in " + status, exception);
      }
    }
    return new ProcessUids(parsed[0], parsed[1]);
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

  private static List<Integer> reservationsOwnedBy(Path idLocksDirectory, String runName)
      throws IOException {
    List<Integer> reservations = new ArrayList<>();
    for (int id = 1; id < 100; id++) {
      Path owner = idOwnerPath(idLocksDirectory, id);
      String actual;
      try {
        actual = readReservationOwner(owner);
      } catch (IOException malformed) {
        System.err.println("Preserving malformed CCM ID reservation at " + owner);
        continue;
      }
      if (runName.equals(actual)) {
        reservations.add(id);
      }
    }
    return reservations;
  }

  private static void releaseReservationsForMissingRuns(Path runsDirectory, Path idLocksDirectory)
      throws IOException {
    for (int id = 1; id < 100; id++) {
      Path ownerPath = idOwnerPath(idLocksDirectory, id);
      String owner;
      try {
        owner = readReservationOwner(ownerPath);
      } catch (IOException malformed) {
        System.err.println("Preserving malformed CCM ID reservation at " + ownerPath);
        continue;
      }
      if (owner == null) {
        continue;
      }
      Path run = runsDirectory.resolve(owner);
      if (Files.exists(run, LinkOption.NOFOLLOW_LINKS)) {
        continue;
      }
      if (!Files.notExists(run, LinkOption.NOFOLLOW_LINKS)
          || !isAddressRangeAvailable(id, true, true, true)) {
        System.err.println("Preserving CCM ID reservation without a retired run at " + ownerPath);
        continue;
      }
      try {
        releaseStaleReservation(idLocksDirectory, id, owner);
      } catch (IOException failure) {
        System.err.println("Preserving CCM ID reservation after release failure: " + ownerPath);
        failure.printStackTrace(System.err);
      }
    }
  }

  private static void releaseStaleReservation(Path idLocksDirectory, int id, String expectedOwner)
      throws IOException {
    Path ownerPath = idOwnerPath(idLocksDirectory, id);
    String actualOwner = readReservationOwner(ownerPath);
    if (actualOwner == null) {
      return;
    }
    if (!expectedOwner.equals(actualOwner)) {
      throw new IOException("CCM ID " + id + " is reserved by a different run");
    }
    try (FileChannel channel = openLockChannel(idLockPath(idLocksDirectory, id))) {
      FileLock lock;
      try {
        lock = channel.tryLock();
      } catch (OverlappingFileLockException occupied) {
        throw new IOException("CCM ID " + id + " is still locked", occupied);
      }
      if (lock == null) {
        throw new IOException("CCM ID " + id + " is still locked");
      }
      try (FileLock ignored = lock) {
        deleteReservationStagingFiles(idLocksDirectory, id);
        actualOwner = readReservationOwner(ownerPath);
        if (actualOwner != null && !expectedOwner.equals(actualOwner)) {
          throw new IOException("CCM ID " + id + " changed owner during stale cleanup");
        }
        Files.deleteIfExists(ownerPath);
      }
    }
  }

  private static void ensureStaleReservation(Path idLocksDirectory, int id, String expectedOwner)
      throws IOException {
    Path ownerPath = idOwnerPath(idLocksDirectory, id);
    String actualOwner = readReservationOwner(ownerPath);
    if (expectedOwner.equals(actualOwner)) {
      return;
    }
    if (actualOwner != null) {
      throw new IOException("CCM ID " + id + " is reserved by a different run");
    }
    try (FileChannel channel = openLockChannel(idLockPath(idLocksDirectory, id))) {
      FileLock lock;
      try {
        lock = channel.tryLock();
      } catch (OverlappingFileLockException occupied) {
        throw new IOException("CCM ID " + id + " is still locked", occupied);
      }
      if (lock == null) {
        throw new IOException("CCM ID " + id + " is still locked");
      }
      try (FileLock ignored = lock) {
        actualOwner = readReservationOwner(ownerPath);
        if (actualOwner == null) {
          writeExclusive(ownerPath, expectedOwner + "\n");
        } else if (!expectedOwner.equals(actualOwner)) {
          throw new IOException("CCM ID " + id + " changed owner during stale cleanup");
        }
      }
    }
  }

  private static String readReservationOwner(Path ownerPath) throws IOException {
    if (Files.notExists(ownerPath, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    if (!Files.isRegularFile(ownerPath, LinkOption.NOFOLLOW_LINKS)
        || Files.isSymbolicLink(ownerPath)
        || Files.size(ownerPath) > MAXIMUM_METADATA_BYTES) {
      throw new IOException("Unsafe CCM ID reservation at " + ownerPath);
    }
    String value = Files.readString(ownerPath, StandardCharsets.US_ASCII);
    if (!value.matches(RUN_NAME_PATTERN + "\\n")) {
      throw new IOException("Malformed CCM ID reservation at " + ownerPath);
    }
    return value.trim();
  }

  private static void cleanupLegacyReservationStagingFiles(
      Path idLocksDirectory, String staleRunName) throws IOException {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(idLocksDirectory)) {
      for (Path entry : entries) {
        if (!entry.getFileName().toString().matches(LEGACY_STAGING_PATTERN)) {
          continue;
        }
        try {
          validateStagingFile(entry);
          if ((staleRunName + "\n").equals(Files.readString(entry, StandardCharsets.US_ASCII))) {
            Files.delete(entry);
          }
        } catch (IOException unsafeOrUnreadable) {
          // A legacy temporary name does not encode an ID or run. Preserve an unsafe entry, but do
          // not let unrelated malformed state prevent retirement of a valid stale run.
        }
      }
    }
  }

  private static void deleteReservationStagingFiles(Path idLocksDirectory, int id)
      throws IOException {
    String prefix = ".owner-" + id + ".owner-";
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(idLocksDirectory)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (name.startsWith(prefix)
            && name.endsWith(".tmp")
            && name.substring(prefix.length(), name.length() - 4).matches("[0-9]+")) {
          validateStagingFile(entry);
          Files.delete(entry);
        }
      }
    }
  }

  private static boolean isMetadataStagingName(String name) {
    return name.matches(LEGACY_STAGING_PATTERN)
        || name.matches("\\.owner-[A-Za-z0-9][A-Za-z0-9._-]{0,200}-[0-9]+\\.tmp");
  }

  private static void validateStagingFile(Path path) throws IOException {
    if (Files.isSymbolicLink(path)
        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
        || Files.size(path) > MAXIMUM_METADATA_BYTES) {
      throw new IOException("Unsafe CCM metadata staging file at " + path);
    }
    requireCurrentUserOwner(path, "CCM metadata staging file");
  }

  private static Path createRunDirectory(Path runsDirectory) throws IOException {
    for (int attempt = 0; attempt < 10; attempt++) {
      String name = "ccm-runtime." + UUID.randomUUID().toString().replace("-", "");
      Path run = runsDirectory.resolve(name);
      try {
        Files.createDirectory(run);
      } catch (FileAlreadyExistsException collision) {
        continue;
      }
      try {
        setOwnerOnlyDirectoryPermissions(run);
        Files.createDirectory(run.resolve("owned"));
        setOwnerOnlyDirectoryPermissions(run.resolve("owned"));
        writeExclusive(
            run.resolve("OWNER"),
            "format="
                + FORMAT_VERSION
                + "\npid="
                + ProcessHandle.current().pid()
                + "\nstart_ticks="
                + readProcessStartTicks(ProcessHandle.current().pid())
                + "\nboot_id="
                + currentBootId()
                + "\n");
        return run.toRealPath();
      } catch (IOException | RuntimeException exception) {
        try {
          deleteRecursively(run);
        } catch (IOException cleanupFailure) {
          exception.addSuppressed(cleanupFailure);
        }
        throw exception;
      }
    }
    throw new IOException("Unable to allocate a unique CCM run directory");
  }

  private static Path prepareRoot(Path requested) throws IOException {
    Path normalized = requested.toAbsolutePath().normalize();
    rejectUnsafeRoot(normalized);
    rejectSymbolicLinkComponents(normalized);
    try {
      Files.createDirectories(normalized);
    } catch (FileAlreadyExistsException ignored) {
      // Validation below reports a useful error for an unsafe existing path.
    }
    if (Files.isSymbolicLink(normalized)
        || !Files.isDirectory(normalized, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("CCM root is not a safe directory: " + normalized);
    }
    Path real = normalized.toRealPath();
    if (!real.equals(normalized)) {
      throw new IOException("CCM root must not traverse symbolic links: " + normalized);
    }
    requireCurrentUserOwner(real, "CCM root");
    setOwnerOnlyDirectoryPermissions(real);
    return real;
  }

  private static void rejectUnsafeRoot(Path root) throws IOException {
    Path filesystemRoot = root.getRoot();
    Path temporaryRoot =
        Path.of(System.getProperty("java.io.tmpdir", "/tmp")).toAbsolutePath().normalize();
    Path home = Path.of(System.getProperty("user.home", "/")).toAbsolutePath().normalize();
    Path project =
        Path.of(System.getProperty("basedir", System.getProperty("user.dir")))
            .toAbsolutePath()
            .normalize();
    Path target = project.resolve("target");
    if (filesystemRoot == null
        || root.equals(filesystemRoot)
        || root.getParent() == null
        || root.getParent().equals(filesystemRoot)
        || root.equals(temporaryRoot)
        || root.equals(Path.of("/var/tmp"))
        || root.equals(Path.of("/dev/shm"))
        || root.equals(home)
        || project.startsWith(root)
        || root.startsWith(target)) {
      throw new IOException("Refusing unsafe CCM root: " + root);
    }
  }

  private static void rejectSymbolicLinkComponents(Path path) throws IOException {
    for (Path current = path; current != null; current = current.getParent()) {
      if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
        if (Files.isSymbolicLink(current)) {
          throw new IOException("CCM root must not traverse symbolic links: " + path);
        }
      } else if (!Files.notExists(current, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("Cannot determine CCM root path state at " + current);
      }
    }
  }

  private static Path prepareChildDirectory(Path root, String name) throws IOException {
    Path child = root.resolve(name);
    try {
      Files.createDirectory(child);
    } catch (FileAlreadyExistsException ignored) {
      // Validate an existing entry without following it.
    }
    validateOwnedChildDirectory(root, child, name);
    setOwnerOnlyDirectoryPermissions(child);
    return child.toRealPath();
  }

  private static void validateOwnedChildDirectory(Path parent, Path child, String description)
      throws IOException {
    if (!parent.equals(child.getParent())
        || Files.isSymbolicLink(child)
        || !Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)
        || !child.equals(child.toRealPath())) {
      throw new IOException("Unsafe CCM " + description + " at " + child);
    }
    requireCurrentUserOwner(child, "CCM " + description);
  }

  private static void validateRunDirectory(Path run, Path runsDirectory) throws IOException {
    if (!run.getFileName().toString().matches(RUN_NAME_PATTERN)) {
      throw new IOException("Unsafe CCM run name at " + run);
    }
    validateOwnedChildDirectory(runsDirectory, run, "run directory");
  }

  private static void requireCurrentUserOwner(Path path, String description) throws IOException {
    UserPrincipal current = Files.getOwner(Path.of("/proc/self/status"), LinkOption.NOFOLLOW_LINKS);
    if (!current.equals(Files.getOwner(path, LinkOption.NOFOLLOW_LINKS))) {
      throw new IOException(description + " is not owned by the current user: " + path);
    }
  }

  private static void setOwnerOnlyDirectoryPermissions(Path directory) throws IOException {
    try {
      Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
    } catch (UnsupportedOperationException ignored) {
      // The harness is Linux-only; ownership and no-symlink checks remain mandatory.
    }
  }

  private static Path defaultRoot() throws IOException {
    Object uid =
        Files.getAttribute(Path.of("/proc/self/status"), "unix:uid", LinkOption.NOFOLLOW_LINKS);
    return Path.of("/tmp", "alternator-client-java-ccm-" + uid);
  }

  private static String currentBootId() throws IOException {
    return Files.readString(Path.of("/proc/sys/kernel/random/boot_id"), StandardCharsets.US_ASCII)
        .trim();
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

  private static Map<String, String> readKeyValueFile(Path path, Set<String> expectedKeys)
      throws IOException {
    if (Files.isSymbolicLink(path)
        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
        || Files.size(path) > MAXIMUM_METADATA_BYTES) {
      throw new IOException("Unsafe CCM metadata at " + path);
    }
    String text = Files.readString(path, StandardCharsets.US_ASCII);
    if (!text.endsWith("\n")) {
      throw new IOException("Malformed CCM metadata at " + path);
    }
    Map<String, String> values = new HashMap<>();
    for (String line : text.substring(0, text.length() - 1).split("\\n", -1)) {
      int separator = line.indexOf('=');
      if (separator < 1 || separator == line.length() - 1) {
        throw new IOException("Malformed CCM metadata at " + path);
      }
      String key = line.substring(0, separator);
      String value = line.substring(separator + 1);
      if (!expectedKeys.contains(key) || values.put(key, value) != null) {
        throw new IOException("Malformed CCM metadata at " + path);
      }
    }
    if (!values.keySet().equals(expectedKeys)) {
      throw new IOException("Incomplete CCM metadata at " + path);
    }
    return values;
  }

  private static void requireFormat(Map<String, String> values, Path path) throws IOException {
    if (!Integer.toString(FORMAT_VERSION).equals(values.get("format"))) {
      throw new IOException("Unsupported CCM metadata format at " + path);
    }
  }

  private static int parseCcmId(String value, Path path) throws IOException {
    try {
      int id = Integer.parseInt(value);
      if (id < 1 || id >= 100 || !Integer.toString(id).equals(value)) {
        throw new NumberFormatException();
      }
      return id;
    } catch (NumberFormatException | NullPointerException exception) {
      throw new IOException("Malformed CCM ID at " + path, exception);
    }
  }

  private static void validateInstanceId(String instanceId) {
    if (instanceId == null || !instanceId.matches("[A-Za-z0-9][A-Za-z0-9-]{0,127}")) {
      throw new IllegalArgumentException("Unsafe CCM cluster instance ID: " + instanceId);
    }
  }

  private static void writeExclusive(Path target, String contents) throws IOException {
    Path parent = target.getParent();
    Path temporary = Files.createTempFile(parent, ".owner-" + target.getFileName() + "-", ".tmp");
    boolean published = false;
    IOException failure = null;
    try {
      try {
        Files.setPosixFilePermissions(temporary, PosixFilePermissions.fromString("rw-------"));
      } catch (UnsupportedOperationException ignored) {
        // Ownership validation still applies on non-POSIX providers.
      }
      Files.writeString(
          temporary,
          contents,
          StandardCharsets.US_ASCII,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE);
      Files.createLink(target, temporary);
      published = true;
    } catch (IOException exception) {
      failure = exception;
    }
    try {
      Files.deleteIfExists(temporary);
    } catch (IOException cleanupFailure) {
      if (published) {
        System.err.println("Leaving recoverable CCM metadata staging file at " + temporary);
      } else if (failure == null) {
        failure = cleanupFailure;
      } else {
        failure.addSuppressed(cleanupFailure);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static FileChannel openLockChannel(Path path) throws IOException {
    try {
      return FileChannel.open(
          path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    } catch (FileAlreadyExistsException exists) {
      if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("Unsafe CCM lock file at " + path, exists);
      }
      return FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    }
  }

  private static <T> T withFileLock(Path path, LockedOperation<T> operation) throws IOException {
    synchronized (SCAN_MUTEX) {
      try (FileChannel channel = openLockChannel(path);
          FileLock ignored = channel.lock()) {
        return operation.run();
      }
    }
  }

  private interface LockedOperation<T> {
    T run() throws IOException;
  }

  static Path idLockPath(Path idLocksDirectory, int id) {
    return idLocksDirectory.resolve(id + ".lock");
  }

  static Path idOwnerPath(Path idLocksDirectory, int id) {
    return idLocksDirectory.resolve(id + ".owner");
  }

  private static void closeLockBestEffort(FileLock lock, FileChannel channel) {
    try {
      if (lock != null && lock.isValid()) {
        lock.release();
      }
    } catch (IOException ignored) {
      // The reservation was not published, so there is no durable state to recover.
    }
    try {
      if (channel != null) {
        channel.close();
      }
    } catch (IOException ignored) {
      // The reservation was not published, so there is no durable state to recover.
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
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

  private static IOException combine(IOException first, IOException second) {
    if (first == null) {
      return second;
    }
    first.addSuppressed(second);
    return first;
  }
}
