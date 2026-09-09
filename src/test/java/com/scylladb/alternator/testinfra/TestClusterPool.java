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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/** Memory-aware pool for reusable and private CCM clusters. */
final class TestClusterPool implements AutoCloseable {
  interface ResourceCleanup {
    void cleanup(TestResourceScope resources) throws Exception;
  }

  static final class PooledCluster {
    final String reuseKey;
    final ClusterSpec spec;
    final String instanceId;
    final int ccmId;
    final CompletableFuture<PhysicalTestCluster> ready = new CompletableFuture<>();
    CompletableFuture<Boolean> reuseValidation;
    int referenceCount;
    int pendingReleases;
    boolean poisoned;
    boolean reservationReleased;
    long lastReleasedOrder;

    PooledCluster(String reuseKey, ClusterSpec spec, String instanceId, int ccmId) {
      this.reuseKey = reuseKey;
      this.spec = spec;
      this.instanceId = instanceId;
      this.ccmId = ccmId;
    }
  }

  /** Capacity owned by a private or partially provisioned physical cluster. */
  private static final class ClusterReservation {
    final int memoryMiBPerNode;
    int nodes;

    ClusterReservation(int nodes, int memoryMiBPerNode) {
      this.nodes = nodes;
      this.memoryMiBPerNode = memoryMiBPerNode;
    }
  }

  private static final class IdLock implements AutoCloseable {
    final FileChannel channel;
    final FileLock lock;
    final Path reservationFile;
    boolean reservationRemoved;
    boolean closed;

    IdLock(FileChannel channel, FileLock lock, Path reservationFile) {
      this.channel = channel;
      this.lock = lock;
      this.reservationFile = reservationFile;
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      IOException failure = null;
      if (!reservationRemoved && reservationFile != null) {
        try {
          Files.delete(reservationFile);
          reservationRemoved = true;
        } catch (IOException exception) {
          failure = exception;
        }
      }
      try {
        if (lock.isValid()) {
          lock.release();
        }
      } catch (IOException exception) {
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
      try {
        channel.close();
      } catch (IOException exception) {
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
      if (failure != null) {
        throw failure;
      }
      closed = true;
    }
  }

  private enum RemovalOwner {
    POOLED,
    PRIVATE,
    FAILED_PROVISIONING
  }

  private static final class RemovalCandidate {
    final RemovalOwner owner;
    final PooledCluster pooledCluster;
    final PhysicalTestCluster cluster;

    RemovalCandidate(RemovalOwner owner, PooledCluster pooledCluster, PhysicalTestCluster cluster) {
      this.owner = owner;
      this.pooledCluster = pooledCluster;
      this.cluster = cluster;
    }
  }

  private static final class InterruptTracker {
    private InterruptedException interruption;

    void captureCurrent() {
      if (Thread.interrupted()) {
        capture(new InterruptedException("CCM cluster pool cleanup was interrupted"));
      }
    }

    void capture(InterruptedException exception) {
      Thread.interrupted();
      if (interruption == null) {
        interruption = exception;
      } else if (interruption != exception) {
        interruption.addSuppressed(exception);
      }
    }

    InterruptedException interruption() {
      return interruption;
    }
  }

  @FunctionalInterface
  private interface CleanupOperation {
    void run() throws Exception;
  }

  private final CcmProvisioner provisioner;
  private final ClusterCapacity capacity;
  private final int maximumNodes;
  private final ResourceCleanup cleanupResources;
  private final Path ccmIdLockRoot;
  private final boolean publishDurableIdReservations;
  private final Map<String, PooledCluster> reusableClusters = new HashMap<>();
  private final Set<PooledCluster> ownedPooledClusters = new HashSet<>();
  private final Set<PooledCluster> failedPooledRemovals = new HashSet<>();
  private final Set<PhysicalTestCluster> failedPrivateRemovals = new HashSet<>();
  private final Set<PhysicalTestCluster> failedProvisioningRemovals = new HashSet<>();
  private final Set<PhysicalTestCluster> privateClusters = new HashSet<>();
  private final Map<PhysicalTestCluster, ClusterReservation> privateReservations = new HashMap<>();
  private final Set<CompletableFuture<Void>> privateProvisioning = new HashSet<>();
  private final Map<PhysicalTestCluster, CompletableFuture<Void>> physicalRemovals =
      new HashMap<>();
  private final Map<Integer, IdLock> ccmIdLocks = new HashMap<>();
  private final AtomicLong leaseCounter = new AtomicLong();
  private int instanceCounter;
  private long releaseCounter;
  private int usedNodes;
  private long usedMemoryMiB;
  private boolean closed;

  TestClusterPool(
      CcmProvisioner provisioner,
      ClusterCapacity capacity,
      int maximumNodes,
      ResourceCleanup cleanupResources) {
    this(
        provisioner,
        capacity,
        maximumNodes,
        cleanupResources,
        provisioner.runDirectory().resolve("ccm-id-locks"),
        false);
  }

  TestClusterPool(
      CcmProvisioner provisioner,
      ClusterCapacity capacity,
      int maximumNodes,
      ResourceCleanup cleanupResources,
      Path ccmIdLockRoot) {
    this(provisioner, capacity, maximumNodes, cleanupResources, ccmIdLockRoot, false);
  }

  TestClusterPool(
      CcmProvisioner provisioner,
      ClusterCapacity capacity,
      int maximumNodes,
      ResourceCleanup cleanupResources,
      Path ccmIdLockRoot,
      boolean publishDurableIdReservations) {
    this.provisioner = provisioner;
    this.capacity = capacity;
    this.maximumNodes = maximumNodes;
    this.cleanupResources =
        cleanupResources == null ? TestResourceScope::cleanup : cleanupResources;
    this.ccmIdLockRoot = ccmIdLockRoot;
    this.publishDurableIdReservations = publishDurableIdReservations;
    System.out.println(
        "CCM capacity: "
            + capacity.availableMemoryMiB()
            + " MiB available, "
            + capacity.reservedMemoryMiB()
            + " MiB reserved, "
            + capacity.usableMemoryMiB()
            + " MiB usable, maximum "
            + maximumNodes
            + " nodes.");
  }

  static TestClusterPool createDefault() throws IOException {
    String configuredRunDirectory = System.getenv("SCYLLA_CCM_RUN_DIR");
    Path runDirectory =
        configuredRunDirectory == null || configuredRunDirectory.trim().isEmpty()
            ? Files.createTempDirectory(
                "alternator-client-java-ccm-" + ProcessHandle.current().pid() + "-")
            : Path.of(configuredRunDirectory);
    Path ccmIdLockRoot = defaultCcmIdLockRoot();
    ClusterCapacity capacity =
        ClusterCapacity.fromAvailableMemory(AvailableMemoryDetector.getAvailableMemoryMiB());
    int configuredMaximum =
        parsePositiveInteger("SCYLLA_CCM_MAX_NODES", ClusterCapacity.MAXIMUM_NODE_COUNT);
    return new TestClusterPool(
        new CcmProvisioner(runDirectory),
        capacity,
        Math.min(configuredMaximum, ClusterCapacity.MAXIMUM_NODE_COUNT),
        null,
        ccmIdLockRoot,
        configuredRunDirectory != null && !configuredRunDirectory.trim().isEmpty());
  }

  static Path defaultCcmIdLockRoot() throws IOException {
    String configured = System.getenv("SCYLLA_CCM_ID_LOCK_ROOT");
    if (configured != null && !configured.trim().isEmpty()) {
      return Path.of(configured).toAbsolutePath().normalize();
    }
    Object uid =
        Files.getAttribute(Path.of("/proc/self/status"), "unix:uid", LinkOption.NOFOLLOW_LINKS);
    return Path.of("/tmp").resolve("alternator-client-java-ccm-" + uid).resolve("ccm-id-locks");
  }

  ReusableClusterLease acquireReusable(ClusterSpec spec) throws Exception {
    spec.validate();
    validateDemand(spec);
    while (true) {
      PooledCluster selected;
      PooledCluster eviction = null;
      RemovalCandidate failedRemoval = null;
      boolean provision = false;
      CompletableFuture<Boolean> reuseValidation = null;
      synchronized (this) {
        throwIfClosed();
        selected = reusableClusters.get(spec.reuseKey());
        if (selected != null && (selected.poisoned || selected.pendingReleases > 0)) {
          if (selected.referenceCount == 0) {
            reusableClusters.remove(selected.reuseKey);
            eviction = selected;
          } else {
            wait();
          }
          selected = null;
        } else if (selected != null) {
          if (selected.referenceCount == 0 && selected.ready.isDone()) {
            selected.reuseValidation = selected.ready.thenApplyAsync(provisioner::isHealthy);
          }
          selected.referenceCount++;
          reuseValidation = selected.reuseValidation;
        } else if (fits(spec.topology().nodeCount(), memoryRequired(spec))) {
          int ccmId = allocateCcmId();
          selected = new PooledCluster(spec.reuseKey(), spec, createInstanceId(), ccmId);
          selected.referenceCount = 1;
          reserve(spec.topology().nodeCount(), memoryRequired(spec));
          reusableClusters.put(selected.reuseKey, selected);
          ownedPooledClusters.add(selected);
          provision = true;
        } else {
          failedRemoval = findFailedRemoval();
          if (failedRemoval == null) {
            eviction = findIdleEviction();
          }
          if (eviction != null) {
            reusableClusters.remove(eviction.reuseKey);
          } else if (failedRemoval == null) {
            wait();
          }
          selected = null;
        }
      }

      if (failedRemoval != null) {
        retryFailedRemoval(failedRemoval);
        continue;
      }
      if (eviction != null) {
        destroyPooled(eviction);
        continue;
      }
      if (selected == null) {
        continue;
      }
      if (provision) {
        startPooledProvisioning(selected);
      }
      PhysicalTestCluster cluster;
      try {
        cluster = await(selected.ready);
      } catch (Exception exception) {
        abandonReusable(selected, false);
        throw exception;
      }
      if (reuseValidation != null) {
        boolean healthy;
        try {
          healthy = awaitBoolean(reuseValidation);
        } catch (Exception exception) {
          abandonReusable(selected, false);
          throw exception;
        }
        if (!healthy) {
          abandonReusable(selected, true);
          continue;
        }
      }
      TestResourceScope resources;
      synchronized (this) {
        if (closed) {
          selected.referenceCount--;
          if (selected.referenceCount == 0) {
            selected.reuseValidation = null;
          }
          notifyAll();
          throw new IllegalStateException(
              "The CCM cluster pool closed during reusable provisioning");
        }
        resources = createResourceScope(cluster);
      }
      return new ReusableClusterLease(this, selected, cluster, resources);
    }
  }

  PrivateClusterLease provisionPrivate(ClusterSpec spec) throws Exception {
    spec.validate();
    validateDemand(spec);
    reservePrivateCapacity(spec);
    int ccmId;
    String instanceId;
    CompletableFuture<Void> provisioningFinished = new CompletableFuture<>();
    synchronized (this) {
      try {
        throwIfClosed();
        ccmId = allocateCcmId();
        instanceId = createInstanceId();
        privateProvisioning.add(provisioningFinished);
      } catch (Exception exception) {
        releaseCapacity(spec.topology().nodeCount(), memoryRequired(spec));
        throw exception;
      }
    }
    boolean provisioned = false;
    try {
      PhysicalTestCluster cluster = provisioner.provision(spec, instanceId, ccmId);
      provisioned = true;
      boolean closeWon;
      synchronized (this) {
        privateClusters.add(cluster);
        privateReservations.put(
            cluster,
            new ClusterReservation(spec.topology().nodeCount(), spec.resources().memoryMiB()));
        closeWon = closed;
      }
      if (closeWon) {
        releasePrivate(cluster);
        throw new IllegalStateException("The CCM cluster pool closed during private provisioning");
      }
      return new PrivateClusterLease(this, cluster, createResourceScope(cluster));
    } catch (CcmProvisioner.CcmClusterProvisioningException exception) {
      synchronized (this) {
        failedProvisioningRemovals.add(exception.cluster());
        privateReservations.put(
            exception.cluster(),
            new ClusterReservation(spec.topology().nodeCount(), spec.resources().memoryMiB()));
      }
      throw exception;
    } catch (Exception exception) {
      if (!provisioned) {
        synchronized (this) {
          releaseReservation(spec.topology().nodeCount(), memoryRequired(spec), ccmId);
        }
      }
      throw exception;
    } finally {
      synchronized (this) {
        privateProvisioning.remove(provisioningFinished);
        notifyAll();
      }
      provisioningFinished.complete(null);
    }
  }

  void releaseReusable(PooledCluster pooledCluster, TestResourceScope resources) throws Exception {
    boolean cleanupStarted;
    synchronized (this) {
      cleanupStarted = !closed;
      if (cleanupStarted) {
        // Publishing this before leaving the monitor lets close() wait for cleanup that has
        // already begun. A release that loses the race with close skips SDK calls and joins
        // physical retirement below.
        pooledCluster.pendingReleases++;
      }
    }
    boolean poisoned = false;
    Exception cleanupFailure = null;
    if (cleanupStarted) {
      try {
        cleanupResources.cleanup(resources);
        PhysicalTestCluster cluster = await(pooledCluster.ready);
        poisoned = !provisioner.isHealthy(cluster);
      } catch (Exception exception) {
        poisoned = true;
        cleanupFailure = exception;
      }
    }

    boolean destroy = false;
    synchronized (this) {
      pooledCluster.poisoned |= poisoned;
      if (cleanupStarted) {
        pooledCluster.pendingReleases--;
      }
      pooledCluster.referenceCount--;
      pooledCluster.lastReleasedOrder = ++releaseCounter;
      if (pooledCluster.referenceCount < 0) {
        throw new IllegalStateException("A reusable cluster lease was released more than once");
      }
      if (pooledCluster.referenceCount == 0 && (pooledCluster.poisoned || closed)) {
        reusableClusters.remove(pooledCluster.reuseKey);
        destroy = true;
      }
      if (pooledCluster.referenceCount == 0) {
        pooledCluster.reuseValidation = null;
      }
      notifyAll();
    }
    if (destroy) {
      destroyPooled(pooledCluster);
    }
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  void releasePrivate(PhysicalTestCluster cluster) throws Exception {
    destroyPrivate(cluster);
  }

  void reserveAdditionalPrivateNode(PhysicalTestCluster cluster) throws Exception {
    while (true) {
      RemovalCandidate failedRemoval;
      PooledCluster eviction;
      synchronized (this) {
        throwIfClosed();
        ClusterReservation reservation = privateReservations.get(cluster);
        if (reservation == null || !privateClusters.contains(cluster)) {
          throw new IllegalStateException("The private cluster is no longer owned by this pool");
        }
        int resultingNodeCount = reservation.nodes + 1;
        long resultingMemory = resultingNodeCount * (long) reservation.memoryMiBPerNode;
        if (resultingNodeCount > maximumNodes || resultingMemory > capacity.usableMemoryMiB()) {
          throw new IllegalStateException(
              "Adding a node would exceed this run's node or memory capacity");
        }
        if (fits(1, reservation.memoryMiBPerNode)) {
          reserve(1, reservation.memoryMiBPerNode);
          reservation.nodes++;
          return;
        }
        failedRemoval = findFailedRemoval();
        eviction = failedRemoval == null ? findIdleEviction() : null;
        if (eviction != null) {
          reusableClusters.remove(eviction.reuseKey);
        }
      }
      if (failedRemoval != null) {
        retryFailedRemoval(failedRemoval);
      } else if (eviction != null) {
        destroyPooled(eviction);
      } else {
        throw new IllegalStateException(
            "Adding a node requires capacity held by active leases and cannot wait safely");
      }
    }
  }

  synchronized void releaseAdditionalPrivateNode(PhysicalTestCluster cluster) {
    ClusterReservation reservation = privateReservations.get(cluster);
    if (reservation == null) {
      // Whole-cluster removal already released the pending node reservation.
      return;
    }
    if (reservation.nodes <= 0) {
      throw new IllegalStateException("Private cluster capacity was released more than once");
    }
    reservation.nodes--;
    releaseCapacity(1, reservation.memoryMiBPerNode);
  }

  @Override
  public void close() throws Exception {
    InterruptTracker interrupts = new InterruptTracker();
    interrupts.captureCurrent();
    List<PooledCluster> pooled;
    List<CompletableFuture<Void>> privateProvisioningSnapshot;
    synchronized (this) {
      closed = true;
      // This authoritative set includes clusters already removed from the reuse index by an
      // eviction or poison path. Ownership ends only after physical removal succeeds.
      pooled = new ArrayList<>(ownedPooledClusters);
      privateProvisioningSnapshot = new ArrayList<>(privateProvisioning);
      reusableClusters.clear();
      notifyAll();
    }
    List<Exception> failures = new ArrayList<>();
    for (PooledCluster cluster : pooled) {
      try {
        awaitPooledCleanupForClose(cluster, interrupts);
        destroyPooledForClose(cluster, interrupts);
      } catch (Exception exception) {
        failures.add(exception);
        interrupts.captureCurrent();
      }
    }
    for (CompletableFuture<Void> provisioning : privateProvisioningSnapshot) {
      awaitForClose(provisioning, interrupts);
    }
    List<PhysicalTestCluster> privateSnapshot;
    synchronized (this) {
      privateSnapshot = new ArrayList<>(privateClusters);
    }
    for (PhysicalTestCluster cluster : privateSnapshot) {
      try {
        runCloseCleanup(() -> destroyPrivate(cluster), interrupts);
      } catch (Exception exception) {
        failures.add(exception);
        interrupts.captureCurrent();
      }
    }
    List<PhysicalTestCluster> failedProvisioningSnapshot;
    synchronized (this) {
      // Provisioning failures are published before their ready futures complete. Snapshotting here,
      // after destroyPooled has awaited those futures, also includes failures racing with close().
      failedProvisioningSnapshot = new ArrayList<>(failedProvisioningRemovals);
    }
    for (PhysicalTestCluster cluster : failedProvisioningSnapshot) {
      try {
        runCloseCleanup(() -> destroyFailedProvisioning(cluster), interrupts);
      } catch (Exception exception) {
        failures.add(exception);
        interrupts.captureCurrent();
      }
    }
    interrupts.captureCurrent();
    InterruptedException interruption = interrupts.interruption();
    if (!failures.isEmpty()) {
      Exception aggregate = new Exception("One or more CCM clusters could not be removed");
      failures.forEach(aggregate::addSuppressed);
      if (interruption != null && !failures.contains(interruption)) {
        aggregate.addSuppressed(interruption);
      }
      if (interruption != null) {
        Thread.currentThread().interrupt();
      }
      throw aggregate;
    }
    if (interruption != null) {
      Thread.currentThread().interrupt();
      throw interruption;
    }
  }

  private void startPooledProvisioning(PooledCluster pooledCluster) {
    CompletableFuture.runAsync(
        () -> {
          try {
            PhysicalTestCluster cluster =
                provisioner.provision(
                    pooledCluster.spec, pooledCluster.instanceId, pooledCluster.ccmId);
            pooledCluster.ready.complete(cluster);
            synchronized (this) {
              notifyAll();
            }
          } catch (Exception exception) {
            synchronized (this) {
              reusableClusters.remove(pooledCluster.reuseKey);
              if (exception instanceof CcmProvisioner.CcmClusterProvisioningException) {
                PhysicalTestCluster failedCluster =
                    ((CcmProvisioner.CcmClusterProvisioningException) exception).cluster();
                failedProvisioningRemovals.add(failedCluster);
                privateReservations.put(
                    failedCluster,
                    new ClusterReservation(
                        pooledCluster.spec.topology().nodeCount(),
                        pooledCluster.spec.resources().memoryMiB()));
                // The physical rollback record now owns this unchanged reservation.
                pooledCluster.reservationReleased = true;
                ownedPooledClusters.remove(pooledCluster);
              } else {
                releasePooledReservation(pooledCluster);
              }
              notifyAll();
            }
            pooledCluster.ready.completeExceptionally(exception);
          }
        });
  }

  private void abandonReusable(PooledCluster pooledCluster, boolean poison) throws Exception {
    boolean destroy = false;
    synchronized (this) {
      pooledCluster.poisoned |= poison;
      pooledCluster.referenceCount--;
      if (pooledCluster.referenceCount == 0) {
        pooledCluster.reuseValidation = null;
      }
      if (pooledCluster.referenceCount == 0 && pooledCluster.poisoned) {
        reusableClusters.remove(pooledCluster.reuseKey);
        destroy = true;
      }
      notifyAll();
    }
    if (destroy) {
      destroyPooled(pooledCluster);
    }
  }

  private void reservePrivateCapacity(ClusterSpec spec) throws Exception {
    int nodes = spec.topology().nodeCount();
    long memory = memoryRequired(spec);
    while (true) {
      PooledCluster eviction = null;
      RemovalCandidate failedRemoval = null;
      synchronized (this) {
        throwIfClosed();
        if (fits(nodes, memory)) {
          reserve(nodes, memory);
          return;
        }
        failedRemoval = findFailedRemoval();
        if (failedRemoval == null) {
          eviction = findIdleEviction();
          if (eviction != null) {
            reusableClusters.remove(eviction.reuseKey);
          } else {
            wait();
          }
        }
      }
      if (failedRemoval != null) {
        retryFailedRemoval(failedRemoval);
      } else if (eviction != null) {
        destroyPooled(eviction);
      }
    }
  }

  private void destroyPooled(PooledCluster pooledCluster) throws Exception {
    PhysicalTestCluster cluster;
    try {
      cluster = await(pooledCluster.ready);
    } catch (InterruptedException exception) {
      synchronized (this) {
        failedPooledRemovals.add(pooledCluster);
        notifyAll();
      }
      throw exception;
    } catch (Exception exception) {
      synchronized (this) {
        if (!(exception instanceof CcmProvisioner.CcmClusterProvisioningException)) {
          releasePooledReservation(pooledCluster);
        }
      }
      return;
    }
    destroyReadyPooled(pooledCluster, cluster);
  }

  private void destroyPooledForClose(PooledCluster pooledCluster, InterruptTracker interrupts)
      throws Exception {
    PhysicalTestCluster cluster;
    try {
      cluster = awaitForClose(pooledCluster.ready, interrupts);
    } catch (Exception exception) {
      synchronized (this) {
        if (!(exception instanceof CcmProvisioner.CcmClusterProvisioningException)) {
          releasePooledReservation(pooledCluster);
        }
      }
      return;
    }
    runCloseCleanup(() -> destroyReadyPooled(pooledCluster, cluster), interrupts);
  }

  private void destroyReadyPooled(PooledCluster pooledCluster, PhysicalTestCluster cluster)
      throws Exception {
    try {
      removePhysical(cluster);
    } catch (Exception exception) {
      synchronized (this) {
        failedPooledRemovals.add(pooledCluster);
        notifyAll();
      }
      throw exception;
    }
    synchronized (this) {
      try {
        releasePooledReservation(pooledCluster);
        failedPooledRemovals.remove(pooledCluster);
      } catch (RuntimeException exception) {
        failedPooledRemovals.add(pooledCluster);
        notifyAll();
        throw exception;
      }
    }
  }

  private void destroyPrivate(PhysicalTestCluster cluster) throws Exception {
    try {
      removePhysical(cluster);
    } catch (Exception exception) {
      synchronized (this) {
        if (privateClusters.contains(cluster)) {
          failedPrivateRemovals.add(cluster);
          notifyAll();
        }
      }
      throw exception;
    }
    synchronized (this) {
      try {
        if (privateClusters.contains(cluster)) {
          releaseClusterReservation(cluster);
          privateClusters.remove(cluster);
        }
        failedPrivateRemovals.remove(cluster);
      } catch (RuntimeException exception) {
        failedPrivateRemovals.add(cluster);
        notifyAll();
        throw exception;
      }
    }
  }

  private void destroyFailedProvisioning(PhysicalTestCluster cluster) throws Exception {
    removePhysical(cluster);
    synchronized (this) {
      if (failedProvisioningRemovals.contains(cluster)) {
        releaseClusterReservation(cluster);
        failedProvisioningRemovals.remove(cluster);
      }
    }
  }

  private void removePhysical(PhysicalTestCluster cluster) throws Exception {
    CompletableFuture<Void> removal;
    boolean execute;
    synchronized (this) {
      removal = physicalRemovals.get(cluster);
      execute = removal == null;
      if (execute) {
        removal = new CompletableFuture<>();
        physicalRemovals.put(cluster, removal);
      }
    }
    if (!execute) {
      awaitVoid(removal);
      return;
    }
    try {
      cluster.removePhysical();
      removal.complete(null);
    } catch (Exception exception) {
      removal.completeExceptionally(exception);
      synchronized (this) {
        physicalRemovals.remove(cluster, removal);
      }
      throw exception;
    }
  }

  private void retryFailedRemoval(RemovalCandidate candidate) throws Exception {
    switch (candidate.owner) {
      case POOLED:
        destroyPooled(candidate.pooledCluster);
        return;
      case PRIVATE:
        destroyPrivate(candidate.cluster);
        return;
      case FAILED_PROVISIONING:
        destroyFailedProvisioning(candidate.cluster);
        return;
      default:
        throw new IllegalStateException("Unknown removal owner " + candidate.owner);
    }
  }

  private synchronized RemovalCandidate findFailedRemoval() {
    if (!failedPooledRemovals.isEmpty()) {
      return new RemovalCandidate(
          RemovalOwner.POOLED, failedPooledRemovals.iterator().next(), null);
    }
    if (!failedPrivateRemovals.isEmpty()) {
      return new RemovalCandidate(
          RemovalOwner.PRIVATE, null, failedPrivateRemovals.iterator().next());
    }
    if (!failedProvisioningRemovals.isEmpty()) {
      return new RemovalCandidate(
          RemovalOwner.FAILED_PROVISIONING, null, failedProvisioningRemovals.iterator().next());
    }
    return null;
  }

  private static void runCloseCleanup(CleanupOperation operation, InterruptTracker interrupts)
      throws Exception {
    for (int attempt = 0; ; attempt++) {
      try {
        operation.run();
        interrupts.captureCurrent();
        return;
      } catch (InterruptedException exception) {
        interrupts.capture(exception);
        if (attempt == 1) {
          throw exception;
        }
      }
    }
  }

  private void awaitPooledCleanupForClose(
      PooledCluster pooledCluster, InterruptTracker interrupts) {
    synchronized (this) {
      while (pooledCluster.pendingReleases > 0) {
        try {
          wait();
        } catch (InterruptedException exception) {
          interrupts.capture(exception);
        }
      }
    }
  }

  private void releaseClusterReservation(PhysicalTestCluster cluster) {
    ClusterReservation reservation = privateReservations.get(cluster);
    if (reservation != null) {
      releaseReservation(
          reservation.nodes,
          reservation.nodes * (long) reservation.memoryMiBPerNode,
          cluster.ccmId());
      privateReservations.remove(cluster, reservation);
    }
  }

  private synchronized TestResourceScope createResourceScope(PhysicalTestCluster cluster) {
    return new TestResourceScope(
        cluster,
        provisioner.runDirectory().getFileName().toString(),
        leaseCounter.incrementAndGet());
  }

  private static PhysicalTestCluster await(CompletableFuture<PhysicalTestCluster> future)
      throws Exception {
    try {
      return future.get();
    } catch (ExecutionException exception) {
      Throwable cause = exception.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw exception;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw exception;
    }
  }

  private static boolean awaitBoolean(CompletableFuture<Boolean> future) throws Exception {
    try {
      return future.get();
    } catch (ExecutionException exception) {
      Throwable cause = exception.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw exception;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw exception;
    }
  }

  private static void awaitVoid(CompletableFuture<Void> future) throws Exception {
    try {
      future.get();
    } catch (ExecutionException exception) {
      Throwable cause = exception.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw exception;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw exception;
    }
  }

  private static <T> T awaitForClose(CompletableFuture<T> future, InterruptTracker interrupts)
      throws Exception {
    while (true) {
      try {
        return future.get();
      } catch (InterruptedException exception) {
        interrupts.capture(exception);
      } catch (ExecutionException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof Exception) {
          throw (Exception) cause;
        }
        throw exception;
      }
    }
  }

  private long memoryRequired(ClusterSpec spec) {
    return spec.topology().nodeCount() * (long) spec.resources().memoryMiB();
  }

  private synchronized boolean fits(int nodes, long memoryMiB) {
    return usedNodes + nodes <= maximumNodes
        && usedMemoryMiB + memoryMiB <= capacity.usableMemoryMiB();
  }

  private void validateDemand(ClusterSpec spec) {
    int nodes = spec.topology().nodeCount();
    long memory = memoryRequired(spec);
    if (nodes > maximumNodes || memory > capacity.usableMemoryMiB()) {
      throw new IllegalStateException(
          "Cluster needs "
              + nodes
              + " nodes and "
              + memory
              + " MiB, but this run permits "
              + maximumNodes
              + " nodes and "
              + capacity.usableMemoryMiB()
              + " MiB after reserving "
              + capacity.reservedMemoryMiB()
              + " MiB for the system");
    }
  }

  private PooledCluster findIdleEviction() {
    return reusableClusters.values().stream()
        .filter(cluster -> cluster.referenceCount == 0 && cluster.ready.isDone())
        .min(Comparator.comparingLong(cluster -> cluster.lastReleasedOrder))
        .orElse(null);
  }

  private void reserve(int nodes, long memoryMiB) {
    usedNodes += nodes;
    usedMemoryMiB += memoryMiB;
  }

  private void releasePooledReservation(PooledCluster pooledCluster) {
    if (pooledCluster.reservationReleased) {
      return;
    }
    releaseReservation(
        pooledCluster.spec.topology().nodeCount(),
        memoryRequired(pooledCluster.spec),
        pooledCluster.ccmId);
    pooledCluster.reservationReleased = true;
    ownedPooledClusters.remove(pooledCluster);
  }

  private void releaseReservation(int nodes, long memoryMiB, int ccmId) {
    IdLock idLock = ccmIdLocks.get(ccmId);
    if (idLock != null) {
      try {
        idLock.close();
        ccmIdLocks.remove(ccmId, idLock);
      } catch (IOException exception) {
        throw new IllegalStateException("Failed to release CCM ID " + ccmId, exception);
      }
    }
    releaseCapacity(nodes, memoryMiB);
  }

  private void releaseCapacity(int nodes, long memoryMiB) {
    usedNodes -= nodes;
    usedMemoryMiB -= memoryMiB;
    notifyAll();
  }

  private int allocateCcmId() throws IOException {
    prepareCcmIdLockRoot();
    IOException lastLockFailure = null;
    for (int id = 1; id < 100; id++) {
      if (ccmIdLocks.containsKey(id) || isCcmAddressRangeInUse(id)) {
        continue;
      }
      FileChannel channel = null;
      FileLock fileLock = null;
      Path reservationFile = ccmIdReservationPath(ccmIdLockRoot, id);
      Path preparedReservation = null;
      boolean reservationPublished = false;
      try {
        channel = openIdLockChannel(ccmIdLockPath(ccmIdLockRoot, id));
        fileLock = channel.tryLock(0, 1, false);
        if (fileLock == null) {
          channel.close();
          continue;
        }
        if (isCcmAddressRangeInUse(id)) {
          fileLock.release();
          channel.close();
          continue;
        }
        if (Files.exists(reservationFile, LinkOption.NOFOLLOW_LINKS)) {
          fileLock.release();
          channel.close();
          continue;
        }
        if (publishDurableIdReservations) {
          preparedReservation =
              Files.createTempFile(ccmIdLockRoot, "." + id + ".reservation.", ".tmp");
          Files.setPosixFilePermissions(
              preparedReservation,
              java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
          Files.writeString(
              preparedReservation,
              provisioner.runDirectory().toAbsolutePath().normalize() + "\n",
              java.nio.charset.StandardCharsets.UTF_8,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE);
          try {
            // A hard-link publication is atomic, cannot replace an existing reservation, and
            // makes the fully-written owner visible in the same operation.
            Files.createLink(reservationFile, preparedReservation);
          } catch (FileAlreadyExistsException occupied) {
            fileLock.release();
            channel.close();
            Files.delete(preparedReservation);
            continue;
          }
          reservationPublished = true;
          Files.delete(preparedReservation);
          preparedReservation = null;
        }
        ccmIdLocks.put(
            id,
            new IdLock(channel, fileLock, publishDurableIdReservations ? reservationFile : null));
        return id;
      } catch (OverlappingFileLockException exception) {
        closeBestEffort(channel);
        deleteBestEffort(preparedReservation);
        if (reservationPublished) {
          deleteBestEffort(reservationFile);
        }
      } catch (IOException exception) {
        closeBestEffort(channel);
        deleteBestEffort(preparedReservation);
        if (reservationPublished) {
          deleteBestEffort(reservationFile);
        }
        lastLockFailure = exception;
      }
    }
    throw new IllegalStateException("No CCM cluster IDs are available", lastLockFailure);
  }

  private void prepareCcmIdLockRoot() throws IOException {
    try {
      Files.createDirectories(ccmIdLockRoot);
    } catch (FileAlreadyExistsException ignored) {
      // The validation below reports a useful error for a non-directory path.
    }
    if (Files.isSymbolicLink(ccmIdLockRoot)
        || !Files.isDirectory(ccmIdLockRoot, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("CCM ID lock root is not a safe directory: " + ccmIdLockRoot);
    }
    if (!Files.getOwner(ccmIdLockRoot)
        .equals(Files.getOwner(Path.of("/proc/self/status"), LinkOption.NOFOLLOW_LINKS))) {
      throw new IOException("CCM ID lock root is not owned by the current user: " + ccmIdLockRoot);
    }
    try {
      Files.setPosixFilePermissions(
          ccmIdLockRoot, java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"));
    } catch (UnsupportedOperationException ignored) {
      // The harness is Linux-only, but retain a clear directory/ownership check on other providers.
    }
  }

  static Path ccmIdLockPath(Path lockDirectory, int id) {
    return lockDirectory.resolve(id + ".lock");
  }

  static Path ccmIdReservationPath(Path lockDirectory, int id) {
    return lockDirectory.resolve(id + ".reservation");
  }

  private static void deleteBestEffort(Path path) {
    if (path == null) {
      return;
    }
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // A durable reservation is safer than reusing an address after an ambiguous failure.
    }
  }

  private static FileChannel openIdLockChannel(Path lockPath) throws IOException {
    FileChannel channel;
    try {
      channel =
          FileChannel.open(
              lockPath,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE,
              LinkOption.NOFOLLOW_LINKS);
    } catch (FileAlreadyExistsException exception) {
      if (!Files.isRegularFile(lockPath, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("CCM ID lock path is not a regular file: " + lockPath, exception);
      }
      return FileChannel.open(lockPath, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    }
    return channel;
  }

  private static void closeBestEffort(FileChannel channel) {
    if (channel == null) {
      return;
    }
    try {
      channel.close();
    } catch (IOException ignored) {
      // The caller is already abandoning this candidate ID.
    }
  }

  static boolean isCcmAddressRangeInUse(int id) {
    int[] ports = {
      CcmProvisioner.STORAGE_PORT,
      7199,
      9042,
      9180,
      CcmProvisioner.HTTP_PORT,
      CcmProvisioner.HTTPS_PORT,
      CcmProvisioner.API_PORT,
      19042
    };
    for (int node = 1; node <= ClusterCapacity.MAXIMUM_NODE_COUNT; node++) {
      for (int port : ports) {
        try (Socket socket = new Socket()) {
          socket.connect(new InetSocketAddress("127.0." + id + "." + node, port), 20);
          return true;
        } catch (IOException ignored) {
          // This address and port are available.
        }
      }
    }
    return false;
  }

  private String createInstanceId() {
    return "alternator-java-" + ProcessHandle.current().pid() + "-" + (++instanceCounter);
  }

  private void throwIfClosed() {
    if (closed) {
      throw new IllegalStateException("The CCM cluster pool is closed");
    }
  }

  private static int parsePositiveInteger(String variable, int defaultValue) {
    String value = System.getenv(variable);
    if (value == null) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed < 1 || !value.matches("[0-9]+")) {
        throw new NumberFormatException();
      }
      return parsed;
    } catch (NumberFormatException exception) {
      throw new IllegalStateException(variable + " must be a positive integer", exception);
    }
  }
}
