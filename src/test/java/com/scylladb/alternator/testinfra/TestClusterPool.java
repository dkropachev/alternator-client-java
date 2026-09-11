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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileLockInterruptionException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** A deliberately small, single-physical-cluster CCM pool. */
final class TestClusterPool implements AutoCloseable {
  static final int MAXIMUM_NODE_COUNT = 9;

  private static final AtomicLong INSTANCE_COUNTER = new AtomicLong();
  private static final String PROCESS_INSTANCE_TOKEN =
      UUID.randomUUID().toString().replace("-", "").substring(0, 16);

  interface ResourceCleanup {
    void cleanup(TestResourceScope resources) throws Exception;
  }

  /** Opaque identity used to reject a late or duplicate reusable release. */
  static final class PooledCluster {
    private final long generation;

    private PooledCluster(long generation) {
      this.generation = generation;
    }
  }

  private static final class Ownership {
    final int ccmId;
    final CcmRunState.ClusterHandle durableHandle;

    Ownership(int ccmId, CcmRunState.ClusterHandle durableHandle) {
      this.ccmId = ccmId;
      this.durableHandle = durableHandle;
    }
  }

  private static final class Slot {
    final PooledCluster token;
    final String reuseKey;
    final boolean privateCluster;
    final PhysicalTestCluster cluster;
    final Ownership ownership;
    int references;
    boolean poisoned;
    boolean retiring;

    Slot(
        PooledCluster token,
        String reuseKey,
        boolean privateCluster,
        PhysicalTestCluster cluster,
        Ownership ownership,
        int references) {
      this.token = token;
      this.reuseKey = reuseKey;
      this.privateCluster = privateCluster;
      this.cluster = cluster;
      this.ownership = ownership;
      this.references = references;
    }
  }

  private final CcmProvisioner provisioner;
  private final int maximumNodes;
  private final ResourceCleanup cleanupResources;
  private final CcmRunState runState;
  private final AtomicLong leaseCounter = new AtomicLong();
  private long generation;
  private Slot current;
  private Exception terminalFailure;
  private boolean closed;

  /** Constructor for focused tests whose fake provisioner does not need durable ownership. */
  TestClusterPool(CcmProvisioner provisioner, int maximumNodes, ResourceCleanup cleanupResources) {
    this(provisioner, maximumNodes, cleanupResources, null);
  }

  TestClusterPool(
      CcmProvisioner provisioner,
      int maximumNodes,
      ResourceCleanup cleanupResources,
      CcmRunState runState) {
    if (maximumNodes < 1 || maximumNodes > MAXIMUM_NODE_COUNT) {
      throw new IllegalArgumentException(
          "maximumNodes must be between 1 and " + MAXIMUM_NODE_COUNT);
    }
    this.provisioner = provisioner;
    this.maximumNodes = maximumNodes;
    this.cleanupResources =
        cleanupResources == null ? TestResourceScope::cleanup : cleanupResources;
    this.runState = runState;
  }

  static TestClusterPool createDefault() throws IOException {
    CcmRunState state =
        CcmRunState.openDefault(
            (runDirectory, manifest) ->
                new CcmProvisioner(runDirectory)
                    .cleanupStaleCluster(
                        manifest.instanceId(),
                        manifest.ccmId(),
                        runDirectory.resolve("clusters").resolve(manifest.instanceId())));
    try {
      CcmProvisioner provisioner = new CcmProvisioner(state.runDirectory());
      int configuredMaximum = parsePositiveInteger("SCYLLA_CCM_MAX_NODES", MAXIMUM_NODE_COUNT);
      return new TestClusterPool(
          provisioner, Math.min(configuredMaximum, MAXIMUM_NODE_COUNT), null, state);
    } catch (IOException | RuntimeException exception) {
      try {
        state.close();
      } catch (IOException cleanupException) {
        exception.addSuppressed(cleanupException);
      }
      throw exception;
    }
  }

  synchronized ReusableClusterLease acquireReusable(ClusterSpec spec) throws Exception {
    validateDemand(spec);
    throwIfUnavailable();
    throwIfInterrupted();

    if (current != null) {
      if (current.privateCluster) {
        throw new IllegalStateException("A private CCM cluster is already active in this JVM");
      }
      if (current.references > 0) {
        if (!current.reuseKey.equals(spec.reuseKey()) || current.poisoned) {
          throw new IllegalStateException(
              "The active reusable CCM cluster is incompatible with the requested specification");
        }
        current.references++;
        return reusableLease(current);
      }

      if (current.reuseKey.equals(spec.reuseKey())
          && !current.poisoned
          && provisioner.isHealthy(current.cluster)) {
        current.references = 1;
        return reusableLease(current);
      }
      retireCurrent();
      throwIfInterrupted();
    }

    Slot created = provision(spec, false);
    created.references = 1;
    current = created;
    return reusableLease(created);
  }

  synchronized PrivateClusterLease provisionPrivate(ClusterSpec spec) throws Exception {
    validateDemand(spec);
    throwIfUnavailable();
    throwIfInterrupted();

    if (current != null) {
      if (current.privateCluster || current.references > 0) {
        throw new IllegalStateException("A CCM cluster lease is already active in this JVM");
      }
      retireCurrent();
      throwIfInterrupted();
    }

    Slot created = provision(spec, true);
    current = created;
    return new PrivateClusterLease(this, created.cluster, createResourceScope(created.cluster));
  }

  synchronized void releaseReusable(PooledCluster token, TestResourceScope resources)
      throws Exception {
    if (current == null || current.token != token || current.token.generation != token.generation) {
      return;
    }
    if (current.references <= 0) {
      throw new IllegalStateException("A reusable CCM cluster lease was released more than once");
    }

    Exception failure = null;
    if (!closed) {
      try {
        cleanupResources.cleanup(resources);
      } catch (Exception exception) {
        current.poisoned = true;
        terminalFailure = exception;
        failure = exception;
      }
    }
    current.references--;
    if (current.references == 0 && (current.poisoned || closed) && !current.retiring) {
      try {
        retireCurrent();
        if (current == null) {
          terminalFailure = null;
        }
      } catch (Exception exception) {
        failure = combine(failure, exception);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  void releasePrivate(PhysicalTestCluster cluster) throws Exception {
    synchronized (this) {
      if (current == null || current.cluster != cluster) {
        return;
      }
      if (!current.privateCluster) {
        throw new IllegalStateException("The cluster is not privately owned by this pool");
      }
    }
    retireCluster(cluster);
  }

  synchronized void reserveAdditionalPrivateNode(PhysicalTestCluster cluster) {
    throwIfClosed();
    if (current == null || !current.privateCluster || current.cluster != cluster) {
      throw new IllegalStateException("The private cluster is no longer owned by this pool");
    }
    if (cluster.nodes().size() >= maximumNodes) {
      throw new IllegalStateException(
          "Adding a node would exceed this run's " + maximumNodes + "-node limit");
    }
  }

  synchronized void releaseAdditionalPrivateNode(PhysicalTestCluster cluster) {
    // No capacity ledger exists in the single-slot model. Membership in the physical cluster is
    // the authoritative node count.
  }

  private void retireCluster(PhysicalTestCluster cluster) throws Exception {
    Slot retiring;
    synchronized (this) {
      while (current != null && current.cluster == cluster && current.retiring) {
        wait();
      }
      if (current == null || current.cluster != cluster) {
        return;
      }
      retiring = current;
      retiring.retiring = true;
    }

    try {
      runCleanupPreservingInterrupt(
          () -> {
            cluster.removePhysical();
            completeOwnership(retiring.ownership);
          });
    } catch (Exception exception) {
      synchronized (this) {
        retiring.retiring = false;
        terminalFailure = exception;
        notifyAll();
      }
      throw exception;
    }
    synchronized (this) {
      if (current == retiring) {
        current = null;
      }
      retiring.retiring = false;
      terminalFailure = null;
      notifyAll();
    }
  }

  private void retireCurrent() throws Exception {
    Slot retiring = current;
    if (retiring == null) {
      return;
    }
    try {
      runCleanupPreservingInterrupt(
          () -> {
            retiring.cluster.removePhysical();
            completeOwnership(retiring.ownership);
          });
      current = null;
      terminalFailure = null;
    } catch (Exception exception) {
      terminalFailure = exception;
      throw exception;
    }
  }

  private Slot provision(ClusterSpec spec, boolean privateCluster) throws Exception {
    String instanceId = createInstanceId();
    Ownership ownership = reserveOwnership(spec, instanceId);
    try {
      PhysicalTestCluster cluster = provisioner.provision(spec, instanceId, ownership.ccmId);
      return new Slot(
          new PooledCluster(++generation),
          privateCluster ? null : spec.reuseKey(),
          privateCluster,
          cluster,
          ownership,
          0);
    } catch (CcmProvisioner.CcmClusterProvisioningException exception) {
      PhysicalTestCluster cluster = exception.cluster();
      cluster.markDirty();
      if (CcmProvisioner.requiresNextRunRecovery(exception)) {
        cluster.markRecoveryRequired();
      }
      Slot failed = new Slot(new PooledCluster(++generation), null, true, cluster, ownership, 0);
      failed.poisoned = true;
      current = failed;
      terminalFailure = exception;
      throw exception;
    } catch (Exception exception) {
      try {
        completeOwnership(ownership);
      } catch (Exception cleanupException) {
        exception.addSuppressed(cleanupException);
        terminalFailure = cleanupException;
      }
      throw exception;
    }
  }

  private Ownership reserveOwnership(ClusterSpec spec, String instanceId) throws IOException {
    boolean includeJmxPort = provisioner.requiresJmxPortReservation(spec);
    if (runState != null) {
      CcmRunState.ClusterHandle handle = runState.beginCluster(instanceId, spec, includeJmxPort);
      return new Ownership(handle.ccmId(), handle);
    }
    for (int id = 1; id < 100; id++) {
      if (CcmRunState.isAddressRangeAvailable(spec, id, includeJmxPort)) {
        return new Ownership(id, null);
      }
    }
    throw new IllegalStateException("No CCM cluster IDs are available");
  }

  private void completeOwnership(Ownership ownership) throws IOException {
    if (runState != null && ownership.durableHandle != null) {
      runState.completeCluster(ownership.durableHandle);
    }
  }

  private ReusableClusterLease reusableLease(Slot slot) {
    return new ReusableClusterLease(
        this, slot.token, slot.cluster, createResourceScope(slot.cluster));
  }

  private TestResourceScope createResourceScope(PhysicalTestCluster cluster) {
    return new TestResourceScope(
        cluster,
        provisioner.runDirectory().getFileName().toString(),
        leaseCounter.incrementAndGet());
  }

  private void validateDemand(ClusterSpec spec) {
    spec.validate();
    if (spec.topology().nodeCount() > maximumNodes) {
      throw new IllegalStateException(
          "The requested cluster exceeds this run's " + maximumNodes + "-node limit");
    }
  }

  private void throwIfUnavailable() {
    throwIfClosed();
    if (terminalFailure != null) {
      throw new IllegalStateException(
          "The CCM pool retained failed cleanup state; close it before provisioning again",
          terminalFailure);
    }
  }

  private void throwIfClosed() {
    if (closed) {
      throw new IllegalStateException("The CCM cluster pool is closed");
    }
  }

  @Override
  public void close() throws Exception {
    Slot retiring;
    synchronized (this) {
      closed = true;
      retiring = current;
    }
    Exception failure = null;
    if (retiring != null) {
      try {
        runCleanupPreservingInterrupt(() -> retireCluster(retiring.cluster));
      } catch (Exception exception) {
        failure = exception;
      }
    }
    if (failure == null && runState != null) {
      try {
        runCleanupPreservingInterrupt(runState::close);
      } catch (IOException exception) {
        failure = exception;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  Path runDirectory() {
    return provisioner.runDirectory();
  }

  boolean publishesDurableIdReservations() {
    return runState != null;
  }

  private String createInstanceId() {
    return "alternator-java-"
        + ProcessHandle.current().pid()
        + "-"
        + PROCESS_INSTANCE_TOKEN
        + "-"
        + INSTANCE_COUNTER.incrementAndGet();
  }

  private static int parsePositiveInteger(String variable, int defaultValue) {
    String value = System.getenv(variable);
    if (value == null || value.trim().isEmpty()) {
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

  private static Exception combine(Exception first, Exception second) {
    if (first == null) {
      return second;
    }
    first.addSuppressed(second);
    return first;
  }

  private static void throwIfInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("CCM cluster acquisition was interrupted");
    }
  }

  private static void runCleanupPreservingInterrupt(CleanupOperation operation) throws Exception {
    boolean interrupted = Thread.interrupted();
    int interruptedAttempts = 0;
    try {
      while (true) {
        try {
          operation.run();
          return;
        } catch (InterruptedException
            | ClosedByInterruptException
            | FileLockInterruptionException exception) {
          interrupted = true;
          Thread.interrupted();
          if (++interruptedAttempts == 2) {
            throw exception;
          }
        }
      }
    } finally {
      interrupted |= Thread.interrupted();
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @FunctionalInterface
  private interface CleanupOperation {
    void run() throws Exception;
  }
}
