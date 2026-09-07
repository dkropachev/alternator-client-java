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
package com.scylladb.alternator.internal;

import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import software.amazon.awssdk.http.ExecutableHttpRequest;

/** Coordinates node-health state, direct probes, and probe executor lifecycle. */
final class NodeHealthManager {
  private static final int PROBE_QUEUE_MULTIPLIER = 16;
  private static final AtomicInteger PROBE_THREAD_ID = new AtomicInteger();
  private static final ThreadLocal<Boolean> PROBE_WORKER = new ThreadLocal<>();

  private final NodeHealthConfig config;
  private final NodeHealthStore healthStore;
  private final Supplier<List<URI>> discoveredNodes;
  private final ProbeTransport probeTransport;
  private final Object topologyHealthLock = new Object();
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);
  private final ThreadPoolExecutor probeExecutor;
  private final ScheduledThreadPoolExecutor timeoutExecutor;
  private final Semaphore probeCapacity;
  private final ConcurrentMap<URI, ProbeJob> inFlightProbes = new ConcurrentHashMap<>();
  private final Set<URI> skipNextBackgroundQuarantineProbe = ConcurrentHashMap.newKeySet();
  private final AtomicLong probeSequence = new AtomicLong();
  private final AtomicInteger nextDownBackgroundProbeIndex = new AtomicInteger();
  private final AtomicInteger nextQuarantineBackgroundProbeIndex = new AtomicInteger();
  private final AtomicInteger backgroundSingleSlotTier = new AtomicInteger();

  NodeHealthManager(
      NodeHealthConfig config,
      List<URI> initialNodes,
      Supplier<List<URI>> discoveredNodes,
      ProbeTransport probeTransport) {
    if (discoveredNodes == null || probeTransport == null) {
      throw new IllegalArgumentException("discoveredNodes and probeTransport cannot be null");
    }
    this.config = config != null ? config : NodeHealthConfig.getDefault();
    this.discoveredNodes = discoveredNodes;
    this.probeTransport = probeTransport;

    int probeConcurrency = this.config.getHealthProbeConcurrency();
    this.probeCapacity = new Semaphore(probeConcurrency * (PROBE_QUEUE_MULTIPLIER + 1));
    this.probeExecutor =
        new ThreadPoolExecutor(
            probeConcurrency,
            probeConcurrency,
            30,
            TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(),
            runnable -> daemonThread(runnable, "alternator-health-probe-"));
    this.probeExecutor.allowCoreThreadTimeOut(true);
    this.timeoutExecutor =
        new ScheduledThreadPoolExecutor(
            1, runnable -> daemonThread(runnable, "alternator-health-probe-timeout-"));
    this.timeoutExecutor.setKeepAliveTime(30, TimeUnit.SECONDS);
    this.timeoutExecutor.allowCoreThreadTimeOut(true);
    this.timeoutExecutor.setRemoveOnCancelPolicy(true);

    this.healthStore = new NodeHealthStore(this.config, Collections.<URI>emptyList());
    if (initialNodes != null) {
      for (URI initialNode : initialNodes) {
        this.healthStore.addQuarantinedNode(initialNode);
      }
    }
  }

  void publishDiscoveredNodes(List<URI> nodes, Consumer<List<URI>> publisher) {
    if (nodes == null || publisher == null) {
      throw new IllegalArgumentException("nodes and publisher cannot be null");
    }
    synchronized (topologyHealthLock) {
      for (URI node : nodes) {
        healthStore.addQuarantinedNode(node);
      }
      publisher.accept(nodes);
    }
  }

  NodeHealthStatus getNodeStatus(URI node) {
    return healthStore.getNodeStatus(node);
  }

  long getNodeGeneration(URI node) {
    NodeHealthStatus status = healthStore.getNodeStatus(node);
    return status != null ? status.getGeneration() : 0;
  }

  NodeHealthState getNodeState(URI node) {
    NodeHealthStatus status = healthStore.getNodeStatus(node);
    return status != null ? status.getState() : NodeHealthState.ACTIVE;
  }

  void reportNodeResult(URI node, NodeHealthObservation observation) {
    healthStore.reportNodeResult(node, observation);
    updateBackgroundProbeSuppression(node, observation);
  }

  void reportNodeResult(
      URI node, NodeHealthObservation observation, long expectedTrafficGeneration) {
    if (healthStore.reportNodeResult(node, observation, expectedTrafficGeneration)) {
      updateBackgroundProbeSuppression(node, observation);
    }
  }

  List<URI> runDownNodeProbes(List<URI> candidateNodes) {
    List<URI> candidates = copyCandidates(candidateNodes);
    List<CompletableFuture<ProbeOutcome>> futures = new ArrayList<>();
    for (URI node : candidates) {
      futures.add(submitProbe(node, ProbePriority.DOWN, true));
    }
    awaitProbeFutures(futures);
    List<URI> recovered = new ArrayList<>();
    for (URI node : candidates) {
      NodeHealthStatus status = healthStore.getNodeStatus(node);
      if (status != null && status.getState() == NodeHealthState.QUARANTINED) {
        recovered.add(node);
      }
    }
    return recovered;
  }

  void scheduleBackgroundProbes(List<URI> downNodes, List<URI> quarantinedNodes) {
    List<URI> down = copyCandidates(downNodes);
    List<URI> quarantined = copyCandidates(quarantinedNodes);
    int available = probeCapacity.availablePermits();
    Set<URI> scheduledThisCycle = new HashSet<>();

    int quarantineBudget;
    if (down.isEmpty() || quarantined.isEmpty()) {
      quarantineBudget = down.isEmpty() ? available : 0;
    } else if (available == 1) {
      quarantineBudget = Math.floorMod(backgroundSingleSlotTier.getAndIncrement(), 2);
    } else {
      quarantineBudget = available / 2;
    }
    int downBudget = available - quarantineBudget;

    submitBackgroundProbeBatch(
        down, ProbePriority.DOWN, downBudget, nextDownBackgroundProbeIndex, scheduledThisCycle);
    submitBackgroundProbeBatch(
        quarantined,
        ProbePriority.QUARANTINED,
        quarantineBudget,
        nextQuarantineBackgroundProbeIndex,
        scheduledThisCycle);

    // Reuse budget left because one tier was smaller than its share. Executor priority still
    // ensures admitted down-node work runs before queued quarantine work.
    submitBackgroundProbeBatch(
        down,
        ProbePriority.DOWN,
        probeCapacity.availablePermits(),
        nextDownBackgroundProbeIndex,
        scheduledThisCycle);
    submitBackgroundProbeBatch(
        quarantined,
        ProbePriority.QUARANTINED,
        probeCapacity.availablePermits(),
        nextQuarantineBackgroundProbeIndex,
        scheduledThisCycle);
  }

  private void submitBackgroundProbeBatch(
      List<URI> candidates,
      ProbePriority priority,
      int admissionBudget,
      AtomicInteger nextIndex,
      Set<URI> scheduledThisCycle) {
    if (candidates.isEmpty() || admissionBudget <= 0) {
      return;
    }

    int start = Math.floorMod(nextIndex.get(), candidates.size());
    int examined = 0;
    int submissions = 0;
    while (examined < candidates.size() && submissions < admissionBudget) {
      URI node = candidates.get((start + examined) % candidates.size());
      examined++;
      URI key = NodeHealthStore.canonicalNodeKey(node);
      if (key == null || !scheduledThisCycle.add(key)) {
        continue;
      }
      if (inFlightProbes.containsKey(key)) {
        continue;
      }
      submitProbe(node, priority, false);
      submissions++;
    }
    nextIndex.addAndGet(examined);
  }

  List<URI> probeQuarantinedNodes(List<URI> candidateNodes) {
    if (isProbeWorkerThread()) {
      throw new IllegalStateException("blocking probe API cannot run on a health-probe worker");
    }
    try {
      return probeQuarantinedNodesAsync(candidateNodes).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof IllegalStateException) {
        throw (IllegalStateException) e.getCause();
      }
      throw e;
    }
  }

  CompletableFuture<List<URI>> probeQuarantinedNodesAsync(List<URI> candidateNodes) {
    if (shutdownRequested.get()) {
      return failedFuture(new IllegalStateException("live-node manager is shut down"));
    }
    List<URI> candidates = copyCandidates(candidateNodes);
    List<CompletableFuture<ProbeOutcome>> futures = new ArrayList<>();
    for (URI node : candidates) {
      futures.add(submitProbe(node, ProbePriority.EXPLICIT, true));
    }
    CompletableFuture<Void> completed =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    return completed.thenApply(
        ignored -> {
          List<URI> successful = new ArrayList<>();
          for (int i = 0; i < candidates.size(); i++) {
            if (futures.get(i).join() == ProbeOutcome.SUCCESS) {
              successful.add(candidates.get(i));
            }
          }
          return Collections.unmodifiableList(successful);
        });
  }

  private CompletableFuture<ProbeOutcome> submitProbe(
      URI node, ProbePriority priority, boolean explicit) {
    if (shutdownRequested.get()) {
      return failedFuture(new IllegalStateException("live-node manager is shut down"));
    }
    URI key = NodeHealthStore.canonicalNodeKey(node);
    if (key == null) {
      return CompletableFuture.completedFuture(ProbeOutcome.SKIPPED);
    }

    while (true) {
      ProbeJob existing = inFlightProbes.get(key);
      if (existing != null) {
        return joinProbe(node, priority, explicit, existing);
      }
      if (!probeCapacity.tryAcquire()) {
        return explicit
            ? failedFuture(new RejectedExecutionException("health-probe queue is full"))
            : CompletableFuture.completedFuture(ProbeOutcome.SKIPPED);
      }

      ProbeJob created = new ProbeJob(node, key, priority, explicit);
      existing = inFlightProbes.putIfAbsent(key, created);
      if (existing != null) {
        probeCapacity.release();
        return joinProbe(node, priority, explicit, existing);
      }
      if (shutdownRequested.get()) {
        created.cancelForShutdown();
        return created.result;
      }
      try {
        probeExecutor.execute(created);
      } catch (RejectedExecutionException e) {
        inFlightProbes.remove(key, created);
        created.releaseCapacity();
        created.result.completeExceptionally(e);
      }
      return created.result;
    }
  }

  private CompletableFuture<ProbeOutcome> joinProbe(
      URI node, ProbePriority priority, boolean explicit, ProbeJob existing) {
    if (!explicit) {
      return existing.result;
    }
    boolean completedBeforeJoin = existing.isCompleted();
    existing.requestExplicit();
    if (completedBeforeJoin) {
      // Result completion precedes physical cleanup so timeouts are observable even when a
      // transport is slow to honor abortion. A later explicit caller must not reuse that completed
      // result: wait for the old job to leave the in-flight map, then submit fresh work.
      return existing.physicalCompletion.thenCompose(ignored -> submitProbe(node, priority, true));
    }
    // The background worker may already have committed to skipping just before the upgrade. Wait
    // for it to leave the in-flight map before retrying so an explicit caller gets an actual probe
    // while the node remains quarantined.
    return existing.result.thenCompose(
        outcome ->
            outcome == ProbeOutcome.SKIPPED
                ? existing.physicalCompletion.thenCompose(
                    ignored -> submitProbe(node, priority, true))
                : CompletableFuture.completedFuture(outcome));
  }

  private void awaitProbeFutures(List<CompletableFuture<ProbeOutcome>> futures) {
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    } catch (CompletionException | CancellationException e) {
      if (!shutdownRequested.get()) {
        throw e;
      }
    }
  }

  private boolean shouldRunProbe(ProbeJob job) {
    NodeHealthStatus status = healthStore.getNodeStatus(job.node);
    if (status == null || status.getState() == NodeHealthState.ACTIVE) {
      return false;
    }
    if (status.getState() == NodeHealthState.QUARANTINED) {
      synchronized (topologyHealthLock) {
        status = healthStore.getNodeStatus(job.node);
        if (status == null
            || status.getState() != NodeHealthState.QUARANTINED
            || !isCurrentlyDiscoveredLocked(job.key)) {
          return false;
        }
        if (!job.isExplicit() && skipNextBackgroundQuarantineProbe.remove(job.key)) {
          return false;
        }
      }
    }
    return true;
  }

  private void applyProbeObservation(URI node, NodeHealthObservation observation) {
    NodeHealthStatus status = healthStore.getNodeStatus(node);
    if (status == null || status.getState() == NodeHealthState.ACTIVE) {
      return;
    }
    if (status.getState() == NodeHealthState.QUARANTINED) {
      URI key = NodeHealthStore.canonicalNodeKey(node);
      synchronized (topologyHealthLock) {
        status = healthStore.getNodeStatus(node);
        if (status != null
            && status.getState() == NodeHealthState.QUARANTINED
            && isCurrentlyDiscoveredLocked(key)) {
          reportNodeResult(node, observation);
        }
      }
      return;
    }
    reportNodeResult(node, observation);
  }

  private boolean isCurrentlyDiscoveredLocked(URI key) {
    List<URI> currentNodes = discoveredNodes.get();
    if (currentNodes == null) {
      return false;
    }
    for (URI discovered : currentNodes) {
      if (Objects.equals(key, NodeHealthStore.canonicalNodeKey(discovered))) {
        return true;
      }
    }
    return false;
  }

  private void updateBackgroundProbeSuppression(URI node, NodeHealthObservation observation) {
    URI key = NodeHealthStore.canonicalNodeKey(node);
    if (key == null) {
      return;
    }
    NodeHealthStatus status = healthStore.getNodeStatus(node);
    if (observation == NodeHealthObservation.TRAFFIC_SUCCESS
        && status != null
        && status.getState() == NodeHealthState.QUARANTINED) {
      ProbeJob job = inFlightProbes.get(key);
      if (job == null || !job.isRunning()) {
        skipNextBackgroundQuarantineProbe.add(key);
      }
      return;
    }
    if (observation == NodeHealthObservation.TRAFFIC_FAILURE
        || status == null
        || status.getState() != NodeHealthState.QUARANTINED) {
      skipNextBackgroundQuarantineProbe.remove(key);
    }
  }

  void shutdown() {
    shutdownRequested.set(true);
    if (!shutdownStarted.compareAndSet(false, true)) {
      return;
    }
    for (ProbeJob job : inFlightProbes.values()) {
      job.cancelForShutdown();
    }
    probeExecutor.shutdownNow();
    timeoutExecutor.shutdownNow();
  }

  boolean isProbeWorkerThread() {
    return Boolean.TRUE.equals(PROBE_WORKER.get());
  }

  boolean isTerminated() {
    return probeExecutor.isTerminated() && timeoutExecutor.isTerminated();
  }

  boolean awaitTermination(long deadlineNanos) throws InterruptedException {
    boolean probeStopped =
        probeExecutor.awaitTermination(remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS);
    boolean timeoutStopped =
        timeoutExecutor.awaitTermination(remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS);
    return probeStopped && timeoutStopped;
  }

  private static List<URI> copyCandidates(List<URI> candidates) {
    return candidates != null ? new ArrayList<>(candidates) : Collections.emptyList();
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable failure) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(failure);
    return future;
  }

  private static Thread daemonThread(Runnable runnable, String prefix) {
    Thread thread = new Thread(runnable, prefix + PROBE_THREAD_ID.incrementAndGet());
    thread.setDaemon(true);
    return thread;
  }

  private static long remainingMillis(long deadlineNanos) {
    long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      return 0;
    }
    return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
  }

  private static void abortQuietly(ExecutableHttpRequest request) {
    try {
      request.abort();
    } catch (RuntimeException ignored) {
      // Best-effort shutdown and timeout cancellation.
    }
  }

  @FunctionalInterface
  interface ProbeTransport {
    int execute(URI node, ProbeRequest request) throws IOException, URISyntaxException;
  }

  interface ProbeRequest {
    void setRequest(ExecutableHttpRequest request);

    void clearRequest(ExecutableHttpRequest request);
  }

  private enum ProbeOutcome {
    SUCCESS,
    FAILURE,
    SKIPPED
  }

  private enum ProbePriority {
    EXPLICIT(0),
    DOWN(1),
    QUARANTINED(2);

    private final int value;

    ProbePriority(int value) {
      this.value = value;
    }
  }

  private final class ProbeJob implements Runnable, Comparable<ProbeJob>, ProbeRequest {
    private final URI node;
    private final URI key;
    private final long sequence = probeSequence.incrementAndGet();
    private final CompletableFuture<ProbeOutcome> result = new CompletableFuture<>();
    private final CompletableFuture<Void> physicalCompletion = new CompletableFuture<>();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicBoolean capacityReleased = new AtomicBoolean();
    private volatile ProbePriority priority;
    private volatile boolean explicit;
    private volatile boolean running;
    private volatile ExecutableHttpRequest request;
    private volatile ScheduledFuture<?> timeoutTask;

    private ProbeJob(URI node, URI key, ProbePriority priority, boolean explicit) {
      this.node = node;
      this.key = key;
      this.priority = priority;
      this.explicit = explicit;
    }

    @Override
    public int compareTo(ProbeJob other) {
      int byPriority = Integer.compare(priority.value, other.priority.value);
      return byPriority != 0 ? byPriority : Long.compare(sequence, other.sequence);
    }

    private void requestExplicit() {
      boolean removed = false;
      synchronized (this) {
        if (completed.get()) {
          return;
        }
        if (!running && priority != ProbePriority.EXPLICIT) {
          removed = probeExecutor.remove(this);
        }
        explicit = true;
        priority = ProbePriority.EXPLICIT;
      }
      if (removed) {
        try {
          probeExecutor.execute(this);
        } catch (RejectedExecutionException e) {
          cancelForShutdown();
        }
      }
    }

    private boolean isExplicit() {
      return explicit;
    }

    private boolean isRunning() {
      return running;
    }

    private boolean isCompleted() {
      return completed.get();
    }

    @Override
    public void run() {
      PROBE_WORKER.set(Boolean.TRUE);
      synchronized (this) {
        if (completed.get()) {
          cleanupAfterPhysicalCompletion();
          PROBE_WORKER.remove();
          return;
        }
        running = true;
      }
      try {
        if (shutdownRequested.get()) {
          cancelForShutdown();
          return;
        }
        if (!shouldRunProbe(this)) {
          complete(ProbeOutcome.SKIPPED, null);
          return;
        }
        timeoutTask =
            timeoutExecutor.schedule(
                this::timeout, config.getHealthProbeTimeoutMs(), TimeUnit.MILLISECONDS);
        int statusCode = probeTransport.execute(node, this);
        if (statusCode == HttpURLConnection.HTTP_OK) {
          complete(ProbeOutcome.SUCCESS, NodeHealthObservation.PROBE_SUCCESS);
        } else {
          complete(ProbeOutcome.FAILURE, NodeHealthObservation.PROBE_FAILURE);
        }
      } catch (IOException | RuntimeException | URISyntaxException e) {
        if (shutdownRequested.get()) {
          cancelForShutdown();
        } else {
          complete(ProbeOutcome.FAILURE, NodeHealthObservation.PROBE_FAILURE);
        }
      } finally {
        ScheduledFuture<?> timeout = timeoutTask;
        if (timeout != null) {
          timeout.cancel(false);
        }
        cleanupAfterPhysicalCompletion();
        PROBE_WORKER.remove();
      }
    }

    private void timeout() {
      complete(ProbeOutcome.FAILURE, NodeHealthObservation.PROBE_FAILURE);
      ExecutableHttpRequest current = request;
      if (current != null) {
        abortQuietly(current);
      }
    }

    @Override
    public void setRequest(ExecutableHttpRequest request) {
      this.request = request;
      if (completed.get() || shutdownRequested.get()) {
        abortQuietly(request);
      }
    }

    @Override
    public void clearRequest(ExecutableHttpRequest request) {
      if (this.request == request) {
        this.request = null;
      }
    }

    private void complete(ProbeOutcome outcome, NodeHealthObservation observation) {
      if (!completed.compareAndSet(false, true)) {
        return;
      }
      if (observation != null) {
        applyProbeObservation(node, observation);
      }
      result.complete(outcome);
    }

    private void cancelForShutdown() {
      if (completed.compareAndSet(false, true)) {
        result.completeExceptionally(
            new CancellationException("health probe cancelled by shutdown"));
      }
      ScheduledFuture<?> timeout = timeoutTask;
      if (timeout != null) {
        timeout.cancel(false);
      }
      ExecutableHttpRequest current = request;
      if (current != null) {
        abortQuietly(current);
      }
      synchronized (this) {
        if (!running) {
          inFlightProbes.remove(key, this);
          releaseCapacity();
          physicalCompletion.complete(null);
        }
      }
    }

    private void cleanupAfterPhysicalCompletion() {
      running = false;
      inFlightProbes.remove(key, this);
      releaseCapacity();
      physicalCompletion.complete(null);
    }

    private void releaseCapacity() {
      if (capacityReleased.compareAndSet(false, true)) {
        probeCapacity.release();
      }
    }
  }
}
