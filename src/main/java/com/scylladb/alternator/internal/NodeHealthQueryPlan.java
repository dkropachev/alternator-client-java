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

import com.scylladb.alternator.NodeHealthState;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/** Request-scoped active-first endpoint selector around a {@link LazyQueryPlan}. */
public final class NodeHealthQueryPlan {
  /** Source-plan role controlling traffic-cycle reset and ordering behavior. */
  public enum Mode {
    /** Non-deterministic DynamoDB traffic order. */
    REGULAR,
    /** Deterministic affinity DynamoDB traffic order. */
    AFFINITY,
    /** Control-plane candidate order. */
    PROBE
  }

  private final AlternatorLiveNodes liveNodes;
  private final LazyQueryPlan delegate;
  private final Mode mode;
  private final List<URI> candidates = new ArrayList<>();
  private final Set<URI> tried = new HashSet<>();
  private boolean initialized;

  NodeHealthQueryPlan(AlternatorLiveNodes liveNodes, LazyQueryPlan delegate, Mode mode) {
    if (liveNodes == null || delegate == null || mode == null) {
      throw new IllegalArgumentException("liveNodes, delegate, and mode cannot be null");
    }
    this.liveNodes = liveNodes;
    this.delegate = delegate;
    this.mode = mode;
  }

  /**
   * Returns the next active candidate, then the next quarantined candidate after active candidates
   * are exhausted.
   *
   * <p>The source order is captured lazily and preserved within each health pass. Health is checked
   * when each candidate is considered, down nodes are never returned, and one endpoint is returned
   * at most once per traffic cycle. Traffic plans start another cycle when every eligible endpoint
   * has been tried; regular plans reshuffle while affinity plans retain their deterministic order.
   * Probe plans stop after one traversal.
   *
   * @return the next candidate, or null when no eligible candidate exists
   */
  public synchronized URI nextRouteCandidate() {
    ensureInitialized();
    URI candidate = nextUntriedCandidate();
    if (candidate != null || mode == Mode.PROBE) {
      return candidate;
    }

    tried.clear();
    if (mode == Mode.REGULAR) {
      Collections.shuffle(candidates, ThreadLocalRandom.current());
    }
    return nextUntriedCandidate();
  }

  /**
   * Returns the next plan candidate without using the legacy request-endpoint fallback.
   *
   * @param fallbackEndpoint ignored; retained for source compatibility
   * @return the next candidate, or null when no eligible candidate exists
   * @deprecated Use {@link #nextRouteCandidate()}.
   */
  @Deprecated
  public URI nextRouteCandidate(URI fallbackEndpoint) {
    return nextRouteCandidate();
  }

  private void ensureInitialized() {
    if (initialized) {
      return;
    }
    Set<URI> seen = new HashSet<>();
    while (delegate.hasNext()) {
      URI candidate = delegate.next();
      URI key = NodeHealthStore.canonicalNodeKey(candidate);
      if (candidate != null && seen.add(key)) {
        candidates.add(candidate);
      }
    }
    initialized = true;
  }

  private URI nextUntriedCandidate() {
    URI candidate = firstUntriedCandidateInState(NodeHealthState.ACTIVE);
    return candidate != null
        ? candidate
        : firstUntriedCandidateInState(NodeHealthState.QUARANTINED);
  }

  private URI firstUntriedCandidateInState(NodeHealthState expectedState) {
    for (URI candidate : candidates) {
      URI key = NodeHealthStore.canonicalNodeKey(candidate);
      if (!tried.contains(key) && liveNodes.getQueryPlanNodeState(candidate) == expectedState) {
        tried.add(key);
        return candidate;
      }
    }
    return null;
  }
}
