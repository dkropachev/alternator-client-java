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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A lazy iterator over URI nodes that pulls nodes from {@link AlternatorLiveNodes} one at a time
 * only when needed.
 *
 * <p>Query plans build their candidate order from all known nodes supplied by {@link
 * AlternatorLiveNodes}. Final node-health eligibility and request outcome reporting are handled
 * outside this iterator by the query-plan interceptor.
 *
 * <p>When created with a seed, this implementation uses a Go-compatible Lagged Fibonacci Generator
 * ({@link GoRand}) and pick-and-remove selection to produce identical discovered-node candidate
 * sequences across all Alternator client implementations (Go, Java, etc.) for the same seed. This
 * cross-language compatibility is critical for key route affinity, where requests with the same
 * partition key hash must start from the same coordinator candidate regardless of client language.
 *
 * <p>When created without a seed, uses {@link ThreadLocalRandom} for non-deterministic load
 * balancing.
 *
 * <p>The pick-and-remove algorithm works by maintaining a mutable copy of the candidate list. For
 * random plans it picks candidates while advancing the iterator; for seeded plans it builds the
 * deterministic candidate order up front. Each candidate is returned at most once.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorLiveNodes liveNodes = ...;
 *
 * // Create with non-deterministic order for load balancing
 * LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
 *
 * // Or create with deterministic seed for key affinity
 * long partitionKeyHash = AttributeValueHasher.hash(pkValue);
 * LazyQueryPlan affinityPlan = new LazyQueryPlan(liveNodes, partitionKeyHash);
 *
 * // Get the first currently eligible node
 * if (plan.hasNext()) {
 *     URI node = plan.next();
 *     // Use node...
 * }
 * }</pre>
 *
 * <p><strong>Thread Safety:</strong> This class is NOT thread-safe. It is designed for single
 * request use only, where the same thread (or async context) iterates through nodes during retry
 * attempts. Do not share instances between concurrent requests.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class LazyQueryPlan implements Iterator<URI>, Iterable<URI> {
  private final AlternatorLiveNodes liveNodes;
  private final long seed;
  private final List<URI> preferredNodes;
  private final boolean seeded;

  /** Iterator over a lazily captured candidate snapshot. */
  private Iterator<URI> candidates;

  private boolean initialized;

  /**
   * Creates a LazyQueryPlan with non-deterministic candidate order, suitable for general load
   * balancing over discovered nodes.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.seed = 0;
    this.preferredNodes = null;
    this.seeded = false;
  }

  /**
   * Creates a LazyQueryPlan with a specified seed for deterministic iteration. Uses Go-compatible
   * PRNG and pick-and-remove selection over discovered nodes to produce identical candidate
   * sequences across all Alternator client implementations.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   * @param seed a long value used to initialize the Go-compatible PRNG
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes, long seed) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.seed = seed;
    this.preferredNodes = null;
    this.seeded = true;
  }

  /**
   * Creates a LazyQueryPlan that tries the given preferred nodes first, then the remaining
   * discovered nodes in canonical affinity order.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   * @param preferredNodes nodes to try first, in preference order
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes, List<URI> preferredNodes) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    if (preferredNodes == null) {
      throw new IllegalArgumentException("preferredNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.seed = 0;
    this.preferredNodes = new ArrayList<>(preferredNodes);
    this.seeded = false;
  }

  /**
   * Initializes the candidate list from current discovered nodes. Called lazily on first access so
   * the candidate snapshot is as fresh as possible.
   */
  private void ensureInitialized() {
    if (!initialized) {
      if (preferredNodes != null) {
        candidates = liveNodes.getQueryPlanNodesWithPreferredNodes(preferredNodes).iterator();
      } else if (seeded) {
        candidates = liveNodes.getQueryPlanNodesForHash(seed).iterator();
      } else {
        candidates = randomPlanCandidates().iterator();
      }
      initialized = true;
    }
  }

  private List<URI> randomPlanCandidates() {
    List<URI> queryPlanNodes = liveNodes.getQueryPlanNodes();
    List<URI> candidates = new ArrayList<>(queryPlanNodes);
    Collections.shuffle(candidates, ThreadLocalRandom.current());
    return candidates;
  }

  /**
   * Returns the current discovered nodes in the canonical order used by affinity routing.
   *
   * @param liveNodes the live nodes manager
   * @return sorted discovered nodes for affinity hashing
   * @deprecated Use {@link AlternatorLiveNodes#getQueryPlanNodes()} instead.
   */
  @Deprecated
  public static List<URI> sortedAffinityNodes(AlternatorLiveNodes liveNodes) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    return liveNodes.getQueryPlanNodes();
  }

  /**
   * Returns the first node selected by the seeded affinity plan without consuming a full query
   * plan.
   *
   * @param liveNodes the live nodes manager
   * @param seed deterministic seed
   * @return the preferred node, or null when no candidates exist
   * @deprecated Use {@link AlternatorLiveNodes#getPreferredQueryPlanNodeForHash(long)} instead.
   */
  @Deprecated
  public static URI preferredNodeForHash(AlternatorLiveNodes liveNodes, long seed) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    return liveNodes.getPreferredQueryPlanNodeForHash(seed);
  }

  @Override
  public boolean hasNext() {
    ensureInitialized();
    return candidates.hasNext();
  }

  @Override
  public URI next() {
    ensureInitialized();
    return candidates.next();
  }

  @Override
  public Iterator<URI> iterator() {
    return this;
  }
}
