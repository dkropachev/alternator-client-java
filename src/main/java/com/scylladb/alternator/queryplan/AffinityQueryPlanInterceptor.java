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
package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier.BatchWriteRoutingTarget;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.PartitionKeyResolver;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;

/**
 * Execution interceptor that implements key-based route affinity.
 *
 * <p>This interceptor extends {@link BasicQueryPlanInterceptor} to provide deterministic
 * discovered-node candidate order based on partition key values. When key affinity conditions are
 * met, it creates a {@link LazyQueryPlan} with a seed derived from the partition key hash. Final
 * node-health eligibility is applied by {@link BasicQueryPlanInterceptor} as each request attempt
 * is routed.
 *
 * <p>When key affinity conditions are not met (e.g., request type doesn't qualify, partition key
 * not found), the interceptor falls back to the random plan created by the base class.
 *
 * <p>The interceptor is automatically configured when key route affinity is enabled via {@link
 * com.scylladb.alternator.AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder#withKeyRouteAffinity}.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AffinityQueryPlanInterceptor extends BasicQueryPlanInterceptor {

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private volatile DynamoDbClient clientForDiscovery;

  /**
   * Creates a new interceptor with the given configuration and live nodes.
   *
   * @param config the key route affinity configuration
   * @param liveNodes the live nodes manager
   * @param clientForDiscovery the DynamoDB client to use for PK discovery (may be null)
   */
  public AffinityQueryPlanInterceptor(
      KeyRouteAffinityConfig config,
      AlternatorLiveNodes liveNodes,
      DynamoDbClient clientForDiscovery) {
    this(config, liveNodes, clientForDiscovery, null);
  }

  /**
   * Creates an interceptor with optional metadata discovery and a configured legacy signer.
   *
   * @param config key route affinity configuration
   * @param liveNodes live nodes manager
   * @param clientForDiscovery DynamoDB client for metadata discovery, or null
   * @param configuredClientSigner client-level legacy signer override, or null
   */
  public AffinityQueryPlanInterceptor(
      KeyRouteAffinityConfig config,
      AlternatorLiveNodes liveNodes,
      DynamoDbClient clientForDiscovery,
      Signer configuredClientSigner) {
    super(liveNodes, configuredClientSigner);
    this.config = config;
    this.pkResolver = new PartitionKeyResolver(config.getPkInfoPerTable());
    this.clientForDiscovery = clientForDiscovery;
  }

  /**
   * Creates a new interceptor without auto-discovery support.
   *
   * <p>Note: To enable auto-discovery, call {@link #setClientForDiscovery(DynamoDbClient)} after
   * the client is built.
   *
   * @param config the key route affinity configuration
   * @param liveNodes the live nodes manager
   */
  public AffinityQueryPlanInterceptor(
      KeyRouteAffinityConfig config, AlternatorLiveNodes liveNodes) {
    this(config, liveNodes, null, null);
  }

  /**
   * Sets the DynamoDB client used for partition key auto-discovery.
   *
   * <p>This method should be called after the client is built to enable auto-discovery of partition
   * key names for tables not pre-configured via {@link
   * KeyRouteAffinityConfig.Builder#withPkInfo(String, String)}.
   *
   * <p>Thread-safe: This method can be called from any thread after construction.
   *
   * @param client the DynamoDB client to use for DescribeTable calls
   */
  public void setClientForDiscovery(DynamoDbClient client) {
    this.clientForDiscovery = client;
  }

  private LazyQueryPlan getQueryPlan(SdkRequest request) {
    if (!config.isEnabled()) {
      return null;
    }

    // Check if this request qualifies for key affinity
    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      // Keep the random plan from base class
      return null;
    }

    if (request instanceof BatchWriteItemRequest) {
      return getBatchWriteQueryPlan((BatchWriteItemRequest) request);
    }

    // Extract table name
    String tableName = KeyAffinityRequestClassifier.extractTableName(request);
    if (tableName == null) {
      // Keep the random plan from base class
      return null;
    }

    // Get partition key name
    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      // Trigger async discovery if we have a client
      if (clientForDiscovery != null) {
        pkResolver.triggerDiscovery(tableName, clientForDiscovery);
      }
      // Keep the random plan from base class for this request
      return null;
    }

    // Extract partition key value
    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      // Keep the random plan from base class
      return null;
    }

    // Hash the partition key and create a deterministic known-node candidate order.
    try {
      long hash = AttributeValueHasher.hash(pkValue);
      return new LazyQueryPlan(liveNodes, hash);
    } catch (IllegalArgumentException e) {
      // Unsupported partition-key shapes cannot be routed by key affinity.
      return null;
    }
  }

  private LazyQueryPlan getBatchWriteQueryPlan(BatchWriteItemRequest request) {
    Map<URI, Integer> votes = new HashMap<>();
    for (BatchWriteRoutingTarget target :
        KeyAffinityRequestClassifier.extractBatchWriteRoutingTargets(request)) {
      String tableName = target.tableName();
      String pkName = pkResolver.getPartitionKeyName(tableName);
      if (pkName == null) {
        if (clientForDiscovery != null) {
          pkResolver.triggerDiscovery(tableName, clientForDiscovery);
        }
        continue;
      }

      AttributeValue pkValue = target.partitionKeyValue(pkName);
      if (pkValue == null) {
        continue;
      }

      try {
        long hash = AttributeValueHasher.hash(pkValue);
        URI preferredNode = liveNodes.getPreferredQueryPlanNodeForHash(hash);
        if (preferredNode != null) {
          votes.merge(preferredNode, 1, Integer::sum);
        }
      } catch (IllegalArgumentException e) {
        // Unsupported partition-key shapes cannot be routed by key affinity.
      }
    }

    List<URI> preferredNodes = votePreferenceOrder(votes);
    if (preferredNodes.isEmpty()) {
      return null;
    }
    return new LazyQueryPlan(liveNodes, preferredNodes);
  }

  private static List<URI> votePreferenceOrder(Map<URI, Integer> votes) {
    List<Map.Entry<URI, Integer>> votedNodes = new ArrayList<>(votes.entrySet());
    votedNodes.sort(
        Comparator.<Map.Entry<URI, Integer>, Integer>comparing(Map.Entry::getValue)
            .reversed()
            .thenComparing(entry -> entry.getKey().toString()));

    List<URI> preferredNodes = new ArrayList<>(votedNodes.size());
    for (Map.Entry<URI, Integer> entry : votedNodes) {
      preferredNodes.add(entry.getKey());
    }
    return preferredNodes;
  }

  @Override
  protected void initializeQueryPlan(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = getQueryPlan(context.request());
    boolean affinity = plan != null;
    if (plan == null) {
      plan = new LazyQueryPlan(liveNodes);
    }
    setQueryPlan(executionAttributes, plan, affinity);
  }

  /**
   * Returns the partition key resolver used by this interceptor.
   *
   * @return the partition key resolver
   */
  public PartitionKeyResolver getPartitionKeyResolver() {
    return pkResolver;
  }

  /**
   * Returns the key route affinity configuration.
   *
   * @return the configuration
   */
  public KeyRouteAffinityConfig getConfig() {
    return config;
  }
}
