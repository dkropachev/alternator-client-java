package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetricLabels;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetrics;
import com.scylladb.alternator.keyrouting.PartitionKeyResolver;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Execution interceptor that implements key-based route affinity.
 *
 * <p>This interceptor extends {@link BasicQueryPlanInterceptor} to provide deterministic routing
 * based on partition key values. When key affinity conditions are met, it creates a {@link
 * LazyQueryPlan} with a seed derived from the partition key hash, ensuring that requests for the
 * same partition key are routed to the same node.
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
  private static final Logger logger =
      Logger.getLogger(AffinityQueryPlanInterceptor.class.getName());
  private static final ExecutionAttribute<RoutingDecision> ROUTING_DECISION =
      new ExecutionAttribute<>("AffinityQueryPlanInterceptor.routingDecision");

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private final KeyRouteAffinityMetrics metrics;
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
    super(liveNodes);
    this.config = config;
    this.metrics = config.getMetrics();
    this.pkResolver = new PartitionKeyResolver(config.getPkInfoPerTable(), metrics);
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
    this(config, liveNodes, null);
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

  private RoutingDecision evaluateRequest(SdkRequest request) {
    String tableName = KeyAffinityRequestClassifier.extractTableName(request);
    String metricTableName = metricTableName(tableName);

    if (!config.isEnabled()) {
      return RoutingDecision.skip(
          tableName, KeyRouteAffinityMetricLabels.NOT_QUALIFYING_REQUEST, metricTableName);
    }

    emitRequest(metricTableName);

    // Check if this request qualifies for key affinity
    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      return RoutingDecision.skip(
          tableName, KeyRouteAffinityMetricLabels.NOT_QUALIFYING_REQUEST, metricTableName);
    }

    if (tableName == null) {
      return RoutingDecision.skip(
          null, KeyRouteAffinityMetricLabels.TABLE_NAME_MISSING, metricTableName);
    }

    // Get partition key name
    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      emitPartitionKeyCacheMiss(metricTableName);
      // Trigger async discovery if we have a client
      if (clientForDiscovery != null) {
        pkResolver.triggerDiscovery(tableName, clientForDiscovery);
      }
      return RoutingDecision.skip(
          tableName, KeyRouteAffinityMetricLabels.PK_NOT_CACHED, metricTableName);
    }
    emitPartitionKeyCacheHit(metricTableName);

    // Extract partition key value
    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      return RoutingDecision.skip(
          tableName, KeyRouteAffinityMetricLabels.PK_VALUE_MISSING, metricTableName);
    }

    // Hash the partition key and create a deterministic query plan
    long hash = AttributeValueHasher.hash(pkValue);
    return RoutingDecision.affinity(tableName, metricTableName, pkValue, hash);
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    RoutingDecision decision = evaluateRequest(context.request());
    executionAttributes.putAttribute(ROUTING_DECISION, decision);

    LazyQueryPlan plan;
    if (decision.affinityApplied) {
      emitAffinityApplied(decision.metricTableName);
      plan = new LazyQueryPlan(liveNodes, decision.hash);
    } else {
      emitAffinitySkipped(decision.metricTableName, decision.reason);
      logAffinitySkipped(decision);
      plan = new LazyQueryPlan(liveNodes);
    }

    // Override the random plan with the deterministic one
    executionAttributes.putAttribute(QUERY_PLAN, plan);
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    if (plan == null || !plan.hasNext()) {
      return context.httpRequest();
    }

    URI targetUri = plan.next();
    RoutingDecision decision = executionAttributes.getAttribute(ROUTING_DECISION);
    if (decision != null && decision.affinityApplied && logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "Key affinity applied: table={0}, pk_fingerprint={1}, target={2}",
          new Object[] {
            decision.metricTableName, formatPartitionKeyFingerprint(decision), targetUri
          });
    }

    SdkHttpRequest originalRequest = context.httpRequest();
    return originalRequest.toBuilder()
        .protocol(targetUri.getScheme())
        .host(targetUri.getHost())
        .port(targetUri.getPort())
        .putHeader("Connection", "keep-alive")
        .build();
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

  private void emitRequest(String metricTableName) {
    if (metrics != null) {
      metrics.onRequest(metricTableName, config.getType());
    }
  }

  private void emitAffinityApplied(String metricTableName) {
    if (metrics != null) {
      metrics.onAffinityApplied(metricTableName, config.getType());
    }
  }

  private void emitAffinitySkipped(String metricTableName, String reason) {
    if (metrics != null) {
      metrics.onAffinitySkipped(metricTableName, config.getType(), reason);
    }
  }

  private void emitPartitionKeyCacheHit(String metricTableName) {
    if (metrics != null) {
      metrics.onPartitionKeyCacheHit(metricTableName);
    }
  }

  private void emitPartitionKeyCacheMiss(String metricTableName) {
    if (metrics != null) {
      metrics.onPartitionKeyCacheMiss(metricTableName);
    }
  }

  private void logAffinitySkipped(RoutingDecision decision) {
    if (logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "Key affinity skipped: table={0}, reason={1}",
          new Object[] {decision.metricTableName, decision.reason});
    }
  }

  private String metricTableName(String tableName) {
    return tableName != null ? tableName : KeyRouteAffinityMetricLabels.UNKNOWN_TABLE;
  }

  private String formatPartitionKeyFingerprint(RoutingDecision decision) {
    if (decision.partitionKeyValue == null) {
      return "null";
    }
    return Long.toUnsignedString(decision.hash, 16);
  }

  private static final class RoutingDecision {
    private final boolean affinityApplied;
    private final String tableName;
    private final String metricTableName;
    private final String reason;
    private final AttributeValue partitionKeyValue;
    private final long hash;

    private RoutingDecision(
        boolean affinityApplied,
        String tableName,
        String metricTableName,
        String reason,
        AttributeValue partitionKeyValue,
        long hash) {
      this.affinityApplied = affinityApplied;
      this.tableName = tableName;
      this.metricTableName = metricTableName;
      this.reason = reason;
      this.partitionKeyValue = partitionKeyValue;
      this.hash = hash;
    }

    private static RoutingDecision affinity(
        String tableName, String metricTableName, AttributeValue partitionKeyValue, long hash) {
      return new RoutingDecision(true, tableName, metricTableName, null, partitionKeyValue, hash);
    }

    private static RoutingDecision skip(String tableName, String reason, String metricTableName) {
      return new RoutingDecision(false, tableName, metricTableName, reason, null, 0L);
    }
  }
}
