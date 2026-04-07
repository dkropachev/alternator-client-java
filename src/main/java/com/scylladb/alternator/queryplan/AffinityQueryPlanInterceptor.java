package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AffinityMetricsCallback;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.PartitionKeyResolver;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
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

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private final boolean metricsEnabled;
  private final boolean debugLoggingEnabled;
  private final AffinityMetricsCallback metricsCallback;
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
    this.pkResolver = new PartitionKeyResolver(config.getPkInfoPerTable());
    this.pkResolver.configureObservability(config);
    this.metricsEnabled = config.isMetricsEnabled();
    this.debugLoggingEnabled = config.isDebugLoggingEnabled();
    this.metricsCallback =
        config.getMetricsCallback() != null
            ? config.getMetricsCallback()
            : AffinityMetricsCallback.NO_OP;
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
   * @param client the DynamoDB client to use for DescribeTable calls
   */
  public void setClientForDiscovery(DynamoDbClient client) {
    this.clientForDiscovery = client;
  }

  private LazyQueryPlan getQueryPlan(SdkRequest request) {
    String tableName = KeyAffinityRequestClassifier.extractTableName(request);
    recordRequestObserved(tableName);

    if (!config.isEnabled()) {
      return null;
    }

    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      recordRoundRobinFallback(tableName, "request_not_qualifying");
      return null;
    }

    if (tableName == null) {
      recordRoundRobinFallback(null, "table_name_unavailable");
      return null;
    }

    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      if (clientForDiscovery != null) {
        pkResolver.triggerDiscovery(tableName, clientForDiscovery);
      }
      recordRoundRobinFallback(tableName, "pk_not_cached");
      return null;
    }

    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      recordRoundRobinFallback(tableName, "pk_value_unavailable");
      return null;
    }

    long hash = AttributeValueHasher.hash(pkValue);
    URI targetNode = previewTargetNode(hash);
    recordAffinityApplied(tableName, pkValue, targetNode);
    return new LazyQueryPlan(liveNodes, hash);
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = getQueryPlan(context.request());
    if (plan == null) {
      plan = new LazyQueryPlan(liveNodes);
    }
    executionAttributes.putAttribute(QUERY_PLAN, plan);
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

  private void recordRequestObserved(String tableName) {
    if (metricsEnabled) {
      metricsCallback.onRequestObserved(tableName, config.getType());
    }
  }

  private void recordAffinityApplied(String tableName, AttributeValue pkValue, URI targetNode) {
    String pkValueText = formatPartitionKeyValue(pkValue);
    if (debugLoggingEnabled && logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "Key affinity applied: table={0}, pk={1}, target={2}, mode={3}",
          new Object[] {safeTableName(tableName), pkValueText, targetNode, config.getType()});
    }
    if (metricsEnabled) {
      metricsCallback.onAffinityApplied(tableName, pkValueText, targetNode, config.getType());
    }
  }

  private void recordRoundRobinFallback(String tableName, String reason) {
    if (debugLoggingEnabled && logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "Key affinity skipped: table={0}, reason={1}, mode={2}",
          new Object[] {safeTableName(tableName), reason, config.getType()});
    }
    if (metricsEnabled) {
      metricsCallback.onRoundRobinFallback(tableName, config.getType(), reason);
    }
  }

  private URI previewTargetNode(long hash) {
    LazyQueryPlan preview = new LazyQueryPlan(liveNodes, hash);
    if (!preview.hasNext()) {
      return null;
    }
    return preview.next();
  }

  private String formatPartitionKeyValue(AttributeValue pkValue) {
    if (pkValue == null) {
      return "<null>";
    }
    if (pkValue.s() != null) {
      return pkValue.s();
    }
    if (pkValue.n() != null) {
      return pkValue.n();
    }
    if (pkValue.bool() != null) {
      return String.valueOf(pkValue.bool());
    }
    return pkValue.toString();
  }

  private String safeTableName(String tableName) {
    return tableName != null ? tableName : "<unknown>";
  }
}
