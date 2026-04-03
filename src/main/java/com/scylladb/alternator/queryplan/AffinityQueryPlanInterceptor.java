package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityFallbackReason;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetricsCollector;
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
  private static final ExecutionAttribute<AffinityDecision> AFFINITY_DECISION =
      new ExecutionAttribute<>("AffinityQueryPlanInterceptor.affinityDecision");
  private static final int MAX_LOGGED_PK_LENGTH = 64;

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private final KeyRouteAffinityMetricsCollector metricsCollector;
  private final boolean metricsEnabled;
  private volatile DynamoDbClient clientForDiscovery;

  private static final class AffinityDecision {
    private final String tableName;
    private final String partitionKeyName;
    private final AttributeValue partitionKeyValue;
    private final long partitionKeyHash;
    private final KeyRouteAffinityFallbackReason fallbackReason;
    private final PartitionKeyResolver.DiscoveryTriggerResult discoveryTriggerResult;

    private AffinityDecision(
        String tableName,
        String partitionKeyName,
        AttributeValue partitionKeyValue,
        long partitionKeyHash,
        KeyRouteAffinityFallbackReason fallbackReason,
        PartitionKeyResolver.DiscoveryTriggerResult discoveryTriggerResult) {
      this.tableName = tableName;
      this.partitionKeyName = partitionKeyName;
      this.partitionKeyValue = partitionKeyValue;
      this.partitionKeyHash = partitionKeyHash;
      this.fallbackReason = fallbackReason;
      this.discoveryTriggerResult = discoveryTriggerResult;
    }

    static AffinityDecision applied(
        String tableName, String partitionKeyName, AttributeValue partitionKeyValue, long hash) {
      return new AffinityDecision(tableName, partitionKeyName, partitionKeyValue, hash, null, null);
    }

    static AffinityDecision fallback(String tableName, KeyRouteAffinityFallbackReason reason) {
      return new AffinityDecision(tableName, null, null, 0L, reason, null);
    }

    static AffinityDecision fallback(
        String tableName,
        KeyRouteAffinityFallbackReason reason,
        PartitionKeyResolver.DiscoveryTriggerResult discoveryTriggerResult) {
      return new AffinityDecision(tableName, null, null, 0L, reason, discoveryTriggerResult);
    }

    boolean isAffinityApplied() {
      return fallbackReason == null;
    }
  }

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
    this.metricsCollector = config.getMetricsCollector();
    this.metricsEnabled = metricsCollector != KeyRouteAffinityMetricsCollector.NO_OP;
    this.pkResolver = new PartitionKeyResolver(config.getPkInfoPerTable(), metricsCollector);
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

  private AffinityDecision evaluateDecision(SdkRequest request) {
    String tableName = KeyAffinityRequestClassifier.extractTableName(request);

    if (!config.isEnabled()) {
      return AffinityDecision.fallback(
          tableName, KeyRouteAffinityFallbackReason.REQUEST_NOT_QUALIFYING);
    }

    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      return AffinityDecision.fallback(
          tableName, KeyRouteAffinityFallbackReason.REQUEST_NOT_QUALIFYING);
    }

    if (tableName == null) {
      return AffinityDecision.fallback(null, KeyRouteAffinityFallbackReason.TABLE_NAME_UNAVAILABLE);
    }

    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      PartitionKeyResolver.DiscoveryTriggerResult discoveryTriggerResult = null;
      if (clientForDiscovery != null) {
        discoveryTriggerResult =
            pkResolver.triggerDiscoveryWithResult(tableName, clientForDiscovery);
      }
      return AffinityDecision.fallback(
          tableName, KeyRouteAffinityFallbackReason.PK_NOT_CACHED, discoveryTriggerResult);
    }

    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      return AffinityDecision.fallback(
          tableName, KeyRouteAffinityFallbackReason.PK_VALUE_UNAVAILABLE);
    }

    long hash = AttributeValueHasher.hash(pkValue);
    return AffinityDecision.applied(tableName, pkName, pkValue, hash);
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    AffinityDecision decision = evaluateDecision(context.request());
    if (metricsEnabled) {
      metricsCollector.onRequest(decision.tableName, config.getType());
    }

    LazyQueryPlan plan =
        decision.isAffinityApplied()
            ? new LazyQueryPlan(liveNodes, decision.partitionKeyHash)
            : new LazyQueryPlan(liveNodes);
    executionAttributes.putAttribute(QUERY_PLAN, plan);
    executionAttributes.putAttribute(AFFINITY_DECISION, decision);

    if (!decision.isAffinityApplied()) {
      if (metricsEnabled) {
        metricsCollector.onRoundRobinFallback(
            decision.tableName, config.getType(), decision.fallbackReason);
      }
      logFallbackDecision(decision);
    }
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    if (plan == null || !plan.hasNext()) {
      return context.httpRequest();
    }

    URI targetUri = plan.next();
    AffinityDecision decision = executionAttributes.getAttribute(AFFINITY_DECISION);
    if (decision != null && decision.isAffinityApplied()) {
      if (metricsEnabled) {
        metricsCollector.onAffinityApplied(
            decision.tableName,
            config.getType(),
            decision.partitionKeyName,
            decision.partitionKeyValue,
            targetUri);
      }
      logAffinityApplied(decision, targetUri);
    }

    return context.httpRequest().toBuilder()
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

  private void logFallbackDecision(AffinityDecision decision) {
    if (!logger.isLoggable(Level.FINE)) {
      return;
    }

    StringBuilder message = new StringBuilder("Key affinity skipped: ");
    message.append("table=").append(safeTableName(decision.tableName));
    message.append(", mode=").append(config.getType().name());
    message.append(", reason=").append(decision.fallbackReason.getTagValue());
    if (decision.discoveryTriggerResult != null) {
      message.append(", discovery=").append(decision.discoveryTriggerResult.getLogValue());
    }
    logger.log(Level.FINE, message.toString());
  }

  private void logAffinityApplied(AffinityDecision decision, URI targetUri) {
    if (!logger.isLoggable(Level.FINE)) {
      return;
    }

    logger.log(
        Level.FINE,
        "Key affinity applied: table={0}, mode={1}, pk_name={2}, pk_value={3}, target={4}",
        new Object[] {
          safeTableName(decision.tableName),
          config.getType().name(),
          decision.partitionKeyName,
          formatPartitionKeyValue(decision.partitionKeyValue),
          targetUri
        });
  }

  private String safeTableName(String tableName) {
    return tableName != null ? tableName : "unknown";
  }

  private String formatPartitionKeyValue(AttributeValue value) {
    if (value == null) {
      return "null";
    }
    if (value.s() != null) {
      return truncate(value.s());
    }
    if (value.n() != null) {
      return truncate(value.n());
    }
    if (value.bool() != null) {
      return value.bool().toString();
    }
    if (Boolean.TRUE.equals(value.nul())) {
      return "NULL";
    }
    if (value.hasSs()) {
      return "SS(size=" + value.ss().size() + ")";
    }
    if (value.hasNs()) {
      return "NS(size=" + value.ns().size() + ")";
    }
    if (value.hasBs()) {
      return "BS(size=" + value.bs().size() + ")";
    }
    if (value.hasL()) {
      return "L(size=" + value.l().size() + ")";
    }
    if (value.hasM()) {
      return "M(size=" + value.m().size() + ")";
    }
    return truncate(value.toString());
  }

  private String truncate(String value) {
    if (value == null || value.length() <= MAX_LOGGED_PK_LENGTH) {
      return value;
    }
    return value.substring(0, MAX_LOGGED_PK_LENGTH - 3) + "...";
  }
}
