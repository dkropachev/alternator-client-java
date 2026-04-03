package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityFallbackReason;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetricsListener;
import com.scylladb.alternator.keyrouting.PartitionKeyResolver;
import java.net.URI;
import java.util.function.Consumer;
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
      new ExecutionAttribute<>("AffinityQueryPlanInterceptor.decision");

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private final KeyRouteAffinityMetricsListener metricsListener;
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
    this.metricsListener = config.getMetricsListener();
    this.pkResolver = new PartitionKeyResolver(config.getPkInfoPerTable(), metricsListener);
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

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    AffinityDecision decision = evaluateDecision(context.request());
    executionAttributes.putAttribute(AFFINITY_DECISION, decision);
    executionAttributes.putAttribute(
        QUERY_PLAN,
        decision.affinityApplied
            ? new LazyQueryPlan(liveNodes, decision.partitionKeyHash)
            : new LazyQueryPlan(liveNodes));

    recordMetrics(decision);
    logSkippedDecision(decision);
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    if (plan == null || !plan.hasNext()) {
      return context.httpRequest();
    }

    URI targetUri = plan.next();
    logAppliedDecision(executionAttributes.getAttribute(AFFINITY_DECISION), targetUri);

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

  private AffinityDecision evaluateDecision(SdkRequest request) {
    KeyRouteAffinity mode = config.getType();
    String requestType = request.getClass().getSimpleName();

    if (!config.isEnabled()) {
      return AffinityDecision.roundRobin(
          "unknown", mode, requestType, KeyRouteAffinityFallbackReason.MODE_DISABLED, null);
    }

    String tableName = KeyAffinityRequestClassifier.extractTableName(request);
    String telemetryTableName = normalizeTableName(tableName);

    if (!KeyAffinityRequestClassifier.shouldApply(mode, request)) {
      return AffinityDecision.roundRobin(
          telemetryTableName,
          mode,
          requestType,
          KeyRouteAffinityFallbackReason.REQUEST_NOT_QUALIFYING,
          null);
    }

    if (tableName == null) {
      return AffinityDecision.roundRobin(
          telemetryTableName,
          mode,
          requestType,
          KeyRouteAffinityFallbackReason.TABLE_NAME_UNAVAILABLE,
          null);
    }

    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      String detail = "discovery_unavailable";
      if (clientForDiscovery != null) {
        boolean discoveryTriggered = pkResolver.triggerDiscoveryIfNeeded(tableName, clientForDiscovery);
        if (discoveryTriggered) {
          detail = "discovery_triggered";
        } else if (pkResolver.isInFailureCooldown(tableName)) {
          detail = "discovery_cooldown";
        } else {
          detail = "discovery_pending";
        }
      }

      return AffinityDecision.roundRobin(
          telemetryTableName,
          mode,
          requestType,
          KeyRouteAffinityFallbackReason.PARTITION_KEY_NOT_CACHED,
          detail);
    }

    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      return AffinityDecision.roundRobin(
          telemetryTableName,
          mode,
          requestType,
          KeyRouteAffinityFallbackReason.PARTITION_KEY_MISSING,
          "pk_attribute=" + pkName);
    }

    return AffinityDecision.applied(
        telemetryTableName,
        mode,
        requestType,
        pkName,
        formatPartitionKeyValue(pkValue),
        AttributeValueHasher.hash(pkValue));
  }

  private void recordMetrics(AffinityDecision decision) {
    if (metricsListener == null) {
      return;
    }

    notifyMetrics(listener -> listener.onRequest(decision.tableName, decision.mode));
    if (decision.affinityApplied) {
      notifyMetrics(listener -> listener.onAffinityApplied(decision.tableName, decision.mode));
      return;
    }

    notifyMetrics(
        listener ->
            listener.onRoundRobinFallback(
                decision.tableName, decision.mode, decision.fallbackReason));
  }

  private void logSkippedDecision(AffinityDecision decision) {
    if (decision.affinityApplied || !logger.isLoggable(Level.FINE)) {
      return;
    }

    StringBuilder message =
        new StringBuilder("Key affinity skipped: table=")
            .append(decision.tableName)
            .append(", reason=")
            .append(decision.fallbackReason.label())
            .append(", mode=")
            .append(decision.mode)
            .append(", request=")
            .append(decision.requestType);

    if (decision.detail != null && !decision.detail.isEmpty()) {
      message.append(", detail=").append(decision.detail);
    }

    logger.fine(message.toString());
  }

  private void logAppliedDecision(AffinityDecision decision, URI targetUri) {
    if (decision == null || !decision.affinityApplied || !logger.isLoggable(Level.FINE)) {
      return;
    }

    logger.fine(
        "Key affinity applied: table="
            + decision.tableName
            + ", pk="
            + decision.partitionKeyValue
            + ", pk_attribute="
            + decision.partitionKeyName
            + ", target="
            + targetUri.getHost()
            + ":"
            + targetUri.getPort()
            + ", mode="
            + decision.mode
            + ", request="
            + decision.requestType);
  }

  private String normalizeTableName(String tableName) {
    return tableName != null ? tableName : "unknown";
  }

  private String formatPartitionKeyValue(AttributeValue pkValue) {
    if (!logger.isLoggable(Level.FINE)) {
      return null;
    }
    if (pkValue.s() != null) {
      return pkValue.s();
    }
    if (pkValue.n() != null) {
      return pkValue.n();
    }
    if (pkValue.bool() != null) {
      return pkValue.bool().toString();
    }
    if (Boolean.TRUE.equals(pkValue.nul())) {
      return "null";
    }
    return pkValue.toString();
  }

  private void notifyMetrics(Consumer<KeyRouteAffinityMetricsListener> callback) {
    try {
      callback.accept(metricsListener);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Key route affinity metrics listener threw an exception", e);
    }
  }

  private static final class AffinityDecision {
    private final boolean affinityApplied;
    private final String tableName;
    private final KeyRouteAffinity mode;
    private final String requestType;
    private final KeyRouteAffinityFallbackReason fallbackReason;
    private final String detail;
    private final String partitionKeyName;
    private final String partitionKeyValue;
    private final long partitionKeyHash;

    private AffinityDecision(
        boolean affinityApplied,
        String tableName,
        KeyRouteAffinity mode,
        String requestType,
        KeyRouteAffinityFallbackReason fallbackReason,
        String detail,
        String partitionKeyName,
        String partitionKeyValue,
        long partitionKeyHash) {
      this.affinityApplied = affinityApplied;
      this.tableName = tableName;
      this.mode = mode;
      this.requestType = requestType;
      this.fallbackReason = fallbackReason;
      this.detail = detail;
      this.partitionKeyName = partitionKeyName;
      this.partitionKeyValue = partitionKeyValue;
      this.partitionKeyHash = partitionKeyHash;
    }

    private static AffinityDecision roundRobin(
        String tableName,
        KeyRouteAffinity mode,
        String requestType,
        KeyRouteAffinityFallbackReason fallbackReason,
        String detail) {
      return new AffinityDecision(
          false, tableName, mode, requestType, fallbackReason, detail, null, null, 0L);
    }

    private static AffinityDecision applied(
        String tableName,
        KeyRouteAffinity mode,
        String requestType,
        String partitionKeyName,
        String partitionKeyValue,
        long partitionKeyHash) {
      return new AffinityDecision(
          true,
          tableName,
          mode,
          requestType,
          null,
          null,
          partitionKeyName,
          partitionKeyValue,
          partitionKeyHash);
    }
  }
}
