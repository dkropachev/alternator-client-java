package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetricLabel;
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

/** Execution interceptor that implements key-based route affinity. */
public class AffinityQueryPlanInterceptor extends BasicQueryPlanInterceptor {
  private static final Logger logger =
      Logger.getLogger(AffinityQueryPlanInterceptor.class.getName());
  private static final ExecutionAttribute<RoutingDecision> ROUTING_DECISION =
      new ExecutionAttribute<>("AffinityQueryPlanInterceptor.routingDecision");

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private final KeyRouteAffinityMetrics metrics;
  private volatile DynamoDbClient clientForDiscovery;

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

  public AffinityQueryPlanInterceptor(
      KeyRouteAffinityConfig config, AlternatorLiveNodes liveNodes) {
    this(config, liveNodes, null);
  }

  public void setClientForDiscovery(DynamoDbClient client) {
    this.clientForDiscovery = client;
  }

  private RoutingDecision evaluateRequest(SdkRequest request) {
    String requestTableName = KeyAffinityRequestClassifier.extractTableName(request);
    String metricsTableName =
        requestTableName != null
            ? requestTableName
            : KeyRouteAffinityMetricLabel.UNKNOWN_TABLE.value();

    metrics.onRequest(metricsTableName, config.getType());

    if (!config.isEnabled()) {
      return RoutingDecision.skipped(metricsTableName, "affinity_disabled");
    }

    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      return RoutingDecision.skipped(
          metricsTableName, KeyRouteAffinityMetricLabel.NOT_QUALIFYING_REQUEST.value());
    }

    if (requestTableName == null) {
      return RoutingDecision.skipped(
          metricsTableName, KeyRouteAffinityMetricLabel.TABLE_NAME_MISSING.value());
    }

    String pkName = pkResolver.getPartitionKeyName(requestTableName);
    if (pkName == null) {
      if (!pkResolver.isInFailureCooldown(requestTableName) && clientForDiscovery != null) {
        pkResolver.triggerDiscovery(requestTableName, clientForDiscovery);
      }
      return RoutingDecision.skipped(
          metricsTableName, KeyRouteAffinityMetricLabel.PK_NOT_CACHED.value());
    }

    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      return RoutingDecision.skipped(
          metricsTableName, KeyRouteAffinityMetricLabel.PK_VALUE_MISSING.value());
    }

    long hash = AttributeValueHasher.hash(pkValue);
    return RoutingDecision.applied(metricsTableName, hash, Long.toUnsignedString(hash, 16));
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    RoutingDecision decision = evaluateRequest(context.request());
    executionAttributes.putAttribute(ROUTING_DECISION, decision);

    LazyQueryPlan plan;
    if (decision.affinityApplied) {
      metrics.onAffinityApplied(decision.tableName, config.getType());
      plan = new LazyQueryPlan(liveNodes, decision.hash);
    } else {
      logAffinitySkipped(decision);
      metrics.onAffinitySkipped(decision.tableName, config.getType(), decision.reason);
      plan = new LazyQueryPlan(liveNodes);
    }

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
          new Object[] {decision.tableName, decision.partitionKeyFingerprint, targetUri});
    }

    SdkHttpRequest originalRequest = context.httpRequest();
    return originalRequest.toBuilder()
        .protocol(targetUri.getScheme())
        .host(targetUri.getHost())
        .port(targetUri.getPort())
        .putHeader("Connection", "keep-alive")
        .build();
  }

  public PartitionKeyResolver getPartitionKeyResolver() {
    return pkResolver;
  }

  public KeyRouteAffinityConfig getConfig() {
    return config;
  }

  private void logAffinitySkipped(RoutingDecision decision) {
    if (logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "Key affinity skipped: table={0}, reason={1}",
          new Object[] {decision.tableName, decision.reason});
    }
  }

  private static final class RoutingDecision {
    final String tableName;
    final String reason;
    final String partitionKeyFingerprint;
    final long hash;
    final boolean affinityApplied;

    private RoutingDecision(
        String tableName,
        String reason,
        String partitionKeyFingerprint,
        long hash,
        boolean affinityApplied) {
      this.tableName = tableName;
      this.reason = reason;
      this.partitionKeyFingerprint = partitionKeyFingerprint;
      this.hash = hash;
      this.affinityApplied = affinityApplied;
    }

    static RoutingDecision skipped(String tableName, String reason) {
      return new RoutingDecision(tableName, reason, null, 0L, false);
    }

    static RoutingDecision applied(String tableName, long hash, String partitionKeyFingerprint) {
      return new RoutingDecision(tableName, null, partitionKeyFingerprint, hash, true);
    }
  }
}
