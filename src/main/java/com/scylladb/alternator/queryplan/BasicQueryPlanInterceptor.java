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

import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.internal.NodeHealthQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Execution interceptor that creates a {@link LazyQueryPlan} for each request.
 *
 * <p>This interceptor creates a non-deterministic query plan during {@link #beforeExecution}, which
 * provides load balancing over query-plan nodes supplied by {@link AlternatorLiveNodes}. The plan
 * is stored in {@link ExecutionAttributes}. The initial route is applied during {@link
 * #modifyHttpRequest}; retry routes are applied by the client's per-transmission HTTP wrapper.
 *
 * <p>This approach correctly handles both synchronous and asynchronous clients, as {@link
 * ExecutionAttributes} travel with the request throughout its lifecycle.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class BasicQueryPlanInterceptor implements ExecutionInterceptor {

  /** Execution attribute key for storing the LazyQueryPlan per request. */
  protected static final ExecutionAttribute<LazyQueryPlan> QUERY_PLAN =
      new ExecutionAttribute<>("QueryPlanInterceptor.queryPlan");

  private static final ExecutionAttribute<RoutingState> ROUTING_STATE =
      new ExecutionAttribute<>("QueryPlanInterceptor.routingState");

  private static final String SDK_INVOCATION_ID_HEADER = "amz-sdk-invocation-id";

  protected final AlternatorLiveNodes liveNodes;
  private final Signer configuredClientSigner;
  private final ConcurrentMap<String, ExecutionAttributes> routingExecutions =
      new ConcurrentHashMap<>();

  /**
   * Creates a new interceptor with the given live nodes manager.
   *
   * @param liveNodes the live nodes manager
   */
  public BasicQueryPlanInterceptor(AlternatorLiveNodes liveNodes) {
    this(liveNodes, null);
  }

  /**
   * Creates an interceptor that preserves an explicitly configured SDK legacy signer.
   *
   * @param liveNodes the live nodes manager
   * @param configuredClientSigner client-level signer override, or null for SDK-selected auth
   */
  public BasicQueryPlanInterceptor(AlternatorLiveNodes liveNodes, Signer configuredClientSigner) {
    this.liveNodes = liveNodes;
    this.configuredClientSigner = configuredClientSigner;
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    initializeQueryPlan(context, executionAttributes);
  }

  /** Creates the request's lazy plan and health-aware regular-routing wrapper. */
  protected void initializeQueryPlan(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    setQueryPlan(executionAttributes, new LazyQueryPlan(liveNodes), false);
  }

  /** Stores a lazy plan and creates its request-scoped health-aware wrapper. */
  protected final void setQueryPlan(
      ExecutionAttributes executionAttributes, LazyQueryPlan plan, boolean affinity) {
    executionAttributes.putAttribute(QUERY_PLAN, plan);
    executionAttributes.putAttribute(
        ROUTING_STATE,
        new RoutingState(
            affinity ? liveNodes.newAffinityQueryPlan(plan) : liveNodes.newRegularQueryPlan(plan),
            plan));
    AttemptRequestSigner.installModernSigner(this, executionAttributes);
  }

  @Override
  public SdkRequest modifyRequest(
      Context.ModifyRequest context, ExecutionAttributes executionAttributes) {
    return AttemptRequestSigner.installLegacySigner(
        this, context.request(), executionAttributes, configuredClientSigner);
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    // SDK 2.21 resolves its HTTP auth scheme after beforeExecution. Retry here so every supported
    // SDK line installs the routing signer before the first signing stage.
    AttemptRequestSigner.installModernSigner(this, executionAttributes);
    return selectRoute(context.httpRequest(), executionAttributes);
  }

  private SdkHttpRequest selectRoute(
      SdkHttpRequest originalRequest, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    if (plan == null) {
      return originalRequest;
    }

    RoutingState routingState = executionAttributes.getAttribute(ROUTING_STATE);
    if (routingState == null || routingState.healthPlanSource != plan) {
      // Preserve compatibility for subclasses that directly replace the protected QUERY_PLAN
      // attribute instead of using the plan-construction hook.
      if (routingState == null) {
        routingState = new RoutingState(liveNodes.newRegularQueryPlan(plan), plan);
        executionAttributes.putAttribute(ROUTING_STATE, routingState);
      } else {
        routingState.replaceHealthPlan(liveNodes.newRegularQueryPlan(plan), plan);
      }
    }

    URI targetUri = routingState.healthPlan.nextRouteCandidate();
    if (targetUri == null) {
      throw new IllegalStateException("No live nodes available");
    }

    // Build new request with the target node's host and port
    return routeToNode(originalRequest, targetUri);
  }

  SdkHttpRequest routeAttempt(SdkHttpRequest request) {
    RoutedRequest routed = routeAttemptWithContext(request);
    armTransportAttempt(routed);
    return routed.request;
  }

  RoutedRequest routeAttemptWithContext(SdkHttpRequest request) {
    String executionId = request.firstMatchingHeader(SDK_INVOCATION_ID_HEADER).orElse(null);
    ExecutionAttributes executionAttributes =
        executionId != null ? routingExecutions.get(executionId) : null;
    if (executionAttributes == null) {
      return new RoutedRequest(request, null, false);
    }

    return preparePendingAttempt(
        request, executionAttributes, requireRoutingState(executionAttributes));
  }

  RoutedRequest routeAttemptForSigning(
      SdkHttpRequest request, ExecutionAttributes executionAttributes) {
    return preparePendingAttempt(
        request, executionAttributes, requireRoutingState(executionAttributes));
  }

  private RoutedRequest preparePendingAttempt(
      SdkHttpRequest request, ExecutionAttributes executionAttributes, RoutingState routingState) {
    InFlightNode pendingNode = routingState.pendingNode;
    if (pendingNode != null) {
      InFlightNode currentNode = captureEligibleInFlightNode(pendingNode.node);
      if (currentNode != null) {
        // The route has not reached transport yet. Reuse its authority, but refresh the generation
        // in case the node entered DOWN and recovered while signing or another local stage retried.
        routingState.pendingNode = currentNode;
        SdkHttpRequest routedRequest =
            sameAuthority(request, currentNode.node)
                ? request
                : routeToNode(request, currentNode.node);
        return new RoutedRequest(
            routedRequest,
            executionAttributes,
            !sameAuthority(request, routedRequest),
            routingState,
            currentNode);
      }
      // A pending route that became DOWN was never sent and must not consume another failure. Drop
      // it and continue the same query plan at the next eligible candidate.
      routingState.pendingNode = null;
    }

    reportInFlightTransportFailure(routingState);
    RoutedAttempt routedAttempt =
        routingState.firstAttempt
            ? revalidateFirstRoute(request, executionAttributes)
            : selectFinalRoute(request, executionAttributes);
    routingState.firstAttempt = false;
    routingState.pendingNode = routedAttempt.inFlightNode;
    return new RoutedRequest(
        routedAttempt.request,
        executionAttributes,
        !sameAuthority(request, routedAttempt.request),
        routingState,
        routedAttempt.inFlightNode);
  }

  void armTransportAttempt(RoutedRequest routed) {
    if (routed.routingState == null || routed.pendingNode == null) {
      return;
    }
    RoutingState routingState = routed.routingState;
    if (routingState.pendingNode == routed.pendingNode) {
      routingState.pendingNode = null;
      routingState.inFlightNode = routed.pendingNode;
    }
  }

  private static boolean sameAuthority(SdkHttpRequest left, SdkHttpRequest right) {
    return left.protocol().equalsIgnoreCase(right.protocol())
        && normalizedHost(left.host()).equalsIgnoreCase(normalizedHost(right.host()))
        && left.port() == right.port();
  }

  private static boolean sameAuthority(SdkHttpRequest request, URI node) {
    return request.protocol().equalsIgnoreCase(node.getScheme())
        && normalizedHost(request.host()).equalsIgnoreCase(normalizedHost(node.getHost()))
        && request.port() == node.getPort();
  }

  private static String normalizedHost(String host) {
    if (host != null && host.length() >= 2 && host.startsWith("[") && host.endsWith("]")) {
      return host.substring(1, host.length() - 1);
    }
    return host;
  }

  private RoutedAttempt revalidateFirstRoute(
      SdkHttpRequest request, ExecutionAttributes executionAttributes) {
    InFlightNode inFlightNode = captureEligibleInFlightNode(request);
    return inFlightNode != null
        ? new RoutedAttempt(request, inFlightNode)
        : selectFinalRoute(request, executionAttributes);
  }

  private SdkHttpRequest routeToNode(SdkHttpRequest request, URI node) {
    return request.toBuilder()
        .protocol(node.getScheme())
        .host(node.getHost())
        .port(node.getPort())
        .putHeader("Connection", "keep-alive")
        .build();
  }

  private RoutedAttempt selectFinalRoute(
      SdkHttpRequest request, ExecutionAttributes executionAttributes) {
    while (true) {
      SdkHttpRequest routedRequest = selectRoute(request, executionAttributes);
      InFlightNode inFlightNode = captureEligibleInFlightNode(routedRequest);
      if (inFlightNode != null) {
        return new RoutedAttempt(routedRequest, inFlightNode);
      }
      // The selected endpoint became down after the health-aware plan returned it. Advance the
      // same request-scoped plan instead of transmitting to an endpoint that is no longer eligible.
    }
  }

  private InFlightNode captureEligibleInFlightNode(SdkHttpRequest request) {
    return captureEligibleInFlightNode(requestEndpoint(request));
  }

  private InFlightNode captureEligibleInFlightNode(URI routedNode) {
    NodeHealthStatus status = liveNodes.getNodeHealthStatus(routedNode);
    if (status != null && status.getState() == NodeHealthState.DOWN) {
      return null;
    }
    return new InFlightNode(routedNode, status != null ? status.getGeneration() : 0);
  }

  private URI requestEndpoint(SdkHttpRequest request) {
    try {
      return new URI(request.protocol(), null, request.host(), request.port(), null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid request endpoint: " + request.getUri(), e);
    }
  }

  private void reportInFlightTransportFailure(RoutingState routingState) {
    reportInFlightNodeResult(routingState, NodeHealthObservation.TRAFFIC_FAILURE);
  }

  private void reportInFlightNodeResult(
      ExecutionAttributes executionAttributes, NodeHealthObservation observation) {
    RoutingState routingState = executionAttributes.getAttribute(ROUTING_STATE);
    if (routingState != null) {
      reportInFlightNodeResult(routingState, observation);
    }
  }

  private void reportInFlightNodeResult(
      RoutingState routingState, NodeHealthObservation observation) {
    InFlightNode inFlightNode = clearInFlightNode(routingState);
    if (inFlightNode == null) {
      return;
    }
    liveNodes.reportNodeResult(inFlightNode.node, observation, inFlightNode.healthGeneration);
  }

  private InFlightNode clearInFlightNode(RoutingState routingState) {
    InFlightNode node = routingState.inFlightNode;
    routingState.inFlightNode = null;
    return node;
  }

  private static boolean isRetryableServerError(int statusCode) {
    return statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504;
  }

  @Override
  public void beforeTransmission(
      Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
    String executionId =
        context.httpRequest().firstMatchingHeader(SDK_INVOCATION_ID_HEADER).orElse(null);
    if (executionId == null) {
      // Preserve the direct-interceptor lifecycle used by custom integrations and older tests.
      RoutingState routingState = requireRoutingState(executionAttributes);
      if (routingState.pendingNode != null) {
        InFlightNode pendingNode = routingState.pendingNode;
        routingState.pendingNode = null;
        routingState.inFlightNode = pendingNode;
        return;
      }
      reportInFlightTransportFailure(routingState);
      URI routedNode = requestEndpoint(context.httpRequest());
      routingState.inFlightNode =
          new InFlightNode(routedNode, liveNodes.getNodeHealthGeneration(routedNode));
      return;
    }
    RoutingState routingState = requireRoutingState(executionAttributes);
    String previousExecutionId = routingState.executionId;
    if (previousExecutionId != null && !previousExecutionId.equals(executionId)) {
      routingExecutions.remove(previousExecutionId, executionAttributes);
    }
    routingState.executionId = executionId;
    routingExecutions.put(executionId, executionAttributes);
  }

  @Override
  public void afterTransmission(
      Context.AfterTransmission context, ExecutionAttributes executionAttributes) {
    if (isRetryableServerError(context.httpResponse().statusCode())) {
      // Alternator reports coordinator timeouts and unrelated internal failures with the same
      // InternalServerError type. Clear transmission bookkeeping, but do not let an ambiguous 5xx
      // either advance or reset node-health counters.
      RoutingState routingState = executionAttributes.getAttribute(ROUTING_STATE);
      if (routingState != null) {
        clearInFlightNode(routingState);
      }
    } else {
      // Authentication and application errors prove that the node handled the request and are not
      // node-health failures.
      reportInFlightNodeResult(executionAttributes, NodeHealthObservation.TRAFFIC_SUCCESS);
    }
  }

  @Override
  public void onExecutionFailure(
      Context.FailedExecution context, ExecutionAttributes executionAttributes) {
    try {
      // afterTransmission clears every received HTTP response, so a remaining in-flight node means
      // the final attempt failed before receiving a response.
      reportInFlightNodeResult(executionAttributes, NodeHealthObservation.TRAFFIC_FAILURE);
    } finally {
      unregisterRoutingExecution(executionAttributes);
    }
  }

  @Override
  public void afterExecution(
      Context.AfterExecution context, ExecutionAttributes executionAttributes) {
    unregisterRoutingExecution(executionAttributes);
  }

  private void unregisterRoutingExecution(ExecutionAttributes executionAttributes) {
    RoutingState routingState = executionAttributes.getAttribute(ROUTING_STATE);
    if (routingState != null) {
      routingState.pendingNode = null;
      if (routingState.executionId != null) {
        routingExecutions.remove(routingState.executionId, executionAttributes);
        routingState.executionId = null;
      }
    }
  }

  private RoutingState requireRoutingState(ExecutionAttributes executionAttributes) {
    RoutingState routingState = executionAttributes.getAttribute(ROUTING_STATE);
    if (routingState == null) {
      LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
      if (plan == null) {
        throw new IllegalStateException("query plan routing state is not initialized");
      }
      routingState = new RoutingState(liveNodes.newRegularQueryPlan(plan), plan);
      executionAttributes.putAttribute(ROUTING_STATE, routingState);
    }
    return routingState;
  }

  /**
   * Returns the live nodes manager used by this interceptor.
   *
   * @return the live nodes manager
   */
  public AlternatorLiveNodes getLiveNodes() {
    return liveNodes;
  }

  /**
   * Wraps a client-level legacy signer for SDK versions that resolve auth after user interceptors.
   *
   * @param signer signer selected by the client configuration
   * @return signer that applies final attempt routing before delegation
   */
  public Signer wrapClientSigner(Signer signer) {
    return AttemptRequestSigner.wrapClientSigner(this, signer);
  }

  /**
   * Returns whether this SDK generation needs routing installed at client-signer configuration.
   *
   * @return true for SDK generations whose auth resolution runs after user interceptor hooks
   */
  public static boolean requiresLegacyClientSigner() {
    return AttemptRequestSigner.requiresLegacyClientSigner();
  }

  private static final class InFlightNode {
    private final URI node;
    private final long healthGeneration;

    private InFlightNode(URI node, long healthGeneration) {
      this.node = node;
      this.healthGeneration = healthGeneration;
    }
  }

  private static final class RoutingState {
    private NodeHealthQueryPlan healthPlan;
    private LazyQueryPlan healthPlanSource;
    private InFlightNode pendingNode;
    private InFlightNode inFlightNode;
    private boolean firstAttempt = true;
    private String executionId;

    private RoutingState(NodeHealthQueryPlan healthPlan, LazyQueryPlan healthPlanSource) {
      this.healthPlan = healthPlan;
      this.healthPlanSource = healthPlanSource;
    }

    private void replaceHealthPlan(NodeHealthQueryPlan healthPlan, LazyQueryPlan healthPlanSource) {
      this.healthPlan = healthPlan;
      this.healthPlanSource = healthPlanSource;
    }
  }

  private static final class RoutedAttempt {
    private final SdkHttpRequest request;
    private final InFlightNode inFlightNode;

    private RoutedAttempt(SdkHttpRequest request, InFlightNode inFlightNode) {
      this.request = request;
      this.inFlightNode = inFlightNode;
    }
  }

  static final class RoutedRequest {
    final SdkHttpRequest request;
    final ExecutionAttributes executionAttributes;
    final boolean authorityChanged;
    private final RoutingState routingState;
    private final InFlightNode pendingNode;

    RoutedRequest(
        SdkHttpRequest request, ExecutionAttributes executionAttributes, boolean authorityChanged) {
      this(request, executionAttributes, authorityChanged, null, null);
    }

    private RoutedRequest(
        SdkHttpRequest request,
        ExecutionAttributes executionAttributes,
        boolean authorityChanged,
        RoutingState routingState,
        InFlightNode pendingNode) {
      this.request = request;
      this.executionAttributes = executionAttributes;
      this.authorityChanged = authorityChanged;
      this.routingState = routingState;
      this.pendingNode = pendingNode;
    }
  }
}
