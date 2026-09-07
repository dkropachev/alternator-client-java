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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

public class BasicQueryPlanInterceptorTest {
  @Test
  public void firstTransmissionAdvancesPastNodeThatBecameDown() throws Exception {
    List<URI> nodes =
        Arrays.asList(URI.create("http://127.0.0.1:8000"), URI.create("http://127.0.0.2:8000"));
    FixedLiveNodes liveNodes = new FixedLiveNodes(nodes);
    try {
      BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);
      ExecutionAttributes attributes = ExecutionAttributes.builder().build();
      interceptor.beforeExecution(null, attributes);

      SdkHttpRequest request =
          SdkHttpRequest.builder()
              .protocol("http")
              .host("placeholder")
              .port(8000)
              .method(SdkHttpMethod.POST)
              .encodedPath("/")
              .putHeader("amz-sdk-invocation-id", "first-route-revalidation")
              .build();
      SdkHttpRequest initiallyRouted =
          interceptor.modifyHttpRequest(new RequestContext(request), attributes);
      URI initiallySelected = endpoint(initiallyRouted);

      interceptor.beforeTransmission(new RequestContext(initiallyRouted), attributes);
      liveNodes.reportNodeResult(initiallySelected, NodeHealthObservation.TRAFFIC_FAILURE);

      SdkHttpRequest finallyRouted = interceptor.routeAttempt(initiallyRouted);

      assertNotEquals(initiallySelected, endpoint(finallyRouted));
      assertEquals(
          NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(initiallySelected).getState());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void replacingProtectedQueryPlanPreservesInFlightAccounting() throws Exception {
    List<URI> nodes =
        Arrays.asList(URI.create("http://127.0.0.1:8000"), URI.create("http://127.0.0.2:8000"));
    RecordingLiveNodes liveNodes = new RecordingLiveNodes(nodes);
    try {
      TestableInterceptor interceptor = new TestableInterceptor(liveNodes);
      ExecutionAttributes attributes = ExecutionAttributes.builder().build();
      interceptor.beforeExecution(null, attributes);
      SdkHttpRequest request = request("legacy-plan-replacement");
      SdkHttpRequest initiallyRouted =
          interceptor.modifyHttpRequest(new RequestContext(request), attributes);

      interceptor.beforeTransmission(new RequestContext(initiallyRouted), attributes);
      interceptor.routeAttempt(initiallyRouted);
      interceptor.replaceQueryPlan(attributes, new LazyQueryPlan(liveNodes, 1));
      interceptor.modifyHttpRequest(new RequestContext(request), attributes);

      interceptor.routeAttempt(request);

      assertEquals(1, liveNodes.generationAwareReports.get());
      assertEquals(NodeHealthObservation.TRAFFIC_FAILURE, liveNodes.lastObservation);
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void executionCompletionUnregistersRoutingBridge() throws Exception {
    List<URI> nodes =
        Arrays.asList(URI.create("http://127.0.0.1:8000"), URI.create("http://127.0.0.2:8000"));
    FixedLiveNodes liveNodes = new FixedLiveNodes(nodes);
    try {
      BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);
      ExecutionAttributes attributes = ExecutionAttributes.builder().build();
      interceptor.beforeExecution(null, attributes);
      SdkHttpRequest request = request("routing-cleanup");
      SdkHttpRequest routed =
          interceptor.modifyHttpRequest(new RequestContext(request), attributes);

      interceptor.beforeTransmission(new RequestContext(routed), attributes);
      assertEquals(1, routingExecutionCount(interceptor));

      interceptor.afterExecution(null, attributes);
      assertEquals(0, routingExecutionCount(interceptor));
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  private static SdkHttpRequest request(String invocationId) {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("placeholder")
        .port(8000)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .putHeader("amz-sdk-invocation-id", invocationId)
        .build();
  }

  private static int routingExecutionCount(BasicQueryPlanInterceptor interceptor) throws Exception {
    Field field = BasicQueryPlanInterceptor.class.getDeclaredField("routingExecutions");
    field.setAccessible(true);
    return ((ConcurrentMap<?, ?>) field.get(interceptor)).size();
  }

  private static URI endpoint(SdkHttpRequest request) throws Exception {
    return new URI(request.protocol(), null, request.host(), request.port(), null, null, null);
  }

  private static class FixedLiveNodes extends AlternatorLiveNodes {
    private final List<URI> nodes;

    protected FixedLiveNodes(List<URI> nodes) {
      super(
          AlternatorConfig.builder()
              .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
              .withScheme(nodes.get(0).getScheme())
              .withPort(nodes.get(0).getPort())
              .withNodeHealthConfig(
                  NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build())
              .build(),
          new NoOpHttpClient());
      this.nodes = new ArrayList<>(nodes);
      for (URI node : nodes) {
        reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
      }
    }

    @Override
    protected List<URI> getDiscoveredNodesInternal() {
      return nodes;
    }
  }

  private static final class RecordingLiveNodes extends FixedLiveNodes {
    private final AtomicInteger generationAwareReports = new AtomicInteger();
    private volatile NodeHealthObservation lastObservation;

    private RecordingLiveNodes(List<URI> nodes) {
      super(nodes);
    }

    @Override
    public void reportNodeResult(
        URI node, NodeHealthObservation observation, long expectedTrafficGeneration) {
      lastObservation = observation;
      generationAwareReports.incrementAndGet();
      super.reportNodeResult(node, observation, expectedTrafficGeneration);
    }
  }

  private static final class TestableInterceptor extends BasicQueryPlanInterceptor {
    private TestableInterceptor(AlternatorLiveNodes liveNodes) {
      super(liveNodes);
    }

    private void replaceQueryPlan(ExecutionAttributes attributes, LazyQueryPlan plan) {
      attributes.putAttribute(QUERY_PLAN, plan);
    }
  }

  private static final class NoOpHttpClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "NoOpHttpClient";
    }
  }

  private static final class RequestContext
      implements Context.ModifyHttpRequest, Context.BeforeTransmission {
    private final SdkHttpRequest request;

    private RequestContext(SdkHttpRequest request) {
      this.request = request;
    }

    @Override
    public SdkHttpRequest httpRequest() {
      return request;
    }

    @Override
    public SdkRequest request() {
      return null;
    }

    @Override
    public Optional<RequestBody> requestBody() {
      return Optional.empty();
    }

    @Override
    public Optional<AsyncRequestBody> asyncRequestBody() {
      return Optional.empty();
    }
  }
}
