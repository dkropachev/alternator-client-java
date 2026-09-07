package com.scylladb.alternator.queryplan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.internal.AlternatorLiveNodes;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

  private static URI endpoint(SdkHttpRequest request) throws Exception {
    return new URI(request.protocol(), null, request.host(), request.port(), null, null, null);
  }

  private static final class FixedLiveNodes extends AlternatorLiveNodes {
    private final List<URI> nodes;

    private FixedLiveNodes(List<URI> nodes) {
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
