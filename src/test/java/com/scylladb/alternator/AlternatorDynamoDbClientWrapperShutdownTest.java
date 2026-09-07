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
package com.scylladb.alternator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.keyrouting.PartitionKeyResolver;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class AlternatorDynamoDbClientWrapperShutdownTest {

  @Test
  public void testSyncWrapperStopsLiveNodesBeforeClosingClients() throws Exception {
    List<String> events = new ArrayList<>();
    TrackingLiveNodes liveNodes = new TrackingLiveNodes(events);
    TrackingPollingClient pollingClient = new TrackingPollingClient(events);
    DynamoDbClient client = mock(DynamoDbClient.class);
    doAnswer(
            invocation -> {
              events.add("client");
              return null;
            })
        .when(client)
        .close();

    AlternatorDynamoDbClientWrapper wrapper =
        new AlternatorDynamoDbClientWrapper(client, liveNodes, null, null, pollingClient);

    wrapper.close();

    assertEquals(Arrays.asList("live-nodes", "polling-client", "client"), events);
  }

  @Test
  public void testAsyncWrapperStopsLiveNodesBeforeClosingClients() throws Exception {
    List<String> events = new ArrayList<>();
    TrackingLiveNodes liveNodes = new TrackingLiveNodes(events);
    TrackingPollingClient pollingClient = new TrackingPollingClient(events);
    DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    doAnswer(
            invocation -> {
              events.add("client");
              return null;
            })
        .when(client)
        .close();

    AlternatorDynamoDbAsyncClientWrapper wrapper =
        new AlternatorDynamoDbAsyncClientWrapper(client, liveNodes, null, pollingClient);

    wrapper.close();

    assertEquals(Arrays.asList("live-nodes", "polling-client", "client"), events);
  }

  @Test
  @CoversRequirements("AFF-REQ-007")
  public void testAsyncWrapperShutsDownAffinityResolverBeforeClosingClients() {
    List<String> events = new ArrayList<>();
    TrackingLiveNodes liveNodes = new TrackingLiveNodes(events);
    TrackingPollingClient pollingClient = new TrackingPollingClient(events);
    DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    AffinityQueryPlanInterceptor affinityInterceptor = mock(AffinityQueryPlanInterceptor.class);
    PartitionKeyResolver resolver = mock(PartitionKeyResolver.class);
    when(affinityInterceptor.getPartitionKeyResolver()).thenReturn(resolver);
    doAnswer(
            invocation -> {
              events.add("resolver");
              return null;
            })
        .when(resolver)
        .shutdown();
    doAnswer(
            invocation -> {
              events.add("client");
              return null;
            })
        .when(client)
        .close();

    AlternatorDynamoDbAsyncClientWrapper wrapper =
        new AlternatorDynamoDbAsyncClientWrapper(
            client, liveNodes, null, affinityInterceptor, pollingClient);

    wrapper.close();

    assertEquals(Arrays.asList("resolver", "live-nodes", "polling-client", "client"), events);
  }

  private static final class TrackingLiveNodes extends AlternatorLiveNodes {
    private final List<String> events;

    private TrackingLiveNodes(List<String> events) {
      super(
          AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8000")).build(),
          new NoopPollingClient());
      this.events = events;
    }

    @Override
    public boolean shutdownAndWait() {
      events.add("live-nodes");
      return true;
    }
  }

  private static final class NoopPollingClient implements SdkHttpClient {

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "noop";
    }
  }

  private static final class TrackingPollingClient implements SdkHttpClient {
    private final List<String> events;

    private TrackingPollingClient(List<String> events) {
      this.events = events;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      events.add("polling-client");
    }

    @Override
    public String clientName() {
      return "tracking";
    }
  }
}
