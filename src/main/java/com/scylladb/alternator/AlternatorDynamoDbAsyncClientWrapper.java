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

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

/**
 * A wrapper that holds a DynamoDbAsyncClient along with Alternator-specific functionality.
 *
 * <p>This class provides access to the underlying {@link DynamoDbAsyncClient} via {@link
 * #getClient()}, and adds methods to access Alternator infrastructure such as the list of live
 * nodes for load balancing.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorDynamoDbAsyncClientWrapper wrapper = AlternatorDynamoDbAsyncClient.builder()
 *     .endpointOverride(URI.create("http://localhost:8000"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // Get the DynamoDB async client for normal operations
 * DynamoDbAsyncClient client = wrapper.getClient();
 * client.createTable(...).join();
 *
 * // Access Alternator-specific functionality
 * List<URI> nodes = wrapper.getLiveNodes();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AlternatorDynamoDbAsyncClientWrapper implements AutoCloseable {

  private final DynamoDbAsyncClient client;
  private final AlternatorLiveNodes liveNodes;
  private final AlternatorConfig config;
  private final AffinityQueryPlanInterceptor affinityInterceptor;
  private final SdkHttpClient pollingHttpClient;

  /**
   * Creates a new wrapper with the given client and live nodes.
   *
   * @param client the underlying DynamoDbAsyncClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   */
  public AlternatorDynamoDbAsyncClientWrapper(
      DynamoDbAsyncClient client, AlternatorLiveNodes liveNodes) {
    this(client, liveNodes, null, null, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, and config.
   *
   * @param client the underlying DynamoDbAsyncClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   */
  public AlternatorDynamoDbAsyncClientWrapper(
      DynamoDbAsyncClient client, AlternatorLiveNodes liveNodes, AlternatorConfig config) {
    this(client, liveNodes, config, null, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, config, and polling client.
   *
   * @param client the underlying DynamoDbAsyncClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   * @param pollingHttpClient the SdkHttpClient used for LiveNodes polling (may be null)
   * @since 2.1.0
   */
  public AlternatorDynamoDbAsyncClientWrapper(
      DynamoDbAsyncClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorConfig config,
      SdkHttpClient pollingHttpClient) {
    this(client, liveNodes, config, null, pollingHttpClient);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, config, interceptor, and polling
   * client.
   *
   * @param client the underlying DynamoDbAsyncClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   * @param affinityInterceptor the AffinityQueryPlanInterceptor (may be null)
   * @param pollingHttpClient the SdkHttpClient used for LiveNodes polling (may be null)
   * @since 2.1.0
   */
  public AlternatorDynamoDbAsyncClientWrapper(
      DynamoDbAsyncClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorConfig config,
      AffinityQueryPlanInterceptor affinityInterceptor,
      SdkHttpClient pollingHttpClient) {
    this.client = client;
    this.liveNodes = liveNodes;
    this.config = config;
    this.affinityInterceptor = affinityInterceptor;
    this.pollingHttpClient = pollingHttpClient;
  }

  /**
   * Returns the underlying DynamoDbAsyncClient.
   *
   * @return the DynamoDbAsyncClient instance
   */
  public DynamoDbAsyncClient getClient() {
    return client;
  }

  /**
   * Returns a snapshot of the current discovered nodes list.
   *
   * @return an unmodifiable list of the current discovered node URIs
   * @since 2.1.0
   */
  public List<URI> getDiscoveredNodes() {
    return liveNodes.getDiscoveredNodes();
  }

  /**
   * Returns a snapshot of discovered nodes currently active for normal routing.
   *
   * @return an unmodifiable list of active discovered node URIs
   */
  public List<URI> getLiveNodes() {
    return liveNodes.getLiveNodes();
  }

  /**
   * Returns the next node URI using round-robin selection.
   *
   * @return the next eligible node URI
   * @deprecated Request routing is automatic; use {@link #getLiveNodes()} to inspect active nodes.
   */
  @Deprecated
  public URI nextAsURI() {
    return liveNodes.nextAsURI();
  }

  /**
   * Directly probes and activates reachable quarantined nodes in the current discovered set.
   *
   * <p>This control-plane operation is synchronous and uses the client's polling HTTP transport.
   *
   * @return endpoints that returned HTTP 200 during this probe operation
   * @since 2.1.0
   */
  public List<URI> probeQuarantinedNodes() {
    return liveNodes.probeQuarantinedNodes();
  }

  /**
   * Asynchronously probes and activates reachable quarantined nodes.
   *
   * @return future containing endpoints that returned HTTP 200 in snapshot order
   * @since 2.1.0
   */
  public CompletableFuture<List<URI>> probeQuarantinedNodesAsync() {
    return liveNodes.probeQuarantinedNodesAsync();
  }

  /**
   * Checks if the server supports rack/datacenter filtering for node discovery.
   *
   * @return true if rack/datacenter filtering is supported, false otherwise
   * @throws AlternatorLiveNodes.FailedToCheck if the check cannot be completed
   */
  public boolean checkIfRackDatacenterFeatureIsSupported()
      throws AlternatorLiveNodes.FailedToCheck {
    return liveNodes.checkIfRackDatacenterFeatureIsSupported();
  }

  /**
   * Returns the AlternatorLiveNodes instance used for node discovery.
   *
   * @return the {@link AlternatorLiveNodes} instance
   */
  public AlternatorLiveNodes getAlternatorLiveNodes() {
    return liveNodes;
  }

  /**
   * Returns the AlternatorConfig used to create this client.
   *
   * @return the {@link AlternatorConfig} instance, or null if not available
   */
  public AlternatorConfig getAlternatorConfig() {
    return config;
  }

  /**
   * Closes the underlying DynamoDbAsyncClient and releases associated resources.
   *
   * <p>This includes:
   *
   * <ul>
   *   <li>Shutting down the partition key resolver's discovery executor if key route affinity is
   *       enabled
   *   <li>Stopping the LiveNodes background thread
   *   <li>Closing the polling HTTP client
   *   <li>Closing the underlying async DynamoDB client
   * </ul>
   */
  @Override
  public void close() {
    if (affinityInterceptor != null) {
      affinityInterceptor.getPartitionKeyResolver().shutdown();
    }
    liveNodes.shutdownAndWait();
    if (pollingHttpClient != null) {
      pollingHttpClient.close();
    }
    client.close();
  }
}
