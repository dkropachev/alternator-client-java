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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * A wrapper that holds a DynamoDbClient along with Alternator-specific functionality.
 *
 * <p>This class provides access to the underlying {@link DynamoDbClient} via {@link #getClient()},
 * and adds methods to access Alternator infrastructure such as the list of live nodes for load
 * balancing.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorDynamoDbClientWrapper wrapper = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("http://localhost:8000"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // Get the DynamoDB client for normal operations
 * DynamoDbClient client = wrapper.getClient();
 * client.createTable(...);
 *
 * // Access Alternator-specific functionality
 * List<URI> nodes = wrapper.getLiveNodes();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AlternatorDynamoDbClientWrapper implements AutoCloseable {

  private final DynamoDbClient client;
  private final AlternatorLiveNodes liveNodes;
  private final AlternatorConfig config;
  private final AlternatorClientResources resources;
  private boolean closed;

  /**
   * Creates a new wrapper with the given client and live nodes.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   */
  public AlternatorDynamoDbClientWrapper(DynamoDbClient client, AlternatorLiveNodes liveNodes) {
    this(client, liveNodes, null, null, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, and config.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   */
  public AlternatorDynamoDbClientWrapper(
      DynamoDbClient client, AlternatorLiveNodes liveNodes, AlternatorConfig config) {
    this(client, liveNodes, config, null, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, config, and interceptor.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   * @param affinityInterceptor the AffinityQueryPlanInterceptor (may be null)
   */
  public AlternatorDynamoDbClientWrapper(
      DynamoDbClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorConfig config,
      AffinityQueryPlanInterceptor affinityInterceptor) {
    this(client, liveNodes, config, affinityInterceptor, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, config, interceptor, and polling
   * client.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   * @param affinityInterceptor the AffinityQueryPlanInterceptor (may be null)
   * @param pollingHttpClient the SdkHttpClient used for LiveNodes polling (may be null)
   * @since 2.1.0
   */
  public AlternatorDynamoDbClientWrapper(
      DynamoDbClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorConfig config,
      AffinityQueryPlanInterceptor affinityInterceptor,
      SdkHttpClient pollingHttpClient) {
    this(
        client,
        liveNodes,
        config,
        affinityInterceptor,
        pollingHttpClient,
        new AlternatorClientResources(liveNodes, affinityInterceptor, pollingHttpClient));
  }

  AlternatorDynamoDbClientWrapper(
      DynamoDbClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorConfig config,
      AffinityQueryPlanInterceptor affinityInterceptor,
      SdkHttpClient pollingHttpClient,
      AlternatorClientResources resources) {
    this.client = client;
    this.liveNodes = liveNodes;
    this.config = config;
    this.resources = resources;

    // Enable auto-discovery by providing the built client to the interceptor
    if (affinityInterceptor != null) {
      affinityInterceptor.setClientForDiscovery(client);
    }
  }

  /**
   * Returns the underlying DynamoDbClient.
   *
   * @return the DynamoDbClient instance
   */
  public DynamoDbClient getClient() {
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
   * Returns a snapshot of the current discovered nodes list.
   *
   * @return an unmodifiable list of the current discovered node URIs
   */
  public List<URI> getLiveNodes() {
    return liveNodes.getLiveNodes();
  }

  /**
   * Returns the next node URI using round-robin selection.
   *
   * @return the next eligible node URI
   * @deprecated Request routing is automatic; use {@link #getLiveNodes()} to inspect topology.
   */
  @Deprecated
  public URI nextAsURI() {
    return liveNodes.nextAsURI();
  }

  /**
   * Directly probes and activates reachable quarantined nodes in the current discovered set.
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
   * Closes the underlying DynamoDbClient and releases associated resources.
   *
   * <p>This includes:
   *
   * <ul>
   *   <li>Shutting down the partition key resolver's discovery executor if key route affinity is
   *       enabled
   *   <li>Stopping the LiveNodes background thread
   *   <li>Closing the polling HTTP client
   *   <li>Closing the underlying DynamoDB client
   *   <li>Closing the main HTTP client when it is owned by the Alternator builder
   * </ul>
   */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;

    RuntimeException failure = null;
    try {
      resources.close();
    } catch (RuntimeException e) {
      failure = e;
    }

    try {
      client.close();
    } catch (RuntimeException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }

    if (failure != null) {
      throw failure;
    }
  }
}
