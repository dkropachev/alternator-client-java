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
package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;

/** Socket-level coverage for the synchronous transport used by LiveNodes polling. */
public class AlternatorLiveNodesSocketLifecycleTest {
  private static final long REUSE_IDLE_TIME_MS = 60_000;
  private static final long SHORT_IDLE_TIME_MS = 25;
  private static final long EXPIRY_WAIT_MS = 200;
  private static final List<String> SIX_LOOPBACK_ROUTES =
      Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6");

  @Test(timeout = 30_000)
  public void pollingConnectionIdentityTracksShortIdleBoundary() throws Exception {
    assertPollingConnectionIdentity(REUSE_IDLE_TIME_MS, 0, false);
    assertPollingConnectionIdentity(SHORT_IDLE_TIME_MS, EXPIRY_WAIT_MS, true);
  }

  private void assertPollingConnectionIdentity(
      long idleTimeMs, long waitBetweenRequestsMs, boolean expectReplacement) throws Exception {
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            request -> SocketTrackingHttpServer.Response.json("[\"127.0.0.1\"]"))) {
      server.start();
      AlternatorConfig config =
          AlternatorConfig.builder()
              .withSeedNode(server.uri())
              .withRoutingScope(ClusterScope.create())
              .withNodeHealthDisabled()
              .withMaxConnections(1)
              .withConnectionMaxIdleTimeMs(idleTimeMs)
              .build();
      SdkHttpClient pollingClient = ApacheSyncClientFactory.create(null, config, null);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, pollingClient);
      try {
        liveNodes.refreshDiscoveredNodes();
        if (waitBetweenRequestsMs > 0) {
          Thread.sleep(waitBetweenRequestsMs);
        }
        liveNodes.refreshDiscoveredNodes();

        List<SocketTrackingHttpServer.Request> requests = server.requestsSince(0);
        assertEquals(2, requests.size());
        if (expectReplacement) {
          assertNotEquals(
              "polling should replace a connection after the configured idle threshold",
              requests.get(0).remotePort(),
              requests.get(1).remotePort());
          assertEquals(2, server.acceptedConnections());
          assertEquals(2, server.uniqueRemotePorts().size());
        } else {
          assertEquals(
              "polling should reuse its physical connection before the idle threshold",
              requests.get(0).remotePort(),
              requests.get(1).remotePort());
          assertEquals(1, server.acceptedConnections());
          assertEquals(1, server.uniqueRemotePorts().size());
        }
        server.assertHealthy();
      } finally {
        try {
          liveNodes.shutdownAndWait();
        } finally {
          pollingClient.close();
        }
      }
    }
  }

  @Test(timeout = 30_000)
  public void clusterPollingVisitsMoreRoutesThanApacheGlobalCapacity() throws Exception {
    String responseBody = "[\"" + String.join("\",\"", SIX_LOOPBACK_ROUTES) + "\"]";
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            InetAddress.getByName("0.0.0.0"),
            request -> SocketTrackingHttpServer.Response.json(responseBody))) {
      server.start();
      SdkHttpClient pollingClient = ApacheSyncClientFactory.createPollingClient(null, 5);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(pollingConfig(server), pollingClient);
      try {
        liveNodes.refreshDiscoveredNodes();
        assertEquals(6, liveNodes.getDiscoveredNodes().size());

        int requestsBeforeFullRefresh = server.requestCount();
        liveNodes.refreshDiscoveredNodes();
        List<SocketTrackingHttpServer.Request> fullRefresh =
            server.requestsSince(requestsBeforeFullRefresh);

        assertEquals(
            "cluster polling should contact every discovered route", 6, fullRefresh.size());
        Set<String> destinations = new LinkedHashSet<>();
        for (SocketTrackingHttpServer.Request request : fullRefresh) {
          destinations.add(request.destinationAddress());
        }
        assertEquals(
            "host rotation must be measured separately from connection replacement",
            new LinkedHashSet<>(SIX_LOOPBACK_ROUTES),
            destinations);

        assertTrue(
            "six endpoint routes require at least six physical connections",
            server.acceptedConnections() >= 6);
        assertTrue(
            "one already-idle route may be replaced, but no request should be retried",
            server.acceptedConnections() <= 7);
        assertTrue(
            "Apache should return to its configured global pool capacity",
            server.awaitActiveConnections(5, 5, TimeUnit.SECONDS));
        server.assertHealthy();
      } finally {
        try {
          liveNodes.shutdownAndWait();
        } finally {
          pollingClient.close();
        }
      }
    }
  }

  private AlternatorConfig pollingConfig(SocketTrackingHttpServer server) {
    return AlternatorConfig.builder()
        .withSeedNode(server.uri())
        .withRoutingScope(ClusterScope.create())
        .withNodeHealthDisabled()
        .build();
  }
}
