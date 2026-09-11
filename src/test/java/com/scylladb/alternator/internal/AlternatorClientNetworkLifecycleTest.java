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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;
import com.scylladb.alternator.AlternatorDynamoDbAsyncClientWrapper;
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.AlternatorDynamoDbClientWrapper;
import com.scylladb.alternator.HttpClientType;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Black-box shutdown coverage for every internally managed HTTP transport. */
@RunWith(Parameterized.class)
public class AlternatorClientNetworkLifecycleTest {
  private static final long REFRESH_INTERVAL_MS = 250;

  private enum ClientPath {
    APACHE_SYNC,
    CRT_SYNC,
    NETTY_ASYNC,
    CRT_ASYNC
  }

  private final ClientPath clientPath;
  private final boolean closeWrapper;

  public AlternatorClientNetworkLifecycleTest(
      ClientPath clientPath, String closePath, boolean closeWrapper) {
    this.clientPath = clientPath;
    this.closeWrapper = closeWrapper;
  }

  @Parameters(name = "{0}-{1}")
  public static Collection<Object[]> clientsAndClosePaths() {
    List<Object[]> parameters = new ArrayList<>();
    for (ClientPath clientPath : ClientPath.values()) {
      parameters.add(new Object[] {clientPath, "standard-build", false});
      parameters.add(new Object[] {clientPath, "alternator-wrapper", true});
    }
    return parameters;
  }

  @Test(timeout = 30_000)
  public void closeStopsPollingAndClosesOwnedSockets() throws Exception {
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            request ->
                "GET".equals(request.method())
                    ? SocketTrackingHttpServer.Response.json("[\"127.0.0.1\"]")
                    : SocketTrackingHttpServer.Response.json("{\"TableNames\":[]}"))) {
      server.start();
      ClientSession session = openClient(server.uri());
      try {
        assertTrue(
            "background polling should reach /localnodes",
            server.awaitRequestCountByMethod("GET", 1, 10, TimeUnit.SECONDS));

        session.executeDynamoDbRequest();
        assertTrue(
            "main and polling transports should each retain a socket",
            server.awaitActiveConnectionsAtLeast(2, 10, TimeUnit.SECONDS));

        SocketTrackingHttpServer.Request pollingRequest =
            findRequest(server.requestsSince(0), "GET", "/localnodes");
        SocketTrackingHttpServer.Request mainRequest =
            findRequest(server.requestsSince(0), "POST", "/");
        assertNotNull("a polling request should be recorded", pollingRequest);
        assertNotNull("a DynamoDB request should be recorded", mainRequest);
        assertNotEquals(
            "main and polling transports must use different physical connections",
            pollingRequest.remotePort(),
            mainRequest.remotePort());

        Set<Integer> activePorts = server.activeRemotePorts();
        assertTrue(
            "polling connection should be open before close",
            activePorts.contains(pollingRequest.remotePort()));
        assertTrue(
            "main connection should be open before close",
            activePorts.contains(mainRequest.remotePort()));
      } finally {
        session.close();
      }

      long closeCompletedAtNanos = session.closeCompletedAtNanos();
      assertTrue(
          "all internally owned transport sockets should close",
          server.awaitActiveConnections(0, 10, TimeUnit.SECONDS));

      Thread.sleep(REFRESH_INTERVAL_MS * 4);
      assertEquals(
          "no /localnodes request may start after close returns",
          0,
          server.requestCountByMethodAfter("GET", closeCompletedAtNanos));
      assertEquals(
          "no transport socket may reopen after close returns",
          0,
          server.acceptedConnectionsAfter(closeCompletedAtNanos));
      assertEquals("all transport sockets should remain closed", 0, server.activeConnections());
      server.assertHealthy();
    }
  }

  private ClientSession openClient(URI endpoint) {
    switch (clientPath) {
      case APACHE_SYNC:
        return openSyncClient(endpoint, HttpClientType.APACHE);
      case CRT_SYNC:
        return openSyncClient(endpoint, HttpClientType.CRT);
      case NETTY_ASYNC:
        return openAsyncClient(endpoint, HttpClientType.NETTY);
      case CRT_ASYNC:
        return openAsyncClient(endpoint, HttpClientType.CRT);
      default:
        throw new IllegalStateException("Unknown client path: " + clientPath);
    }
  }

  private ClientSession openSyncClient(URI endpoint, HttpClientType httpClientType) {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(endpoint)
            .withHttpClientType(httpClientType)
            .withNodeHealthDisabled()
            .withActiveRefreshIntervalMs(REFRESH_INTERVAL_MS)
            .withIdleRefreshIntervalMs(REFRESH_INTERVAL_MS);

    if (closeWrapper) {
      AlternatorDynamoDbClientWrapper wrapper = builder.buildWithAlternatorAPI();
      return new ClientSession(() -> wrapper.getClient().listTables(), wrapper::close);
    }

    DynamoDbClient client = builder.build();
    return new ClientSession(client::listTables, client::close);
  }

  private ClientSession openAsyncClient(URI endpoint, HttpClientType httpClientType) {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(endpoint)
            .withHttpClientType(httpClientType)
            .withNodeHealthDisabled()
            .withActiveRefreshIntervalMs(REFRESH_INTERVAL_MS)
            .withIdleRefreshIntervalMs(REFRESH_INTERVAL_MS);

    if (closeWrapper) {
      AlternatorDynamoDbAsyncClientWrapper wrapper = builder.buildWithAlternatorAPI();
      return new ClientSession(
          () -> wrapper.getClient().listTables().get(10, TimeUnit.SECONDS), wrapper::close);
    }

    DynamoDbAsyncClient client = builder.build();
    return new ClientSession(() -> client.listTables().get(10, TimeUnit.SECONDS), client::close);
  }

  private SocketTrackingHttpServer.Request findRequest(
      List<SocketTrackingHttpServer.Request> requests, String method, String targetPrefix) {
    for (SocketTrackingHttpServer.Request request : requests) {
      if (method.equals(request.method()) && request.path().startsWith(targetPrefix)) {
        return request;
      }
    }
    return null;
  }

  private interface RequestAction {
    void run() throws Exception;
  }

  private static final class ClientSession implements AutoCloseable {
    private final RequestAction requestAction;
    private final Runnable closeAction;
    private long closeCompletedAtNanos;

    private ClientSession(RequestAction requestAction, Runnable closeAction) {
      this.requestAction = requestAction;
      this.closeAction = closeAction;
    }

    private void executeDynamoDbRequest() throws Exception {
      requestAction.run();
    }

    @Override
    public void close() {
      try {
        closeAction.run();
      } finally {
        closeCompletedAtNanos = System.nanoTime();
      }
    }

    private long closeCompletedAtNanos() {
      return closeCompletedAtNanos;
    }
  }
}
