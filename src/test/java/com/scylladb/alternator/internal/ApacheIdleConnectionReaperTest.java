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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Tests Apache connection identity across the default, disabled, and short idle-time settings.
 *
 * <p>Uses a raw loopback HTTP server so a successful replacement connection cannot masquerade as
 * connection reuse.
 */
public class ApacheIdleConnectionReaperTest {

  private static final int REQUESTS = 2;
  private static final long REUSE_IDLE_PERIOD_MS = 100;
  private static final long SHORT_IDLE_TIME_MS = 25;
  private static final long EXPIRY_WAIT_MS = 200;

  private SocketTrackingHttpServer server;

  @Before
  public void setUp() throws IOException {
    server = new SocketTrackingHttpServer(request -> SocketTrackingHttpServer.Response.text("OK"));
    server.start();
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  /** Verifies that the ten-minute default does not replace a briefly idle connection. */
  @Test(timeout = 30000)
  public void testDefaultIdleTimeReusesConnection() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(1).build();
    assertEquals(
        "Default connectionMaxIdleTimeMs should be ten minutes",
        AlternatorConfig.DEFAULT_CONNECTION_MAX_IDLE_TIME_MS,
        config.getConnectionMaxIdleTimeMs());

    assertConnectionReused(config, REUSE_IDLE_PERIOD_MS);
  }

  /** Verifies that an explicit zero does not immediately expire a connection. */
  @Test(timeout = 30000)
  public void testZeroIdleTimeDoesNotImmediatelyExpireConnection() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withMaxConnections(1).withConnectionMaxIdleTimeMs(0).build();
    assertEquals("connectionMaxIdleTimeMs should be 0", 0, config.getConnectionMaxIdleTimeMs());

    assertConnectionReused(config, REUSE_IDLE_PERIOD_MS);
  }

  /** Verifies that a connection older than a positive idle timeout is replaced. */
  @Test(timeout = 30000)
  public void testPositiveIdleTimeExpiresConnection() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(1)
            .withConnectionMaxIdleTimeMs(SHORT_IDLE_TIME_MS)
            .build();

    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    try {
      executeRequest(client);
      Thread.sleep(EXPIRY_WAIT_MS);
      executeRequest(client);
      assertConnectionIdentity(2);
    } finally {
      client.close();
    }
  }

  /** Verifies that a connection older than a positive TTL is replaced. */
  @Test(timeout = 30000)
  public void testPositiveTtlExpiresConnection() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(1)
            .withConnectionTimeToLiveMs(SHORT_IDLE_TIME_MS)
            .build();

    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    try {
      executeRequest(client);
      Thread.sleep(EXPIRY_WAIT_MS);
      executeRequest(client);
      assertConnectionIdentity(2);
    } finally {
      client.close();
    }
  }

  private void assertConnectionReused(AlternatorConfig config, long idlePeriodMs) throws Exception {
    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    try {
      executeRequest(client);
      Thread.sleep(idlePeriodMs);
      executeRequest(client);
      assertConnectionIdentity(1);
    } finally {
      client.close();
    }
  }

  private void assertConnectionIdentity(int expectedConnections) throws Exception {
    assertTrue(
        "Both requests should reach the server",
        server.awaitRequestCount(REQUESTS, 5, TimeUnit.SECONDS));
    server.assertHealthy();

    List<SocketTrackingHttpServer.Request> requests = server.requestsSince(0);
    assertEquals("Both requests should reach the server", REQUESTS, server.requestCount());
    assertEquals(
        "Unexpected number of accepted TCP connections",
        expectedConnections,
        server.acceptedConnections());
    assertEquals(
        "Unexpected number of client TCP ports",
        expectedConnections,
        server.uniqueRemotePorts().size());
    assertEquals("A remote port should be recorded for each request", REQUESTS, requests.size());
    if (expectedConnections == 1) {
      assertEquals(
          "The idle connection should be reused",
          requests.get(0).remotePort(),
          requests.get(1).remotePort());
    } else {
      assertNotEquals(
          "The expired connection should be replaced",
          requests.get(0).remotePort(),
          requests.get(1).remotePort());
    }
  }

  private void executeRequest(SdkHttpClient client) throws Exception {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(server.uri().resolve("/test"))
            .method(SdkHttpMethod.GET)
            .putHeader("Connection", "keep-alive")
            .build();
    HttpExecuteResponse response =
        client.prepareRequest(HttpExecuteRequest.builder().request(request).build()).call();
    assertEquals("Request should succeed with 200", 200, response.httpResponse().statusCode());

    if (response.responseBody().isPresent()) {
      drainAndClose(response.responseBody().get());
    }
  }

  private void drainAndClose(AbortableInputStream body) throws IOException {
    try (AbortableInputStream stream = body) {
      byte[] buffer = new byte[256];
      while (stream.read(buffer) != -1) {
        // Drain the response so the connection is returned to the pool.
      }
    }
  }
}
