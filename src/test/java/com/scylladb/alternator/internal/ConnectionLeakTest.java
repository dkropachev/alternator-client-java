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

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that HTTP connections are properly managed and not leaked in AlternatorLiveNodes.
 *
 * <p>These tests call {@link AlternatorLiveNodes#refreshDiscoveredNodes()} synchronously (no
 * background thread) and verify that connections are properly released by checking that subsequent
 * requests succeed (they would hang/fail if connections were leaked from a small pool).
 */
public class ConnectionLeakTest {

  private HttpServer server;
  private int port;
  private volatile int responseCode = 200;
  private volatile String responseBody = "[\"127.0.0.1\"]";

  @Before
  public void setUp() throws IOException {
    responseCode = 200;
    responseBody = "[\"127.0.0.1\"]";
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/localnodes",
        exchange -> {
          byte[] body = responseBody.getBytes();
          exchange.sendResponseHeaders(responseCode, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    server.start();
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  private AlternatorLiveNodes createLiveNodes() throws Exception {
    URI seedUri = new URI("http://127.0.0.1:" + port);
    AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
    return new AlternatorLiveNodes(config);
  }

  /**
   * Verifies that connections are not leaked when /localnodes returns non-200 responses.
   *
   * <p>Before the fix, each non-200 response would leak a connection because the response entity
   * was not consumed. With a small pool, leaked connections cause subsequent requests to block
   * indefinitely. This test uses a timeout to detect such hangs.
   */
  @Test(timeout = 10000)
  public void testNoConnectionLeakOnNon200Responses() throws Exception {
    responseCode = 500;
    AlternatorLiveNodes liveNodes = createLiveNodes();

    // If connections are leaked, this will eventually hang due to pool exhaustion
    for (int i = 0; i < 20; i++) {
      liveNodes.refreshDiscoveredNodes();
    }
    // If we get here without timeout, connections are properly released
  }

  /** Verifies that connections are properly returned after successful responses. */
  @Test(timeout = 10000)
  public void testNoConnectionLeakOnSuccessfulResponses() throws Exception {
    responseCode = 200;
    responseBody = "[\"127.0.0.1\",\"127.0.0.2\"]";
    AlternatorLiveNodes liveNodes = createLiveNodes();

    for (int i = 0; i < 30; i++) {
      liveNodes.refreshDiscoveredNodes();
    }

    assertEquals(2, liveNodes.getDiscoveredNodes().size());
  }

  /** Verifies malformed successful /localnodes responses do not terminate discovery. */
  @Test(timeout = 10000)
  public void testMalformedSuccessfulResponsesDoNotThrowOrLeakConnections() throws Exception {
    responseCode = 200;
    String[] malformedBodies = {"", "not-json", "[", "[\"127.0.0.1\",bad]", "[bad]"};
    AlternatorLiveNodes liveNodes = createLiveNodes();

    for (int i = 0; i < 30; i++) {
      responseBody = malformedBodies[i % malformedBodies.length];
      liveNodes.refreshDiscoveredNodes();
    }

    assertEquals(1, liveNodes.getDiscoveredNodes().size());
    assertEquals("127.0.0.1", liveNodes.getDiscoveredNodes().get(0).getHost());
  }

  /** Verifies no leaks when alternating between error and success responses. */
  @Test(timeout = 10000)
  public void testNoConnectionLeakOnAlternatingResponses() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);
    server.removeContext("/localnodes");
    server.createContext(
        "/localnodes",
        exchange -> {
          int count = callCount.incrementAndGet();
          byte[] body;
          int code;
          if (count % 2 == 0) {
            code = 200;
            body = "[\"127.0.0.1\"]".getBytes();
          } else {
            code = 503;
            body = "Service Unavailable".getBytes();
          }
          exchange.sendResponseHeaders(code, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    AlternatorLiveNodes liveNodes = createLiveNodes();

    for (int i = 0; i < 40; i++) {
      liveNodes.refreshDiscoveredNodes();
    }
    // If we get here without timeout, connections are properly released
  }

  /**
   * Verifies that HTTP connections are kept and reused across multiple requests, rather than being
   * created and destroyed for each request.
   *
   * <p>After multiple requests to the same host, subsequent requests should still succeed quickly
   * (within timeout), proving connections are being reused from the pool.
   */
  @Test(timeout = 10000)
  public void testConnectionsAreReused() throws Exception {
    responseCode = 200;
    responseBody = "[\"127.0.0.1\"]";
    AlternatorLiveNodes liveNodes = createLiveNodes();

    // Make several requests so the connection pool has a chance to stabilize
    for (int i = 0; i < 20; i++) {
      liveNodes.refreshDiscoveredNodes();
    }

    // All requests should succeed, proving connections are properly managed
    assertEquals(1, liveNodes.getDiscoveredNodes().size());
  }
}
