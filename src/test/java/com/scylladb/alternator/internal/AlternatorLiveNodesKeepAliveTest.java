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
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Unit tests verifying that AlternatorLiveNodes sends the {@code Connection: keep-alive} header on
 * its polling requests to {@code /localnodes}.
 */
public class AlternatorLiveNodesKeepAliveTest {

  /**
   * Mock SdkHttpClient that captures outgoing requests and returns a valid /localnodes JSON
   * response so that refreshDiscoveredNodes() succeeds.
   */
  private static class CapturingHttpClient implements SdkHttpClient {
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequests.add(request.httpRequest());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          byte[] body = "[\"127.0.0.1\"]".getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "CapturingHttpClient";
    }
  }

  @Test
  public void testPollingRequestIncludesConnectionKeepAliveHeader() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    // Trigger a polling cycle
    liveNodes.refreshDiscoveredNodes();

    assertFalse(
        "Should have captured at least one request", capturingClient.capturedRequests.isEmpty());

    for (SdkHttpRequest req : capturingClient.capturedRequests) {
      List<String> connectionValues = req.headers().get("Connection");
      assertNotNull(
          "Connection header should be present on polling request to " + req.encodedPath(),
          connectionValues);
      assertTrue(
          "Connection header should contain 'keep-alive', got: " + connectionValues,
          connectionValues.stream().anyMatch(v -> v.equalsIgnoreCase("keep-alive")));
    }
  }

  @Test
  public void testMultiplePollingRequestsAllIncludeKeepAlive() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    // Multiple polling cycles
    for (int i = 0; i < 5; i++) {
      liveNodes.refreshDiscoveredNodes();
    }

    assertTrue(
        "Should have captured multiple requests", capturingClient.capturedRequests.size() >= 5);

    for (SdkHttpRequest req : capturingClient.capturedRequests) {
      List<String> connectionValues = req.headers().get("Connection");
      assertNotNull(
          "Connection header should be present on every polling request", connectionValues);
      assertTrue(
          "Connection header should contain 'keep-alive' on every polling request",
          connectionValues.stream().anyMatch(v -> v.equalsIgnoreCase("keep-alive")));
    }
  }

  @Test
  public void testPollingRequestIncludesConnectionKeepAliveHeaderForHttps() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("https://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    // Trigger a polling cycle
    liveNodes.refreshDiscoveredNodes();

    assertFalse(
        "Should have captured at least one request", capturingClient.capturedRequests.isEmpty());

    for (SdkHttpRequest req : capturingClient.capturedRequests) {
      assertEquals("Request should use HTTPS scheme", "https", req.protocol());
      List<String> connectionValues = req.headers().get("Connection");
      assertNotNull(
          "Connection header should be present on HTTPS polling request to " + req.encodedPath(),
          connectionValues);
      assertTrue(
          "Connection header should contain 'keep-alive' for HTTPS, got: " + connectionValues,
          connectionValues.stream().anyMatch(v -> v.equalsIgnoreCase("keep-alive")));
    }
  }

  @Test
  public void testMultipleHttpsPollingRequestsAllIncludeKeepAlive() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("https://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    // Multiple polling cycles
    for (int i = 0; i < 5; i++) {
      liveNodes.refreshDiscoveredNodes();
    }

    assertTrue(
        "Should have captured multiple requests", capturingClient.capturedRequests.size() >= 5);

    for (SdkHttpRequest req : capturingClient.capturedRequests) {
      assertEquals("Request should use HTTPS scheme", "https", req.protocol());
      List<String> connectionValues = req.headers().get("Connection");
      assertNotNull(
          "Connection header should be present on every HTTPS polling request", connectionValues);
      assertTrue(
          "Connection header should contain 'keep-alive' on every HTTPS polling request",
          connectionValues.stream().anyMatch(v -> v.equalsIgnoreCase("keep-alive")));
    }
  }
}
