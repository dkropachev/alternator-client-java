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

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.metrics.MetricCollector;

/**
 * Unit tests for HeadersFilteringSdkAsyncHttpClient.
 *
 * @author dmitry.kropachev
 */
public class HeadersFilteringSdkAsyncHttpClientTest {

  /** Mock SdkAsyncHttpClient that captures the request for inspection. */
  private static class MockSdkAsyncHttpClient implements SdkAsyncHttpClient {
    SdkHttpRequest capturedRequest;
    AsyncExecuteRequest capturedExecuteRequest;
    boolean closeCalled = false;
    String clientNameValue = "MockSdkAsyncHttpClient";

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      this.capturedRequest = request.request();
      this.capturedExecuteRequest = request;
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      closeCalled = true;
    }

    @Override
    public String clientName() {
      return clientNameValue;
    }
  }

  /** Mock response handler for async requests. */
  private static class MockResponseHandler implements SdkAsyncHttpResponseHandler {
    @Override
    public void onHeaders(software.amazon.awssdk.http.SdkHttpResponse response) {}

    @Override
    public void onStream(org.reactivestreams.Publisher<java.nio.ByteBuffer> stream) {}

    @Override
    public void onError(Throwable error) {}
  }

  private AsyncExecuteRequest createRequest(SdkHttpRequest httpRequest) {
    return AsyncExecuteRequest.builder()
        .request(httpRequest)
        .responseHandler(new MockResponseHandler())
        .build();
  }

  @Test
  @CoversRequirements("HEAD-REQ-006")
  public void testFiltersNonWhitelistedHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", "Authorization"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "AWS4-HMAC-SHA256...")
            .appendHeader("User-Agent", "aws-sdk-java/2.x")
            .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id")
            .appendHeader("amz-sdk-request", "attempt=1")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // Whitelisted headers should be present
    assertTrue(mockClient.capturedRequest.headers().containsKey("Host"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Authorization"));

    // Non-whitelisted headers should be removed
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("amz-sdk-request"));
  }

  @Test
  public void testCaseInsensitiveMatching() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Content-Type", "authorization"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("content-type", "application/json")
            .appendHeader("AUTHORIZATION", "Bearer token")
            .appendHeader("User-Agent", "test")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // Headers should be preserved with original case (case-insensitive matching)
    assertTrue(mockClient.capturedRequest.headers().containsKey("content-type"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("AUTHORIZATION"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
  }

  @Test
  public void testPreservesMultiValueHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Accept-Encoding"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Accept-Encoding", "gzip")
            .appendHeader("Accept-Encoding", "deflate")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    java.util.List<String> values = mockClient.capturedRequest.headers().get("Accept-Encoding");
    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.contains("gzip"));
    assertTrue(values.contains("deflate"));
  }

  @Test
  public void testPreservesRequestUriAndMethod() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    URI originalUri = URI.create("https://localhost:8043/path?query=value");
    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(originalUri)
            .appendHeader("Host", "localhost:8043")
            .appendHeader("User-Agent", "test")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // URI and method should be preserved
    assertEquals(originalUri, mockClient.capturedRequest.getUri());
    assertEquals(SdkHttpMethod.POST, mockClient.capturedRequest.method());
  }

  @Test
  public void testFiltersSdkMetadataHeadersExceptUserAgent() {
    // Use the default required headers whitelist
    AlternatorConfig config = AlternatorConfig.builder().authenticationEnabled(true).build();
    Set<String> whitelist = config.getRequiredHeaders();

    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("X-Amz-Target", "DynamoDB_20120810.GetItem")
            .appendHeader("Content-Type", "application/x-amz-json-1.0")
            .appendHeader("Content-Length", "123")
            .appendHeader("Authorization", "AWS4-HMAC-SHA256...")
            .appendHeader("X-Amz-Date", "20240101T000000Z")
            .appendHeader("Accept-Encoding", "gzip")
            .appendHeader("User-Agent", "aws-sdk-java/2.x")
            // SDK metadata headers that should be filtered
            .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id")
            .appendHeader("amz-sdk-request", "attempt=1")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // Required headers should be preserved
    assertTrue(mockClient.capturedRequest.headers().containsKey("Host"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("X-Amz-Target"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Content-Type"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Content-Length"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Authorization"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("X-Amz-Date"));
    // Accept-Encoding is preserved only when response compression is enabled.
    assertFalse(mockClient.capturedRequest.headers().containsKey("Accept-Encoding"));

    // User-Agent should be preserved for driver reporting; other SDK metadata is filtered
    assertTrue(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertEquals(
        "aws-sdk-java/2.x", mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
    assertFalse(mockClient.capturedRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("amz-sdk-request"));
  }

  @Test
  public void testCloseDelegatesToWrappedClient() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, new HashSet<>());

    assertFalse(mockClient.closeCalled);
    filteringClient.close();
    assertTrue(mockClient.closeCalled);
  }

  @Test
  public void testClientNameDelegatesToWrappedClient() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    mockClient.clientNameValue = "CustomClientName";
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, new HashSet<>());

    assertEquals("CustomClientName", filteringClient.clientName());
  }

  @Test
  public void testEmptyWhitelistFiltersAllHeaders() {
    Set<String> whitelist = new HashSet<>();
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "test")
            .appendHeader("User-Agent", "test")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // All headers should be filtered
    assertTrue(mockClient.capturedRequest.headers().isEmpty());
  }

  @Test
  public void testNullWhitelistFiltersAllHeaders() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, null);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "test")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // All headers should be filtered
    assertTrue(mockClient.capturedRequest.headers().isEmpty());
  }

  @Test
  public void testRequestWithNoHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", "Authorization"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // Should handle empty headers gracefully
    assertTrue(mockClient.capturedRequest.headers().isEmpty());
  }

  @Test
  public void testPreservesFullDuplexFlag() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(originalRequest)
            .responseHandler(new MockResponseHandler())
            .fullDuplex(true)
            .build();

    filteringClient.execute(executeRequest);

    // fullDuplex flag should be preserved
    assertTrue(mockClient.capturedExecuteRequest.fullDuplex());
  }

  @Test
  @CoversRequirements("HEAD-REQ-005")
  public void testPreservesMetricCollector() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    MetricCollector collector = MetricCollector.create("test");
    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(originalRequest)
            .responseHandler(new MockResponseHandler())
            .metricCollector(collector)
            .build();

    filteringClient.execute(executeRequest);

    // MetricCollector should be preserved
    assertTrue(mockClient.capturedExecuteRequest.metricCollector().isPresent());
    assertEquals(collector, mockClient.capturedExecuteRequest.metricCollector().get());
  }

  @Test
  public void testHandlesNullMetricCollector() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(originalRequest)
            .responseHandler(new MockResponseHandler())
            .build();

    filteringClient.execute(executeRequest);

    // Should handle null metric collector gracefully
    assertFalse(mockClient.capturedExecuteRequest.metricCollector().isPresent());
  }

  @Test
  public void testReturnsCompletableFutureFromDelegate() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    CompletableFuture<Void> result = filteringClient.execute(createRequest(originalRequest));

    assertNotNull(result);
    assertTrue(result.isDone());
  }

  @Test
  public void testWhitelistIgnoresNullAndEmptyEntries() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", null, "", "Authorization"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "test")
            .appendHeader("User-Agent", "test")
            .build();

    filteringClient.execute(createRequest(originalRequest));

    // Valid whitelist entries should work
    assertTrue(mockClient.capturedRequest.headers().containsKey("Host"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Authorization"));
    // Non-whitelisted headers should be filtered
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
  }

  @Test
  public void testPreservesResponseHandler() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    MockResponseHandler handler = new MockResponseHandler();
    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder().request(originalRequest).responseHandler(handler).build();

    filteringClient.execute(executeRequest);

    // Response handler should be preserved
    assertSame(handler, mockClient.capturedExecuteRequest.responseHandler());
  }

  @Test
  public void testPreservesRequestContentPublisher() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    HeadersFilteringSdkAsyncHttpClient filteringClient =
        new HeadersFilteringSdkAsyncHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    software.amazon.awssdk.http.async.SdkHttpContentPublisher contentPublisher =
        new software.amazon.awssdk.http.async.SdkHttpContentPublisher() {
          @Override
          public java.util.Optional<Long> contentLength() {
            return java.util.Optional.of(4L);
          }

          @Override
          public void subscribe(org.reactivestreams.Subscriber<? super java.nio.ByteBuffer> s) {
            // No-op for test
          }
        };

    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(originalRequest)
            .responseHandler(new MockResponseHandler())
            .requestContentPublisher(contentPublisher)
            .build();

    filteringClient.execute(executeRequest);

    // Content publisher should be preserved
    assertSame(contentPublisher, mockClient.capturedExecuteRequest.requestContentPublisher());
  }
}
