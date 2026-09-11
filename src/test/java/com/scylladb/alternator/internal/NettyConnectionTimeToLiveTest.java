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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

/**
 * Tests Netty connection identity with unlimited and short connection time-to-live settings.
 *
 * <p>Uses a raw loopback HTTP server so a successful replacement connection cannot masquerade as
 * connection reuse.
 */
public class NettyConnectionTimeToLiveTest {

  private static final int REQUESTS = 2;
  private static final long REUSE_IDLE_PERIOD_MS = 100;
  private static final long SHORT_TTL_MS = 25;
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

  /**
   * Verifies that the default config (connectionTimeToLiveMs=0) creates a working client where
   * Duration.ZERO means "unlimited TTL", not "instant connection expiry".
   */
  @Test(timeout = 30000)
  public void testZeroTtlMeansUnlimited() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(1).build();
    assertEquals(
        "Default connectionTimeToLiveMs should be 0", 0, config.getConnectionTimeToLiveMs());

    assertConnectionReused(config);
  }

  /** Verifies that explicit idle zero does not immediately expire a connection. */
  @Test(timeout = 30000)
  public void testZeroIdleTimeDoesNotImmediatelyExpireConnection() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withMaxConnections(1).withConnectionMaxIdleTimeMs(0).build();

    assertConnectionReused(config);
  }

  /** Verifies that a connection older than a positive TTL is replaced. */
  @Test(timeout = 30000)
  public void testShortTtlExpireConnections() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(1)
            .withConnectionTimeToLiveMs(SHORT_TTL_MS)
            .build();
    assertEquals(SHORT_TTL_MS, config.getConnectionTimeToLiveMs());

    SdkAsyncHttpClient client = NettyAsyncClientFactory.create(null, config, null);
    try {
      executeRequest(client);
      Thread.sleep(EXPIRY_WAIT_MS);
      executeRequest(client);
      assertConnectionIdentity(2);
    } finally {
      client.close();
    }
  }

  private void assertConnectionReused(AlternatorConfig config) throws Exception {
    SdkAsyncHttpClient client = NettyAsyncClientFactory.create(null, config, null);
    try {
      executeRequest(client);
      Thread.sleep(REUSE_IDLE_PERIOD_MS);
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
          "The unlimited-TTL connection should be reused",
          requests.get(0).remotePort(),
          requests.get(1).remotePort());
    } else {
      assertNotEquals(
          "The expired connection should be replaced",
          requests.get(0).remotePort(),
          requests.get(1).remotePort());
    }
  }

  private void executeRequest(SdkAsyncHttpClient client) throws Exception {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(server.uri().resolve("/test"))
            .method(SdkHttpMethod.GET)
            .putHeader("Connection", "keep-alive")
            .build();
    CompletableFuture<Integer> statusFuture = new CompletableFuture<>();
    AtomicInteger statusCode = new AtomicInteger(-1);

    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(request)
            .requestContentPublisher(new EmptyPublisher())
            .responseHandler(
                new SdkAsyncHttpResponseHandler() {
                  @Override
                  public void onHeaders(SdkHttpResponse headers) {
                    statusCode.set(headers.statusCode());
                  }

                  @Override
                  public void onStream(Publisher<ByteBuffer> stream) {
                    stream.subscribe(
                        new Subscriber<ByteBuffer>() {
                          @Override
                          public void onSubscribe(Subscription s) {
                            s.request(Long.MAX_VALUE);
                          }

                          @Override
                          public void onNext(ByteBuffer byteBuffer) {}

                          @Override
                          public void onError(Throwable error) {
                            statusFuture.completeExceptionally(error);
                          }

                          @Override
                          public void onComplete() {
                            statusFuture.complete(statusCode.get());
                          }
                        });
                  }

                  @Override
                  public void onError(Throwable error) {
                    statusFuture.completeExceptionally(error);
                  }
                })
            .build();

    client.execute(executeRequest).get(5, TimeUnit.SECONDS);
    int status = statusFuture.get(5, TimeUnit.SECONDS);
    assertEquals("Request should succeed with 200", 200, status);
  }

  /** An empty request body publisher for GET requests. */
  private static class EmptyPublisher implements SdkHttpContentPublisher {
    @Override
    public Optional<Long> contentLength() {
      return Optional.of(0L);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {
              subscriber.onComplete();
            }

            @Override
            public void cancel() {}
          });
    }
  }
}
