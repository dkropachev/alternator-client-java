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
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

/**
 * Verifies configured HTTP clients keep reusable TCP connections after drained non-2xx responses.
 */
public class HttpClientNon2xxConnectionReuseTest {

  private static final int REQUESTS = 5;
  private static final int[] NON_2XX_STATUSES = {400, 500};

  @Test(timeout = 30000)
  public void apacheSyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertSyncClientReusesConnections(
        "apache-sync", ApacheSyncClientFactory.create(null, config(), null));
  }

  @Test(timeout = 30000)
  public void crtSyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertSyncClientReusesConnections(
        "crt-sync", CrtSyncClientFactory.create(null, config(), null));
  }

  @Test(timeout = 30000)
  public void nettyAsyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertAsyncClientReusesConnections(
        "netty-async", NettyAsyncClientFactory.create(null, config(), null));
  }

  @Test(timeout = 30000)
  public void crtAsyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertAsyncClientReusesConnections(
        "crt-async", CrtAsyncClientFactory.create(null, config(), null));
  }

  private AlternatorConfig config() {
    return AlternatorConfig.builder()
        .withMaxConnections(1)
        .withConnectionAcquisitionTimeoutMs(5_000)
        .withConnectionMaxIdleTimeMs(60_000)
        .withConnectionTimeoutMs(5_000)
        .build();
  }

  private void assertSyncClientReusesConnections(String clientName, SdkHttpClient client)
      throws Exception {
    try {
      for (int status : NON_2XX_STATUSES) {
        assertSyncClientReusesConnections(clientName, status, client);
      }
    } finally {
      client.close();
    }
  }

  private void assertSyncClientReusesConnections(
      String clientName, int status, SdkHttpClient client) throws Exception {
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            request -> SocketTrackingHttpServer.Response.status(status, "response-" + status))) {
      server.start();
      for (int i = 0; i < REQUESTS; i++) {
        HttpExecuteResponse response =
            client
                .prepareRequest(HttpExecuteRequest.builder().request(request(server)).build())
                .call();
        assertEquals(status, response.httpResponse().statusCode());
        if (response.responseBody().isPresent()) {
          drainAndClose(response.responseBody().get());
        }
      }
      assertConnectionReuse(clientName, status, server);
    }
  }

  private void assertAsyncClientReusesConnections(String clientName, SdkAsyncHttpClient client)
      throws Exception {
    try {
      for (int status : NON_2XX_STATUSES) {
        assertAsyncClientReusesConnections(clientName, status, client);
      }
    } finally {
      client.close();
    }
  }

  private void assertAsyncClientReusesConnections(
      String clientName, int status, SdkAsyncHttpClient client) throws Exception {
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            request -> SocketTrackingHttpServer.Response.status(status, "response-" + status))) {
      server.start();
      for (int i = 0; i < REQUESTS; i++) {
        assertEquals(
            status, executeAsync(client, request(server)).get(10, TimeUnit.SECONDS).intValue());
      }
      assertConnectionReuse(clientName, status, server);
    }
  }

  private void assertConnectionReuse(String clientName, int status, SocketTrackingHttpServer server)
      throws Exception {
    assertTrue(
        clientName + " status " + status + " should reach the server",
        server.awaitRequestCount(REQUESTS, 5, TimeUnit.SECONDS));
    server.assertHealthy();
    assertEquals(
        clientName + " status " + status + " should reach the server",
        REQUESTS,
        server.requestCount());
    if (allowsCrtSyncServerErrorReconnect(clientName, status)) {
      // AWS CRT sync documents 5xx server errors as a condition where a connection may be
      // closed instead of returned to the pool. The native path does not rotate on every 5xx
      // in this short probe, but CI has observed one replacement connection across five
      // sequential 500 responses.
      assertTrue(
          clientName + " status " + status + " should reuse TCP connections",
          server.acceptedConnections() <= 2);
      assertTrue(
          clientName + " status " + status + " should reuse client TCP ports",
          server.uniqueRemotePorts().size() <= 2);
      return;
    }
    assertEquals(
        clientName + " status " + status + " should reuse one TCP connection",
        1,
        server.acceptedConnections());
    assertEquals(
        clientName + " status " + status + " should use one client TCP port",
        1,
        server.uniqueRemotePorts().size());
  }

  private SdkHttpRequest request(SocketTrackingHttpServer server) {
    return SdkHttpRequest.builder()
        .uri(URI.create("http://127.0.0.1:" + server.port() + "/test"))
        .method(SdkHttpMethod.GET)
        .putHeader("Connection", "keep-alive")
        .build();
  }

  private boolean allowsCrtSyncServerErrorReconnect(String clientName, int status) {
    return "crt-sync".equals(clientName) && status >= 500;
  }

  private void drainAndClose(AbortableInputStream body) throws IOException {
    try (AbortableInputStream stream = body) {
      byte[] buffer = new byte[1024];
      while (stream.read(buffer) != -1) {
        // Drain response so the connection can return to the pool.
      }
    }
  }

  private CompletableFuture<Integer> executeAsync(
      SdkAsyncHttpClient client, SdkHttpRequest request) {
    CompletableFuture<Integer> result = new CompletableFuture<>();
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
                          public void onSubscribe(Subscription subscription) {
                            subscription.request(Long.MAX_VALUE);
                          }

                          @Override
                          public void onNext(ByteBuffer byteBuffer) {}

                          @Override
                          public void onError(Throwable error) {
                            result.completeExceptionally(error);
                          }

                          @Override
                          public void onComplete() {
                            result.complete(statusCode.get());
                          }
                        });
                  }

                  @Override
                  public void onError(Throwable error) {
                    result.completeExceptionally(error);
                  }
                })
            .build();

    client
        .execute(executeRequest)
        .whenComplete(
            (ignored, error) -> {
              if (error != null) {
                result.completeExceptionally(error);
              }
            });
    return result;
  }

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
