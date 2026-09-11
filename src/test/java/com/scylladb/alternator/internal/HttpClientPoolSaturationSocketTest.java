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
import static org.junit.Assert.fail;

import com.scylladb.alternator.AlternatorConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
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

/** Verifies saturated pools do not evict their leased physical connection. */
@RunWith(Parameterized.class)
public class HttpClientPoolSaturationSocketTest {
  private enum Transport {
    APACHE_SYNC,
    CRT_SYNC,
    NETTY_ASYNC,
    CRT_ASYNC
  }

  private final Transport transport;

  public HttpClientPoolSaturationSocketTest(Transport transport) {
    this.transport = transport;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> transports() {
    return Arrays.asList(
        new Object[][] {
          {Transport.APACHE_SYNC},
          {Transport.CRT_SYNC},
          {Transport.NETTY_ASYNC},
          {Transport.CRT_ASYNC}
        });
  }

  @Test(timeout = 30_000)
  public void acquisitionTimeoutDoesNotReplaceLeasedConnection() throws Exception {
    CountDownLatch firstRequestAtServer = new CountDownLatch(1);
    CountDownLatch releaseFirstResponse = new CountDownLatch(1);
    AtomicInteger responseCount = new AtomicInteger();
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            request -> {
              if (responseCount.getAndIncrement() == 0) {
                firstRequestAtServer.countDown();
                if (!releaseFirstResponse.await(10, TimeUnit.SECONDS)) {
                  throw new IOException("Timed out waiting to release the first response");
                }
              }
              return SocketTrackingHttpServer.Response.text("OK");
            })) {
      server.start();
      ExecutorService syncExecutor = Executors.newFixedThreadPool(2);
      TransportClient client = null;
      try {
        client = openClient(server, syncExecutor);
        CompletableFuture<Void> firstRequest = client.execute();
        assertTrue(
            "the first request should lease the only connection",
            firstRequestAtServer.await(5, TimeUnit.SECONDS));

        CompletableFuture<Void> blockedRequest = client.execute();
        assertAcquisitionTimeout(blockedRequest);
        assertEquals(
            "pool saturation must not create or evict a physical connection",
            1,
            server.acceptedConnections());
        assertEquals("the blocked request must not reach the server", 1, server.requestCount());

        releaseFirstResponse.countDown();
        firstRequest.get(5, TimeUnit.SECONDS);
        client.execute().get(5, TimeUnit.SECONDS);

        List<SocketTrackingHttpServer.Request> requests = server.requestsSince(0);
        assertEquals(2, requests.size());
        assertEquals(
            "the released connection should return to the pool",
            requests.get(0).remotePort(),
            requests.get(1).remotePort());
        assertEquals(1, server.acceptedConnections());
        assertEquals(1, server.uniqueRemotePorts().size());
        server.assertHealthy();
      } finally {
        releaseFirstResponse.countDown();
        try {
          if (client != null) {
            client.close();
          }
        } finally {
          syncExecutor.shutdownNow();
        }
      }
    }
  }

  private TransportClient openClient(
      SocketTrackingHttpServer server, ExecutorService syncExecutor) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(1)
            .withConnectionAcquisitionTimeoutMs(250)
            .withConnectionTimeoutMs(2_000)
            .build();
    switch (transport) {
      case APACHE_SYNC:
        return syncClient(ApacheSyncClientFactory.create(null, config, null), server, syncExecutor);
      case CRT_SYNC:
        return syncClient(CrtSyncClientFactory.create(null, config, null), server, syncExecutor);
      case NETTY_ASYNC:
        return asyncClient(NettyAsyncClientFactory.create(null, config, null), server);
      case CRT_ASYNC:
        return asyncClient(CrtAsyncClientFactory.create(null, config, null), server);
      default:
        throw new IllegalStateException("Unknown transport: " + transport);
    }
  }

  private TransportClient syncClient(
      SdkHttpClient client, SocketTrackingHttpServer server, ExecutorService syncExecutor) {
    return new TransportClient() {
      @Override
      public CompletableFuture<Void> execute() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        syncExecutor.execute(
            () -> {
              try {
                executeSyncRequest(client, server);
                result.complete(null);
              } catch (Throwable error) {
                result.completeExceptionally(error);
              }
            });
        return result;
      }

      @Override
      public void close() {
        client.close();
      }
    };
  }

  private TransportClient asyncClient(SdkAsyncHttpClient client, SocketTrackingHttpServer server) {
    return new TransportClient() {
      @Override
      public CompletableFuture<Void> execute() {
        return executeAsyncRequest(client, server);
      }

      @Override
      public void close() {
        client.close();
      }
    };
  }

  private void executeSyncRequest(SdkHttpClient client, SocketTrackingHttpServer server)
      throws Exception {
    HttpExecuteResponse response =
        client
            .prepareRequest(HttpExecuteRequest.builder().request(httpRequest(server)).build())
            .call();
    assertEquals(200, response.httpResponse().statusCode());
    if (response.responseBody().isPresent()) {
      drainAndClose(response.responseBody().get());
    }
  }

  private CompletableFuture<Void> executeAsyncRequest(
      SdkAsyncHttpClient client, SocketTrackingHttpServer server) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    AtomicInteger statusCode = new AtomicInteger(-1);
    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(httpRequest(server))
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
                            if (statusCode.get() == 200) {
                              result.complete(null);
                            } else {
                              result.completeExceptionally(
                                  new AssertionError("Unexpected status: " + statusCode.get()));
                            }
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

  private SdkHttpRequest httpRequest(SocketTrackingHttpServer server) {
    return SdkHttpRequest.builder()
        .uri(server.uri().resolve("/test"))
        .method(SdkHttpMethod.GET)
        .putHeader("Connection", "keep-alive")
        .build();
  }

  private void drainAndClose(AbortableInputStream body) throws IOException {
    try (AbortableInputStream stream = body) {
      byte[] buffer = new byte[256];
      while (stream.read(buffer) != -1) {
        // Drain the response so the connection returns to the pool.
      }
    }
  }

  private void assertAcquisitionTimeout(CompletableFuture<Void> request) throws Exception {
    try {
      request.get(5, TimeUnit.SECONDS);
      fail("Expected the saturated connection pool acquisition to time out");
    } catch (ExecutionException expected) {
      String failure = fullFailureMessage(expected).toLowerCase();
      assertTrue(
          "failure should identify connection acquisition timeout: " + failure,
          failure.contains("timeout")
              || failure.contains("timed out")
              || failure.contains("acquir")
              || failure.contains("pending"));
    }
  }

  private String fullFailureMessage(Throwable error) {
    StringBuilder result = new StringBuilder();
    Throwable current = error;
    while (current != null) {
      result.append(current.getClass().getSimpleName()).append(':');
      if (current.getMessage() != null) {
        result.append(current.getMessage());
      }
      result.append(' ');
      current = current.getCause();
    }
    return result.toString();
  }

  private interface TransportClient extends AutoCloseable {
    CompletableFuture<Void> execute();

    @Override
    void close();
  }

  private static final class EmptyPublisher implements SdkHttpContentPublisher {
    @Override
    public Optional<Long> contentLength() {
      return Optional.of(0L);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            @Override
            public void request(long count) {
              subscriber.onComplete();
            }

            @Override
            public void cancel() {}
          });
    }
  }
}
