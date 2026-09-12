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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute;
import software.amazon.awssdk.core.internal.metrics.RequestBodyMetrics;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

public class RetrySigningMetricsTest {
  @Test
  public void syncRetryCountsOnlyBytesReadByTransport() throws Exception {
    MetricsInterceptor metrics = new MetricsInterceptor();
    RecordingSyncClient transport = new RecordingSyncClient();
    List<URI> nodes = nodes();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(Arrays.asList("127.0.0.1", "127.0.0.2"))
            .withNodeHealthDisabled()
            .credentialsProvider(credentials())
            .httpClient(transport)
            .overrideConfiguration(override(metrics))
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build());
    }

    assertEquals(transport.bodySizes, metrics.bytesWritten);
  }

  @Test
  public void asyncRetryStartsWriteMetricsAtTransportSubscription() throws Exception {
    MetricsInterceptor metrics = new MetricsInterceptor();
    RecordingAsyncClient transport = new RecordingAsyncClient();
    List<URI> nodes = nodes();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(Arrays.asList("127.0.0.1", "127.0.0.2"))
            .withNodeHealthDisabled()
            .credentialsProvider(credentials())
            .httpClient(transport)
            .overrideConfiguration(override(metrics))
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build()).join();
    }

    assertEquals(transport.bodySizes, metrics.bytesWritten);
    assertEquals(transport.subscriptionStartedNanos.size(), metrics.firstByteWrittenNanos.size());
    for (int i = 0; i < transport.subscriptionStartedNanos.size(); i++) {
      assertTrue(
          "write timing must start after the transport subscribes",
          metrics.firstByteWrittenNanos.get(i) >= transport.subscriptionStartedNanos.get(i));
    }
  }

  private static ClientOverrideConfiguration override(ExecutionInterceptor metrics) {
    return ClientOverrideConfiguration.builder()
        .addExecutionInterceptor(metrics)
        .retryPolicy(RetryPolicy.builder().numRetries(1).build())
        .build();
  }

  private static StaticCredentialsProvider credentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key"));
  }

  private static List<URI> nodes() {
    return Arrays.asList(URI.create("http://127.0.0.1:8000"), URI.create("http://127.0.0.2:8000"));
  }

  private static HttpExecuteResponse response(int attempt) {
    byte[] body =
        (attempt == 1 ? "{\"message\":\"retry\"}" : "{}").getBytes(StandardCharsets.UTF_8);
    return HttpExecuteResponse.builder()
        .response(
            SdkHttpFullResponse.builder()
                .statusCode(attempt == 1 ? 500 : 200)
                .putHeader("Content-Type", "application/x-amz-json-1.0")
                .putHeader("Content-Length", String.valueOf(body.length))
                .build())
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
        .build();
  }

  private static byte[] readAll(java.io.InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[128];
    int read;
    while ((read = input.read(buffer)) != -1) {
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  private static CompletableFuture<byte[]> readAll(Publisher<ByteBuffer> publisher) {
    CompletableFuture<byte[]> result = new CompletableFuture<>();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    publisher.subscribe(
        new Subscriber<ByteBuffer>() {
          @Override
          public void onSubscribe(org.reactivestreams.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(ByteBuffer item) {
            ByteBuffer copy = item.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            output.write(bytes, 0, bytes.length);
          }

          @Override
          public void onError(Throwable failure) {
            result.completeExceptionally(failure);
          }

          @Override
          public void onComplete() {
            result.complete(output.toByteArray());
          }
        });
    return result;
  }

  private static final class MetricsInterceptor implements ExecutionInterceptor {
    private final List<Integer> bytesWritten = new ArrayList<>();
    private final List<Long> firstByteWrittenNanos = new ArrayList<>();

    @Override
    public void afterTransmission(
        Context.AfterTransmission context, ExecutionAttributes executionAttributes) {
      RequestBodyMetrics metrics =
          executionAttributes.getAttribute(InternalCoreExecutionAttribute.REQUEST_BODY_METRICS);
      bytesWritten.add(Math.toIntExact(metrics.bytesWritten().get()));
      firstByteWrittenNanos.add(metrics.firstByteWrittenNanoTime().get());
    }
  }

  private static final class RecordingSyncClient implements SdkHttpClient {
    private final List<Integer> bodySizes = new ArrayList<>();
    private int attempts;

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      try {
        bodySizes.add(
            request.contentStreamProvider().isPresent()
                ? readAll(request.contentStreamProvider().get().newStream()).length
                : 0);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      int attempt = ++attempts;
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return response(attempt);
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}
  }

  private static final class RecordingAsyncClient implements SdkAsyncHttpClient {
    private final List<Integer> bodySizes = new ArrayList<>();
    private final List<Long> subscriptionStartedNanos = new ArrayList<>();
    private int attempts;

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      int attempt = ++attempts;
      subscriptionStartedNanos.add(System.nanoTime());
      return readAll(request.requestContentPublisher())
          .thenAccept(
              body -> {
                bodySizes.add(body.length);
                HttpExecuteResponse response = response(attempt);
                request.responseHandler().onHeaders(response.httpResponse());
                byte[] responseBody;
                try {
                  responseBody = readAll(response.responseBody().get());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                request
                    .responseHandler()
                    .onStream(
                        subscriber -> {
                          subscriber.onSubscribe(
                              new org.reactivestreams.Subscription() {
                                private boolean complete;

                                @Override
                                public void request(long count) {
                                  if (!complete) {
                                    complete = true;
                                    subscriber.onNext(ByteBuffer.wrap(responseBody));
                                    subscriber.onComplete();
                                  }
                                }

                                @Override
                                public void cancel() {
                                  complete = true;
                                }
                              });
                        });
              });
    }

    @Override
    public void close() {}
  }
}
