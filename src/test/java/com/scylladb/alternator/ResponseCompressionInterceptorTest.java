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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/** Unit tests for gzip/deflate HTTP response compression support. */
public class ResponseCompressionInterceptorTest {

  private final ResponseCompressionInterceptor interceptor = new ResponseCompressionInterceptor();

  @Test
  public void testAddsAcceptEncodingWhenMissing() {
    SdkHttpRequest request = createHttpRequest();

    SdkHttpRequest modified =
        interceptor.modifyHttpRequest(requestContext(request), new ExecutionAttributes());

    assertEquals(
        ResponseCompressionInterceptor.ACCEPT_ENCODING,
        modified.firstMatchingHeader("Accept-Encoding").get());
  }

  @Test
  public void testReplacesExistingAcceptEncoding() {
    SdkHttpRequest request =
        createHttpRequest().toBuilder().putHeader("Accept-Encoding", "br").build();

    SdkHttpRequest modified =
        interceptor.modifyHttpRequest(requestContext(request), new ExecutionAttributes());

    assertEquals(
        ResponseCompressionInterceptor.ACCEPT_ENCODING,
        modified.firstMatchingHeader("Accept-Encoding").get());
  }

  @Test
  public void testCustomAcceptEncodingOrder() {
    ResponseCompressionInterceptor customInterceptor =
        new ResponseCompressionInterceptor(
            Arrays.asList(ResponseCompressionAlgorithm.DEFLATE, ResponseCompressionAlgorithm.GZIP));
    SdkHttpRequest request = createHttpRequest();

    SdkHttpRequest modified =
        customInterceptor.modifyHttpRequest(requestContext(request), new ExecutionAttributes());

    assertEquals("deflate, gzip", modified.firstMatchingHeader("Accept-Encoding").get());
  }

  @Test
  @CoversRequirements("COMP-REQ-004")
  public void testGzipSyncResponseIsDecompressedAndHeadersAreStripped() throws Exception {
    byte[] original = "compressed gzip response".getBytes(StandardCharsets.UTF_8);
    byte[] compressed = gzip(original);
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Encoding", "gzip")
            .putHeader("Content-Length", Integer.toString(compressed.length))
            .putHeader("X-Test", "kept")
            .build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, new ByteArrayInputStream(compressed), null);

    SdkHttpResponse modifiedResponse = interceptor.modifyHttpResponse(context, attrs);
    Optional<InputStream> modifiedContent = interceptor.modifyHttpResponseContent(context, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Encoding").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertEquals("kept", modifiedResponse.firstMatchingHeader("X-Test").get());
    assertArrayEquals(original, readAll(modifiedContent.get()));
  }

  @Test
  public void testDeflateSyncResponseIsDecompressed() throws Exception {
    byte[] original = "compressed deflate response".getBytes(StandardCharsets.UTF_8);
    byte[] compressed = deflate(original);
    SdkHttpResponse response =
        SdkHttpResponse.builder().statusCode(200).putHeader("Content-Encoding", "deflate").build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, new ByteArrayInputStream(compressed), null);

    interceptor.modifyHttpResponse(context, attrs);
    Optional<InputStream> modifiedContent = interceptor.modifyHttpResponseContent(context, attrs);

    assertArrayEquals(original, readAll(modifiedContent.get()));
  }

  @Test
  public void testUnsupportedEncodingIsNotModified() {
    SdkHttpResponse response =
        SdkHttpResponse.builder().statusCode(200).putHeader("Content-Encoding", "br").build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, new ByteArrayInputStream(new byte[] {1, 2, 3}), null);

    SdkHttpResponse modifiedResponse = interceptor.modifyHttpResponse(context, attrs);
    Optional<InputStream> modifiedContent = interceptor.modifyHttpResponseContent(context, attrs);

    assertEquals("br", modifiedResponse.firstMatchingHeader("Content-Encoding").get());
    assertFalse(modifiedContent.isPresent());
  }

  @Test
  public void testSupportedButNotConfiguredEncodingIsNotModified() throws Exception {
    ResponseCompressionInterceptor gzipOnlyInterceptor =
        new ResponseCompressionInterceptor(
            Collections.singletonList(ResponseCompressionAlgorithm.GZIP));
    byte[] compressed = deflate("deflate response".getBytes(StandardCharsets.UTF_8));
    SdkHttpResponse response =
        SdkHttpResponse.builder().statusCode(200).putHeader("Content-Encoding", "deflate").build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, new ByteArrayInputStream(compressed), null);

    SdkHttpResponse modifiedResponse = gzipOnlyInterceptor.modifyHttpResponse(context, attrs);
    Optional<InputStream> modifiedContent =
        gzipOnlyInterceptor.modifyHttpResponseContent(context, attrs);

    assertEquals("deflate", modifiedResponse.firstMatchingHeader("Content-Encoding").get());
    assertFalse(modifiedContent.isPresent());
  }

  @Test
  public void testMultipleContentEncodingsAreNotModified() {
    SdkHttpResponse response =
        SdkHttpResponse.builder().statusCode(200).putHeader("Content-Encoding", "gzip, br").build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, new ByteArrayInputStream(new byte[] {1, 2, 3}), null);

    SdkHttpResponse modifiedResponse = interceptor.modifyHttpResponse(context, attrs);
    Optional<InputStream> modifiedContent = interceptor.modifyHttpResponseContent(context, attrs);

    assertEquals("gzip, br", modifiedResponse.firstMatchingHeader("Content-Encoding").get());
    assertFalse(modifiedContent.isPresent());
  }

  @Test
  @CoversRequirements("COMP-REQ-006")
  public void testGzipAsyncResponseIsDecompressedAndHeadersAreStripped() throws Exception {
    byte[] original = "async gzip response".getBytes(StandardCharsets.UTF_8);
    byte[] compressed = gzip(original);
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Encoding", "gzip")
            .putHeader("Content-Length", Integer.toString(compressed.length))
            .build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, null, singleBufferPublisher(compressed));

    SdkHttpResponse modifiedResponse = interceptor.modifyHttpResponse(context, attrs);
    Optional<Publisher<ByteBuffer>> modifiedPublisher =
        interceptor.modifyAsyncHttpResponseContent(context, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Encoding").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertArrayEquals(original, collect(modifiedPublisher.get()));
  }

  @Test
  public void testDeflateAsyncResponseIsDecompressed() throws Exception {
    byte[] original = "async deflate response".getBytes(StandardCharsets.UTF_8);
    byte[] compressed = deflate(original);
    SdkHttpResponse response =
        SdkHttpResponse.builder().statusCode(200).putHeader("Content-Encoding", "deflate").build();
    ExecutionAttributes attrs = new ExecutionAttributes();
    Context.ModifyHttpResponse context =
        responseContext(response, null, singleBufferPublisher(compressed));

    interceptor.modifyHttpResponse(context, attrs);
    Optional<Publisher<ByteBuffer>> modifiedPublisher =
        interceptor.modifyAsyncHttpResponseContent(context, attrs);

    assertArrayEquals(original, collect(modifiedPublisher.get()));
  }

  private static SdkHttpRequest createHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("localhost")
        .port(8043)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .build();
  }

  private static Context.ModifyHttpRequest requestContext(SdkHttpRequest httpRequest) {
    return new Context.ModifyHttpRequest() {
      @Override
      public SdkHttpRequest httpRequest() {
        return httpRequest;
      }

      @Override
      public Optional<RequestBody> requestBody() {
        return Optional.empty();
      }

      @Override
      public Optional<AsyncRequestBody> asyncRequestBody() {
        return Optional.empty();
      }

      @Override
      public SdkRequest request() {
        return ListTablesRequest.builder().build();
      }
    };
  }

  private static Context.ModifyHttpResponse responseContext(
      SdkHttpResponse response, InputStream body, Publisher<ByteBuffer> publisher) {
    return new Context.ModifyHttpResponse() {
      @Override
      public SdkHttpResponse httpResponse() {
        return response;
      }

      @Override
      public Optional<Publisher<ByteBuffer>> responsePublisher() {
        return Optional.ofNullable(publisher);
      }

      @Override
      public Optional<InputStream> responseBody() {
        return Optional.ofNullable(body);
      }

      @Override
      public SdkHttpRequest httpRequest() {
        return createHttpRequest();
      }

      @Override
      public Optional<RequestBody> requestBody() {
        return Optional.empty();
      }

      @Override
      public Optional<AsyncRequestBody> asyncRequestBody() {
        return Optional.empty();
      }

      @Override
      public SdkRequest request() {
        return ListTablesRequest.builder().build();
      }
    };
  }

  private static byte[] gzip(byte[] bytes) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
      gzip.write(bytes);
    }
    return output.toByteArray();
  }

  private static byte[] deflate(byte[] bytes) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (DeflaterOutputStream deflate = new DeflaterOutputStream(output)) {
      deflate.write(bytes);
    }
    return output.toByteArray();
  }

  private static byte[] readAll(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = input.read(buffer)) != -1) {
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  private static Publisher<ByteBuffer> singleBufferPublisher(byte[] bytes) {
    return subscriber ->
        subscriber.onSubscribe(
            new Subscription() {
              private boolean done;

              @Override
              public void request(long n) {
                if (done) {
                  return;
                }
                done = true;
                if (n <= 0) {
                  subscriber.onError(new IllegalArgumentException("Demand must be positive"));
                  return;
                }
                subscriber.onNext(ByteBuffer.wrap(bytes));
                subscriber.onComplete();
              }

              @Override
              public void cancel() {
                done = true;
              }
            });
  }

  private static byte[] collect(Publisher<ByteBuffer> publisher) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();
    publisher.subscribe(
        new Subscriber<ByteBuffer>() {
          @Override
          public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(ByteBuffer byteBuffer) {
            ByteBuffer copy = byteBuffer.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            output.write(bytes, 0, bytes.length);
          }

          @Override
          public void onError(Throwable throwable) {
            error.set(throwable);
            done.countDown();
          }

          @Override
          public void onComplete() {
            done.countDown();
          }
        });

    assertTrue("Publisher should complete", done.await(5, TimeUnit.SECONDS));
    if (error.get() != null) {
      throw new AssertionError("Publisher failed", error.get());
    }
    return output.toByteArray();
  }
}
