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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import org.junit.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/**
 * Unit tests for GzipRequestInterceptor.
 *
 * <p>Tests cover GZIP compression of HTTP request bodies, including Content-Encoding header
 * handling, body size thresholds, round-trip compression/decompression correctness, and edge cases.
 */
public class GzipRequestInterceptorTest {

  private static final int DEFAULT_MIN_COMPRESSION_SIZE = 256;

  private SdkHttpRequest createHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("https")
        .host("localhost")
        .port(8043)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .build();
  }

  private Context.ModifyHttpRequest createMockContext(
      SdkHttpRequest httpRequest, RequestBody body) {
    return new Context.ModifyHttpRequest() {
      @Override
      public SdkHttpRequest httpRequest() {
        return httpRequest;
      }

      @Override
      public Optional<RequestBody> requestBody() {
        return Optional.ofNullable(body);
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

  private Context.ModifyHttpRequest createAsyncMockContext(
      SdkHttpRequest httpRequest, AsyncRequestBody body) {
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
        return Optional.ofNullable(body);
      }

      @Override
      public SdkRequest request() {
        return ListTablesRequest.builder().build();
      }
    };
  }

  private byte[] gzipDecompress(byte[] compressed) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
    GZIPInputStream gis = new GZIPInputStream(bis);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int len;
    while ((len = gis.read(buffer)) != -1) {
      bos.write(buffer, 0, len);
    }
    return bos.toByteArray();
  }

  private byte[] generateTestData(int size) {
    byte[] data = new byte[size];
    new Random(42).nextBytes(data);
    return data;
  }

  private byte[] readRequestBody(Optional<RequestBody> body) throws IOException {
    assertTrue("Request body should be present", body.isPresent());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int len;
    java.io.InputStream is = body.get().contentStreamProvider().newStream();
    while ((len = is.read(buf)) != -1) {
      bos.write(buf, 0, len);
    }
    return bos.toByteArray();
  }

  private byte[] readAsyncRequestBody(Optional<AsyncRequestBody> body) {
    assertTrue("Async request body should be present", body.isPresent());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    body.get()
        .subscribe(
            byteBuffer -> {
              ByteBuffer copy = byteBuffer.asReadOnlyBuffer();
              byte[] bytes = new byte[copy.remaining()];
              copy.get(bytes);
              bos.write(bytes, 0, bytes.length);
            })
        .join();
    return bos.toByteArray();
  }

  @Test
  public void testContentEncodingHeaderSetOnCompressedRequest() {
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(DEFAULT_MIN_COMPRESSION_SIZE);
    byte[] testData = generateTestData(1024);
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context =
        createMockContext(httpRequest, RequestBody.fromBytes(testData));
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);

    assertTrue(
        "Content-Encoding header should be present",
        modifiedRequest.headers().containsKey("Content-Encoding"));
    assertEquals("gzip", modifiedRequest.headers().get("Content-Encoding").get(0));
  }

  @Test
  @CoversRequirements("COMP-REQ-002")
  public void testCompressedBodyIsValidGzip() throws IOException {
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(DEFAULT_MIN_COMPRESSION_SIZE);
    byte[] testData = generateTestData(1024);
    SdkHttpRequest httpRequest = createHttpRequest();
    RequestBody requestBody = RequestBody.fromBytes(testData);
    Context.ModifyHttpRequest context = createMockContext(httpRequest, requestBody);
    ExecutionAttributes attrs = new ExecutionAttributes();

    interceptor.modifyHttpRequest(context, attrs);
    Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

    byte[] compressedBytes = readRequestBody(modifiedBody);
    byte[] decompressed = gzipDecompress(compressedBytes);
    assertArrayEquals("Decompressed body should match original", testData, decompressed);
  }

  @Test
  public void testAsyncCompressedBodyIsValidGzip() throws IOException {
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(DEFAULT_MIN_COMPRESSION_SIZE);
    byte[] testData = generateTestData(1024);
    SdkHttpRequest httpRequest = createHttpRequest();
    AsyncRequestBody requestBody = AsyncRequestBody.fromBytes(testData);
    Context.ModifyHttpRequest context = createAsyncMockContext(httpRequest, requestBody);
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
    Optional<AsyncRequestBody> modifiedBody = interceptor.modifyAsyncHttpContent(context, attrs);

    assertTrue(
        "Content-Encoding header should be present",
        modifiedRequest.headers().containsKey("Content-Encoding"));
    byte[] compressedBytes = readAsyncRequestBody(modifiedBody);
    byte[] decompressed = gzipDecompress(compressedBytes);
    assertArrayEquals("Decompressed async body should match original", testData, decompressed);
  }

  @Test
  public void testAsyncBodyBelowMinCompressionSizeIsNotCompressed() {
    int minSize = 512;
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(minSize);
    byte[] testData = generateTestData(256);
    SdkHttpRequest httpRequest = createHttpRequest();
    AsyncRequestBody requestBody = AsyncRequestBody.fromBytes(testData);
    Context.ModifyHttpRequest context = createAsyncMockContext(httpRequest, requestBody);
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
    Optional<AsyncRequestBody> modifiedBody = interceptor.modifyAsyncHttpContent(context, attrs);

    assertFalse(
        "Content-Encoding header should NOT be present for small async body",
        modifiedRequest.headers().containsKey("Content-Encoding"));
    assertArrayEquals(testData, readAsyncRequestBody(modifiedBody));
  }

  @Test
  public void testParametricBodySizes() throws IOException {
    List<Integer> sizes = Arrays.asList(0, 100, 1024, 10 * 1024, 1024 * 1024);
    int minSize = DEFAULT_MIN_COMPRESSION_SIZE;
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(minSize);

    for (int size : sizes) {
      byte[] testData = generateTestData(size);
      SdkHttpRequest httpRequest = createHttpRequest();
      RequestBody requestBody = RequestBody.fromBytes(testData);
      Context.ModifyHttpRequest context = createMockContext(httpRequest, requestBody);
      ExecutionAttributes attrs = new ExecutionAttributes();

      SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
      Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

      if (size >= minSize) {
        assertTrue(
            "Size " + size + ": Content-Encoding header should be present",
            modifiedRequest.headers().containsKey("Content-Encoding"));
        byte[] compressedBytes = readRequestBody(modifiedBody);
        byte[] decompressed = gzipDecompress(compressedBytes);
        assertArrayEquals(
            "Size " + size + ": Decompressed body should match original", testData, decompressed);
      } else {
        assertFalse(
            "Size " + size + ": Content-Encoding header should NOT be present",
            modifiedRequest.headers().containsKey("Content-Encoding"));
        byte[] bodyBytes = readRequestBody(modifiedBody);
        assertArrayEquals("Size " + size + ": Body should be unchanged", testData, bodyBytes);
      }
    }
  }

  @Test
  public void testBelowMinCompressionSizeNotCompressed() {
    int minSize = 512;
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(minSize);
    byte[] testData = generateTestData(256);
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context =
        createMockContext(httpRequest, RequestBody.fromBytes(testData));
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);

    assertFalse(
        "Content-Encoding header should NOT be present for small body",
        modifiedRequest.headers().containsKey("Content-Encoding"));
  }

  @Test
  public void testAtMinCompressionSizeIsCompressed() throws IOException {
    int minSize = 256;
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(minSize);
    byte[] testData = generateTestData(minSize);
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context =
        createMockContext(httpRequest, RequestBody.fromBytes(testData));
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
    Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

    assertTrue(
        "Content-Encoding header should be present at exact threshold",
        modifiedRequest.headers().containsKey("Content-Encoding"));

    byte[] compressedBytes = readRequestBody(modifiedBody);
    byte[] decompressed = gzipDecompress(compressedBytes);
    assertArrayEquals("Decompressed body should match original", testData, decompressed);
  }

  @Test
  public void testAboveMinCompressionSizeIsCompressed() throws IOException {
    int minSize = 256;
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(minSize);
    byte[] testData = generateTestData(minSize + 1);
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context =
        createMockContext(httpRequest, RequestBody.fromBytes(testData));
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
    Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

    assertTrue(
        "Content-Encoding header should be present above threshold",
        modifiedRequest.headers().containsKey("Content-Encoding"));

    byte[] compressedBytes = readRequestBody(modifiedBody);
    byte[] decompressed = gzipDecompress(compressedBytes);
    assertArrayEquals("Decompressed body should match original", testData, decompressed);
  }

  @Test
  public void testNoRequestBody() {
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(DEFAULT_MIN_COMPRESSION_SIZE);
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context = createMockContext(httpRequest, null);
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
    Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

    assertFalse(
        "Content-Encoding header should NOT be present when no body",
        modifiedRequest.headers().containsKey("Content-Encoding"));
    assertFalse(
        "Modified body should not be present when original body is absent",
        modifiedBody.isPresent());
  }

  @Test
  public void testCompressionDisabledWithVeryLargeMinSize() {
    int veryLargeMinSize = Integer.MAX_VALUE;
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(veryLargeMinSize);
    byte[] testData = generateTestData(1024 * 1024);
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context =
        createMockContext(httpRequest, RequestBody.fromBytes(testData));
    ExecutionAttributes attrs = new ExecutionAttributes();

    SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, attrs);
    Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

    assertFalse(
        "Content-Encoding header should NOT be present when minSize is very large",
        modifiedRequest.headers().containsKey("Content-Encoding"));

    assertTrue("Body should be present", modifiedBody.isPresent());
    try {
      byte[] bodyBytes = readRequestBody(modifiedBody);
      assertArrayEquals(
          "Body should be unchanged when compression is disabled", testData, bodyBytes);
    } catch (IOException e) {
      fail("Should not throw IOException reading uncompressed body: " + e.getMessage());
    }
  }

  @Test
  public void testDecompressedBodyMatchesOriginalExactly() throws IOException {
    GzipRequestInterceptor interceptor = new GzipRequestInterceptor(DEFAULT_MIN_COMPRESSION_SIZE);
    // Use a known string pattern to verify exact match
    byte[] testData = new byte[10 * 1024];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) (i % 251);
    }
    SdkHttpRequest httpRequest = createHttpRequest();
    Context.ModifyHttpRequest context =
        createMockContext(httpRequest, RequestBody.fromBytes(testData));
    ExecutionAttributes attrs = new ExecutionAttributes();

    interceptor.modifyHttpRequest(context, attrs);
    Optional<RequestBody> modifiedBody = interceptor.modifyHttpContent(context, attrs);

    byte[] compressedBytes = readRequestBody(modifiedBody);
    byte[] decompressed = gzipDecompress(compressedBytes);

    assertEquals("Decompressed length should match original", testData.length, decompressed.length);
    for (int i = 0; i < testData.length; i++) {
      assertEquals("Byte mismatch at index " + i, testData[i], decompressed[i]);
    }
  }
}
