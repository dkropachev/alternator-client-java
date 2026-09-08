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
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/** Integration tests for SDK response parsing with gzip/deflate compressed HTTP bodies. */
public class ResponseCompressionIT {

  private static final String TABLE_NAME = "compressed_table";
  private static final byte[] RESPONSE_JSON =
      ("{\"TableNames\":[\"" + TABLE_NAME + "\"]}").getBytes(StandardCharsets.UTF_8);

  @Test
  public void testSyncSdkClientParsesGzipResponse() throws Exception {
    verifySyncResponse("gzip", gzip(RESPONSE_JSON));
  }

  @Test
  public void testSyncSdkClientParsesDeflateResponse() throws Exception {
    verifySyncResponse("deflate", deflate(RESPONSE_JSON));
  }

  @Test
  @CoversRequirements("COMP-REQ-008")
  public void testAsyncSdkClientParsesGzipResponse() throws Exception {
    verifyAsyncResponse("gzip", gzip(RESPONSE_JSON));
  }

  @Test
  public void testAsyncSdkClientParsesDeflateResponse() throws Exception {
    verifyAsyncResponse("deflate", deflate(RESPONSE_JSON));
  }

  @Test
  public void testSyncSdkClientUsesConfiguredResponseCompressionAlgorithms() throws Exception {
    verifySyncResponse(
        "gzip",
        gzip(RESPONSE_JSON),
        Collections.singletonList(ResponseCompressionAlgorithm.GZIP),
        "gzip");
  }

  @Test
  public void testAsyncSdkClientUsesConfiguredResponseCompressionAlgorithms() throws Exception {
    verifyAsyncResponse(
        "gzip",
        gzip(RESPONSE_JSON),
        Collections.singletonList(ResponseCompressionAlgorithm.GZIP),
        "gzip");
  }

  private static void verifySyncResponse(String encoding, byte[] compressedBody) {
    verifySyncResponse(
        encoding,
        compressedBody,
        ResponseCompressionAlgorithm.supportedAlgorithms(),
        ResponseCompressionInterceptor.ACCEPT_ENCODING);
  }

  private static void verifySyncResponse(
      String encoding,
      byte[] compressedBody,
      Collection<ResponseCompressionAlgorithm> algorithms,
      String expectedAcceptEncoding) {
    CompressedResponseHttpClient httpClient =
        new CompressedResponseHttpClient(encoding, compressedBody);
    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .region(Region.US_EAST_1)
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .httpClient(httpClient)
            .overrideConfiguration(
                c -> c.addExecutionInterceptor(new ResponseCompressionInterceptor(algorithms)))
            .build();

    try {
      ListTablesResponse response = client.listTables();

      assertEquals(TABLE_NAME, response.tableNames().get(0));
      assertEquals(
          expectedAcceptEncoding,
          httpClient.capturedRequest.firstMatchingHeader("Accept-Encoding").get());
    } finally {
      client.close();
    }
  }

  private static void verifyAsyncResponse(String encoding, byte[] compressedBody) throws Exception {
    verifyAsyncResponse(
        encoding,
        compressedBody,
        ResponseCompressionAlgorithm.supportedAlgorithms(),
        ResponseCompressionInterceptor.ACCEPT_ENCODING);
  }

  private static void verifyAsyncResponse(
      String encoding,
      byte[] compressedBody,
      Collection<ResponseCompressionAlgorithm> algorithms,
      String expectedAcceptEncoding)
      throws Exception {
    CompressedResponseAsyncHttpClient httpClient =
        new CompressedResponseAsyncHttpClient(encoding, compressedBody);
    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .region(Region.US_EAST_1)
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .httpClient(httpClient)
            .overrideConfiguration(
                c -> c.addExecutionInterceptor(new ResponseCompressionInterceptor(algorithms)))
            .build();

    try {
      ListTablesResponse response = client.listTables().get();

      assertEquals(TABLE_NAME, response.tableNames().get(0));
      assertEquals(
          expectedAcceptEncoding,
          httpClient.capturedRequest.firstMatchingHeader("Accept-Encoding").get());
    } finally {
      client.close();
    }
  }

  private static SdkHttpFullResponse compressedResponse(String encoding, int contentLength) {
    return SdkHttpFullResponse.builder()
        .statusCode(200)
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .putHeader("Content-Encoding", encoding)
        .putHeader("Content-Length", Integer.toString(contentLength))
        .build();
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

  private static final class CompressedResponseHttpClient implements SdkHttpClient {
    private final String encoding;
    private final byte[] compressedBody;
    private SdkHttpRequest capturedRequest;

    private CompressedResponseHttpClient(String encoding, byte[] compressedBody) {
      this.encoding = encoding;
      this.compressedBody = compressedBody;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return HttpExecuteResponse.builder()
              .response(compressedResponse(encoding, compressedBody.length))
              .responseBody(
                  AbortableInputStream.create(new ByteArrayInputStream(compressedBody)))
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
      return "compressed-response-test";
    }
  }

  private static final class CompressedResponseAsyncHttpClient implements SdkAsyncHttpClient {
    private final String encoding;
    private final byte[] compressedBody;
    private SdkHttpRequest capturedRequest;

    private CompressedResponseAsyncHttpClient(String encoding, byte[] compressedBody) {
      this.encoding = encoding;
      this.compressedBody = compressedBody;
    }

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      capturedRequest = request.request();
      request.responseHandler().onHeaders(compressedResponse(encoding, compressedBody.length));
      request.responseHandler().onStream(singleBufferPublisher(compressedBody));
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "compressed-response-async-test";
    }
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
}
