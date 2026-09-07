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

import com.scylladb.alternator.internal.AsyncClientDetector;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.function.UnaryOperator;
import org.junit.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

/**
 * Unit tests for async client builder customizer API and validation.
 *
 * <p>Verifies mutual exclusion rules, customizer invocation, and configuration propagation for
 * {@link AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder}.
 */
public class AlternatorDynamoDbAsyncClientCustomizerTest {

  private static final URI SEED_URI = URI.create("http://127.0.0.1:9999");

  @Test(expected = IllegalStateException.class)
  public void testCannotUseBothNettyAndCrtCustomizers() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withNettyHttpClientCustomizer(builder -> {})
        .withCrtAsyncHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withNettyHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCrtCustomizerWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withCrtAsyncHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClientBuilder() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
        .withNettyHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testEndpointOverrideRequired() {
    AlternatorDynamoDbAsyncClient.builder().build();
  }

  @Test
  public void testNettyCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withNettyHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testCrtCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtAsyncHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testNettyCustomizerWithConfig() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withNettyHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testCrtCustomizerWithConfig() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtAsyncHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectAsyncClientType();
  }

  @Test(expected = IllegalStateException.class)
  public void testApacheHttpClientTypeOnAsyncBuilder() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.APACHE)
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeNettyConflictsWithCrtCustomizer() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.NETTY)
        .withCrtAsyncHttpClientCustomizer(b -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeCrtConflictsWithNettyCustomizer() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.CRT)
        .withNettyHttpClientCustomizer(b -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeConflictsWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withHttpClientType(HttpClientType.NETTY)
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test
  public void testHttpClientTypeBuilderReturnsThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY);
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testUserAgentBuilderMethodsReturnThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI);

    assertSame(builder, builder.withUserAgent("custom/1"));
    assertSame(builder, builder.withUserAgent(userAgent -> userAgent + " app/1"));
    assertSame(builder, builder.withoutUserAgent());
  }

  @Test
  public void testSeedHostsBuilderPreservesExplicitSeeds() {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withSeedHosts(Arrays.asList("127.0.0.1", "127.0.0.2"))
            .buildWithAlternatorAPI();
    try {
      AlternatorConfig config = wrapper.getAlternatorConfig();
      assertEquals(Arrays.asList("127.0.0.1", "127.0.0.2"), config.getSeedHosts());
      assertEquals(SEED_URI.getScheme(), config.getScheme());
      assertEquals(SEED_URI.getPort(), config.getPort());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testResponseCompressionAlgorithmsPropagateToConfig() {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withResponseCompression(
                ResponseCompressionAlgorithm.DEFLATE, ResponseCompressionAlgorithm.GZIP)
            .buildWithAlternatorAPI();
    try {
      AlternatorConfig config = wrapper.getAlternatorConfig();
      assertEquals(
          Arrays.asList(ResponseCompressionAlgorithm.DEFLATE, ResponseCompressionAlgorithm.GZIP),
          config.getResponseCompressionAlgorithms());
      assertTrue(config.isResponseCompressionEnabled());
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testResponseCompressionDisabledPropagatesToConfig() {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withResponseCompressionDisabled()
            .buildWithAlternatorAPI();
    try {
      AlternatorConfig config = wrapper.getAlternatorConfig();
      assertTrue(config.getResponseCompressionAlgorithms().isEmpty());
      assertFalse(config.isResponseCompressionEnabled());
    } finally {
      wrapper.close();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsNullString() {
    AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI).withUserAgent((String) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsBlankString() {
    AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI).withUserAgent(" ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsNullTransformer() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withUserAgent((UnaryOperator<String>) null);
  }

  @Test
  public void testHttpClientTypeAutoWithNettyCustomizerPassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .withNettyHttpClientCustomizer(b -> {});
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeNettyWithMatchingCustomizerPassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY)
            .withNettyHttpClientCustomizer(b -> b.maxConcurrency(100));
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeCrtWithMatchingCustomizerPassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT)
            .withCrtAsyncHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeAutoAlonePassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO);
    builder.validateAndDetectAsyncClientType();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeAutoConflictsWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withHttpClientType(HttpClientType.AUTO)
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = NullPointerException.class)
  public void testHttpClientTypeRejectsNull() {
    AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI).withHttpClientType(null);
  }

  @Test
  public void testHttpClientTypeNettyAlonePassesValidation() {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeCrtAlonePassesValidation() {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeAutoWithCrtAsyncCustomizerPassesValidation() {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .withCrtAsyncHttpClientCustomizer(b -> {});
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testDetectAsyncClientTypeWithNettyCustomizerOnly() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.NETTY,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withNettyHttpClientCustomizer(b -> {})
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testDetectAsyncClientTypeWithCrtCustomizerOnly() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.CRT,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtAsyncHttpClientCustomizer(b -> {})
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testDetectAsyncClientTypeWithExplicitNetty() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.NETTY,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY)
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testDetectAsyncClientTypeWithExplicitCrt() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.CRT,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT)
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testHttpClientTypeAutoResolveSameAsNull() {
    AsyncClientDetector.AsyncClientType autoResult =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .validateAndDetectAsyncClientType();
    AsyncClientDetector.AsyncClientType nullResult =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .validateAndDetectAsyncClientType();
    assertEquals(
        "AUTO and null (default) should resolve to the same client type", nullResult, autoResult);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHttpClientTypeCrtWithAlternatorConfigPassesValidation() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withAlternatorConfig(config)
            .withHttpClientType(HttpClientType.CRT);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHttpClientTypeNettyWithAlternatorConfigPassesValidation() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY)
            .withAlternatorConfig(config);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigDisablesUserAgentTransformer() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withOptimizeHeaders(true).withUserAgentEnabled(false).build();
    var builder = AlternatorDynamoDbAsyncClient.builder().withAlternatorConfig(config);

    assertNull(userAgentTransformer(builder).apply("aws-sdk-java/2.x"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigEnablesDefaultUserAgentTransformer() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withUserAgentEnabled(true).build();
    var builder =
        AlternatorDynamoDbAsyncClient.builder().withoutUserAgent().withAlternatorConfig(config);

    assertEquals(
        AlternatorUserAgent.userAgentToken(),
        userAgentTransformer(builder).apply("aws-sdk-java/2.x"));
  }

  @Test
  public void testPollingClientAddsDefaultUserAgent() throws Exception {
    var builder = AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI);
    CapturingSdkHttpClient delegate = new CapturingSdkHttpClient();
    SdkHttpClient configured =
        configurePollingSyncClient(builder, delegate, AlternatorConfig.builder().build());

    configured.prepareRequest(HttpExecuteRequest.builder().request(localNodesRequest()).build());

    assertEquals(
        AlternatorUserAgent.userAgentToken(),
        delegate.capturedRequest.firstMatchingHeader("User-Agent").get());
  }

  @Test
  public void testPollingClientAppliesCustomUserAgent() throws Exception {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withUserAgent(userAgent -> userAgent + " app/1");
    CapturingSdkHttpClient delegate = new CapturingSdkHttpClient();
    SdkHttpClient configured =
        configurePollingSyncClient(builder, delegate, AlternatorConfig.builder().build());

    configured.prepareRequest(HttpExecuteRequest.builder().request(localNodesRequest()).build());

    assertEquals(
        AlternatorUserAgent.userAgentToken() + " app/1",
        delegate.capturedRequest.firstMatchingHeader("User-Agent").get());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigCopiesNodeHealthConfig() throws Exception {
    NodeHealthConfig nodeHealthConfig =
        NodeHealthConfig.builder().withDownNodeProbePeriodMs(1234).build();
    AlternatorConfig config =
        AlternatorConfig.builder().withNodeHealthConfig(nodeHealthConfig).build();
    var builder = AlternatorDynamoDbAsyncClient.builder().withAlternatorConfig(config);

    assertSame(nodeHealthConfig, alternatorConfig(builder).getNodeHealthConfig());
  }

  @Test
  public void testWithNodeHealthDisabledConfiguresBuilder() throws Exception {
    var builder = AlternatorDynamoDbAsyncClient.builder().withNodeHealthDisabled();

    assertTrue(alternatorConfig(builder).getNodeHealthConfig().isDisabled());
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeConflictsWithHttpClientBuilder() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
        .withHttpClientType(HttpClientType.NETTY)
        .build();
  }

  @Test
  public void testTlsConfigWithCustomizer() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("https://127.0.0.1:9999"))
            .withTlsConfig(TlsConfig.trustAll())
            .withNettyHttpClientCustomizer(b -> {});
    assertNotNull("Builder with TLS and customizer should be valid", builder);
  }

  @SuppressWarnings("unchecked")
  private UnaryOperator<String> userAgentTransformer(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder) throws Exception {
    Field field = builder.getClass().getDeclaredField("userAgentTransformer");
    field.setAccessible(true);
    return (UnaryOperator<String>) field.get(builder);
  }

  private SdkHttpClient configurePollingSyncClient(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder,
      SdkHttpClient pollingClient,
      AlternatorConfig config)
      throws Exception {
    Method method =
        builder
            .getClass()
            .getDeclaredMethod(
                "configurePollingSyncClient", SdkHttpClient.class, AlternatorConfig.class);
    method.setAccessible(true);
    return (SdkHttpClient) method.invoke(builder, pollingClient, config);
  }

  private SdkHttpRequest localNodesRequest() {
    return SdkHttpRequest.builder()
        .method(SdkHttpMethod.GET)
        .uri(SEED_URI.resolve("/localnodes"))
        .putHeader("Host", SEED_URI.getHost() + ":" + SEED_URI.getPort())
        .putHeader("Connection", "keep-alive")
        .build();
  }

  private static class CapturingSdkHttpClient implements SdkHttpClient {
    SdkHttpRequest capturedRequest;

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return null;
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "capturing";
    }
  }

  private AlternatorConfig alternatorConfig(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder) throws Exception {
    Field field = builder.getClass().getDeclaredField("configBuilder");
    field.setAccessible(true);
    return ((AlternatorConfig.Builder) field.get(builder)).build();
  }
}
