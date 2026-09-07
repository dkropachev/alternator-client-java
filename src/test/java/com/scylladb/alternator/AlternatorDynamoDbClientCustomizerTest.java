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

import com.scylladb.alternator.internal.SyncClientDetector;
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
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/**
 * Unit tests for sync client builder customizer API and validation.
 *
 * <p>Verifies mutual exclusion rules, customizer invocation, and configuration propagation for
 * {@link AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder}.
 */
public class AlternatorDynamoDbClientCustomizerTest {

  private static final URI SEED_URI = URI.create("http://127.0.0.1:9999");

  @Test(expected = IllegalStateException.class)
  public void testCannotUseBothApacheAndCrtCustomizers() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .withApacheHttpClientCustomizer(builder -> {})
        .withCrtHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClient() {
    SdkHttpClient httpClient = ApacheHttpClient.create();
    try {
      AlternatorDynamoDbClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withApacheHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCrtCustomizerWithHttpClient() {
    SdkHttpClient httpClient = ApacheHttpClient.create();
    try {
      AlternatorDynamoDbClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withCrtHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClientBuilder() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(ApacheHttpClient.builder())
        .withApacheHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testEndpointOverrideRequired() {
    AlternatorDynamoDbClient.builder().build();
  }

  @Test
  public void testApacheCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withApacheHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testCrtCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testApacheCustomizerWithConfig() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withApacheHttpClientCustomizer(b -> b.maxConnections(200));
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testCrtCustomizerWithConfig() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectSyncClientType();
  }

  @Test(expected = IllegalStateException.class)
  public void testNettyHttpClientTypeOnSyncBuilder() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.NETTY)
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeApacheConflictsWithCrtCustomizer() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.APACHE)
        .withCrtHttpClientCustomizer(b -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeCrtConflictsWithApacheCustomizer() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.CRT)
        .withApacheHttpClientCustomizer(b -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeConflictsWithHttpClient() {
    SdkHttpClient httpClient = ApacheHttpClient.create();
    try {
      AlternatorDynamoDbClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withHttpClientType(HttpClientType.APACHE)
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test
  public void testHttpClientTypeBuilderReturnsThis() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.APACHE);
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testUserAgentBuilderMethodsReturnThis() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder().endpointOverride(SEED_URI);

    assertSame(builder, builder.withUserAgent("custom/1"));
    assertSame(builder, builder.withUserAgent(userAgent -> userAgent + " app/1"));
    assertSame(builder, builder.withoutUserAgent());
  }

  @Test
  public void testSeedHostsBuilderPreservesExplicitSeeds() {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
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
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
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
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
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
    AlternatorDynamoDbClient.builder().endpointOverride(SEED_URI).withUserAgent((String) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsBlankString() {
    AlternatorDynamoDbClient.builder().endpointOverride(SEED_URI).withUserAgent(" ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsNullTransformer() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .withUserAgent((UnaryOperator<String>) null);
  }

  @Test
  public void testHttpClientTypeAutoWithApacheCustomizerPassesValidation() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .withApacheHttpClientCustomizer(b -> {});
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testHttpClientTypeApacheWithMatchingCustomizerPassesValidation() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.APACHE)
            .withApacheHttpClientCustomizer(b -> b.maxConnections(100));
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testHttpClientTypeCrtWithMatchingCustomizerPassesValidation() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT)
            .withCrtHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testHttpClientTypeAutoAlonePassesValidation() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO);
    builder.validateAndDetectSyncClientType();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeAutoConflictsWithHttpClient() {
    SdkHttpClient httpClient = ApacheHttpClient.create();
    try {
      AlternatorDynamoDbClient.builder()
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
    AlternatorDynamoDbClient.builder().endpointOverride(SEED_URI).withHttpClientType(null);
  }

  @Test
  public void testHttpClientTypeApacheAlonePassesValidation() {
    var builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.APACHE);
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testHttpClientTypeCrtAlonePassesValidation() {
    var builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT);
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testHttpClientTypeAutoWithCrtCustomizerPassesValidation() {
    var builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .withCrtHttpClientCustomizer(b -> {});
    builder.validateAndDetectSyncClientType();
  }

  @Test
  public void testDetectSyncClientTypeWithApacheCustomizerOnly() {
    assertEquals(
        SyncClientDetector.SyncClientType.APACHE,
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withApacheHttpClientCustomizer(b -> {})
            .validateAndDetectSyncClientType());
  }

  @Test
  public void testDetectSyncClientTypeWithCrtCustomizerOnly() {
    assertEquals(
        SyncClientDetector.SyncClientType.CRT,
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtHttpClientCustomizer(b -> {})
            .validateAndDetectSyncClientType());
  }

  @Test
  public void testDetectSyncClientTypeWithExplicitApache() {
    assertEquals(
        SyncClientDetector.SyncClientType.APACHE,
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.APACHE)
            .validateAndDetectSyncClientType());
  }

  @Test
  public void testDetectSyncClientTypeWithExplicitCrt() {
    assertEquals(
        SyncClientDetector.SyncClientType.CRT,
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT)
            .validateAndDetectSyncClientType());
  }

  @Test
  public void testHttpClientTypeAutoResolveSameAsNull() {
    SyncClientDetector.SyncClientType autoResult =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .validateAndDetectSyncClientType();
    SyncClientDetector.SyncClientType nullResult =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .validateAndDetectSyncClientType();
    assertEquals(
        "AUTO and null (default) should resolve to the same client type", nullResult, autoResult);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHttpClientTypeCrtWithAlternatorConfigPassesValidation() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    var builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withAlternatorConfig(config)
            .withHttpClientType(HttpClientType.CRT);
    builder.validateAndDetectSyncClientType();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHttpClientTypeApacheWithAlternatorConfigPassesValidation() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    var builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.APACHE)
            .withAlternatorConfig(config);
    builder.validateAndDetectSyncClientType();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigDisablesUserAgentTransformer() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withOptimizeHeaders(true).withUserAgentEnabled(false).build();
    var builder = AlternatorDynamoDbClient.builder().withAlternatorConfig(config);

    assertNull(userAgentTransformer(builder).apply("aws-sdk-java/2.x"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigEnablesDefaultUserAgentTransformer() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withUserAgentEnabled(true).build();
    var builder =
        AlternatorDynamoDbClient.builder().withoutUserAgent().withAlternatorConfig(config);

    assertEquals(
        AlternatorUserAgent.userAgentToken(),
        userAgentTransformer(builder).apply("aws-sdk-java/2.x"));
  }

  @Test
  public void testPollingClientAddsDefaultUserAgent() throws Exception {
    var builder = AlternatorDynamoDbClient.builder().endpointOverride(SEED_URI);
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
        AlternatorDynamoDbClient.builder()
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
    var builder = AlternatorDynamoDbClient.builder().withAlternatorConfig(config);

    assertSame(nodeHealthConfig, alternatorConfig(builder).getNodeHealthConfig());
  }

  @Test
  public void testWithNodeHealthDisabledConfiguresBuilder() throws Exception {
    var builder = AlternatorDynamoDbClient.builder().withNodeHealthDisabled();

    assertTrue(alternatorConfig(builder).getNodeHealthConfig().isDisabled());
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeConflictsWithHttpClientBuilder() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(ApacheHttpClient.builder())
        .withHttpClientType(HttpClientType.APACHE)
        .build();
  }

  @Test
  public void testTlsConfigWithCustomizer() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(URI.create("https://127.0.0.1:9999"))
            .withTlsConfig(TlsConfig.trustAll())
            .withApacheHttpClientCustomizer(b -> {});
    assertNotNull("Builder with TLS and customizer should be valid", builder);
  }

  @SuppressWarnings("unchecked")
  private UnaryOperator<String> userAgentTransformer(
      AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder) throws Exception {
    Field field = builder.getClass().getDeclaredField("userAgentTransformer");
    field.setAccessible(true);
    return (UnaryOperator<String>) field.get(builder);
  }

  private SdkHttpClient configurePollingSyncClient(
      AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder,
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
      AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder) throws Exception {
    Field field = builder.getClass().getDeclaredField("configBuilder");
    field.setAccessible(true);
    return ((AlternatorConfig.Builder) field.get(builder)).build();
  }
}
