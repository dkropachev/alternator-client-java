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

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.signer.AsyncRequestBodySigner;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/**
 * Tests that verify retry distribution across nodes via {@link
 * com.scylladb.alternator.internal.LazyQueryPlan}.
 *
 * <p>When a request fails and is retried, the per-transmission routing wrapper advances the query
 * plan. Since {@link com.scylladb.alternator.internal.LazyQueryPlan} returns nodes without
 * duplicates, each retry attempt is routed to a different node. These tests verify the query-plan
 * selection logic directly and verify retry routing through the real SDK pipeline.
 *
 * <ul>
 *   <li>Using a mock AlternatorLiveNodes with a controlled set of nodes
 *   <li>Exercising successive query-plan selections directly
 *   <li>Driving sync and async retries through the actual SDK pipeline
 *   <li>Verifying that captured request URIs show correct node distribution across retries
 * </ul>
 *
 * @author dmitry.kropachev
 */
public class RetryDistributionTest {

  @Test
  @CoversRequirements("QUERY-REQ-005")
  public void testSdkRetryPipelineRoutesEachAttemptToDifferentNode() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build());
    }

    assertRetryWasRerouted(httpClient.requests, httpClient.requestBodies);
  }

  @Test
  @CoversRequirements("QUERY-REQ-007")
  public void testAsyncSdkRetryPipelineRoutesEachAttemptToDifferentNode() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build()).join();
    }

    assertRetryWasRerouted(httpClient.requests, httpClient.requestBodies);
  }

  @Test
  public void testSyncFailureBeforeTransportDoesNotChangeNodeHealth() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthConfig(failOnFirstTrafficFailure())
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(localFailureBeforeTransport())
            .buildWithAlternatorAPI()) {
      assertThrows(
          RuntimeException.class,
          () -> client.getClient().listTables(ListTablesRequest.builder().build()));

      assertTrue(httpClient.requests.isEmpty());
      assertNoDownNodes(client.getAlternatorLiveNodes(), nodes);
    }
  }

  @Test
  public void testAsyncFailureBeforeTransportDoesNotChangeNodeHealth() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthConfig(failOnFirstTrafficFailure())
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(localFailureBeforeTransport())
            .buildWithAlternatorAPI()) {
      assertThrows(
          RuntimeException.class,
          () -> client.getClient().listTables(ListTablesRequest.builder().build()).join());

      assertTrue(httpClient.requests.isEmpty());
      assertNoDownNodes(client.getAlternatorLiveNodes(), nodes);
    }
  }

  @Test
  public void testSyncUserAgentFailureBeforeTransportDoesNotChangeNodeHealth() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthConfig(failOnFirstTrafficFailure())
            .withUserAgent(
                value -> {
                  throw new IllegalStateException("simulated user-agent failure");
                })
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(0).build())
                    .build())
            .buildWithAlternatorAPI()) {
      assertThrows(
          RuntimeException.class,
          () -> client.getClient().listTables(ListTablesRequest.builder().build()));

      assertTrue(httpClient.requests.isEmpty());
      assertNoDownNodes(client.getAlternatorLiveNodes(), nodes);
    }
  }

  @Test
  public void testAsyncUserAgentFailureBeforeTransportDoesNotChangeNodeHealth() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthConfig(failOnFirstTrafficFailure())
            .withUserAgent(
                value -> {
                  throw new IllegalStateException("simulated user-agent failure");
                })
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(0).build())
                    .build())
            .buildWithAlternatorAPI()) {
      assertThrows(
          RuntimeException.class,
          () -> client.getClient().listTables(ListTablesRequest.builder().build()).join());

      assertTrue(httpClient.requests.isEmpty());
      assertNoDownNodes(client.getAlternatorLiveNodes(), nodes);
    }
  }

  @Test
  public void testSynchronousAsyncTransportFailureChangesNodeHealth() throws Exception {
    List<URI> nodes = createNodes(1);
    SynchronouslyFailingSdkAsyncHttpClient httpClient =
        new SynchronouslyFailingSdkAsyncHttpClient();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthConfig(failOnFirstTrafficFailure())
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(0).build())
                    .build())
            .buildWithAlternatorAPI()) {
      assertThrows(
          RuntimeException.class,
          () -> client.getClient().listTables(ListTablesRequest.builder().build()).join());

      assertEquals(1, httpClient.calls.get());
      assertEquals(
          NodeHealthState.DOWN,
          client.getAlternatorLiveNodes().getNodeHealthStatus(nodes.get(0)).getState());
    }
  }

  @Test
  public void testSdkRetryPreservesCallerAuthorizationWithAnonymousCredentials() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();
    ListTablesRequest request =
        ListTablesRequest.builder()
            .overrideConfiguration(builder -> builder.putHeader("Authorization", "Bearer token"))
            .build();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(request);
    }

    assertEquals(2, httpClient.requests.size());
    for (SdkHttpRequest sent : httpClient.requests) {
      assertEquals("Bearer token", sent.firstMatchingHeader("Authorization").orElse(null));
    }
  }

  @Test
  public void testAsyncSdkRetryPreservesCallerAuthorizationWithAnonymousCredentials()
      throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();
    ListTablesRequest request =
        ListTablesRequest.builder()
            .overrideConfiguration(builder -> builder.putHeader("Authorization", "Bearer token"))
            .build();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(request).join();
    }

    assertEquals(2, httpClient.requests.size());
    for (SdkHttpRequest sent : httpClient.requests) {
      assertEquals("Bearer token", sent.firstMatchingHeader("Authorization").orElse(null));
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSdkRetryPipelinePreservesClientLegacySigner() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();
    LegacyAuthoritySigner signer = new LegacyAuthoritySigner();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer)
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build());
    }

    assertLegacySignerUsed(httpClient.requests);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testRetryableLegacySignerFailureReusesUnsentRoute() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient(false);
    FailOnceLegacySigner signer = new FailOnceLegacySigner();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer)
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build());
    }

    assertEquals(2, signer.signedAuthorities.size());
    assertEquals(1, httpClient.requests.size());
    assertEquals(signer.signedAuthorities.get(0), signer.signedAuthorities.get(1));
    assertEquals(signer.signedAuthorities.get(1), httpClient.requests.get(0).getUri());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAsyncSdkRetryPipelinePreservesClientLegacySigner() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();
    LegacyAuthoritySigner signer = new LegacyAuthoritySigner();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer)
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build()).join();
    }

    assertLegacySignerUsed(httpClient.requests);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSdkRetryPipelinePreservesRequestLegacySigner() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();
    LegacyAuthoritySigner signer = new LegacyAuthoritySigner();
    ListTablesRequest request =
        ListTablesRequest.builder()
            .overrideConfiguration(builder -> builder.signer(signer))
            .build();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(request);
    }

    assertLegacySignerUsed(httpClient.requests);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAsyncSdkRetryPipelinePreservesRequestLegacySigner() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();
    LegacyAuthoritySigner signer = new LegacyAuthoritySigner();
    ListTablesRequest request =
        ListTablesRequest.builder()
            .overrideConfiguration(builder -> builder.signer(signer))
            .build();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(request).join();
    }

    assertLegacySignerUsed(httpClient.requests);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAsyncRetryTransformsLegacySignerBodyOncePerAttempt() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();
    LegacyAsyncBodySigner signer = new LegacyAsyncBodySigner();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer)
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build()).join();
    }

    assertEquals(2, httpClient.requests.size());
    assertEquals(2, signer.bodySignings.get());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSyncRetryTransformsLegacySignerBodyOncePerAttempt() throws Exception {
    List<URI> nodes = createNodes(2);
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();
    TransformingLegacySigner signer = new TransformingLegacySigner();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer)
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build());
    }

    assertEquals(2, signer.signings.get());
    for (byte[] body : httpClient.requestBodies) {
      assertEquals('X', body[0]);
      assertTrue(body.length < 2 || body[1] != 'X');
    }
  }

  @Test
  public void testSdkRetryPipelineSignsBracketedIpv6Authorities() throws Exception {
    List<URI> nodes = createIpv6Nodes();
    RetryingSdkHttpClient httpClient = new RetryingSdkHttpClient();

    try (AlternatorDynamoDbClientWrapper client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build());
    }

    assertRetryWasRerouted(httpClient.requests, httpClient.requestBodies);
    assertIpv6AuthoritiesAreBracketed(httpClient.requests);
  }

  @Test
  public void testAsyncSdkRetryPipelineSignsBracketedIpv6Authorities() throws Exception {
    List<URI> nodes = createIpv6Nodes();
    RetryingSdkAsyncHttpClient httpClient = new RetryingSdkAsyncHttpClient();

    try (AlternatorDynamoDbAsyncClientWrapper client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
                    .build())
            .buildWithAlternatorAPI()) {
      client.getClient().listTables(ListTablesRequest.builder().build()).join();
    }

    assertRetryWasRerouted(httpClient.requests, httpClient.requestBodies);
    assertIpv6AuthoritiesAreBracketed(httpClient.requests);
  }

  private StaticCredentialsProvider testCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key"));
  }

  private NodeHealthConfig failOnFirstTrafficFailure() {
    return NodeHealthConfig.builder()
        .withConsecutiveFailureThreshold(1)
        .withQuarantineFailureThreshold(1)
        .build();
  }

  private ClientOverrideConfiguration localFailureBeforeTransport() {
    ExecutionInterceptor failure =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            throw new IllegalStateException("simulated local interceptor failure");
          }
        };
    return ClientOverrideConfiguration.builder()
        .addExecutionInterceptor(failure)
        .retryPolicy(RetryPolicy.builder().numRetries(0).build())
        .build();
  }

  private void assertNoDownNodes(AlternatorLiveNodes liveNodes, List<URI> nodes) {
    for (URI node : nodes) {
      assertNotEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(node).getState());
    }
  }

  private void assertIpv6AuthoritiesAreBracketed(List<SdkHttpRequest> requests) {
    for (SdkHttpRequest request : requests) {
      String host = request.firstMatchingHeader("Host").get();
      assertTrue(host, host.startsWith("[") && host.contains("]:"));
    }
  }

  private void assertRetryWasRerouted(List<SdkHttpRequest> requests, List<byte[]> requestBodies) {
    assertEquals(2, requests.size());
    assertEquals(requests.size(), requestBodies.size());
    assertNotEquals(requests.get(0).getUri(), requests.get(1).getUri());
    assertTrue(requests.get(0).firstMatchingHeader("Authorization").isPresent());
    assertTrue(requests.get(1).firstMatchingHeader("Authorization").isPresent());
    assertTrue(requests.get(0).firstMatchingHeader("Host").isPresent());
    for (SdkHttpRequest request : requests) {
      assertEquals(request.getUri().getRawAuthority(), request.firstMatchingHeader("Host").get());
    }
    assertNotEquals(
        requests.get(0).firstMatchingHeader("Authorization"),
        requests.get(1).firstMatchingHeader("Authorization"));
    for (int i = 0; i < requests.size(); i++) {
      assertValidSignature(requests.get(i), requestBodies.get(i));
    }
  }

  private void assertLegacySignerUsed(List<SdkHttpRequest> requests) {
    assertEquals(2, requests.size());
    assertNotEquals(requests.get(0).getUri(), requests.get(1).getUri());
    for (SdkHttpRequest request : requests) {
      assertEquals(
          "Legacy " + request.getUri().getRawAuthority(),
          request.firstMatchingHeader("Authorization").orElse(null));
      assertEquals(Arrays.asList("once"), request.matchingHeaders("X-Legacy-Signer"));
      assertEquals(
          Arrays.asList("once"), request.firstMatchingRawQueryParameters("legacy-signature"));
    }
  }

  @SuppressWarnings("deprecation")
  private static void assertValidSignature(SdkHttpRequest request, byte[] body) {
    String authorization = request.firstMatchingHeader("Authorization").get();
    String scope = authorization.substring(authorization.indexOf("Credential=") + 11);
    scope = scope.substring(scope.indexOf('/') + 1, scope.indexOf(','));
    String[] scopeParts = scope.split("/");
    String date = request.firstMatchingHeader("X-Amz-Date").get();
    Instant signingInstant =
        LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"))
            .toInstant(ZoneOffset.UTC);
    SdkHttpFullRequest fullRequest =
        ((SdkHttpFullRequest) request)
            .toBuilder()
                .removeHeader("Authorization")
                .contentStreamProvider(() -> new ByteArrayInputStream(body))
                .build();
    SdkHttpFullRequest expected =
        Aws4Signer.create()
            .sign(
                fullRequest,
                Aws4SignerParams.builder()
                    .awsCredentials(AwsBasicCredentials.create("access-key", "secret-key"))
                    .signingRegion(Region.of(scopeParts[1]))
                    .signingName(scopeParts[2])
                    .doubleUrlEncode(true)
                    .normalizePath(true)
                    .signingClockOverride(Clock.fixed(signingInstant, ZoneOffset.UTC))
                    .build());

    assertEquals(expected.firstMatchingHeader("Authorization").get(), authorization);
  }

  /**
   * Helper that exercises repeated query-plan selections for one set of execution attributes.
   *
   * <p>The SDK calls {@code beforeExecution} once per API call, which creates a {@link
   * com.scylladb.alternator.internal.LazyQueryPlan} stored in {@link ExecutionAttributes}. Then
   * Direct calls to {@code modifyHttpRequest} isolate query-plan selection from the SDK retry
   * pipeline, which is covered separately by {@link
   * #testSdkRetryPipelineRoutesEachAttemptToDifferentNode()}.
   *
   * @param interceptor the query plan interceptor
   * @param totalSelections the number of query-plan selections to exercise
   * @return the selected URIs
   */
  private List<URI> selectRoutes(BasicQueryPlanInterceptor interceptor, int totalSelections)
      throws Exception {
    ExecutionAttributes executionAttributes = ExecutionAttributes.builder().build();

    // beforeExecution is called once per SDK call - creates the LazyQueryPlan
    interceptor.beforeExecution(null, executionAttributes);

    // Build a base HTTP request to be modified by the interceptor
    SdkHttpRequest baseRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("placeholder")
            .port(8000)
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();

    // Create a mock context that returns the base request
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest);

    List<URI> selectedNodes = new ArrayList<>();
    for (int selection = 0; selection < totalSelections; selection++) {
      // Exercise one successive query-plan selection.
      SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, executionAttributes);
      selectedNodes.add(
          new URI(
              modifiedRequest.protocol(),
              null,
              modifiedRequest.host(),
              modifiedRequest.port(),
              null,
              null,
              null));
    }
    return selectedNodes;
  }

  /** Helper to create a list of test node URIs. */
  private List<URI> createNodes(int count) throws Exception {
    List<URI> nodes = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      nodes.add(new URI("http://127.0.0." + i + ":8000"));
    }
    return nodes;
  }

  private List<URI> createIpv6Nodes() throws Exception {
    return Arrays.asList(
        new URI("http", null, "::1", 8000, null, null, null),
        new URI("http", null, "::2", 8000, null, null, null));
  }

  @Test
  public void subclassPlanReplacementRebuildsHealthWrapper() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    URI customNode = URI.create("http://custom-plan.local:8000");
    BasicQueryPlanInterceptor interceptor =
        new BasicQueryPlanInterceptor(liveNodes) {
          @Override
          public void beforeExecution(
              Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
            super.beforeExecution(context, executionAttributes);
            executionAttributes.putAttribute(
                QUERY_PLAN,
                new LazyQueryPlan(liveNodes) {
                  private boolean available = true;

                  @Override
                  public boolean hasNext() {
                    return available;
                  }

                  @Override
                  public URI next() {
                    available = false;
                    return customNode;
                  }
                });
          }
        };

    assertEquals(Arrays.asList(customNode), selectRoutes(interceptor, 1));
  }

  // ========== Test: 1 node, 0 retries -> 1 request to single node ==========

  /**
   * With a single node and no retries, exactly one request should be made to the only available
   * node.
   */
  @Test
  public void testSingleNodeNoRetries() throws Exception {
    List<URI> nodes = createNodes(1);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = selectRoutes(interceptor, 1);

    assertEquals("Should select exactly 1 node", 1, selectedNodes.size());
    assertEquals(
        "Request should go to the single available node",
        "127.0.0.1",
        selectedNodes.get(0).getHost());
    assertEquals("Request should use the correct port", 8000, selectedNodes.get(0).getPort());
  }

  // ========== Test: 2 nodes, 1 retry -> request to node1, retry to node2 ==========

  /**
   * With two nodes and one retry, the initial request goes to one node and the retry goes to the
   * other node. LazyQueryPlan ensures no duplicates.
   */
  @Test
  public void testTwoNodesOneRetry() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = selectRoutes(interceptor, 2);

    assertEquals("Should select 2 nodes (initial + 1 retry)", 2, selectedNodes.size());

    Set<String> uniqueHosts = selectedNodes.stream().map(URI::getHost).collect(Collectors.toSet());
    assertEquals(
        "Retry should go to a different node than the initial request", 2, uniqueHosts.size());

    // Verify both nodes were used
    assertTrue("Should include node 127.0.0.1", uniqueHosts.contains("127.0.0.1"));
    assertTrue("Should include node 127.0.0.2", uniqueHosts.contains("127.0.0.2"));
  }

  // ========== Test: 3 nodes, 2 retries -> request fans across 3 nodes ==========

  /**
   * With three nodes and two retries, each attempt should go to a different node. The initial
   * request and both retries should each target a unique node.
   */
  @Test
  public void testThreeNodesTwoRetries() throws Exception {
    List<URI> nodes = createNodes(3);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = selectRoutes(interceptor, 3);

    assertEquals("Should select 3 nodes (initial + 2 retries)", 3, selectedNodes.size());

    Set<String> uniqueHosts = selectedNodes.stream().map(URI::getHost).collect(Collectors.toSet());
    assertEquals("Each attempt should go to a different node", 3, uniqueHosts.size());

    // Verify all three nodes were used
    assertTrue("Should include node 127.0.0.1", uniqueHosts.contains("127.0.0.1"));
    assertTrue("Should include node 127.0.0.2", uniqueHosts.contains("127.0.0.2"));
    assertTrue("Should include node 127.0.0.3", uniqueHosts.contains("127.0.0.3"));
  }

  // ========== Test: 1 node, further retries -> new routing cycles ==========

  /**
   * With only one node, each later SDK attempt starts another routing cycle and reuses that node
   * without falling back to the SDK request endpoint.
   */
  @Test
  public void testSingleNodeRetriesStartNewCycles() throws Exception {
    List<URI> nodes = createNodes(1);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = selectRoutes(interceptor, 1);

    assertEquals("Should select 1 node on initial attempt", 1, selectedNodes.size());
    assertEquals(
        "Request should go to the single node", "127.0.0.1", selectedNodes.get(0).getHost());

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);

    SdkHttpRequest baseRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("127.0.0.1")
            .port(8000)
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest);

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    assertEquals("First attempt goes to the node", "127.0.0.1", first.host());

    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    SdkHttpRequest third = interceptor.modifyHttpRequest(context, attrs);
    assertEquals("127.0.0.1", second.host());
    assertEquals("127.0.0.1", third.host());
  }

  // ========== Test: verify actual node distribution via captured request URIs ==========

  /**
   * Verifies retry distribution by running multiple independent requests and checking that each
   * request's retry attempts are spread across different nodes. This uses the full mock HTTP client
   * to capture actual request URIs sent by the interceptor.
   */
  @Test
  public void testRetryDistributionAcrossMultipleRequests() throws Exception {
    List<URI> nodes = createNodes(5);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Simulate 10 independent requests, each with up to 4 retries (5 total attempts)
    for (int request = 0; request < 10; request++) {
      List<URI> selectedNodes = selectRoutes(interceptor, 5);

      assertEquals(
          "Request " + request + ": should select 5 nodes (all available)",
          5,
          selectedNodes.size());

      Set<String> uniqueHosts =
          selectedNodes.stream().map(URI::getHost).collect(Collectors.toSet());
      assertEquals(
          "Request " + request + ": all 5 attempts should go to different nodes",
          5,
          uniqueHosts.size());
    }
  }

  /**
   * Verifies that with more retry attempts than available nodes, the subsequent selection starts a
   * new routing cycle instead of using the SDK request endpoint.
   */
  @Test
  public void testRetriesExceedNodeCount() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);

    SdkHttpRequest baseRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("fallback-host")
            .port(9999)
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest);

    // First two calls get different nodes from the plan
    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    assertNotEquals("First two attempts should go to different nodes", first.host(), second.host());

    SdkHttpRequest third = interceptor.modifyHttpRequest(context, attrs);
    assertTrue(nodes.contains(endpoint(third)));
  }

  @Test
  public void testRetryReportsPreviousAttemptTransportFailure() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    assertTrue(liveNodes.reports.isEmpty());

    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);

    URI firstNode = endpoint(first);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_FAILURE), liveNodes.reports.get(0));
  }

  @Test
  @CoversRequirements("HEALTH-REQ-002")
  public void testRetryableServerErrorsDoNotReportHealthResults() throws Exception {
    for (int statusCode : Arrays.asList(500, 502, 503, 504)) {
      List<URI> nodes = createNodes(2);
      MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
      BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);
      ExecutionAttributes attrs = ExecutionAttributes.builder().build();
      interceptor.beforeExecution(null, attrs);

      SdkHttpRequest first =
          interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);
      interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
      interceptor.afterTransmission(new MockAfterTransmissionContext(first, statusCode), attrs);
      interceptor.onExecutionFailure(
          new MockFailedExecutionContext(
              first, statusCode, dynamoDbException(statusCode, "InternalServerError")),
          attrs);

      assertTrue("status " + statusCode + " should be health-neutral", liveNodes.reports.isEmpty());
    }
  }

  @Test
  public void testRetryableServerErrorIsClearedBeforeNextRetry() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 500), attrs);
    assertTrue(liveNodes.reports.isEmpty());

    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(second), attrs);

    URI firstNode = endpoint(first);
    URI secondNode = endpoint(second);
    assertNotEquals(firstNode, secondNode);
    assertTrue(liveNodes.reports.isEmpty());
  }

  @Test
  public void testRetryableServerErrorDoesNotResetTrafficFailureProgress() throws Exception {
    List<URI> nodes = createNodes(1);
    URI node = nodes.get(0);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build());
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    reportCurrentTraffic(liveNodes, node, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reports.clear();

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    SdkHttpRequest request =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(request), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(request, 500), attrs);

    assertTrue(liveNodes.reports.isEmpty());
    assertEquals(1, liveNodes.getNodeHealthStatus(node).getConsecutiveFailures());
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(node).getState());
  }

  @Test
  public void testNonRetryableServerErrorCountsAsSuccessfulContact() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);
    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);

    SdkHttpRequest first =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 501), attrs);

    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(endpoint(first), NodeHealthObservation.TRAFFIC_SUCCESS),
        liveNodes.reports.get(0));
  }

  @Test
  public void testDynamoDbAuthenticationErrorCodeCountsAsSuccessfulContact() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 400), attrs);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(endpoint(first), NodeHealthObservation.TRAFFIC_SUCCESS),
        liveNodes.reports.get(0));
    interceptor.onExecutionFailure(
        new MockFailedExecutionContext(
            first,
            400,
            dynamoDbException(
                400, "com.amazonaws.dynamodb.v20120810#MissingAuthenticationTokenException")),
        attrs);

    URI firstNode = endpoint(first);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_SUCCESS), liveNodes.reports.get(0));
  }

  @Test
  public void testDynamoDbAuthenticationHttpStatusCountsAsSuccessfulContact() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 403), attrs);

    URI firstNode = endpoint(first);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_SUCCESS), liveNodes.reports.get(0));
  }

  @Test
  public void testModifyHttpRequestSkipsDownCandidateAtFinalGate() throws Exception {
    List<URI> nodes = createNodes(2);
    URI down = nodes.get(0);
    URI active = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    reportCurrentTraffic(liveNodes, down, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reports.clear();
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(
        attrs, new LazyQueryPlan(liveNodes, seedWhereFirstNodeIs(nodes, down)));

    SdkHttpRequest routed =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);

    assertEquals(active, endpoint(routed));
  }

  @Test
  public void testModifyHttpRequestRejectsKnownDownNodeWhenNoActiveNodes() throws Exception {
    List<URI> nodes = createNodes(1);
    URI down = nodes.get(0);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    reportCurrentTraffic(liveNodes, down, NodeHealthObservation.TRAFFIC_FAILURE);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, 0));

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .protocol(down.getScheme())
            .host(down.getHost())
            .port(down.getPort())
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                interceptor.modifyHttpRequest(
                    new MockModifyHttpRequestContext(originalRequest), attrs));

    assertEquals("No live nodes available", failure.getMessage());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(down).getState());
  }

  @Test
  public void testModifyHttpRequestRejectsAllKnownDownNodesWhenNoActiveNodes() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    reportCurrentTraffic(liveNodes, nodes.get(0), NodeHealthObservation.TRAFFIC_FAILURE);
    reportCurrentTraffic(liveNodes, nodes.get(1), NodeHealthObservation.TRAFFIC_FAILURE);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, 0));
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class, () -> interceptor.modifyHttpRequest(context, attrs));

    assertEquals("No live nodes available", failure.getMessage());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(nodes.get(0)).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(nodes.get(1)).getState());
  }

  @Test
  public void testModifyHttpRequestUsesQuarantineAfterActivePass() throws Exception {
    List<URI> nodes = createNodes(2);
    URI recovering = nodes.get(0);
    URI active = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .build());
    reportCurrentTraffic(liveNodes, recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reports.clear();
    long seed = seedWhereFirstNodeIs(nodes, recovering);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, seed));
    SdkHttpRequest first =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);
    SdkHttpRequest second =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);

    assertEquals(active, endpoint(first));
    assertEquals(recovering, endpoint(second));
  }

  @Test
  public void testModifyHttpRequestPreservesQuarantineRelativeOrderAfterActiveCandidate()
      throws Exception {
    List<URI> nodes = createNodes(2);
    URI recovering = nodes.get(0);
    URI active = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .build());
    reportCurrentTraffic(liveNodes, recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reports.clear();
    long seed = seedWhereFirstNodeIs(nodes, active);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, seed));
    SdkHttpRequest first =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);
    SdkHttpRequest second =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);

    assertEquals(active, endpoint(first));
    assertEquals(recovering, endpoint(second));
  }

  @Test
  public void testModifyHttpRequestStartsNewCycleWhenQuarantineBecomesDown() throws Exception {
    List<URI> nodes = createNodes(2);
    URI active = nodes.get(0);
    URI recovering = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineFailureThreshold(1)
                .build());
    reportCurrentTraffic(liveNodes, recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reports.clear();
    long seed = seedWhereFirstNodeIs(nodes, active);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, seed));
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    reportCurrentTraffic(liveNodes, recovering, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(active, endpoint(first));
    assertEquals(active, endpoint(interceptor.modifyHttpRequest(context, attrs)));
  }

  // ========== Mock implementations ==========

  private SdkHttpRequest baseRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("placeholder")
        .port(8000)
        .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
        .encodedPath("/")
        .build();
  }

  private URI endpoint(SdkHttpRequest request) throws Exception {
    return new URI(request.protocol(), null, request.host(), request.port(), null, null, null);
  }

  private void reportCurrentTraffic(
      AlternatorLiveNodes liveNodes, URI node, NodeHealthObservation observation) {
    liveNodes.reportNodeResult(node, observation, liveNodes.getNodeHealthGeneration(node));
  }

  private long seedWhereFirstNodeIs(List<URI> candidates, URI expected) throws Exception {
    for (long seed = -1000; seed < 1000; seed++) {
      if (expected.equals(firstNodeForSeed(candidates, seed))) {
        return seed;
      }
    }
    fail("Could not find seed for " + expected);
    return 0;
  }

  private URI firstNodeForSeed(List<URI> candidates, long seed) {
    LazyQueryPlan plan = new LazyQueryPlan(new MockAlternatorLiveNodes(candidates), seed);
    return plan.hasNext() ? plan.next() : null;
  }

  private Throwable dynamoDbException(int statusCode, String errorCode) {
    return DynamoDbException.builder()
        .message(errorCode)
        .statusCode(statusCode)
        .awsErrorDetails(
            AwsErrorDetails.builder()
                .errorCode(errorCode)
                .errorMessage(errorCode)
                .sdkHttpResponse(SdkHttpFullResponse.builder().statusCode(statusCode).build())
                .build())
        .build();
  }

  /**
   * Mock AlternatorLiveNodes that provides a fixed list of nodes without network calls.
   *
   * <p>This mock:
   *
   * <ul>
   *   <li>Does not start any background threads
   *   <li>Provides a fixed list of test nodes
   *   <li>Supports LazyQueryPlan creation (via base class)
   * </ul>
   */
  private static class MockAlternatorLiveNodes extends AlternatorLiveNodes {
    private final List<URI> nodes;
    private final List<NodeReport> reports = new ArrayList<>();

    MockAlternatorLiveNodes(List<URI> nodes) {
      this(nodes, NodeHealthConfig.getDefault());
    }

    MockAlternatorLiveNodes(List<URI> nodes, NodeHealthConfig nodeHealthConfig) {
      super(
          AlternatorConfig.builder()
              .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
              .withScheme(nodes.get(0).getScheme())
              .withPort(nodes.get(0).getPort())
              .withNodeHealthConfig(nodeHealthConfig)
              .build());
      this.nodes = new ArrayList<>(nodes);
      for (URI node : nodes) {
        super.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
      }
      reports.clear();
    }

    @Override
    protected List<URI> getDiscoveredNodesInternal() {
      return nodes;
    }

    @Override
    public void start() {}

    @Override
    public void reportNodeResult(URI node, NodeHealthObservation observation) {
      super.reportNodeResult(node, observation);
      reports.add(new NodeReport(node, observation));
    }

    @Override
    public void reportNodeResult(
        URI node, NodeHealthObservation observation, long expectedTrafficGeneration) {
      super.reportNodeResult(node, observation, expectedTrafficGeneration);
      reports.add(new NodeReport(node, observation));
    }
  }

  private static class RetryingSdkHttpClient implements SdkHttpClient {
    private final List<SdkHttpRequest> requests = new ArrayList<>();
    private final List<byte[]> requestBodies = new ArrayList<>();
    private final boolean retryFirstResponse;

    private RetryingSdkHttpClient() {
      this(true);
    }

    private RetryingSdkHttpClient(boolean retryFirstResponse) {
      this.retryFirstResponse = retryFirstResponse;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      requests.add(request.httpRequest());
      try {
        requestBodies.add(
            request.contentStreamProvider().isPresent()
                ? readAll(request.contentStreamProvider().get().newStream())
                : new byte[0]);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      int attempt = requests.size();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          boolean retry = retryFirstResponse && attempt == 1;
          String responseBody = retry ? "{\"message\":\"retry\"}" : "{}";
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          SdkHttpFullResponse response =
              SdkHttpFullResponse.builder()
                  .statusCode(retry ? 500 : 200)
                  .putHeader("Content-Type", "application/x-amz-json-1.0")
                  .putHeader("Content-Length", String.valueOf(body.length))
                  .build();
          return HttpExecuteResponse.builder()
              .response(response)
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
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
      return "RetryingSdkHttpClient";
    }
  }

  @SuppressWarnings("deprecation")
  private static final class LegacyAuthoritySigner implements Signer {
    @Override
    public SdkHttpFullRequest sign(
        SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
      return request.toBuilder()
          .putHeader("Authorization", "Legacy " + request.getUri().getRawAuthority())
          .appendHeader("X-Legacy-Signer", "once")
          .appendRawQueryParameter("legacy-signature", "once")
          .build();
    }
  }

  @SuppressWarnings("deprecation")
  private static final class FailOnceLegacySigner implements Signer {
    private final List<URI> signedAuthorities = new ArrayList<>();

    @Override
    public SdkHttpFullRequest sign(
        SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
      signedAuthorities.add(request.getUri());
      if (signedAuthorities.size() == 1) {
        throw RetryableException.create("simulated retryable signing failure");
      }
      return request.toBuilder()
          .putHeader("Authorization", "Legacy " + request.getUri().getRawAuthority())
          .build();
    }
  }

  @SuppressWarnings("deprecation")
  private static final class TransformingLegacySigner implements Signer {
    private final AtomicInteger signings = new AtomicInteger();

    @Override
    public SdkHttpFullRequest sign(
        SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
      signings.incrementAndGet();
      ContentStreamProvider original = request.contentStreamProvider().orElse(null);
      ContentStreamProvider transformed =
          original == null
              ? null
              : () ->
                  new java.io.SequenceInputStream(
                      new ByteArrayInputStream(new byte[] {'X'}), original.newStream());
      SdkHttpFullRequest.Builder signed =
          request.toBuilder()
              .putHeader("Authorization", "Legacy " + request.getUri().getRawAuthority())
              .contentStreamProvider(transformed);
      request
          .firstMatchingHeader("Content-Length")
          .map(Long::parseLong)
          .ifPresent(length -> signed.putHeader("Content-Length", Long.toString(length + 1)));
      return signed.build();
    }
  }

  @SuppressWarnings("deprecation")
  private static final class LegacyAsyncBodySigner implements Signer, AsyncRequestBodySigner {
    private final AtomicInteger bodySignings = new AtomicInteger();

    @Override
    public SdkHttpFullRequest sign(
        SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
      return request.toBuilder()
          .putHeader("Authorization", "Legacy " + request.getUri().getRawAuthority())
          .build();
    }

    @Override
    public AsyncRequestBody signAsyncRequestBody(
        SdkHttpFullRequest request,
        AsyncRequestBody requestBody,
        ExecutionAttributes executionAttributes) {
      bodySignings.incrementAndGet();
      if (requestBody == null) {
        return null;
      }
      return new AsyncRequestBody() {
        @Override
        public Optional<Long> contentLength() {
          return requestBody.contentLength().map(length -> length + 1);
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
          requestBody.subscribe(
              new Subscriber<ByteBuffer>() {
                private boolean first = true;

                @Override
                public void onSubscribe(Subscription subscription) {
                  subscriber.onSubscribe(subscription);
                }

                @Override
                public void onNext(ByteBuffer item) {
                  if (!first) {
                    subscriber.onNext(item);
                    return;
                  }
                  first = false;
                  ByteBuffer source = item.asReadOnlyBuffer();
                  ByteBuffer prefixed = ByteBuffer.allocate(source.remaining() + 1);
                  prefixed.put((byte) 'X').put(source).flip();
                  subscriber.onNext(prefixed);
                }

                @Override
                public void onError(Throwable failure) {
                  subscriber.onError(failure);
                }

                @Override
                public void onComplete() {
                  subscriber.onComplete();
                }
              });
        }
      };
    }
  }

  private static class RetryingSdkAsyncHttpClient implements SdkAsyncHttpClient {
    private final List<SdkHttpRequest> requests = new ArrayList<>();
    private final List<byte[]> requestBodies = new ArrayList<>();

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      requests.add(request.request());
      int attempt = requests.size();
      return readAll(request.requestContentPublisher())
          .thenAccept(
              requestBody -> {
                requestBodies.add(requestBody);
                String responseBody = attempt == 1 ? "{\"message\":\"retry\"}" : "{}";
                byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
                SdkHttpFullResponse response =
                    SdkHttpFullResponse.builder()
                        .statusCode(attempt == 1 ? 500 : 200)
                        .putHeader("Content-Type", "application/x-amz-json-1.0")
                        .putHeader("Content-Length", String.valueOf(body.length))
                        .build();
                request.responseHandler().onHeaders(response);
                request.responseHandler().onStream(new ByteArrayPublisher(body));
              });
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "RetryingSdkAsyncHttpClient";
    }
  }

  private static final class SynchronouslyFailingSdkAsyncHttpClient implements SdkAsyncHttpClient {
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      calls.incrementAndGet();
      throw new IllegalStateException("simulated synchronous transport failure");
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "SynchronouslyFailingSdkAsyncHttpClient";
    }
  }

  private static byte[] readAll(java.io.InputStream input) throws IOException {
    java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int read;
    while ((read = input.read(buffer)) != -1) {
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  private static CompletableFuture<byte[]> readAll(Publisher<ByteBuffer> publisher) {
    CompletableFuture<byte[]> result = new CompletableFuture<>();
    java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
    publisher.subscribe(
        new Subscriber<ByteBuffer>() {
          @Override
          public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
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

  private static class ByteArrayPublisher implements Publisher<ByteBuffer> {
    private final byte[] body;

    private ByteArrayPublisher(byte[] body) {
      this.body = body;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            private boolean complete;

            @Override
            public void request(long count) {
              if (!complete) {
                complete = true;
                subscriber.onNext(ByteBuffer.wrap(body));
                subscriber.onComplete();
              }
            }

            @Override
            public void cancel() {
              complete = true;
            }
          });
    }
  }

  private static class TestableBasicQueryPlanInterceptor extends BasicQueryPlanInterceptor {
    TestableBasicQueryPlanInterceptor(AlternatorLiveNodes liveNodes) {
      super(liveNodes);
    }

    void setQueryPlan(ExecutionAttributes attrs, LazyQueryPlan plan) {
      attrs.putAttribute(QUERY_PLAN, plan);
    }
  }

  private static class NodeReport {
    private final URI node;
    private final NodeHealthObservation observation;

    NodeReport(URI node, NodeHealthObservation observation) {
      this.node = node;
      this.observation = observation;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof NodeReport)) {
        return false;
      }
      NodeReport that = (NodeReport) other;
      return Objects.equals(node, that.node) && observation == that.observation;
    }

    @Override
    public int hashCode() {
      return Objects.hash(node, observation);
    }
  }

  /**
   * Minimal mock implementation of {@link
   * software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest} that returns a fixed {@link
   * SdkHttpRequest}.
   *
   * <p>This allows calling {@code modifyHttpRequest} on the interceptor without a full SDK request
   * pipeline.
   */
  private static class MockModifyHttpRequestContext
      implements software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest {

    private final SdkHttpRequest httpRequest;

    MockModifyHttpRequestContext(SdkHttpRequest httpRequest) {
      this.httpRequest = httpRequest;
    }

    @Override
    public SdkHttpRequest httpRequest() {
      return httpRequest;
    }

    @Override
    public software.amazon.awssdk.core.SdkRequest request() {
      // Not needed for modifyHttpRequest tests
      return null;
    }

    @Override
    public Optional<software.amazon.awssdk.core.sync.RequestBody> requestBody() {
      return Optional.empty();
    }

    @Override
    public Optional<software.amazon.awssdk.core.async.AsyncRequestBody> asyncRequestBody() {
      return Optional.empty();
    }
  }

  private static class MockBeforeTransmissionContext extends MockModifyHttpRequestContext
      implements software.amazon.awssdk.core.interceptor.Context.BeforeTransmission {

    MockBeforeTransmissionContext(SdkHttpRequest httpRequest) {
      super(httpRequest);
    }
  }

  private static class MockAfterTransmissionContext extends MockModifyHttpRequestContext
      implements software.amazon.awssdk.core.interceptor.Context.AfterTransmission {

    private final SdkHttpResponse response;

    MockAfterTransmissionContext(SdkHttpRequest httpRequest, int statusCode) {
      super(httpRequest);
      this.response = SdkHttpFullResponse.builder().statusCode(statusCode).build();
    }

    @Override
    public SdkHttpResponse httpResponse() {
      return response;
    }

    @Override
    public Optional<org.reactivestreams.Publisher<java.nio.ByteBuffer>> responsePublisher() {
      return Optional.empty();
    }

    @Override
    public Optional<java.io.InputStream> responseBody() {
      return Optional.empty();
    }
  }

  private static class MockFailedExecutionContext
      implements software.amazon.awssdk.core.interceptor.Context.FailedExecution {

    private final SdkHttpRequest httpRequest;
    private final SdkHttpResponse httpResponse;
    private final Throwable exception;

    MockFailedExecutionContext(SdkHttpRequest httpRequest, int statusCode, Throwable exception) {
      this.httpRequest = httpRequest;
      this.httpResponse = SdkHttpFullResponse.builder().statusCode(statusCode).build();
      this.exception = exception;
    }

    @Override
    public Throwable exception() {
      return exception;
    }

    @Override
    public SdkRequest request() {
      return null;
    }

    @Override
    public Optional<SdkHttpRequest> httpRequest() {
      return Optional.of(httpRequest);
    }

    @Override
    public Optional<SdkHttpResponse> httpResponse() {
      return Optional.of(httpResponse);
    }

    @Override
    public Optional<SdkResponse> response() {
      return Optional.empty();
    }
  }
}
