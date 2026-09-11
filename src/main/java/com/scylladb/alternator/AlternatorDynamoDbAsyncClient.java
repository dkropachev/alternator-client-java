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

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.AsyncClientDetector;
import com.scylladb.alternator.internal.CrtAsyncClientFactory;
import com.scylladb.alternator.internal.NettyAsyncClientFactory;
import com.scylladb.alternator.internal.SyncClientDetector;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.AttemptRoutingSdkAsyncHttpClient;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.endpoints.AccountIdEndpointMode;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory class for creating DynamoDB async clients with Alternator load balancing support.
 *
 * <p>This class provides a builder that simplifies the construction of an async DynamoDB client
 * (AWS SDK v2) that automatically distributes requests across all nodes in an Alternator cluster.
 * It provides a fluent API compatible with {@link DynamoDbAsyncClient#builder()} while
 * automatically integrating query plan interceptors for client-side load balancing.
 *
 * <p>The builder implements {@link DynamoDbAsyncClientBuilder}, ensuring compatibility with
 * standard AWS SDK v2 patterns while adding Alternator-specific configuration via {@link
 * AlternatorDynamoDbAsyncClientBuilder#withRoutingScope}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Standard usage - returns DynamoDbAsyncClient
 * DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // With Alternator-specific APIs - returns AlternatorDynamoDbAsyncClientWrapper
 * AlternatorDynamoDbAsyncClientWrapper client = AlternatorDynamoDbAsyncClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .credentialsProvider(credentialsProvider)
 *     .buildWithAlternatorAPI();
 *
 * // Access Alternator-specific functionality
 * List<URI> nodes = client.getLiveNodes();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AlternatorDynamoDbAsyncClient {

  /**
   * Creates a new builder instance using the standard builder pattern, similar to {@link
   * DynamoDbAsyncClient#builder()}.
   *
   * @return a new {@link AlternatorDynamoDbAsyncClientBuilder} configured for Alternator load
   *     balancing
   */
  public static AlternatorDynamoDbAsyncClientBuilder builder() {
    return new AlternatorDynamoDbAsyncClientBuilder();
  }

  /**
   * Builder implementation for constructing async DynamoDB clients with Alternator load balancing.
   *
   * <p>This builder implements {@link DynamoDbAsyncClientBuilder} and delegates most configuration
   * to the standard AWS SDK {@link DynamoDbAsyncClient} builder while automatically integrating
   * query plan interceptors for node discovery and load balancing.
   *
   * <p>Note: Some AWS-specific features are not supported by Alternator and will throw {@link
   * UnsupportedOperationException}, including endpoint discovery, FIPS mode, and dual-stack
   * networking.
   */
  public static class AlternatorDynamoDbAsyncClientBuilder implements DynamoDbAsyncClientBuilder {
    private final DynamoDbAsyncClientBuilder delegate;
    private final AlternatorConfig.Builder configBuilder;
    private URI seedUri;
    private Region region;
    private boolean seedHostsSet = false;
    private boolean disableCertificateChecks = false;
    private boolean httpClientSet = false;
    private boolean credentialsProviderSet = false;
    private SdkAsyncHttpClient customHttpClient;
    private SdkAsyncHttpClient.Builder customHttpClientBuilder;
    private Consumer<NettyNioAsyncHttpClient.Builder> nettyCustomizer;
    private Consumer<AwsCrtAsyncHttpClient.Builder> crtAsyncCustomizer;
    private HttpClientType httpClientType;
    private UnaryOperator<String> userAgentTransformer = AlternatorUserAgent.defaultUserAgent();

    AlternatorDynamoDbAsyncClientBuilder() {
      this.delegate = DynamoDbAsyncClient.builder();
      this.configBuilder = AlternatorConfig.builder();
    }

    /**
     * Sets the routing scope for node targeting with fallback support.
     *
     * @param routingScope the routing scope, or {@code null} to use all nodes
     * @return this builder instance
     * @since 2.0.0
     */
    public AlternatorDynamoDbAsyncClientBuilder withRoutingScope(RoutingScope routingScope) {
      configBuilder.withRoutingScope(routingScope);
      return this;
    }

    /**
     * Sets the seed host addresses used for Alternator node discovery.
     *
     * <p>{@link #endpointOverride(URI)} is still required and supplies the scheme and port for
     * these hosts. When {@code ClusterScope} is used, each configured seed is queried for {@code
     * /localnodes}; provide seeds from every datacenter that should participate in cluster-wide
     * routing. For datacenter or rack scoped routing in a multi-datacenter cluster, provide a seed
     * from the target datacenter.
     *
     * @param hosts collection of seed host addresses (IP or hostname)
     * @return this builder instance
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withSeedHosts(Collection<String> hosts) {
      if (hosts != null) {
        configBuilder.withSeedHosts(hosts);
        seedHostsSet = true;
      }
      return this;
    }

    /**
     * Sets a single seed host address used for Alternator node discovery.
     *
     * @param host seed host address (IP or hostname)
     * @return this builder instance
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withSeedHost(String host) {
      if (host != null) {
        configBuilder.withSeedHost(host);
        seedHostsSet = true;
      }
      return this;
    }

    /**
     * Sets the request compression algorithm.
     *
     * @param algorithm the compression algorithm to use, or null to disable compression
     * @return this builder instance
     */
    public AlternatorDynamoDbAsyncClientBuilder withCompressionAlgorithm(
        RequestCompressionAlgorithm algorithm) {
      configBuilder.withCompressionAlgorithm(algorithm);
      return this;
    }

    /**
     * Sets the minimum request body size (in bytes) that triggers compression.
     *
     * @param minCompressionSizeBytes minimum request size in bytes, must be non-negative
     * @return this builder instance
     * @throws IllegalArgumentException if minCompressionSizeBytes is negative
     */
    public AlternatorDynamoDbAsyncClientBuilder withMinCompressionSizeBytes(
        int minCompressionSizeBytes) {
      configBuilder.withMinCompressionSizeBytes(minCompressionSizeBytes);
      return this;
    }

    /**
     * Sets the HTTP response compression algorithms to advertise and decode.
     *
     * <p>The configured order is used for the {@code Accept-Encoding} header.
     *
     * @param algorithms the response compression algorithms to enable
     * @return this builder instance
     * @throws IllegalArgumentException if algorithms is null, empty, or contains null
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withResponseCompression(
        ResponseCompressionAlgorithm... algorithms) {
      configBuilder.withResponseCompression(algorithms);
      return this;
    }

    /**
     * Sets the HTTP response compression algorithms to advertise and decode.
     *
     * <p>The configured order is used for the {@code Accept-Encoding} header.
     *
     * @param algorithms the response compression algorithms to enable
     * @return this builder instance
     * @throws IllegalArgumentException if algorithms is null, empty, or contains null
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withResponseCompression(
        Collection<ResponseCompressionAlgorithm> algorithms) {
      configBuilder.withResponseCompression(algorithms);
      return this;
    }

    /**
     * Disables HTTP response compression negotiation and decompression.
     *
     * @return this builder instance
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withResponseCompressionDisabled() {
      configBuilder.withResponseCompressionDisabled();
      return this;
    }

    /**
     * Enables or disables HTTP header optimization.
     *
     * @param optimizeHeaders true to enable header filtering, false to disable
     * @return this builder instance
     */
    public AlternatorDynamoDbAsyncClientBuilder withOptimizeHeaders(boolean optimizeHeaders) {
      configBuilder.withOptimizeHeaders(optimizeHeaders);
      return this;
    }

    /**
     * Sets a custom whitelist of HTTP headers to preserve when optimization is enabled.
     *
     * @param headers collection of header names to preserve (case-insensitive)
     * @return this builder instance
     * @throws IllegalArgumentException if headers is null or empty
     */
    public AlternatorDynamoDbAsyncClientBuilder withHeadersWhitelist(Collection<String> headers) {
      configBuilder.withHeadersWhitelist(headers);
      return this;
    }

    /**
     * Replaces the final {@code User-Agent} header with the provided value.
     *
     * @param userAgent the exact user-agent value to send
     * @return this builder instance
     * @throws IllegalArgumentException if userAgent is null or blank
     * @since 2.0.5
     */
    public AlternatorDynamoDbAsyncClientBuilder withUserAgent(String userAgent) {
      this.userAgentTransformer = AlternatorUserAgent.replaceWith(userAgent);
      configBuilder.withUserAgentEnabled(true);
      return this;
    }

    /**
     * Transforms the final {@code User-Agent} header before the request is sent.
     *
     * <p>The function receives the default ScyllaDB Alternator client user-agent. Returning null or
     * blank removes the {@code User-Agent} header.
     *
     * @param userAgentTransformer function that maps the generated user-agent to the value to send
     * @return this builder instance
     * @throws IllegalArgumentException if userAgentTransformer is null
     * @since 2.0.5
     */
    public AlternatorDynamoDbAsyncClientBuilder withUserAgent(
        UnaryOperator<String> userAgentTransformer) {
      this.userAgentTransformer = AlternatorUserAgent.transformDefault(userAgentTransformer);
      configBuilder.withUserAgentEnabled(true);
      return this;
    }

    /**
     * Removes the {@code User-Agent} header from outgoing requests.
     *
     * <p>When header optimization is enabled, {@code User-Agent} is also removed from the required
     * optimized header whitelist.
     *
     * @return this builder instance
     * @since 2.0.5
     */
    public AlternatorDynamoDbAsyncClientBuilder withoutUserAgent() {
      this.userAgentTransformer = AlternatorUserAgent.disable();
      configBuilder.withUserAgentEnabled(false);
      return this;
    }

    /**
     * Sets the TLS session cache configuration for quick TLS renegotiation.
     *
     * @param tlsSessionCacheConfig the TLS session cache configuration, or null to use default
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use {@link #withTlsConfig(TlsConfig)} instead
     */
    @Deprecated
    public AlternatorDynamoDbAsyncClientBuilder withTlsSessionCacheConfig(
        TlsSessionCacheConfig tlsSessionCacheConfig) {
      configBuilder.withTlsSessionCacheConfig(tlsSessionCacheConfig);
      return this;
    }

    /**
     * Sets the TLS configuration for secure connections.
     *
     * @param tlsConfig the TLS configuration, or null to use default (trust-all)
     * @return this builder instance
     * @since 1.0.9
     */
    public AlternatorDynamoDbAsyncClientBuilder withTlsConfig(TlsConfig tlsConfig) {
      configBuilder.withTlsConfig(tlsConfig);
      return this;
    }

    /**
     * Sets the key route affinity configuration.
     *
     * @param keyRouteAffinityConfig the key route affinity configuration
     * @return this builder instance
     * @since 1.0.7
     */
    public AlternatorDynamoDbAsyncClientBuilder withKeyRouteAffinity(
        KeyRouteAffinityConfig keyRouteAffinityConfig) {
      configBuilder.withKeyRouteAffinity(keyRouteAffinityConfig);
      return this;
    }

    /**
     * Sets the key route affinity type with no pre-configured PK info.
     *
     * @param type the key route affinity type
     * @return this builder instance
     * @since 1.0.7
     */
    public AlternatorDynamoDbAsyncClientBuilder withKeyRouteAffinity(KeyRouteAffinity type) {
      configBuilder.withKeyRouteAffinity(type);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when there are active requests.
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbAsyncClientBuilder withActiveRefreshIntervalMs(long intervalMs) {
      configBuilder.withActiveRefreshIntervalMs(intervalMs);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when the client is idle.
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbAsyncClientBuilder withIdleRefreshIntervalMs(long intervalMs) {
      configBuilder.withIdleRefreshIntervalMs(intervalMs);
      return this;
    }

    /**
     * Sets the node health tracking configuration.
     *
     * @param nodeHealthConfig the node health configuration, or null to use defaults
     * @return this builder instance
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withNodeHealthConfig(
        NodeHealthConfig nodeHealthConfig) {
      configBuilder.withNodeHealthConfig(nodeHealthConfig);
      return this;
    }

    /**
     * Disables node health tracking.
     *
     * @return this builder instance
     * @since 2.0.6
     */
    public AlternatorDynamoDbAsyncClientBuilder withNodeHealthDisabled() {
      configBuilder.withNodeHealthDisabled();
      return this;
    }

    /**
     * Sets the maximum number of connections in the HTTP client connection pool.
     *
     * @param maxConnections the maximum number of connections (must be positive)
     * @return this builder instance
     * @since 2.0.2
     */
    public AlternatorDynamoDbAsyncClientBuilder withMaxConnections(int maxConnections) {
      configBuilder.withMaxConnections(maxConnections);
      return this;
    }

    /**
     * Sets the maximum idle time for pooled connections in milliseconds.
     *
     * @param connectionMaxIdleTimeMs the maximum idle time in milliseconds (must be non-negative)
     * @return this builder instance
     * @since 2.0.2
     */
    public AlternatorDynamoDbAsyncClientBuilder withConnectionMaxIdleTimeMs(
        long connectionMaxIdleTimeMs) {
      configBuilder.withConnectionMaxIdleTimeMs(connectionMaxIdleTimeMs);
      return this;
    }

    /**
     * Sets the maximum lifetime for pooled connections in milliseconds.
     *
     * @param connectionTimeToLiveMs the connection time-to-live in milliseconds (0 for unlimited)
     * @return this builder instance
     * @since 2.0.2
     */
    public AlternatorDynamoDbAsyncClientBuilder withConnectionTimeToLiveMs(
        long connectionTimeToLiveMs) {
      configBuilder.withConnectionTimeToLiveMs(connectionTimeToLiveMs);
      return this;
    }

    /**
     * Sets the maximum time to wait for a connection from the pool in milliseconds.
     *
     * @param connectionAcquisitionTimeoutMs the acquisition timeout in milliseconds (must be
     *     non-negative)
     * @return this builder instance
     * @since 2.0.3
     */
    public AlternatorDynamoDbAsyncClientBuilder withConnectionAcquisitionTimeoutMs(
        long connectionAcquisitionTimeoutMs) {
      configBuilder.withConnectionAcquisitionTimeoutMs(connectionAcquisitionTimeoutMs);
      return this;
    }

    /**
     * Sets the maximum time to wait for a connection to be established in milliseconds.
     *
     * @param connectionTimeoutMs the connection timeout in milliseconds (must be non-negative)
     * @return this builder instance
     * @since 2.0.3
     */
    public AlternatorDynamoDbAsyncClientBuilder withConnectionTimeoutMs(long connectionTimeoutMs) {
      configBuilder.withConnectionTimeoutMs(connectionTimeoutMs);
      return this;
    }

    /**
     * Sets the complete Alternator configuration.
     *
     * @param config the Alternator configuration
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use individual builder methods instead
     */
    @Deprecated
    public AlternatorDynamoDbAsyncClientBuilder withAlternatorConfig(AlternatorConfig config) {
      if (config != null) {
        if (!config.getSeedHosts().isEmpty()) {
          configBuilder.withSeedHosts(config.getSeedHosts());
          seedHostsSet = true;
        }
        configBuilder.withRoutingScope(config.getRoutingScope());
        configBuilder.withCompressionAlgorithm(config.getCompressionAlgorithm());
        configBuilder.withMinCompressionSizeBytes(config.getMinCompressionSizeBytes());
        if (config.isResponseCompressionEnabled()) {
          configBuilder.withResponseCompression(config.getResponseCompressionAlgorithms());
        } else {
          configBuilder.withResponseCompressionDisabled();
        }
        configBuilder.withOptimizeHeaders(config.isOptimizeHeaders());
        configBuilder.withHeadersWhitelist(config.getHeadersWhitelist());
        configBuilder.withUserAgentEnabled(config.isUserAgentEnabled());
        userAgentTransformer =
            config.isUserAgentEnabled()
                ? AlternatorUserAgent.defaultUserAgent()
                : AlternatorUserAgent.disable();
        configBuilder.withTlsConfig(config.getTlsConfig());
        configBuilder.withTlsSessionCacheConfig(config.getTlsSessionCacheConfig());
        configBuilder.withKeyRouteAffinity(config.getKeyRouteAffinityConfig());
        configBuilder.withActiveRefreshIntervalMs(config.getActiveRefreshIntervalMs());
        configBuilder.withIdleRefreshIntervalMs(config.getIdleRefreshIntervalMs());
        configBuilder.withNodeHealthConfig(config.getNodeHealthConfig());
        configBuilder.withMaxConnections(config.getMaxConnections());
        configBuilder.withConnectionMaxIdleTimeMs(config.getConnectionMaxIdleTimeMs());
        configBuilder.withConnectionTimeToLiveMs(config.getConnectionTimeToLiveMs());
        configBuilder.withConnectionAcquisitionTimeoutMs(
            config.getConnectionAcquisitionTimeoutMs());
        configBuilder.withConnectionTimeoutMs(config.getConnectionTimeoutMs());
      }
      return this;
    }

    /**
     * Disables SSL certificate validation for testing purposes.
     *
     * <p><strong>WARNING:</strong> This should only be used for testing with self-signed
     * certificates. Never use this in production.
     *
     * @return this builder instance
     */
    public AlternatorDynamoDbAsyncClientBuilder withDisableCertificateChecks() {
      this.disableCertificateChecks = true;
      return this;
    }

    /**
     * Sets a customizer for the Netty NIO async HTTP client builder.
     *
     * <p>The customizer is called after Alternator-optimized defaults are applied, allowing users
     * to override specific settings. This is mutually exclusive with {@link
     * #withCrtAsyncHttpClientCustomizer} and {@link #httpClient(SdkAsyncHttpClient)}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .withNettyHttpClientCustomizer(builder -> builder
     *         .maxConcurrency(200)
     *         .connectionTimeout(Duration.ofSeconds(5)))
     *     .build();
     * }</pre>
     *
     * @param customizer a consumer that customizes the Netty HTTP client builder
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbAsyncClientBuilder withNettyHttpClientCustomizer(
        Consumer<NettyNioAsyncHttpClient.Builder> customizer) {
      this.nettyCustomizer = customizer;
      return this;
    }

    /**
     * Sets a customizer for the AWS CRT async HTTP client builder.
     *
     * <p>The customizer is called after Alternator-optimized defaults are applied, allowing users
     * to override specific settings. This is mutually exclusive with {@link
     * #withNettyHttpClientCustomizer} and {@link #httpClient(SdkAsyncHttpClient)}.
     *
     * @param customizer a consumer that customizes the CRT async HTTP client builder
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbAsyncClientBuilder withCrtAsyncHttpClientCustomizer(
        Consumer<AwsCrtAsyncHttpClient.Builder> customizer) {
      this.crtAsyncCustomizer = customizer;
      return this;
    }

    /**
     * Explicitly selects which HTTP client implementation to use.
     *
     * <p>By default, the builder auto-detects the HTTP client from the classpath (Netty &gt; CRT).
     * Use this method to force a specific implementation. If the requested implementation is not on
     * the classpath, the builder will throw {@link IllegalStateException} at build time.
     *
     * <p>Only {@link HttpClientType#NETTY}, {@link HttpClientType#CRT}, and {@link
     * HttpClientType#AUTO} are valid for async clients. Setting {@link HttpClientType#APACHE} will
     * throw {@link IllegalStateException} since Apache is sync-only.
     *
     * <p>This is mutually exclusive with {@link #httpClient(SdkAsyncHttpClient)} and {@link
     * #httpClientBuilder}. It can be combined with a matching customizer (e.g., {@link
     * HttpClientType#NETTY} with {@link #withNettyHttpClientCustomizer}), but not with a mismatched
     * one.
     *
     * @param httpClientType the HTTP client type to use
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbAsyncClientBuilder withHttpClientType(HttpClientType httpClientType) {
      this.httpClientType = Objects.requireNonNull(httpClientType, "httpClientType");
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder region(Region region) {
      this.region = region;
      delegate.region(region);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder credentialsProvider(
        AwsCredentialsProvider credentialsProvider) {
      if (credentialsProvider != null) {
        this.credentialsProviderSet = true;
        configBuilder.authenticationEnabled(true);
      }
      delegate.credentialsProvider(credentialsProvider);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder endpointOverride(URI endpointOverride) {
      this.seedUri = endpointOverride;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder httpClient(SdkAsyncHttpClient httpClient) {
      this.httpClientSet = true;
      this.customHttpClient = httpClient;
      this.customHttpClientBuilder = null;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder httpClientBuilder(
        SdkAsyncHttpClient.Builder httpClientBuilder) {
      this.httpClientSet = true;
      this.customHttpClient = null;
      this.customHttpClientBuilder = httpClientBuilder;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClientOverrideConfiguration overrideConfiguration() {
      return delegate.overrideConfiguration();
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder overrideConfiguration(
        ClientOverrideConfiguration overrideConfiguration) {
      delegate.overrideConfiguration(overrideConfiguration);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder overrideConfiguration(
        Consumer<ClientOverrideConfiguration.Builder> builderConsumer) {
      delegate.overrideConfiguration(builderConsumer);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder asyncConfiguration(
        ClientAsyncConfiguration asyncConfiguration) {
      delegate.asyncConfiguration(asyncConfiguration);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder asyncConfiguration(
        Consumer<ClientAsyncConfiguration.Builder> builderConsumer) {
      delegate.asyncConfiguration(builderConsumer);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder endpointProvider(
        DynamoDbEndpointProvider endpointProvider) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support custom endpoint providers. "
              + "Use endpointOverride(URI) to specify the seed endpoint instead.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder endpointDiscoveryEnabled(
        boolean endpointDiscoveryEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder enableEndpointDiscovery() {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder fipsEnabled(Boolean fipsEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support FIPS mode (AWS-specific feature).");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder dualstackEnabled(Boolean dualstackEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support dual-stack networking "
              + "(AWS-specific feature).");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder accountIdEndpointMode(
        AccountIdEndpointMode accountIdEndpointMode) {
      delegate.accountIdEndpointMode(accountIdEndpointMode);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public DynamoDbAsyncClient build() {
      return buildWithAlternatorAPI().getClient();
    }

    /**
     * Builds and returns an async DynamoDB client wrapper with Alternator load balancing and access
     * to Alternator-specific APIs.
     *
     * @return an {@link AlternatorDynamoDbAsyncClientWrapper} instance
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called, or if
     *     conflicting HTTP client options are set
     */
    public AlternatorDynamoDbAsyncClientWrapper buildWithAlternatorAPI() {
      if (seedUri == null) {
        throw new IllegalStateException(
            "endpointOverride must be set when using AlternatorDynamoDbAsyncClientBuilder. "
                + "Call endpointOverride(URI) with the seed Alternator node URI.");
      }

      AsyncClientDetector.AsyncClientType asyncType = validateAndDetectAsyncClientType();

      if (!credentialsProviderSet) {
        configBuilder.authenticationEnabled(false);
        delegate.credentialsProvider(AnonymousCredentialsProvider.create());
      }

      applyEndpointToConfig();

      if (disableCertificateChecks) {
        configBuilder.withTlsConfig(TlsConfig.trustAll());
      }

      AlternatorConfig alternatorConfig = configBuilder.build();

      ClientOverrideConfiguration.Builder compressionOverrideBuilder =
          delegate.overrideConfiguration() != null
              ? delegate.overrideConfiguration().toBuilder()
              : ClientOverrideConfiguration.builder();
      if (alternatorConfig.isResponseCompressionEnabled()) {
        compressionOverrideBuilder.addExecutionInterceptor(
            new ResponseCompressionInterceptor(
                alternatorConfig.getResponseCompressionAlgorithms()));
      }
      if (alternatorConfig.getCompressionAlgorithm().isEnabled()) {
        compressionOverrideBuilder.addExecutionInterceptor(
            new GzipRequestInterceptor(alternatorConfig.getMinCompressionSizeBytes()));
      }
      delegate.overrideConfiguration(compressionOverrideBuilder.build());

      SdkHttpClient pollingClient = null;
      AlternatorLiveNodes liveNodes = null;
      AlternatorClientResources resources = null;
      RoutingSdkAsyncHttpClientBuilder routingClientBuilder = null;
      DynamoDbAsyncClient client = null;
      try {
        TlsConfig tlsConfig = alternatorConfig.getTlsConfig();
        SdkAsyncHttpClient.Builder mainClientBuilder;
        boolean closeMainClient;
        if (!httpClientSet) {
          mainClientBuilder =
              new FactorySdkAsyncHttpClientBuilder(
                  () -> createMainAsyncClient(asyncType, alternatorConfig, tlsConfig));
          closeMainClient = true;
        } else if (customHttpClient != null) {
          mainClientBuilder = new FixedSdkAsyncHttpClientBuilder(customHttpClient);
          closeMainClient = false;
        } else {
          mainClientBuilder = customHttpClientBuilder;
          closeMainClient = true;
        }

        SyncClientDetector.SyncClientType syncType = SyncClientDetector.detect();
        pollingClient =
            createPollingClient(
                syncType,
                tlsConfig,
                alternatorConfig.getNodeHealthConfig().getHealthProbeConcurrency() + 1);
        pollingClient = configurePollingSyncClient(pollingClient, alternatorConfig);
        liveNodes = createLiveNodes(alternatorConfig, pollingClient);

        ClientOverrideConfiguration.Builder overrideBuilder =
            delegate.overrideConfiguration() != null
                ? delegate.overrideConfiguration().toBuilder()
                : ClientOverrideConfiguration.builder();

        KeyRouteAffinityConfig keyAffinityConfig = alternatorConfig.getKeyRouteAffinityConfig();
        AffinityQueryPlanInterceptor affinityInterceptor = null;
        BasicQueryPlanInterceptor queryPlanInterceptor;
        if (keyAffinityConfig != null
            && keyAffinityConfig.getType() != null
            && keyAffinityConfig.getType() != KeyRouteAffinity.NONE) {
          affinityInterceptor = new AffinityQueryPlanInterceptor(keyAffinityConfig, liveNodes);
          queryPlanInterceptor = affinityInterceptor;
        } else {
          queryPlanInterceptor = new BasicQueryPlanInterceptor(liveNodes);
        }
        resources = new AlternatorClientResources(liveNodes, affinityInterceptor, pollingClient);
        overrideBuilder.addExecutionInterceptor(queryPlanInterceptor);
        delegate.overrideConfiguration(overrideBuilder.build());
        routingClientBuilder =
            new RoutingSdkAsyncHttpClientBuilder(
                mainClientBuilder,
                alternatorConfig,
                queryPlanInterceptor,
                resources,
                closeMainClient);
        delegate.httpClientBuilder(routingClientBuilder);

        delegate.endpointOverride(seedUri);
        if (region == null) {
          delegate.region(Region.of("fake-aws-region"));
        }

        client = delegate.build();
        AlternatorDynamoDbAsyncClientWrapper wrapper =
            new AlternatorDynamoDbAsyncClientWrapper(
                client, liveNodes, alternatorConfig, affinityInterceptor, pollingClient, resources);
        liveNodes.start();
        return wrapper;
      } catch (RuntimeException | Error e) {
        closeAfterBuildFailure(
            e, client, routingClientBuilder, resources, liveNodes, pollingClient);
        throw e;
      }
    }

    /**
     * Validates mutually exclusive HTTP client options and detects the async client type.
     *
     * @return the resolved client type, or {@code null} if a custom HTTP client was provided via
     *     {@link #httpClient} / {@link #httpClientBuilder}
     */
    AsyncClientDetector.AsyncClientType validateAndDetectAsyncClientType() {
      boolean hasCustomizer = nettyCustomizer != null || crtAsyncCustomizer != null;

      if (httpClientSet && hasCustomizer) {
        throw new IllegalStateException(
            "Cannot use httpClient()/httpClientBuilder() together with "
                + "withNettyHttpClientCustomizer()/withCrtAsyncHttpClientCustomizer(). "
                + "Use one approach or the other.");
      }
      if (nettyCustomizer != null && crtAsyncCustomizer != null) {
        throw new IllegalStateException(
            "Cannot set both withNettyHttpClientCustomizer() and "
                + "withCrtAsyncHttpClientCustomizer(). Choose one HTTP client implementation.");
      }
      if (httpClientType != null && httpClientSet) {
        throw new IllegalStateException(
            "Cannot use httpClient()/httpClientBuilder() together with withHttpClientType(). "
                + "Use one approach or the other.");
      }

      if (httpClientSet) {
        return null;
      }

      HttpClientType effectiveType = httpClientType != null ? httpClientType : HttpClientType.AUTO;

      switch (effectiveType) {
        case NETTY:
          if (crtAsyncCustomizer != null) {
            throw new IllegalStateException(
                "HttpClientType.NETTY was set, but withCrtAsyncHttpClientCustomizer() was also "
                    + "called. Use withNettyHttpClientCustomizer() or change HttpClientType to CRT.");
          }
          AsyncClientDetector.requireAvailableHttpClient(AsyncClientDetector.AsyncClientType.NETTY);
          return AsyncClientDetector.AsyncClientType.NETTY;
        case CRT:
          if (nettyCustomizer != null) {
            throw new IllegalStateException(
                "HttpClientType.CRT was set, but withNettyHttpClientCustomizer() was also called. "
                    + "Use withCrtAsyncHttpClientCustomizer() or change HttpClientType to NETTY.");
          }
          AsyncClientDetector.requireAvailableHttpClient(AsyncClientDetector.AsyncClientType.CRT);
          return AsyncClientDetector.AsyncClientType.CRT;
        case APACHE:
          throw new IllegalStateException(
              "HttpClientType.APACHE does not support asynchronous clients. "
                  + "Use HttpClientType.NETTY, HttpClientType.CRT, or HttpClientType.AUTO.");
        case AUTO:
          if (nettyCustomizer != null) {
            AsyncClientDetector.requireAvailableHttpClient(
                AsyncClientDetector.AsyncClientType.NETTY);
            return AsyncClientDetector.AsyncClientType.NETTY;
          }
          if (crtAsyncCustomizer != null) {
            AsyncClientDetector.requireAvailableHttpClient(AsyncClientDetector.AsyncClientType.CRT);
            return AsyncClientDetector.AsyncClientType.CRT;
          }
          return AsyncClientDetector.detect();
        default:
          throw new IllegalStateException("Unknown HttpClientType: " + effectiveType);
      }
    }

    SdkAsyncHttpClient createMainAsyncClient(
        AsyncClientDetector.AsyncClientType asyncType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      switch (asyncType) {
        case NETTY:
          return NettyAsyncClientFactory.create(nettyCustomizer, config, tlsConfig);
        case CRT:
          return CrtAsyncClientFactory.create(crtAsyncCustomizer, config, tlsConfig);
        default:
          throw new IllegalStateException("Unknown async client type: " + asyncType);
      }
    }

    SdkHttpClient createPollingClient(
        SyncClientDetector.SyncClientType clientType, TlsConfig tlsConfig, int maxConnections) {
      return SyncClientDetector.createPollingClient(clientType, tlsConfig, maxConnections);
    }

    AlternatorLiveNodes createLiveNodes(
        AlternatorConfig alternatorConfig, SdkHttpClient pollingClient) {
      return new AlternatorLiveNodes(alternatorConfig, pollingClient);
    }

    private SdkAsyncHttpClient configureMainAsyncClient(
        SdkAsyncHttpClient mainClient,
        AlternatorConfig alternatorConfig,
        BasicQueryPlanInterceptor queryPlanInterceptor,
        AlternatorClientResources resources,
        boolean closeMainClient) {
      SdkAsyncHttpClient configuredClient = mainClient;
      if (userAgentTransformer != null) {
        configuredClient = new UserAgentSdkAsyncHttpClient(configuredClient, userAgentTransformer);
      }
      if (alternatorConfig.isOptimizeHeaders()) {
        configuredClient =
            new HeadersFilteringSdkAsyncHttpClient(
                configuredClient, alternatorConfig.getHeadersWhitelist());
      }
      configuredClient =
          new AttemptRoutingSdkAsyncHttpClient(configuredClient, queryPlanInterceptor);
      return new LifecycleSdkAsyncHttpClient(configuredClient, resources, closeMainClient);
    }

    private final class RoutingSdkAsyncHttpClientBuilder
        implements SdkAsyncHttpClient.Builder<RoutingSdkAsyncHttpClientBuilder> {
      private final SdkAsyncHttpClient.Builder delegateBuilder;
      private final AlternatorConfig alternatorConfig;
      private final BasicQueryPlanInterceptor queryPlanInterceptor;
      private final AlternatorClientResources resources;
      private final boolean closeDelegate;
      private SdkAsyncHttpClient builtClient;

      private RoutingSdkAsyncHttpClientBuilder(
          SdkAsyncHttpClient.Builder delegateBuilder,
          AlternatorConfig alternatorConfig,
          BasicQueryPlanInterceptor queryPlanInterceptor,
          AlternatorClientResources resources,
          boolean closeDelegate) {
        this.delegateBuilder = delegateBuilder;
        this.alternatorConfig = alternatorConfig;
        this.queryPlanInterceptor = queryPlanInterceptor;
        this.resources = resources;
        this.closeDelegate = closeDelegate;
      }

      @Override
      public synchronized SdkAsyncHttpClient build() {
        return configure(delegateBuilder.build());
      }

      @Override
      public synchronized SdkAsyncHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
        return configure(delegateBuilder.buildWithDefaults(serviceDefaults));
      }

      private SdkAsyncHttpClient configure(SdkAsyncHttpClient client) {
        if (builtClient != null) {
          if (closeDelegate) {
            client.close();
          }
          throw new IllegalStateException("HTTP client builder may only build one client");
        }
        try {
          builtClient =
              configureMainAsyncClient(
                  client, alternatorConfig, queryPlanInterceptor, resources, closeDelegate);
          return builtClient;
        } catch (RuntimeException | Error e) {
          if (closeDelegate) {
            try {
              client.close();
            } catch (RuntimeException closeFailure) {
              e.addSuppressed(closeFailure);
            }
          }
          throw e;
        }
      }

      private synchronized void closeBuiltClient() {
        if (builtClient != null) {
          builtClient.close();
        }
      }
    }

    private static final class FixedSdkAsyncHttpClientBuilder
        implements SdkAsyncHttpClient.Builder<FixedSdkAsyncHttpClientBuilder> {
      private final SdkAsyncHttpClient client;

      private FixedSdkAsyncHttpClientBuilder(SdkAsyncHttpClient client) {
        this.client = client;
      }

      @Override
      public SdkAsyncHttpClient build() {
        return client;
      }

      @Override
      public SdkAsyncHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
        return client;
      }
    }

    private static final class FactorySdkAsyncHttpClientBuilder
        implements SdkAsyncHttpClient.Builder<FactorySdkAsyncHttpClientBuilder> {
      private final Supplier<SdkAsyncHttpClient> clientFactory;

      private FactorySdkAsyncHttpClientBuilder(Supplier<SdkAsyncHttpClient> clientFactory) {
        this.clientFactory = clientFactory;
      }

      @Override
      public SdkAsyncHttpClient build() {
        return clientFactory.get();
      }

      @Override
      public SdkAsyncHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
        return clientFactory.get();
      }
    }

    private void closeAfterBuildFailure(
        Throwable failure,
        DynamoDbAsyncClient client,
        RoutingSdkAsyncHttpClientBuilder routingClientBuilder,
        AlternatorClientResources resources,
        AlternatorLiveNodes liveNodes,
        SdkHttpClient pollingClient) {
      if (client != null) {
        runCleanupAfterBuildFailure(failure, client::close);
      }
      if (routingClientBuilder != null) {
        runCleanupAfterBuildFailure(failure, routingClientBuilder::closeBuiltClient);
      }
      if (resources != null) {
        runCleanupAfterBuildFailure(failure, resources::close);
      } else {
        if (liveNodes != null) {
          runCleanupAfterBuildFailure(failure, liveNodes::shutdownAndWait);
        }
        if (pollingClient != null) {
          runCleanupAfterBuildFailure(failure, pollingClient::close);
        }
      }
    }

    private void runCleanupAfterBuildFailure(Throwable failure, Runnable cleanup) {
      try {
        cleanup.run();
      } catch (RuntimeException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }

    private SdkHttpClient configurePollingSyncClient(
        SdkHttpClient pollingClient, AlternatorConfig alternatorConfig) {
      if (alternatorConfig.isUserAgentEnabled()) {
        return new UserAgentSdkHttpClient(pollingClient, userAgentTransformer);
      }
      return pollingClient;
    }

    private void applyEndpointToConfig() {
      if (seedHostsSet) {
        configBuilder.withScheme(seedUri.getScheme());
        configBuilder.withPort(seedUri.getPort());
        return;
      }
      configBuilder.withSeedNode(seedUri);
    }
  }
}
