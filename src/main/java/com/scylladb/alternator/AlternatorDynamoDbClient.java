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
import com.scylladb.alternator.internal.ApacheSyncClientFactory;
import com.scylladb.alternator.internal.CrtSyncClientFactory;
import com.scylladb.alternator.internal.SyncClientDetector;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.AttemptRoutingSdkHttpClient;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.endpoints.AccountIdEndpointMode;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory class for creating DynamoDB clients with Alternator load balancing support.
 *
 * <p>This class provides a builder that simplifies the construction of a DynamoDB client (AWS SDK
 * v2) that automatically distributes requests across all nodes in an Alternator cluster. It
 * provides a fluent API compatible with {@link DynamoDbClient#builder()} while automatically
 * integrating query plan interceptors for client-side load balancing.
 *
 * <p>The builder implements {@link DynamoDbClientBuilder}, ensuring compatibility with standard AWS
 * SDK v2 patterns while adding Alternator-specific configuration via {@link
 * AlternatorDynamoDbClientBuilder#withRoutingScope}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Standard usage - returns DynamoDbClient
 * DynamoDbClient client = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // With Alternator-specific APIs - returns AlternatorDynamoDbClientWrapper
 * AlternatorDynamoDbClientWrapper client = AlternatorDynamoDbClient.builder()
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
public class AlternatorDynamoDbClient {

  /**
   * Creates a new builder instance using the standard builder pattern, similar to {@link
   * DynamoDbClient#builder()}.
   *
   * @return a new {@link AlternatorDynamoDbClientBuilder} configured for Alternator load balancing
   */
  public static AlternatorDynamoDbClientBuilder builder() {
    return new AlternatorDynamoDbClientBuilder();
  }

  /**
   * Builder implementation for constructing DynamoDB clients with Alternator load balancing.
   *
   * <p>This builder implements {@link DynamoDbClientBuilder} and delegates most configuration to
   * the standard AWS SDK {@link DynamoDbClient} builder while automatically integrating query plan
   * interceptors for node discovery and load balancing.
   *
   * <p>The builder tracks the seed URI (via {@link #endpointOverride(URI)}) and optional
   * datacenter/rack configuration, then creates the appropriate query plan interceptor during the
   * {@link #build()} phase.
   *
   * <p>Note: Some AWS-specific features are not supported by Alternator and will throw {@link
   * UnsupportedOperationException}, including endpoint discovery, FIPS mode, and dual-stack
   * networking.
   */
  public static class AlternatorDynamoDbClientBuilder implements DynamoDbClientBuilder {
    private final DynamoDbClientBuilder delegate;
    private final AlternatorConfig.Builder configBuilder;
    private URI seedUri;
    private Region region;
    private boolean seedHostsSet = false;
    private boolean disableCertificateChecks = false;
    private boolean httpClientSet = false;
    private boolean credentialsProviderSet = false;
    private SdkHttpClient customHttpClient;
    private SdkHttpClient.Builder customHttpClientBuilder;
    private Consumer<ApacheHttpClient.Builder> apacheCustomizer;
    private Consumer<AwsCrtHttpClient.Builder> crtCustomizer;
    private HttpClientType httpClientType;
    private UnaryOperator<String> userAgentTransformer = AlternatorUserAgent.defaultUserAgent();

    private AlternatorDynamoDbClientBuilder() {
      this.delegate = DynamoDbClient.builder();
      this.configBuilder = AlternatorConfig.builder();
    }

    /**
     * Sets the routing scope for node targeting with fallback support.
     *
     * <p>The routing scope defines which nodes should be used for load balancing and provides
     * hierarchical fallback capabilities. Common usage patterns:
     *
     * <pre>{@code
     * // Target rack with fallback to datacenter and cluster
     * builder.withRoutingScope(RackScope.of("dc1", "rack1",
     *     DatacenterScope.of("dc1",
     *         ClusterScope.create())));
     *
     * // Target datacenter with fallback to cluster
     * builder.withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()));
     *
     * // Strict datacenter targeting (no fallback)
     * builder.withRoutingScope(DatacenterScope.of("dc1", null));
     * }</pre>
     *
     * @param routingScope the routing scope, or {@code null} to use all nodes
     * @return this builder instance
     * @since 2.0.0
     */
    public AlternatorDynamoDbClientBuilder withRoutingScope(RoutingScope routingScope) {
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
    public AlternatorDynamoDbClientBuilder withSeedHosts(Collection<String> hosts) {
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
    public AlternatorDynamoDbClientBuilder withSeedHost(String host) {
      if (host != null) {
        configBuilder.withSeedHost(host);
        seedHostsSet = true;
      }
      return this;
    }

    /**
     * Sets the request compression algorithm.
     *
     * <p>When a compression algorithm other than {@link RequestCompressionAlgorithm#NONE} is
     * specified, request bodies exceeding the minimum size threshold will be compressed.
     *
     * @param algorithm the compression algorithm to use, or null to disable compression
     * @return this builder instance
     */
    public AlternatorDynamoDbClientBuilder withCompressionAlgorithm(
        RequestCompressionAlgorithm algorithm) {
      configBuilder.withCompressionAlgorithm(algorithm);
      return this;
    }

    /**
     * Sets the minimum request body size (in bytes) that triggers compression.
     *
     * <p>Requests smaller than this threshold will not be compressed.
     *
     * @param minCompressionSizeBytes minimum request size in bytes, must be non-negative
     * @return this builder instance
     * @throws IllegalArgumentException if minCompressionSizeBytes is negative
     */
    public AlternatorDynamoDbClientBuilder withMinCompressionSizeBytes(
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
    public AlternatorDynamoDbClientBuilder withResponseCompression(
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
    public AlternatorDynamoDbClientBuilder withResponseCompression(
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
    public AlternatorDynamoDbClientBuilder withResponseCompressionDisabled() {
      configBuilder.withResponseCompressionDisabled();
      return this;
    }

    /**
     * Enables or disables HTTP header optimization.
     *
     * <p>When enabled, outgoing requests will have their HTTP headers filtered to include only
     * those in the configured whitelist, reducing network traffic overhead.
     *
     * @param optimizeHeaders true to enable header filtering, false to disable
     * @return this builder instance
     */
    public AlternatorDynamoDbClientBuilder withOptimizeHeaders(boolean optimizeHeaders) {
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
    public AlternatorDynamoDbClientBuilder withHeadersWhitelist(Collection<String> headers) {
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
    public AlternatorDynamoDbClientBuilder withUserAgent(String userAgent) {
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
    public AlternatorDynamoDbClientBuilder withUserAgent(
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
    public AlternatorDynamoDbClientBuilder withoutUserAgent() {
      this.userAgentTransformer = AlternatorUserAgent.disable();
      configBuilder.withUserAgentEnabled(false);
      return this;
    }

    /**
     * Sets the TLS session cache configuration for quick TLS renegotiation.
     *
     * <p>TLS session tickets (RFC 5077) allow clients to resume TLS sessions without performing a
     * full handshake. This significantly reduces latency when reconnecting to Alternator nodes.
     *
     * <p>Default: {@link TlsSessionCacheConfig#getDefault()} (enabled with 1024 sessions, 24h
     * timeout)
     *
     * @param tlsSessionCacheConfig the TLS session cache configuration, or null to use default
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use {@link #withTlsConfig(TlsConfig)} instead
     */
    @Deprecated
    public AlternatorDynamoDbClientBuilder withTlsSessionCacheConfig(
        TlsSessionCacheConfig tlsSessionCacheConfig) {
      configBuilder.withTlsSessionCacheConfig(tlsSessionCacheConfig);
      return this;
    }

    /**
     * Sets the TLS configuration for secure connections.
     *
     * <p>The TLS configuration controls certificate validation, custom CA certificates, hostname
     * verification, and session caching settings.
     *
     * @param tlsConfig the TLS configuration, or null to use default (trust-all)
     * @return this builder instance
     * @since 1.0.9
     */
    public AlternatorDynamoDbClientBuilder withTlsConfig(TlsConfig tlsConfig) {
      configBuilder.withTlsConfig(tlsConfig);
      return this;
    }

    /**
     * Sets the key route affinity configuration.
     *
     * @param keyRouteAffinityConfig the key route affinity configuration
     * @return this builder instance
     * @since 2.0.0
     */
    public AlternatorDynamoDbClientBuilder withKeyRouteAffinity(
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
    public AlternatorDynamoDbClientBuilder withKeyRouteAffinity(KeyRouteAffinity type) {
      configBuilder.withKeyRouteAffinity(type);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when there are active requests.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_ACTIVE_REFRESH_INTERVAL_MS} (1000ms / 1 second)
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbClientBuilder withActiveRefreshIntervalMs(long intervalMs) {
      configBuilder.withActiveRefreshIntervalMs(intervalMs);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when the client is idle.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_IDLE_REFRESH_INTERVAL_MS} (60000ms / 1 minute)
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbClientBuilder withIdleRefreshIntervalMs(long intervalMs) {
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
    public AlternatorDynamoDbClientBuilder withNodeHealthConfig(NodeHealthConfig nodeHealthConfig) {
      configBuilder.withNodeHealthConfig(nodeHealthConfig);
      return this;
    }

    /**
     * Disables node health tracking.
     *
     * @return this builder instance
     * @since 2.0.6
     */
    public AlternatorDynamoDbClientBuilder withNodeHealthDisabled() {
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
    public AlternatorDynamoDbClientBuilder withMaxConnections(int maxConnections) {
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
    public AlternatorDynamoDbClientBuilder withConnectionMaxIdleTimeMs(
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
    public AlternatorDynamoDbClientBuilder withConnectionTimeToLiveMs(long connectionTimeToLiveMs) {
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
    public AlternatorDynamoDbClientBuilder withConnectionAcquisitionTimeoutMs(
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
    public AlternatorDynamoDbClientBuilder withConnectionTimeoutMs(long connectionTimeoutMs) {
      configBuilder.withConnectionTimeoutMs(connectionTimeoutMs);
      return this;
    }

    /**
     * Sets the complete Alternator configuration.
     *
     * @param config the Alternator configuration
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use individual builder methods instead (e.g., {@link #withRoutingScope}, {@link
     *     #withCompressionAlgorithm}, {@link #withTlsConfig}, etc.)
     */
    @Deprecated
    public AlternatorDynamoDbClientBuilder withAlternatorConfig(AlternatorConfig config) {
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
    public AlternatorDynamoDbClientBuilder withDisableCertificateChecks() {
      this.disableCertificateChecks = true;
      return this;
    }

    /**
     * Sets a customizer for the Apache HTTP client builder.
     *
     * <p>The customizer is called after Alternator-optimized defaults are applied, allowing users
     * to override specific settings. This is mutually exclusive with {@link
     * #withCrtHttpClientCustomizer} and {@link #httpClient(SdkHttpClient)}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .withApacheHttpClientCustomizer(builder -> builder
     *         .maxConnections(200)
     *         .connectionTimeout(Duration.ofSeconds(5)))
     *     .build();
     * }</pre>
     *
     * @param customizer a consumer that customizes the Apache HTTP client builder
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbClientBuilder withApacheHttpClientCustomizer(
        Consumer<ApacheHttpClient.Builder> customizer) {
      this.apacheCustomizer = customizer;
      return this;
    }

    /**
     * Sets a customizer for the AWS CRT HTTP client builder.
     *
     * <p>The customizer is called after Alternator-optimized defaults are applied, allowing users
     * to override specific settings. This is mutually exclusive with {@link
     * #withApacheHttpClientCustomizer} and {@link #httpClient(SdkHttpClient)}.
     *
     * @param customizer a consumer that customizes the CRT HTTP client builder
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbClientBuilder withCrtHttpClientCustomizer(
        Consumer<AwsCrtHttpClient.Builder> customizer) {
      this.crtCustomizer = customizer;
      return this;
    }

    /**
     * Explicitly selects which HTTP client implementation to use.
     *
     * <p>By default, the builder auto-detects the HTTP client from the classpath (Apache &gt; CRT).
     * Use this method to force a specific implementation. If the requested implementation is not on
     * the classpath, the builder will throw {@link IllegalStateException} at build time.
     *
     * <p>Only {@link HttpClientType#APACHE}, {@link HttpClientType#CRT}, and {@link
     * HttpClientType#AUTO} are valid for sync clients. Setting {@link HttpClientType#NETTY} will
     * throw {@link IllegalStateException} since Netty is async-only.
     *
     * <p>This is mutually exclusive with {@link #httpClient(SdkHttpClient)} and {@link
     * #httpClientBuilder}. It can be combined with a matching customizer (e.g., {@link
     * HttpClientType#APACHE} with {@link #withApacheHttpClientCustomizer}), but not with a
     * mismatched one.
     *
     * @param httpClientType the HTTP client type to use
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbClientBuilder withHttpClientType(HttpClientType httpClientType) {
      this.httpClientType = Objects.requireNonNull(httpClientType, "httpClientType");
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder region(Region region) {
      this.region = region;
      delegate.region(region);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder credentialsProvider(
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
    public AlternatorDynamoDbClientBuilder endpointOverride(URI endpointOverride) {
      this.seedUri = endpointOverride;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder httpClient(SdkHttpClient httpClient) {
      this.httpClientSet = true;
      this.customHttpClient = httpClient;
      this.customHttpClientBuilder = null;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder httpClientBuilder(
        SdkHttpClient.Builder httpClientBuilder) {
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
    public AlternatorDynamoDbClientBuilder overrideConfiguration(
        ClientOverrideConfiguration overrideConfiguration) {
      delegate.overrideConfiguration(overrideConfiguration);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder overrideConfiguration(
        Consumer<ClientOverrideConfiguration.Builder> builderConsumer) {
      delegate.overrideConfiguration(builderConsumer);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder endpointProvider(
        DynamoDbEndpointProvider endpointProvider) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support custom endpoint providers. "
              + "Use endpointOverride(URI) to specify the seed endpoint instead.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder endpointDiscoveryEnabled(
        boolean endpointDiscoveryEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder enableEndpointDiscovery() {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder fipsEnabled(Boolean fipsEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support FIPS mode (AWS-specific feature).");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder dualstackEnabled(Boolean dualstackEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support dual-stack networking "
              + "(AWS-specific feature).");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder accountIdEndpointMode(
        AccountIdEndpointMode accountIdEndpointMode) {
      delegate.accountIdEndpointMode(accountIdEndpointMode);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public DynamoDbClient build() {
      return buildWithAlternatorAPI().getClient();
    }

    /**
     * Builds and returns a DynamoDB client wrapper with Alternator load balancing and access to
     * Alternator-specific APIs.
     *
     * @return an {@link AlternatorDynamoDbClientWrapper} instance
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called, or if
     *     conflicting HTTP client options are set
     */
    public AlternatorDynamoDbClientWrapper buildWithAlternatorAPI() {
      if (seedUri == null) {
        throw new IllegalStateException(
            "endpointOverride must be set when using AlternatorDynamoDbClientBuilder. "
                + "Call endpointOverride(URI) with the seed Alternator node URI.");
      }

      SyncClientDetector.SyncClientType clientType = validateAndDetectSyncClientType();

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

      TlsConfig tlsConfig = alternatorConfig.getTlsConfig();
      SdkHttpClient mainClient = null;
      SdkHttpClient pollingClient;
      if (!httpClientSet) {
        mainClient = createMainSyncClient(clientType, alternatorConfig, tlsConfig);
        pollingClient =
            SyncClientDetector.createPollingClient(
                clientType,
                tlsConfig,
                alternatorConfig.getNodeHealthConfig().getHealthProbeConcurrency() + 1);
      } else if (customHttpClient != null) {
        mainClient = customHttpClient;
        SyncClientDetector.SyncClientType pollingType = SyncClientDetector.detect();
        pollingClient =
            SyncClientDetector.createPollingClient(
                pollingType,
                tlsConfig,
                alternatorConfig.getNodeHealthConfig().getHealthProbeConcurrency() + 1);
      } else {
        SyncClientDetector.SyncClientType pollingType = SyncClientDetector.detect();
        pollingClient =
            SyncClientDetector.createPollingClient(
                pollingType,
                tlsConfig,
                alternatorConfig.getNodeHealthConfig().getHealthProbeConcurrency() + 1);
      }

      pollingClient = configurePollingSyncClient(pollingClient, alternatorConfig);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(alternatorConfig, pollingClient);
      liveNodes.start();

      ClientOverrideConfiguration.Builder overrideBuilder =
          delegate.overrideConfiguration() != null
              ? delegate.overrideConfiguration().toBuilder()
              : ClientOverrideConfiguration.builder();

      AffinityQueryPlanInterceptor affinityInterceptor = null;
      BasicQueryPlanInterceptor queryPlanInterceptor;
      KeyRouteAffinityConfig keyAffinityConfig = alternatorConfig.getKeyRouteAffinityConfig();
      if (keyAffinityConfig != null
          && keyAffinityConfig.getType() != null
          && keyAffinityConfig.getType() != KeyRouteAffinity.NONE) {
        affinityInterceptor = new AffinityQueryPlanInterceptor(keyAffinityConfig, liveNodes);
        queryPlanInterceptor = affinityInterceptor;
      } else {
        queryPlanInterceptor = new BasicQueryPlanInterceptor(liveNodes);
      }
      overrideBuilder.addExecutionInterceptor(queryPlanInterceptor);
      delegate.overrideConfiguration(overrideBuilder.build());
      if (customHttpClientBuilder != null) {
        delegate.httpClientBuilder(
            new RoutingSdkHttpClientBuilder(
                customHttpClientBuilder, alternatorConfig, queryPlanInterceptor));
      } else {
        delegate.httpClient(
            configureMainSyncClient(mainClient, alternatorConfig, queryPlanInterceptor));
      }

      delegate.endpointOverride(seedUri);

      if (region == null) {
        delegate.region(Region.of("fake-aws-region"));
      }

      DynamoDbClient client = delegate.build();
      return new AlternatorDynamoDbClientWrapper(
          client, liveNodes, alternatorConfig, affinityInterceptor, pollingClient);
    }

    /**
     * Validates mutually exclusive HTTP client options and detects the sync client type.
     *
     * @return the resolved client type, or {@code null} if a custom HTTP client was provided via
     *     {@link #httpClient} / {@link #httpClientBuilder}
     */
    SyncClientDetector.SyncClientType validateAndDetectSyncClientType() {
      boolean hasCustomizer = apacheCustomizer != null || crtCustomizer != null;

      if (httpClientSet && hasCustomizer) {
        throw new IllegalStateException(
            "Cannot use httpClient()/httpClientBuilder() together with "
                + "withApacheHttpClientCustomizer()/withCrtHttpClientCustomizer(). "
                + "Use one approach or the other.");
      }
      if (apacheCustomizer != null && crtCustomizer != null) {
        throw new IllegalStateException(
            "Cannot set both withApacheHttpClientCustomizer() and "
                + "withCrtHttpClientCustomizer(). Choose one HTTP client implementation.");
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
        case APACHE:
          if (crtCustomizer != null) {
            throw new IllegalStateException(
                "HttpClientType.APACHE was set, but withCrtHttpClientCustomizer() was also called. "
                    + "Use withApacheHttpClientCustomizer() or change HttpClientType to CRT.");
          }
          SyncClientDetector.requireAvailableHttpClient(SyncClientDetector.SyncClientType.APACHE);
          return SyncClientDetector.SyncClientType.APACHE;
        case CRT:
          if (apacheCustomizer != null) {
            throw new IllegalStateException(
                "HttpClientType.CRT was set, but withApacheHttpClientCustomizer() was also called. "
                    + "Use withCrtHttpClientCustomizer() or change HttpClientType to APACHE.");
          }
          SyncClientDetector.requireAvailableHttpClient(SyncClientDetector.SyncClientType.CRT);
          return SyncClientDetector.SyncClientType.CRT;
        case NETTY:
          throw new IllegalStateException(
              "HttpClientType.NETTY does not support synchronous clients. "
                  + "Use HttpClientType.APACHE, HttpClientType.CRT, or HttpClientType.AUTO.");
        case AUTO:
          if (apacheCustomizer != null) {
            SyncClientDetector.requireAvailableHttpClient(SyncClientDetector.SyncClientType.APACHE);
            return SyncClientDetector.SyncClientType.APACHE;
          }
          if (crtCustomizer != null) {
            SyncClientDetector.requireAvailableHttpClient(SyncClientDetector.SyncClientType.CRT);
            return SyncClientDetector.SyncClientType.CRT;
          }
          return SyncClientDetector.detect();
        default:
          throw new IllegalStateException("Unknown HttpClientType: " + effectiveType);
      }
    }

    private SdkHttpClient createMainSyncClient(
        SyncClientDetector.SyncClientType clientType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      switch (clientType) {
        case APACHE:
          return ApacheSyncClientFactory.create(apacheCustomizer, config, tlsConfig);
        case CRT:
          return CrtSyncClientFactory.create(crtCustomizer, config, tlsConfig);
        default:
          throw new IllegalStateException("Unknown sync client type: " + clientType);
      }
    }

    private SdkHttpClient configureMainSyncClient(
        SdkHttpClient mainClient,
        AlternatorConfig alternatorConfig,
        BasicQueryPlanInterceptor queryPlanInterceptor) {
      SdkHttpClient configuredClient = mainClient;
      if (userAgentTransformer != null) {
        configuredClient = new UserAgentSdkHttpClient(configuredClient, userAgentTransformer);
      }
      if (alternatorConfig.isOptimizeHeaders()) {
        configuredClient =
            new HeadersFilteringSdkHttpClient(
                configuredClient, alternatorConfig.getHeadersWhitelist());
      }
      return new AttemptRoutingSdkHttpClient(configuredClient, queryPlanInterceptor);
    }

    private SdkHttpClient configurePollingSyncClient(
        SdkHttpClient pollingClient, AlternatorConfig alternatorConfig) {
      if (alternatorConfig.isUserAgentEnabled()) {
        return new UserAgentSdkHttpClient(pollingClient, userAgentTransformer);
      }
      return pollingClient;
    }

    private final class RoutingSdkHttpClientBuilder
        implements SdkHttpClient.Builder<RoutingSdkHttpClientBuilder> {
      private final SdkHttpClient.Builder delegateBuilder;
      private final AlternatorConfig alternatorConfig;
      private final BasicQueryPlanInterceptor queryPlanInterceptor;

      private RoutingSdkHttpClientBuilder(
          SdkHttpClient.Builder delegateBuilder,
          AlternatorConfig alternatorConfig,
          BasicQueryPlanInterceptor queryPlanInterceptor) {
        this.delegateBuilder = delegateBuilder;
        this.alternatorConfig = alternatorConfig;
        this.queryPlanInterceptor = queryPlanInterceptor;
      }

      @Override
      public SdkHttpClient build() {
        return configureMainSyncClient(
            delegateBuilder.build(), alternatorConfig, queryPlanInterceptor);
      }

      @Override
      public SdkHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
        return configureMainSyncClient(
            delegateBuilder.buildWithDefaults(serviceDefaults),
            alternatorConfig,
            queryPlanInterceptor);
      }
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
