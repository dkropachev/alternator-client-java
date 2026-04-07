package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.AsyncClientDetector;
import com.scylladb.alternator.internal.CloseAwareClientProxy;
import com.scylladb.alternator.internal.CrtAsyncClientFactory;
import com.scylladb.alternator.internal.NettyAsyncClientFactory;
import com.scylladb.alternator.internal.SyncClientDetector;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
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
 * URI nextNode = client.nextAsURI();
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
    private boolean disableCertificateChecks = false;
    private boolean httpClientSet = false;
    private boolean credentialsProviderSet = false;
    private Consumer<NettyNioAsyncHttpClient.Builder> nettyCustomizer;
    private Consumer<AwsCrtAsyncHttpClient.Builder> crtAsyncCustomizer;
    private HttpClientType httpClientType;

    private AlternatorDynamoDbAsyncClientBuilder() {
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
        configBuilder.withRoutingScope(config.getRoutingScope());
        configBuilder.withCompressionAlgorithm(config.getCompressionAlgorithm());
        configBuilder.withMinCompressionSizeBytes(config.getMinCompressionSizeBytes());
        configBuilder.withOptimizeHeaders(config.isOptimizeHeaders());
        configBuilder.withHeadersWhitelist(config.getHeadersWhitelist());
        configBuilder.withTlsConfig(config.getTlsConfig());
        configBuilder.withTlsSessionCacheConfig(config.getTlsSessionCacheConfig());
        configBuilder.withKeyRouteAffinity(config.getKeyRouteAffinityConfig());
        configBuilder.withActiveRefreshIntervalMs(config.getActiveRefreshIntervalMs());
        configBuilder.withIdleRefreshIntervalMs(config.getIdleRefreshIntervalMs());
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
      delegate.httpClient(httpClient);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder httpClientBuilder(
        SdkAsyncHttpClient.Builder httpClientBuilder) {
      this.httpClientSet = true;
      delegate.httpClientBuilder(httpClientBuilder);
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
      AlternatorDynamoDbAsyncClientWrapper wrapper = buildWithAlternatorAPI();
      return CloseAwareClientProxy.wrap(
          DynamoDbAsyncClient.class, wrapper.getClient(), wrapper::close);
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

      configBuilder.withSeedNode(seedUri);

      if (disableCertificateChecks) {
        configBuilder.withTlsConfig(TlsConfig.trustAll());
      }

      AlternatorConfig alternatorConfig = configBuilder.build();

      if (alternatorConfig.getCompressionAlgorithm().isEnabled()) {
        ClientOverrideConfiguration.Builder overrideBuilder =
            delegate.overrideConfiguration() != null
                ? delegate.overrideConfiguration().toBuilder()
                : ClientOverrideConfiguration.builder();
        overrideBuilder.addExecutionInterceptor(
            new GzipRequestInterceptor(alternatorConfig.getMinCompressionSizeBytes()));
        delegate.overrideConfiguration(overrideBuilder.build());
      }

      TlsConfig tlsConfig = alternatorConfig.getTlsConfig();
      boolean optimizeHeaders = alternatorConfig.isOptimizeHeaders();

      if (!httpClientSet) {
        SdkAsyncHttpClient mainClient =
            createMainAsyncClient(asyncType, alternatorConfig, tlsConfig);

        if (optimizeHeaders) {
          delegate.httpClient(
              new HeadersFilteringSdkAsyncHttpClient(
                  mainClient, alternatorConfig.getHeadersWhitelist()));
        } else {
          delegate.httpClient(mainClient);
        }
      }

      SyncClientDetector.SyncClientType syncType = SyncClientDetector.detect();
      SdkHttpClient pollingClient = SyncClientDetector.createPollingClient(syncType, tlsConfig);

      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(alternatorConfig, pollingClient);
      liveNodes.start();

      ClientOverrideConfiguration.Builder overrideBuilder =
          delegate.overrideConfiguration() != null
              ? delegate.overrideConfiguration().toBuilder()
              : ClientOverrideConfiguration.builder();

      KeyRouteAffinityConfig keyAffinityConfig = alternatorConfig.getKeyRouteAffinityConfig();
      if (keyAffinityConfig != null
          && keyAffinityConfig.getType() != null
          && keyAffinityConfig.getType() != KeyRouteAffinity.NONE) {
        overrideBuilder.addExecutionInterceptor(
            new AffinityQueryPlanInterceptor(keyAffinityConfig, liveNodes));
      } else {
        overrideBuilder.addExecutionInterceptor(new BasicQueryPlanInterceptor(liveNodes));
      }
      delegate.overrideConfiguration(overrideBuilder.build());

      delegate.endpointOverride(seedUri);

      if (region == null) {
        delegate.region(Region.of("fake-aws-region"));
      }

      DynamoDbAsyncClient client = delegate.build();
      return new AlternatorDynamoDbAsyncClientWrapper(
          client, liveNodes, alternatorConfig, pollingClient);
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

    private SdkAsyncHttpClient createMainAsyncClient(
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
  }
}
