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

import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Configuration class for Alternator load balancing settings.
 *
 * <p>This class holds all configuration needed for connecting to an Alternator cluster:
 *
 * <ul>
 *   <li>Seed hosts, scheme, and port for cluster discovery
 *   <li>Routing scope for node targeting with fallback support
 *   <li>Request compression settings
 *   <li>HTTP header optimization settings
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Using a seed URI (convenience)
 * AlternatorConfig config = AlternatorConfig.builder()
 *     .withSeedNode(URI.create("https://localhost:8043"))
 *     .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
 *     .build();
 *
 * // Using explicit host, scheme, and port
 * AlternatorConfig config = AlternatorConfig.builder()
 *     .withSeedHosts(Arrays.asList("192.168.1.100", "192.168.1.101"))
 *     .withScheme("https")
 *     .withPort(8043)
 *     .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AlternatorConfig {
  private static final Logger logger = Logger.getLogger(AlternatorConfig.class.getName());

  /** Default minimum request body size (in bytes) that triggers compression. */
  public static final int DEFAULT_MIN_COMPRESSION_SIZE_BYTES = 1024;

  /** Default refresh interval (in milliseconds) when there are active requests. */
  public static final long DEFAULT_ACTIVE_REFRESH_INTERVAL_MS = 1000;

  /** Default refresh interval (in milliseconds) when the client is idle. */
  public static final long DEFAULT_IDLE_REFRESH_INTERVAL_MS = 60000;

  /** Default maximum number of connections in the HTTP client pool. */
  public static final int DEFAULT_MAX_CONNECTIONS = 400;

  /**
   * Recommended maximum retry attempts for partition key discovery (currently hard-coded).
   *
   * <p><strong>Note:</strong> This value is currently hard-coded in PartitionKeyResolver and cannot
   * be configured at runtime. It is exposed here for documentation and future configurability.
   */
  public static final int RECOMMENDED_PARTITION_KEY_DISCOVERY_MAX_RETRIES = 3;

  /**
   * Recommended initial retry delay for partition key discovery in milliseconds (currently
   * hard-coded).
   *
   * <p><strong>Note:</strong> This value is currently hard-coded in PartitionKeyResolver and cannot
   * be configured at runtime. It is exposed here for documentation and future configurability.
   */
  public static final long RECOMMENDED_PARTITION_KEY_DISCOVERY_INITIAL_DELAY_MS = 100;

  /**
   * Recommended maximum retry delay for partition key discovery in milliseconds (currently
   * hard-coded).
   *
   * <p><strong>Note:</strong> This value is currently hard-coded in PartitionKeyResolver and cannot
   * be configured at runtime. It is exposed here for documentation and future configurability.
   */
  public static final long RECOMMENDED_PARTITION_KEY_DISCOVERY_MAX_DELAY_MS = 2000;

  /**
   * Recommended cooldown period after permanent failure for partition key discovery in milliseconds
   * (currently hard-coded).
   *
   * <p>After a permanent failure (e.g., table doesn't exist), the library waits this long before
   * allowing another discovery attempt for the same table. Default is 5 minutes.
   *
   * <p><strong>Note:</strong> This constant is currently hard-coded in PartitionKeyResolver and
   * cannot be configured at runtime. It is exposed here for documentation and future
   * configurability.
   */
  public static final long RECOMMENDED_PARTITION_KEY_DISCOVERY_COOLDOWN_MS = 5 * 60 * 1000;

  /** Default maximum idle time for pooled connections in milliseconds (10 minutes). */
  public static final long DEFAULT_CONNECTION_MAX_IDLE_TIME_MS = 600_000;

  /**
   * Default maximum lifetime for pooled connections in milliseconds (0 = unlimited).
   *
   * <p>A value of 0 means connections are not closed based on age.
   */
  public static final long DEFAULT_CONNECTION_TIME_TO_LIVE_MS = 0;

  /** Default timeout for acquiring a connection from the pool in milliseconds (10 seconds). */
  public static final long DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MS = 10_000;

  /** Default timeout for establishing a TCP connection in milliseconds (15 seconds). */
  public static final long DEFAULT_CONNECTION_TIMEOUT_MS = 15_000;

  /**
   * Base HTTP headers required for proper operation with Alternator.
   *
   * <p>These headers are always required regardless of configuration:
   *
   * <ul>
   *   <li>{@code Host} - Required by HTTP/1.1
   *   <li>{@code X-Amz-Target} - Specifies the DynamoDB operation
   *   <li>{@code Content-Type} - MIME type for DynamoDB API (application/x-amz-json-1.0)
   *   <li>{@code Content-Length} - Required for request body
   *   <li>{@code Connection} - HTTP keep-alive for connection reuse
   * </ul>
   *
   * @since 2.0.0
   */
  public static final Set<String> BASE_REQUIRED_HEADERS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "Host", "X-Amz-Target", "Content-Type", "Content-Length", "Connection")));

  /**
   * HTTP headers required when response compression is enabled.
   *
   * <ul>
   *   <li>{@code Accept-Encoding} - For response compression negotiation
   * </ul>
   *
   * @since 2.0.6
   */
  public static final Set<String> RESPONSE_COMPRESSION_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("Accept-Encoding")));

  /**
   * HTTP headers required when user-agent reporting is enabled.
   *
   * <ul>
   *   <li>{@code User-Agent} - Reports the ScyllaDB Alternator client version
   * </ul>
   *
   * @since 2.0.5
   */
  public static final Set<String> USER_AGENT_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("User-Agent")));

  /**
   * HTTP headers required when compression is enabled.
   *
   * <ul>
   *   <li>{@code Content-Encoding} - For request compression (e.g., gzip)
   * </ul>
   *
   * @since 2.0.0
   */
  public static final Set<String> COMPRESSION_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("Content-Encoding")));

  /**
   * HTTP headers required when authentication is enabled.
   *
   * <ul>
   *   <li>{@code Authorization} - AWS SigV4 signature
   *   <li>{@code X-Amz-Date} - Timestamp for AWS signature
   * </ul>
   *
   * @since 2.0.0
   */
  public static final Set<String> AUTHENTICATION_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("Authorization", "X-Amz-Date")));

  private final List<String> seedHosts;
  private final String scheme;
  private final int port;
  private final RoutingScope routingScope;
  private final RequestCompressionAlgorithm compressionAlgorithm;
  private final int minCompressionSizeBytes;
  private final List<ResponseCompressionAlgorithm> responseCompressionAlgorithms;
  private final boolean optimizeHeaders;
  private final Set<String> headersWhitelist;
  private final boolean userAgentEnabled;
  private final boolean authenticationEnabled;
  private final TlsSessionCacheConfig tlsSessionCacheConfig;
  private final TlsConfig tlsConfig;
  private final KeyRouteAffinityConfig keyRouteAffinityConfig;
  private final long activeRefreshIntervalMs;
  private final long idleRefreshIntervalMs;
  private final int maxConnections;
  private final long connectionMaxIdleTimeMs;
  private final long connectionTimeToLiveMs;
  private final long connectionAcquisitionTimeoutMs;
  private final long connectionTimeoutMs;
  private final NodeHealthConfig nodeHealthConfig;

  /**
   * Protected constructor kept for source and binary compatibility with 2.0.x subclasses.
   *
   * @param seedHosts the list of seed host addresses (IP or hostname) for cluster discovery
   * @param scheme the URI scheme (http or https)
   * @param port the port number
   * @param routingScope the routing scope for node targeting
   * @param compressionAlgorithm the compression algorithm to use
   * @param minCompressionSizeBytes minimum request size in bytes to trigger compression
   * @param responseCompressionAlgorithms response compression algorithms to advertise and decode
   * @param optimizeHeaders whether to enable HTTP header optimization
   * @param headersWhitelist the set of headers to preserve when optimization is enabled
   * @param userAgentEnabled whether user-agent reporting is enabled
   * @param authenticationEnabled whether authentication is enabled
   * @param tlsSessionCacheConfig the TLS session cache configuration for quick TLS renegotiation
   * @param tlsConfig the TLS configuration including CA certificates and verification settings
   * @param keyRouteAffinityConfig the key route affinity configuration
   * @param activeRefreshIntervalMs refresh interval when there are active requests
   * @param idleRefreshIntervalMs refresh interval when the client is idle
   * @param maxConnections maximum number of connections in the HTTP client pool
   * @param connectionMaxIdleTimeMs maximum idle time for pooled connections in milliseconds
   * @param connectionTimeToLiveMs maximum lifetime for pooled connections in milliseconds (0 =
   *     unlimited)
   * @param connectionAcquisitionTimeoutMs maximum time to wait for a connection from the pool in
   *     milliseconds
   * @param connectionTimeoutMs maximum time to wait for a connection to be established in
   *     milliseconds
   */
  protected AlternatorConfig(
      List<String> seedHosts,
      String scheme,
      int port,
      RoutingScope routingScope,
      RequestCompressionAlgorithm compressionAlgorithm,
      int minCompressionSizeBytes,
      List<ResponseCompressionAlgorithm> responseCompressionAlgorithms,
      boolean optimizeHeaders,
      Set<String> headersWhitelist,
      boolean userAgentEnabled,
      boolean authenticationEnabled,
      TlsSessionCacheConfig tlsSessionCacheConfig,
      TlsConfig tlsConfig,
      KeyRouteAffinityConfig keyRouteAffinityConfig,
      long activeRefreshIntervalMs,
      long idleRefreshIntervalMs,
      int maxConnections,
      long connectionMaxIdleTimeMs,
      long connectionTimeToLiveMs,
      long connectionAcquisitionTimeoutMs,
      long connectionTimeoutMs) {
    this(
        seedHosts,
        scheme,
        port,
        routingScope,
        compressionAlgorithm,
        minCompressionSizeBytes,
        responseCompressionAlgorithms,
        optimizeHeaders,
        headersWhitelist,
        userAgentEnabled,
        authenticationEnabled,
        tlsSessionCacheConfig,
        tlsConfig,
        keyRouteAffinityConfig,
        activeRefreshIntervalMs,
        idleRefreshIntervalMs,
        maxConnections,
        connectionMaxIdleTimeMs,
        connectionTimeToLiveMs,
        connectionAcquisitionTimeoutMs,
        connectionTimeoutMs,
        NodeHealthConfig.getDefault());
  }

  /**
   * Private constructor. Use {@link AlternatorConfig#builder()} to create instances.
   *
   * @param seedHosts the list of seed host addresses (IP or hostname) for cluster discovery
   * @param scheme the URI scheme (http or https)
   * @param port the port number
   * @param routingScope the routing scope for node targeting
   * @param compressionAlgorithm the compression algorithm to use
   * @param minCompressionSizeBytes minimum request size in bytes to trigger compression
   * @param responseCompressionAlgorithms response compression algorithms to advertise and decode
   * @param optimizeHeaders whether to enable HTTP header optimization
   * @param headersWhitelist the set of headers to preserve when optimization is enabled
   * @param userAgentEnabled whether user-agent reporting is enabled
   * @param authenticationEnabled whether authentication is enabled
   * @param tlsSessionCacheConfig the TLS session cache configuration for quick TLS renegotiation
   * @param tlsConfig the TLS configuration including CA certificates and verification settings
   * @param keyRouteAffinityConfig the key route affinity configuration
   * @param activeRefreshIntervalMs refresh interval when there are active requests
   * @param idleRefreshIntervalMs refresh interval when the client is idle
   * @param maxConnections maximum number of connections in the HTTP client pool
   * @param connectionMaxIdleTimeMs maximum idle time for pooled connections in milliseconds
   * @param connectionTimeToLiveMs maximum lifetime for pooled connections in milliseconds (0 =
   *     unlimited)
   * @param connectionAcquisitionTimeoutMs maximum time to wait for a connection from the pool in
   *     milliseconds
   * @param connectionTimeoutMs maximum time to wait for a connection to be established in
   *     milliseconds
   * @param nodeHealthConfig configuration for node health tracking
   */
  private AlternatorConfig(
      List<String> seedHosts,
      String scheme,
      int port,
      RoutingScope routingScope,
      RequestCompressionAlgorithm compressionAlgorithm,
      int minCompressionSizeBytes,
      List<ResponseCompressionAlgorithm> responseCompressionAlgorithms,
      boolean optimizeHeaders,
      Set<String> headersWhitelist,
      boolean userAgentEnabled,
      boolean authenticationEnabled,
      TlsSessionCacheConfig tlsSessionCacheConfig,
      TlsConfig tlsConfig,
      KeyRouteAffinityConfig keyRouteAffinityConfig,
      long activeRefreshIntervalMs,
      long idleRefreshIntervalMs,
      int maxConnections,
      long connectionMaxIdleTimeMs,
      long connectionTimeToLiveMs,
      long connectionAcquisitionTimeoutMs,
      long connectionTimeoutMs,
      NodeHealthConfig nodeHealthConfig) {
    this.seedHosts =
        seedHosts != null
            ? Collections.unmodifiableList(new ArrayList<>(seedHosts))
            : Collections.<String>emptyList();
    this.scheme = scheme != null ? scheme : "";
    this.port = port;
    this.routingScope = routingScope;
    this.compressionAlgorithm =
        compressionAlgorithm != null ? compressionAlgorithm : RequestCompressionAlgorithm.NONE;
    this.minCompressionSizeBytes =
        minCompressionSizeBytes >= 0 ? minCompressionSizeBytes : DEFAULT_MIN_COMPRESSION_SIZE_BYTES;
    this.responseCompressionAlgorithms =
        responseCompressionAlgorithms != null
            ? Collections.unmodifiableList(new ArrayList<>(responseCompressionAlgorithms))
            : Collections.emptyList();
    this.optimizeHeaders = optimizeHeaders;
    this.userAgentEnabled = userAgentEnabled;
    this.authenticationEnabled = authenticationEnabled;

    // Compute default whitelist based on configuration if not provided
    if (headersWhitelist != null) {
      this.headersWhitelist = Collections.unmodifiableSet(new HashSet<>(headersWhitelist));
    } else {
      this.headersWhitelist = getRequiredHeaders();
    }

    // Use default TLS session cache config if not provided
    this.tlsSessionCacheConfig =
        tlsSessionCacheConfig != null ? tlsSessionCacheConfig : TlsSessionCacheConfig.getDefault();
    // Use default TLS config (trust-all) if not provided - for backwards compatibility
    this.tlsConfig = tlsConfig != null ? tlsConfig : TlsConfig.trustAll();
    this.keyRouteAffinityConfig = keyRouteAffinityConfig;
    this.activeRefreshIntervalMs =
        activeRefreshIntervalMs > 0 ? activeRefreshIntervalMs : DEFAULT_ACTIVE_REFRESH_INTERVAL_MS;
    this.idleRefreshIntervalMs =
        idleRefreshIntervalMs > 0 ? idleRefreshIntervalMs : DEFAULT_IDLE_REFRESH_INTERVAL_MS;
    this.maxConnections = maxConnections;
    this.connectionMaxIdleTimeMs = connectionMaxIdleTimeMs;
    this.connectionTimeToLiveMs = connectionTimeToLiveMs;
    this.connectionAcquisitionTimeoutMs = connectionAcquisitionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.nodeHealthConfig =
        nodeHealthConfig != null ? nodeHealthConfig : NodeHealthConfig.getDefault();
  }

  /**
   * Gets the list of seed host addresses for cluster discovery.
   *
   * <p>The seed hosts are the initial endpoints (IP addresses or hostnames) used to discover the
   * full cluster topology via the {@code /localnodes} API. Use {@link #getScheme()} and {@link
   * #getPort()} to construct full URIs.
   *
   * @return an unmodifiable list of seed host addresses, never null but may be empty
   * @since 2.0.0
   */
  public List<String> getSeedHosts() {
    return seedHosts;
  }

  /**
   * Gets the URI scheme (http or https).
   *
   * @return the scheme, or empty string if not set
   * @since 2.0.0
   */
  public String getScheme() {
    return scheme;
  }

  /**
   * Gets the port number for Alternator connections.
   *
   * @return the port number
   * @since 2.0.0
   */
  public int getPort() {
    return port;
  }

  /**
   * Gets the configured routing scope.
   *
   * <p>The routing scope defines which nodes should be used for load balancing and how to fall back
   * when nodes are unavailable. This method always returns a non-null scope (defaults to {@link
   * ClusterScope} if not configured).
   *
   * @return the routing scope, never {@code null}
   * @since 2.0.0
   */
  public RoutingScope getRoutingScope() {
    return routingScope;
  }

  /**
   * Gets the configured request compression algorithm.
   *
   * <p>Compression is applied to request bodies that exceed {@link #getMinCompressionSizeBytes()}.
   * By default, compression is disabled ({@link RequestCompressionAlgorithm#NONE}).
   *
   * @return the compression algorithm, never null
   * @since 2.0.0
   */
  public RequestCompressionAlgorithm getCompressionAlgorithm() {
    return compressionAlgorithm;
  }

  /**
   * Gets the minimum request body size (in bytes) that triggers compression.
   *
   * <p>Requests smaller than this threshold will not be compressed, even if a compression algorithm
   * is configured. This avoids the overhead of compressing small payloads that may not benefit from
   * compression or may even increase in size.
   *
   * <p>Default value: {@link #DEFAULT_MIN_COMPRESSION_SIZE_BYTES} (1024 bytes / 1 KB)
   *
   * @return the minimum request size in bytes, always non-negative
   * @since 2.0.0
   */
  public int getMinCompressionSizeBytes() {
    return minCompressionSizeBytes;
  }

  /**
   * Gets the configured HTTP response compression algorithms.
   *
   * <p>The algorithms are ordered as they will be sent in the {@code Accept-Encoding} header. The
   * default is an empty list, which means response compression is disabled.
   *
   * @return response compression algorithms, never null
   * @since 2.0.6
   */
  public List<ResponseCompressionAlgorithm> getResponseCompressionAlgorithms() {
    return responseCompressionAlgorithms;
  }

  /**
   * Checks if HTTP response compression negotiation and decompression are enabled.
   *
   * @return true if at least one response compression algorithm is configured
   * @since 2.0.6
   */
  public boolean isResponseCompressionEnabled() {
    return !responseCompressionAlgorithms.isEmpty();
  }

  /**
   * Checks if HTTP header optimization is enabled.
   *
   * <p>When enabled, outgoing requests will have their HTTP headers filtered to include only those
   * in the whitelist, reducing network traffic overhead. Alternator does not use all headers that
   * DynamoDB normally uses, so this optimization can reduce outgoing traffic by up to 56%.
   *
   * @return true if header optimization is enabled, false otherwise
   * @since 2.0.0
   */
  public boolean isOptimizeHeaders() {
    return optimizeHeaders;
  }

  /**
   * Gets the set of HTTP headers to preserve when optimization is enabled.
   *
   * <p>Only headers in this whitelist will be sent with requests when header optimization is
   * enabled. All other headers will be removed. Header names are matched case-insensitively per RFC
   * 7230.
   *
   * @return unmodifiable set of allowed header names
   * @since 2.0.0
   */
  public Set<String> getHeadersWhitelist() {
    return headersWhitelist;
  }

  /**
   * Checks if user-agent reporting is enabled.
   *
   * <p>When enabled, the client sends a {@code User-Agent} header that includes the ScyllaDB
   * Alternator client version. When disabled, optimized header whitelists do not require {@code
   * User-Agent}.
   *
   * @return true if user-agent reporting is enabled, false otherwise
   * @since 2.0.5
   */
  public boolean isUserAgentEnabled() {
    return userAgentEnabled;
  }

  /**
   * Checks if authentication is enabled.
   *
   * <p>When authentication is disabled, the client will use anonymous credentials and exclude
   * authentication headers ({@code Authorization}, {@code X-Amz-Date}, {@code
   * X-Amz-Content-Sha256}) from requests when header optimization is enabled.
   *
   * <p>This is useful when connecting to Alternator clusters that have authentication disabled.
   *
   * @return true if authentication is enabled (default), false if disabled
   * @since 2.0.0
   */
  public boolean isAuthenticationEnabled() {
    return authenticationEnabled;
  }

  /**
   * Gets the TLS session cache configuration.
   *
   * <p>TLS session caching enables quick TLS renegotiation when reconnecting to Alternator nodes by
   * using session tickets (RFC 5077). This reduces handshake latency for subsequent connections.
   *
   * @return the TLS session cache configuration, never null
   * @since 2.0.0
   * @deprecated Use {@link #getTlsConfig()} and {@link TlsConfig#getSessionCacheConfig()} instead
   */
  @Deprecated
  public TlsSessionCacheConfig getTlsSessionCacheConfig() {
    return tlsSessionCacheConfig;
  }

  /**
   * Gets the TLS configuration.
   *
   * <p>The TLS configuration controls certificate validation, custom CA certificates, client TLS
   * certificates, hostname verification, and session caching settings.
   *
   * @return the TLS configuration, never null
   * @since 1.0.9
   */
  public TlsConfig getTlsConfig() {
    return tlsConfig;
  }

  /**
   * Gets the configured key route affinity configuration.
   *
   * <p>Key route affinity ensures that all requests for the same partition key are routed to the
   * same Alternator node, which improves performance for Lightweight Transactions (LWT) that use
   * Paxos consensus.
   *
   * @return the key route affinity configuration, or null if not configured
   * @since 1.0.7
   */
  public KeyRouteAffinityConfig getKeyRouteAffinityConfig() {
    return keyRouteAffinityConfig;
  }

  /**
   * Gets the refresh interval (in milliseconds) for updating the node list when there are active
   * requests.
   *
   * <p>When requests are being made to the cluster, the node list is refreshed at this interval to
   * quickly detect topology changes.
   *
   * <p>Default: {@link #DEFAULT_ACTIVE_REFRESH_INTERVAL_MS} (1000ms / 1 second)
   *
   * @return the active refresh interval in milliseconds
   * @since 1.0.8
   */
  public long getActiveRefreshIntervalMs() {
    return activeRefreshIntervalMs;
  }

  /**
   * Gets the refresh interval (in milliseconds) for updating the node list when the client is idle.
   *
   * <p>When no requests have been made recently, the node list is refreshed at this longer interval
   * to reduce unnecessary network traffic while still keeping the node list reasonably up-to-date.
   *
   * <p>Default: {@link #DEFAULT_IDLE_REFRESH_INTERVAL_MS} (60000ms / 1 minute)
   *
   * @return the idle refresh interval in milliseconds
   * @since 1.0.8
   */
  public long getIdleRefreshIntervalMs() {
    return idleRefreshIntervalMs;
  }

  /**
   * Gets the maximum number of connections in the HTTP client connection pool.
   *
   * @return the maximum number of connections
   * @since 2.0.2
   */
  public int getMaxConnections() {
    return maxConnections;
  }

  /**
   * Gets the maximum idle time for pooled connections in milliseconds.
   *
   * <p>Connections that have been idle longer than this value will be closed.
   *
   * @return the maximum idle time in milliseconds
   * @since 2.0.2
   */
  public long getConnectionMaxIdleTimeMs() {
    return connectionMaxIdleTimeMs;
  }

  /**
   * Gets the maximum lifetime for pooled connections in milliseconds.
   *
   * <p>Connections older than this value will be closed regardless of activity. A value of 0 means
   * unlimited lifetime.
   *
   * @return the connection time-to-live in milliseconds
   * @since 2.0.2
   */
  public long getConnectionTimeToLiveMs() {
    return connectionTimeToLiveMs;
  }

  /**
   * Returns the maximum time to wait for a connection from the pool in milliseconds.
   *
   * <p>When the connection pool is exhausted, this controls how long the client will wait for a
   * connection to become available before failing.
   *
   * @return the connection acquisition timeout in milliseconds
   * @since 2.0.3
   */
  public long getConnectionAcquisitionTimeoutMs() {
    return connectionAcquisitionTimeoutMs;
  }

  /**
   * Returns the maximum time to wait for a connection to be established in milliseconds.
   *
   * <p>This controls how long the client will wait for the TCP connection handshake to complete
   * when connecting to a node.
   *
   * @return the connection timeout in milliseconds
   * @since 2.0.3
   */
  public long getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  /**
   * Gets the node health tracking configuration.
   *
   * @return the node health configuration, never null
   * @since 2.0.6
   */
  public NodeHealthConfig getNodeHealthConfig() {
    return nodeHealthConfig;
  }

  /**
   * Returns the set of HTTP headers required for this configuration.
   *
   * <p>This method returns the minimum set of headers needed based on the current settings
   * (compression algorithm and authentication state).
   *
   * @return an unmodifiable set of required header names
   * @since 2.0.0
   */
  public Set<String> getRequiredHeaders() {
    Set<String> required = new HashSet<>(BASE_REQUIRED_HEADERS);
    if (isResponseCompressionEnabled()) {
      required.addAll(RESPONSE_COMPRESSION_HEADERS);
    }
    if (compressionAlgorithm != null && compressionAlgorithm != RequestCompressionAlgorithm.NONE) {
      required.addAll(COMPRESSION_HEADERS);
    }
    if (userAgentEnabled) {
      required.addAll(USER_AGENT_HEADERS);
    }
    if (authenticationEnabled) {
      required.addAll(AUTHENTICATION_HEADERS);
    }
    return Collections.unmodifiableSet(required);
  }

  /**
   * Creates a new builder for AlternatorConfig.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> seedHosts = new ArrayList<>();
    private String scheme = "";
    private int port = -1;
    private RoutingScope routingScope = null;
    private RequestCompressionAlgorithm compressionAlgorithm = RequestCompressionAlgorithm.NONE;
    private int minCompressionSizeBytes = DEFAULT_MIN_COMPRESSION_SIZE_BYTES;
    private List<ResponseCompressionAlgorithm> responseCompressionAlgorithms =
        Collections.emptyList();
    private boolean optimizeHeaders = false;
    private Set<String> headersWhitelist = null; // null means use default based on config
    private boolean headersWhitelistWasSet = false;
    private boolean userAgentEnabled = true;
    private boolean authenticationEnabled = true;
    private TlsSessionCacheConfig tlsSessionCacheConfig = null; // null means use default
    private TlsConfig tlsConfig = null; // null means use default (trust-all for backwards compat)
    private KeyRouteAffinityConfig keyRouteAffinityConfig = null;
    private long activeRefreshIntervalMs = DEFAULT_ACTIVE_REFRESH_INTERVAL_MS;
    private long idleRefreshIntervalMs = DEFAULT_IDLE_REFRESH_INTERVAL_MS;
    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private long connectionMaxIdleTimeMs = DEFAULT_CONNECTION_MAX_IDLE_TIME_MS;
    private boolean connectionMaxIdleTimeMsSet = false;
    private long connectionTimeToLiveMs = DEFAULT_CONNECTION_TIME_TO_LIVE_MS;
    private long connectionAcquisitionTimeoutMs = DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MS;
    private long connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
    private NodeHealthConfig nodeHealthConfig = NodeHealthConfig.getDefault();

    /** Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances. */
    Builder() {}

    /**
     * Sets whether authentication is enabled. Package-private - authentication is auto-detected
     * based on whether credentials are provided to the client builder.
     *
     * @param authenticationEnabled true if credentials are provided
     * @return this builder instance
     */
    Builder authenticationEnabled(boolean authenticationEnabled) {
      this.authenticationEnabled = authenticationEnabled;
      return this;
    }

    Builder withUserAgentEnabled(boolean userAgentEnabled) {
      this.userAgentEnabled = userAgentEnabled;
      return this;
    }

    /**
     * Sets a single seed node URI for cluster discovery.
     *
     * <p>This is a convenience method that extracts the host, scheme, and port from a URI. The URI
     * will be used to discover all other nodes in the cluster via the {@code /localnodes} API.
     *
     * @param seedUri the seed URI for Alternator cluster discovery
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withSeedNode(URI seedUri) {
      if (seedUri != null) {
        this.seedHosts = Collections.singletonList(seedUri.getHost());
        this.scheme = seedUri.getScheme();
        this.port = seedUri.getPort();
      }
      return this;
    }

    /**
     * Sets the URI scheme for Alternator connections.
     *
     * @param scheme the URI scheme ("http" or "https")
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withScheme(String scheme) {
      this.scheme = scheme != null ? scheme : "";
      return this;
    }

    /**
     * Sets the port number for Alternator connections.
     *
     * @param port the port number
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    /**
     * Sets seed host addresses for cluster discovery.
     *
     * <p>The seed hosts are IP addresses or hostnames of Alternator nodes. You must also set the
     * scheme and port using {@link #withScheme(String)} and {@link #withPort(int)}.
     *
     * @param hosts the collection of seed host addresses (IP or hostname)
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withSeedHosts(Collection<String> hosts) {
      if (hosts != null) {
        this.seedHosts = new ArrayList<>(hosts);
      }
      return this;
    }

    /**
     * Sets a single seed host address for cluster discovery.
     *
     * <p>The seed host is an IP address or hostname of an Alternator node. You must also set the
     * scheme and port using {@link #withScheme(String)} and {@link #withPort(int)}.
     *
     * @param host the seed host address (IP or hostname)
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withSeedHost(String host) {
      if (host != null) {
        this.seedHosts = Collections.singletonList(host);
      }
      return this;
    }

    /**
     * Returns the set of HTTP headers required for the current configuration.
     *
     * <p>This method computes the minimum set of headers needed based on the current builder
     * settings (compression algorithm and authentication state). Use this to understand what
     * headers must be included when providing a custom whitelist.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig.Builder builder = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP);
     *
     * Set<String> required = builder.getRequiredHeaders();
     * // required contains: Host, X-Amz-Target, Content-Type, Content-Length,
     * //                    Content-Encoding, Authorization, X-Amz-Date
     * }</pre>
     *
     * @return an unmodifiable set of required header names for the current configuration
     * @since 2.0.0
     */
    public Set<String> getRequiredHeaders() {
      Set<String> required = new HashSet<>(BASE_REQUIRED_HEADERS);
      if (!responseCompressionAlgorithms.isEmpty()) {
        required.addAll(RESPONSE_COMPRESSION_HEADERS);
      }
      if (compressionAlgorithm != null && compressionAlgorithm.isEnabled()) {
        required.addAll(COMPRESSION_HEADERS);
      }
      if (userAgentEnabled) {
        required.addAll(USER_AGENT_HEADERS);
      }
      if (authenticationEnabled) {
        required.addAll(AUTHENTICATION_HEADERS);
      }
      return Collections.unmodifiableSet(required);
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
     * @param routingScope the routing scope, or {@code null} to use all nodes (ClusterScope)
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withRoutingScope(RoutingScope routingScope) {
      this.routingScope = routingScope;
      return this;
    }

    /**
     * Sets the request compression algorithm.
     *
     * <p>When a compression algorithm other than {@link RequestCompressionAlgorithm#NONE} is
     * specified, request bodies exceeding the minimum size threshold will be compressed before
     * transmission.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .build();
     * }</pre>
     *
     * @param algorithm the compression algorithm to use, or null to disable compression
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withCompressionAlgorithm(RequestCompressionAlgorithm algorithm) {
      this.compressionAlgorithm = algorithm != null ? algorithm : RequestCompressionAlgorithm.NONE;
      return this;
    }

    /**
     * Sets the minimum request body size (in bytes) that triggers compression.
     *
     * <p>Requests smaller than this threshold will not be compressed, even if a compression
     * algorithm is configured. This avoids compressing small payloads that may not benefit.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_MIN_COMPRESSION_SIZE_BYTES} (1024 bytes / 1 KB)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .withMinCompressionSizeBytes(2048)  // Only compress requests > 2KB
     *     .build();
     * }</pre>
     *
     * @param minCompressionSizeBytes minimum request size in bytes, must be non-negative
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withMinCompressionSizeBytes(int minCompressionSizeBytes) {
      this.minCompressionSizeBytes = minCompressionSizeBytes;
      return this;
    }

    /**
     * Sets the HTTP response compression algorithms to advertise and decode.
     *
     * <p>The configured order is used for the {@code Accept-Encoding} header. Response compression
     * is disabled by default.
     *
     * @param algorithms the response compression algorithms to enable
     * @return this builder instance
     * @throws IllegalArgumentException if algorithms is null, empty, or contains null
     * @since 2.0.6
     */
    public Builder withResponseCompression(ResponseCompressionAlgorithm... algorithms) {
      if (algorithms == null) {
        throw new IllegalArgumentException(
            "responseCompressionAlgorithms cannot be null or empty. "
                + "Use withResponseCompressionDisabled() to disable response compression.");
      }
      return withResponseCompression(Arrays.asList(algorithms));
    }

    /**
     * Sets the HTTP response compression algorithms to advertise and decode.
     *
     * <p>The configured order is used for the {@code Accept-Encoding} header. Response compression
     * is disabled by default.
     *
     * @param algorithms the response compression algorithms to enable
     * @return this builder instance
     * @throws IllegalArgumentException if algorithms is null, empty, or contains null
     * @since 2.0.6
     */
    public Builder withResponseCompression(Collection<ResponseCompressionAlgorithm> algorithms) {
      this.responseCompressionAlgorithms = ResponseCompressionAlgorithm.validatedList(algorithms);
      return this;
    }

    /**
     * Disables HTTP response compression negotiation and decompression.
     *
     * <p>When disabled, the client does not set {@code Accept-Encoding} and does not decompress
     * compressed response bodies.
     *
     * @return this builder instance
     * @since 2.0.6
     */
    public Builder withResponseCompressionDisabled() {
      this.responseCompressionAlgorithms = Collections.emptyList();
      return this;
    }

    /**
     * Enables or disables HTTP header optimization.
     *
     * <p>When enabled, outgoing requests will have their HTTP headers filtered to include only
     * those in the configured whitelist (see {@link #withHeadersWhitelist(Collection)}). This
     * reduces network traffic by removing headers that Alternator does not use.
     *
     * <p>Default: false (disabled)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withOptimizeHeaders(true)
     *     .build();
     * }</pre>
     *
     * @param optimizeHeaders true to enable header filtering, false to disable
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withOptimizeHeaders(boolean optimizeHeaders) {
      this.optimizeHeaders = optimizeHeaders;
      return this;
    }

    /**
     * Sets a custom whitelist of HTTP headers to preserve when optimization is enabled.
     *
     * <p>Only headers in this list will be sent with requests when header optimization is enabled.
     * All other headers will be removed. Header names are matched case-insensitively per RFC 7230.
     *
     * <p>The provided whitelist must include all headers required for the current configuration.
     * Use {@link #getRequiredHeaders()} to see which headers are required. If required headers are
     * missing, an {@link IllegalArgumentException} will be thrown at build time.
     *
     * <p>Default: Computed automatically based on compression and authentication settings. See
     * {@link AlternatorConfig#getRequiredHeaders()}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig.Builder builder = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .withOptimizeHeaders(true);
     *
     * // Get required headers and add custom ones
     * Set<String> whitelist = new HashSet<>(builder.getRequiredHeaders());
     * whitelist.add("X-Custom-Header");
     *
     * AlternatorConfig config = builder
     *     .withHeadersWhitelist(whitelist)
     *     .build();
     * }</pre>
     *
     * @param headers collection of header names to preserve (case-insensitive), must not be null
     * @return this builder instance
     * @since 2.0.0
     */
    public Builder withHeadersWhitelist(Collection<String> headers) {
      this.headersWhitelist = headers != null ? new HashSet<>(headers) : null;
      this.headersWhitelistWasSet = true;
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
     * <p>Example:
     *
     * <pre>{@code
     * // Custom TLS session cache configuration
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withTlsSessionCacheConfig(TlsSessionCacheConfig.builder()
     *         .withSessionCacheSize(200)
     *         .withSessionTimeoutSeconds(3600)
     *         .build())
     *     .build();
     *
     * // Disable TLS session caching
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withTlsSessionCacheConfig(TlsSessionCacheConfig.disabled())
     *     .build();
     * }</pre>
     *
     * @param tlsSessionCacheConfig the TLS session cache configuration, or null to use default
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use {@link #withTlsConfig(TlsConfig)} instead
     */
    @Deprecated
    public Builder withTlsSessionCacheConfig(TlsSessionCacheConfig tlsSessionCacheConfig) {
      this.tlsSessionCacheConfig = tlsSessionCacheConfig;
      return this;
    }

    /**
     * Sets the TLS configuration for secure connections.
     *
     * <p>The TLS configuration controls certificate validation, custom CA certificates, hostname
     * verification, and session caching settings.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // Use system CA certificates
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withTlsConfig(TlsConfig.systemDefault())
     *     .build();
     *
     * // Use custom CA certificate
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withTlsConfig(TlsConfig.builder()
     *         .withCaCertPath(Paths.get("/path/to/ca.pem"))
     *         .withTrustSystemCaCerts(false)
     *         .build())
     *     .build();
     *
     * // Use mutual TLS client certificate authentication
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withTlsConfig(TlsConfig.builder()
     *         .withCaCertPath(Paths.get("/path/to/ca.pem"))
     *         .withClientCertificate(
     *             Paths.get("/path/to/client.crt"),
     *             Paths.get("/path/to/client.key"))
     *         .withTrustSystemCaCerts(false)
     *         .build())
     *     .build();
     *
     * // Trust all certificates (development/testing only)
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withTlsConfig(TlsConfig.trustAll())
     *     .build();
     * }</pre>
     *
     * @param tlsConfig the TLS configuration, or null to use default (trust-all)
     * @return this builder instance
     * @since 1.0.9
     */
    public Builder withTlsConfig(TlsConfig tlsConfig) {
      this.tlsConfig = tlsConfig;
      return this;
    }

    /**
     * Sets the key route affinity configuration.
     *
     * <p>Key route affinity ensures that all requests for the same partition key are routed to the
     * same Alternator node, which improves performance for Lightweight Transactions (LWT) that use
     * Paxos consensus.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withKeyRouteAffinity(KeyRouteAffinityConfig.builder()
     *         .withType(KeyRouteAffinity.RMW)
     *         .withPkInfo("users", "user_id")
     *         .build())
     *     .build();
     * }</pre>
     *
     * @param config the key route affinity configuration
     * @return this builder instance
     * @since 1.0.7
     */
    public Builder withKeyRouteAffinity(KeyRouteAffinityConfig config) {
      this.keyRouteAffinityConfig = config;
      return this;
    }

    /**
     * Sets the key route affinity type with no pre-configured PK info.
     *
     * <p>This is a convenience method for configuring key route affinity without providing
     * pre-configured partition key information. Partition key names will be auto-discovered via
     * DescribeTable API on first access to each table.
     *
     * @param type the key route affinity type
     * @return this builder instance
     * @since 1.0.7
     */
    public Builder withKeyRouteAffinity(KeyRouteAffinity type) {
      this.keyRouteAffinityConfig = KeyRouteAffinityConfig.of(type);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when there are active requests.
     *
     * <p>When requests are being made to the cluster, the node list is refreshed at this interval
     * to quickly detect topology changes.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_ACTIVE_REFRESH_INTERVAL_MS} (1000ms / 1 second)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withActiveRefreshIntervalMs(500)  // Refresh every 500ms when active
     *     .build();
     * }</pre>
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public Builder withActiveRefreshIntervalMs(long intervalMs) {
      this.activeRefreshIntervalMs = intervalMs;
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when the client is idle.
     *
     * <p>When no requests have been made recently, the node list is refreshed at this longer
     * interval to reduce unnecessary network traffic while still keeping the node list reasonably
     * up-to-date.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_IDLE_REFRESH_INTERVAL_MS} (60000ms / 1 minute)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withIdleRefreshIntervalMs(120000)  // Refresh every 2 minutes when idle
     *     .build();
     * }</pre>
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public Builder withIdleRefreshIntervalMs(long intervalMs) {
      this.idleRefreshIntervalMs = intervalMs;
      return this;
    }

    /**
     * Sets the maximum number of connections in the HTTP client connection pool.
     *
     * <p>This controls how many simultaneous connections the SDK HTTP client can open. Increasing
     * this value is recommended for multi-node Alternator clusters to allow concurrent requests to
     * different nodes.
     *
     * <p>Default: {@link #DEFAULT_MAX_CONNECTIONS} (400)
     *
     * @param maxConnections the maximum number of connections (must be positive)
     * @return this builder instance
     * @since 2.0.2
     */
    public Builder withMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    /**
     * Sets the maximum idle time for pooled connections in milliseconds.
     *
     * <p>Connections that have been idle longer than this value will be closed and removed from the
     * pool. This helps reclaim resources when the cluster is underutilized.
     *
     * <p>Default: {@link #DEFAULT_CONNECTION_MAX_IDLE_TIME_MS} (10 minutes)
     *
     * @param connectionMaxIdleTimeMs the maximum idle time in milliseconds (must be non-negative)
     * @return this builder instance
     * @since 2.0.2
     */
    public Builder withConnectionMaxIdleTimeMs(long connectionMaxIdleTimeMs) {
      this.connectionMaxIdleTimeMs = connectionMaxIdleTimeMs;
      this.connectionMaxIdleTimeMsSet = true;
      return this;
    }

    /**
     * Sets the maximum lifetime for pooled connections in milliseconds.
     *
     * <p>Connections older than this value will be closed regardless of activity. A value of 0
     * means unlimited lifetime.
     *
     * <p>Default: {@link #DEFAULT_CONNECTION_TIME_TO_LIVE_MS} (0, unlimited)
     *
     * @param connectionTimeToLiveMs the connection time-to-live in milliseconds (must be
     *     non-negative, 0 for unlimited)
     * @return this builder instance
     * @since 2.0.2
     */
    public Builder withConnectionTimeToLiveMs(long connectionTimeToLiveMs) {
      this.connectionTimeToLiveMs = connectionTimeToLiveMs;
      return this;
    }

    /**
     * Sets the maximum time to wait for a connection from the pool in milliseconds.
     *
     * <p>When the connection pool is exhausted, this controls how long the client will wait for a
     * connection to become available before failing with a timeout exception.
     *
     * <p>A value of 0 means "unlimited" (wait indefinitely) for Apache and Netty HTTP clients. The
     * CRT HTTP client does not support zero-duration timeouts; setting 0 with CRT will fall back to
     * the SDK default and log an info message.
     *
     * <p>Default: {@link #DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MS} (10 seconds)
     *
     * @param connectionAcquisitionTimeoutMs the acquisition timeout in milliseconds (must be
     *     non-negative)
     * @return this builder instance
     * @since 2.0.3
     */
    public Builder withConnectionAcquisitionTimeoutMs(long connectionAcquisitionTimeoutMs) {
      this.connectionAcquisitionTimeoutMs = connectionAcquisitionTimeoutMs;
      return this;
    }

    /**
     * Sets the maximum time to wait for a connection to be established in milliseconds.
     *
     * <p>This controls how long the client will wait for the TCP connection handshake to complete
     * when connecting to a node.
     *
     * <p>A value of 0 means "unlimited" (wait indefinitely) for Apache and Netty HTTP clients. The
     * CRT HTTP client does not support zero-duration timeouts; setting 0 with CRT will fall back to
     * the SDK default and log an info message.
     *
     * <p>Default: {@link #DEFAULT_CONNECTION_TIMEOUT_MS} (15 seconds)
     *
     * @param connectionTimeoutMs the connection timeout in milliseconds (must be non-negative)
     * @return this builder instance
     * @since 2.0.3
     */
    public Builder withConnectionTimeoutMs(long connectionTimeoutMs) {
      this.connectionTimeoutMs = connectionTimeoutMs;
      return this;
    }

    /**
     * Sets node health tracking configuration.
     *
     * @param nodeHealthConfig node health configuration, or null to use defaults
     * @return this builder instance
     * @since 2.0.6
     */
    public Builder withNodeHealthConfig(NodeHealthConfig nodeHealthConfig) {
      this.nodeHealthConfig =
          nodeHealthConfig != null ? nodeHealthConfig : NodeHealthConfig.getDefault();
      return this;
    }

    /**
     * Disables node health tracking.
     *
     * @return this builder instance
     * @since 2.0.6
     */
    public Builder withNodeHealthDisabled() {
      this.nodeHealthConfig = NodeHealthConfig.disabled();
      return this;
    }

    /**
     * Builds and returns an {@link AlternatorConfig} instance with the configured settings.
     *
     * @return a new {@link AlternatorConfig} instance
     * @throws IllegalArgumentException if minCompressionSizeBytes is negative, if connection pool
     *     settings are negative, or if headersWhitelist is empty or missing required headers
     */
    public AlternatorConfig build() {
      // Validate minCompressionSizeBytes
      if (minCompressionSizeBytes < 0) {
        throw new IllegalArgumentException(
            "minCompressionSizeBytes must be non-negative, but was: " + minCompressionSizeBytes);
      }

      // Validate connection pool settings
      if (maxConnections <= 0) {
        throw new IllegalArgumentException(
            "maxConnections must be positive, but was: " + maxConnections);
      }
      if (connectionMaxIdleTimeMs < 0) {
        throw new IllegalArgumentException(
            "connectionMaxIdleTimeMs must be non-negative, but was: " + connectionMaxIdleTimeMs);
      }
      if (connectionTimeToLiveMs < 0) {
        throw new IllegalArgumentException(
            "connectionTimeToLiveMs must be non-negative, but was: " + connectionTimeToLiveMs);
      }
      if (connectionAcquisitionTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "connectionAcquisitionTimeoutMs must be non-negative, but was: "
                + connectionAcquisitionTimeoutMs);
      }
      if (connectionTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "connectionTimeoutMs must be non-negative, but was: " + connectionTimeoutMs);
      }

      // Warn when idle connection eviction is explicitly disabled
      if (connectionMaxIdleTimeMsSet && connectionMaxIdleTimeMs == 0) {
        logger.warning(
            "connectionMaxIdleTimeMs is set to 0, which disables idle connection eviction. "
                + "This can lead to stale connections in long-running applications.");
      }

      // Validate headersWhitelist if it was explicitly set
      if (headersWhitelistWasSet) {
        if (headersWhitelist == null || headersWhitelist.isEmpty()) {
          throw new IllegalArgumentException(
              "headersWhitelist cannot be null or empty. "
                  + "To disable optimization, use withOptimizeHeaders(false)");
        }

        // Validate that all required headers are present (case-insensitive)
        Set<String> required = getRequiredHeaders();
        Set<String> headersLowerCase = new HashSet<>();
        for (String h : headersWhitelist) {
          headersLowerCase.add(h.toLowerCase());
        }

        Set<String> missing = new HashSet<>();
        for (String requiredHeader : required) {
          if (!headersLowerCase.contains(requiredHeader.toLowerCase())) {
            missing.add(requiredHeader);
          }
        }

        if (!missing.isEmpty()) {
          throw new IllegalArgumentException(
              "Custom headers whitelist is missing required headers: "
                  + missing
                  + ". "
                  + "Use getRequiredHeaders() to see all required headers for the current configuration.");
        }
      }

      // Determine the effective routing scope (default to ClusterScope if not set)
      RoutingScope effectiveRoutingScope =
          routingScope != null ? routingScope : ClusterScope.create();

      // Determine the effective TLS config:
      // 1. If tlsConfig is explicitly set, use it
      // 2. If tlsSessionCacheConfig is set, create a trust-all config with that session config
      // 3. Otherwise, use default trust-all config
      TlsConfig effectiveTlsConfig = tlsConfig;
      if (effectiveTlsConfig == null && tlsSessionCacheConfig != null) {
        effectiveTlsConfig =
            TlsConfig.builder()
                .withTrustAllCertificates(true)
                .withSessionCacheConfig(tlsSessionCacheConfig)
                .build();
      }

      return new AlternatorConfig(
          seedHosts,
          scheme,
          port,
          effectiveRoutingScope,
          compressionAlgorithm,
          minCompressionSizeBytes,
          responseCompressionAlgorithms,
          optimizeHeaders,
          headersWhitelist,
          userAgentEnabled,
          authenticationEnabled,
          tlsSessionCacheConfig,
          effectiveTlsConfig,
          keyRouteAffinityConfig,
          activeRefreshIntervalMs,
          idleRefreshIntervalMs,
          maxConnections,
          connectionMaxIdleTimeMs,
          connectionTimeToLiveMs,
          connectionAcquisitionTimeoutMs,
          connectionTimeoutMs,
          nodeHealthConfig);
    }
  }
}
