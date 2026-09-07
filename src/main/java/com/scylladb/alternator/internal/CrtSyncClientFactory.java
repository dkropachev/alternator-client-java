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
package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.logging.Logger;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.http.crt.TcpKeepAliveConfiguration;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory for creating sync HTTP clients using AWS CRT.
 *
 * <p>This factory creates an {@link AwsCrtHttpClient} with Alternator-optimized defaults and
 * applies an optional user customizer callback.
 *
 * @since 2.1.0
 */
public final class CrtSyncClientFactory {

  private static final Logger logger = Logger.getLogger(CrtSyncClientFactory.class.getName());

  private CrtSyncClientFactory() {}

  /**
   * Creates a sync HTTP client using AWS CRT.
   *
   * @param customizer optional consumer to customize the builder (may be null)
   * @param config the Alternator configuration
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient
   */
  public static SdkHttpClient create(
      Consumer<AwsCrtHttpClient.Builder> customizer, AlternatorConfig config, TlsConfig tlsConfig) {
    AwsCrtHttpClient.Builder builder = AwsCrtHttpClient.builder();
    builder.tcpKeepAliveConfiguration(
        TcpKeepAliveConfiguration.builder()
            .keepAliveInterval(Duration.ofSeconds(30))
            .keepAliveTimeout(Duration.ofSeconds(30))
            .build());

    // Apply Alternator-optimized defaults from config
    if (config != null) {
      builder.maxConcurrency(config.getMaxConnections());
      if (config.getConnectionMaxIdleTimeMs() > 0) {
        builder.connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleTimeMs()));
      } else if (config.getConnectionMaxIdleTimeMs() == 0) {
        logger.info(
            "connectionMaxIdleTimeMs=0 is not supported by CRT HTTP client;"
                + " falling back to SDK default.");
      }
      // CRT SDK requires positive durations for these timeouts; skip when 0 (use SDK default)
      if (config.getConnectionAcquisitionTimeoutMs() > 0) {
        builder.connectionAcquisitionTimeout(
            Duration.ofMillis(config.getConnectionAcquisitionTimeoutMs()));
      } else if (config.getConnectionAcquisitionTimeoutMs() == 0) {
        logger.info(
            "connectionAcquisitionTimeoutMs=0 is not supported by CRT HTTP client;"
                + " falling back to SDK default.");
      }
      if (config.getConnectionTimeoutMs() > 0) {
        builder.connectionTimeout(Duration.ofMillis(config.getConnectionTimeoutMs()));
      } else if (config.getConnectionTimeoutMs() == 0) {
        logger.info(
            "connectionTimeoutMs=0 is not supported by CRT HTTP client;"
                + " falling back to SDK default.");
      }
      if (config.getConnectionTimeToLiveMs() > 0) {
        logger.warning(
            "connectionTimeToLiveMs is not supported by the CRT HTTP client and will be ignored. "
                + "Use Apache HTTP client if connection TTL is required.");
      }
    }

    validateTlsConfig(tlsConfig);

    // Apply user customizer last — allows overriding any defaults
    if (customizer != null) {
      customizer.accept(builder);
    }

    return buildWithTls(builder, tlsConfig);
  }

  /**
   * Creates a small sync HTTP client for LiveNodes polling using CRT.
   *
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig) {
    return createPollingClient(tlsConfig, 4);
  }

  /** Creates a sync HTTP client for LiveNodes polling with the requested pool size. */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig, int maxConcurrency) {
    validateTlsConfig(tlsConfig);
    AwsCrtHttpClient.Builder builder = AwsCrtHttpClient.builder();
    builder.tcpKeepAliveConfiguration(
        TcpKeepAliveConfiguration.builder()
            .keepAliveInterval(Duration.ofSeconds(30))
            .keepAliveTimeout(Duration.ofSeconds(30))
            .build());
    builder.maxConcurrency(maxConcurrency);

    return buildWithTls(builder, tlsConfig);
  }

  private static SdkHttpClient buildWithTls(AwsCrtHttpClient.Builder builder, TlsConfig tlsConfig) {
    if (tlsConfig != null) {
      validateTlsConfig(tlsConfig);
      if (tlsConfig.isTrustAllCertificates()) {
        return builder.buildWithDefaults(
            AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                .build());
      }
    }
    return builder.build();
  }

  private static void validateTlsConfig(TlsConfig tlsConfig) {
    if (tlsConfig == null) {
      return;
    }
    TlsHttpClientSupport.rejectUnsupportedHostnameVerification(tlsConfig, "CRT sync");
    TlsSessionCacheSupport.rejectUnsupportedSessionCacheConfig(tlsConfig, "CRT sync");
    if (!tlsConfig.getCustomCaCertPaths().isEmpty() || tlsConfig.hasClientCertificate()) {
      throw new UnsupportedOperationException(
          "Custom CA certificates and client TLS certificates are not supported with the CRT HTTP client. "
              + "Use Apache or Netty HTTP client instead, or use TlsConfig.trustAll() for testing.");
    }
  }
}
