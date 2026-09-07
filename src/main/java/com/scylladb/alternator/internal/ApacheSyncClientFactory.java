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
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory for creating sync HTTP clients using Apache HttpClient.
 *
 * <p>This factory creates an {@link ApacheHttpClient} with Alternator-optimized defaults and
 * applies an optional user customizer callback.
 *
 * @since 2.1.0
 */
public final class ApacheSyncClientFactory {

  private ApacheSyncClientFactory() {}

  /**
   * Creates a sync HTTP client using Apache HttpClient.
   *
   * @param customizer optional consumer to customize the builder (may be null)
   * @param config the Alternator configuration
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient
   */
  public static SdkHttpClient create(
      Consumer<ApacheHttpClient.Builder> customizer, AlternatorConfig config, TlsConfig tlsConfig) {
    ApacheHttpClient.Builder builder = ApacheHttpClient.builder();
    builder.tcpKeepAlive(true);

    // Apply Alternator-optimized defaults from config
    if (config != null) {
      builder.maxConnections(config.getMaxConnections());
      if (config.getConnectionMaxIdleTimeMs() > 0) {
        builder.connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleTimeMs()));
        builder.useIdleConnectionReaper(true);
      } else {
        // Idle time is 0 — disable idle eviction entirely
        builder.useIdleConnectionReaper(false);
      }
      if (config.getConnectionTimeToLiveMs() > 0) {
        builder.connectionTimeToLive(Duration.ofMillis(config.getConnectionTimeToLiveMs()));
      }
      builder.connectionAcquisitionTimeout(
          Duration.ofMillis(config.getConnectionAcquisitionTimeoutMs()));
      builder.connectionTimeout(Duration.ofMillis(config.getConnectionTimeoutMs()));
    }

    applyTlsManagers(builder, tlsConfig);

    // Apply user customizer last — allows overriding any defaults including TLS
    if (customizer != null) {
      customizer.accept(builder);
    }

    if (tlsConfig != null && tlsConfig.isTrustAllCertificates()) {
      return builder.buildWithDefaults(
          AttributeMap.builder()
              .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
              .build());
    }
    return builder.build();
  }

  /**
   * Creates a small sync HTTP client for LiveNodes polling.
   *
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig) {
    return createPollingClient(tlsConfig, 4);
  }

  /** Creates a sync HTTP client for LiveNodes polling with the requested pool size. */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig, int maxConnections) {
    ApacheHttpClient.Builder builder = ApacheHttpClient.builder();
    builder.tcpKeepAlive(true);
    builder.maxConnections(maxConnections);

    return buildWithTls(builder, tlsConfig);
  }

  private static SdkHttpClient buildWithTls(ApacheHttpClient.Builder builder, TlsConfig tlsConfig) {
    if (tlsConfig != null) {
      applyTlsManagers(builder, tlsConfig);
      if (tlsConfig.isTrustAllCertificates()) {
        return builder.buildWithDefaults(
            AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                .build());
      }
    }
    return builder.build();
  }

  private static void applyTlsManagers(ApacheHttpClient.Builder builder, TlsConfig tlsConfig) {
    if (tlsConfig == null) {
      return;
    }
    if (TlsHttpClientSupport.requiresHostnameVerificationDisabled(tlsConfig)
        || TlsSessionCacheSupport.hasCustomSessionCacheConfig(tlsConfig)) {
      SSLContext sslContext = TlsContextFactory.createSslContext(tlsConfig);
      HostnameVerifier hostnameVerifier =
          tlsConfig.isVerifyHostname()
              ? SSLConnectionSocketFactory.getDefaultHostnameVerifier()
              : NoopHostnameVerifier.INSTANCE;
      builder.socketFactory(new SSLConnectionSocketFactory(sslContext, hostnameVerifier));
      return;
    }
    if (tlsConfig.hasClientCertificate()) {
      KeyManager[] keyManagers = TlsContextFactory.createKeyManagers(tlsConfig);
      builder.tlsKeyManagersProvider(() -> keyManagers);
    }
    if (!tlsConfig.isTrustAllCertificates()
        && (!tlsConfig.getCustomCaCertPaths().isEmpty() || !tlsConfig.isTrustSystemCaCerts())) {
      TrustManager[] trustManagers = TlsContextFactory.createTrustManagers(tlsConfig);
      builder.tlsTrustManagersProvider(() -> trustManagers);
    }
  }
}
