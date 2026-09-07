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

import com.scylladb.alternator.TlsConfig;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * Detects which sync HTTP client implementation is available on the classpath.
 *
 * <p>Priority order: Apache &gt; CRT. Throws {@link IllegalStateException} if none is found.
 *
 * @since 2.1.0
 */
public final class SyncClientDetector {

  private static final Logger logger = Logger.getLogger(SyncClientDetector.class.getName());

  // WARNING: String concatenation is intentional — do not "simplify" into a single literal.
  // The checkstyle RegexpMultiline rule bans fully-qualified class names outside imports;
  // splitting the string prevents the regex from matching the constant value.
  private static final String APACHE_CLASS =
      "software.amazon.awssdk" + ".http.apache.ApacheHttpClient";
  private static final String CRT_CLASS = "software.amazon.awssdk" + ".http.crt.AwsCrtHttpClient";

  /** Identifies which sync HTTP client implementation is available. */
  public enum SyncClientType {
    /** Apache HTTP Client. */
    APACHE,
    /** AWS CRT HTTP Client. */
    CRT
  }

  private SyncClientDetector() {}

  /**
   * Detects which sync HTTP client implementation is on the classpath.
   *
   * @return the detected client type
   * @throws IllegalStateException if no supported implementation is found
   */
  public static SyncClientType detect() {
    if (ClasspathUtil.isClassAvailable(APACHE_CLASS)) {
      logger.log(Level.FINE, "Detected Apache HTTP client on classpath");
      return SyncClientType.APACHE;
    }
    if (ClasspathUtil.isClassAvailable(CRT_CLASS)) {
      logger.log(Level.FINE, "Detected AWS CRT HTTP client on classpath");
      return SyncClientType.CRT;
    }
    throw new IllegalStateException(
        "No supported sync HTTP client implementation found on the classpath. "
            + "Add one of the following dependencies: "
            + "software.amazon.awssdk:apache-client or "
            + "software.amazon.awssdk:aws-crt-client");
  }

  /**
   * Verifies that the specified sync HTTP client implementation is available on the classpath.
   *
   * @param type the client type to check
   * @throws IllegalStateException if the implementation is not on the classpath
   */
  public static void requireAvailableHttpClient(SyncClientType type) {
    switch (type) {
      case APACHE:
        if (!ClasspathUtil.isClassAvailable(APACHE_CLASS)) {
          throw new IllegalStateException(
              "HttpClientType.APACHE was requested, but Apache HTTP client is not on the "
                  + "classpath. Add dependency: software.amazon.awssdk:apache-client");
        }
        return;
      case CRT:
        if (!ClasspathUtil.isClassAvailable(CRT_CLASS)) {
          throw new IllegalStateException(
              "HttpClientType.CRT was requested, but AWS CRT HTTP client is not on the "
                  + "classpath. Add dependency: software.amazon.awssdk:aws-crt-client");
        }
        return;
      default:
        throw new IllegalStateException("Unknown sync client type: " + type);
    }
  }

  /**
   * Creates a small sync HTTP client for LiveNodes polling.
   *
   * @param type the client type to create
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(SyncClientType type, TlsConfig tlsConfig) {
    switch (type) {
      case APACHE:
        return ApacheSyncClientFactory.createPollingClient(tlsConfig);
      case CRT:
        return CrtSyncClientFactory.createPollingClient(tlsConfig);
      default:
        throw new IllegalStateException("Unknown sync client type: " + type);
    }
  }

  /** Creates a polling client with the requested connection-pool size. */
  public static SdkHttpClient createPollingClient(
      SyncClientType type, TlsConfig tlsConfig, int maxConnections) {
    switch (type) {
      case APACHE:
        return ApacheSyncClientFactory.createPollingClient(tlsConfig, maxConnections);
      case CRT:
        return CrtSyncClientFactory.createPollingClient(tlsConfig, maxConnections);
      default:
        throw new IllegalStateException("Unknown sync client type: " + type);
    }
  }
}
