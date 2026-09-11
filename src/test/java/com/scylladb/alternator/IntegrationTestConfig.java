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

import com.scylladb.alternator.testinfra.AlternatorConnection;
import com.scylladb.alternator.testinfra.AlternatorTransport;
import com.scylladb.alternator.testinfra.ClusterSpecs;
import com.scylladb.alternator.testinfra.ReusableClusterLease;
import com.scylladb.alternator.testinfra.TestClusters;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Centralized configuration for integration tests, reading all settings from environment variables
 * in one place.
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>{@code INTEGRATION_TESTS} - Set to "true" to enable integration tests
 *   <li>{@code SCYLLA_VERSION} - CCM Scylla package selector (default: release:2025.2.5)
 *   <li>{@code SCYLLA_CCM_PATH} - CCM executable selected by the Makefile
 *   <li>{@code SCYLLA_CCM_ROOT} - Optional private root for runs and address reservations
 *   <li>{@code SCYLLA_CCM_DIAGNOSTICS_DIR} - External diagnostic artifact directory
 *   <li>{@code SCYLLA_CCM_MAX_NODES} - Optional lower override for the nine-node ceiling
 * </ul>
 */
public final class IntegrationTestConfig {
  public static final boolean ENABLED =
      Boolean.parseBoolean(System.getenv().getOrDefault("INTEGRATION_TESTS", "false"));

  public static final String HOST;
  public static final int HTTP_PORT;
  public static final int HTTPS_PORT;
  public static final String DATACENTER;
  public static final String RACK;
  public static final Path CA_CERT_PATH;
  public static final URI HTTP_SEED_URI;
  public static final URI HTTPS_SEED_URI;
  public static final StaticCredentialsProvider CREDENTIALS;

  private static final ReusableClusterLease SUITE_CLUSTER;

  static {
    if (ENABLED) {
      try {
        SUITE_CLUSTER = TestClusters.acquireReusable(ClusterSpecs.defaultSpec());
        AlternatorConnection http = SUITE_CLUSTER.cluster().connection(AlternatorTransport.HTTP);
        AlternatorConnection https = SUITE_CLUSTER.cluster().connection(AlternatorTransport.HTTPS);
        HTTP_SEED_URI = http.seedEndpoint();
        HTTPS_SEED_URI = https.seedEndpoint();
        HOST = HTTP_SEED_URI.getHost();
        HTTP_PORT = HTTP_SEED_URI.getPort();
        HTTPS_PORT = HTTPS_SEED_URI.getPort();
        DATACENTER = SUITE_CLUSTER.cluster().nodes().get(0).datacenter();
        RACK = SUITE_CLUSTER.cluster().nodes().get(0).rack();
        CA_CERT_PATH = https.caCertificatePath();
        CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
      } catch (Exception exception) {
        throw new ExceptionInInitializerError(exception);
      }
    } else {
      SUITE_CLUSTER = null;
      HOST = "127.0.0.1";
      HTTP_PORT = 8080;
      HTTPS_PORT = 8043;
      DATACENTER = "dc1";
      RACK = "RAC1";
      CA_CERT_PATH = null;
      HTTP_SEED_URI = URI.create("http://" + HOST + ":" + HTTP_PORT);
      HTTPS_SEED_URI = URI.create("https://" + HOST + ":" + HTTPS_PORT);
      CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
    }
  }

  private IntegrationTestConfig() {}

  /** Returns parameterized data for {@code @Parameters} — one entry per protocol (HTTP, HTTPS). */
  public static Collection<Object[]> httpAndHttpsEndpoints() {
    return Arrays.asList(
        new Object[] {"http", HTTP_SEED_URI}, new Object[] {"https", HTTPS_SEED_URI});
  }
}
