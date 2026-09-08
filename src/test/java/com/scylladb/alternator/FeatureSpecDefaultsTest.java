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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.junit.Test;

/** Executes the machine-readable feature defaults. */
public class FeatureSpecDefaultsTest {
  private static final Path DEFAULTS = Paths.get("feature-specs", "vectors", "defaults.tsv");

  @Test
  @CoversRequirements("COMP-REQ-001")
  public void compressionDefaultsMatchSpecification() throws Exception {
    Map<String, String> expected =
        defaults(
            "COMP-REQ-001",
            "request_algorithm",
            "minimum_request_size_bytes",
            "response_algorithm_count");
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertEquals(expected.get("request_algorithm"), config.getCompressionAlgorithm().name());
    assertEquals(
        Integer.parseInt(expected.get("minimum_request_size_bytes")),
        config.getMinCompressionSizeBytes());
    assertEquals(
        Integer.parseInt(expected.get("response_algorithm_count")),
        config.getResponseCompressionAlgorithms().size());
  }

  @Test
  @CoversRequirements("HEAD-REQ-001")
  public void headerOptimizationDefaultMatchesSpecification() throws Exception {
    Map<String, String> expected = defaults("HEAD-REQ-001", "header_optimization_enabled");
    assertEquals(
        Boolean.parseBoolean(expected.get("header_optimization_enabled")),
        AlternatorConfig.builder().build().isOptimizeHeaders());
  }

  @Test
  @CoversRequirements("AFF-REQ-001")
  public void affinityDefaultMatchesSpecification() throws Exception {
    Map<String, String> expected = defaults("AFF-REQ-001", "affinity_enabled");
    AlternatorConfig config = AlternatorConfig.builder().build();
    boolean enabled =
        config.getKeyRouteAffinityConfig() != null
            && config.getKeyRouteAffinityConfig().isEnabled();
    assertEquals(Boolean.parseBoolean(expected.get("affinity_enabled")), enabled);
  }

  @Test
  @CoversRequirements("HEALTH-REQ-001")
  public void nodeHealthDefaultsMatchSpecification() throws Exception {
    Map<String, String> expected =
        defaults(
            "HEALTH-REQ-001",
            "active_failure_threshold",
            "down_recovery_threshold",
            "quarantine_promotion_threshold",
            "quarantine_failure_threshold",
            "background_probe_period_ms",
            "probe_concurrency",
            "probe_timeout_ms",
            "health_disabled");
    NodeHealthConfig config = NodeHealthConfig.getDefault();

    assertEquals(
        Integer.parseInt(expected.get("active_failure_threshold")),
        config.getConsecutiveFailureThreshold());
    assertEquals(
        Integer.parseInt(expected.get("down_recovery_threshold")),
        config.getDownNodeRecoverySuccessThreshold());
    assertEquals(
        Integer.parseInt(expected.get("quarantine_promotion_threshold")),
        config.getQuarantineSuccessThreshold());
    assertEquals(
        Integer.parseInt(expected.get("quarantine_failure_threshold")),
        config.getQuarantineFailureThreshold());
    assertEquals(
        Long.parseLong(expected.get("background_probe_period_ms")),
        config.getDownNodeProbePeriodMs());
    assertEquals(
        Integer.parseInt(expected.get("probe_concurrency")), config.getHealthProbeConcurrency());
    assertEquals(
        Long.parseLong(expected.get("probe_timeout_ms")), config.getHealthProbeTimeoutMs());
    assertEquals(Boolean.parseBoolean(expected.get("health_disabled")), config.isDisabled());
  }

  private static Map<String, String> defaults(String requirement, String... expectedSettings)
      throws Exception {
    Map<String, String> values = new LinkedHashMap<>();
    for (String line : Files.readAllLines(DEFAULTS, StandardCharsets.UTF_8)) {
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] fields = line.split("\\t", -1);
      if (requirement.equals(fields[0])) {
        values.put(fields[1], fields[2]);
      }
    }
    assertEquals(new LinkedHashSet<>(Arrays.asList(expectedSettings)), values.keySet());
    return values;
  }
}
