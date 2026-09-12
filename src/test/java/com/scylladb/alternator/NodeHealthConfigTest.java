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

import org.junit.Test;

public class NodeHealthConfigTest {
  @Test
  public void defaultConsecutiveFailureThresholdIsTen() {
    assertEquals(10, NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD);
    assertEquals(10, NodeHealthConfig.getDefault().getConsecutiveFailureThreshold());
    assertEquals(10, NodeHealthConfig.builder().build().getConsecutiveFailureThreshold());
  }

  @Test
  public void quarantineFailureThresholdDefaultsToThree() {
    assertEquals(3, NodeHealthConfig.DEFAULT_QUARANTINE_FAILURE_THRESHOLD);
    assertEquals(3, NodeHealthConfig.getDefault().getQuarantineFailureThreshold());
  }

  @Test
  public void quarantineFailureThresholdIsConfigurableAndNormalized() {
    assertEquals(
        5,
        NodeHealthConfig.builder()
            .withQuarantineFailureThreshold(5)
            .build()
            .getQuarantineFailureThreshold());
    assertEquals(
        1,
        NodeHealthConfig.builder()
            .withQuarantineFailureThreshold(0)
            .build()
            .getQuarantineFailureThreshold());
  }

  @Test
  public void allHealthThresholdsBelowOneNormalizeToOne() {
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(0)
            .withDownNodeRecoverySuccessThreshold(-1)
            .withQuarantineSuccessThreshold(Integer.MIN_VALUE)
            .withQuarantineFailureThreshold(0)
            .build();

    assertEquals(1, config.getConsecutiveFailureThreshold());
    assertEquals(1, config.getDownNodeRecoverySuccessThreshold());
    assertEquals(1, config.getQuarantineSuccessThreshold());
    assertEquals(1, config.getQuarantineFailureThreshold());
  }

  @Test
  public void quarantineTrafficIdlePeriodDefaultsToOneHundredMilliseconds() {
    assertEquals(100, NodeHealthConfig.DEFAULT_QUARANTINE_TRAFFIC_IDLE_PERIOD_MS);
    assertEquals(100, NodeHealthConfig.getDefault().getQuarantineTrafficIdlePeriodMs());
  }

  @Test
  public void quarantineTrafficIdlePeriodPreservesCustomAndNonPositiveValues() {
    assertEquals(
        250,
        NodeHealthConfig.builder()
            .withQuarantineTrafficIdlePeriodMs(250)
            .build()
            .getQuarantineTrafficIdlePeriodMs());
    assertEquals(
        0,
        NodeHealthConfig.builder()
            .withQuarantineTrafficIdlePeriodMs(0)
            .build()
            .getQuarantineTrafficIdlePeriodMs());
    assertEquals(
        -1,
        NodeHealthConfig.builder()
            .withQuarantineTrafficIdlePeriodMs(-1)
            .build()
            .getQuarantineTrafficIdlePeriodMs());
  }

  @Test
  public void downNodeProbePeriodPreservesPositiveValue() {
    assertEquals(
        250,
        NodeHealthConfig.builder()
            .withDownNodeProbePeriodMs(250)
            .build()
            .getDownNodeProbePeriodMs());
  }

  @Test
  public void downNodeProbePeriodRejectsZero() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> NodeHealthConfig.builder().withDownNodeProbePeriodMs(0).build());

    assertEquals("downNodeProbePeriodMs must be positive, but was: 0", failure.getMessage());
  }

  @Test
  public void downNodeProbePeriodRejectsNegativeValue() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> NodeHealthConfig.builder().withDownNodeProbePeriodMs(-1).build());

    assertEquals("downNodeProbePeriodMs must be positive, but was: -1", failure.getMessage());
  }

  @Test
  public void healthProbeConcurrencyDefaultsToFourAndIsConfigurable() {
    assertEquals(4, NodeHealthConfig.DEFAULT_HEALTH_PROBE_CONCURRENCY);
    assertEquals(4, NodeHealthConfig.getDefault().getHealthProbeConcurrency());
    assertEquals(
        8,
        NodeHealthConfig.builder()
            .withHealthProbeConcurrency(8)
            .build()
            .getHealthProbeConcurrency());
  }

  @Test
  public void healthProbeConcurrencyRejectsValuesOutsideRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> NodeHealthConfig.builder().withHealthProbeConcurrency(0).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> NodeHealthConfig.builder().withHealthProbeConcurrency(65).build());
  }

  @Test
  public void healthProbeTimeoutDefaultsToFiveSecondsAndMustBePositive() {
    assertEquals(5_000, NodeHealthConfig.DEFAULT_HEALTH_PROBE_TIMEOUT_MS);
    assertEquals(5_000, NodeHealthConfig.getDefault().getHealthProbeTimeoutMs());
    assertEquals(
        250,
        NodeHealthConfig.builder().withHealthProbeTimeoutMs(250).build().getHealthProbeTimeoutMs());
    assertThrows(
        IllegalArgumentException.class,
        () -> NodeHealthConfig.builder().withHealthProbeTimeoutMs(0).build());
  }
}
