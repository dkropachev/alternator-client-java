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

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.routing.ClusterScope;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

public class AlternatorConfigCompatibilityTest {
  @Test
  public void legacyNextAsUriMethodsRemainDeprecatedPublicApi() throws Exception {
    assertDeprecatedMethod(AlternatorDynamoDbClientWrapper.class, "nextAsURI");
    assertDeprecatedMethod(AlternatorDynamoDbAsyncClientWrapper.class, "nextAsURI");
    assertDeprecatedMethod(AlternatorLiveNodes.class, "nextAsURI");
    assertDeprecatedMethod(AlternatorLiveNodes.class, "nextAsURI", String.class, String.class);
  }

  @Test
  public void builderWithoutNodeHealthConfigUsesDefaultNodeHealthConfig() {
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://localhost:8000")).build();

    assertNotNull(config.getNodeHealthConfig());
    assertEquals(
        NodeHealthConfig.DEFAULT_DOWN_NODE_PROBE_PERIOD_MS,
        config.getNodeHealthConfig().getDownNodeProbePeriodMs());
    assertFalse(config.getNodeHealthConfig().isDisabled());
  }

  @Test
  public void legacyProtectedConstructorUsesDefaultNodeHealthConfig() {
    AlternatorConfig config = new LegacyAlternatorConfig();

    assertNotNull(config.getNodeHealthConfig());
    assertEquals(
        NodeHealthConfig.DEFAULT_DOWN_NODE_PROBE_PERIOD_MS,
        config.getNodeHealthConfig().getDownNodeProbePeriodMs());
    assertFalse(config.getNodeHealthConfig().isDisabled());
  }

  private static void assertDeprecatedMethod(
      Class<?> declaringClass, String methodName, Class<?>... parameterTypes) throws Exception {
    assertTrue(
        declaringClass.getMethod(methodName, parameterTypes).isAnnotationPresent(Deprecated.class));
  }

  private static final class LegacyAlternatorConfig extends AlternatorConfig {
    private LegacyAlternatorConfig() {
      super(
          Collections.singletonList("localhost"),
          "http",
          8000,
          ClusterScope.create(),
          RequestCompressionAlgorithm.NONE,
          AlternatorConfig.DEFAULT_MIN_COMPRESSION_SIZE_BYTES,
          Collections.<ResponseCompressionAlgorithm>emptyList(),
          false,
          null,
          true,
          true,
          null,
          null,
          null,
          AlternatorConfig.DEFAULT_ACTIVE_REFRESH_INTERVAL_MS,
          AlternatorConfig.DEFAULT_IDLE_REFRESH_INTERVAL_MS,
          AlternatorConfig.DEFAULT_MAX_CONNECTIONS,
          AlternatorConfig.DEFAULT_CONNECTION_MAX_IDLE_TIME_MS,
          AlternatorConfig.DEFAULT_CONNECTION_TIME_TO_LIVE_MS,
          AlternatorConfig.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MS,
          AlternatorConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    }
  }
}
