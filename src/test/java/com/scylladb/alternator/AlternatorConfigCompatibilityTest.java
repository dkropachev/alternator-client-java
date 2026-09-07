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
