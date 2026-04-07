package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for KeyRouteAffinityConfig.
 *
 * @author dmitry.kropachev
 */
public class KeyRouteAffinityConfigTest {

  @Test
  public void testDefaultBuilder() {
    KeyRouteAffinityConfig config = KeyRouteAffinityConfig.builder().build();

    assertEquals(KeyRouteAffinity.NONE, config.getType());
    assertTrue(config.getPkInfoPerTable().isEmpty());
    assertFalse(config.isEnabled());
  }

  @Test
  public void testBuilderWithType() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder().withType(KeyRouteAffinity.RMW).build();

    assertEquals(KeyRouteAffinity.RMW, config.getType());
    assertTrue(config.isEnabled());
  }

  @Test
  public void testBuilderWithTypeAnyWrite() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder().withType(KeyRouteAffinity.ANY_WRITE).build();

    assertEquals(KeyRouteAffinity.ANY_WRITE, config.getType());
    assertTrue(config.isEnabled());
  }

  @Test
  public void testBuilderWithTypeNone() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder().withType(KeyRouteAffinity.NONE).build();

    assertEquals(KeyRouteAffinity.NONE, config.getType());
    assertFalse(config.isEnabled());
  }

  @Test
  public void testBuilderWithNullType() {
    KeyRouteAffinityConfig config = KeyRouteAffinityConfig.builder().withType(null).build();

    assertEquals(KeyRouteAffinity.NONE, config.getType());
    assertFalse(config.isEnabled());
  }

  @Test
  public void testBuilderWithPkInfo() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("users", "user_id")
            .withPkInfo("orders", "order_id")
            .build();

    Map<String, String> pkInfo = config.getPkInfoPerTable();
    assertEquals(2, pkInfo.size());
    assertEquals("user_id", pkInfo.get("users"));
    assertEquals("order_id", pkInfo.get("orders"));
  }

  @Test
  public void testBuilderWithPkInfoMap() {
    Map<String, String> pkInfoMap = new HashMap<>();
    pkInfoMap.put("table1", "pk1");
    pkInfoMap.put("table2", "pk2");
    pkInfoMap.put("table3", "pk3");

    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfoMap(pkInfoMap)
            .build();

    Map<String, String> pkInfo = config.getPkInfoPerTable();
    assertEquals(3, pkInfo.size());
    assertEquals("pk1", pkInfo.get("table1"));
    assertEquals("pk2", pkInfo.get("table2"));
    assertEquals("pk3", pkInfo.get("table3"));
  }

  @Test
  public void testBuilderWithNullPkInfo() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo(null, "pk")
            .withPkInfo("table", null)
            .withPkInfoMap(null)
            .build();

    assertTrue(config.getPkInfoPerTable().isEmpty());
  }

  @Test
  public void testStaticOfMethod() {
    KeyRouteAffinityConfig config = KeyRouteAffinityConfig.of(KeyRouteAffinity.RMW);

    assertEquals(KeyRouteAffinity.RMW, config.getType());
    assertTrue(config.getPkInfoPerTable().isEmpty());
    assertTrue(config.isEnabled());
  }

  @Test
  public void testStaticOfMethodWithNone() {
    KeyRouteAffinityConfig config = KeyRouteAffinityConfig.of(KeyRouteAffinity.NONE);

    assertEquals(KeyRouteAffinity.NONE, config.getType());
    assertFalse(config.isEnabled());
  }

  @Test
  public void testStaticOfMethodWithNull() {
    KeyRouteAffinityConfig config = KeyRouteAffinityConfig.of(null);

    assertEquals(KeyRouteAffinity.NONE, config.getType());
    assertFalse(config.isEnabled());
  }

  @Test
  public void testPkInfoMapIsUnmodifiable() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo("users", "user_id")
            .build();

    try {
      config.getPkInfoPerTable().put("new_table", "new_pk");
      fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testBuilderPkInfoMerge() {
    Map<String, String> pkInfoMap = new HashMap<>();
    pkInfoMap.put("table1", "pk1");
    pkInfoMap.put("table2", "pk2");

    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withPkInfo("table0", "pk0")
            .withPkInfoMap(pkInfoMap)
            .withPkInfo("table3", "pk3")
            .build();

    Map<String, String> pkInfo = config.getPkInfoPerTable();
    assertEquals(4, pkInfo.size());
    assertEquals("pk0", pkInfo.get("table0"));
    assertEquals("pk1", pkInfo.get("table1"));
    assertEquals("pk2", pkInfo.get("table2"));
    assertEquals("pk3", pkInfo.get("table3"));
  }

  @Test
  public void testBuilderPkInfoOverwrite() {
    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withPkInfo("users", "old_pk")
            .withPkInfo("users", "new_pk")
            .build();

    assertEquals("new_pk", config.getPkInfoPerTable().get("users"));
  }

  @Test
  public void testIsEnabledForAllTypes() {
    assertFalse(KeyRouteAffinityConfig.of(KeyRouteAffinity.NONE).isEnabled());
    assertTrue(KeyRouteAffinityConfig.of(KeyRouteAffinity.RMW).isEnabled());
    assertTrue(KeyRouteAffinityConfig.of(KeyRouteAffinity.ANY_WRITE).isEnabled());
  }

  @Test
  public void testMetricsAndLoggingDefaults() {
    KeyRouteAffinityConfig config = KeyRouteAffinityConfig.builder().build();

    assertFalse(config.isMetricsEnabled());
    assertFalse(config.isDebugLoggingEnabled());
    assertSame(AffinityMetricsCallback.NO_OP, config.getMetricsCallback());
  }

  @Test
  public void testBuilderWithMetricsAndLogging() {
    AffinityMetricsCallback callback = new AffinityMetricsCallback() {};

    KeyRouteAffinityConfig config =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withMetricsEnabled(true)
            .withDebugLoggingEnabled(true)
            .withMetricsCallback(callback)
            .build();

    assertTrue(config.isMetricsEnabled());
    assertTrue(config.isDebugLoggingEnabled());
    assertSame(callback, config.getMetricsCallback());
  }
}
