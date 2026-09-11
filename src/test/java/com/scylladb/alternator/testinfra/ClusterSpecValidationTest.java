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
package com.scylladb.alternator.testinfra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.scylladb.alternator.CoversRequirements;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

/** Focused validation tests for typed cluster specifications. */
public class ClusterSpecValidationTest {
  private static final Set<String> VALID_SECURITY_CONFIGURATIONS =
      Set.of(
          "false|ALLOW_ALL|ALLOW_ALL",
          "false|PASSWORD|ALLOW_ALL",
          "false|PASSWORD|CASSANDRA",
          "false|PASSWORD|TRANSITIONAL",
          "false|TRANSITIONAL|ALLOW_ALL",
          "false|TRANSITIONAL|CASSANDRA",
          "false|TRANSITIONAL|TRANSITIONAL",
          "true|PASSWORD|CASSANDRA");

  @Test
  public void rejectsTopologyNodeCountsThatOverflowInteger() {
    assertEquals(
        Integer.MAX_VALUE, ClusterTopology.singleDatacenter(Integer.MAX_VALUE).nodeCount());

    ClusterTopology overflowing =
        ClusterTopology.singleDatacenter(Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertThrows(IllegalArgumentException.class, () -> new ClusterSpec().withTopology(overflowing));
  }

  @Test
  @CoversRequirements("CCM-REQ-001")
  public void validatesEverySecurityModeCombination() {
    for (boolean enforced : new boolean[] {false, true}) {
      for (AuthenticationMode authentication : AuthenticationMode.values()) {
        for (AuthorizationMode authorization : AuthorizationMode.values()) {
          String configuration = securityKey(enforced, authentication, authorization);
          boolean expectedValid = VALID_SECURITY_CONFIGURATIONS.contains(configuration);
          try {
            ClusterSecuritySpec actual =
                new ClusterSecuritySpec(authentication, authorization, enforced);
            if (!expectedValid) {
              fail("Accepted invalid security configuration " + configuration);
            }
            assertEquals(authentication, actual.authentication());
            assertEquals(authorization, actual.authorization());
            assertEquals(enforced, actual.enforceAlternatorAuthorization());
          } catch (IllegalArgumentException rejected) {
            if (expectedValid) {
              throw new AssertionError(
                  "Rejected valid security configuration " + configuration, rejected);
            }
          }
        }
      }
    }
  }

  @Test
  public void canonicalizesOuterUnicodeWhitespaceForStorageAndReuse() {
    ClusterSpec canonical = new ClusterSpec().withYamlOverride("custom_option", "true");
    ClusterSpec padded =
        new ClusterSpec().withYamlOverride("\u2003\u00a0custom_option\u3000", "true");

    assertEquals(Map.of("custom_option", "true"), padded.scyllaYamlOverrides());
    assertEquals(canonical.reuseKey(), padded.reuseKey());
  }

  @Test
  public void canonicalAliasesReplaceOneStoredSetting() {
    ClusterSpec spec =
        new ClusterSpec()
            .withYamlOverride("custom_option", "first")
            .withYamlOverride("\u2003custom_option\u00a0", "second");
    ClusterSpec canonical = new ClusterSpec().withYamlOverride("custom_option", "second");

    assertEquals(Map.of("custom_option", "second"), spec.scyllaYamlOverrides());
    assertEquals(canonical.reuseKey(), spec.reuseKey());
  }

  @Test
  public void reservedYamlRootsCannotBeDisguised() {
    for (String key :
        Arrays.asList(
            "alternator_enforce_authorization ",
            "\u00a0alternator_enforce_authorization\u2003",
            " \u2003alternator_encryption_options.enabled\u3000",
            "api_address",
            "api_port",
            "auto_bootstrap",
            "initial_token",
            "listen_interface",
            "listen_interface_prefer_ipv6",
            "listen_on_broadcast_address",
            "num_tokens",
            "storage_port",
            "native_transport_port",
            "native_shard_aware_transport_port",
            "native_shard_aware_transport_port_ssl",
            "rpc_interface",
            "rpc_interface_prefer_ipv6",
            "data_file_directories",
            "commitlog_directory",
            "smp",
            "memory")) {
      assertThrows(
          "Accepted reserved YAML key " + printable(key),
          IllegalArgumentException.class,
          () -> new ClusterSpec().withYamlOverride(key, "false"));
    }
  }

  @Test
  public void scyllaAliasesCannotBypassOwnedYamlRoots() {
    Map.of("datadir", "data_file_directories", "cql_port", "native_transport_port")
        .forEach(
            (alias, canonical) -> {
              IllegalArgumentException failure =
                  assertThrows(
                      IllegalArgumentException.class,
                      () -> new ClusterSpec().withYamlOverride(alias, "false"));
              assertTrue(
                  failure.getMessage(), failure.getMessage().contains("'" + canonical + "'"));
            });
  }

  @Test
  public void additionalOwnedYamlRootsCannotBeOverridden() {
    for (String key :
        Arrays.asList(
            "commitlog_use_o_dsync",
            "default_log_level",
            "ignore_dead_nodes_for_replace",
            "join_ring",
            "load_ring_state",
            "log_to_stdout",
            "maintenance_mode",
            "maintenance_socket",
            "maintenance_socket_group",
            "redis_port",
            "redis_ssl_port",
            "replace_address",
            "replace_address_first_boot",
            "replace_node_first_boot",
            "role_manager",
            "server_encryption_options")) {
      assertThrows(
          "Accepted behavior owned by a typed cluster option " + printable(key),
          IllegalArgumentException.class,
          () -> new ClusterSpec().withYamlOverride(key, "false"));
    }
  }

  @Test
  public void rejectsUnsafeOrUnsupportedYamlKeys() {
    for (String key :
        Arrays.asList(
            null,
            "",
            " ",
            "\u200b",
            ".option",
            "option.",
            "parent..child",
            "parent.child.grandchild",
            "option:value",
            "option\ninjected",
            "\toption",
            "option-name",
            "option name",
            "na\u00efve",
            "1option")) {
      assertThrows(
          "Accepted unsafe YAML key " + printable(key),
          IllegalArgumentException.class,
          () -> new ClusterSpec().withYamlOverride(key, "true"));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new ClusterSpec().withYamlOverride("custom_option", "\u2003\u00a0"));
  }

  @Test
  public void acceptsSafeTopLevelAndNestedYamlKeys() {
    ClusterSpec spec =
        new ClusterSpec()
            .withYamlOverride("_custom2", "true")
            .withYamlOverride("custom_group.child_2", "42");

    assertEquals(
        Map.of("_custom2", "true", "custom_group.child_2", "42"), spec.scyllaYamlOverrides());
  }

  @Test
  public void rejectsNestedYamlOverrideBelowScalarRootBeforeProvisioning() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new ClusterSpec()
                    .withYamlOverride("custom_group", "5")
                    .withYamlOverride("custom_group.child", "42"));

    assertTrue(failure.getMessage(), failure.getMessage().contains("must be a mapping"));

    new ClusterSpec()
        .withYamlOverride("custom_group", "{existing: true}")
        .withYamlOverride("custom_group.child", "42");
  }

  @Test
  public void rejectsInvalidYamlValuesBeforeProvisioning() {
    for (String value : Arrays.asList("[", "{key: value", "'unterminated", "a: 1\na: 2")) {
      assertThrows(
          "Accepted invalid YAML value " + printable(value),
          IllegalArgumentException.class,
          () -> new ClusterSpec().withYamlOverride("custom_option", value));
    }

    new ClusterSpec()
        .withYamlOverride("custom_null", "null")
        .withYamlOverride("custom_list", "[one, two]")
        .withYamlOverride("custom_mapping", "{one: 1, two: false}");
  }

  @Test
  public void reuseIdentityCannotBeConfusedByVersionDelimiters() {
    ClusterSpec first =
        new ClusterSpec()
            .withScyllaVersion(
                "X|[HTTP, HTTPS]|ALLOW_ALL|ALLOW_ALL|false|2|1024|dc:3|yaml:16:logger_log_level=59:info # ");
    ClusterSpec second =
        new ClusterSpec()
            .withScyllaVersion("X")
            .withYamlOverride(
                "logger_log_level", "info # |[HTTP, HTTPS]|ALLOW_ALL|ALLOW_ALL|false|2|1024|dc:3");

    org.junit.Assert.assertNotEquals(first.reuseKey(), second.reuseKey());
  }

  private static String securityKey(
      boolean enforced, AuthenticationMode authentication, AuthorizationMode authorization) {
    return enforced + "|" + authentication + "|" + authorization;
  }

  private static String printable(String value) {
    return value == null ? "<null>" : '"' + value + '"';
  }
}
