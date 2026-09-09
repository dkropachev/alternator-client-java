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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.schema.CoreSchema;

/** Immutable, typed description of a CCM-provisioned Scylla cluster. */
public final class ClusterSpec {
  private static final Pattern YAML_KEY_PATTERN =
      Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)?");
  private static final Set<String> RESERVED_YAML_KEYS =
      Set.of(
          "alternator_address",
          "alternator_encryption_options",
          "alternator_enforce_authorization",
          "alternator_https_port",
          "alternator_port",
          "alternator_write_isolation",
          "api_address",
          "api_port",
          "auth_superuser_name",
          "auth_superuser_salted_password",
          "authenticator",
          "auto_bootstrap",
          "authorizer",
          "broadcast_address",
          "broadcast_rpc_address",
          "cluster_name",
          "commitlog_directory",
          "data_file_directories",
          "developer_mode",
          "endpoint_snitch",
          "hints_directory",
          "initial_token",
          "listen_address",
          "listen_interface",
          "listen_interface_prefer_ipv6",
          "listen_on_broadcast_address",
          "memory",
          "native_shard_aware_transport_port",
          "native_shard_aware_transport_port_proxy_protocol",
          "native_shard_aware_transport_port_ssl",
          "native_shard_aware_transport_port_ssl_proxy_protocol",
          "native_transport_port",
          "native_transport_port_ssl",
          "num_tokens",
          "partitioner",
          "prometheus_address",
          "prometheus_port",
          "rpc_address",
          "rpc_interface",
          "rpc_interface_prefer_ipv6",
          "rpc_port",
          "saved_caches_directory",
          "schema_commitlog_directory",
          "seeds",
          "seed_provider",
          "smp",
          "ssl_storage_port",
          "start_native_transport",
          "storage_port",
          "view_hints_directory",
          "workdir");
  private static final LoadSettings YAML_SETTINGS =
      LoadSettings.builder().setAllowDuplicateKeys(false).setSchema(new CoreSchema()).build();

  private final String scyllaVersion;
  private final ClusterTopology topology;
  private final Set<AlternatorTransport> transports;
  private final ClusterSecuritySpec security;
  private final NodeResources resources;
  private final Map<String, String> scyllaYamlOverrides;

  public ClusterSpec() {
    this(
        "release:2025.2",
        ClusterTopology.singleDatacenter(3),
        EnumSet.allOf(AlternatorTransport.class),
        ClusterSecuritySpec.DISABLED,
        NodeResources.DEFAULT,
        Collections.emptyMap());
  }

  private ClusterSpec(
      String scyllaVersion,
      ClusterTopology topology,
      Set<AlternatorTransport> transports,
      ClusterSecuritySpec security,
      NodeResources resources,
      Map<String, String> scyllaYamlOverrides) {
    this.scyllaVersion = Objects.requireNonNull(scyllaVersion, "scyllaVersion");
    this.topology = Objects.requireNonNull(topology, "topology");
    this.transports = Collections.unmodifiableSet(EnumSet.copyOf(transports));
    this.security = Objects.requireNonNull(security, "security");
    this.resources = Objects.requireNonNull(resources, "resources");
    this.scyllaYamlOverrides = canonicalizeYamlOverrides(scyllaYamlOverrides);
    validate();
  }

  public String scyllaVersion() {
    return scyllaVersion;
  }

  public ClusterTopology topology() {
    return topology;
  }

  public Set<AlternatorTransport> transports() {
    return transports;
  }

  public ClusterSecuritySpec security() {
    return security;
  }

  public NodeResources resources() {
    return resources;
  }

  public Map<String, String> scyllaYamlOverrides() {
    return scyllaYamlOverrides;
  }

  public ClusterSpec withScyllaVersion(String value) {
    return copy(value, topology, transports, security, resources, scyllaYamlOverrides);
  }

  public ClusterSpec withTopology(ClusterTopology value) {
    return copy(scyllaVersion, value, transports, security, resources, scyllaYamlOverrides);
  }

  public ClusterSpec withTransports(AlternatorTransport... values) {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("At least one Alternator transport is required");
    }
    EnumSet<AlternatorTransport> selected = EnumSet.noneOf(AlternatorTransport.class);
    Collections.addAll(selected, values);
    return copy(scyllaVersion, topology, selected, security, resources, scyllaYamlOverrides);
  }

  public ClusterSpec withSecurity(ClusterSecuritySpec value) {
    return copy(scyllaVersion, topology, transports, value, resources, scyllaYamlOverrides);
  }

  public ClusterSpec withResources(NodeResources value) {
    return copy(scyllaVersion, topology, transports, security, value, scyllaYamlOverrides);
  }

  public ClusterSpec withYamlOverride(String key, String yamlValue) {
    Map<String, String> overrides = new TreeMap<>(scyllaYamlOverrides);
    overrides.put(canonicalizeYamlKey(key), yamlValue);
    return copy(scyllaVersion, topology, transports, security, resources, overrides);
  }

  String reuseKey() {
    StringBuilder key =
        new StringBuilder()
            .append(scyllaVersion.length())
            .append(':')
            .append(scyllaVersion)
            .append('|')
            .append(transports)
            .append('|')
            .append(security.authentication())
            .append('|')
            .append(security.authorization())
            .append('|')
            .append(security.enforceAlternatorAuthorization())
            .append('|')
            .append(resources.smp())
            .append('|')
            .append(resources.memoryMiB());
    for (DatacenterSpec datacenter : topology.datacenters()) {
      key.append("|dc");
      for (RackSpec rack : datacenter.racks()) {
        key.append(':').append(rack.nodeCount());
      }
    }
    for (Map.Entry<String, String> option : scyllaYamlOverrides.entrySet()) {
      key.append("|yaml:")
          .append(option.getKey().length())
          .append(':')
          .append(option.getKey())
          .append('=')
          .append(option.getValue().length())
          .append(':')
          .append(option.getValue());
    }
    return key.toString();
  }

  void validate() {
    if (scyllaVersion.trim().isEmpty()) {
      throw new IllegalArgumentException("A Scylla version is required");
    }
    if (transports.isEmpty()) {
      throw new IllegalArgumentException("At least one Alternator transport is required");
    }
    if (topology.nodeCount() > ClusterCapacity.MAXIMUM_NODE_COUNT) {
      throw new IllegalArgumentException(
          "A cluster cannot exceed " + ClusterCapacity.MAXIMUM_NODE_COUNT + " nodes");
    }
    security.validate();
    for (Map.Entry<String, String> option : scyllaYamlOverrides.entrySet()) {
      String value = option.getValue();
      if (value == null || stripUnicodeWhitespace(value).isEmpty()) {
        throw new IllegalArgumentException("Scylla YAML override keys and values cannot be empty");
      }
      try {
        parseYamlValue(value);
      } catch (RuntimeException exception) {
        throw new IllegalArgumentException(
            "Scylla YAML override '" + option.getKey() + "' has an invalid YAML value", exception);
      }
    }
  }

  private static Map<String, String> canonicalizeYamlOverrides(Map<String, String> overrides) {
    Objects.requireNonNull(overrides, "scyllaYamlOverrides");
    Map<String, String> canonical = new TreeMap<>();
    for (Map.Entry<String, String> option : overrides.entrySet()) {
      canonical.put(canonicalizeYamlKey(option.getKey()), option.getValue());
    }
    return Collections.unmodifiableMap(canonical);
  }

  private static String canonicalizeYamlKey(String key) {
    if (key == null) {
      throw new IllegalArgumentException("A Scylla YAML override key cannot be null");
    }
    for (int offset = 0; offset < key.length(); ) {
      int codePoint = key.codePointAt(offset);
      if (Character.isISOControl(codePoint)) {
        throw new IllegalArgumentException("Scylla YAML override keys cannot contain controls");
      }
      offset += Character.charCount(codePoint);
    }

    String canonical = stripUnicodeWhitespace(key);
    if (!YAML_KEY_PATTERN.matcher(canonical).matches()) {
      throw new IllegalArgumentException(
          "A Scylla YAML override key must contain one or two ASCII identifier segments");
    }

    int separator = canonical.indexOf('.');
    String rootKey = separator < 0 ? canonical : canonical.substring(0, separator);
    if (RESERVED_YAML_KEYS.contains(rootKey)) {
      throw new IllegalArgumentException(
          "Scylla YAML key '" + rootKey + "' is owned by a typed cluster option");
    }
    return canonical;
  }

  private static String stripUnicodeWhitespace(String value) {
    int start = 0;
    int end = value.length();
    while (start < end) {
      int codePoint = value.codePointAt(start);
      if (!isUnicodeWhitespace(codePoint)) {
        break;
      }
      start += Character.charCount(codePoint);
    }
    while (start < end) {
      int codePoint = value.codePointBefore(end);
      if (!isUnicodeWhitespace(codePoint)) {
        break;
      }
      end -= Character.charCount(codePoint);
    }
    return value.substring(start, end);
  }

  private static boolean isUnicodeWhitespace(int codePoint) {
    return Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint);
  }

  static Object parseYamlValue(String yaml) {
    return new Load(YAML_SETTINGS).loadFromString(yaml);
  }

  private static ClusterSpec copy(
      String scyllaVersion,
      ClusterTopology topology,
      Set<AlternatorTransport> transports,
      ClusterSecuritySpec security,
      NodeResources resources,
      Map<String, String> overrides) {
    return new ClusterSpec(scyllaVersion, topology, transports, security, resources, overrides);
  }
}
