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

import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Unit tests for AlternatorConfig headers optimization configuration.
 *
 * @author dmitry.kropachev
 */
public class AlternatorConfigHeadersTest {

  @Test
  public void testDefaultHeadersOptimizationSettings() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertFalse(config.isOptimizeHeaders());
    // Without compression, Content-Encoding is not required
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersOptimizationEnabled() {
    AlternatorConfig config = AlternatorConfig.builder().withOptimizeHeaders(true).build();

    assertTrue(config.isOptimizeHeaders());
    // Without compression, Content-Encoding is not required
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testCustomHeadersWhitelist() {
    // Get required headers first, then add custom ones
    Set<String> requiredHeaders = AlternatorConfig.builder().getRequiredHeaders();
    Set<String> customHeaders = new HashSet<>(requiredHeaders);
    customHeaders.add("X-Custom");

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    assertTrue(config.isOptimizeHeaders());
    assertEquals(customHeaders, config.getHeadersWhitelist());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullHeadersWhitelistThrowsException() {
    AlternatorConfig.builder().withHeadersWhitelist(null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyHeadersWhitelistThrowsException() {
    AlternatorConfig.builder().withHeadersWhitelist(Arrays.asList()).build();
  }

  @Test
  public void testHeadersWhitelistIsImmutable() {
    // Get required headers first, then add custom ones
    Set<String> requiredHeaders = AlternatorConfig.builder().getRequiredHeaders();
    Set<String> customHeaders = new HashSet<>(requiredHeaders);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    // Modifying the original set should not affect the config
    customHeaders.add("X-Modified");
    assertFalse(config.getHeadersWhitelist().contains("X-Modified"));
  }

  @Test
  public void testHeadersOptimizationWithRoutingScope() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withRoutingScope(
                RackScope.of(
                    "us-east", "rack1", DatacenterScope.of("us-east", ClusterScope.create())))
            .withOptimizeHeaders(true)
            .build();

    assertEquals("Rack", config.getRoutingScope().getName());
    assertTrue(config.isOptimizeHeaders());
    // Without compression, Content-Encoding is not required
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  @CoversRequirements("COMP-REQ-007")
  public void testHeadersOptimizationWithCompression() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withOptimizeHeaders(true)
            .build();

    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertTrue(config.isOptimizeHeaders());
    // With compression, Content-Encoding should be included
    assertTrue(config.getHeadersWhitelist().contains("Content-Encoding"));
    assertTrue(config.getRequiredHeaders().contains("Content-Encoding"));
  }

  @Test
  public void testDefaultWithoutHeadersOptimization() {
    // Existing code without headers settings should still work
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    assertEquals("Rack", config.getRoutingScope().getName());
    // Headers optimization defaults to disabled
    assertFalse(config.isOptimizeHeaders());
    // Default whitelist is based on current config (no compression, with auth)
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testExplicitlyDisableHeadersOptimization() {
    AlternatorConfig config = AlternatorConfig.builder().withOptimizeHeaders(false).build();

    assertFalse(config.isOptimizeHeaders());
  }

  @Test
  @CoversRequirements("HEAD-REQ-002")
  public void testFullHeadersWhitelistContents() {
    // Config with request compression, response compression, and auth should have all headers
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withResponseCompression(
                ResponseCompressionAlgorithm.GZIP, ResponseCompressionAlgorithm.DEFLATE)
            .authenticationEnabled(true)
            .build();
    Set<String> headers = config.getRequiredHeaders();

    assertTrue(headers.contains("Host"));
    assertTrue(headers.contains("X-Amz-Target"));
    assertTrue(headers.contains("Content-Type"));
    assertTrue(headers.contains("Content-Length"));
    assertTrue(headers.contains("Accept-Encoding"));
    assertTrue(headers.contains("Content-Encoding"));
    assertTrue(headers.contains("Authorization"));
    assertTrue(headers.contains("X-Amz-Date"));
    assertTrue(headers.contains("Connection"));
    assertTrue(headers.contains("User-Agent"));
    assertEquals(10, headers.size());
  }

  @Test
  public void testResponseCompressionDisabledRemovesAcceptEncodingFromRequiredHeaders() {
    AlternatorConfig config = AlternatorConfig.builder().withResponseCompressionDisabled().build();

    assertFalse(config.isResponseCompressionEnabled());
    assertFalse(config.getRequiredHeaders().contains("Accept-Encoding"));
    assertFalse(config.getHeadersWhitelist().contains("Accept-Encoding"));
  }

  @Test
  public void testCustomWhitelistWithoutAcceptEncodingSucceedsWhenResponseCompressionDisabled() {
    AlternatorConfig.Builder builder =
        AlternatorConfig.builder().withResponseCompressionDisabled().withOptimizeHeaders(true);
    Set<String> whitelist = new HashSet<>(builder.getRequiredHeaders());

    AlternatorConfig config = builder.withHeadersWhitelist(whitelist).build();

    assertFalse(config.getHeadersWhitelist().contains("Accept-Encoding"));
    assertEquals(whitelist, config.getHeadersWhitelist());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCustomWhitelistWithoutAcceptEncodingFailsWhenResponseCompressionEnabled() {
    AlternatorConfig.Builder builder =
        AlternatorConfig.builder().withResponseCompression(ResponseCompressionAlgorithm.GZIP);
    Set<String> whitelist = new HashSet<>(builder.getRequiredHeaders());
    whitelist.remove("Accept-Encoding");

    builder.withOptimizeHeaders(true).withHeadersWhitelist(whitelist).build();
  }

  @Test
  public void testUserAgentDisabledRemovesUserAgentFromRequiredHeaders() {
    AlternatorConfig config =
        AlternatorConfig.builder().withOptimizeHeaders(true).withUserAgentEnabled(false).build();

    assertFalse(config.isUserAgentEnabled());
    assertFalse(config.getRequiredHeaders().contains("User-Agent"));
    assertFalse(config.getHeadersWhitelist().contains("User-Agent"));
  }

  @Test
  public void testCustomWhitelistWithoutUserAgentSucceedsWhenUserAgentDisabled() {
    AlternatorConfig.Builder builder =
        AlternatorConfig.builder().withOptimizeHeaders(true).withUserAgentEnabled(false);
    Set<String> whitelist = new HashSet<>(builder.getRequiredHeaders());

    AlternatorConfig config = builder.withHeadersWhitelist(whitelist).build();

    assertFalse(config.getHeadersWhitelist().contains("User-Agent"));
    assertEquals(whitelist, config.getHeadersWhitelist());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCustomWhitelistWithoutUserAgentFailsWhenUserAgentEnabled() {
    Set<String> whitelist = new HashSet<>(AlternatorConfig.builder().getRequiredHeaders());
    whitelist.remove("User-Agent");

    AlternatorConfig.builder().withOptimizeHeaders(true).withHeadersWhitelist(whitelist).build();
  }

  @Test
  public void testRequiredHeadersIsImmutable() {
    AlternatorConfig config = AlternatorConfig.builder().build();
    try {
      config.getRequiredHeaders().add("X-Should-Fail");
      fail("getRequiredHeaders() should return immutable set");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testAuthenticationEnabledByDefault() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertTrue(config.isAuthenticationEnabled());
  }

  @Test
  public void testAuthenticationDisabled() {
    AlternatorConfig config = AlternatorConfig.builder().authenticationEnabled(false).build();

    assertFalse(config.isAuthenticationEnabled());
  }

  @Test
  public void testAuthenticationDisabledUsesNoAuthWhitelist() {
    AlternatorConfig config =
        AlternatorConfig.builder().authenticationEnabled(false).withOptimizeHeaders(true).build();

    Set<String> whitelist = config.getHeadersWhitelist();

    // Should use the computed whitelist based on config (no compression, no auth)
    assertTrue(whitelist.contains("Host"));
    assertTrue(whitelist.contains("X-Amz-Target"));
    assertTrue(whitelist.contains("Content-Type"));
    assertTrue(whitelist.contains("Content-Length"));
    assertFalse(whitelist.contains("Authorization"));
    assertFalse(whitelist.contains("X-Amz-Date"));
    // Without compression enabled, Content-Encoding is not included
    assertFalse(whitelist.contains("Content-Encoding"));
    assertTrue(whitelist.contains("User-Agent"));
    assertEquals(config.getRequiredHeaders(), whitelist);
  }

  @Test
  public void testAuthenticationDisabledWithCustomWhitelist() {
    // Get required headers for no-auth config
    Set<String> requiredHeaders =
        AlternatorConfig.builder().authenticationEnabled(false).getRequiredHeaders();
    Set<String> customHeaders = new HashSet<>(requiredHeaders);
    customHeaders.add("X-Custom");

    AlternatorConfig config =
        AlternatorConfig.builder()
            .authenticationEnabled(false)
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    // Custom whitelist should override the default
    assertEquals(customHeaders, config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersWhitelistNoAuthContents() {
    // Config with request compression, response compression, and auth=false
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withResponseCompression(ResponseCompressionAlgorithm.GZIP)
            .authenticationEnabled(false)
            .build();
    Set<String> noAuthHeaders = config.getRequiredHeaders();

    assertTrue(noAuthHeaders.contains("Host"));
    assertTrue(noAuthHeaders.contains("X-Amz-Target"));
    assertTrue(noAuthHeaders.contains("Content-Type"));
    assertTrue(noAuthHeaders.contains("Content-Length"));
    assertTrue(noAuthHeaders.contains("Accept-Encoding"));
    assertTrue(noAuthHeaders.contains("Content-Encoding"));
    assertFalse(noAuthHeaders.contains("Authorization"));
    assertFalse(noAuthHeaders.contains("X-Amz-Date"));
    assertTrue(noAuthHeaders.contains("Connection"));
    assertTrue(noAuthHeaders.contains("User-Agent"));
    assertEquals(8, noAuthHeaders.size());
  }

  @Test
  public void testComputeRequiredHeadersVariations() {
    // Test the internal helper via built configs
    AlternatorConfig noCompressionNoAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.NONE)
            .authenticationEnabled(false)
            .build();
    assertEquals(6, noCompressionNoAuth.getRequiredHeaders().size());
    assertFalse(noCompressionNoAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertFalse(noCompressionNoAuth.getRequiredHeaders().contains("Accept-Encoding"));
    assertFalse(noCompressionNoAuth.getRequiredHeaders().contains("Authorization"));
    assertTrue(noCompressionNoAuth.getRequiredHeaders().contains("User-Agent"));

    AlternatorConfig withCompressionNoAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(false)
            .build();
    assertEquals(7, withCompressionNoAuth.getRequiredHeaders().size());
    assertTrue(withCompressionNoAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertFalse(withCompressionNoAuth.getRequiredHeaders().contains("Accept-Encoding"));
    assertFalse(withCompressionNoAuth.getRequiredHeaders().contains("Authorization"));

    AlternatorConfig noCompressionWithAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.NONE)
            .authenticationEnabled(true)
            .build();
    assertEquals(8, noCompressionWithAuth.getRequiredHeaders().size());
    assertFalse(noCompressionWithAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertFalse(noCompressionWithAuth.getRequiredHeaders().contains("Accept-Encoding"));
    assertTrue(noCompressionWithAuth.getRequiredHeaders().contains("Authorization"));

    AlternatorConfig withCompressionWithAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(true)
            .build();
    assertEquals(9, withCompressionWithAuth.getRequiredHeaders().size());
    assertTrue(withCompressionWithAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertFalse(withCompressionWithAuth.getRequiredHeaders().contains("Accept-Encoding"));
    assertTrue(withCompressionWithAuth.getRequiredHeaders().contains("Authorization"));
  }

  @Test
  public void testBuilderGetRequiredHeaders() {
    // Test builder's getRequiredHeaders method
    AlternatorConfig.Builder builder = AlternatorConfig.builder();
    Set<String> defaultRequired = builder.getRequiredHeaders();
    // Default is no compression, with auth
    assertEquals(8, defaultRequired.size());
    assertFalse(defaultRequired.contains("Content-Encoding"));
    assertFalse(defaultRequired.contains("Accept-Encoding"));
    assertTrue(defaultRequired.contains("Authorization"));
    assertTrue(defaultRequired.contains("User-Agent"));

    // Enable request compression
    builder.withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP);
    Set<String> withCompression = builder.getRequiredHeaders();
    assertEquals(9, withCompression.size());
    assertTrue(withCompression.contains("Content-Encoding"));
    assertFalse(withCompression.contains("Accept-Encoding"));

    // Disable auth
    builder.authenticationEnabled(false);
    Set<String> noAuth = builder.getRequiredHeaders();
    assertEquals(7, noAuth.size());
    assertTrue(noAuth.contains("Content-Encoding"));
    assertFalse(noAuth.contains("Accept-Encoding"));
    assertFalse(noAuth.contains("Authorization"));

    // Enable response compression
    builder.withResponseCompression(ResponseCompressionAlgorithm.GZIP);
    Set<String> withResponseCompression = builder.getRequiredHeaders();
    assertEquals(8, withResponseCompression.size());
    assertTrue(withResponseCompression.contains("Content-Encoding"));
    assertTrue(withResponseCompression.contains("Accept-Encoding"));
    assertFalse(withResponseCompression.contains("Authorization"));
  }

  @Test
  public void testConfigGetRequiredHeaders() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(true)
            .build();

    Set<String> required = config.getRequiredHeaders();
    assertEquals(9, required.size());
    assertTrue(required.contains("Content-Encoding"));
    assertFalse(required.contains("Accept-Encoding"));
    assertTrue(required.contains("Authorization"));
    assertTrue(required.contains("User-Agent"));
  }

  @Test(expected = IllegalArgumentException.class)
  @CoversRequirements("HEAD-REQ-003")
  public void testCustomWhitelistMissingRequiredHeadersThrows() {
    // This should throw because we're missing required headers
    Set<String> incompleteHeaders = new HashSet<>(Arrays.asList("Host", "Authorization"));

    AlternatorConfig.builder()
        .withOptimizeHeaders(true)
        .withHeadersWhitelist(incompleteHeaders)
        .build();
  }

  @Test
  public void testCustomWhitelistWithAllRequiredHeadersSucceeds() {
    Set<String> requiredHeaders = AlternatorConfig.builder().getRequiredHeaders();

    // Adding all required headers should work
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(requiredHeaders)
            .build();

    assertEquals(requiredHeaders, config.getHeadersWhitelist());
  }
}
