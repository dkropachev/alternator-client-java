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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.junit.Test;

/** Executes the portable canonical endpoint ordering table. */
public class FeatureSpecCanonicalEndpointVectorsTest {
  private static final Path VECTORS = Paths.get("feature-specs", "vectors", "endpoint-order.tsv");

  @Test
  public void canonicalEndpointOrderMatchesPortableVectors() throws Exception {
    List<String> vectors =
        Files.readAllLines(VECTORS, StandardCharsets.UTF_8).stream()
            .filter(line -> !line.isEmpty() && !line.startsWith("#"))
            .collect(Collectors.toList());
    assertFalse("endpoint-order vectors must not be empty", vectors.isEmpty());

    for (String line : vectors) {
      String[] fields = line.split("\\t", -1);
      List<URI> nodes =
          Arrays.stream(fields[1].split(",")).map(URI::create).collect(Collectors.toList());
      List<URI> sorted = AlternatorLiveNodes.sortAndDedupeNodes(nodes);
      List<String> canonical =
          sorted.stream()
              .map(NodeHealthStore::canonicalNodeKey)
              .map(uri -> uri.toString().toLowerCase(Locale.ROOT))
              .collect(Collectors.toList());
      assertEquals(
          "endpoint-order vector " + fields[0], Arrays.asList(fields[2].split(",")), canonical);
      List<String> representatives =
          sorted.stream().map(URI::toString).collect(Collectors.toList());
      assertEquals(
          "endpoint representatives " + fields[0],
          Arrays.asList(fields[3].split(",")),
          representatives);
    }
  }
}
