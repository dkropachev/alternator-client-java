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
import static org.junit.Assert.assertFalse;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Executes the portable vector files. */
public class FeatureSpecVectorsTest {
  private static final Path VECTORS = Paths.get("feature-specs", "vectors");

  @Test
  @CoversRequirements("AFF-REQ-003")
  public void affinityHashesMatchPortableVectors() throws Exception {
    List<String> vectors = dataLines(VECTORS.resolve("affinity-hash.tsv"));
    assertFalse("affinity-hash vectors must not be empty", vectors.isEmpty());
    for (String line : vectors) {
      String[] fields = line.split("\\t", -1);
      AttributeValue value = attributeValue(fields[0], fields[1], fields[2]);
      assertEquals("vector " + line, Long.parseLong(fields[3]), AttributeValueHasher.hash(value));
    }
  }

  @Test
  @CoversRequirements("QUERY-REQ-003")
  public void seededPlansMatchPortableVectors() throws Exception {
    List<String> vectors = dataLines(VECTORS.resolve("query-plan.tsv"));
    assertFalse("query-plan vectors must not be empty", vectors.isEmpty());
    for (String line : vectors) {
      String[] fields = line.split("\\t", -1);
      long seed = Long.parseLong(fields[0]);
      int candidateCount = Integer.parseInt(fields[1]);
      List<URI> nodes = new ArrayList<>();
      for (int i = 1; i <= candidateCount; i++) {
        nodes.add(URI.create("http://node" + i + ".example.com:8043"));
      }
      LazyQueryPlan plan =
          new LazyQueryPlan(new AlternatorLiveNodes(nodes, "http", 8043, "", ""), seed);
      List<String> actual = new ArrayList<>();
      for (String ignored : fields[2].split(",")) {
        actual.add(shortName(plan.next()));
      }
      List<String> expected =
          java.util.Arrays.stream(fields[2].split(","))
              .map(index -> "node" + index)
              .collect(Collectors.toList());
      assertEquals("complete vector " + line, candidateCount, expected.size());
      assertEquals("vector " + line, expected, actual);
    }
  }

  @Test
  public void queryPlanGeneratorConstantsMatchPortableVector() throws Exception {
    List<String> vectors = dataLines(VECTORS.resolve("query-plan-rng-cooked.tsv"));
    assertEquals(607, vectors.size());

    Class<?> generator = Class.forName("com.scylladb.alternator.internal.GoRand");
    Field constantsField = generator.getDeclaredField("RNG_COOKED");
    constantsField.setAccessible(true);
    long[] constants = (long[]) constantsField.get(null);
    assertEquals(vectors.size(), constants.length);

    for (String line : vectors) {
      String[] fields = line.split("\\t", -1);
      int index = Integer.parseInt(fields[0]);
      assertEquals("generator constant " + index, Long.parseLong(fields[1]), constants[index]);
    }
  }

  private static AttributeValue attributeValue(String type, String encoding, String payload) {
    if ("S".equals(type) && "utf8".equals(encoding)) {
      return AttributeValue.builder().s(payload).build();
    }
    if ("N".equals(type) && "utf8".equals(encoding)) {
      return AttributeValue.builder().n(payload).build();
    }
    if (!"B".equals(type) || !"hex".equals(encoding)) {
      throw new IllegalArgumentException("Unsupported vector type: " + type + "/" + encoding);
    }
    byte[] bytes = new byte[payload.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(payload.substring(i * 2, i * 2 + 2), 16);
    }
    return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
  }

  private static String shortName(URI uri) {
    String host = uri.getHost();
    return host.substring(0, host.indexOf('.'));
  }

  private static List<String> dataLines(Path path) throws Exception {
    return Files.readAllLines(path, StandardCharsets.UTF_8).stream()
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .collect(Collectors.toList());
  }
}
