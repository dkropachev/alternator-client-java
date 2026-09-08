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

import com.scylladb.alternator.CoversRequirements;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.Test;

/** Executes the portable node-health state-transition table. */
public class FeatureSpecNodeHealthTransitionsTest {
  private static final URI NODE = URI.create("http://node.example.com:8000");
  private static final Path TRANSITIONS =
      Paths.get("feature-specs", "vectors", "node-health-transitions.tsv");

  @Test
  @CoversRequirements("HEALTH-REQ-003")
  public void stateTransitionsMatchPortableTable() throws Exception {
    for (String line : Files.readAllLines(TRANSITIONS, StandardCharsets.UTF_8)) {
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      verify(line.split("\\t", -1));
    }
  }

  private static void verify(String[] fields) {
    assertEquals("transition field count", 11, fields.length);
    String name = fields[0];
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(Integer.parseInt(fields[2]))
            .withDownNodeRecoverySuccessThreshold(Integer.parseInt(fields[3]))
            .withQuarantineSuccessThreshold(Integer.parseInt(fields[4]))
            .withQuarantineFailureThreshold(Integer.parseInt(fields[5]))
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Collections.emptyList());
    NodeHealthState initial = NodeHealthState.valueOf(fields[1]);
    if (initial == NodeHealthState.ACTIVE) {
      store.addActiveNode(NODE);
    } else {
      store.addQuarantinedNode(NODE);
    }

    for (String observation : fields[6].split(",")) {
      store.reportNodeResult(NODE, NodeHealthObservation.valueOf(observation));
    }

    NodeHealthStatus status = store.getNodeStatus(NODE);
    assertEquals(name, NodeHealthState.valueOf(fields[7]), status.getState());
    assertEquals(name, Integer.parseInt(fields[8]), status.getConsecutiveFailures());
    assertEquals(name, Integer.parseInt(fields[9]), status.getConsecutiveSuccesses());
    assertEquals(name, Long.parseLong(fields[10]), status.getGeneration());
  }
}
