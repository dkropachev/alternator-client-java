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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Datacenter and rack layout for a cluster. */
public final class ClusterTopology {
  private final List<DatacenterSpec> datacenters;

  public ClusterTopology(List<DatacenterSpec> datacenters) {
    if (datacenters == null || datacenters.isEmpty()) {
      throw new IllegalArgumentException("A cluster must contain at least one datacenter");
    }
    this.datacenters = Collections.unmodifiableList(new ArrayList<>(datacenters));
  }

  public static ClusterTopology singleDatacenter(int... nodesPerRack) {
    return new ClusterTopology(Arrays.asList(DatacenterSpec.create(nodesPerRack)));
  }

  public List<DatacenterSpec> datacenters() {
    return datacenters;
  }

  public int nodeCount() {
    long count = 0;
    for (DatacenterSpec datacenter : datacenters) {
      for (RackSpec rack : datacenter.racks()) {
        count += rack.nodeCount();
        if (count > Integer.MAX_VALUE) {
          throw new IllegalArgumentException(
              "A cluster topology cannot contain more than " + Integer.MAX_VALUE + " nodes");
        }
      }
    }
    return (int) count;
  }
}
