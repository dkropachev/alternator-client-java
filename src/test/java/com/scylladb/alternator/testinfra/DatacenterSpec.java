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
import java.util.Collections;
import java.util.List;

/** Rack layout for one datacenter. */
public final class DatacenterSpec {
  private final List<RackSpec> racks;

  public DatacenterSpec(List<RackSpec> racks) {
    if (racks == null || racks.isEmpty()) {
      throw new IllegalArgumentException("A datacenter must contain at least one rack");
    }
    this.racks = Collections.unmodifiableList(new ArrayList<>(racks));
  }

  public static DatacenterSpec create(int... nodesPerRack) {
    List<RackSpec> racks = new ArrayList<>();
    for (int count : nodesPerRack) {
      racks.add(new RackSpec(count));
    }
    return new DatacenterSpec(racks);
  }

  public List<RackSpec> racks() {
    return racks;
  }
}
