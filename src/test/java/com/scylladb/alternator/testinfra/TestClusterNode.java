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

/** A node provisioned by CCM. */
public final class TestClusterNode {
  private final String name;
  private final String address;
  private final String datacenter;
  private final String rack;

  TestClusterNode(String name, String address, String datacenter, String rack) {
    this.name = name;
    this.address = address;
    this.datacenter = datacenter;
    this.rack = rack;
  }

  public String name() {
    return name;
  }

  public String address() {
    return address;
  }

  public String datacenter() {
    return datacenter;
  }

  public String rack() {
    return rack;
  }
}
