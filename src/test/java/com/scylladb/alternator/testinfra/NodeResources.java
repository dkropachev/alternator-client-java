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

/** Per-node CPU and memory allocation. */
public final class NodeResources {
  public static final NodeResources DEFAULT = new NodeResources(2, 1024);

  private final int smp;
  private final int memoryMiB;

  public NodeResources(int smp, int memoryMiB) {
    if (smp < 1 || memoryMiB < 1) {
      throw new IllegalArgumentException("Node SMP and memory must be positive");
    }
    this.smp = smp;
    this.memoryMiB = memoryMiB;
  }

  public int smp() {
    return smp;
  }

  public int memoryMiB() {
    return memoryMiB;
  }
}
