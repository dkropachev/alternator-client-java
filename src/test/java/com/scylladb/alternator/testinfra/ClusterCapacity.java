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

/** Memory capacity available to the CCM scheduler. */
public final class ClusterCapacity {
  public static final int MAXIMUM_NODE_COUNT = 9;

  private final long availableMemoryMiB;
  private final long reservedMemoryMiB;
  private final long usableMemoryMiB;

  ClusterCapacity(long availableMemoryMiB, long reservedMemoryMiB, long usableMemoryMiB) {
    this.availableMemoryMiB = availableMemoryMiB;
    this.reservedMemoryMiB = reservedMemoryMiB;
    this.usableMemoryMiB = usableMemoryMiB;
  }

  public static ClusterCapacity fromAvailableMemory(long availableMemoryMiB) {
    long reserved = Math.max(512, Math.min(4096, availableMemoryMiB / 4));
    return new ClusterCapacity(
        availableMemoryMiB, reserved, Math.max(0, availableMemoryMiB - reserved));
  }

  public long availableMemoryMiB() {
    return availableMemoryMiB;
  }

  public long reservedMemoryMiB() {
    return reservedMemoryMiB;
  }

  public long usableMemoryMiB() {
    return usableMemoryMiB;
  }
}
