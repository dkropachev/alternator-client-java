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

import java.util.concurrent.atomic.AtomicBoolean;

/** Concurrently shareable cluster lease with an independent resource namespace. */
public final class ReusableClusterLease implements AutoCloseable {
  private final TestClusterPool pool;
  private final TestClusterPool.PooledCluster pooledCluster;
  private final TestClusterInfo cluster;
  private final TestResourceScope resources;
  private final AtomicBoolean closed = new AtomicBoolean();

  ReusableClusterLease(
      TestClusterPool pool,
      TestClusterPool.PooledCluster pooledCluster,
      PhysicalTestCluster cluster,
      TestResourceScope resources) {
    this.pool = pool;
    this.pooledCluster = pooledCluster;
    this.cluster = new ReadOnlyTestCluster(cluster);
    this.resources = resources;
  }

  public TestClusterInfo cluster() {
    return cluster;
  }

  public TestResourceScope resources() {
    return resources;
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      pool.releaseReusable(pooledCluster, resources);
    }
  }
}
