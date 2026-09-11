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

/** Exclusive cluster lease that exposes topology and lifecycle mutation. */
public final class PrivateClusterLease implements AutoCloseable {
  private final TestClusterPool pool;
  private final PhysicalTestCluster cluster;
  private final PrivateClusterControl control;
  private final TestResourceScope resources;
  private boolean closed;

  PrivateClusterLease(
      TestClusterPool pool, PhysicalTestCluster cluster, TestResourceScope resources) {
    this.pool = pool;
    this.cluster = cluster;
    this.resources = resources;
    this.control =
        new PrivateClusterControl() {
          @Override
          public void start() throws Exception {
            cluster.start();
          }

          @Override
          public void stop() throws Exception {
            cluster.stop();
          }

          @Override
          public void startNode(TestClusterNode node) throws Exception {
            cluster.startNode(node);
          }

          @Override
          public void stopNode(TestClusterNode node) throws Exception {
            cluster.stopNode(node);
          }

          @Override
          public TestClusterNode addNode(String datacenter, String rack) throws Exception {
            return cluster.addNode(pool, datacenter, rack);
          }

          @Override
          public void removeNode(TestClusterNode node) throws Exception {
            cluster.removeNode(pool, node);
          }
        };
  }

  public TestClusterInfo cluster() {
    return cluster;
  }

  public PrivateClusterControl control() {
    return control;
  }

  public TestResourceScope resources() {
    return resources;
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      pool.releasePrivate(cluster);
      closed = true;
    }
  }
}
