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

import com.scylladb.alternator.AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder;
import java.util.List;

/** Snapshot view that deliberately exposes no node mutation controls. */
final class ReadOnlyTestCluster implements TestClusterInfo {
  private final PhysicalTestCluster cluster;
  private final List<TestClusterNode> nodes;

  ReadOnlyTestCluster(PhysicalTestCluster cluster) {
    this.cluster = cluster;
    this.nodes = cluster.nodes();
  }

  @Override
  public String instanceId() {
    return cluster.instanceId();
  }

  @Override
  public ClusterSpec spec() {
    return cluster.spec();
  }

  @Override
  public List<TestClusterNode> nodes() {
    return nodes;
  }

  @Override
  public AlternatorConnection connection(AlternatorTransport transport) {
    return cluster.connection(transport);
  }

  @Override
  public AlternatorDynamoDbClientBuilder clientBuilder(AlternatorTransport transport) {
    return cluster.clientBuilder(transport);
  }
}
