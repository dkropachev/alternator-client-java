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

/** Read-only cluster information available to every lease. */
public interface TestClusterInfo {
  String instanceId();

  ClusterSpec spec();

  List<TestClusterNode> nodes();

  AlternatorConnection connection(AlternatorTransport transport);

  AlternatorDynamoDbClientBuilder clientBuilder(AlternatorTransport transport);
}
