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

import java.io.IOException;

/** Process-wide entry point for CCM cluster leases. */
public final class TestClusters {
  private static TestClusterPool sharedPool;

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    closeAll();
                  } catch (Exception exception) {
                    System.err.println("Failed to remove CCM clusters during JVM shutdown");
                    exception.printStackTrace(System.err);
                  }
                },
                "alternator-ccm-cleanup"));
  }

  private TestClusters() {}

  public static ReusableClusterLease acquireReusable(ClusterSpec spec) throws Exception {
    return pool().acquireReusable(spec);
  }

  public static PrivateClusterLease provisionPrivate(ClusterSpec spec) throws Exception {
    return pool().provisionPrivate(spec);
  }

  public static synchronized void closeAll() throws Exception {
    if (sharedPool != null) {
      sharedPool.close();
      sharedPool = null;
    }
  }

  private static synchronized TestClusterPool pool() throws IOException {
    if (sharedPool == null) {
      sharedPool = TestClusterPool.createDefault();
    }
    return sharedPool;
  }
}
