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
package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import software.amazon.awssdk.http.SdkHttpClient;

/** Owns background resources shared by the standard client and Alternator API wrapper. */
final class AlternatorClientResources implements AutoCloseable {
  private final AlternatorLiveNodes liveNodes;
  private final AffinityQueryPlanInterceptor affinityInterceptor;
  private final SdkHttpClient pollingHttpClient;
  private boolean closed;

  AlternatorClientResources(
      AlternatorLiveNodes liveNodes,
      AffinityQueryPlanInterceptor affinityInterceptor,
      SdkHttpClient pollingHttpClient) {
    this.liveNodes = liveNodes;
    this.affinityInterceptor = affinityInterceptor;
    this.pollingHttpClient = pollingHttpClient;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;

    RuntimeException failure = null;
    if (affinityInterceptor != null) {
      failure = runCleanup(failure, () -> affinityInterceptor.getPartitionKeyResolver().shutdown());
    }
    if (liveNodes != null) {
      failure = runCleanup(failure, liveNodes::shutdownAndWait);
    }
    if (pollingHttpClient != null) {
      failure = runCleanup(failure, pollingHttpClient::close);
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static RuntimeException runCleanup(RuntimeException failure, Runnable cleanup) {
    try {
      cleanup.run();
    } catch (RuntimeException e) {
      if (failure == null) {
        return e;
      }
      failure.addSuppressed(e);
    }
    return failure;
  }
}
