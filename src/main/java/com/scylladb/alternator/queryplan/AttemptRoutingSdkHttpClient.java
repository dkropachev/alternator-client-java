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
package com.scylladb.alternator.queryplan;

import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Routes each synchronous SDK transmission through its request query plan. */
public final class AttemptRoutingSdkHttpClient implements SdkHttpClient {
  private final SdkHttpClient delegate;
  private final BasicQueryPlanInterceptor router;

  public AttemptRoutingSdkHttpClient(SdkHttpClient delegate, BasicQueryPlanInterceptor router) {
    if (delegate == null) {
      throw new IllegalArgumentException("delegate cannot be null");
    }
    if (router == null) {
      throw new IllegalArgumentException("router cannot be null");
    }
    this.delegate = delegate;
    this.router = router;
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    SdkHttpRequest routedRequest = router.routeAttempt(request.httpRequest());
    HttpExecuteRequest routedExecuteRequest =
        HttpExecuteRequest.builder()
            .request(routedRequest)
            .contentStreamProvider(request.contentStreamProvider().orElse(null))
            .metricCollector(request.metricCollector().orElse(null))
            .build();
    return delegate.prepareRequest(routedExecuteRequest);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }
}
