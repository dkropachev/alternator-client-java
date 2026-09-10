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

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/** Propagates service-client shutdown to Alternator resources and an optionally owned transport. */
final class LifecycleSdkAsyncHttpClient implements SdkAsyncHttpClient {
  private final SdkAsyncHttpClient delegate;
  private final AlternatorClientResources resources;
  private final boolean closeDelegate;
  private boolean closed;

  LifecycleSdkAsyncHttpClient(
      SdkAsyncHttpClient delegate, AlternatorClientResources resources, boolean closeDelegate) {
    this.delegate = delegate;
    this.resources = resources;
    this.closeDelegate = closeDelegate;
  }

  @Override
  public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
    return delegate.execute(request);
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;

    RuntimeException failure = null;
    try {
      resources.close();
    } catch (RuntimeException e) {
      failure = e;
    }

    if (closeDelegate) {
      try {
        delegate.close();
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }

    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }
}
