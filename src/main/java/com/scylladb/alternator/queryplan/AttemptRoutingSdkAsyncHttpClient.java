package com.scylladb.alternator.queryplan;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/** Routes each asynchronous SDK transmission through its request query plan. */
public final class AttemptRoutingSdkAsyncHttpClient implements SdkAsyncHttpClient {
  private final SdkAsyncHttpClient delegate;
  private final BasicQueryPlanInterceptor router;

  public AttemptRoutingSdkAsyncHttpClient(
      SdkAsyncHttpClient delegate, BasicQueryPlanInterceptor router) {
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
  public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
    SdkHttpRequest routedRequest = router.routeAttempt(request.request());
    AsyncExecuteRequest routedExecuteRequest =
        AsyncExecuteRequest.builder()
            .request(routedRequest)
            .requestContentPublisher(request.requestContentPublisher())
            .responseHandler(request.responseHandler())
            .fullDuplex(request.fullDuplex())
            .metricCollector(request.metricCollector().orElse(null))
            .httpExecutionAttributes(request.httpExecutionAttributes())
            .build();
    return delegate.execute(routedExecuteRequest);
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
