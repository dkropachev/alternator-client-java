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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.SelectedAuthScheme;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.Identity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;

/** HTTP-auth SPI implementation isolated so AWS SDK 2.20 never loads its newer API references. */
final class HttpAuthAttemptSigner {
  private HttpAuthAttemptSigner() {}

  @SuppressWarnings({"rawtypes", "unchecked"})
  static boolean install(
      BasicQueryPlanInterceptor router, ExecutionAttributes executionAttributes) {
    boolean installed = false;
    SelectedAuthScheme selected =
        executionAttributes.getAttribute(SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME);
    if (selected != null) {
      if (!(selected.signer() instanceof RoutingHttpSigner)) {
        SelectedAuthScheme routed =
            new SelectedAuthScheme(
                selected.identity(),
                new RoutingHttpSigner(router, executionAttributes, selected.signer()),
                selected.authSchemeOption());
        executionAttributes.putAttribute(
            SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME, routed);
      }
      installed = true;
    }

    Map<String, AuthScheme<?>> schemes =
        executionAttributes.getAttribute(SdkInternalExecutionAttribute.AUTH_SCHEMES);
    if (schemes == null) {
      return installed;
    }
    Map<String, AuthScheme<?>> routedSchemes = new LinkedHashMap<>();
    schemes.forEach(
        (schemeId, scheme) ->
            routedSchemes.put(
                schemeId,
                scheme instanceof RoutingAuthScheme
                    ? scheme
                    : new RoutingAuthScheme<>(router, executionAttributes, scheme)));
    executionAttributes.putAttribute(SdkInternalExecutionAttribute.AUTH_SCHEMES, routedSchemes);
    return true;
  }

  private static final class RoutingAuthScheme<T extends Identity> implements AuthScheme<T> {
    private final BasicQueryPlanInterceptor router;
    private final ExecutionAttributes executionAttributes;
    private final AuthScheme<T> delegate;

    private RoutingAuthScheme(
        BasicQueryPlanInterceptor router,
        ExecutionAttributes executionAttributes,
        AuthScheme<T> delegate) {
      this.router = router;
      this.executionAttributes = executionAttributes;
      this.delegate = delegate;
    }

    @Override
    public String schemeId() {
      return delegate.schemeId();
    }

    @Override
    public IdentityProvider<T> identityProvider(IdentityProviders identityProviders) {
      return delegate.identityProvider(identityProviders);
    }

    @Override
    public HttpSigner<T> signer() {
      return new RoutingHttpSigner<>(router, executionAttributes, delegate.signer());
    }
  }

  private static final class RoutingHttpSigner<T extends Identity> implements HttpSigner<T> {
    private final BasicQueryPlanInterceptor router;
    private final ExecutionAttributes executionAttributes;
    private final HttpSigner<T> delegate;

    private RoutingHttpSigner(
        BasicQueryPlanInterceptor router,
        ExecutionAttributes executionAttributes,
        HttpSigner<T> delegate) {
      this.router = router;
      this.executionAttributes = executionAttributes;
      this.delegate = delegate;
    }

    @Override
    public SignedRequest sign(SignRequest<? extends T> request) {
      SdkHttpRequest routed = route(request.request());
      return delegate.sign(request.toBuilder().request(routed).build());
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends T> request) {
      SdkHttpRequest routed = route(request.request());
      return delegate.signAsync(request.toBuilder().request(routed).build());
    }

    private SdkHttpRequest route(SdkHttpRequest request) {
      return AttemptRequestSigner.withCurrentAuthority(
          router.routeAttemptForSigning(request, executionAttributes).request);
    }
  }
}
