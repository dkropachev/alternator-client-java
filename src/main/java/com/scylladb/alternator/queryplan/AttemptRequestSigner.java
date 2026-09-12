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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.CredentialType;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.signer.AsyncRequestBodySigner;
import software.amazon.awssdk.core.signer.AsyncSigner;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Installs per-attempt routing immediately before the SDK signs each physical transmission. */
final class AttemptRequestSigner {
  private static final String MODERN_SIGNER_CLASS =
      AttemptRequestSigner.class.getPackage().getName() + ".HttpAuthAttemptSigner";
  private static final ExecutionAttribute<Boolean> MODERN_SIGNER_INSTALLED =
      new ExecutionAttribute<>("AlternatorModernAttemptSignerInstalled");
  private static final Method INSTALL_MODERN_SIGNER = findModernSignerInstaller();
  private static final int HTTP_AUTH_USER_INTERCEPTOR_MIN_MINOR = 26;

  private AttemptRequestSigner() {}

  static void installModernSigner(
      BasicQueryPlanInterceptor router, ExecutionAttributes executionAttributes) {
    if (INSTALL_MODERN_SIGNER == null
        || Boolean.TRUE.equals(executionAttributes.getAttribute(MODERN_SIGNER_INSTALLED))) {
      return;
    }
    try {
      boolean installed = (Boolean) INSTALL_MODERN_SIGNER.invoke(null, router, executionAttributes);
      if (installed) {
        executionAttributes.putAttribute(MODERN_SIGNER_INSTALLED, true);
      }
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("cannot install the SDK HTTP-auth routing signer", e);
    } catch (InvocationTargetException e) {
      throw propagate(e.getCause());
    }
  }

  static SdkRequest installLegacySigner(
      BasicQueryPlanInterceptor router,
      SdkRequest request,
      ExecutionAttributes executionAttributes,
      Signer configuredClientSigner) {
    Signer requestSigner =
        request
            .overrideConfiguration()
            .flatMap(configuration -> configuration.signer())
            .orElse(null);
    Signer signer = requestSigner != null ? requestSigner : configuredClientSigner;
    boolean modernInstalled =
        Boolean.TRUE.equals(executionAttributes.getAttribute(MODERN_SIGNER_INSTALLED));
    if (signer == null && modernInstalled) {
      return request;
    }
    if (signer == null
        && Boolean.TRUE.equals(
            executionAttributes.getAttribute(SdkExecutionAttribute.SIGNER_OVERRIDDEN))) {
      throw new IllegalStateException(
          "cannot route retries because the configured SDK signer is unavailable; "
              + "construct the client through AlternatorDynamoDbClient");
    }
    if (signer == null) {
      // AWS SDK 2.20 predates the HTTP-auth SPI. DynamoDB's generated default is Aws4Signer.
      signer = Aws4Signer.create();
    }
    if (signer instanceof RoutingSigner) {
      return request;
    }
    if (!(request instanceof AwsRequest)) {
      throw new IllegalStateException("DynamoDB request does not implement AwsRequest");
    }

    AwsRequest awsRequest = (AwsRequest) request;
    AwsRequestOverrideConfiguration.Builder override =
        awsRequest
            .overrideConfiguration()
            .map(AwsRequestOverrideConfiguration::toBuilder)
            .orElseGet(AwsRequestOverrideConfiguration::builder);
    override.signer(wrapLegacySigner(router, executionAttributes, signer));
    return awsRequest.toBuilder().overrideConfiguration(override.build()).build();
  }

  static boolean requiresLegacyClientSigner() {
    if (INSTALL_MODERN_SIGNER == null) {
      return true;
    }
    try {
      // SDK_VERSION is a compile-time constant, so reflection is required to read the consumer's
      // runtime version instead of inlining the version used to compile this library.
      String version =
          (String)
              Class.forName("software.amazon.awssdk." + "core.util.VersionInfo")
                  .getField("SDK_VERSION")
                  .get(null);
      String[] components = version.split("\\.");
      return components.length >= 2
          && Integer.parseInt(components[0]) == 2
          && Integer.parseInt(components[1]) < HTTP_AUTH_USER_INTERCEPTOR_MIN_MINOR;
    } catch (ReflectiveOperationException | RuntimeException unavailable) {
      return false;
    }
  }

  static Signer wrapClientSigner(BasicQueryPlanInterceptor router, Signer signer) {
    if (signer == null) {
      throw new IllegalArgumentException("signer cannot be null");
    }
    return wrapLegacySigner(router, null, signer);
  }

  static SdkHttpRequest prepareUnsignedFallback(BasicQueryPlanInterceptor.RoutedRequest routed) {
    if (!routed.authorityChanged) {
      return routed.request;
    }
    if (routed.request.firstMatchingHeader("Authorization").isPresent()) {
      throw new IllegalStateException(
          "cannot reroute a signed request because its signer was not installed");
    }
    return withCurrentAuthority(routed.request);
  }

  static SdkHttpRequest withCurrentAuthority(SdkHttpRequest request) {
    String host = bracketIpv6Literal(request.host());
    return request.toBuilder()
        .host(host)
        .putHeader("Host", authority(request.protocol(), host, request.port()))
        .build();
  }

  private static Method findModernSignerInstaller() {
    try {
      ClassLoader loader = AttemptRequestSigner.class.getClassLoader();
      Class.forName("software.amazon.awssdk." + "core.SelectedAuthScheme", false, loader);
      Class<?> implementation = Class.forName(MODERN_SIGNER_CLASS, true, loader);
      return implementation.getDeclaredMethod(
          "install", BasicQueryPlanInterceptor.class, ExecutionAttributes.class);
    } catch (ClassNotFoundException | NoSuchMethodException | LinkageError unavailable) {
      return null;
    }
  }

  private static Signer wrapLegacySigner(
      BasicQueryPlanInterceptor router, ExecutionAttributes executionAttributes, Signer signer) {
    if (signer instanceof AsyncSigner) {
      return new RoutingAsyncSigner(router, executionAttributes, signer, (AsyncSigner) signer);
    }
    if (signer instanceof AsyncRequestBodySigner) {
      return new RoutingAsyncRequestBodySigner(
          router, executionAttributes, signer, (AsyncRequestBodySigner) signer);
    }
    return new RoutingSigner(router, executionAttributes, signer);
  }

  private static RuntimeException propagate(Throwable failure) {
    if (failure instanceof RuntimeException) {
      return (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    return new IllegalStateException(failure);
  }

  private static String bracketIpv6Literal(String host) {
    if (host.indexOf(':') >= 0 && !(host.startsWith("[") && host.endsWith("]"))) {
      return "[" + host + "]";
    }
    return host;
  }

  private static String authority(String protocol, String host, int port) {
    boolean standard =
        ("http".equalsIgnoreCase(protocol) && port == 80)
            || ("https".equalsIgnoreCase(protocol) && port == 443);
    return standard ? host : host + ":" + port;
  }

  private static SdkHttpFullRequest fullRequest(SdkHttpRequest request) {
    if (request instanceof SdkHttpFullRequest) {
      return (SdkHttpFullRequest) request;
    }
    return SdkHttpFullRequest.builder()
        .protocol(request.protocol())
        .host(request.host())
        .port(request.port())
        .method(request.method())
        .encodedPath(request.encodedPath())
        .applyMutation(builder -> request.forEachHeader(builder::putHeader))
        .applyMutation(builder -> request.forEachRawQueryParameter(builder::putRawQueryParameter))
        .build();
  }

  private static class RoutingSigner implements Signer {
    final BasicQueryPlanInterceptor router;
    final ExecutionAttributes executionAttributes;
    final Signer delegate;

    private RoutingSigner(
        BasicQueryPlanInterceptor router,
        ExecutionAttributes executionAttributes,
        Signer delegate) {
      this.router = router;
      this.executionAttributes = executionAttributes;
      this.delegate = delegate;
    }

    @Override
    public SdkHttpFullRequest sign(
        SdkHttpFullRequest request, ExecutionAttributes suppliedExecutionAttributes) {
      ExecutionAttributes attributes = attributes(suppliedExecutionAttributes);
      SdkHttpRequest routed =
          withCurrentAuthority(router.routeAttemptForSigning(request, attributes).request);
      return delegate.sign(fullRequest(routed), attributes);
    }

    @Override
    public CredentialType credentialType() {
      return delegate.credentialType();
    }

    final ExecutionAttributes attributes(ExecutionAttributes suppliedExecutionAttributes) {
      return executionAttributes != null ? executionAttributes : suppliedExecutionAttributes;
    }
  }

  @SuppressWarnings("deprecation")
  private static final class RoutingAsyncSigner extends RoutingSigner implements AsyncSigner {
    private final AsyncSigner asyncDelegate;

    private RoutingAsyncSigner(
        BasicQueryPlanInterceptor router,
        ExecutionAttributes executionAttributes,
        Signer delegate,
        AsyncSigner asyncDelegate) {
      super(router, executionAttributes, delegate);
      this.asyncDelegate = asyncDelegate;
    }

    @Override
    public CompletableFuture<SdkHttpFullRequest> sign(
        SdkHttpFullRequest request,
        AsyncRequestBody requestBody,
        ExecutionAttributes suppliedExecutionAttributes) {
      ExecutionAttributes attributes = attributes(suppliedExecutionAttributes);
      SdkHttpRequest routed =
          withCurrentAuthority(router.routeAttemptForSigning(request, attributes).request);
      return asyncDelegate.sign(fullRequest(routed), requestBody, attributes);
    }
  }

  @SuppressWarnings("deprecation")
  private static final class RoutingAsyncRequestBodySigner extends RoutingSigner
      implements AsyncRequestBodySigner {
    private final AsyncRequestBodySigner asyncDelegate;

    private RoutingAsyncRequestBodySigner(
        BasicQueryPlanInterceptor router,
        ExecutionAttributes executionAttributes,
        Signer delegate,
        AsyncRequestBodySigner asyncDelegate) {
      super(router, executionAttributes, delegate);
      this.asyncDelegate = asyncDelegate;
    }

    @Override
    public AsyncRequestBody signAsyncRequestBody(
        SdkHttpFullRequest request,
        AsyncRequestBody requestBody,
        ExecutionAttributes suppliedExecutionAttributes) {
      ExecutionAttributes attributes = attributes(suppliedExecutionAttributes);
      return asyncDelegate.signAsyncRequestBody(request, requestBody, attributes);
    }
  }
}
