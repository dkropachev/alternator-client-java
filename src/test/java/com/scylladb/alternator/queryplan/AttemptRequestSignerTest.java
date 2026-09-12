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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.CredentialType;
import software.amazon.awssdk.core.SelectedAuthScheme;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.Identity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;

public class AttemptRequestSignerTest {
  private static final Identity IDENTITY =
      new Identity() {
        @Override
        public Optional<Instant> expirationTime() {
          return Optional.empty();
        }

        @Override
        public Optional<String> providerName() {
          return Optional.of("test");
        }
      };

  @Test
  public void currentAuthorityBracketsIpv6AndOmitsStandardPort() {
    SdkHttpRequest nonStandard = request("http", "::1", 8080, null);
    SdkHttpRequest standard = request("https", "[2001:db8::1]", 443, null);

    SdkHttpRequest routedNonStandard = AttemptRequestSigner.withCurrentAuthority(nonStandard);
    SdkHttpRequest routedStandard = AttemptRequestSigner.withCurrentAuthority(standard);

    assertEquals("[::1]:8080", routedNonStandard.firstMatchingHeader("Host").get());
    assertEquals("[::1]:8080", routedNonStandard.getUri().getRawAuthority());
    assertEquals("[2001:db8::1]", routedStandard.firstMatchingHeader("Host").get());
  }

  @Test
  public void unsignedFallbackUpdatesAuthorityWithoutChangingOtherHeaders() {
    SdkHttpRequest request = request("http", "new.local", 8080, null);
    BasicQueryPlanInterceptor.RoutedRequest routed =
        new BasicQueryPlanInterceptor.RoutedRequest(request, null, true);

    SdkHttpRequest result = AttemptRequestSigner.prepareUnsignedFallback(routed);

    assertEquals("new.local:8080", result.firstMatchingHeader("Host").get());
    assertEquals("preserved", result.firstMatchingHeader("X-Test").get());
  }

  @Test
  public void signedFallbackFailsInsteadOfSendingAStaleSignature() {
    SdkHttpRequest request = request("http", "new.local", 8080, "old-signature");
    BasicQueryPlanInterceptor.RoutedRequest routed =
        new BasicQueryPlanInterceptor.RoutedRequest(request, null, true);

    assertThrows(
        IllegalStateException.class, () -> AttemptRequestSigner.prepareUnsignedFallback(routed));
  }

  @Test
  public void httpAuthSignerRoutesBeforeOneSyncPayloadTransformation() throws Exception {
    TransformingHttpSigner delegate = new TransformingHttpSigner();
    ExecutionAttributes attributes = attributes(delegate);
    assertTrue(HttpAuthAttemptSigner.install(new FixedRouter(), attributes));
    @SuppressWarnings("unchecked")
    HttpSigner<Identity> signer =
        (HttpSigner<Identity>)
            attributes.getAttribute(SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME).signer();

    SignedRequest signed =
        signer.sign(
            SignRequest.builder(IDENTITY)
                .request(request("http", "old.local", 8080, null))
                .payload(ContentStreamProvider.fromUtf8String("body"))
                .build());

    assertEquals("Xbody", read(signed.payload().get()));
    assertEquals("new.local:8080", signed.request().firstMatchingHeader("Host").get());
    assertEquals(1, delegate.syncSignings);
  }

  @Test
  public void httpAuthSignerRoutesBeforeOneAsyncPayloadTransformation() {
    TransformingHttpSigner delegate = new TransformingHttpSigner();
    ExecutionAttributes attributes = attributes(delegate);
    assertTrue(HttpAuthAttemptSigner.install(new FixedRouter(), attributes));
    @SuppressWarnings("unchecked")
    HttpSigner<Identity> signer =
        (HttpSigner<Identity>)
            attributes.getAttribute(SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME).signer();

    AsyncSignedRequest signed =
        signer
            .signAsync(
                AsyncSignRequest.builder(IDENTITY)
                    .request(request("http", "old.local", 8080, null))
                    .payload(singleBuffer("body"))
                    .build())
            .join();

    assertEquals("Xbody", read(signed.payload().get()));
    assertEquals("new.local:8080", signed.request().firstMatchingHeader("Host").get());
    assertEquals(1, delegate.asyncSignings);
  }

  @Test
  public void unresolvedHttpAuthSelectionWrapsSchemeResolvedAfterInterceptors() throws Exception {
    TransformingHttpSigner delegate = new TransformingHttpSigner();
    ExecutionAttributes attributes = ExecutionAttributes.builder().build();
    attributes.putAttribute(
        SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME,
        new SelectedAuthScheme<>(
            CompletableFuture.completedFuture(IDENTITY),
            new TransformingHttpSigner(),
            AuthSchemeOption.builder().schemeId("unset").build()));
    Map<String, AuthScheme<?>> schemes = new LinkedHashMap<>();
    schemes.put("test", new TestAuthScheme(delegate));
    attributes.putAttribute(SdkInternalExecutionAttribute.AUTH_SCHEMES, schemes);

    assertTrue(HttpAuthAttemptSigner.install(new FixedRouter(), attributes));
    @SuppressWarnings("unchecked")
    AuthScheme<Identity> resolvedScheme =
        (AuthScheme<Identity>)
            attributes.getAttribute(SdkInternalExecutionAttribute.AUTH_SCHEMES).get("test");
    SignedRequest signed =
        resolvedScheme
            .signer()
            .sign(
                SignRequest.builder(IDENTITY)
                    .request(request("http", "old.local", 8080, null))
                    .payload(ContentStreamProvider.fromUtf8String("body"))
                    .build());

    assertEquals("Xbody", read(signed.payload().get()));
    assertEquals("new.local:8080", signed.request().firstMatchingHeader("Host").get());
    assertEquals(1, delegate.syncSignings);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void legacyRoutingSignerForwardsCredentialType() {
    Signer routed =
        AttemptRequestSigner.wrapClientSigner(
            new FixedRouter(),
            new Signer() {
              @Override
              public SdkHttpFullRequest sign(
                  SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
                return request;
              }

              @Override
              public CredentialType credentialType() {
                return CredentialType.TOKEN;
              }
            });

    assertEquals(CredentialType.TOKEN, routed.credentialType());
  }

  private static ExecutionAttributes attributes(HttpSigner<Identity> signer) {
    ExecutionAttributes attributes = ExecutionAttributes.builder().build();
    attributes.putAttribute(
        SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME,
        new SelectedAuthScheme<>(
            CompletableFuture.completedFuture(IDENTITY),
            signer,
            AuthSchemeOption.builder().schemeId("test").build()));
    return attributes;
  }

  private static String read(ContentStreamProvider payload) throws IOException {
    byte[] buffer = new byte[64];
    int read;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (java.io.InputStream stream = payload.newStream()) {
      while ((read = stream.read(buffer)) != -1) {
        output.write(buffer, 0, read);
      }
    }
    return new String(output.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
  }

  private static String read(Publisher<ByteBuffer> payload) {
    CompletableFuture<String> result = new CompletableFuture<>();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    payload.subscribe(
        new Subscriber<ByteBuffer>() {
          @Override
          public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(ByteBuffer item) {
            ByteBuffer copy = item.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            output.write(bytes, 0, bytes.length);
          }

          @Override
          public void onError(Throwable failure) {
            result.completeExceptionally(failure);
          }

          @Override
          public void onComplete() {
            result.complete(
                new String(output.toByteArray(), java.nio.charset.StandardCharsets.UTF_8));
          }
        });
    return result.join();
  }

  private static Publisher<ByteBuffer> singleBuffer(String value) {
    return subscriber -> {
      subscriber.onSubscribe(
          new Subscription() {
            private boolean complete;

            @Override
            public void request(long count) {
              if (!complete) {
                complete = true;
                subscriber.onNext(
                    ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                subscriber.onComplete();
              }
            }

            @Override
            public void cancel() {
              complete = true;
            }
          });
    };
  }

  private static SdkHttpRequest request(
      String protocol, String host, int port, String authorization) {
    SdkHttpRequest.Builder builder =
        SdkHttpRequest.builder()
            .protocol(protocol)
            .host(host)
            .port(port)
            .method(SdkHttpMethod.POST)
            .encodedPath("/")
            .putHeader("Host", "old.local:8080")
            .putHeader("X-Test", "preserved");
    if (authorization != null) {
      builder.putHeader("Authorization", authorization);
    }
    return builder.build();
  }

  private static final class FixedRouter extends BasicQueryPlanInterceptor {
    private FixedRouter() {
      super(null);
    }

    @Override
    RoutedRequest routeAttemptForSigning(
        SdkHttpRequest request, ExecutionAttributes executionAttributes) {
      return new RoutedRequest(
          request.toBuilder().host("new.local").port(8080).build(), executionAttributes, true);
    }
  }

  private static final class TransformingHttpSigner implements HttpSigner<Identity> {
    private int syncSignings;
    private int asyncSignings;

    @Override
    public SignedRequest sign(SignRequest<? extends Identity> request) {
      syncSignings++;
      ContentStreamProvider original = request.payload().get();
      ContentStreamProvider transformed =
          () ->
              new java.io.SequenceInputStream(
                  new ByteArrayInputStream(new byte[] {'X'}), original.newStream());
      return SignedRequest.builder().request(request.request()).payload(transformed).build();
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
        AsyncSignRequest<? extends Identity> request) {
      asyncSignings++;
      Publisher<ByteBuffer> original = request.payload().get();
      Publisher<ByteBuffer> transformed =
          subscriber ->
              original.subscribe(
                  new Subscriber<ByteBuffer>() {
                    private boolean first = true;

                    @Override
                    public void onSubscribe(Subscription subscription) {
                      subscriber.onSubscribe(subscription);
                    }

                    @Override
                    public void onNext(ByteBuffer item) {
                      if (!first) {
                        subscriber.onNext(item);
                        return;
                      }
                      first = false;
                      ByteBuffer source = item.asReadOnlyBuffer();
                      ByteBuffer prefixed = ByteBuffer.allocate(source.remaining() + 1);
                      prefixed.put((byte) 'X').put(source).flip();
                      subscriber.onNext(prefixed);
                    }

                    @Override
                    public void onError(Throwable failure) {
                      subscriber.onError(failure);
                    }

                    @Override
                    public void onComplete() {
                      subscriber.onComplete();
                    }
                  });
      return CompletableFuture.completedFuture(
          AsyncSignedRequest.builder().request(request.request()).payload(transformed).build());
    }
  }

  private static final class TestAuthScheme implements AuthScheme<Identity> {
    private final HttpSigner<Identity> signer;

    private TestAuthScheme(HttpSigner<Identity> signer) {
      this.signer = signer;
    }

    @Override
    public String schemeId() {
      return "test";
    }

    @Override
    public IdentityProvider<Identity> identityProvider(IdentityProviders identityProviders) {
      return identityProviders.identityProvider(Identity.class);
    }

    @Override
    public HttpSigner<Identity> signer() {
      return signer;
    }
  }
}
