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
package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;

public class AlternatorLiveNodesShutdownTest {

  @Test
  public void testPollingFailureRespectsRefreshInterval() throws Exception {
    FailingRefreshLiveNodes liveNodes = new FailingRefreshLiveNodes();

    try {
      liveNodes.start();
      assertTrue(
          "initial polling request should start", liveNodes.firstCall.await(5, TimeUnit.SECONDS));

      assertFalse(
          "failed polling should wait for the refresh interval before retrying",
          liveNodes.secondCall.await(500, TimeUnit.MILLISECONDS));
    } finally {
      liveNodes.shutdownAndWait(5_000);
    }
  }

  @Test
  public void testDownNodeProbeRunsWhenRefreshFails() throws Exception {
    FailingRefreshWithProbeLiveNodes liveNodes = new FailingRefreshWithProbeLiveNodes();

    try {
      liveNodes.start();
      assertTrue(
          "down-node probe should run even when refresh keeps failing",
          liveNodes.probeCall.await(5, TimeUnit.SECONDS));
    } finally {
      liveNodes.shutdownAndWait(5_000);
    }
  }

  @Test
  public void testShutdownPathPollingFailureDoesNotEscapeThread() throws Exception {
    BlockingShutdownHttpClient client = new BlockingShutdownHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    liveNodes.setUncaughtExceptionHandler((thread, throwable) -> uncaught.set(throwable));

    liveNodes.start();
    assertTrue("polling request should start", client.callStarted.await(5, TimeUnit.SECONDS));

    liveNodes.shutdown();
    client.releaseCall.countDown();

    assertTrue("live-node thread should stop", liveNodes.shutdownAndWait(5_000));
    assertNull("shutdown-path polling exception should not escape", uncaught.get());
  }

  @Test
  public void testRuntimePollingFailureDoesNotStopThread() throws Exception {
    RuntimeThenSuccessHttpClient client = new RuntimeThenSuccessHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://127.0.0.1:8000"))
            .withIdleRefreshIntervalMs(25)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    liveNodes.setUncaughtExceptionHandler((thread, throwable) -> uncaught.set(throwable));

    liveNodes.start();

    assertTrue(
        "recovery polling request should run", client.successfulCall.await(5, TimeUnit.SECONDS));
    assertTrue("live-node thread should still be running", liveNodes.isRunning());
    assertNull("runtime polling failure should not escape", uncaught.get());
    assertTrue("live-node thread should stop", liveNodes.shutdownAndWait(5_000));
  }

  @Test
  public void testShutdownAndWaitClosesOwnedPollingClientWhenThreadNeverStarted() throws Exception {
    CloseCountingHttpClient client = new CloseCountingHttpClient();
    AlternatorLiveNodes liveNodes = newOwnedLiveNodes(client);

    assertTrue("never-started live-node thread should be stopped", liveNodes.shutdownAndWait(0));
    assertTrue(
        "second shutdown should remain stopped and close should be idempotent",
        liveNodes.shutdownAndWait(0));

    assertEquals("owned polling client should close exactly once", 1, client.closeCount.get());
  }

  private static AlternatorLiveNodes newOwnedLiveNodes(SdkHttpClient client) throws Exception {
    Constructor<AlternatorLiveNodes> constructor =
        AlternatorLiveNodes.class.getDeclaredConstructor(
            AlternatorConfig.class, SdkHttpClient.class, boolean.class);
    constructor.setAccessible(true);
    return constructor.newInstance(
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8000")).build(),
        client,
        true);
  }

  private static final class FailingRefreshLiveNodes extends AlternatorLiveNodes {
    private static final long REFRESH_INTERVAL_MS = 10_000;

    private final AtomicInteger callCount = new AtomicInteger(0);
    private final CountDownLatch firstCall = new CountDownLatch(1);
    private final CountDownLatch secondCall = new CountDownLatch(1);

    private FailingRefreshLiveNodes() {
      super(failingRefreshConfig(), new UnusedHttpClient());
    }

    @Override
    void refreshDiscoveredNodes() throws IOException {
      int attempt = callCount.incrementAndGet();
      if (attempt == 1) {
        firstCall.countDown();
      } else if (attempt == 2) {
        secondCall.countDown();
      }
      throw new IOException("simulated polling failure");
    }

    private static AlternatorConfig failingRefreshConfig() {
      return AlternatorConfig.builder()
          .withSeedNode(URI.create("http://127.0.0.1:8000"))
          .withActiveRefreshIntervalMs(REFRESH_INTERVAL_MS)
          .withIdleRefreshIntervalMs(REFRESH_INTERVAL_MS)
          .build();
    }
  }

  private static final class FailingRefreshWithProbeLiveNodes extends AlternatorLiveNodes {
    private static final long PERIOD_MS = 10;

    private final CountDownLatch probeCall = new CountDownLatch(1);

    private FailingRefreshWithProbeLiveNodes() {
      super(config(), new UnusedHttpClient());
    }

    @Override
    void refreshDiscoveredNodes() throws IOException {
      throw new IOException("simulated polling failure");
    }

    @Override
    void scheduleBackgroundHealthProbes() {
      probeCall.countDown();
    }

    private static AlternatorConfig config() {
      return AlternatorConfig.builder()
          .withSeedNode(URI.create("http://127.0.0.1:8000"))
          .withActiveRefreshIntervalMs(PERIOD_MS)
          .withIdleRefreshIntervalMs(PERIOD_MS)
          .withNodeHealthConfig(
              NodeHealthConfig.builder().withDownNodeProbePeriodMs(PERIOD_MS).build())
          .build();
    }
  }

  private static final class UnusedHttpClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new AssertionError("polling client should not be used by this test");
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "unused";
    }
  }

  private static final class CloseCountingHttpClient implements SdkHttpClient {
    private final AtomicInteger closeCount = new AtomicInteger();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new AssertionError("polling client should not be used by this test");
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }

    @Override
    public String clientName() {
      return "close-counting";
    }
  }

  private static final class BlockingShutdownHttpClient implements SdkHttpClient {
    private final CountDownLatch callStarted = new CountDownLatch(1);
    private final CountDownLatch releaseCall = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          callStarted.countDown();
          while (releaseCall.getCount() > 0) {
            try {
              releaseCall.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          throw new IllegalStateException("Connection pool shut down");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "blocking-shutdown";
    }
  }

  private static final class RuntimeThenSuccessHttpClient implements SdkHttpClient {
    private final AtomicInteger calls = new AtomicInteger();
    private final CountDownLatch successfulCall = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      int callNumber = calls.incrementAndGet();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          if (callNumber == 1) {
            throw new IllegalStateException("unexpected polling failure");
          }
          successfulCall.countDown();
          byte[] body = "[\"127.0.0.2\"]".getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "runtime-then-success";
    }
  }
}
