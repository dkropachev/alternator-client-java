package com.scylladb.alternator;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Verifies that clients returned from build() release Alternator background resources on close. */
public class AlternatorClientCloseLifecycleTest {

  @Test
  public void testSyncBuildClientCloseStopsAlternatorLiveNodesThread() throws Exception {
    long baseline = countAlternatorLiveNodesThreads();

    DynamoDbClient client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(loopbackSeed())
            .withConnectionTimeoutMs(50)
            .build();
    waitForThreadDeltaAtLeast(baseline, 1);

    client.close();

    waitForThreadCountAtMost(baseline);
  }

  @Test
  public void testAsyncBuildClientCloseStopsAlternatorLiveNodesThread() throws Exception {
    long baseline = countAlternatorLiveNodesThreads();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(loopbackSeed())
            .withConnectionTimeoutMs(50)
            .build();
    waitForThreadDeltaAtLeast(baseline, 1);

    client.close();

    waitForThreadCountAtMost(baseline);
  }

  private static URI loopbackSeed() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return URI.create("http://127.0.0.1:" + socket.getLocalPort());
    }
  }

  private static long countAlternatorLiveNodesThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread instanceof com.scylladb.alternator.internal.AlternatorLiveNodes)
        .filter(Thread::isAlive)
        .count();
  }

  private static void waitForThreadDeltaAtLeast(long baseline, long minDelta)
      throws InterruptedException {
    waitUntil(() -> countAlternatorLiveNodesThreads() >= baseline + minDelta);
  }

  private static void waitForThreadCountAtMost(long maxCount) throws InterruptedException {
    waitUntil(() -> countAlternatorLiveNodesThreads() <= maxCount);
  }

  private static void waitUntil(Condition condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (condition.matches()) {
        return;
      }
      Thread.sleep(25);
    }
    assertTrue("Condition was not satisfied before timeout", condition.matches());
  }

  @FunctionalInterface
  private interface Condition {
    boolean matches();
  }
}
