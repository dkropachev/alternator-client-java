package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.routing.DatacenterScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

public class AlternatorLiveNodesConcurrentProbeTest {
  @Test
  public void explicitProbesRespectConfiguredConcurrencyAndReturnSnapshotOrder() throws Exception {
    GateHttpClient client = new GateHttpClient(2);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("node-d.local", "node-c.local", "node-b.local", "node-a.local"),
            NodeHealthConfig.builder().withHealthProbeConcurrency(2).build(),
            client,
            null);
    try {
      CompletableFuture<List<URI>> result = liveNodes.probeQuarantinedNodesAsync();

      assertTrue(client.firstWaveStarted.await(5, TimeUnit.SECONDS));
      assertEquals(2, client.maximumConcurrent.get());
      client.release.countDown();

      assertEquals(
          Arrays.asList(
              node("node-a.local"),
              node("node-b.local"),
              node("node-c.local"),
              node("node-d.local")),
          result.get(5, TimeUnit.SECONDS));
      assertEquals(2, client.maximumConcurrent.get());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void probeTimeoutAbortsRequestAndLaterCallsAreRejectedAfterShutdown() throws Exception {
    AbortBlockingHttpClient client = new AbortBlockingHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("node-a.local"),
            NodeHealthConfig.builder().withHealthProbeTimeoutMs(50).build(),
            client,
            null);

    assertTrue(liveNodes.probeQuarantinedNodes().isEmpty());
    assertTrue(client.aborted.await(5, TimeUnit.SECONDS));
    assertEquals(
        NodeHealthState.QUARANTINED,
        liveNodes.getNodeHealthStatus(node("node-a.local")).getState());

    assertTrue(liveNodes.shutdownAndWait());
    assertThrows(IllegalStateException.class, liveNodes::probeQuarantinedNodes);
    assertTrue(liveNodes.probeQuarantinedNodesAsync().isCompletedExceptionally());
  }

  @Test
  public void probeTimeoutCanAbortWhileResponseBodyIsBeingConsumed() throws Exception {
    PartialResponseBlockingHttpClient client = new PartialResponseBlockingHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("node-a.local"),
            NodeHealthConfig.builder().withHealthProbeTimeoutMs(50).build(),
            client,
            null);
    try {
      assertTrue(liveNodes.probeQuarantinedNodes().isEmpty());
      assertTrue(client.bodyReadStarted.await(5, TimeUnit.SECONDS));
      assertTrue(client.aborted.await(5, TimeUnit.SECONDS));
      assertTrue(liveNodes.shutdownAndWait());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void successfulTrafficSkipsQueuedBackgroundProbeOnce() throws Exception {
    FirstProbeBlockingHttpClient client = new FirstProbeBlockingHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("node-a.local", "node-b.local"),
            NodeHealthConfig.builder()
                .withHealthProbeConcurrency(1)
                .withHealthProbeTimeoutMs(5_000)
                .build(),
            client,
            null);
    try {
      liveNodes.scheduleBackgroundHealthProbes();
      assertTrue(client.firstStarted.await(5, TimeUnit.SECONDS));

      liveNodes.reportNodeResult(
          node("node-b.local"), NodeHealthObservation.TRAFFIC_SUCCESS, false);
      client.releaseFirst.countDown();
      waitForState(liveNodes, node("node-a.local"), NodeHealthState.ACTIVE);
      Thread.sleep(100);
      assertEquals(1, client.requests.size());

      for (int i = 0; i < 20 && client.requests.size() == 1; i++) {
        liveNodes.scheduleBackgroundHealthProbes();
        Thread.sleep(25);
      }
      assertTrue(client.secondStarted.await(5, TimeUnit.SECONDS));
      assertEquals("node-b.local", client.requests.get(1).host());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void boundedBackgroundAdmissionIncludesDownAndQuarantinedTiers() throws Exception {
    List<String> hosts = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      hosts.add(String.format("node-%02d.local", i));
    }
    WaveBlockingHttpClient client = new WaveBlockingHttpClient(17);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            hosts,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withHealthProbeConcurrency(1)
                .withHealthProbeTimeoutMs(5_000)
                .build(),
            client,
            null);
    Set<String> downHosts = new HashSet<>();
    Set<String> quarantinedHosts = new HashSet<>();
    for (int i = 0; i < hosts.size(); i++) {
      URI node = node(hosts.get(i));
      if (i < 20) {
        liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS, false);
        liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE, false);
        downHosts.add(hosts.get(i));
      } else {
        quarantinedHosts.add(hosts.get(i));
      }
    }

    try {
      liveNodes.scheduleBackgroundHealthProbes();
      assertTrue(client.firstStarted.await(5, TimeUnit.SECONDS));
      client.release.countDown();
      assertTrue(client.waveStarted.await(5, TimeUnit.SECONDS));

      assertTrue(client.startedHosts.stream().anyMatch(downHosts::contains));
      assertTrue(client.startedHosts.stream().anyMatch(quarantinedHosts::contains));
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void boundedBackgroundAdmissionRotatesAcrossDownNodeTail() throws Exception {
    List<String> hosts = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      hosts.add(String.format("node-%02d.local", i));
    }
    WaveBlockingHttpClient client = new WaveBlockingHttpClient(17);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            hosts,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withHealthProbeConcurrency(1)
                .withHealthProbeTimeoutMs(5_000)
                .build(),
            client,
            null);
    for (String host : hosts) {
      URI node = node(host);
      liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS, false);
      liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE, false);
    }

    try {
      liveNodes.scheduleBackgroundHealthProbes();
      assertTrue(client.firstStarted.await(5, TimeUnit.SECONDS));
      client.release.countDown();
      assertTrue(client.waveStarted.await(5, TimeUnit.SECONDS));

      long firstWaveDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (liveNodes.getNodeHealthStatus(node("node-16.local")).getConsecutiveSuccesses() == 0
          && System.nanoTime() < firstWaveDeadline) {
        Thread.sleep(10);
      }
      client.startedHosts.clear();

      liveNodes.scheduleBackgroundHealthProbes();
      long secondWaveDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (client.startedHosts.isEmpty() && System.nanoTime() < secondWaveDeadline) {
        Thread.sleep(10);
      }
      assertFalse(client.startedHosts.isEmpty());
      assertEquals("node-17.local", client.startedHosts.get(0));
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void successfulRunningProbeIsNotCancelledByTrafficSuccess() throws Exception {
    FirstProbeBlockingHttpClient client = new FirstProbeBlockingHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("node-a.local"),
            NodeHealthConfig.builder().withHealthProbeConcurrency(1).build(),
            client,
            null);
    try {
      liveNodes.scheduleBackgroundHealthProbes();
      assertTrue(client.firstStarted.await(5, TimeUnit.SECONDS));

      liveNodes.reportNodeResult(
          node("node-a.local"), NodeHealthObservation.TRAFFIC_SUCCESS, false);
      client.releaseFirst.countDown();

      waitForState(liveNodes, node("node-a.local"), NodeHealthState.ACTIVE);
      assertEquals(1, client.requests.size());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void explicitCallUpgradesQueuedSuppressedBackgroundProbe() throws Exception {
    FirstProbeBlockingHttpClient client = new FirstProbeBlockingHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("node-a.local", "node-b.local"),
            NodeHealthConfig.builder().withHealthProbeConcurrency(1).build(),
            client,
            null);
    try {
      liveNodes.scheduleBackgroundHealthProbes();
      assertTrue(client.firstStarted.await(5, TimeUnit.SECONDS));
      liveNodes.reportNodeResult(
          node("node-b.local"), NodeHealthObservation.TRAFFIC_SUCCESS, false);

      CompletableFuture<List<URI>> explicit = liveNodes.probeQuarantinedNodesAsync();
      client.releaseFirst.countDown();

      assertEquals(
          Arrays.asList(node("node-a.local"), node("node-b.local")),
          explicit.get(5, TimeUnit.SECONDS));
      assertTrue(client.secondStarted.await(5, TimeUnit.SECONDS));
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  @Test
  public void removedNodeProbeResultCannotActivateRetainedHealthRecord() throws Exception {
    TopologyAndProbeHttpClient client = new TopologyAndProbeHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList("active.local", "quarantined.local"),
            NodeHealthConfig.getDefault(),
            client,
            DatacenterScope.of("dc1", null));
    URI active = node("active.local");
    URI quarantined = node("quarantined.local");
    liveNodes.reportNodeResult(active, NodeHealthObservation.PROBE_SUCCESS, false);
    try {
      CompletableFuture<List<URI>> result = liveNodes.probeQuarantinedNodesAsync();
      assertTrue(client.quarantineProbeStarted.await(5, TimeUnit.SECONDS));

      liveNodes.refreshDiscoveredNodes();
      assertFalse(liveNodes.getDiscoveredNodes().contains(quarantined));
      client.releaseQuarantineProbe.countDown();

      assertEquals(Arrays.asList(quarantined), result.get(5, TimeUnit.SECONDS));
      assertEquals(
          NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(quarantined).getState());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  private static AlternatorLiveNodes liveNodes(
      List<String> hosts,
      NodeHealthConfig healthConfig,
      SdkHttpClient client,
      DatacenterScope scope) {
    AlternatorConfig.Builder builder =
        AlternatorConfig.builder()
            .withSeedHosts(hosts)
            .withScheme("http")
            .withPort(8080)
            .withNodeHealthConfig(healthConfig);
    if (scope != null) {
      builder.withRoutingScope(scope);
    }
    return new AlternatorLiveNodes(builder.build(), client);
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static void waitForState(
      AlternatorLiveNodes liveNodes, URI node, NodeHealthState expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (liveNodes.getNodeHealthStatus(node).getState() != expected
        && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(expected, liveNodes.getNodeHealthStatus(node).getState());
  }

  private static HttpExecuteResponse okResponse() {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(200).build())
        .build();
  }

  private static final class GateHttpClient implements SdkHttpClient {
    private final CountDownLatch firstWaveStarted;
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger concurrent = new AtomicInteger();
    private final AtomicInteger maximumConcurrent = new AtomicInteger();

    private GateHttpClient(int firstWaveSize) {
      firstWaveStarted = new CountDownLatch(firstWaveSize);
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          int current = concurrent.incrementAndGet();
          maximumConcurrent.accumulateAndGet(current, Math::max);
          firstWaveStarted.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IOException("probe gate timed out");
            }
            return okResponse();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          } finally {
            concurrent.decrementAndGet();
          }
        }

        @Override
        public void abort() {
          release.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "gate";
    }
  }

  private static final class AbortBlockingHttpClient implements SdkHttpClient {
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch aborted = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          throw new IOException("aborted");
        }

        @Override
        public void abort() {
          aborted.countDown();
          release.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "abort-blocking";
    }
  }

  private static final class PartialResponseBlockingHttpClient implements SdkHttpClient {
    private final CountDownLatch releaseBody = new CountDownLatch(1);
    private final CountDownLatch bodyReadStarted = new CountDownLatch(1);
    private final CountDownLatch aborted = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      InputStream body =
          new InputStream() {
            @Override
            public int read() throws IOException {
              bodyReadStarted.countDown();
              try {
                releaseBody.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
              }
              return -1;
            }
          };
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(body))
              .build();
        }

        @Override
        public void abort() {
          aborted.countDown();
          releaseBody.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "partial-response-blocking";
    }
  }

  private static final class FirstProbeBlockingHttpClient implements SdkHttpClient {
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();
    private final CountDownLatch firstStarted = new CountDownLatch(1);
    private final CountDownLatch secondStarted = new CountDownLatch(1);
    private final CountDownLatch releaseFirst = new CountDownLatch(1);

    @Override
    public synchronized ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      requests.add(httpRequest);
      int requestNumber = requests.size();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (requestNumber == 1) {
            firstStarted.countDown();
            try {
              releaseFirst.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          } else {
            secondStarted.countDown();
          }
          return okResponse();
        }

        @Override
        public void abort() {
          releaseFirst.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "first-blocking";
    }
  }

  private static final class WaveBlockingHttpClient implements SdkHttpClient {
    private final List<String> startedHosts = new CopyOnWriteArrayList<>();
    private final CountDownLatch firstStarted = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch waveStarted;

    private WaveBlockingHttpClient(int waveSize) {
      this.waveStarted = new CountDownLatch(waveSize);
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      String host = request.httpRequest().host();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          startedHosts.add(host);
          firstStarted.countDown();
          waveStarted.countDown();
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          }
          return okResponse();
        }

        @Override
        public void abort() {
          release.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "wave-blocking";
    }
  }

  private static final class TopologyAndProbeHttpClient implements SdkHttpClient {
    private final CountDownLatch quarantineProbeStarted = new CountDownLatch(1);
    private final CountDownLatch releaseQuarantineProbe = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if ("quarantined.local".equals(httpRequest.host())) {
            quarantineProbeStarted.countDown();
            try {
              releaseQuarantineProbe.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return okResponse();
          }
          byte[] body = "[\"active.local\"]".getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {
          releaseQuarantineProbe.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "topology-and-probe";
    }
  }
}
