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
package com.scylladb.alternator.testinfra;

import java.io.BufferedReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

/** Subprocess entry points used by the hard-kill recovery acceptance test. */
public final class CcmCrashRecoveryProcess {
  private CcmCrashRecoveryProcess() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length == 0) {
      throw new IllegalArgumentException("A subprocess mode is required");
    }
    if ("child".equals(arguments[0])) {
      runProvisioningChild(arguments);
      return;
    }
    if ("serve".equals(arguments[0])) {
      runHttpServer(arguments);
      return;
    }
    throw new IllegalArgumentException("Unknown subprocess mode: " + arguments[0]);
  }

  private static void runProvisioningChild(String[] arguments) throws Exception {
    if (arguments.length != 5) {
      throw new IllegalArgumentException("child requires root, diagnostics, CCM, and ready paths");
    }
    Path root = Path.of(arguments[1]);
    Path diagnostics = Path.of(arguments[2]);
    String ccm = arguments[3];
    Path ready = Path.of(arguments[4]);
    CcmRunState state =
        CcmRunState.open(
            root,
            (run, manifest) ->
                new CcmProvisioner(run, diagnostics, ccm, Duration.ofSeconds(20))
                    .cleanupStaleCluster(
                        manifest.instanceId(),
                        manifest.ccmId(),
                        run.resolve("clusters").resolve(manifest.instanceId())));
    CcmProvisioner provisioner =
        new CcmProvisioner(state.runDirectory(), diagnostics, ccm, Duration.ofSeconds(20));
    TestClusterPool pool = new TestClusterPool(provisioner, 1, resources -> {}, state);
    ClusterSpec spec =
        new ClusterSpec()
            .withTopology(ClusterTopology.singleDatacenter(1))
            .withTransports(AlternatorTransport.HTTP);
    PrivateClusterLease lease = pool.provisionPrivate(spec);
    PhysicalTestCluster cluster = (PhysicalTestCluster) lease.cluster();
    Path serverPid = cluster.ccmDirectory().resolve(".fake-server-pid");
    String[] serverIdentity =
        Files.readString(serverPid, StandardCharsets.US_ASCII).trim().split(" ", -1);
    String result =
        "run_directory="
            + state.runDirectory()
            + "\ninstance_id="
            + cluster.instanceId()
            + "\nccm_id="
            + cluster.ccmId()
            + "\nserver_pid="
            + serverIdentity[0]
            + "\nserver_start_ticks="
            + serverIdentity[1]
            + "\n";
    publish(ready, result);

    // Keep the pool, lease, and owner process alive until the parent deliberately sends SIGKILL.
    while (true) {
      Thread.sleep(1000);
    }
  }

  private static void runHttpServer(String[] arguments) throws Exception {
    if (arguments.length != 5) {
      throw new IllegalArgumentException(
          "serve requires address, port, ready, and PID-record paths");
    }
    InetAddress address = InetAddress.getByName(arguments[1]);
    int port = Integer.parseInt(arguments[2]);
    Path ready = Path.of(arguments[3]);
    Path pidRecord = Path.of(arguments[4]);
    long pid = ProcessHandle.current().pid();
    String identity = pid + " " + readProcessStartTicks(pid) + "\n";
    Thread cleanup =
        new Thread(() -> deleteIdentity(pidRecord, identity), "fake-ccm-server-pid-cleanup");
    Runtime.getRuntime().addShutdownHook(cleanup);
    byte[] response =
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
            .getBytes(StandardCharsets.US_ASCII);
    try (ServerSocket server = new ServerSocket()) {
      server.bind(new InetSocketAddress(address, port));
      publish(pidRecord, identity);
      publish(ready, "ready\n");
      while (true) {
        try (Socket socket = server.accept()) {
          socket.setSoTimeout(2000);
          BufferedReader input =
              new BufferedReader(
                  new java.io.InputStreamReader(
                      socket.getInputStream(), StandardCharsets.US_ASCII));
          String line;
          while ((line = input.readLine()) != null && !line.isEmpty()) {
            // Consume the request headers before responding.
          }
          OutputStream output = socket.getOutputStream();
          output.write(response);
          output.flush();
        }
      }
    } finally {
      deleteIdentity(pidRecord, identity);
    }
  }

  private static long readProcessStartTicks(long pid) throws Exception {
    String stat =
        Files.readString(Path.of("/proc", Long.toString(pid), "stat"), StandardCharsets.US_ASCII);
    int commandEnd = stat.lastIndexOf(')');
    if (commandEnd < 0 || commandEnd + 2 >= stat.length()) {
      throw new IllegalStateException("Unable to parse process identity for PID " + pid);
    }
    String[] fields = stat.substring(commandEnd + 2).split("\\s+");
    if (fields.length <= 19) {
      throw new IllegalStateException("Unable to parse process identity for PID " + pid);
    }
    return Long.parseLong(fields[19]);
  }

  private static void deleteIdentity(Path path, String expected) {
    try {
      if (Files.isRegularFile(path)
          && expected.equals(Files.readString(path, StandardCharsets.US_ASCII))) {
        Files.deleteIfExists(path);
      }
    } catch (Exception ignored) {
      // Test cleanup also validates the process identity before retrying termination.
    }
  }

  private static void publish(Path destination, String contents) throws Exception {
    Path parent = destination.toAbsolutePath().normalize().getParent();
    Files.createDirectories(parent);
    Path temporary = Files.createTempFile(parent, ".ccm-crash-ready-", ".tmp");
    Files.writeString(temporary, contents, StandardCharsets.US_ASCII);
    try {
      Files.move(
          temporary,
          destination,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }
}
