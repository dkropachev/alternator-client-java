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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.scylladb.alternator.CoversRequirements;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.Test;

/** Crash-recovery tests for Java-owned CCM run state. */
public class CcmRunStateTest {
  @BeforeClass
  public static void requireLinux() {
    assumeTrue(
        "CCM run-state tests require Linux",
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
    assumeTrue(
        "CCM run-state tests require /usr/bin/env", Files.isExecutable(Path.of("/usr/bin/env")));
    for (String command :
        List.of("bash", "kill", "mkdir", "ps", "rm", "setsid", "sh", "sleep", "touch")) {
      assumeTrue("CCM run-state tests require " + command, executableAvailable(command));
    }
  }

  @Test(timeout = 60000)
  @CoversRequirements("CCM-REQ-006")
  public void hardKilledJvmIsRecoveredByTheNextHarnessStartup() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-hard-kill-");
    Path root = temporary.resolve("state");
    Path diagnostics = temporary.resolve("diagnostics");
    Path ready = temporary.resolve("child-ready.properties");
    Path childLog = temporary.resolve("child.log");
    Path serverLog = temporary.resolve("server.log");
    String classPath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Path java = Path.of(System.getProperty("java.home"), "bin", "java");
    Path fakeCcm = writeFakeCcm(temporary.resolve("fake-ccm"), java, classPath, serverLog);

    ProcessBuilder childBuilder =
        new ProcessBuilder(
                java.toString(),
                "-cp",
                classPath,
                CcmCrashRecoveryProcess.class.getName(),
                "child",
                root.toString(),
                diagnostics.toString(),
                fakeCcm.toString(),
                ready.toString())
            .redirectErrorStream(true)
            .redirectOutput(childLog.toFile());
    Process child = childBuilder.start();
    long serverPid = -1;
    long serverStartTicks = -1;
    Path staleRun = null;
    try {
      waitForFileOrProcessExit(ready, child, childLog, Duration.ofSeconds(30));
      Properties state = readProperties(ready);
      staleRun = Path.of(state.getProperty("run_directory"));
      String instanceId = state.getProperty("instance_id");
      int ccmId = Integer.parseInt(state.getProperty("ccm_id"));
      serverPid = Long.parseLong(state.getProperty("server_pid"));
      serverStartTicks = Long.parseLong(state.getProperty("server_start_ticks"));
      Path reservation = CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId);
      assertTrue(Files.isDirectory(staleRun));
      assertTrue(Files.isRegularFile(reservation));
      assertTrue(
          "Fake CCM node process did not remain alive",
          processMatches(serverPid, serverStartTicks, staleRun));

      child.destroyForcibly();
      assertTrue("Provisioning JVM did not terminate", child.waitFor(10, TimeUnit.SECONDS));
      assertTrue(
          "Marked node process died with its owner JVM",
          processMatches(serverPid, serverStartTicks, staleRun));

      CcmRunState replacement =
          CcmRunState.open(
              root,
              (run, manifest) ->
                  new CcmProvisioner(run, diagnostics, fakeCcm.toString(), Duration.ofSeconds(20))
                      .cleanupStaleCluster(
                          manifest.instanceId(),
                          manifest.ccmId(),
                          run.resolve("clusters").resolve(manifest.instanceId())));
      assertFalse(Files.exists(staleRun));
      assertFalse(Files.exists(reservation));
      waitForProcessExit(serverPid, serverStartTicks, staleRun, Duration.ofSeconds(10));
      assertTrue(
          "Recovered diagnostics lost CCM command output",
          diagnosticsContain(diagnostics.resolve(instanceId), "FAKE-CCM-COMMAND create"));

      CcmProvisioner provisioner =
          new CcmProvisioner(
              replacement.runDirectory(), diagnostics, fakeCcm.toString(), Duration.ofSeconds(20));
      TestClusterPool pool = new TestClusterPool(provisioner, 1, resources -> {}, replacement);
      try {
        ClusterSpec spec =
            new ClusterSpec()
                .withTopology(ClusterTopology.singleDatacenter(1))
                .withTransports(AlternatorTransport.HTTP);
        try (PrivateClusterLease lease = pool.provisionPrivate(spec)) {
          PhysicalTestCluster cluster = (PhysicalTestCluster) lease.cluster();
          assertEquals("The recovered address ID was not reusable", ccmId, cluster.ccmId());
        }
      } finally {
        pool.close();
      }
    } finally {
      cleanupCrashTestProcesses(child, root);
    }
  }

  @Test
  public void staleRunIsRecoveredOnNextStartup() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000001");
    String instanceId = "alternator-java-stale-1";
    int ccmId = 41;
    Path clusterDirectory = stale.resolve("clusters").resolve(instanceId);
    Files.createDirectories(clusterDirectory);
    Files.writeString(
        stale.resolve("owned").resolve(instanceId + ".properties"),
        "format=1\ninstance_id=" + instanceId + "\nccm_id=" + ccmId + "\n",
        StandardCharsets.US_ASCII);
    Files.writeString(
        CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId),
        stale.getFileName() + "\n",
        StandardCharsets.US_ASCII);
    AtomicInteger cleaned = new AtomicInteger();

    CcmRunState replacement =
        CcmRunState.open(
            root,
            (run, manifest) -> {
              assertEquals(instanceId, manifest.instanceId());
              assertEquals(ccmId, manifest.ccmId());
              Files.delete(run.resolve("clusters").resolve(manifest.instanceId()));
              cleaned.incrementAndGet();
            });
    try {
      assertEquals(1, cleaned.get());
      assertFalse(Files.exists(stale));
      assertFalse(Files.exists(CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId)));
    } finally {
      replacement.close();
    }
  }

  @Test
  public void activeRunIsSkippedAndMalformedOwnerIsPreserved() throws Exception {
    Path root = Files.createTempDirectory("ccm-run-owner-");
    CcmRunState active = CcmRunState.open(root, (run, manifest) -> {});
    Path malformed = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000002");
    Files.writeString(malformed.resolve("OWNER"), "not-owner-metadata\n");
    AtomicInteger cleaned = new AtomicInteger();

    CcmRunState next = CcmRunState.open(root, (run, manifest) -> cleaned.incrementAndGet());
    try {
      assertTrue(Files.isDirectory(active.runDirectory()));
      assertTrue(Files.isDirectory(malformed));
      assertNotEquals(active.runDirectory(), next.runDirectory());
      assertEquals(0, cleaned.get());
    } finally {
      next.close();
      active.close();
    }
  }

  @Test
  public void exactRunMarkerProcessesAreTerminatedBeforeCleanup() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000003");
    ProcessBuilder markedBuilder = new ProcessBuilder("sh", "-c", "exec sleep 30");
    markedBuilder.environment().put("SCYLLA_CCM_RUN_DIR", stale.toString());
    Process marked = markedBuilder.start();
    try {
      CcmRunState replacement = CcmRunState.open(root, (run, manifest) -> {});
      replacement.close();
      assertTrue(marked.waitFor(5, TimeUnit.SECONDS));
      assertFalse(marked.isAlive());
      assertFalse(Files.exists(stale));
    } finally {
      marked.destroyForcibly();
    }
  }

  @Test
  public void unrelatedNondumpableSameUidProcessDoesNotBlockCleanup() throws Exception {
    Path python = Path.of("/usr/bin/python3");
    assumeTrue("This regression test requires /usr/bin/python3", Files.isExecutable(python));
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000009");
    String instanceId = "alternator-java-nondumpable-1";
    int ccmId = 47;
    Files.createDirectory(stale.resolve("clusters").resolve(instanceId));
    writeManifestAndReservation(root, stale, instanceId, ccmId);
    Path ready = root.resolve("nondumpable-ready");
    Path processLog = root.resolve("nondumpable.log");
    ProcessBuilder builder =
        new ProcessBuilder(
                python.toString(),
                "-c",
                "import ctypes,pathlib,sys,time; "
                    + "libc=ctypes.CDLL(None,use_errno=True); "
                    + "result=libc.prctl(4,0,0,0,0); "
                    + "result == 0 or (_ for _ in ()).throw(OSError(ctypes.get_errno())); "
                    + "pathlib.Path(sys.argv[1]).write_text('ready'); time.sleep(30)",
                ready.toString())
            .redirectErrorStream(true)
            .redirectOutput(processLog.toFile());
    Process marked = builder.start();
    try {
      waitForFileOrProcessExit(ready, marked, processLog, Duration.ofSeconds(10));
      AtomicInteger cleaned = new AtomicInteger();
      CcmRunState replacement =
          CcmRunState.open(root, (run, manifest) -> cleaned.incrementAndGet());
      replacement.close();

      assertEquals(1, cleaned.get());
      assertTrue(marked.isAlive());
      assertFalse(Files.exists(stale));
      assertFalse(Files.exists(CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId)));
    } finally {
      marked.destroyForcibly();
      marked.waitFor(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void foreignProcessMentioningRunPathIsNotTerminated() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000010");
    Path ready = stale.resolve("foreign-ready");
    Process foreign =
        new ProcessBuilder("sh", "-c", "touch \"$1\"; sleep 30; :", "sh", ready.toString()).start();
    try {
      waitForFileOrProcessExit(ready, foreign, root.resolve("foreign.log"), Duration.ofSeconds(10));
      CcmRunState replacement = CcmRunState.open(root, (run, manifest) -> {});
      replacement.close();

      assertTrue(foreign.isAlive());
      assertFalse(Files.exists(stale));
    } finally {
      foreign.destroyForcibly();
      foreign.waitFor(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void opaqueProcessMentioningRunPathQuarantinesRunUntilExit() throws Exception {
    Path python = Path.of("/usr/bin/python3");
    assumeTrue("This regression test requires /usr/bin/python3", Files.isExecutable(python));
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000011");
    String instanceId = "alternator-java-nondumpable-owned-1";
    int ccmId = 48;
    Files.createDirectory(stale.resolve("clusters").resolve(instanceId));
    writeManifestAndReservation(root, stale, instanceId, ccmId);
    Path ready = stale.resolve("nondumpable-ready");
    Path processLog = root.resolve("nondumpable-owned.log");
    ProcessBuilder builder =
        new ProcessBuilder(
                python.toString(),
                "-c",
                "import ctypes,pathlib,sys,time; "
                    + "libc=ctypes.CDLL(None,use_errno=True); "
                    + "result=libc.prctl(4,0,0,0,0); "
                    + "result == 0 or (_ for _ in ()).throw(OSError(ctypes.get_errno())); "
                    + "pathlib.Path(sys.argv[1]).write_text('ready'); time.sleep(30)",
                ready.toString())
            .redirectErrorStream(true)
            .redirectOutput(processLog.toFile());
    builder.environment().put("SCYLLA_CCM_RUN_DIR", stale.toString());
    Process marked = builder.start();
    try {
      waitForFileOrProcessExit(ready, marked, processLog, Duration.ofSeconds(10));
      AtomicInteger cleaned = new AtomicInteger();
      CcmRunState replacement =
          CcmRunState.open(root, (run, manifest) -> cleaned.incrementAndGet());
      replacement.close();

      assertEquals(0, cleaned.get());
      assertTrue(marked.isAlive());
      assertTrue(Files.isDirectory(stale));
      assertTrue(Files.exists(CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId)));
    } finally {
      marked.destroyForcibly();
      marked.waitFor(10, TimeUnit.SECONDS);
    }

    AtomicInteger cleaned = new AtomicInteger();
    CcmRunState replacement = CcmRunState.open(root, (run, manifest) -> cleaned.incrementAndGet());
    replacement.close();
    assertEquals(1, cleaned.get());
    assertFalse(Files.exists(stale));
    assertFalse(Files.exists(CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId)));
  }

  @Test
  public void reservationsAreAtomicAndReleasedOnlyThroughTheirHandle() throws Exception {
    Path root = Files.createTempDirectory("ccm-run-reservation-");
    CcmRunState state = CcmRunState.open(root, (run, manifest) -> {});
    ClusterSpec spec =
        new ClusterSpec()
            .withTopology(ClusterTopology.singleDatacenter(1))
            .withTransports(AlternatorTransport.HTTP);
    CcmRunState.ClusterHandle handle = state.beginCluster("alternator-java-owner-1", spec);
    Path owner = CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), handle.ccmId());
    Path manifest = state.runDirectory().resolve("owned/alternator-java-owner-1.properties");

    assertTrue(Files.isRegularFile(owner));
    assertTrue(Files.isRegularFile(manifest));
    state.completeCluster(handle);
    assertFalse(Files.exists(owner));
    assertFalse(Files.exists(manifest));
    state.close();
  }

  @Test
  public void failedStaleCleanupLeavesTheRunAndItsIdQuarantined() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000004");
    String instanceId = "alternator-java-failed-1";
    int ccmId = 42;
    Files.createDirectory(stale.resolve("clusters").resolve(instanceId));
    Files.writeString(
        stale.resolve("owned").resolve(instanceId + ".properties"),
        "format=1\ninstance_id=" + instanceId + "\nccm_id=" + ccmId + "\n",
        StandardCharsets.US_ASCII);

    CcmRunState replacement =
        CcmRunState.open(
            root,
            (run, manifest) -> {
              throw new IOException("injected cleanup failure");
            });
    try {
      assertTrue(Files.isDirectory(stale));
      assertEquals(
          stale.getFileName() + "\n",
          Files.readString(
              CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId),
              StandardCharsets.US_ASCII));
    } finally {
      replacement.close();
    }
  }

  @Test
  public void deadRunPublicationStagingIsReconciled() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000005");
    String instanceId = "alternator-java-staging-1";
    int ccmId = 43;
    Path config = Files.createDirectory(stale.resolve("clusters").resolve(instanceId));
    Files.writeString(config.resolve("ccm-commands.log"), "complete output\n");
    Path manifest = writeManifestAndReservation(root, stale, instanceId, ccmId);
    Files.createLink(stale.resolve("owned/.owner-123.tmp"), manifest);
    Path owner = CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId);
    Path reservationStaging = root.resolve("ccm-id-locks/.owner-" + ccmId + ".owner-456.tmp");
    Files.createLink(reservationStaging, owner);

    CcmRunState replacement = CcmRunState.open(root, (run, value) -> {});
    try {
      assertFalse(Files.exists(stale));
      assertFalse(Files.exists(owner));
      assertFalse(Files.exists(reservationStaging));
    } finally {
      replacement.close();
    }
  }

  @Test
  public void unsafeUnrelatedLegacyStagingDoesNotBlockStaleRunRetirement() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000008");
    String instanceId = "alternator-java-safe-staging-1";
    int ccmId = 46;
    Path config = Files.createDirectory(stale.resolve("clusters").resolve(instanceId));
    Files.writeString(config.resolve("ccm-commands.log"), "complete output\n");
    writeManifestAndReservation(root, stale, instanceId, ccmId);
    Path unrelated = Files.writeString(root.resolve("unrelated"), "must remain\n");
    Path unsafeStaging =
        Files.createSymbolicLink(root.resolve("ccm-id-locks/.owner-987654321.tmp"), unrelated);

    CcmRunState replacement = CcmRunState.open(root, (run, value) -> {});
    try {
      assertFalse(Files.exists(stale));
      assertFalse(Files.exists(CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId)));
      assertTrue(Files.isSymbolicLink(unsafeStaging));
      assertEquals("must remain\n", Files.readString(unrelated));
    } finally {
      replacement.close();
    }
  }

  @Test
  public void unexpectedResidualConfigKeepsRunAndReservationQuarantined() throws Exception {
    Path root = initializedRoot();
    Path stale = createOwnerlessRun(root, "ccm-runtime.00000000000000000000000000000006");
    String instanceId = "alternator-java-unexpected-1";
    int ccmId = 44;
    Path config = Files.createDirectory(stale.resolve("clusters").resolve(instanceId));
    Files.writeString(config.resolve("CURRENT"), instanceId + "\n");
    writeManifestAndReservation(root, stale, instanceId, ccmId);

    CcmRunState replacement = CcmRunState.open(root, (run, value) -> {});
    try {
      assertTrue(Files.isDirectory(stale));
      assertTrue(Files.isRegularFile(stale.resolve("owned").resolve(instanceId + ".properties")));
      assertTrue(Files.isRegularFile(CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId)));
    } finally {
      replacement.close();
    }
  }

  @Test
  public void missingRetiredRunReservationWaitsForItsAddressThenIsReleased() throws Exception {
    Path root = initializedRoot();
    int ccmId = 45;
    Path owner = CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId);
    Files.writeString(
        owner, "ccm-runtime.00000000000000000000000000000007\n", StandardCharsets.US_ASCII);

    try (ServerSocketChannel occupied = ServerSocketChannel.open()) {
      occupied.bind(new InetSocketAddress("127.0." + ccmId + ".1", CcmProvisioner.STORAGE_PORT));
      CcmRunState replacement = CcmRunState.open(root, (run, value) -> {});
      replacement.close();
      assertTrue(Files.isRegularFile(owner));
    }

    CcmRunState replacement = CcmRunState.open(root, (run, value) -> {});
    try {
      assertFalse(Files.exists(owner));
    } finally {
      replacement.close();
    }
  }

  @Test
  public void broadOrBuildOwnedRootsAreRejectedBeforeTheyCanBeChanged() throws Exception {
    assertThrows(IOException.class, () -> CcmRunState.open(Path.of("/etc"), (run, manifest) -> {}));
    assertThrows(
        IOException.class, () -> CcmRunState.open(Path.of("/var/tmp"), (run, manifest) -> {}));
    Path project = Path.of(System.getProperty("basedir", System.getProperty("user.dir")));
    assertThrows(
        IOException.class,
        () -> CcmRunState.open(project.resolve("target/ccm-state"), (run, manifest) -> {}));

    Path parent = Files.createTempDirectory("ccm-root-symlink-");
    Path destination = Files.createDirectory(parent.resolve("destination"));
    Path link = Files.createSymbolicLink(parent.resolve("link"), destination);
    assertThrows(
        IOException.class,
        () -> CcmRunState.open(link.resolve("ccm-state"), (run, manifest) -> {}));
    assertFalse(Files.exists(destination.resolve("ccm-state")));
  }

  private static Path initializedRoot() throws Exception {
    Path root = Files.createTempDirectory("ccm-run-state-");
    CcmRunState initial = CcmRunState.open(root, (run, manifest) -> {});
    initial.close();
    return root;
  }

  private static Path createOwnerlessRun(Path root, String name) throws Exception {
    Path run = root.resolve("runs").resolve(name);
    Files.createDirectory(run);
    Files.createDirectory(run.resolve("owned"));
    Files.createDirectory(run.resolve("clusters"));
    return run;
  }

  private static Path writeManifestAndReservation(Path root, Path run, String instanceId, int ccmId)
      throws Exception {
    Path manifest = run.resolve("owned").resolve(instanceId + ".properties");
    Files.writeString(
        manifest,
        "format=1\ninstance_id=" + instanceId + "\nccm_id=" + ccmId + "\n",
        StandardCharsets.US_ASCII);
    Files.writeString(
        CcmRunState.idOwnerPath(root.resolve("ccm-id-locks"), ccmId),
        run.getFileName() + "\n",
        StandardCharsets.US_ASCII);
    return manifest;
  }

  private static Path writeFakeCcm(Path path, Path java, String classPath, Path serverLog)
      throws Exception {
    String script =
        "#!/usr/bin/env bash\n"
            + "set -euo pipefail\n"
            + "helper_java="
            + shellQuote(java.toString())
            + "\nhelper_classpath="
            + shellQuote(classPath)
            + "\nserver_log="
            + shellQuote(serverLog.toString())
            + "\n"
            + "printf 'FAKE-CCM-COMMAND %s\\n' \"${1:-missing}\"\n"
            + "args=(\"$@\")\n"
            + "action=${args[0]}\n"
            + "[[ $action != node* ]] || action=${args[1]}\n"
            + "config=\n"
            + "name=\n"
            + "id=\n"
            + "for ((i=0; i<${#args[@]}; i++)); do\n"
            + "  if [[ ${args[$i]} == --config-dir ]]; then\n"
            + "    config=${args[$((i + 1))]}\n"
            + "    [[ ${args[0]} != create ]] || name=${args[$((i + 2))]}\n"
            + "  elif [[ ${args[$i]} == --id ]]; then\n"
            + "    id=${args[$((i + 1))]}\n"
            + "  fi\n"
            + "done\n"
            + "case $action in\n"
            + "  create)\n"
            + "    mkdir -p -- \"$config/$name/node1/conf\"\n"
            + "    printf 'name: %s\\nnodes:\\n  - node1\\nseeds:\\n  - node1\\nconfig_options: {}\\n' \"$name\" > \"$config/$name/cluster.conf\"\n"
            + "    printf 'name: node1\\n' > \"$config/$name/node1/node.conf\"\n"
            + "    printf '{}\\n' > \"$config/$name/node1/conf/scylla.yaml\"\n"
            + "    printf '%s\\n' \"$name\" > \"$config/CURRENT\"\n"
            + "    printf '127.0.%s.1\\n' \"$id\" > \"$config/.fake-address\"\n"
            + "    ;;\n"
            + "  updateconf) ;;\n"
            + "  start)\n"
            + "    address=$(< \"$config/.fake-address\")\n"
            + "    ready=\"$config/.fake-server-ready\"\n"
            + "    pid_record=\"$config/.fake-server-pid\"\n"
            + "    rm -f -- \"$ready\"\n"
            + "    \"$helper_java\" -cp \"$helper_classpath\" "
            + CcmCrashRecoveryProcess.class.getName()
            + " serve \"$address\" 8080 \"$ready\" \"$pid_record\" >> \"$server_log\" 2>&1 &\n"
            + "    for ((attempt=0; attempt<200; attempt++)); do\n"
            + "      [[ ! -f $ready ]] || break\n"
            + "      sleep 0.01\n"
            + "    done\n"
            + "    [[ -f $ready ]]\n"
            + "    ;;\n"
            + "  remove)\n"
            + "    name=$(< \"$config/CURRENT\")\n"
            + "    if [[ -f $config/.fake-server-pid ]]; then\n"
            + "      read -r pid start_ticks < \"$config/.fake-server-pid\" || true\n"
            + "      process_matches() {\n"
            + "        [[ ${pid:-} =~ ^[1-9][0-9]*$ && ${start_ticks:-} =~ ^[1-9][0-9]*$ ]] || return 1\n"
            + "        [[ -r /proc/$pid/stat && -r /proc/$pid/environ ]] || return 1\n"
            + "        stat=$(< \"/proc/$pid/stat\") || return 1\n"
            + "        remainder=${stat##*) }\n"
            + "        read -r -a fields <<< \"$remainder\"\n"
            + "        (( ${#fields[@]} > 19 )) && [[ ${fields[19]} == $start_ticks ]] || return 1\n"
            + "        while IFS= read -r -d '' entry; do\n"
            + "          [[ $entry != \"SCYLLA_CCM_RUN_DIR=$SCYLLA_CCM_RUN_DIR\" ]] || return 0\n"
            + "        done < \"/proc/$pid/environ\"\n"
            + "        return 1\n"
            + "      }\n"
            + "      if process_matches; then\n"
            + "        kill -TERM \"$pid\" 2>/dev/null || true\n"
            + "        for ((attempt=0; attempt<100; attempt++)); do\n"
            + "          process_matches || break\n"
            + "          sleep 0.01\n"
            + "        done\n"
            + "        process_matches && kill -KILL \"$pid\" 2>/dev/null || true\n"
            + "      fi\n"
            + "    fi\n"
            + "    rm -rf -- \"$config/$name\"\n"
            + "    rm -f -- \"$config/CURRENT\" \"$config/.fake-address\" \"$config/.fake-server-pid\" \"$config/.fake-server-ready\"\n"
            + "    ;;\n"
            + "  *) exit 2 ;;\n"
            + "esac\n";
    Files.writeString(path, script, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
    return path;
  }

  private static String shellQuote(String value) {
    return "'" + value.replace("'", "'\\\"'\\\"'") + "'";
  }

  private static boolean executableAvailable(String name) {
    String path = System.getenv("PATH");
    if (path == null) {
      return false;
    }
    for (String directory : path.split(java.io.File.pathSeparator, -1)) {
      Path candidate = directory.isEmpty() ? Path.of(name) : Path.of(directory).resolve(name);
      if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
        return true;
      }
    }
    return false;
  }

  private static Properties readProperties(Path path) throws Exception {
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII)) {
      properties.load(reader);
    }
    return properties;
  }

  private static void waitForFileOrProcessExit(
      Path file, Process process, Path processLog, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (Files.isRegularFile(file)) {
        return;
      }
      if (!process.isAlive()) {
        throw new AssertionError(
            "Provisioning subprocess exited before publishing state:\n"
                + Files.readString(processLog, StandardCharsets.UTF_8));
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Timed out waiting for provisioning subprocess state");
  }

  private static boolean diagnosticsContain(Path root, String expected) throws Exception {
    if (!Files.isDirectory(root)) {
      return false;
    }
    try (Stream<Path> files = Files.walk(root)) {
      return files
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith("ccm-command-"))
          .anyMatch(
              path -> {
                try {
                  return Files.readString(path, StandardCharsets.UTF_8).contains(expected);
                } catch (IOException exception) {
                  throw new java.io.UncheckedIOException(exception);
                }
              });
    }
  }

  private static void cleanupCrashTestProcesses(Process child, Path root) {
    boolean[] interrupted = {Thread.interrupted()};
    boolean waitForLatePidRecord = child.isAlive();
    try {
      if (child.isAlive()) {
        child.destroyForcibly();
      }
      long childDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (child.isAlive() && System.nanoTime() < childDeadline) {
        try {
          child.waitFor(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
          interrupted[0] = true;
        }
      }

      long discoveryDeadline =
          System.nanoTime()
              + (waitForLatePidRecord
                  ? TimeUnit.SECONDS.toNanos(2)
                  : TimeUnit.MILLISECONDS.toNanos(1));
      do {
        for (Path pidRecord : findFakeServerPidRecords(root)) {
          terminateRecordedServer(pidRecord, interrupted);
        }
        if (!waitForLatePidRecord) {
          break;
        }
        sleepForCleanup(20, interrupted);
      } while (System.nanoTime() < discoveryDeadline);
    } catch (Exception cleanupFailure) {
      System.err.println("Failed to clean a CCM hard-kill test subprocess");
      cleanupFailure.printStackTrace(System.err);
    } finally {
      if (interrupted[0]) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static List<Path> findFakeServerPidRecords(Path root) throws IOException {
    if (!Files.isDirectory(root)) {
      return new ArrayList<>();
    }
    try (Stream<Path> paths = Files.walk(root)) {
      List<Path> records = new ArrayList<>();
      paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().equals(".fake-server-pid"))
          .forEach(records::add);
      return records;
    }
  }

  private static void terminateRecordedServer(Path pidRecord, boolean[] interrupted)
      throws Exception {
    String[] identity =
        Files.readString(pidRecord, StandardCharsets.US_ASCII).trim().split(" ", -1);
    if (identity.length != 2) {
      return;
    }
    long pid;
    long startTicks;
    try {
      pid = Long.parseLong(identity[0]);
      startTicks = Long.parseLong(identity[1]);
    } catch (NumberFormatException malformed) {
      return;
    }
    Path run = pidRecord.getParent().getParent().getParent();
    if (!processMatches(pid, startTicks, run)) {
      return;
    }
    ProcessHandle.of(pid).ifPresent(ProcessHandle::destroy);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (processMatches(pid, startTicks, run) && System.nanoTime() < deadline) {
      sleepForCleanup(20, interrupted);
    }
    if (processMatches(pid, startTicks, run)) {
      ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly);
    }
  }

  private static boolean processMatches(long pid, long startTicks, Path run) {
    try {
      if (pid < 1 || readProcessStartTicks(pid) != startTicks) {
        return false;
      }
      byte[] environment = Files.readAllBytes(Path.of("/proc", Long.toString(pid), "environ"));
      return containsEnvironmentEntry(
          environment, "SCYLLA_CCM_RUN_DIR=" + run.toAbsolutePath().normalize());
    } catch (IOException | RuntimeException disappeared) {
      return false;
    }
  }

  private static long readProcessStartTicks(long pid) throws IOException {
    String stat =
        Files.readString(Path.of("/proc", Long.toString(pid), "stat"), StandardCharsets.US_ASCII);
    int commandEnd = stat.lastIndexOf(')');
    if (commandEnd < 0 || commandEnd + 2 >= stat.length()) {
      throw new IOException("Unable to parse process identity for PID " + pid);
    }
    String[] fields = stat.substring(commandEnd + 2).split("\\s+");
    if (fields.length <= 19) {
      throw new IOException("Unable to parse process identity for PID " + pid);
    }
    try {
      return Long.parseLong(fields[19]);
    } catch (NumberFormatException exception) {
      throw new IOException("Unable to parse process identity for PID " + pid, exception);
    }
  }

  private static boolean containsEnvironmentEntry(byte[] environment, String expected) {
    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    int start = 0;
    for (int index = 0; index <= environment.length; index++) {
      if (index == environment.length || environment[index] == 0) {
        if (index - start == expectedBytes.length) {
          boolean equal = true;
          for (int offset = 0; offset < expectedBytes.length; offset++) {
            if (environment[start + offset] != expectedBytes[offset]) {
              equal = false;
              break;
            }
          }
          if (equal) {
            return true;
          }
        }
        start = index + 1;
      }
    }
    return false;
  }

  private static void sleepForCleanup(long milliseconds, boolean[] interrupted) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException ignored) {
      interrupted[0] = true;
    }
  }

  private static void waitForProcessExit(long pid, long startTicks, Path run, Duration timeout)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (processMatches(pid, startTicks, run) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(
        "Process " + pid + " survived stale-run recovery", processMatches(pid, startTicks, run));
  }
}
