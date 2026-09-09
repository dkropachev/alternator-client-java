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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

/** Contract tests for the Makefile-owned CCM run lifecycle. */
public class CcmMakefileTest {
  private static final Path REPOSITORY = Path.of("").toAbsolutePath().normalize();

  @BeforeClass
  public static void requireLinux() {
    assumeTrue(
        "CCM Makefile lifecycle tests require Linux",
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
  }

  @Test
  public void integrationTargetExportsOneRunToBothMavenPhases() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-run-");
    Path stateRoot = temporary.resolve("state");
    Path resultsRoot = temporary.resolve("results");
    Path observations = temporary.resolve("observations");
    Path ccm = writeCcm(temporary);
    Path maven =
        writeExecutable(
            temporary,
            "fake-maven",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "printf '%s|%s|%s|%s|%s\\n' \"$SCYLLA_CCM_PATH\" "
                + "\"$SCYLLA_CCM_RUN_DIR\" \"$SCYLLA_CCM_DIAGNOSTICS_DIR\" "
                + "\"$SCYLLA_CCM_ID_LOCK_ROOT\" \"$*\" >> \"$OBSERVATIONS\"\n");

    CommandResult result =
        runMake(
            "test-integration",
            stateRoot,
            resultsRoot,
            ccm,
            maven,
            Map.of("OBSERVATIONS", observations.toString()),
            Duration.ofSeconds(20));

    assertEquals(result.output, 0, result.exitCode);
    List<String> lines = Files.readAllLines(observations);
    assertEquals(lines.toString(), 2, lines.size());
    String[] first = lines.get(0).split("\\|", 5);
    String[] second = lines.get(1).split("\\|", 5);
    assertEquals(ccm.toRealPath().toString(), first[0]);
    assertEquals(first[1], second[1]);
    assertEquals(
        resultsRoot.resolve(Path.of(first[1]).getFileName()).resolve("diagnostics").toString(),
        first[2]);
    assertEquals(temporary.resolve("id-locks").toString(), first[3]);
    assertTrue(first[4], first[4].contains("ClusterProvisioningIT"));
    assertTrue(second[4], second[4].contains("**/*IT,!**/ClusterProvisioningIT"));
    assertFalse(Files.exists(Path.of(first[1])));
    assertTrue(
        Files.isDirectory(
            resultsRoot.resolve(Path.of(first[1]).getFileName()).resolve("diagnostics")));
  }

  @Test
  public void failedMavenPhaseStillRemovesClusterAndCopiesDiagnostics() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-failure-");
    Path stateRoot = temporary.resolve("state");
    Path resultsRoot = temporary.resolve("results");
    Path ccmCalls = temporary.resolve("ccm-calls");
    Path ccm = writeCcm(temporary);
    Path maven =
        writeExecutable(
            temporary,
            "failing-maven",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "config=\"$SCYLLA_CCM_RUN_DIR/clusters/config\"\n"
                + "mkdir -p \"$config/cluster/node1/logs\"\n"
                + "printf 'cluster\\n' > \"$config/CURRENT\"\n"
                + "printf 'name: cluster\\nnodes: [node1]\\n' > \"$config/cluster/cluster.conf\"\n"
                + "printf 'name: node1\\npid: 999999999\\n' > \"$config/cluster/node1/node.conf\"\n"
                + "printf 'diagnostic\\n' > \"$config/cluster/node1/logs/system.log\"\n"
                + "exit 23\n");

    CommandResult result =
        runMake(
            "test-integration",
            stateRoot,
            resultsRoot,
            ccm,
            maven,
            Map.of("CCM_CALLS", ccmCalls.toString()),
            Duration.ofSeconds(20));

    assertTrue(result.output, result.exitCode != 0);
    assertTrue(Files.readString(ccmCalls).contains("remove --config-dir"));
    assertFalse(hasRunDirectory(stateRoot));
    try (java.util.stream.Stream<Path> diagnostics = Files.walk(resultsRoot)) {
      assertTrue(diagnostics.anyMatch(path -> path.getFileName().toString().equals("system.log")));
    }
  }

  @Test
  public void staleTargetRecoversOwnerlessRun() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-stale-");
    Path stateRoot = Files.createDirectory(temporary.resolve("state"));
    Path resultsRoot = temporary.resolve("results");
    Path ccmCalls = temporary.resolve("ccm-calls");
    Path runDirectory = Files.createDirectory(stateRoot.resolve("ccm-runtime.stale"));
    Path nodeDirectory =
        Files.createDirectories(runDirectory.resolve("clusters/config/cluster/node1"));
    Files.createDirectories(runDirectory.resolve("clusters/config/tls/node1"));
    Files.createDirectory(runDirectory.resolve("diagnostics"));
    Files.writeString(
        nodeDirectory.getParent().resolve("cluster.conf"), "name: cluster\nnodes: [node1]\n");
    Files.writeString(runDirectory.resolve("clusters/config/CURRENT"), "cluster\n");
    Path reservation =
        Files.createFile(
            Files.createDirectory(temporary.resolve("id-locks")).resolve("7.reservation"));
    Files.writeString(reservation, runDirectory.toString() + "\n");
    Path ccm = writeCcm(temporary);

    CommandResult result =
        runMake(
            "ccm-clean-stale",
            stateRoot,
            resultsRoot,
            ccm,
            Path.of("/bin/true"),
            Map.of("CCM_CALLS", ccmCalls.toString()),
            Duration.ofSeconds(20));

    assertEquals(result.output, 0, result.exitCode);
    assertFalse(Files.exists(runDirectory));
    assertFalse(Files.exists(reservation));
    assertEquals(1, Files.readAllLines(ccmCalls).size());
    assertTrue(Files.readString(ccmCalls).contains(" cluster"));
  }

  @Test
  public void cleanupTargetRejectsRunOutsideOwnedStateRoot() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-unsafe-");
    Path stateRoot = temporary.resolve("state");
    Path outside = Files.createDirectory(temporary.resolve("ccm-runtime.outside"));

    CommandResult result =
        runMake(
            "ccm-clean-run",
            stateRoot,
            temporary.resolve("results"),
            Path.of("/bin/true"),
            Path.of("/bin/true"),
            Map.of("CCM_RUN_DIR", outside.toString()),
            Duration.ofSeconds(20));

    assertTrue(result.output, result.exitCode != 0);
    assertTrue(result.output, result.output.contains("Refusing CCM run outside"));
    assertTrue(Files.isDirectory(outside));
  }

  @Test
  public void integrationTargetRejectsStateInsideMavenTarget() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-target-state-");
    Path stateRoot = REPOSITORY.resolve("target/ccm-state-test");

    CommandResult result =
        runMake(
            "test-integration",
            stateRoot,
            temporary.resolve("results"),
            Path.of("/bin/true"),
            Path.of("/bin/true"),
            Map.of(),
            Duration.ofSeconds(20));

    assertTrue(result.output, result.exitCode != 0);
    assertTrue(result.output, result.output.contains("beneath Maven build output"));
    assertFalse(Files.exists(stateRoot));
  }

  @Test
  public void staleTargetRecoversLegacyRunUnderResults() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-legacy-");
    Path stateRoot = temporary.resolve("state");
    Path resultsRoot = Files.createDirectory(temporary.resolve("results"));
    Path runDirectory = Files.createDirectory(resultsRoot.resolve("ccm-runtime.legacy"));
    Path nodeDirectory =
        Files.createDirectories(runDirectory.resolve("clusters/config/cluster/node1"));
    Files.writeString(
        nodeDirectory.getParent().resolve("cluster.conf"), "name: cluster\nnodes: [node1]\n");
    Files.writeString(runDirectory.resolve("clusters/config/CURRENT"), "cluster\n");
    Path ccm = writeCcm(temporary);

    CommandResult result =
        runMake(
            "ccm-clean-stale",
            stateRoot,
            resultsRoot,
            ccm,
            Path.of("/bin/true"),
            Map.of(),
            Duration.ofSeconds(20));

    assertEquals(result.output, 0, result.exitCode);
    assertFalse(Files.exists(runDirectory.resolve("clusters")));
    assertTrue(Files.isDirectory(runDirectory.resolve("diagnostics")));
  }

  @Test
  public void staleScanRefusesMalformedOwnershipThatNamesALiveProcess() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-owner-");
    Path stateRoot = Files.createDirectory(temporary.resolve("state"));
    Path runDirectory = Files.createDirectory(stateRoot.resolve("ccm-runtime.active"));
    Process live = new ProcessBuilder("sleep", "30").start();
    try {
      Files.writeString(
          runDirectory.resolve("OWNER"),
          "pid=" + live.pid() + "\npid=" + live.pid() + "\nstart_ticks=invalid\n");

      CommandResult result =
          runMake(
              "ccm-clean-stale",
              stateRoot,
              temporary.resolve("results"),
              Path.of("/bin/true"),
              Path.of("/bin/true"),
              Map.of(),
              Duration.ofSeconds(20));

      assertTrue(result.output, result.exitCode != 0);
      assertTrue(result.output, result.output.contains("Refusing unverifiable live CCM ownership"));
      assertTrue(live.isAlive());
      assertTrue(Files.isDirectory(runDirectory));
    } finally {
      live.destroyForcibly();
      live.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void staleScanLeavesAValidLiveOwnerUntouched() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-live-owner-");
    Path stateRoot = Files.createDirectory(temporary.resolve("state"));
    Path runDirectory = Files.createDirectory(stateRoot.resolve("ccm-runtime.active"));
    Process live = new ProcessBuilder("sleep", "30").start();
    try {
      Files.writeString(
          runDirectory.resolve("OWNER"),
          "pid="
              + live.pid()
              + "\nstart_ticks="
              + processStartTicks(live.pid())
              + "\nboot_id="
              + Files.readString(Path.of("/proc/sys/kernel/random/boot_id")).trim()
              + "\n");

      CommandResult result =
          runMake(
              "ccm-clean-stale",
              stateRoot,
              temporary.resolve("results"),
              Path.of("/bin/true"),
              Path.of("/bin/true"),
              Map.of(),
              Duration.ofSeconds(20));

      assertEquals(result.output, 0, result.exitCode);
      assertTrue(result.output, result.output.contains("Leaving active CCM run untouched"));
      assertTrue(Files.isDirectory(runDirectory));
    } finally {
      live.destroyForcibly();
      live.waitFor(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void terminationIsForwardedAndTheMavenGroupIsReaped() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-signal-");
    Path stateRoot = temporary.resolve("state");
    Path resultsRoot = temporary.resolve("results");
    Path childPid = temporary.resolve("child-pid");
    Path ready = temporary.resolve("ready");
    Path ccm = writeCcm(temporary);
    Path maven =
        writeExecutable(
            temporary,
            "blocking-maven",
            "#!/usr/bin/env bash\n"
                + "set -u\n"
                + "echo $$ > \"$CHILD_PID\"\n"
                + "touch \"$READY\"\n"
                + "trap '' TERM\n"
                + "while :; do sleep 1; done\n");
    Process make =
        startMake(
            "test-integration",
            stateRoot,
            resultsRoot,
            ccm,
            maven,
            Map.of("CHILD_PID", childPid.toString(), "READY", ready.toString()));
    long pid = -1;
    long owner = -1;
    try {
      waitForFile(ready, Duration.ofSeconds(10));
      pid = Long.parseLong(Files.readString(childPid).trim());
      Path runDirectory = waitForRunDirectory(stateRoot, Duration.ofSeconds(10));
      owner = ownerPid(runDirectory.resolve("OWNER"));

      assertTrue(ProcessHandle.of(owner).orElseThrow().destroy());

      assertTrue("Make did not exit after TERM", make.waitFor(20, TimeUnit.SECONDS));
      assertTrue("Signal termination must fail the target", make.exitValue() != 0);
      waitForProcessToStop(pid, Duration.ofSeconds(10));
      waitForNoRunDirectory(stateRoot, Duration.ofSeconds(10));
    } finally {
      make.destroyForcibly();
      if (pid > 0) {
        ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly);
      }
      if (owner > 0) {
        ProcessHandle.of(owner).ifPresent(ProcessHandle::destroyForcibly);
      }
    }
  }

  @Test
  public void phaseCleanupReapsAChildThatClearsTheRunEnvironment() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-unmarked-child-");
    Path childPid = temporary.resolve("child-pid");
    Path ccm = writeCcm(temporary);
    Path maven =
        writeExecutable(
            temporary,
            "unmarking-maven",
            "#!/usr/bin/env bash\n"
                + "env -u SCYLLA_CCM_RUN_DIR bash -c 'trap \"\" TERM; while :; do sleep 1; done' &\n"
                + "echo $! > \"$CHILD_PID\"\n");
    long pid = -1;
    try {
      CommandResult result =
          runMake(
              "test-integration",
              temporary.resolve("state"),
              temporary.resolve("results"),
              ccm,
              maven,
              Map.of("CHILD_PID", childPid.toString()),
              Duration.ofSeconds(40));

      assertEquals(result.output, 0, result.exitCode);
      pid = Long.parseLong(Files.readString(childPid).trim());
      waitForProcessToStop(pid, Duration.ofSeconds(10));
    } finally {
      if (pid > 0) {
        ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly);
      }
    }
  }

  @Test
  public void staleCleanupReapsRecordedGroupAfterRunnerIsKilled() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-killed-runner-");
    Path stateRoot = temporary.resolve("state");
    Path resultsRoot = temporary.resolve("results");
    Path childPid = temporary.resolve("child-pid");
    Path ready = temporary.resolve("ready");
    Path ccm = writeCcm(temporary);
    Path maven =
        writeExecutable(
            temporary,
            "orphaning-maven",
            "#!/usr/bin/env bash\n"
                + "env -u SCYLLA_CCM_RUN_DIR bash -c 'trap \"\" TERM; while :; do sleep 1; done' &\n"
                + "echo $! > \"$CHILD_PID\"\n"
                + "touch \"$READY\"\n"
                + "wait\n");
    Process make =
        startMake(
            "test-integration",
            stateRoot,
            resultsRoot,
            ccm,
            maven,
            Map.of("CHILD_PID", childPid.toString(), "READY", ready.toString()));
    long child = -1;
    long owner = -1;
    try {
      waitForFile(ready, Duration.ofSeconds(10));
      child = Long.parseLong(Files.readString(childPid).trim());
      Path runDirectory = waitForRunDirectory(stateRoot, Duration.ofSeconds(10));
      assertTrue(Files.isRegularFile(runDirectory.resolve("ACTIVE_GROUP")));
      owner = ownerPid(runDirectory.resolve("OWNER"));

      ProcessHandle.of(owner).orElseThrow().destroyForcibly();
      assertTrue("Make did not notice its killed runner", make.waitFor(10, TimeUnit.SECONDS));

      CommandResult cleanup =
          runMake(
              "ccm-clean-stale",
              stateRoot,
              resultsRoot,
              ccm,
              Path.of("/bin/true"),
              Map.of(),
              Duration.ofSeconds(20));

      assertEquals(cleanup.output, 0, cleanup.exitCode);
      waitForProcessToStop(child, Duration.ofSeconds(10));
      assertFalse(Files.exists(runDirectory));
    } finally {
      make.destroyForcibly();
      if (owner > 0) {
        ProcessHandle.of(owner).ifPresent(ProcessHandle::destroyForcibly);
      }
      if (child > 0) {
        ProcessHandle.of(child).ifPresent(ProcessHandle::destroyForcibly);
      }
    }
  }

  private static CommandResult runMake(
      String target,
      Path stateRoot,
      Path resultsRoot,
      Path ccm,
      Path maven,
      Map<String, String> extraEnvironment,
      Duration timeout)
      throws Exception {
    Process process = startMake(target, stateRoot, resultsRoot, ccm, maven, extraEnvironment);
    boolean exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
    if (!exited) {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
      throw new AssertionError("Make target timed out: " + target);
    }
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new CommandResult(process.exitValue(), output);
  }

  private static Process startMake(
      String target,
      Path stateRoot,
      Path resultsRoot,
      Path ccm,
      Path maven,
      Map<String, String> extraEnvironment)
      throws Exception {
    List<String> command =
        new ArrayList<>(
            Arrays.asList(
                "make",
                "--no-print-directory",
                target,
                "mvn=" + maven,
                "SCYLLA_CCM_PATH=" + ccm,
                "SCYLLA_CCM_STATE_ROOT=" + stateRoot,
                "SCYLLA_CCM_ID_LOCK_ROOT=" + stateRoot.getParent().resolve("id-locks"),
                "CCM_RESULTS_DIR=" + resultsRoot));
    if (extraEnvironment.containsKey("CCM_RUN_DIR")) {
      command.add("CCM_RUN_DIR=" + extraEnvironment.get("CCM_RUN_DIR"));
    }
    ProcessBuilder builder = new ProcessBuilder(command).directory(REPOSITORY.toFile());
    builder.redirectErrorStream(true);
    builder.environment().putAll(new HashMap<>(extraEnvironment));
    return builder.start();
  }

  private static Path writeCcm(Path directory) throws Exception {
    return writeExecutable(
        directory,
        "fake-ccm",
        "#!/usr/bin/env bash\n"
            + "set -eu\n"
            + "printf '%s\\n' \"$*\" >> \"${CCM_CALLS:-/dev/null}\"\n"
            + "if [[ ${1:-} == remove ]]; then\n"
            + "  rm -rf -- \"$3/$4\"\n"
            + "  rm -f -- \"$3/CURRENT\"\n"
            + "fi\n");
  }

  private static Path writeExecutable(Path directory, String name, String contents)
      throws Exception {
    Path path = directory.resolve(name);
    Files.writeString(path, contents, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
    return path;
  }

  private static boolean hasRunDirectory(Path stateRoot) throws Exception {
    if (!Files.isDirectory(stateRoot)) {
      return false;
    }
    try (java.nio.file.DirectoryStream<Path> entries =
        Files.newDirectoryStream(stateRoot, "ccm-runtime.*")) {
      return entries.iterator().hasNext();
    }
  }

  private static Path waitForRunDirectory(Path stateRoot, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (Files.isDirectory(stateRoot)) {
        try (java.nio.file.DirectoryStream<Path> entries =
            Files.newDirectoryStream(stateRoot, "ccm-runtime.*")) {
          java.util.Iterator<Path> iterator = entries.iterator();
          if (iterator.hasNext()) {
            return iterator.next();
          }
        }
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Timed out waiting for CCM run state under " + stateRoot);
  }

  private static long ownerPid(Path ownerFile) throws Exception {
    for (String line : Files.readAllLines(ownerFile)) {
      if (line.startsWith("pid=")) {
        return Long.parseLong(line.substring("pid=".length()));
      }
    }
    throw new AssertionError("Missing owner PID in " + ownerFile);
  }

  private static void waitForFile(Path path, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (Files.isRegularFile(path)) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Timed out waiting for " + path);
  }

  private static void waitForProcessToStop(long pid, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (!isNonZombie(pid)) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Process " + pid + " survived Makefile cleanup");
  }

  private static void waitForNoRunDirectory(Path stateRoot, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (!hasRunDirectory(stateRoot)) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("CCM run state survived Makefile cleanup under " + stateRoot);
  }

  private static boolean isNonZombie(long pid) throws Exception {
    Path stat = Path.of("/proc", Long.toString(pid), "stat");
    if (!Files.isRegularFile(stat)) {
      return false;
    }
    String contents = Files.readString(stat);
    int commandEnd = contents.lastIndexOf(')');
    return commandEnd < 0 || contents.charAt(commandEnd + 2) != 'Z';
  }

  private static long processStartTicks(long pid) throws Exception {
    String contents = Files.readString(Path.of("/proc", Long.toString(pid), "stat"));
    int commandEnd = contents.lastIndexOf(')');
    String[] fields = contents.substring(commandEnd + 2).split(" ");
    return Long.parseLong(fields[19]);
  }

  private static final class CommandResult {
    final int exitCode;
    final String output;

    CommandResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }
}
