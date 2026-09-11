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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

/** Contract tests for the thin CCM Makefile integration. */
public class CcmMakefileTest {
  private static final Path REPOSITORY = Path.of("").toAbsolutePath().normalize();
  private static final String CCM_COMMIT = "d15a2fab9d22fffad8a30c806a7c8e1632e58aae";

  @BeforeClass
  public static void requireLinux() {
    assumeTrue(
        "CCM Makefile tests require Linux",
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
    assumeTrue(
        "CCM Makefile tests require /usr/bin/env", Files.isExecutable(Path.of("/usr/bin/env")));
    for (String command :
        List.of(
            "bash", "chmod", "dirname", "flock", "kill", "make", "mkdir", "mktemp", "mv", "openssl",
            "ps", "rm", "setsid", "sleep", "touch")) {
      assumeTrue("CCM Makefile tests require " + command, executableAvailable(command));
    }
  }

  @Test
  public void integrationTargetRunsTwoForegroundPhasesWithTheRequiredEnvironment()
      throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-phases-");
    Path observations = temporary.resolve("observations");
    Path diagnostics = temporary.resolve("results");
    Path root = temporary.resolve("state");
    Path ccm = writeWorkingCcm(temporary);
    Path maven =
        writeExecutable(
            temporary,
            "fake-maven",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "printf '%s|%s|%s|%s|%s|%s\\n' \"$INTEGRATION_TESTS\" \"$SCYLLA_VERSION\" "
                + "\"$SCYLLA_CCM_PATH\" \"$SCYLLA_CCM_ROOT\" "
                + "\"$SCYLLA_CCM_DIAGNOSTICS_DIR\" \"$*\" "
                + ">> \"$OBSERVATIONS\"\n");

    CommandResult result =
        runMake(
            "test-integration",
            List.of(
                "mvn=" + maven,
                "SCYLLA_CCM_PATH=" + ccm,
                "SCYLLA_CCM_DIAGNOSTICS_DIR=" + diagnostics),
            Map.of(
                "OBSERVATIONS", observations.toString(),
                "SCYLLA_CCM_ROOT", root.toString()),
            Duration.ofSeconds(20));

    assertEquals(result.output, 0, result.exitCode);
    List<String> lines = Files.readAllLines(observations);
    assertEquals(lines.toString(), 2, lines.size());
    String[] first = lines.get(0).split("\\|", 6);
    String[] second = lines.get(1).split("\\|", 6);
    for (String[] phase : List.of(first, second)) {
      assertEquals("true", phase[0]);
      assertEquals("release:2025.2.5", phase[1]);
      assertEquals(ccm.toString(), phase[2]);
      assertEquals(root.toString(), phase[3]);
      assertEquals(diagnostics.toString(), phase[4]);
    }
    assertTrue(first[5], first[5].contains("-Dtest=ClusterProvisioningIT"));
    assertTrue(first[5], first[5].contains("-Dtest.forkCount=1"));
    assertTrue(second[5], second[5].contains("-Dtest=**/*IT,!**/ClusterProvisioningIT"));
    assertTrue(second[5], second[5].contains("-Dtest.forkCount=1"));
    assertTrue(Files.isDirectory(diagnostics));
  }

  @Test
  public void integrationTargetRejectsMissingExternalUtilitiesBeforeMavenStarts() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-missing-utility-");
    Path ccm = writeWorkingCcm(temporary);
    Path mavenStarted = temporary.resolve("maven-started");
    Path maven =
        writeExecutable(
            temporary,
            "fake-maven",
            "#!/usr/bin/env bash\n" + "touch \"$MAVEN_STARTED\"\n" + "exit 0\n");

    for (String missing : List.of("openssl", "setsid", "kill", "ps")) {
      Path tools = Files.createDirectory(temporary.resolve("without-" + missing));
      for (String command : List.of("bash", "openssl", "setsid", "kill", "ps")) {
        if (!command.equals(missing)) {
          Files.createSymbolicLink(tools.resolve(command), resolveExecutable(command));
        }
      }
      Files.deleteIfExists(mavenStarted);

      CommandResult result =
          runMake(
              "test-integration",
              List.of("mvn=" + maven, "SCYLLA_CCM_PATH=" + ccm),
              Map.of(
                  "PATH", tools.toString(),
                  "MAVEN_STARTED", mavenStarted.toString()),
              Duration.ofSeconds(20));

      assertTrue(result.output, result.exitCode != 0);
      assertTrue(
          result.output,
          result.output.contains(
              "An external " + missing + " executable is required for CCM integration tests"));
      assertTrue("Maven started despite the failed preflight", Files.notExists(mavenStarted));
    }
  }

  @Test
  public void customCcmExecutableMayBeResolvedFromPath() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-path-override-");
    Path ccm = writeWorkingCcm(temporary);

    CommandResult result =
        runMake(
            "ccm-install",
            List.of("SCYLLA_CCM_PATH=" + ccm.getFileName()),
            Map.of("PATH", temporary + ":" + System.getenv("PATH")),
            Duration.ofSeconds(20));

    assertEquals(result.output, 0, result.exitCode);
    assertTrue(result.output, result.output.contains("Using CCM executable: " + ccm.getFileName()));
  }

  @Test
  public void brokenCachedEntryPointIsRecreatedAndReinstalled() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-broken-cache-");
    Path venv = Files.createDirectories(temporary.resolve("venv"));
    Path bin = Files.createDirectory(venv.resolve("bin"));
    Files.writeString(venv.resolve(".install-complete"), CCM_COMMIT + "\n");
    writeExecutable(bin, "ccm", "#!/missing/old-checkout/python\n");
    Path events = temporary.resolve("events");
    writeUv(temporary, false);

    CommandResult result =
        runMake(
            "ccm-install",
            List.of("SCYLLA_CCM_VENV=" + venv, "SCYLLA_CCM_PATH=" + venv.resolve("bin/ccm")),
            Map.of(
                "PATH",
                temporary + ":" + System.getenv("PATH"),
                "INSTALL_EVENTS",
                events.toString()),
            Duration.ofSeconds(20));

    assertEquals(result.output, 0, result.exitCode);
    assertEquals(List.of("venv", "pip"), Files.readAllLines(events));
    assertEquals(CCM_COMMIT, Files.readString(venv.resolve(".install-complete")).trim());
    assertEquals(
        0,
        new ProcessBuilder(venv.resolve("bin/ccm").toString(), "create", "--help")
            .start()
            .waitFor());
  }

  @Test
  public void concurrentPinnedInstallationIsSingleFlight() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-concurrent-install-");
    Path venv = temporary.resolve("venv");
    Path events = temporary.resolve("events");
    Path pipStarted = temporary.resolve("pip-started");
    Path releasePip = temporary.resolve("release-pip");
    writeUv(temporary, true);
    Map<String, String> environment =
        Map.of(
            "PATH", temporary + ":" + System.getenv("PATH"),
            "INSTALL_EVENTS", events.toString(),
            "PIP_STARTED", pipStarted.toString(),
            "RELEASE_PIP", releasePip.toString());

    Process first = startInstall(venv, environment);
    Process second = null;
    try {
      waitForFile(pipStarted, Duration.ofSeconds(10));
      second = startInstall(venv, environment);
      Thread.sleep(200);
      assertTrue("A concurrent install bypassed the file lock", second.isAlive());

      Files.writeString(releasePip, "release\n");
      assertTrue("First install did not finish", first.waitFor(10, TimeUnit.SECONDS));
      assertTrue("Second install did not finish", second.waitFor(10, TimeUnit.SECONDS));
      assertEquals(processOutput(first), 0, first.exitValue());
      assertEquals(processOutput(second), 0, second.exitValue());
      assertEquals(List.of("venv", "pip"), Files.readAllLines(events));
    } finally {
      Files.writeString(releasePip, "release\n");
      stop(first);
      if (second != null) {
        stop(second);
      }
    }
  }

  @Test
  public void anyFailedMavenPhaseMakesTheIntegrationTargetFail() throws Exception {
    Path temporary = Files.createTempDirectory("ccm-make-phase-failure-");
    Path ccm = writeWorkingCcm(temporary);
    Path counter = temporary.resolve("counter");
    Path observations = temporary.resolve("observations");
    Path maven =
        writeExecutable(
            temporary,
            "conditional-maven",
            "#!/usr/bin/env bash\n"
                + "set -eu\n"
                + "count=0\n"
                + "[[ ! -f $COUNTER ]] || count=$(< \"$COUNTER\")\n"
                + "count=$((count + 1))\n"
                + "printf '%s\\n' \"$count\" > \"$COUNTER\"\n"
                + "printf '%s\\n' \"$*\" >> \"$OBSERVATIONS\"\n"
                + "if [[ $FAILURE_PHASE == $count ]]; then exit 23; fi\n");
    List<String> variables =
        List.of(
            "mvn=" + maven,
            "SCYLLA_CCM_PATH=" + ccm,
            "SCYLLA_CCM_DIAGNOSTICS_DIR=" + temporary.resolve("results"));

    CommandResult firstFailure =
        runMake(
            "test-integration",
            variables,
            Map.of(
                "COUNTER", counter.toString(),
                "OBSERVATIONS", observations.toString(),
                "FAILURE_PHASE", "1"),
            Duration.ofSeconds(20));
    assertTrue(firstFailure.output, firstFailure.exitCode != 0);
    assertEquals(1, Files.readAllLines(observations).size());

    Files.delete(counter);
    Files.delete(observations);
    CommandResult secondFailure =
        runMake(
            "test-integration",
            variables,
            Map.of(
                "COUNTER", counter.toString(),
                "OBSERVATIONS", observations.toString(),
                "FAILURE_PHASE", "2"),
            Duration.ofSeconds(20));
    assertTrue(secondFailure.output, secondFailure.exitCode != 0);
    assertEquals(2, Files.readAllLines(observations).size());
  }

  private static Path writeWorkingCcm(Path directory) throws Exception {
    return writeExecutable(
        directory,
        "fake-ccm",
        "#!/usr/bin/env bash\n" + "[[ ${1:-} == create && ${2:-} == --help ]]\n");
  }

  private static void writeUv(Path directory, boolean blockPip) throws Exception {
    writeExecutable(
        directory,
        "uv",
        "#!/usr/bin/env bash\n"
            + "set -eu\n"
            + "if [[ $1 == venv && $2 == --clear ]]; then\n"
            + "  destination=$3\n"
            + "  rm -rf -- \"$destination\"\n"
            + "  mkdir -p -- \"$destination/bin\"\n"
            + "  printf '%s\\n' '#!/usr/bin/env bash' 'exit 0' > \"$destination/bin/python\"\n"
            + "  chmod +x \"$destination/bin/python\"\n"
            + "  printf 'venv\\n' >> \"$INSTALL_EVENTS\"\n"
            + "  exit 0\n"
            + "fi\n"
            + "if [[ $1 == pip ]]; then\n"
            + "  shift\n"
            + "  while (( $# > 0 )); do\n"
            + "    if [[ $1 == --python ]]; then python=$2; break; fi\n"
            + "    shift\n"
            + "  done\n"
            + "  destination=$(dirname \"$(dirname \"$python\")\")\n"
            + "  printf '%s\\n' '#!/usr/bin/env bash' "
            + "'[[ ${1:-} == create && ${2:-} == --help ]]' > \"$destination/bin/ccm\"\n"
            + "  chmod +x \"$destination/bin/ccm\"\n"
            + "  printf 'pip\\n' >> \"$INSTALL_EVENTS\"\n"
            + (blockPip
                ? "  touch \"$PIP_STARTED\"\n"
                    + "  while [[ ! -e $RELEASE_PIP ]]; do sleep 0.01; done\n"
                : "")
            + "  exit 0\n"
            + "fi\n"
            + "exit 2\n");
  }

  private static CommandResult runMake(
      String target, List<String> variables, Map<String, String> environment, Duration timeout)
      throws Exception {
    Process process = startMake(target, variables, environment);
    if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
      throw new AssertionError("Make target timed out: " + target);
    }
    return new CommandResult(process.exitValue(), processOutput(process));
  }

  private static Process startInstall(Path venv, Map<String, String> environment) throws Exception {
    return startMake(
        "ccm-install",
        List.of("SCYLLA_CCM_VENV=" + venv, "SCYLLA_CCM_PATH=" + venv.resolve("bin/ccm")),
        environment);
  }

  private static Process startMake(
      String target, List<String> variables, Map<String, String> environment) throws Exception {
    List<String> command = new ArrayList<>();
    command.add("make");
    command.add("--no-print-directory");
    command.add(target);
    command.addAll(variables);
    ProcessBuilder builder = new ProcessBuilder(command).directory(REPOSITORY.toFile());
    builder.redirectErrorStream(true);
    builder.environment().putAll(new HashMap<>(environment));
    return builder.start();
  }

  private static Path writeExecutable(Path directory, String name, String contents)
      throws Exception {
    Files.createDirectories(directory);
    Path path = directory.resolve(name);
    Files.writeString(path, contents, StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
    return path;
  }

  private static Path resolveExecutable(String name) throws Exception {
    String path = System.getenv("PATH");
    if (path != null) {
      for (String directory : path.split(java.io.File.pathSeparator, -1)) {
        Path candidate = directory.isEmpty() ? Path.of(name) : Path.of(directory).resolve(name);
        if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
          return candidate.toAbsolutePath().normalize();
        }
      }
    }
    throw new IllegalStateException("Executable not found on PATH: " + name);
  }

  private static boolean executableAvailable(String name) {
    try {
      resolveExecutable(name);
      return true;
    } catch (Exception unavailable) {
      return false;
    }
  }

  private static void waitForFile(Path file, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (Files.exists(file)) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Timed out waiting for " + file);
  }

  private static String processOutput(Process process) throws Exception {
    return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
  }

  private static void stop(Process process) throws Exception {
    if (!process.waitFor(2, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
    }
  }

  private static final class CommandResult {
    private final int exitCode;
    private final String output;

    private CommandResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }
}
