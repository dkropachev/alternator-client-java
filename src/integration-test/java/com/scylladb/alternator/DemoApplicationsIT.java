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
package com.scylladb.alternator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

/** Runtime smoke tests for the demo applications against the CCM-provisioned cluster. */
public class DemoApplicationsIT {
  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  @Test
  public void demo2RunsAgainstCcmCluster() throws Exception {
    runDemo(
        "com.scylladb.alternator.demo.Demo2",
        "--endpoint",
        IntegrationTestConfig.HTTP_SEED_URI.toString(),
        "--datacenter",
        IntegrationTestConfig.DATACENTER,
        "--rack",
        IntegrationTestConfig.RACK);
  }

  @Test
  public void demo3RunsAgainstCcmCluster() throws Exception {
    runDemo(
        "com.scylladb.alternator.demo.Demo3",
        "--endpoint",
        IntegrationTestConfig.HTTP_SEED_URI.toString(),
        "--datacenter",
        IntegrationTestConfig.DATACENTER,
        "--rack",
        IntegrationTestConfig.RACK,
        "--threads",
        "2");
  }

  private static void runDemo(String mainClass, String... arguments) throws Exception {
    String classPath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-cp");
    command.add(classPath);
    command.add(mainClass);
    command.addAll(Arrays.asList(arguments));

    Path outputPath = Files.createTempFile("alternator-demo-", ".log");
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(outputPath.toFile())
            .start();
    boolean exited = false;
    try {
      exited = process.waitFor(2, TimeUnit.MINUTES);
      String output = Files.readString(outputPath, StandardCharsets.UTF_8);
      assertTrue(mainClass + " timed out. Output:\n" + output, exited);
      assertEquals(mainClass + " failed. Output:\n" + output, 0, process.exitValue());
    } finally {
      if (!exited) {
        process.destroyForcibly();
        process.waitFor();
      }
      Files.deleteIfExists(outputPath);
    }
  }
}
