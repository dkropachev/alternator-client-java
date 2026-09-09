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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Detects the tightest host or cgroup memory limit visible to the test process. */
final class AvailableMemoryDetector {
  private static final long MIB = 1024L * 1024L;

  private AvailableMemoryDetector() {}

  static final class CgroupMemoryPaths {
    final Path maximumPath;
    final Path currentPath;

    CgroupMemoryPaths(Path maximumPath, Path currentPath) {
      this.maximumPath = maximumPath;
      this.currentPath = currentPath;
    }

    @Override
    public boolean equals(Object value) {
      if (!(value instanceof CgroupMemoryPaths)) {
        return false;
      }
      CgroupMemoryPaths other = (CgroupMemoryPaths) value;
      return maximumPath.equals(other.maximumPath) && currentPath.equals(other.currentPath);
    }

    @Override
    public int hashCode() {
      return 31 * maximumPath.hashCode() + currentPath.hashCode();
    }
  }

  static long getAvailableMemoryMiB() throws IOException {
    String configured = System.getenv("SCYLLA_CCM_AVAILABLE_MEMORY_MB");
    if (configured != null) {
      return parseConfiguredMemoryMiB(configured);
    }

    List<Long> candidates = new ArrayList<>();
    long hostAvailable = readHostAvailableMemoryMiB();
    if (hostAvailable > 0) {
      candidates.add(hostAvailable);
    }
    Long cgroupAvailable = readCgroupAvailableMemoryMiB();
    if (cgroupAvailable != null) {
      candidates.add(cgroupAvailable);
    }
    return candidates.stream()
        .min(Long::compareTo)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Unable to detect available memory; set SCYLLA_CCM_AVAILABLE_MEMORY_MB explicitly"));
  }

  static long parseConfiguredMemoryMiB(String configured) {
    try {
      long value = Long.parseLong(configured);
      if (value < 1 || !configured.matches("[0-9]+")) {
        throw new NumberFormatException();
      }
      return value;
    } catch (NumberFormatException exception) {
      throw new IllegalStateException(
          "SCYLLA_CCM_AVAILABLE_MEMORY_MB must be a positive integer", exception);
    }
  }

  static Long calculateCgroupAvailableMemoryMiB(String maximumText, String currentText) {
    if ("max".equals(maximumText)) {
      return null;
    }
    try {
      long maximum = Long.parseLong(maximumText);
      long current = Long.parseLong(currentText);
      if (maximum >= (1L << 60)) {
        return null;
      }
      return Math.max(0, maximum - current) / MIB;
    } catch (NumberFormatException exception) {
      return null;
    }
  }

  private static long readHostAvailableMemoryMiB() throws IOException {
    Path memInfo = Path.of("/proc/meminfo");
    if (!Files.isRegularFile(memInfo)) {
      return 0;
    }
    for (String line : Files.readAllLines(memInfo)) {
      if (line.startsWith("MemAvailable:")) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length >= 2) {
          try {
            return Long.parseLong(parts[1]) / 1024;
          } catch (NumberFormatException ignored) {
            return 0;
          }
        }
      }
    }
    return 0;
  }

  private static Long readCgroupAvailableMemoryMiB() throws IOException {
    Path cgroup = Path.of("/proc/self/cgroup");
    Path mountInfo = Path.of("/proc/self/mountinfo");
    if (!Files.isRegularFile(cgroup) || !Files.isRegularFile(mountInfo)) {
      return null;
    }
    Long minimum = null;
    for (CgroupMemoryPaths paths :
        resolveCgroupMemoryPaths(Files.readString(cgroup), Files.readString(mountInfo))) {
      Long available = readCgroupFiles(paths.maximumPath, paths.currentPath);
      if (available != null && (minimum == null || available < minimum)) {
        minimum = available;
      }
    }
    return minimum;
  }

  private static Long readCgroupFiles(Path maximumPath, Path currentPath) throws IOException {
    if (!Files.isRegularFile(maximumPath) || !Files.isRegularFile(currentPath)) {
      return null;
    }
    return calculateCgroupAvailableMemoryMiB(
        Files.readString(maximumPath).trim(), Files.readString(currentPath).trim());
  }

  static List<CgroupMemoryPaths> resolveCgroupMemoryPaths(String cgroupText, String mountInfoText) {
    String v2Membership = null;
    String v1Membership = null;
    for (String line : cgroupText.split("\\R")) {
      String[] fields = line.split(":", 3);
      if (fields.length != 3) {
        continue;
      }
      if (fields[0].equals("0") && fields[1].isEmpty()) {
        v2Membership = fields[2];
      }
      for (String controller : fields[1].split(",")) {
        if (controller.equals("memory")) {
          v1Membership = fields[2];
        }
      }
    }

    Set<CgroupMemoryPaths> paths = new LinkedHashSet<>();
    for (String line : mountInfoText.split("\\R")) {
      String[] fields = line.trim().split("\\s+");
      int separator = indexOf(fields, "-");
      if (separator < 6 || separator + 3 >= fields.length) {
        continue;
      }
      String fileSystem = fields[separator + 1];
      String membership;
      String maximumFile;
      String currentFile;
      if (fileSystem.equals("cgroup2") && v2Membership != null) {
        membership = v2Membership;
        maximumFile = "memory.max";
        currentFile = "memory.current";
      } else if (fileSystem.equals("cgroup")
          && v1Membership != null
          && contains(fields[separator + 3].split(","), "memory")) {
        membership = v1Membership;
        maximumFile = "memory.limit_in_bytes";
        currentFile = "memory.usage_in_bytes";
      } else {
        continue;
      }

      Path mountPoint = Path.of(decodeMountInfoPath(fields[4]));
      String relativeMembership =
          relativeCgroupMembership(decodeMountInfoPath(fields[3]), membership);
      while (true) {
        Path directory =
            relativeMembership.isEmpty() ? mountPoint : mountPoint.resolve(relativeMembership);
        paths.add(
            new CgroupMemoryPaths(directory.resolve(maximumFile), directory.resolve(currentFile)));
        if (relativeMembership.isEmpty()) {
          break;
        }
        Path parent = Path.of(relativeMembership).getParent();
        relativeMembership = parent == null ? "" : parent.toString();
      }
    }
    return new ArrayList<>(paths);
  }

  private static int indexOf(String[] values, String expected) {
    for (int index = 0; index < values.length; index++) {
      if (values[index].equals(expected)) {
        return index;
      }
    }
    return -1;
  }

  private static boolean contains(String[] values, String expected) {
    return indexOf(values, expected) >= 0;
  }

  private static String decodeMountInfoPath(String path) {
    return path.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\");
  }

  private static String relativeCgroupMembership(String root, String membership) {
    root = trimTrailingSlash(root);
    membership = trimTrailingSlash(membership);
    if (root.isEmpty() || root.equals("/")) {
      return trimLeadingSlash(membership);
    }
    if (membership.equals(root)) {
      return "";
    }
    String rootPrefix = root + "/";
    return membership.startsWith(rootPrefix)
        ? membership.substring(rootPrefix.length())
        : trimLeadingSlash(membership);
  }

  private static String trimTrailingSlash(String value) {
    int end = value.length();
    while (end > 0 && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }

  private static String trimLeadingSlash(String value) {
    int start = 0;
    while (start < value.length() && value.charAt(start) == '/') {
      start++;
    }
    return value.substring(start);
  }
}
