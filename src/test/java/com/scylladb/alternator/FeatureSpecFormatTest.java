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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

/** Deterministic structure, isolation, traceability, link, and vector checks for feature specs. */
public class FeatureSpecFormatTest {
  private static final List<String> GENERIC_SECTIONS =
      Arrays.asList(
          "Purpose and scope",
          "Vocabulary",
          "Configuration and defaults",
          "Required behavior",
          "Interactions with other features",
          "Edge cases",
          "Conformance requirements");

  private static final List<String> IMPLEMENTATION_SECTIONS =
      Arrays.asList(
          "Public API",
          "Internal architecture",
          "Lifecycle and concurrency",
          "Requirement mapping",
          "Test coverage",
          "Known conformance gaps");

  private static final Pattern H1 = Pattern.compile("(?m)^# ([^#].*)$");
  private static final Pattern H2 = Pattern.compile("(?m)^## ([^#].*)$");
  private static final Pattern H3 = Pattern.compile("(?m)^### ([^#].*)$");
  private static final Pattern REQUIREMENT =
      Pattern.compile("(?m)^### ([A-Z][A-Z0-9]*-REQ-([0-9]{3})): (.+)$");
  private static final Pattern REQUIREMENT_ID = Pattern.compile("[A-Z][A-Z0-9]*-REQ-[0-9]{3}");
  private static final Pattern LINK = Pattern.compile("\\[([^]]+)]\\(([^)]+)\\)");
  private static final Pattern MAPPING_ROW =
      Pattern.compile(
          "^\\| `([A-Z][A-Z0-9]*-REQ-[0-9]{3})` \\| (.+) \\| (.+) \\| `(conformant|gap|not-applicable)` \\|$");
  private static final Pattern IMPLEMENTATION_LEAK =
      Pattern.compile(
          "(?m)(?i:\\b(?:java|javascript|typescript|golang|rust|python|kotlin|scala|swift|ruby|jvm|junit|maven)\\b|\\baws\\s+sdk\\b)"
              + "|\\bGo\\b|C\\+\\+|C#|(?i:\\.(?:java|go|rs|py|kt)\\b)|(?:^|[(/])src/");
  private static final Pattern NORMATIVE = Pattern.compile("(?i)\\b(must|may)\\b");
  private static final Pattern PACKAGE =
      Pattern.compile("(?m)^package ([A-Za-z_][A-Za-z0-9_.]*);$");

  @Test
  public void featureSpecificationsFollowRepositoryContract() throws Exception {
    Path root = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    List<String> errors = validate(root);
    assertTrue(String.join(System.lineSeparator(), errors), errors.isEmpty());
  }

  @Test
  public void genericIsolationRuleRejectsImplementationLeakage() {
    assertTrue(IMPLEMENTATION_LEAK.matcher("Built by the AWS SDK request pipeline").find());
    assertTrue(IMPLEMENTATION_LEAK.matcher("See ../src/main/example.java").find());
    assertFalse(IMPLEMENTATION_LEAK.matcher("The client must preserve HTTP headers").find());
  }

  @Test
  public void mappingGrammarRejectsUnrecognizedStatus() {
    String valid =
        "| `COMP-REQ-001` | [Code](../../src/main/Code.java) |"
            + " [Test](../../src/test/Test.java) | `gap` |";
    assertTrue(MAPPING_ROW.matcher(valid).matches());
    assertFalse(MAPPING_ROW.matcher(valid.replace("`gap`", "`unknown`")).matches());
  }

  @Test
  public void queryPlanVectorValidationRejectsEmptyAndPartialPermutations() throws Exception {
    Path file = Files.createTempFile("query-plan-vectors", ".tsv");
    try {
      List<String> errors = new ArrayList<>();
      validateQueryPlanVectors(file, errors);
      assertTrue(
          errors.toString(), errors.stream().anyMatch(error -> error.contains("no query-plan")));

      Files.write(file, Collections.singletonList("42\t10\t5,8,4,10,6,1"), StandardCharsets.UTF_8);
      errors.clear();
      validateQueryPlanVectors(file, errors);
      assertTrue(
          errors.toString(),
          errors.stream().anyMatch(error -> error.contains("complete 10-candidate permutation")));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void affinityVectorValidationRejectsMismatchedEncoding() throws Exception {
    Path file = Files.createTempFile("affinity-vectors", ".tsv");
    try {
      Files.write(file, Collections.singletonList("S\thex\t68656c6c6f\t0"), StandardCharsets.UTF_8);
      List<String> errors = new ArrayList<>();
      validateAffinityVectors(file, errors);
      assertTrue(
          errors.toString(),
          errors.stream().anyMatch(error -> error.contains("encoding does not match")));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  private static List<String> validate(Path root) throws IOException {
    List<String> errors = new ArrayList<>();
    Path specDir = root.resolve("feature-specs");
    Path implementationDir = specDir.resolve("implementation");

    Map<String, Path> genericFiles = markdownFiles(specDir, true);
    Map<String, Path> implementationFiles = markdownFiles(implementationDir, false);
    comparePairings(genericFiles, implementationFiles, errors);

    Map<String, Requirement> requirements = new LinkedHashMap<>();
    for (Map.Entry<String, Path> entry : genericFiles.entrySet()) {
      validateGeneric(entry.getKey(), entry.getValue(), specDir, requirements, errors);
    }

    Map<String, Set<TestEvidence>> evidenceByRequirement = new HashMap<>();
    for (Map.Entry<String, Path> entry : implementationFiles.entrySet()) {
      validateImplementation(
          entry.getKey(), entry.getValue(), requirements, evidenceByRequirement, errors);
    }

    validateReadme(specDir.resolve("README.md"), genericFiles.keySet(), errors);
    validateLinks(specDir, errors);
    validateEvidenceAnnotations(root, requirements.keySet(), evidenceByRequirement, errors);
    validateVectors(specDir.resolve("vectors"), requirements.keySet(), errors);
    return errors;
  }

  private static Map<String, Path> markdownFiles(Path directory, boolean excludeReadme)
      throws IOException {
    if (!Files.isDirectory(directory)) {
      return Collections.emptyMap();
    }
    try (Stream<Path> files = Files.list(directory)) {
      return files
          .filter(path -> path.getFileName().toString().endsWith(".md"))
          .filter(path -> !excludeReadme || !path.getFileName().toString().equals("README.md"))
          .sorted()
          .collect(
              Collectors.toMap(
                  FeatureSpecFormatTest::stem,
                  path -> path,
                  (left, right) -> left,
                  LinkedHashMap::new));
    }
  }

  private static void comparePairings(
      Map<String, Path> generic, Map<String, Path> implementation, List<String> errors) {
    if (!generic.keySet().equals(implementation.keySet())) {
      errors.add(
          "Feature specification pair mismatch: generic="
              + generic.keySet()
              + ", implementation="
              + implementation.keySet());
    }
  }

  private static void validateGeneric(
      String feature,
      Path file,
      Path specDir,
      Map<String, Requirement> allRequirements,
      List<String> errors)
      throws IOException {
    String text = read(file);
    requireSingleTitle(file, text, false, errors);
    requireSections(file, text, GENERIC_SECTIONS, errors);
    if (!text.contains(
        "The keywords **must**, **must not**, **should**, and **may** are normative.")) {
      errors.add(relative(file) + ": missing normative-keyword declaration");
    }

    Matcher leak = IMPLEMENTATION_LEAK.matcher(text);
    if (leak.find()) {
      errors.add(
          relative(file) + ": implementation-specific token in generic spec: " + leak.group());
    }

    Matcher links = LINK.matcher(text);
    while (links.find()) {
      String target = withoutFragment(links.group(2));
      if (isExternal(target) || target.isEmpty()) {
        continue;
      }
      Path resolved = file.getParent().resolve(target).normalize();
      boolean genericPeer = resolved.getParent() != null && resolved.getParent().equals(specDir);
      boolean vector = resolved.startsWith(specDir.resolve("vectors"));
      if (!genericPeer && !vector) {
        errors.add(
            relative(file) + ": generic spec links outside generic specs/vectors: " + target);
      }
    }

    int conformanceStart = text.indexOf("## Conformance requirements");
    Matcher matcher = REQUIREMENT.matcher(text);
    Set<String> prefixes = new LinkedHashSet<>();
    int expectedNumber = 1;
    int count = 0;
    while (matcher.find()) {
      count++;
      String id = matcher.group(1);
      prefixes.add(id.substring(0, id.indexOf("-REQ-")));
      int number = Integer.parseInt(matcher.group(2));
      if (number != expectedNumber++) {
        errors.add(relative(file) + ": requirement IDs must be contiguous from 001: " + id);
      }
      if (matcher.start() < conformanceStart) {
        errors.add(relative(file) + ": requirement outside Conformance requirements: " + id);
      }
      int bodyEnd = nextH3(text, matcher.end());
      String body = text.substring(matcher.end(), bodyEnd);
      if (!NORMATIVE.matcher(body).find()) {
        errors.add(relative(file) + ": requirement has no normative must/may statement: " + id);
      }
      Requirement previous = allRequirements.put(id, new Requirement(feature));
      if (previous != null) {
        errors.add(relative(file) + ": duplicate global requirement ID: " + id);
      }
    }
    if (count == 0) {
      errors.add(relative(file) + ": no stable requirement IDs found");
    }
    if (prefixes.size() != 1) {
      errors.add(
          relative(file) + ": a feature must use exactly one requirement prefix: " + prefixes);
    }
  }

  private static void validateImplementation(
      String feature,
      Path file,
      Map<String, Requirement> requirements,
      Map<String, Set<TestEvidence>> evidenceByRequirement,
      List<String> errors)
      throws IOException {
    String text = read(file);
    requireSingleTitle(file, text, true, errors);
    requireSections(file, text, IMPLEMENTATION_SECTIONS, errors);
    if (!text.contains("](../" + feature + ".md)")) {
      errors.add(relative(file) + ": missing backlink to generic specification");
    }

    int mappingStart = text.indexOf("## Requirement mapping");
    int mappingEnd = text.indexOf("## Test coverage", mappingStart);
    if (mappingStart < 0 || mappingEnd < 0) {
      return;
    }

    Map<String, String> statuses = new LinkedHashMap<>();
    String[] lines = text.substring(mappingStart, mappingEnd).split("\\R");
    for (String line : lines) {
      if (!line.startsWith("| `")) {
        continue;
      }
      Matcher row = MAPPING_ROW.matcher(line);
      if (!row.matches()) {
        errors.add(relative(file) + ": malformed requirement mapping row: " + line);
        continue;
      }
      String id = row.group(1);
      String code = row.group(2);
      String evidence = row.group(3);
      String status = row.group(4);
      if (statuses.put(id, status) != null) {
        errors.add(relative(file) + ": duplicate mapping row: " + id);
      }
      Requirement requirement = requirements.get(id);
      if (requirement == null || !requirement.feature.equals(feature)) {
        errors.add(relative(file) + ": unknown or cross-feature requirement: " + id);
      }
      if (!"not-applicable".equals(status)) {
        String requiredCodePath = feature.equals("ccm-integration") ? "src/test" : "src/main";
        requireMappedLink(file, id, code, requiredCodePath, errors);
      }
      boolean hasTestEvidence = LINK.matcher(evidence).find();
      if ("conformant".equals(status) || hasTestEvidence) {
        TestEvidence testEvidence = requireMappedTestEvidence(file, id, evidence, errors);
        if (testEvidence != null) {
          evidenceByRequirement
              .computeIfAbsent(id, ignored -> new LinkedHashSet<>())
              .add(testEvidence);
        }
      }
    }

    Set<String> expected =
        requirements.entrySet().stream()
            .filter(entry -> entry.getValue().feature.equals(feature))
            .map(Map.Entry::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (!statuses.keySet().equals(expected)) {
      errors.add(
          relative(file)
              + ": mapping must cover each requirement exactly once; expected="
              + expected
              + ", actual="
              + statuses.keySet());
    }

    int gapsStart = text.indexOf("## Known conformance gaps");
    String gaps = gapsStart >= 0 ? text.substring(gapsStart) : "";
    for (Map.Entry<String, String> status : statuses.entrySet()) {
      if ("gap".equals(status.getValue()) && !gaps.contains(status.getKey())) {
        errors.add(relative(file) + ": gap status lacks an explanation: " + status.getKey());
      }
    }
    Matcher mentioned = REQUIREMENT_ID.matcher(gaps);
    while (mentioned.find()) {
      if (!"gap".equals(statuses.get(mentioned.group()))) {
        errors.add(
            relative(file)
                + ": gap section references a non-gap requirement: "
                + mentioned.group());
      }
    }
  }

  private static Path requireMappedLink(
      Path file, String id, String cell, String requiredPath, List<String> errors) {
    Matcher link = LINK.matcher(cell);
    if (!link.find()) {
      errors.add(relative(file) + ": mapping cell lacks a link for " + id + ": " + cell);
      return null;
    }
    String target = withoutFragment(link.group(2));
    Path resolved = file.getParent().resolve(target).normalize();
    String normalized = resolved.toString().replace('\\', '/');
    if (!normalized.contains("/" + requiredPath + "/")) {
      errors.add(relative(file) + ": unexpected mapping target for " + id + ": " + target);
    }
    String label = link.group(1).replace("`", "");
    if (!label.equals(stem(resolved))) {
      errors.add(relative(file) + ": link label must match referenced type/file for " + id);
    }
    return resolved;
  }

  private static TestEvidence requireMappedTestEvidence(
      Path file, String id, String cell, List<String> errors) {
    Matcher link = LINK.matcher(cell);
    if (!link.find()) {
      errors.add(relative(file) + ": test evidence lacks a link for " + id + ": " + cell);
      return null;
    }
    String label = link.group(1).replace("`", "");
    int separator = label.lastIndexOf('#');
    if (separator <= 0 || separator == label.length() - 1) {
      errors.add(relative(file) + ": test evidence must name `Class#method` for " + id);
      return null;
    }
    String className = label.substring(0, separator);
    String methodName = label.substring(separator + 1);
    Path resolved = file.getParent().resolve(withoutFragment(link.group(2))).normalize();
    String normalized = resolved.toString().replace('\\', '/');
    if (!(normalized.contains("/src/test/") || normalized.contains("/src/integration-test/"))) {
      errors.add(relative(file) + ": test evidence is not a test source for " + id);
    }
    if (!className.equals(stem(resolved))) {
      errors.add(relative(file) + ": test evidence class must match its file for " + id);
    }
    return new TestEvidence(resolved.toAbsolutePath().normalize(), methodName);
  }

  private static void validateReadme(Path readme, Set<String> features, List<String> errors)
      throws IOException {
    String text = read(readme);
    for (String feature : features) {
      String generic = "](" + feature + ".md)";
      String implementation = "](implementation/" + feature + ".md)";
      if (!text.contains(generic) || !text.contains(implementation)) {
        errors.add(relative(readme) + ": missing indexed pair for " + feature);
      }
    }
  }

  private static void validateLinks(Path specDir, List<String> errors) throws IOException {
    try (Stream<Path> files = Files.walk(specDir)) {
      for (Path file :
          files.filter(path -> path.toString().endsWith(".md")).collect(Collectors.toList())) {
        String text = read(file);
        validateMarkdownBasics(file, text, errors);
        Matcher matcher = LINK.matcher(text);
        while (matcher.find()) {
          String raw = matcher.group(2);
          String target = withoutFragment(raw);
          if (isExternal(target) || target.isEmpty()) {
            continue;
          }
          Path resolved = file.getParent().resolve(target).normalize();
          if (!Files.exists(resolved)) {
            errors.add(relative(file) + ": broken relative link: " + raw);
            continue;
          }
          int hash = raw.indexOf('#');
          if (hash >= 0 && resolved.toString().endsWith(".md")) {
            String fragment = raw.substring(hash + 1);
            if (!headingAnchors(read(resolved)).contains(fragment)) {
              errors.add(relative(file) + ": missing Markdown anchor: " + raw);
            }
          }
        }
      }
    }
  }

  private static void validateMarkdownBasics(Path file, String text, List<String> errors) {
    if (Pattern.compile("(?m)[ \\t]+$").matcher(text).find()) {
      errors.add(relative(file) + ": trailing whitespace");
    }

    long fences =
        Arrays.stream(text.split("\\R", -1)).filter(line -> line.startsWith("```")).count();
    if (fences % 2 != 0) {
      errors.add(relative(file) + ": unbalanced fenced code block");
    }

    Set<String> anchors = new HashSet<>();
    Matcher heading = Pattern.compile("(?m)^#{1,6} (.+)$").matcher(text);
    while (heading.find()) {
      String anchor = slug(heading.group(1));
      if (!anchors.add(anchor)) {
        errors.add(relative(file) + ": duplicate Markdown heading anchor: " + anchor);
      }
    }

    int tableColumns = -1;
    for (String line : text.split("\\R", -1)) {
      if (line.startsWith("|")) {
        int columns = (int) line.chars().filter(character -> character == '|').count() - 1;
        if (tableColumns < 0) {
          tableColumns = columns;
        } else if (columns != tableColumns) {
          errors.add(relative(file) + ": inconsistent Markdown table column count: " + line);
        }
      } else {
        tableColumns = -1;
      }
    }
  }

  private static void validateEvidenceAnnotations(
      Path root,
      Set<String> requirements,
      Map<String, Set<TestEvidence>> evidenceByRequirement,
      List<String> errors)
      throws IOException {
    for (Map.Entry<String, Set<TestEvidence>> entry : evidenceByRequirement.entrySet()) {
      for (TestEvidence evidence : entry.getValue()) {
        validateMappedTestMethod(entry.getKey(), evidence, errors);
      }
    }

    List<Path> testRoots =
        Arrays.asList(root.resolve("src/test"), root.resolve("src/integration-test"));
    for (Path testRoot : testRoots) {
      if (!Files.exists(testRoot)) {
        continue;
      }
      try (Stream<Path> files = Files.walk(testRoot)) {
        for (Path file :
            files.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList())) {
          Class<?> testClass = loadTestClass(file, errors);
          if (testClass == null) {
            continue;
          }
          for (Method method : testClass.getDeclaredMethods()) {
            CoversRequirements annotation = method.getAnnotation(CoversRequirements.class);
            if (annotation == null) {
              continue;
            }
            TestEvidence evidence =
                new TestEvidence(file.toAbsolutePath().normalize(), method.getName());
            Set<String> annotatedRequirements = new HashSet<>();
            for (String id : annotation.value()) {
              if (!annotatedRequirements.add(id)) {
                errors.add(
                    relative(file)
                        + ": duplicate requirement in method annotation: "
                        + id
                        + " -> "
                        + method.getName());
              } else if (!requirements.contains(id)) {
                errors.add(relative(file) + ": stale requirement annotation: " + id);
              } else if (!evidenceByRequirement
                  .getOrDefault(id, Collections.emptySet())
                  .contains(evidence)) {
                errors.add(
                    relative(file)
                        + ": annotated method is not declared as mapping evidence: "
                        + id
                        + " -> "
                        + method.getName());
              }
            }
            if (annotatedRequirements.isEmpty()) {
              errors.add(relative(file) + ": empty @CoversRequirements on " + method.getName());
            }
          }
        }
      }
    }
  }

  private static void validateMappedTestMethod(
      String requirement, TestEvidence evidence, List<String> errors) throws IOException {
    if (!Files.exists(evidence.path)) {
      errors.add(relative(evidence.path) + ": mapped test source is missing");
      return;
    }
    Class<?> testClass = loadTestClass(evidence.path, errors);
    if (testClass == null) {
      return;
    }
    for (Method method : testClass.getDeclaredMethods()) {
      if (!method.getName().equals(evidence.method)) {
        continue;
      }
      CoversRequirements annotation = method.getAnnotation(CoversRequirements.class);
      if (annotation == null || !Arrays.asList(annotation.value()).contains(requirement)) {
        errors.add(
            relative(evidence.path)
                + ": mapped method lacks @CoversRequirements(\""
                + requirement
                + "\"): "
                + evidence.method);
      }
      if (method.getAnnotation(Test.class) == null) {
        errors.add(
            relative(evidence.path)
                + ": mapped evidence method is not a @Test: "
                + evidence.method);
      }
      return;
    }
    errors.add(relative(evidence.path) + ": mapped test method does not exist: " + evidence.method);
  }

  private static Class<?> loadTestClass(Path file, List<String> errors) throws IOException {
    Matcher packageName = PACKAGE.matcher(read(file));
    if (!packageName.find()) {
      errors.add(relative(file) + ": test source has no package declaration");
      return null;
    }
    String className = packageName.group(1) + "." + stem(file);
    try {
      return Class.forName(className, false, FeatureSpecFormatTest.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      errors.add(relative(file) + ": cannot load compiled test class " + className + ": " + e);
      return null;
    }
  }

  private static void validateVectors(Path vectorDir, Set<String> requirements, List<String> errors)
      throws IOException {
    validateEndpointOrderVectors(vectorDir.resolve("endpoint-order.tsv"), errors);
    validateQueryPlanVectors(vectorDir.resolve("query-plan.tsv"), errors);
    validateQueryPlanRngConstants(vectorDir.resolve("query-plan-rng-cooked.tsv"), errors);
    validateAffinityVectors(vectorDir.resolve("affinity-hash.tsv"), errors);
    validateDefaults(vectorDir.resolve("defaults.tsv"), requirements, errors);
    validateNodeHealthTransitions(vectorDir.resolve("node-health-transitions.tsv"), errors);
  }

  private static void validateEndpointOrderVectors(Path file, List<String> errors)
      throws IOException {
    Set<String> cases = new HashSet<>();
    List<String> lines = dataLines(file, errors);
    if (lines.isEmpty()) {
      errors.add(relative(file) + ": no endpoint-order vectors found");
    }
    for (String line : lines) {
      String[] fields = line.split("\\t", -1);
      if (fields.length != 4) {
        errors.add(relative(file) + ": expected 4 tab-separated fields: " + line);
        continue;
      }
      if (!cases.add(fields[0])) {
        errors.add(relative(file) + ": duplicate endpoint-order case: " + fields[0]);
      }
      validateEndpointList(file, fields[1], "input", false, line, errors);
      validateEndpointList(file, fields[2], "expected canonical", true, line, errors);
      validateEndpointList(file, fields[3], "expected representative", false, line, errors);
      List<String> inputEndpoints = Arrays.asList(fields[1].split(",", -1));
      List<String> canonicalEndpoints = Arrays.asList(fields[2].split(",", -1));
      List<String> representativeEndpoints = Arrays.asList(fields[3].split(",", -1));
      if (canonicalEndpoints.size() != representativeEndpoints.size()) {
        errors.add(relative(file) + ": canonical and representative counts differ: " + line);
      }
      if (!inputEndpoints.containsAll(representativeEndpoints)) {
        errors.add(relative(file) + ": representative endpoint is absent from input: " + line);
      }
    }
    Set<String> requiredCases =
        new HashSet<>(
            Arrays.asList(
                "http-default-port-implicit-first",
                "http-default-port-explicit-first",
                "https-default-port",
                "scheme-and-host-case",
                "non-endpoint-components",
                "non-default-port",
                "ipv6-host"));
    if (!cases.containsAll(requiredCases)) {
      requiredCases.removeAll(cases);
      errors.add(relative(file) + ": missing canonical endpoint cases: " + requiredCases);
    }
  }

  private static void validateEndpointList(
      Path file,
      String value,
      String description,
      boolean requireUnique,
      String line,
      List<String> errors) {
    Set<String> entries = new HashSet<>();
    if (value.isEmpty()) {
      errors.add(relative(file) + ": empty " + description + " endpoint list: " + line);
      return;
    }
    for (String endpoint : value.split(",", -1)) {
      try {
        java.net.URI uri = java.net.URI.create(endpoint);
        if (uri.getScheme() == null || uri.getHost() == null) {
          errors.add(relative(file) + ": invalid " + description + " endpoint: " + endpoint);
          continue;
        }
        String canonical = canonicalEndpoint(uri);
        if (requireUnique && !endpoint.equals(canonical)) {
          errors.add(relative(file) + ": expected endpoint is not canonical: " + endpoint);
        }
        if (requireUnique && !entries.add(canonical)) {
          errors.add(relative(file) + ": duplicate expected canonical endpoint: " + line);
        }
      } catch (IllegalArgumentException e) {
        errors.add(relative(file) + ": invalid " + description + " endpoint: " + endpoint);
      }
    }
  }

  private static String canonicalEndpoint(java.net.URI endpoint) {
    String scheme = endpoint.getScheme().toLowerCase(Locale.ROOT);
    String host = endpoint.getHost().toLowerCase(Locale.ROOT);
    int port = endpoint.getPort();
    if (("http".equals(scheme) && port == 80) || ("https".equals(scheme) && port == 443)) {
      port = -1;
    }
    try {
      return new java.net.URI(scheme, null, host, port, null, null, null).toString();
    } catch (java.net.URISyntaxException e) {
      throw new IllegalArgumentException("Invalid endpoint: " + endpoint, e);
    }
  }

  private static void validateQueryPlanVectors(Path file, List<String> errors) throws IOException {
    Set<String> cases = new HashSet<>();
    List<String> lines = dataLines(file, errors);
    if (lines.isEmpty()) {
      errors.add(relative(file) + ": no query-plan vectors found");
    }
    for (String line : lines) {
      String[] fields = line.split("\\t", -1);
      if (fields.length != 3) {
        errors.add(relative(file) + ": expected 3 tab-separated fields: " + line);
        continue;
      }
      try {
        Long.parseLong(fields[0]);
        int count = Integer.parseInt(fields[1]);
        Set<Integer> selected = new HashSet<>();
        for (String value : fields[2].split(",")) {
          int index = Integer.parseInt(value);
          if (index < 1 || index > count || !selected.add(index)) {
            errors.add(relative(file) + ": invalid or duplicate candidate index: " + line);
          }
        }
        if (selected.size() != count) {
          errors.add(
              relative(file)
                  + ": expected a complete "
                  + count
                  + "-candidate permutation: "
                  + line);
        }
        if (!cases.add(fields[0] + ":" + fields[1])) {
          errors.add(relative(file) + ": duplicate seed/count case: " + line);
        }
      } catch (NumberFormatException e) {
        errors.add(relative(file) + ": invalid numeric field: " + line);
      }
    }
    Set<String> requiredCases =
        new HashSet<>(
            Arrays.asList(
                "42:10", "123:10", "999:10", "0:10", "-1:10", "12345:10", Long.MAX_VALUE + ":10"));
    if (!cases.containsAll(requiredCases)) {
      requiredCases.removeAll(cases);
      errors.add(relative(file) + ": missing canonical query-plan cases: " + requiredCases);
    }
  }

  private static void validateQueryPlanRngConstants(Path file, List<String> errors)
      throws IOException {
    List<String> lines = dataLines(file, errors);
    if (lines.size() != 607) {
      errors.add(
          relative(file) + ": expected 607 indexed generator constants, found " + lines.size());
    }
    Set<Integer> indexes = new HashSet<>();
    for (String line : lines) {
      String[] fields = line.split("\\t", -1);
      if (fields.length != 2) {
        errors.add(relative(file) + ": expected index and signed value: " + line);
        continue;
      }
      try {
        int index = Integer.parseInt(fields[0]);
        Long.parseLong(fields[1]);
        if (index < 0 || index >= 607 || !indexes.add(index)) {
          errors.add(relative(file) + ": invalid or duplicate generator index: " + line);
        }
      } catch (NumberFormatException e) {
        errors.add(relative(file) + ": invalid generator constant: " + line);
      }
    }
    for (int index = 0; index < 607; index++) {
      if (!indexes.contains(index)) {
        errors.add(relative(file) + ": missing generator constant index: " + index);
      }
    }
  }

  private static void validateAffinityVectors(Path file, List<String> errors) throws IOException {
    Set<String> cases = new HashSet<>();
    List<String> lines = dataLines(file, errors);
    if (lines.isEmpty()) {
      errors.add(relative(file) + ": no affinity-hash vectors found");
    }
    for (String line : lines) {
      String[] fields = line.split("\\t", -1);
      if (fields.length != 4) {
        errors.add(relative(file) + ": expected 4 tab-separated fields: " + line);
        continue;
      }
      if (!Arrays.asList("S", "N", "B").contains(fields[0])) {
        errors.add(relative(file) + ": invalid attribute type: " + line);
      }
      if (!("utf8".equals(fields[1]) || "hex".equals(fields[1]))) {
        errors.add(relative(file) + ": invalid payload encoding: " + line);
      }
      if (!(("S".equals(fields[0]) || "N".equals(fields[0])) && "utf8".equals(fields[1]))
          && !("B".equals(fields[0]) && "hex".equals(fields[1]))) {
        errors.add(relative(file) + ": encoding does not match attribute type: " + line);
      }
      if ("hex".equals(fields[1])
          && (!fields[2].matches("(?i)[0-9a-f]*") || fields[2].length() % 2 != 0)) {
        errors.add(relative(file) + ": invalid hexadecimal payload: " + line);
      }
      try {
        Long.parseLong(fields[3]);
      } catch (NumberFormatException e) {
        errors.add(relative(file) + ": invalid signed hash: " + line);
      }
      if (!cases.add(fields[0] + ":" + fields[1] + ":" + fields[2])) {
        errors.add(relative(file) + ": duplicate affinity vector: " + line);
      }
    }
    Set<String> requiredCases =
        new HashSet<>(
            Arrays.asList(
                "S:utf8:hello",
                "S:utf8:",
                "S:utf8:user_123",
                "S:utf8:こんにちは",
                "N:utf8:42",
                "N:utf8:-12345",
                "N:utf8:3.14159",
                "N:utf8:1.23E10",
                "B:hex:010203",
                "B:hex:",
                "B:hex:ff0080"));
    if (!cases.containsAll(requiredCases)) {
      requiredCases.removeAll(cases);
      errors.add(relative(file) + ": missing canonical affinity cases: " + requiredCases);
    }
  }

  private static void validateDefaults(Path file, Set<String> requirements, List<String> errors)
      throws IOException {
    Set<String> settings = new HashSet<>();
    for (String line : dataLines(file, errors)) {
      String[] fields = line.split("\\t", -1);
      if (fields.length != 3) {
        errors.add(relative(file) + ": expected 3 tab-separated fields: " + line);
        continue;
      }
      if (!requirements.contains(fields[0])) {
        errors.add(relative(file) + ": default references an unknown requirement: " + fields[0]);
      }
      if (!fields[1].matches("[a-z][a-z0-9_]*") || fields[2].isEmpty()) {
        errors.add(relative(file) + ": invalid setting name or empty value: " + line);
      }
      if (fields[1].endsWith("_enabled") || fields[1].endsWith("_disabled")) {
        if (!("true".equals(fields[2]) || "false".equals(fields[2]))) {
          errors.add(relative(file) + ": boolean default must be `true` or `false`: " + line);
        }
      } else if (!"request_algorithm".equals(fields[1])) {
        try {
          Long.parseLong(fields[2]);
        } catch (NumberFormatException e) {
          errors.add(relative(file) + ": numeric default is invalid: " + line);
        }
      }
      if (!settings.add(fields[0] + ":" + fields[1])) {
        errors.add(relative(file) + ": duplicate requirement setting: " + line);
      }
    }
    Set<String> requiredSettings =
        new HashSet<>(
            Arrays.asList(
                "COMP-REQ-001:request_algorithm",
                "COMP-REQ-001:minimum_request_size_bytes",
                "COMP-REQ-001:response_algorithm_count",
                "HEAD-REQ-001:header_optimization_enabled",
                "AFF-REQ-001:affinity_enabled",
                "HEALTH-REQ-001:active_failure_threshold",
                "HEALTH-REQ-001:down_recovery_threshold",
                "HEALTH-REQ-001:quarantine_promotion_threshold",
                "HEALTH-REQ-001:quarantine_failure_threshold",
                "HEALTH-REQ-001:background_probe_period_ms",
                "HEALTH-REQ-001:probe_concurrency",
                "HEALTH-REQ-001:probe_timeout_ms",
                "HEALTH-REQ-001:health_disabled"));
    if (!settings.containsAll(requiredSettings)) {
      requiredSettings.removeAll(settings);
      errors.add(relative(file) + ": missing required default settings: " + requiredSettings);
    }
  }

  private static void validateNodeHealthTransitions(Path file, List<String> errors)
      throws IOException {
    Set<String> cases = new HashSet<>();
    Set<String> observations =
        new HashSet<>(
            Arrays.asList("TRAFFIC_SUCCESS", "TRAFFIC_FAILURE", "PROBE_SUCCESS", "PROBE_FAILURE"));
    for (String line : dataLines(file, errors)) {
      String[] fields = line.split("\\t", -1);
      if (fields.length != 11) {
        errors.add(relative(file) + ": expected 11 tab-separated fields: " + line);
        continue;
      }
      if (!cases.add(fields[0])) {
        errors.add(relative(file) + ": duplicate transition case: " + fields[0]);
      }
      if (!("ACTIVE".equals(fields[1]) || "QUARANTINED".equals(fields[1]))) {
        errors.add(relative(file) + ": invalid initial state: " + line);
      }
      if (!Arrays.asList("ACTIVE", "QUARANTINED", "DOWN").contains(fields[7])) {
        errors.add(relative(file) + ": invalid expected state: " + line);
      }
      for (String observation : fields[6].split(",")) {
        if (!observations.contains(observation)) {
          errors.add(relative(file) + ": invalid observation: " + observation);
        }
      }
      try {
        for (int i = 2; i <= 5; i++) {
          if (Integer.parseInt(fields[i]) < 1) {
            errors.add(relative(file) + ": thresholds must be positive: " + line);
          }
        }
        for (int i = 8; i <= 10; i++) {
          if (Long.parseLong(fields[i]) < 0) {
            errors.add(relative(file) + ": expected counters must be non-negative: " + line);
          }
        }
      } catch (NumberFormatException e) {
        errors.add(relative(file) + ": invalid numeric transition field: " + line);
      }
    }

    Set<String> requiredCases =
        new HashSet<>(
            Arrays.asList(
                "active-failure-below",
                "active-failure-at",
                "active-success-reset",
                "active-probes-neutral",
                "quarantine-success-below",
                "quarantine-success-at",
                "quarantine-failure-below",
                "quarantine-failure-at",
                "quarantine-failure-resets-promotion",
                "quarantine-probe-success",
                "quarantine-probe-failure",
                "down-recovery-below",
                "down-recovery-at",
                "down-probe-failure-reset",
                "down-traffic-success-ignored",
                "down-traffic-failure-ignored"));
    if (!cases.containsAll(requiredCases)) {
      requiredCases.removeAll(cases);
      errors.add(relative(file) + ": missing required boundary transition cases: " + requiredCases);
    }
  }

  private static List<String> dataLines(Path file, List<String> errors) throws IOException {
    if (!Files.exists(file)) {
      errors.add(relative(file) + ": required vector file is missing");
      return Collections.emptyList();
    }
    return Files.readAllLines(file, StandardCharsets.UTF_8).stream()
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .collect(Collectors.toList());
  }

  private static void requireSections(
      Path file, String text, List<String> expected, List<String> errors) {
    List<String> actual = new ArrayList<>();
    Matcher matcher = H2.matcher(text);
    while (matcher.find()) {
      actual.add(matcher.group(1));
    }
    if (!actual.equals(expected)) {
      errors.add(relative(file) + ": expected H2 sections " + expected + " but found " + actual);
    }
  }

  private static void requireSingleTitle(
      Path file, String text, boolean implementation, List<String> errors) {
    Matcher title = H1.matcher(text);
    if (!title.find()) {
      errors.add(relative(file) + ": missing H1 title");
      return;
    }
    String value = title.group(1);
    if (title.find()) {
      errors.add(relative(file) + ": more than one H1 title");
    }
    if (implementation != value.endsWith(" implementation")) {
      errors.add(
          relative(file)
              + (implementation
                  ? ": implementation title must end with ` implementation`"
                  : ": generic title must not be an implementation title"));
    }
  }

  private static int nextH3(String text, int from) {
    Matcher next = H3.matcher(text);
    next.region(from, text.length());
    return next.find() ? next.start() : text.length();
  }

  private static Set<String> headingAnchors(String markdown) {
    Set<String> anchors = new HashSet<>();
    Matcher heading = Pattern.compile("(?m)^#{1,6} (.+)$").matcher(markdown);
    while (heading.find()) {
      anchors.add(slug(heading.group(1)));
    }
    return anchors;
  }

  private static String slug(String heading) {
    return heading
        .toLowerCase(Locale.ROOT)
        .replace("`", "")
        .replaceAll("[^\\p{L}\\p{N} _-]", "")
        .replace(' ', '-');
  }

  private static String withoutFragment(String target) {
    int hash = target.indexOf('#');
    return hash >= 0 ? target.substring(0, hash) : target;
  }

  private static boolean isExternal(String target) {
    return target.startsWith("http://")
        || target.startsWith("https://")
        || target.startsWith("mailto:");
  }

  private static String read(Path file) throws IOException {
    return Files.readString(file, StandardCharsets.UTF_8);
  }

  private static String stem(Path file) {
    return stem(file.getFileName().toString());
  }

  private static String stem(String filename) {
    int dot = filename.lastIndexOf('.');
    return dot >= 0 ? filename.substring(0, dot) : filename;
  }

  private static String relative(Path path) {
    Path root = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    Path absolute = path.toAbsolutePath().normalize();
    return absolute.startsWith(root) ? root.relativize(absolute).toString() : absolute.toString();
  }

  private static final class Requirement {
    private final String feature;

    private Requirement(String feature) {
      this.feature = feature;
    }
  }

  private static final class TestEvidence {
    private final Path path;
    private final String method;

    private TestEvidence(Path path, String method) {
      this.path = path;
      this.method = method;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof TestEvidence)) {
        return false;
      }
      TestEvidence that = (TestEvidence) other;
      return path.equals(that.path) && method.equals(that.method);
    }

    @Override
    public int hashCode() {
      return 31 * path.hashCode() + method.hashCode();
    }
  }
}
