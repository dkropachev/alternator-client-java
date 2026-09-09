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

import com.scylladb.alternator.AlternatorDynamoDbClientWrapper;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/** Per-lease namespace that prevents tests sharing a cluster from sharing tables. */
public final class TestResourceScope {
  private static final int MAXIMUM_TABLE_NAME_LENGTH = 255;
  private static final int UNIQUE_SUFFIX_LENGTH = 33;
  private static final Duration DEFAULT_CLEANUP_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration MAXIMUM_API_CALL_TIMEOUT = Duration.ofSeconds(10);
  private static final long POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

  private final PhysicalTestCluster cluster;
  private final String prefix;
  private final Duration cleanupTimeout;

  TestResourceScope(PhysicalTestCluster cluster, String runId, long leaseId) {
    this(cluster, runId, leaseId, DEFAULT_CLEANUP_TIMEOUT);
  }

  TestResourceScope(
      PhysicalTestCluster cluster, String runId, long leaseId, Duration cleanupTimeout) {
    if (cleanupTimeout == null || cleanupTimeout.isZero() || cleanupTimeout.isNegative()) {
      throw new IllegalArgumentException("cleanupTimeout must be positive");
    }
    this.cluster = cluster;
    this.cleanupTimeout = cleanupTimeout;
    String leaseComponent = "_" + leaseId + "_";
    int maximumRunIdLength =
        MAXIMUM_TABLE_NAME_LENGTH
            - UNIQUE_SUFFIX_LENGTH
            - "java_it_".length()
            - leaseComponent.length();
    this.prefix = "java_it_" + truncate(sanitize(runId), maximumRunIdLength) + leaseComponent;
  }

  public String newTableName(String hint) {
    String suffix = "_" + UUID.randomUUID().toString().replace("-", "");
    int maximumHintLength = MAXIMUM_TABLE_NAME_LENGTH - prefix.length() - suffix.length();
    return prefix + truncate(sanitize(hint), maximumHintLength) + suffix;
  }

  void cleanup() throws Exception {
    AlternatorTransport transport =
        cluster.spec().transports().contains(AlternatorTransport.HTTP)
            ? AlternatorTransport.HTTP
            : AlternatorTransport.HTTPS;
    Duration apiCallTimeout = minimum(cleanupTimeout, MAXIMUM_API_CALL_TIMEOUT);
    try (AlternatorDynamoDbClientWrapper wrapper =
        cluster
            .clientBuilder(transport)
            .overrideConfiguration(
                configuration ->
                    configuration
                        .apiCallTimeout(apiCallTimeout)
                        .apiCallAttemptTimeout(apiCallTimeout))
            .buildWithAlternatorAPI()) {
      cleanupTables(wrapper.getClient(), prefix, cleanupTimeout);
    }
  }

  static void cleanupTables(DynamoDbClient client, String prefix, Duration timeout)
      throws InterruptedException, TimeoutException {
    long deadline = deadlineAfter(timeout);
    String startName = null;
    do {
      checkDeadline(deadline);
      ListTablesResponse response =
          client.listTables(ListTablesRequest.builder().exclusiveStartTableName(startName).build());
      checkDeadline(deadline);
      for (String tableName : response.tableNames()) {
        if (!tableName.startsWith(prefix)) {
          continue;
        }
        try {
          checkDeadline(deadline);
          client.deleteTable(request -> request.tableName(tableName));
          waitForTableDeletion(client, tableName, deadline);
        } catch (ResourceNotFoundException ignored) {
          // The table is already gone.
        }
      }
      startName = response.lastEvaluatedTableName();
    } while (startName != null && !startName.isEmpty());
  }

  private static void waitForTableDeletion(DynamoDbClient client, String tableName, long deadline)
      throws InterruptedException, TimeoutException {
    while (true) {
      checkDeadline(deadline);
      try {
        client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      } catch (ResourceNotFoundException ignored) {
        return;
      }
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        throw cleanupTimedOut();
      }
      TimeUnit.NANOSECONDS.sleep(Math.min(POLL_INTERVAL_NANOS, remaining));
    }
  }

  private static long deadlineAfter(Duration timeout) {
    long now = System.nanoTime();
    long timeoutNanos;
    try {
      timeoutNanos = timeout.toNanos();
    } catch (ArithmeticException exception) {
      return Long.MAX_VALUE;
    }
    return timeoutNanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + timeoutNanos;
  }

  private static void checkDeadline(long deadline) throws TimeoutException {
    if (System.nanoTime() - deadline >= 0) {
      throw cleanupTimedOut();
    }
  }

  private static TimeoutException cleanupTimedOut() {
    return new TimeoutException("Timed out cleaning DynamoDB tables for a CCM cluster lease");
  }

  private static Duration minimum(Duration first, Duration second) {
    return first.compareTo(second) <= 0 ? first : second;
  }

  private static String sanitize(String value) {
    StringBuilder result = new StringBuilder();
    for (char character : value.toLowerCase(Locale.ROOT).toCharArray()) {
      result.append(isAllowed(character) ? character : '_');
    }
    return result.toString();
  }

  private static boolean isAllowed(char character) {
    return (character >= 'a' && character <= 'z')
        || (character >= '0' && character <= '9')
        || character == '_'
        || character == '-'
        || character == '.';
  }

  private static String truncate(String value, int maximumLength) {
    return value.length() <= maximumLength ? value : value.substring(0, maximumLength);
  }
}
