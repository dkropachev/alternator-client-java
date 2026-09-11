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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mockito.Answers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/** Unit tests for per-lease DynamoDB resource cleanup. */
public class TestResourceScopeTest {
  @Test
  public void cleanupPaginatesBeforeDeletingScyllaCursor() throws Exception {
    List<String> tables = new ArrayList<>();
    for (int index = 0; index < 101; index++) {
      tables.add(String.format("owned_%03d", index));
    }
    DynamoDbClient client = mock(DynamoDbClient.class, Answers.CALLS_REAL_METHODS);
    doAnswer(
            invocation -> {
              ListTablesRequest request = invocation.getArgument(0);
              int start = 0;
              if (request.exclusiveStartTableName() != null) {
                int cursor = tables.indexOf(request.exclusiveStartTableName());
                if (cursor < 0) {
                  return ListTablesResponse.builder().tableNames(List.of()).build();
                }
                start = cursor + 1;
              }
              int end = Math.min(start + 100, tables.size());
              List<String> page = new ArrayList<>(tables.subList(start, end));
              ListTablesResponse.Builder response = ListTablesResponse.builder().tableNames(page);
              if (end < tables.size()) {
                response.lastEvaluatedTableName(page.get(page.size() - 1));
              }
              return response.build();
            })
        .when(client)
        .listTables(any(ListTablesRequest.class));
    doAnswer(
            invocation -> {
              tables.remove(((DeleteTableRequest) invocation.getArgument(0)).tableName());
              return DeleteTableResponse.builder().build();
            })
        .when(client)
        .deleteTable(any(DeleteTableRequest.class));
    doAnswer(
            invocation -> {
              String tableName = ((DescribeTableRequest) invocation.getArgument(0)).tableName();
              if (!tables.contains(tableName)) {
                throw ResourceNotFoundException.builder().message("table was deleted").build();
              }
              return DescribeTableResponse.builder().build();
            })
        .when(client)
        .describeTable(any(DescribeTableRequest.class));

    TestResourceScope.cleanupTables(client, "owned_", Duration.ofSeconds(5));

    assertTrue(tables.toString(), tables.isEmpty());
  }
}
