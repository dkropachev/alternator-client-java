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
package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.CoversRequirements;
import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier.BatchWriteRoutingTarget;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** BatchWriteItem route-affinity vectors for the shared vote-preference algorithm. */
public class BatchWriteItemKeyRouteAffinityCrossLanguageTest {

  @Test
  public void testBatchWriteItemTargetsExposePutItemsAndDeleteKeys() {
    BatchWriteItemRequest request =
        batchWrite(
            table(
                "orders",
                put(attrs("pk", s("put-key"), "data", s("value"))),
                delete(attrs("pk", s("delete-key")))));

    List<BatchWriteRoutingTarget> targets =
        KeyAffinityRequestClassifier.extractBatchWriteRoutingTargets(request);

    assertEquals(2, targets.size());
    assertEquals("orders", targets.get(0).tableName());
    assertEquals("PutRequest", targets.get(0).operation());
    assertEquals("put-key", targets.get(0).partitionKeyValue("pk").s());
    assertEquals("orders", targets.get(1).tableName());
    assertEquals("DeleteRequest", targets.get(1).operation());
    assertEquals("delete-key", targets.get(1).partitionKeyValue("pk").s());
  }

  @Test
  @CoversRequirements("AFF-REQ-005")
  public void testVotePreferenceIgnoresNonKeyAttributes() throws URISyntaxException {
    Map<String, String> pkInfo = pkInfo("orders", "pk");
    BatchWriteItemRequest first =
        batchWrite(
            table(
                "orders",
                put(attrs("pk", s("A"), "data", s("a"))),
                put(attrs("pk", s("B"), "data", s("x")))));
    BatchWriteItemRequest second =
        batchWrite(
            table(
                "orders",
                put(attrs("pk", s("A"), "data", s("x"))),
                put(attrs("pk", s("B"), "data", s("a")))));

    assertEquals(votePreferenceOrder(first, pkInfo), votePreferenceOrder(second, pkInfo));
  }

  @Test
  public void testVotePreferenceSkipsUnusableCandidates() throws URISyntaxException {
    Map<String, String> pkInfo = pkInfo("orders", "pk");
    BatchWriteItemRequest request =
        batchWrite(
            table(
                "orders",
                put(attrs("data", s("missing-pk"))),
                put(attrs("pk", AttributeValue.builder().bool(true).build())),
                put(attrs("pk", b(0x01, 0x02, 0x03)))));

    List<String> preferenceOrder = votePreferenceOrder(request, pkInfo);

    assertEquals(1, preferenceOrder.size());
    assertTrue(preferenceOrder.get(0).startsWith("node"));
  }

  private static List<String> votePreferenceOrder(
      BatchWriteItemRequest request, Map<String, String> pkInfo) throws URISyntaxException {
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes(), "http", 8000, "", "");
    Map<URI, Integer> votes = new HashMap<>();
    for (BatchWriteRoutingTarget target :
        KeyAffinityRequestClassifier.extractBatchWriteRoutingTargets(request)) {
      String pkName = pkInfo.get(target.tableName());
      if (pkName == null) {
        continue;
      }
      AttributeValue pkValue = target.partitionKeyValue(pkName);
      if (pkValue == null) {
        continue;
      }
      try {
        long hash = AttributeValueHasher.hash(pkValue);
        URI preferredNode = liveNodes.getPreferredQueryPlanNodeForHash(hash);
        if (preferredNode != null) {
          votes.merge(preferredNode, 1, Integer::sum);
        }
      } catch (IllegalArgumentException e) {
        // Unsupported partition-key types do not contribute votes.
      }
    }

    List<Map.Entry<URI, Integer>> ordered = new ArrayList<>(votes.entrySet());
    ordered.sort(
        java.util.Comparator.<Map.Entry<URI, Integer>, Integer>comparing(Map.Entry::getValue)
            .reversed()
            .thenComparing(entry -> entry.getKey().toString()));
    List<String> result = new ArrayList<>();
    for (Map.Entry<URI, Integer> entry : ordered) {
      result.add(nodeShortName(entry.getKey()));
    }
    return result;
  }

  private static List<URI> nodes() throws URISyntaxException {
    List<URI> nodes = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      nodes.add(new URI("http", null, "node" + i + ".example.com", 8000, null, null, null));
    }
    return nodes;
  }

  @SafeVarargs
  private static BatchWriteItemRequest batchWrite(Map.Entry<String, List<WriteRequest>>... tables) {
    Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
    for (Map.Entry<String, List<WriteRequest>> table : tables) {
      requestItems.put(table.getKey(), table.getValue());
    }
    return BatchWriteItemRequest.builder().requestItems(requestItems).build();
  }

  private static Map.Entry<String, List<WriteRequest>> table(
      String tableName, WriteRequest... writes) {
    return new AbstractMap.SimpleImmutableEntry<>(tableName, Arrays.asList(writes));
  }

  private static WriteRequest put(Map<String, AttributeValue> item) {
    return WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
  }

  private static WriteRequest delete(Map<String, AttributeValue> key) {
    return WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build();
  }

  private static Map<String, AttributeValue> attrs(Object... keyValues) {
    Map<String, AttributeValue> attrs = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      attrs.put((String) keyValues[i], (AttributeValue) keyValues[i + 1]);
    }
    return attrs;
  }

  private static Map<String, String> pkInfo(String tableName, String pkName) {
    Map<String, String> pkInfo = new LinkedHashMap<>();
    pkInfo.put(tableName, pkName);
    return pkInfo;
  }

  private static AttributeValue s(String value) {
    return AttributeValue.builder().s(value).build();
  }

  private static AttributeValue b(int... values) {
    byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
  }

  private static String nodeShortName(URI uri) {
    String host = uri.getHost();
    return host.substring(0, host.indexOf(".example.com"));
  }
}
