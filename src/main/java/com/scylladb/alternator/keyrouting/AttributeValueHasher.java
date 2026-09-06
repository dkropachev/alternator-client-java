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

import java.nio.charset.StandardCharsets;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Hashes DynamoDB AttributeValue objects using MurmurHash3.
 *
 * <p>Supports the partition key types allowed by ScyllaDB Alternator:
 *
 * <ul>
 *   <li>S (String) - Type prefix 0x01 + UTF-8 bytes
 *   <li>N (Number) - Type prefix 0x02 + UTF-8 bytes of string representation
 *   <li>B (Binary) - Type prefix 0x03 + raw bytes
 * </ul>
 *
 * <p>Other DynamoDB types (BOOL, NULL, SS, NS, BS, L, M) are not supported as partition keys in
 * Alternator and will throw an {@link IllegalArgumentException}.
 *
 * <h2>Composite Partition Keys</h2>
 *
 * <p>This hasher operates on individual {@link AttributeValue} objects. For tables with composite
 * keys (partition key + sort key), only the partition key should be hashed for routing purposes,
 * since DynamoDB partitions data by partition key only. The sort key determines ordering within a
 * partition but does not affect which node stores the data.
 *
 * <p>Example for a table with composite key (user_id, timestamp):
 *
 * <pre>{@code
 * // Only hash the partition key (user_id)
 * AttributeValue partitionKey = item.get("user_id");
 * long hash = AttributeValueHasher.hash(partitionKey);
 * }</pre>
 *
 * <h2>Number Representation</h2>
 *
 * <p>Number values (N type) are hashed using their exact string representation as stored in
 * DynamoDB. This means that numerically equivalent values with different representations will
 * produce different hashes:
 *
 * <ul>
 *   <li>{@code "42"} and {@code "42.0"} produce different hashes
 *   <li>{@code "1e2"} and {@code "100"} produce different hashes
 *   <li>{@code "1.0"} and {@code "1.00"} produce different hashes
 * </ul>
 *
 * <p>This behavior preserves the exact representation stored in DynamoDB and matches how DynamoDB
 * itself handles number comparisons in certain contexts.
 *
 * <h2>Cross-Language Compatibility</h2>
 *
 * <p>This hashing implementation is designed to be compatible with other Alternator client
 * libraries (e.g., the Go client). For clients to produce identical hashes for the same partition
 * key values, all implementations must follow the same encoding format:
 *
 * <ul>
 *   <li>Type prefixes must use the exact byte values (0x01 for S, 0x02 for N, 0x03 for B)
 *   <li>Strings must be encoded as UTF-8 bytes
 *   <li>The MurmurHash3 implementation must use the x64_128 variant with seed 0, returning the
 *       first 64 bits
 * </ul>
 *
 * <p>If you are implementing a compatible hasher in another language, ensure your implementation
 * passes the same test vectors as this Java implementation.
 *
 * <h2>Performance Characteristics</h2>
 *
 * <p>Time complexity is O(n) for all supported types where n is the byte length.
 *
 * <p>Space complexity is O(n) as the entire value is converted to bytes before hashing.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public final class AttributeValueHasher {

  // Type prefix constants for partition key types
  private static final byte TYPE_STRING = 0x01;
  private static final byte TYPE_NUMBER = 0x02;
  private static final byte TYPE_BINARY = 0x03;

  private AttributeValueHasher() {}

  /**
   * Computes a hash for a DynamoDB AttributeValue.
   *
   * <p>Only S (String), N (Number), and B (Binary) types are supported, as these are the only
   * partition key types allowed by ScyllaDB Alternator.
   *
   * @param value the attribute value to hash
   * @return the hash value
   * @throws IllegalArgumentException if the attribute value type is not S, N, or B
   */
  public static long hash(AttributeValue value) {
    if (value == null) {
      return 0L;
    }

    byte[] data = toBytes(value);
    return MurmurHash3.hash(data);
  }

  /**
   * Converts an AttributeValue to bytes for hashing.
   *
   * @param value the attribute value
   * @return the byte representation with type prefix
   */
  private static byte[] toBytes(AttributeValue value) {
    // String
    if (value.s() != null) {
      byte[] strBytes = value.s().getBytes(StandardCharsets.UTF_8);
      return prependTypePrefix(TYPE_STRING, strBytes);
    }

    // Number (stored as string)
    if (value.n() != null) {
      byte[] numBytes = value.n().getBytes(StandardCharsets.UTF_8);
      return prependTypePrefix(TYPE_NUMBER, numBytes);
    }

    // Binary
    if (value.b() != null) {
      byte[] binBytes = value.b().asByteArray();
      return prependTypePrefix(TYPE_BINARY, binBytes);
    }

    throw new IllegalArgumentException(
        "Unsupported AttributeValue type. Only S (String), N (Number), and B (Binary) "
            + "are supported as partition key types in Alternator.");
  }

  /**
   * Prepends a type prefix byte to the given data.
   *
   * @param typePrefix the type prefix byte
   * @param data the data bytes
   * @return new array with type prefix followed by data
   */
  private static byte[] prependTypePrefix(byte typePrefix, byte[] data) {
    byte[] result = new byte[1 + data.length];
    result[0] = typePrefix;
    System.arraycopy(data, 0, result, 1, data.length);
    return result;
  }
}
