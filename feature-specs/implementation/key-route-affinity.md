# Key-route affinity implementation

This document connects the [key-route-affinity specification](../key-route-affinity.md) to the
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`KeyRouteAffinity`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinity.java)
defines `NONE`, `RMW`, and `ANY_WRITE`.
[`KeyRouteAffinityConfig`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfig.java)
stores the mode and optional table-to-partition-key mappings. Both client builders accept either a
mode or full configuration.

## Internal architecture

[`KeyAffinityRequestClassifier`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyAffinityRequestClassifier.java)
classifies operations and extracts keys and batch targets.
[`PartitionKeyResolver`](../../src/main/java/com/scylladb/alternator/keyrouting/PartitionKeyResolver.java)
caches metadata and runs synchronous `DescribeTable` calls on a single background executor.

[`AttributeValueHasher`](../../src/main/java/com/scylladb/alternator/keyrouting/AttributeValueHasher.java)
encodes typed values, and
[`MurmurHash3`](../../src/main/java/com/scylladb/alternator/keyrouting/MurmurHash3.java) produces the
seed. [`AffinityQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java)
selects seeded single-item plans or aggregates batch votes.

## Lifecycle and concurrency

The blocking wrapper supplies its built DynamoDB client to the resolver for background discovery.
The non-blocking wrapper has no discovery client and relies on preconfiguration. Resolver misses are
deduplicated per table. Client close shuts down the resolver before stopping live-node management
and closing transports.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `AFF-REQ-001` | [`KeyAffinityRequestClassifier`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyAffinityRequestClassifier.java) | [`FeatureSpecDefaultsTest#affinityDefaultMatchesSpecification`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java) | `conformant` |
| `AFF-REQ-002` | [`KeyRouteAffinityConfig`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfig.java) and [`PartitionKeyResolver`](../../src/main/java/com/scylladb/alternator/keyrouting/PartitionKeyResolver.java) | [`PartitionKeyResolverTest#testRetryOnModeledStructured400WithoutAwsErrorDetails`](../../src/test/java/com/scylladb/alternator/keyrouting/PartitionKeyResolverTest.java) | `gap` |
| `AFF-REQ-003` | [`AttributeValueHasher`](../../src/main/java/com/scylladb/alternator/keyrouting/AttributeValueHasher.java) | [`FeatureSpecVectorsTest#affinityHashesMatchPortableVectors`](../../src/test/java/com/scylladb/alternator/FeatureSpecVectorsTest.java) | `gap` |
| `AFF-REQ-004` | [`AffinityQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java) | [`AffinityQueryPlanInterceptorTest#testPutItemSimple_AnyWrite_Affinity`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) | `conformant` |
| `AFF-REQ-005` | [`AffinityQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java) | [`BatchWriteItemKeyRouteAffinityCrossLanguageTest#testVotePreferenceIgnoresNonKeyAttributes`](../../src/test/java/com/scylladb/alternator/keyrouting/BatchWriteItemKeyRouteAffinityCrossLanguageTest.java) | `gap` |
| `AFF-REQ-006` | [`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) | [`AffinityQueryPlanInterceptorTest#testBatchWriteItemSkipsDownPreferredNodeUsingStableKnownOrder`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) | `conformant` |
| `AFF-REQ-007` | [`AlternatorDynamoDbClientWrapper`](../../src/main/java/com/scylladb/alternator/AlternatorDynamoDbClientWrapper.java) | [`AlternatorDynamoDbClientWrapperShutdownTest#testAsyncWrapperShutsDownAffinityResolverBeforeClosingClients`](../../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientWrapperShutdownTest.java) | `gap` |

## Test coverage

- [`FeatureSpecDefaultsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java)
  executes the machine-readable disabled default.
- [`KeyRouteAffinityConfigTest`](../../src/test/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfigTest.java)
  covers modes and preconfigured metadata.
- [`PartitionKeyResolverTest`](../../src/test/java/com/scylladb/alternator/keyrouting/PartitionKeyResolverTest.java)
  covers caching, retries, cooldown, clearing, and concurrent misses.
- [`AttributeValueHasherTest`](../../src/test/java/com/scylladb/alternator/keyrouting/AttributeValueHasherTest.java)
  and
  [`AttributeValueHasherCrossLanguageTest`](../../src/test/java/com/scylladb/alternator/keyrouting/AttributeValueHasherCrossLanguageTest.java)
  cover typed inputs and fixed hash vectors.
- [`AffinityQueryPlanInterceptorTest`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java)
  covers plan selection, batch voting, ties, fallback, and health interactions.
- [`LazyQueryPlanCrossLanguageTest`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java)
  covers seeded endpoint orders.

## Known conformance gaps

- `AFF-REQ-003`: The hasher checks `S`, `N`, and `B` fields in order and hashes the first present.
  An invalid value containing multiple supported fields does not use random fallback and lacks a
  malformed-value test.
- `AFF-REQ-005`: The batch classifier adds two targets when an invalid write contains both put and
  delete shapes and dereferences a null write-list entry instead of safely skipping it.
- `AFF-REQ-002`: The bulk preconfiguration helper retains null keys and values, which later fail
  when copied into the concurrent resolver cache. The single-entry helper ignores null arguments.
- `AFF-REQ-007`: Interrupted retry sleep restores interruption but can continue into another
  discovery attempt. After its graceful wait, shutdown calls immediate cancellation without waiting
  again, so an uncooperative discovery task may outlive client close.
