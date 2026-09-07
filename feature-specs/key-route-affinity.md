# Key-route affinity

This specification defines deterministic coordinator preference derived from DynamoDB partition
keys. It is language agnostic and includes the hashing, operation classification, metadata
discovery, batch voting, and fallback rules needed for compatible implementations.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Key-route affinity routes qualifying requests for the same partition key toward the same preferred
Alternator coordinator. This can reduce coordination work and latency for lightweight transactions
and read-before-write operations.

Affinity is a preference, not a guarantee. Retries, topology changes, missing metadata, unsupported
key values, and node health may select a different endpoint.

## Vocabulary and implementation mapping

| Term | Portable meaning | Java implementation |
| --- | --- | --- |
| Affinity mode | Policy deciding which operations qualify: disabled, read-modify-write, or any write. | [`KeyRouteAffinity`](../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinity.java) |
| Affinity configuration | Mode plus optional table-to-partition-key-name mappings. | [`KeyRouteAffinityConfig`](../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfig.java) |
| Qualifying request | Request whose operation and fields satisfy the selected affinity mode. | [`KeyAffinityRequestClassifier`](../src/main/java/com/scylladb/alternator/keyrouting/KeyAffinityRequestClassifier.java) |
| Partition-key metadata | Mapping from table name to HASH-key attribute name. | [`PartitionKeyResolver`](../src/main/java/com/scylladb/alternator/keyrouting/PartitionKeyResolver.java) |
| Affinity value | The request attribute value belonging to the table partition key. | `AttributeValue` extracted by `KeyAffinityRequestClassifier` |
| Affinity hash | Signed 64-bit hash produced from a typed partition-key byte representation. | [`AttributeValueHasher`](../src/main/java/com/scylladb/alternator/keyrouting/AttributeValueHasher.java) and [`MurmurHash3`](../src/main/java/com/scylladb/alternator/keyrouting/MurmurHash3.java) |
| Affinity ring | Canonically sorted discovered endpoint set used to derive preferences. | Query-plan nodes returned by `AlternatorLiveNodes` |
| Preferred endpoint | First endpoint produced by the seeded query-plan algorithm for one affinity hash. | `getPreferredQueryPlanNodeForHash(...)` |
| Seeded affinity plan | Deterministic candidate permutation seeded by one affinity hash. | Seeded [`LazyQueryPlan`](../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) |
| Batch target | One usable put or delete within `BatchWriteItem`. | `BatchWriteRoutingTarget` |
| Vote | One batch target's preferred endpoint contribution. | Vote map in [`AffinityQueryPlanInterceptor`](../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java) |
| Random fallback | Ordinary non-deterministic query plan used when affinity cannot be applied safely. | Base-plan fallback in `AffinityQueryPlanInterceptor.initializeQueryPlan(...)` |

## Affinity modes

### Disabled

When affinity is disabled, every request uses ordinary random query-plan routing. Partition-key
metadata must not be required or discovered solely for affinity.

### Any write

`ANY_WRITE` applies to every `PutItem`, `UpdateItem`, and `DeleteItem`. It applies to
`BatchWriteItem` when at least one put or delete target can be extracted.

Reads and unsupported operations do not qualify.

### Read-modify-write

`RMW` applies only when the operation is expected to read existing state as part of a write.

| Operation | Qualifying condition |
| --- | --- |
| `PutItem` | Non-empty `ConditionExpression`, non-empty legacy `Expected`, or `ReturnValues` other than `NONE`. |
| `DeleteItem` | Non-empty `ConditionExpression`, non-empty legacy `Expected`, or `ReturnValues` other than `NONE`. |
| `UpdateItem` | Non-empty `UpdateExpression`; non-empty `ConditionExpression`; non-empty legacy `Expected`; `ReturnValues` equal to `ALL_OLD`, `UPDATED_OLD`, or `ALL_NEW`; legacy `ADD`; or legacy `DELETE` with a value. |

For `UpdateItem`, `UPDATED_NEW` alone does not qualify, and legacy `DELETE` without a value does not
qualify. `BatchWriteItem` is excluded from `RMW` because it is write-only. Read operations do not
qualify in either enabled mode.

## Partition-key metadata

Affinity requires the partition-key attribute name for each table.

An implementation must support preconfigured metadata. A synchronous implementation may discover
missing metadata using `DescribeTable`; an implementation that cannot perform discovery, including
this asynchronous Java client, must use random fallback until metadata is provided.

Metadata discovery is asynchronous relative to the user request:

1. the request that finds missing metadata uses random fallback;
2. at most one discovery operation per table is active concurrently;
3. successful discovery caches the HASH-key attribute name; and
4. later qualifying requests may use affinity.

Transient failures receive one initial attempt plus at most three retries. Backoff starts at 100 ms,
doubles to at most 2 seconds, and applies random jitter of up to 20 percent in either direction.

Table-not-found, HTTP 403, and errors identified as access-denied or validation failures are treated
as permanent for discovery purposes. A 4xx response without structured error details is also
permanent except for HTTP 429. Permanent failures suppress new discovery for five minutes unless
the failure record is cleared explicitly. Other structured errors, HTTP 429, server failures, and
transport failures are transient. Exhausting transient retries does not impose the permanent-failure
cooldown; a later request may trigger a new discovery sequence.

Discovery resources must be shut down with the owning client. Shutdown must preserve interruption
and may force termination after a bounded graceful wait.

## Partition-key encoding and hashing

Only DynamoDB scalar partition-key types `S`, `N`, and `B` are supported.

The byte representation is:

| Type | Prefix | Payload |
| --- | ---: | --- |
| String (`S`) | `0x01` | UTF-8 bytes of the exact string value |
| Number (`N`) | `0x02` | UTF-8 bytes of the exact number string |
| Binary (`B`) | `0x03` | Raw binary bytes |

The prefix is part of the hash input and prevents equal payload bytes of different DynamoDB types
from sharing the same typed representation.

Number strings must not be numerically normalized. For example, `42`, `42.0`, and `4.2e1` are
different affinity inputs.

The typed bytes are hashed with MurmurHash3 x86 128-bit, seed zero. The first 64 result bits form the
signed affinity seed. Every client implementation must match the shared hash test vectors exactly.

Unsupported DynamoDB types, missing values, or malformed key shapes cause random fallback; they
must not fail the user operation merely because affinity cannot be applied.

For a composite primary key, only the HASH partition-key attribute is used. The sort key must not
affect coordinator preference.

## Single-item routing

For a qualifying `PutItem`, `UpdateItem`, or `DeleteItem`:

1. extract the table name;
2. resolve its partition-key attribute name;
3. extract that attribute from the item or key map;
4. encode and hash the value;
5. build the cross-language seeded candidate plan described in [Query plans](query-plan.md); and
6. preserve that plan for all retries of the logical query.

If any step cannot be completed, use random fallback for the entire logical query.

## Batch-write voting

`BatchWriteItem` under `ANY_WRITE` uses all usable put and delete targets rather than the first map
entry or first item.

For each target:

1. resolve metadata for its table;
2. extract and hash its partition key;
3. calculate its preferred endpoint from the stable affinity ring; and
4. add one vote for that endpoint.

Targets with missing metadata, missing partition keys, unsupported values, null write lists, or
unsupported write shapes are skipped. Missing metadata may trigger background discovery.

Voted endpoints are ordered by descending vote count. Ties are ordered lexicographically by the
complete endpoint string. The remaining discovered endpoints follow in canonical order. This
ordering must not depend on table-map iteration order, write-list order where votes are equivalent,
or non-key item attributes.

If no usable target produces a vote, the entire batch uses random fallback.

## Interactions with other features

### Query plans and retries

Affinity constructs the base candidate order; the general query-plan feature owns traversal,
per-attempt routing, and fallback. Retries must continue through the same affinity order rather than
rehashing or rebuilding the plan. See [Query plans](query-plan.md).

### Node health

Hashing and batch voting use the stable discovered endpoint set, including quarantined and down
endpoints. Health is applied only at final selection so a temporary health change does not remap
unaffected keys.

An affinity health wrapper scans the same deterministic order for an untried active candidate before
scanning it for an untried quarantined candidate. Down endpoints are never returned. After every
currently eligible endpoint has been tried, a subsequent SDK attempt begins another cycle over the
same deterministic order. See [Node health](node-health.md).

### Topology discovery

Adding or removing a discovered endpoint changes the affinity ring and may remap keys. Merely
changing an endpoint's health state must not change the ring or deterministic base order.

### Header optimization and compression

Affinity changes only request destination and plan order. It must preserve headers, request body,
compression, signing, and response handling. Background `DescribeTable` requests use the normal
client stack and must remain compatible with [Header optimization](header-optimization.md) and
[Compression](compression.md).

### Synchronous and asynchronous clients

Both forms must produce identical hashes and endpoint orders when given the same metadata. If an
async client cannot run metadata discovery, missing metadata causes random fallback rather than
blocking or failing the request.

## Interaction edge cases

- A qualifying request arriving before metadata discovery completes uses random fallback; its
  retries must not switch to affinity mid-query after the cache is populated.
- Concurrent misses for one table trigger at most one `DescribeTable` workflow.
- A metadata failure for one table in a batch does not prevent usable targets from other tables
  from voting.
- A down endpoint may win a hash or batch vote but is skipped by final health selection without
  recomputing votes.
- Quarantined candidates must not be returned until the active pass is exhausted.
- A topology update after plan initialization affects later queries, not retries of the current
  query.
- A request compressed or signed before transmission retains identical logical content when routed
  through an affinity plan.
- Closing the client must also stop background metadata discovery without affecting already cached
  metadata during normal operation.

## Regular logic edge cases

- A null affinity mode is normalized to disabled behavior.
- Null entries passed to preconfiguration helpers are ignored; successfully stored configuration is
  immutable from caller mutation.
- An empty condition or expected map does not qualify by itself.
- `ReturnValues.NONE` does not qualify by itself.
- `UpdateExpression` qualifies in `RMW` even without a separate condition.
- A null partition-key value does not create an affinity plan.
- Empty strings and empty binary values are valid hash inputs when accepted by the service.
- High-bit binary bytes are hashed as raw unsigned byte content.
- Unsupported BOOL, NULL, set, list, and map values fall back to random routing.
- Numerically equivalent but textually distinct number values may select different endpoints.
- Batch put routing depends only on the configured partition-key attribute, not other item fields.
- Equal vote counts use deterministic endpoint-string ordering.
- A batch with empty request items or no usable put/delete target uses random fallback.
- Permanent metadata failures become eligible for discovery again only after cooldown or explicit
  clearing.

## Conformance tests

| Area | Test location |
| --- | --- |
| Affinity-mode configuration and preconfigured metadata | [`KeyRouteAffinityConfigTest`](../src/test/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfigTest.java) |
| Operation classification, table extraction, key extraction, and batch target extraction | [`KeyAffinityRequestClassifierTest`](../src/test/java/com/scylladb/alternator/keyrouting/KeyAffinityRequestClassifierTest.java) |
| Typed encoding, scalar types, unsupported values, Unicode, number representation, and concurrency | [`AttributeValueHasherTest`](../src/test/java/com/scylladb/alternator/keyrouting/AttributeValueHasherTest.java) |
| MurmurHash3 algorithm vectors and boundaries | [`MurmurHash3Test`](../src/test/java/com/scylladb/alternator/keyrouting/MurmurHash3Test.java) |
| Metadata caching, discovery retries, cooldown, clearing, and concurrency | [`PartitionKeyResolverTest`](../src/test/java/com/scylladb/alternator/keyrouting/PartitionKeyResolverTest.java) |
| Operation-level plan selection, batch voting, deterministic ties, missing metadata, random fallback, and health interaction | [`AffinityQueryPlanInterceptorTest`](../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) |
| Cross-language batch target and vote behavior | [`BatchWriteItemKeyRouteAffinityCrossLanguageTest`](../src/test/java/com/scylladb/alternator/keyrouting/BatchWriteItemKeyRouteAffinityCrossLanguageTest.java) |
| Cross-language seeded endpoint order | [`LazyQueryPlanCrossLanguageTest`](../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java) |
