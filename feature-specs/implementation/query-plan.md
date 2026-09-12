# Query-plan implementation

This document connects the [query-plan specification](../query-plan.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

Query planning is installed automatically by the client builders. Key-route affinity selects an
affinity-aware interceptor; other requests use ordinary random routing. There is no separate public
plan-lifecycle API.

## Internal architecture

[`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) captures
and orders discovered candidates.
[`GoRand`](../../src/main/java/com/scylladb/alternator/internal/GoRand.java) implements the reference
seeded generator, while `AlternatorLiveNodes.drainSeeded(...)` implements pick-and-remove selection.

[`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java)
applies active and quarantine passes and cycle behavior. Request-scoped routing state belongs to
[`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java).
It is connected to physical transmissions by
[`AttemptRoutingSdkHttpClient`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRoutingSdkHttpClient.java)
and
[`AttemptRoutingSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRoutingSdkAsyncHttpClient.java).
[`AttemptRequestSigner`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRequestSigner.java)
installs routing wrappers around the SDK's legacy and HTTP-auth signers. The HTTP-auth implementation
is isolated behind runtime feature detection so loading the client remains compatible with AWS SDK
2.20, which predates that SPI. SDK 2.20 through 2.25 resolve authentication too late for a user
interceptor to wrap the selected scheme, so the builders install the equivalent DynamoDB legacy
signer wrapper at client configuration time; SDK 2.26 and later wrap the selected HTTP-auth scheme.

## Lifecycle and concurrency

`beforeExecution` creates one plan and routing state and wraps the SDK-selected authentication
signer. `modifyHttpRequest` selects the initial route. On every attempt, the wrapper performs final
route selection immediately before delegating once to the configured signer, so the signer receives
fresh pre-sign request state and the SDK retains ownership of body transformation and signing and
write metrics. The selected route remains pending until the transport wrapper is reached. A local
signing or interceptor failure therefore neither reports node failure nor consumes the unsent route.
`beforeTransmission` registers execution attributes by `amz-sdk-invocation-id`; the outer transport
wrapper consumes that registration and performs the final unsigned-routing guard. Synchronous
requests arm in-flight health attribution when the prepared HTTP request is called; asynchronous
user-agent callbacks run before routing arms attribution, while transport invocation failures remain
associated with the selected node. `afterTransmission` clears in-flight state for every response;
execution completion or failure unregisters routing state.

The routing registry is concurrent. Individual plans are request-scoped and synchronized only where
health-aware selection mutates tried-state.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `QUERY-REQ-001` | [`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) | [`LazyQueryPlanTest#testLazyBehaviorReadsCurrentNodes`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) | `gap` |
| `QUERY-REQ-002` | [`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) | [`LazyQueryPlanTest#testNodesAreNotDuplicated`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) | `conformant` |
| `QUERY-REQ-003` | [`GoRand`](../../src/main/java/com/scylladb/alternator/internal/GoRand.java) | [`FeatureSpecVectorsTest#seededPlansMatchPortableVectors`](../../src/test/java/com/scylladb/alternator/FeatureSpecVectorsTest.java) | `conformant` |
| `QUERY-REQ-004` | [`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) | [`LazyQueryPlanTest#testPreferredNodesAreReturnedBeforeSortedRemaining`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) | `gap` |
| `QUERY-REQ-005` | [`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java) | [`RetryDistributionTest#testSdkRetryPipelineRoutesEachAttemptToDifferentNode`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) | `conformant` |
| `QUERY-REQ-006` | [`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) | [`NodeHealthQueryPlanTest#regularPlanReturnsActiveThenQuarantineInSourceRelativeOrder`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java) | `conformant` |
| `QUERY-REQ-007` | [`AttemptRoutingSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRoutingSdkAsyncHttpClient.java) | [`RetryDistributionTest#testAsyncSdkRetryPipelineRoutesEachAttemptToDifferentNode`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) | `conformant` |

## Test coverage

- [`FeatureSpecVectorsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecVectorsTest.java)
  executes the portable seeded-order table.
- [`FeatureSpecCanonicalEndpointVectorsTest`](../../src/test/java/com/scylladb/alternator/internal/FeatureSpecCanonicalEndpointVectorsTest.java)
  executes the portable canonical-endpoint table against the implementation's sort and deduplication
  path.
- [`LazyQueryPlanTest`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) covers
  iteration, random and seeded uniqueness, preferred prefixes, sorting, and health-agnostic order.
- [`LazyQueryPlanCrossLanguageTest`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java)
  covers canonical seeded sequences for fixed, negative, zero, and maximum seeds.
- [`RetryDistributionTest`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java)
  covers retry traversal, cycles, physical blocking and non-blocking routing, in-flight attribution,
  pre-transport failures, unsent-route reuse, IPv4 and IPv6 authority replacement, signature
  validation, and server-response health classification.
- [`AttemptRequestSignerTest`](../../src/test/java/com/scylladb/alternator/queryplan/AttemptRequestSignerTest.java)
  covers unsigned fallback safety and IPv6 authority formatting. `RetryDistributionTest` exercises
  default, anonymous, legacy, body-transforming, client-level, and request-level signers through the
  complete blocking and non-blocking SDK pipelines.
- [`RetrySigningMetricsTest`](../../src/test/java/com/scylladb/alternator/RetrySigningMetricsTest.java)
  verifies that local signing reads do not count as transport writes for blocking or non-blocking
  attempts.
- [`NodeHealthQueryPlanTest`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java)
  covers active and quarantine passes, dynamic eligibility, canonical duplication, and down-node
  exclusion.

## Known conformance gaps

- `QUERY-REQ-001`: The lazy-snapshot boundary lacks a topology-mutation test. The test named
  `testLazyBehaviorReadsCurrentNodes` neither changes topology nor verifies behavior before and after
  initialization; its comment incorrectly says every iterator call reads current state.
- `QUERY-REQ-004`: Preferred lists containing duplicates or unavailable endpoints lack direct tests.
