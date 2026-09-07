# Query plans

This specification defines how an Alternator client creates a request-scoped endpoint order and
uses it across retries. It is language agnostic; deterministic algorithms and observable routing
behavior are normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

A query plan separates endpoint ordering from transmission. One logical DynamoDB query receives one
candidate order, and successive attempts traverse that order without repeatedly selecting the same
planned endpoint.

This feature owns:

- random, seeded, and preferred candidate ordering;
- lazy topology snapshots;
- plan lifetime and retry traversal;
- per-attempt endpoint replacement; and
- active-then-quarantine endpoint eligibility.

Node eligibility is deliberately not part of base-plan construction. It is applied by the
health-aware routing layer described in [Node health](node-health.md).

## Vocabulary and implementation mapping

| Term | Portable meaning | Java implementation |
| --- | --- | --- |
| Logical query | One user-visible DynamoDB operation, including all SDK retries. | One execution lifecycle handled by [`BasicQueryPlanInterceptor`](../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java) |
| Attempt | One transmission of a logical query to one endpoint. | A call through the attempt-routing transport wrappers |
| Candidate | Endpoint present in the plan snapshot. | A `URI` returned by [`LazyQueryPlan`](../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) |
| Candidate snapshot | Discovered endpoint set captured when the plan is first accessed. | Lazy initialization in `LazyQueryPlan.ensureInitialized()` |
| Base plan | Health-agnostic ordered sequence containing each candidate at most once. | `LazyQueryPlan` |
| Random plan | Non-deterministic permutation used for normal load distribution. | Unseeded `LazyQueryPlan` |
| Seeded plan | Cross-language deterministic permutation derived from a signed 64-bit seed. | Seeded `LazyQueryPlan`, `AlternatorLiveNodes.drainSeeded(...)`, and [`GoRand`](../src/main/java/com/scylladb/alternator/internal/GoRand.java) |
| Preferred plan | Listed preferred endpoints first, followed by remaining endpoints in canonical order. | Preferred-node `LazyQueryPlan` |
| Affinity plan | Seeded or preferred plan whose order represents key-route affinity. | Plan built by [`AffinityQueryPlanInterceptor`](../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java) |
| Health wrapper | Request-scoped final eligibility layer around the base plan. | [`NodeHealthQueryPlan`](../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) |
| Routing state | Request-scoped mutable state containing the health wrapper, its base-plan identity, the current in-flight endpoint, first-attempt status, and execution identifier. | One private `RoutingState` stored in the `ROUTING_STATE` execution attribute |
| In-flight endpoint | Endpoint associated with the current transmission until a response or transport failure is observed. | `RoutingState.inFlightNode` |
| Execution identifier | SDK-provided identifier connecting transport attempts to request-scoped state. | `amz-sdk-invocation-id` and `routingExecutions` |

## Plan construction

Every logical query must receive a new base plan. Plans must not be shared by concurrent logical
queries.

The plan captures the discovered endpoint set lazily on first access. Topology changes before first
access are visible; changes after initialization do not rebuild or reorder that query's plan.

Before ordering, deterministic plans use the canonical discovered set: duplicate endpoint
identities are removed and the remaining endpoints are sorted lexicographically by their complete
endpoint string.

### Random plans

A random plan creates a non-deterministic permutation of the candidate snapshot. Every candidate
appears at most once. The random source need not be cross-language compatible.

Independent logical queries should not consistently choose the same first endpoint when multiple
candidates exist. No exact distribution is guaranteed for a small number of queries.

### Seeded plans

A seeded plan must produce the same sequence in every compatible Alternator client for the same:

- signed 64-bit seed;
- canonical endpoint strings; and
- candidate set.

The portable algorithm is:

1. sort and deduplicate candidates canonically;
2. initialize a generator compatible with `Go math/rand.NewSource(seed)`;
3. while candidates remain, compute `index = Intn(remaining count)`;
4. append the indexed candidate to the output;
5. replace that slot with the last remaining candidate; and
6. remove the last slot.

This pick-and-remove behavior, including swap-with-last removal, is part of the compatibility
contract. A conventional shuffle is not equivalent.

The generator must match the Go lagged-Fibonacci source, including seed normalization, zero-seed
handling, and rejection sampling in `Intn`.

### Preferred plans

A preferred plan must:

1. include each preferred endpoint that exists in the discovered set, in supplied preference order;
2. ignore unavailable preferred identities and repeated canonical identities; and
3. append every remaining discovered endpoint once in canonical order.

The preferred prefix is deterministic and is used by batch affinity. It does not make an endpoint
health-eligible; final health filtering still applies.

## Request and retry routing

The initial request and every retry of one logical query must reuse the same base plan and health
wrapper.

For each attempt, the routing layer must:

1. finish accounting for the preceding in-flight attempt, if any;
2. select the next eligible route from the request-scoped wrapper;
3. replace the request scheme, host, and port with the selected endpoint;
4. preserve operation path, query, method, headers, and body except for changes owned by other
   features; and
5. associate the selected endpoint with the transmission before it is sent.

Receiving any HTTP response clears the in-flight endpoint. A failure before receiving a response is
attributed to that endpoint exactly once. Detailed health classification belongs to the node-health
specification.

### Plan exhaustion

Endpoint eligibility scans for an untried active candidate before scanning for an untried
quarantined candidate on every selection. Down candidates are excluded. Once every currently
eligible canonical endpoint has been tried,
traffic plans clear their tried set and begin another cycle if the SDK requests another attempt.
Regular plans reshuffle the captured snapshot for each new cycle; affinity plans reuse the original
deterministic order. If a fresh scan finds only down endpoints, the request fails locally with a
no-route error. The endpoint already present on the SDK request is never used as fallback.

## Interactions with other features

### Node health

The base plan contains discovered active, quarantined, and down endpoints so topology and affinity
order remain stable. A health wrapper returns active candidates before quarantine, tracks canonical
endpoints tried in the current cycle, and rechecks state before every selection. See [Node
health](node-health.md).

### Key-route affinity

Key-route affinity selects seeded or preferred plan construction instead of random construction. It
does not own retry traversal, endpoint replacement, or result attribution. See
[Key-route affinity](key-route-affinity.md).

### Topology discovery

Plans read from the current discovered routing ring, not directly from seed configuration. Initial
seeds may temporarily form that ring before the first successful topology refresh. The lazy
snapshot ensures the freshest set available at first route selection while preventing mid-request
topology changes from unpredictably rebuilding the plan.

### Header optimization

Attempt routing must consume the SDK execution identifier before header optimization removes it
from the wire. Filtering attempt metadata must not disconnect a retry from its request-scoped plan.
See [Header optimization](header-optimization.md).

### Compression and signing

Changing an attempt destination must not change the logical body. Compressed and uncompressed
bodies must remain replayable. Endpoint replacement, signing, and header filtering must occur in an
order that leaves the transmitted authority and signature consistent. See [Compression](compression.md).

### Synchronous and asynchronous transports

Both transport forms must select the same plan type and advance once per actual transmission. The
async path must route before delegating execution without changing its completion, response-handler,
publisher, cancellation, duplex, or metrics semantics.

## Interaction edge cases

- A topology update after plan construction affects later logical queries, not the initialized base
  order of the current query.
- A topology update before first plan access is visible because snapshot capture is lazy.
- A health transition during retries is visible at final selection even though the base order is
  unchanged.
- An affinity plan must not be recalculated after an endpoint becomes down; each health pass
  preserves the corresponding subsequence of the original deterministic order.
- Traffic plans may repeat endpoints only after every currently eligible canonical endpoint in the
  request snapshot has been tried. Probe plans never repeat.
- A retryable HTTP server response clears in-flight accounting even though it produces no health
  observation.
- A transport exception before response processing remains associated with the endpoint selected
  for that attempt.
- Header optimization may remove the execution identifier from the transmitted request only after
  the attempt router has used it.
- Request compression must not consume a one-shot body such that later routed attempts cannot replay
  it.
- Preferred candidates that are down remain in deterministic plan order but fail the final health
  gate.

## Regular logic edge cases

- An empty candidate snapshot exhausts immediately and produces no route.
- A one-endpoint traffic plan may return that endpoint once per cycle while SDK retries continue.
- Calling `next` after base-plan exhaustion follows the host language's iterator contract and must
  not invent a candidate.
- Duplicate discovered endpoints appear once after canonical deduplication.
- Null plan dependencies and null preferred lists are invalid construction inputs.
- A preferred list may be empty, contain duplicates, or mention endpoints not in the discovered set.
- Equal seeds and equal canonical candidate sets produce byte-for-byte equivalent endpoint orders.
- Different seeds are allowed to produce the same order by chance; they are not required to differ.
- Negative, zero, and maximum signed 64-bit seeds are valid.
- The base plan itself is request-scoped and need not be thread-safe; shared routing registries must
  be concurrency-safe.
- If request-scoped routing state cannot be found for an attempt, the transport must delegate the
  request unchanged rather than attach it to another query's plan.

## Conformance tests

| Area | Test location |
| --- | --- |
| Lazy snapshots, random and seeded behavior, uniqueness, preferred prefixes, and health-agnostic stable order | [`LazyQueryPlanTest`](../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) |
| Go-compatible seeded order for fixed, negative, zero, and maximum seeds | [`LazyQueryPlanCrossLanguageTest`](../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java) |
| Retry traversal, plan exhaustion, sync/async routing, in-flight result attribution, and health-neutral server responses | [`RetryDistributionTest`](../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) |
| Active and quarantine passes, source-order preservation, dynamic eligibility, and down-node exclusion | [`NodeHealthQueryPlanTest`](../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java) |
| Affinity and random plan selection by request type | [`AffinityQueryPlanInterceptorTest`](../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) |
| Stable batch preferred-node ordering | [`BatchWriteItemKeyRouteAffinityCrossLanguageTest`](../src/test/java/com/scylladb/alternator/keyrouting/BatchWriteItemKeyRouteAffinityCrossLanguageTest.java) |
