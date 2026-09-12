# Header optimization implementation

This document connects the [header-optimization specification](../header-optimization.md) to the
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java) exposes
`optimizeHeaders`, `headersWhitelist`, and computed required-header sets. Client builders infer
authentication and user-agent enablement and combine those settings with compression requirements.

## Internal architecture

[`HeadersFilteringSdkHttpClient`](../../src/main/java/com/scylladb/alternator/HeadersFilteringSdkHttpClient.java)
and
[`HeadersFilteringSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClient.java)
rebuild each outgoing request with allowed headers. Their normalized whitelist snapshots use
locale-independent lowercase matching.

User-agent transformation is performed by
[`UserAgentSdkHttpClient`](../../src/main/java/com/scylladb/alternator/UserAgentSdkHttpClient.java)
and
[`UserAgentSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/UserAgentSdkAsyncHttpClient.java).

## Lifecycle and concurrency

Attempt-routing wrappers are outermost, so they consume invocation metadata before header filtering
removes it. Header filters wrap user-agent transforms and the main transport. Closing the outer
transport delegates through each wrapper to the underlying transport. Polling clients are configured
separately and are not wrapped by main-transport filtering.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEAD-REQ-001` | [`HeadersFilteringSdkHttpClient`](../../src/main/java/com/scylladb/alternator/HeadersFilteringSdkHttpClient.java) | [`FeatureSpecDefaultsTest#headerOptimizationDefaultMatchesSpecification`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java) | `conformant` |
| `HEAD-REQ-002` | [`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java) | [`AlternatorConfigHeadersTest#testFullHeadersWhitelistContents`](../../src/test/java/com/scylladb/alternator/AlternatorConfigHeadersTest.java) | `gap` |
| `HEAD-REQ-003` | [`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java) | [`AlternatorConfigHeadersTest#testCustomWhitelistMissingRequiredHeadersThrows`](../../src/test/java/com/scylladb/alternator/AlternatorConfigHeadersTest.java) | `gap` |
| `HEAD-REQ-004` | [`AlternatorDynamoDbClient`](../../src/main/java/com/scylladb/alternator/AlternatorDynamoDbClient.java) | [`AlternatorDynamoDbClientCustomizerTest#testWithAlternatorConfigEnablesDefaultUserAgentTransformer`](../../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientCustomizerTest.java) | `gap` |
| `HEAD-REQ-005` | [`HeadersFilteringSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClient.java) | [`HeadersFilteringSdkAsyncHttpClientTest#testPreservesMetricCollector`](../../src/test/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClientTest.java) | `gap` |
| `HEAD-REQ-006` | [`HeadersFilteringSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClient.java) | [`HeadersFilteringSdkAsyncHttpClientTest#testFiltersNonWhitelistedHeaders`](../../src/test/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClientTest.java) | `gap` |

## Test coverage

- [`FeatureSpecDefaultsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java)
  executes the machine-readable opt-in default.
- [`AlternatorConfigHeadersTest`](../../src/test/java/com/scylladb/alternator/AlternatorConfigHeadersTest.java)
  covers required sets, custom validation, immutability, and feature combinations.
- [`HeadersFilteringSdkHttpClientTest`](../../src/test/java/com/scylladb/alternator/HeadersFilteringSdkHttpClientTest.java)
  covers blocking filtering, case, multi-value headers, body providers, and lifecycle.
- [`HeadersFilteringSdkAsyncHttpClientTest`](../../src/test/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClientTest.java)
  covers non-blocking publishers, handlers, duplex, metrics, completion, and lifecycle.
- [`UserAgentSdkHttpClientTest`](../../src/test/java/com/scylladb/alternator/UserAgentSdkHttpClientTest.java)
  and
  [`UserAgentSdkAsyncHttpClientTest`](../../src/test/java/com/scylladb/alternator/UserAgentSdkAsyncHttpClientTest.java)
  cover user-agent wrapper behavior.

## Known conformance gaps

- `HEAD-REQ-002`: The computed authentication whitelist contains only `Authorization` and
  `X-Amz-Date`. It drops `X-Amz-Security-Token` for temporary credentials and cannot automatically
  preserve extra headers introduced by a custom signer. Tests use only basic credentials.
- `HEAD-REQ-003`: A non-empty custom whitelist containing null reaches validation and throws
  `NullPointerException`; empty entries are accepted when all required headers are present.
  Validation also uses the process default locale, so ASCII-equivalent casing can fail under locales
  such as Turkish.
- `HEAD-REQ-004`: Retry routing now consumes invocation metadata before filtering and signs the
  selected authority. However, filtering can still remove SDK headers covered by the generated
  signature, such as retry metadata or `X-Amz-Content-Sha256`, without regenerating
  `Authorization`; combined authenticated filtering therefore remains a gap.
- `HEAD-REQ-005`, `HEAD-REQ-006`: Blocking filtering and user-agent wrappers drop the request metric
  collector. Non-blocking filtering and user-agent wrappers drop HTTP execution attributes. Direct
  tests do not exercise these fields.
