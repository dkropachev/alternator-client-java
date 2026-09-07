# Header optimization

This specification defines how an Alternator client may reduce request size by removing HTTP
headers that Alternator does not require. It is language agnostic; header names and observable
wire behavior are normative, while implementation names are informative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Header optimization is an opt-in transport feature. It filters the final outgoing header set while
preserving every header required by the active authentication, compression, connection, operation,
and client-identification configuration.

Filtering must not change the request URI, method, body, response handling, retry routing, metrics,
or transport lifecycle.

## Vocabulary and implementation mapping

| Term | Portable meaning | Java implementation |
| --- | --- | --- |
| Header optimization | Removal of non-whitelisted outgoing HTTP headers immediately before transport execution. | `optimizeHeaders` in [`AlternatorConfig`](../src/main/java/com/scylladb/alternator/AlternatorConfig.java) |
| Whitelist | Case-insensitive set of header names allowed on the wire. | `headersWhitelist` in `AlternatorConfig` |
| Required headers | Minimum whitelist derived from enabled client features. | `getRequiredHeaders()` and the header-set constants in `AlternatorConfig` |
| Base headers | Headers required for every Alternator request. | `BASE_REQUIRED_HEADERS` |
| Conditional headers | Headers required only when authentication, compression, or user-agent reporting is enabled. | `AUTHENTICATION_HEADERS`, `COMPRESSION_HEADERS`, `RESPONSE_COMPRESSION_HEADERS`, and `USER_AGENT_HEADERS` |
| Filter | Transport wrapper that rebuilds a request with only allowed headers. | [`HeadersFilteringSdkHttpClient`](../src/main/java/com/scylladb/alternator/HeadersFilteringSdkHttpClient.java) and [`HeadersFilteringSdkAsyncHttpClient`](../src/main/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClient.java) |
| Main transport | Transport used for DynamoDB data-plane operations. | Main SDK HTTP client configured by the sync and async client builders |
| Polling transport | Transport used for `/localnodes` discovery and probes. | Polling client owned or accepted by `AlternatorLiveNodes` |

## Core behavior

Header optimization is disabled by default. When disabled, the client must not remove SDK-generated
headers through this feature.

When enabled, the client must:

1. inspect the final request after normal SDK request construction;
2. compare header names case-insensitively;
3. remove every header whose name is not in the effective whitelist;
4. preserve the original spelling and all values of every allowed header; and
5. pass the request body and all non-header transport metadata through unchanged.

Filtering applies independently to every retry attempt because the SDK may regenerate attempt
headers. Whitelist membership is by header name, not by value.

## Required header sets

The effective default whitelist is the union of the applicable sets below.

### Always required

| Header | Purpose |
| --- | --- |
| `Host` | HTTP authority and request signing. |
| `X-Amz-Target` | DynamoDB operation selection. |
| `Content-Type` | DynamoDB protocol media type. |
| `Content-Length` | Request-body framing. |
| `Connection` | Explicit connection reuse behavior. |

### Conditionally required

| Condition | Required headers |
| --- | --- |
| Authentication enabled | `Authorization`, `X-Amz-Date` |
| Request compression enabled | `Content-Encoding` |
| Response compression enabled | `Accept-Encoding` |
| User-agent reporting enabled | `User-Agent` |

Disabling a feature must remove only that feature's conditional requirement. For example, disabling
authentication removes `Authorization` and `X-Amz-Date` from the computed minimum, but it does not
remove base, compression, or user-agent requirements.

## Custom whitelists

A custom whitelist replaces the computed default and may add application-specific headers. It must
not be null or empty and must contain the complete required set for the final configuration.

Validation must be case-insensitive and must occur after considering all feature settings. Missing
required headers are configuration errors and must fail client configuration rather than silently
producing malformed or unauthenticated requests.

The stored whitelist and every exposed required-header set must be immutable snapshots. Mutating a
caller-owned input collection after configuration must not change client behavior.

Setting a custom whitelist is validated even if optimization is currently disabled, so later
enabling optimization cannot activate an invalid configuration.

## Interactions with other features

### Authentication

When credentials are configured, the signing headers are mandatory. When anonymous access is used,
authentication headers are not required and should be removed unless explicitly retained for a
separate application purpose.

Filtering must happen late enough to see signed requests, but it must not remove a header required
by the signature represented in `Authorization`.

### Request and response compression

Request compression requires `Content-Encoding`; response compression requires `Accept-Encoding`.
Changing either compression setting changes the computed minimum whitelist. See
[Compression](compression.md).

### User-agent reporting

If user-agent reporting is enabled, `User-Agent` must reach the wire even when filtering is enabled.
If reporting is disabled, it is not required and must not be reintroduced by wrapper ordering.

### Query plans and retries

Per-attempt routing may depend on SDK-internal execution identifiers that are present before the
filter runs. The routing layer must consume any required internal metadata before the filter removes
it. Removing retry metadata from the wire must not disable retry rerouting. See
[Query plans](query-plan.md).

### Node health

Filtering must not change attempt outcome attribution. A filtered request is reported against the
endpoint selected before filtering, using the classification in [Node health](node-health.md).

### Polling and control-plane requests

Header optimization is a main-transport feature. A polling transport may use a smaller independent
configuration, but it must retain headers required for its own authentication and user-agent
behavior. Main-transport filtering must not accidentally wrap or close a caller-owned polling
transport.

### Synchronous and asynchronous transports

Both forms must produce the same filtered header map. A synchronous wrapper must preserve the body
provider. An asynchronous wrapper must preserve the body publisher, response handler, duplex flag,
metric collector, returned completion object, and cancellation/error behavior.

## Interaction edge cases

- Enabling request compression after constructing a custom whitelist without `Content-Encoding`
  must fail validation.
- Enabling response compression without `Accept-Encoding` in a custom whitelist must fail.
- Disabling response compression makes a whitelist without `Accept-Encoding` valid.
- Disabling user-agent reporting makes a whitelist without `User-Agent` valid.
- Disabling authentication makes a whitelist without `Authorization` and `X-Amz-Date` valid.
- Retry routing must still work after `amz-sdk-invocation-id` and `amz-sdk-request` are removed from
  the wire.
- Route changes must update the effective request destination before filtering without losing the
  required `Host` behavior.
- Compression and signing interceptors may replace headers; filtering must evaluate the final values
  on each attempt rather than cache an earlier header map.
- Closing the filtering wrapper must close its wrapped main transport exactly according to normal
  transport ownership rules.

## Regular logic edge cases

- Header matching is case-insensitive.
- Multi-value headers preserve value count and order.
- A request with no headers remains valid input to the filter and produces no headers.
- Null and empty entries passed directly to a low-level filter wrapper are ignored; public
  configuration still rejects a null or empty whitelist.
- Headers not in the whitelist are removed even if they were generated internally by the SDK.
- Filtering changes only headers; URI, method, request content, and response callbacks are
  unchanged.
- The wrapper reports the same transport name as its delegate and propagates the delegate's
  completion or executable request.

## Conformance tests

| Area | Test location |
| --- | --- |
| Required-set computation, custom whitelist validation, immutability, authentication, compression, and user-agent interaction | [`AlternatorConfigHeadersTest`](../src/test/java/com/scylladb/alternator/AlternatorConfigHeadersTest.java) |
| Synchronous filtering, casing, multi-value headers, request identity, body providers, and lifecycle | [`HeadersFilteringSdkHttpClientTest`](../src/test/java/com/scylladb/alternator/HeadersFilteringSdkHttpClientTest.java) |
| Asynchronous filtering, publishers, handlers, duplex, metrics, completion, and lifecycle | [`HeadersFilteringSdkAsyncHttpClientTest`](../src/test/java/com/scylladb/alternator/HeadersFilteringSdkAsyncHttpClientTest.java) |
| User-agent construction, transformation, filtering interaction, and lifecycle | [`UserAgentSdkHttpClientTest`](../src/test/java/com/scylladb/alternator/UserAgentSdkHttpClientTest.java) and [`UserAgentSdkAsyncHttpClientTest`](../src/test/java/com/scylladb/alternator/UserAgentSdkAsyncHttpClientTest.java) |
| Sync and async builder propagation and wrapper composition | [`AlternatorDynamoDbClientCustomizerTest`](../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientCustomizerTest.java) and [`AlternatorDynamoDbAsyncClientCustomizerTest`](../src/test/java/com/scylladb/alternator/AlternatorDynamoDbAsyncClientCustomizerTest.java) |
