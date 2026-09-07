# Compression

This specification defines request compression, response-compression negotiation, and transparent
response decompression for Alternator clients. It is language agnostic; algorithm tokens and wire
behavior are normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Compression reduces bandwidth at the cost of CPU, buffering, and latency. Request and response
compression are independent opt-in features:

- request compression transforms a DynamoDB request body before transmission; and
- response compression advertises acceptable encodings and decodes a matching server response
  before protocol unmarshalling.

Enabling one direction must not implicitly enable the other.

## Vocabulary and implementation mapping

| Term | Portable meaning | Java implementation |
| --- | --- | --- |
| Request compression | Encoding an outgoing DynamoDB body and declaring that encoding on the request. | [`GzipRequestInterceptor`](../src/main/java/com/scylladb/alternator/GzipRequestInterceptor.java) |
| Request algorithm | Configured request-body encoding or disabled value. | [`RequestCompressionAlgorithm`](../src/main/java/com/scylladb/alternator/RequestCompressionAlgorithm.java) |
| Compression threshold | Original uncompressed body size at or above which request compression applies. | `minCompressionSizeBytes` in [`AlternatorConfig`](../src/main/java/com/scylladb/alternator/AlternatorConfig.java) |
| Response negotiation | Advertising an ordered list of response encodings in `Accept-Encoding`. | `ResponseCompressionInterceptor.modifyHttpRequest(...)` |
| Response algorithm | Encoding the client advertises and can decode. | [`ResponseCompressionAlgorithm`](../src/main/java/com/scylladb/alternator/ResponseCompressionAlgorithm.java) |
| Transparent decompression | Replacing the encoded response stream with decoded bytes before DynamoDB unmarshalling. | [`ResponseCompressionInterceptor`](../src/main/java/com/scylladb/alternator/ResponseCompressionInterceptor.java) |
| Original body | Exact serialized DynamoDB request bytes before compression. | Request-scoped cached bytes in `GzipRequestInterceptor` |
| Encoded body | Compressed bytes placed on the wire. | Replacement sync body or async publisher produced by the request interceptor |
| Content coding | Case-insensitive HTTP token such as `gzip` or `deflate`. | `contentEncoding()` on `ResponseCompressionAlgorithm` |

## Request compression

Request compression is disabled by default. The portable disabled value is `NONE`; the supported
enabled algorithm is `GZIP`.

When enabled, the client must:

1. obtain the complete original serialized request body;
2. compare its byte length with the configured threshold;
3. compress when `original length >= threshold`;
4. replace the transmitted body with a valid gzip stream;
5. set `Content-Encoding: gzip` only when the transmitted bytes are gzip encoded; and
6. preserve bytes exactly when compression is not selected.

The default threshold is 1,024 bytes. Zero is valid and compresses every request that has a body.
A negative threshold is invalid.

A request with no body is not compressed and must not gain a compression header. The compression
decision uses byte length, not character count or estimated object size.

Decompressing the encoded body must reproduce the original body byte for byte.

### Request compression failures

Failure to read or encode a request body must never produce a header/body mismatch. An
implementation may fail the request or cleanly fall back to the original body, but an uncompressed
body must not be sent with `Content-Encoding: gzip`.

Any fallback must preserve replayability for retries. A partial encoded body must never be sent.

## Response compression

Response compression is disabled by default. The supported algorithms are `gzip` and `deflate`.

When enabled, the client must:

1. validate a non-empty ordered algorithm list with no null entries;
2. remove duplicates while retaining first-occurrence order;
3. replace any existing `Accept-Encoding` value with the configured tokens joined by comma and
   space;
4. inspect `Content-Encoding` case-insensitively on each response;
5. decode a response only when it contains exactly one encoding token supported and configured by
   the client;
6. expose decoded bytes to the DynamoDB protocol parser; and
7. remove `Content-Encoding` and `Content-Length` from the decoded response metadata.

Algorithm order in `Accept-Encoding` is observable and must preserve configuration order.

An encoding token may be matched case-insensitively and may ignore parameters following a
semicolon. Multiple content-coding tokens are not decoded by this feature.

Unsupported, unconfigured, missing, or ambiguous encodings must be left unchanged for the normal
transport or SDK behavior to handle. The client must not label undecoded bytes as decoded.

### Response decompression failures

If a response declares a configured encoding but its body cannot be decoded, the request must fail
with a client-side decompression error. Corrupt compressed bytes must not be passed to the DynamoDB
unmarshaller as if they were valid JSON.

## Streaming and memory behavior

The portable contract does not require streaming compression. An implementation may buffer a
complete request or response, but it must preserve protocol correctness and transport cancellation.

For asynchronous responses, the decompression adapter must:

- obey positive downstream demand;
- request upstream data at most once according to its buffering strategy;
- propagate upstream errors;
- propagate cancellation upstream;
- emit decoded content at most once; and
- signal completion after decoded content is emitted.

Non-positive demand is a protocol error and must cancel upstream work before notifying the
subscriber.

## Interactions with other features

### Header optimization

Request compression adds `Content-Encoding` to the required whitelist. Response compression adds
`Accept-Encoding`. Header optimization must preserve these headers when their corresponding
feature is enabled. See [Header optimization](header-optimization.md).

### Authentication and signing

The request body and compression headers must be finalized at the correct SDK phase so signing and
payload integrity apply to the bytes actually transmitted. Filtering or routing must not change the
encoded bytes after signature calculation in a way that invalidates authentication.

### Query plans and retries

All retry attempts for one logical query must transmit equivalent request content. Rerouting an
attempt changes the endpoint, not whether the body is compressed or its decoded meaning. Cached or
replacement bodies must remain replayable for the SDK retry policy. See [Query plans](query-plan.md).

### Node health

Compression does not change health classification. A server response is classified by final status
even when its body is compressed. Failure before receiving a response, including a transport-level
request-body failure, follows [Node health](node-health.md). A client-side decompression failure
after receiving a response must not be mistaken for absence of an HTTP response.

### User-agent and other interceptors

Compression negotiation and content transformation must compose with user-agent customization,
header filtering, query-plan routing, and user-provided SDK interceptors. No feature may overwrite
unrelated request or response metadata.

### Synchronous and asynchronous clients

Both client forms must make the same threshold decision, produce equivalent gzip bytes, advertise
the same ordered algorithms, decode the same server responses, and expose equivalent errors.

## Interaction edge cases

- Request compression plus header optimization must preserve `Content-Encoding` on every retry.
- Response compression plus header optimization must preserve `Accept-Encoding` while removing
  unrelated SDK headers.
- Enabling both directions may produce both headers; neither direction changes the other.
- Rerouting a compressed request must not recompress already compressed bytes.
- A retry after an encoded request must use the same original payload semantics and a fresh or
  replayable encoded body.
- A compressed retryable-server-error response is decoded for SDK error handling, while its status
  remains health-neutral.
- A decompression error after response headers arrive must clear transmission bookkeeping without
  being double-counted as a transport failure.
- Custom interceptors that set `Accept-Encoding` are overridden when response compression is
  enabled, because the client may advertise only encodings it will decode.
- Disabling response compression must neither advertise encodings nor decode a compressed response.

## Regular logic edge cases

- A body one byte below the threshold is unchanged; a body exactly at the threshold is compressed.
- Empty bodies and absent bodies are distinct from a zero-byte threshold but are never given a
  false compression declaration.
- Unicode and binary request content is measured and reproduced as bytes.
- Duplicate response algorithms appear once in the first configured position.
- A null or empty response-algorithm list is invalid; explicit disablement uses the dedicated
  disabled configuration.
- `gzip`, `GZIP`, and a single token with parameters identify the same supported coding.
- Multiple `Content-Encoding` header values or comma-separated tokens are not decoded.
- A supported but unconfigured response coding is not decoded.
- After decoding, stale encoded `Content-Length` and `Content-Encoding` metadata are absent.
- Cancellation before async completion prevents decoded emission.

## Conformance tests

| Area | Test location |
| --- | --- |
| Request algorithm values and enablement | [`RequestCompressionAlgorithmTest`](../src/test/java/com/scylladb/alternator/RequestCompressionAlgorithmTest.java) |
| Defaults, thresholds, validation, ordered response algorithms, and explicit disablement | [`AlternatorConfigCompressionTest`](../src/test/java/com/scylladb/alternator/AlternatorConfigCompressionTest.java) |
| Request threshold boundaries, header setting, gzip validity, exact round trip, missing bodies, and sync/async bodies | [`GzipRequestInterceptorTest`](../src/test/java/com/scylladb/alternator/GzipRequestInterceptorTest.java) |
| Negotiation order, header replacement, gzip/deflate decoding, header stripping, unsupported encodings, and async behavior | [`ResponseCompressionInterceptorTest`](../src/test/java/com/scylladb/alternator/ResponseCompressionInterceptorTest.java) |
| Header-whitelist interaction | [`AlternatorConfigHeadersTest`](../src/test/java/com/scylladb/alternator/AlternatorConfigHeadersTest.java) |
| Sync and async builder propagation | [`AlternatorDynamoDbClientCustomizerTest`](../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientCustomizerTest.java) and [`AlternatorDynamoDbAsyncClientCustomizerTest`](../src/test/java/com/scylladb/alternator/AlternatorDynamoDbAsyncClientCustomizerTest.java) |

### Known conformance gap

The invariant that a request-compression failure must never send original bytes with a gzip header
is not covered by an injected compressor-failure test. The current exceptional fallback is selected
after the compression header decision and must be hardened before compression failures can be
considered fully conformant. Implementations must preserve the invariant even when the underlying
compression library fails.
